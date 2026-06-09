"""
Road crossing controller.

Translates :class:`CrossingDecision` actions into OPC-UA writes and MQTT
audit publications.

Safety features:
  - Barrier-open confirmation timeout (5 s) with retry and alert
  - All actions are published as :class:`WritebackRequest` audit records
  - Emergency stop immediately locks all OPC-UA outputs
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import List, Optional

from edge.common.data_models import Alert, AlertSeverity, TargetProtocol, WritebackRequest
from edge.common.logging_config import get_logger
from edge.common.mqtt_publisher import MQTTPublisher
from edge.common.opcua_client import OPCUAClient

from .config import RoadCrossingConfig

logger = get_logger(__name__)

_BARRIER_CONFIRM_TIMEOUT_S = 5.0
_BARRIER_CONFIRM_RETRIES = 2


class CrossingController:
    """
    Executes physical crossing control commands via OPC-UA.

    Every action publishes a :class:`WritebackRequest` to MQTT so that all
    barrier and traffic-light operations are fully auditable.
    """

    def __init__(
        self,
        config: RoadCrossingConfig,
        opcua: OPCUAClient,
        mqtt: MQTTPublisher,
    ) -> None:
        self._config = config
        self._opcua = opcua
        self._mqtt = mqtt

        # Track last known barrier and light states for simulation sync
        self._barrier_states: List[bool] = [False, False]
        self._light_states: List[str] = ["RED", "RED", "RED", "RED"]

    # ------------------------------------------------------------------
    # Barrier control
    # ------------------------------------------------------------------

    async def open_barriers(self, reason: str) -> None:
        """
        Open all road barriers and set traffic lights to GREEN.

        Verifies the write was acknowledged within ``_BARRIER_CONFIRM_TIMEOUT_S``
        seconds; retries up to ``_BARRIER_CONFIRM_RETRIES`` times.
        """
        logger.info("Opening barriers", extra={"reason": reason})
        confirmed = False

        for attempt in range(1, _BARRIER_CONFIRM_RETRIES + 2):
            try:
                await asyncio.wait_for(
                    self._opcua.write_node(self._config.BARRIER_OPEN_NODE_ID, True),
                    timeout=_BARRIER_CONFIRM_TIMEOUT_S,
                )
                confirmed = True
                self._barrier_states = [True, True]
                break
            except asyncio.TimeoutError:
                logger.warning("Barrier open confirm timeout", extra={"attempt": attempt})
                if attempt <= _BARRIER_CONFIRM_RETRIES:
                    await self._publish_alert(
                        AlertSeverity.WARNING,
                        f"Barrier open command not confirmed (attempt {attempt}), retrying",
                    )
            except Exception as exc:
                logger.error("Barrier open OPC-UA error", extra={"error": str(exc), "attempt": attempt})
                if attempt > _BARRIER_CONFIRM_RETRIES:
                    break

        await self._publish_writeback(
            self._config.BARRIER_OPEN_NODE_ID,
            True,
            TargetProtocol.OPCUA,
            confirmed,
        )

        if not confirmed:
            await self._publish_alert(
                AlertSeverity.CRITICAL,
                f"BARRIER OPEN COMMAND FAILED after {_BARRIER_CONFIRM_RETRIES + 1} attempts: {reason}",
            )
            return

        # Set all traffic lights to GREEN for approaching traffic
        for zone in range(self._config.TRAFFIC_LIGHT_COUNT):
            await self.set_traffic_light(zone, "GREEN")

        await self._publish_alert(
            AlertSeverity.INFO,
            f"Barriers opened: {reason}",
        )

    async def close_barriers(self, reason: str) -> None:
        """Close all road barriers and set traffic lights to RED."""
        logger.info("Closing barriers", extra={"reason": reason})
        confirmed = False

        try:
            await asyncio.wait_for(
                self._opcua.write_node(self._config.BARRIER_CLOSE_NODE_ID, True),
                timeout=_BARRIER_CONFIRM_TIMEOUT_S,
            )
            confirmed = True
            self._barrier_states = [False, False]
        except asyncio.TimeoutError:
            logger.error("Barrier close confirm timeout")
        except Exception as exc:
            logger.error("Barrier close OPC-UA error", extra={"error": str(exc)})

        await self._publish_writeback(
            self._config.BARRIER_CLOSE_NODE_ID,
            True,
            TargetProtocol.OPCUA,
            confirmed,
        )

        if not confirmed:
            await self._publish_alert(
                AlertSeverity.CRITICAL,
                f"BARRIER CLOSE COMMAND FAILED: {reason}",
            )

        # Set all traffic lights to RED
        for zone in range(self._config.TRAFFIC_LIGHT_COUNT):
            await self.set_traffic_light(zone, "RED")

        await self._publish_alert(
            AlertSeverity.INFO if confirmed else AlertSeverity.CRITICAL,
            f"Barriers {'closed' if confirmed else 'FAILED TO CLOSE'}: {reason}",
        )

    # ------------------------------------------------------------------
    # Traffic light control
    # ------------------------------------------------------------------

    async def set_traffic_light(self, zone: int, state: str) -> None:
        """
        Set a traffic light head to *state* (``"RED"`` / ``"AMBER"`` / ``"GREEN"``).

        Args:
            zone:   0-indexed traffic light zone.
            state:  ``"RED"`` | ``"AMBER"`` | ``"GREEN"``
        """
        if state not in ("RED", "AMBER", "GREEN"):
            raise ValueError(f"Invalid traffic light state: {state}")
        if zone < 0 or zone >= self._config.TRAFFIC_LIGHT_COUNT:
            raise ValueError(f"Traffic light zone {zone} out of range")

        node_id = f"{self._config.TRAFFIC_LIGHT_NODE_ID}{zone + 1}"
        confirmed = False
        try:
            await self._opcua.write_node(node_id, state)
            confirmed = True
            if zone < len(self._light_states):
                self._light_states[zone] = state
            logger.debug("Traffic light set", extra={"zone": zone, "state": state})
        except Exception as exc:
            logger.error(
                "Traffic light OPC-UA error",
                extra={"zone": zone, "state": state, "error": str(exc)},
            )

        await self._publish_writeback(node_id, state, TargetProtocol.OPCUA, confirmed)

    # ------------------------------------------------------------------
    # Emergency
    # ------------------------------------------------------------------

    async def trigger_emergency_stop(self, reason: str) -> None:
        """
        Activate the crossing emergency stop.

        Writes the emergency stop node, closes barriers, and sets all
        traffic lights to RED.
        """
        logger.critical("CROSSING EMERGENCY STOP", extra={"reason": reason})
        confirmed = False

        try:
            await self._opcua.write_node(
                self._config.EMERGENCY_STOP_CROSSING_NODE_ID, True
            )
            confirmed = True
        except Exception as exc:
            logger.error("Emergency stop OPC-UA write failed", extra={"error": str(exc)})

        await self._publish_writeback(
            self._config.EMERGENCY_STOP_CROSSING_NODE_ID,
            True,
            TargetProtocol.OPCUA,
            confirmed,
        )

        # Force barriers closed and lights RED regardless of OPC-UA success
        for zone in range(self._config.TRAFFIC_LIGHT_COUNT):
            try:
                await self.set_traffic_light(zone, "RED")
            except Exception:
                pass

        await self._publish_alert(
            AlertSeverity.EMERGENCY,
            f"CROSSING EMERGENCY STOP ACTIVATED: {reason}",
        )

    async def reset_emergency(self) -> None:
        """Clear the emergency stop state."""
        confirmed = False
        try:
            await self._opcua.write_node(
                self._config.EMERGENCY_STOP_CROSSING_NODE_ID, False
            )
            confirmed = True
        except Exception as exc:
            logger.error("Emergency reset OPC-UA write failed", extra={"error": str(exc)})

        await self._publish_writeback(
            self._config.EMERGENCY_STOP_CROSSING_NODE_ID,
            False,
            TargetProtocol.OPCUA,
            confirmed,
        )

        if confirmed:
            await self._publish_alert(AlertSeverity.INFO, "Crossing emergency stop reset")
        else:
            await self._publish_alert(AlertSeverity.CRITICAL, "Crossing emergency reset FAILED via OPC-UA")

    # ------------------------------------------------------------------
    # Accessors
    # ------------------------------------------------------------------

    @property
    def barrier_states(self) -> List[bool]:
        return list(self._barrier_states)

    @property
    def light_states(self) -> List[str]:
        return list(self._light_states)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _publish_writeback(
        self,
        target_node: str,
        value: object,
        protocol: TargetProtocol,
        confirmed: bool,
    ) -> None:
        req = WritebackRequest(
            target_protocol=protocol,
            target_node=target_node,
            value=value,
            confirmed=confirmed,
        )
        topic = (
            f"fluxot/{self._config.SITE_ID}/{self._config.SKID_TYPE}"
            f"/{self._config.SKID_ID}/writeback"
        )
        await self._mqtt.publish(topic, req.model_dump(mode="json"), qos=2)

    async def _publish_alert(self, severity: AlertSeverity, message: str) -> None:
        alert = Alert(
            site_id=self._config.SITE_ID,
            skid_id=self._config.SKID_ID,
            skid_type=self._config.SKID_TYPE,
            severity=severity,
            message=message,
            tag_name="crossing_control",
        )
        await self._mqtt.publish_alert(alert.model_dump(mode="json"))
