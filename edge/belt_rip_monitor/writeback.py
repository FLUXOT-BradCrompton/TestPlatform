"""
Belt writeback controller.

Handles all physical write-back operations:
  - Emergency stop
  - Speed setpoint changes
  - Alarm acknowledgement
  - Belt resume

Every action:
  1. Writes the OPC-UA node
  2. Publishes a :class:`WritebackRequest` to MQTT for audit trail
  3. Publishes an alert to notify operators
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional

from edge.common.data_models import Alert, AlertSeverity, TargetProtocol, WritebackRequest
from edge.common.logging_config import get_logger
from edge.common.mqtt_publisher import MQTTPublisher
from edge.common.opcua_client import OPCUAClient

from .config import BeltRipConfig

logger = get_logger(__name__)

# OPC-UA node ID for the alarm acknowledgement node
_ALARM_ACK_NODE = "ns=2;s=BeltAlarms.Acknowledge"

# Maximum allowed speed setpoint (hard safety limit)
_MAX_SPEED_SETPOINT_MS = 6.0


class BeltWritebackController:
    """
    Executes physical write-back commands on belt control hardware.

    All methods publish an audit :class:`WritebackRequest` over MQTT
    regardless of whether the OPC-UA write succeeds, enabling post-incident
    forensic analysis.
    """

    def __init__(
        self,
        config: BeltRipConfig,
        opcua: OPCUAClient,
        mqtt: MQTTPublisher,
    ) -> None:
        self._config = config
        self._opcua = opcua
        self._mqtt = mqtt

    # ------------------------------------------------------------------
    # Emergency stop
    # ------------------------------------------------------------------

    async def execute_emergency_stop(self, reason: str) -> None:
        """
        Activate the belt emergency stop.

        Writes ``True`` to :attr:`BeltRipConfig.EMERGENCY_STOP_NODE_ID`.
        """
        if self._config.MAINTENANCE_MODE:
            logger.warning("Emergency stop suppressed – maintenance mode active")
            await self._publish_alert(
                AlertSeverity.WARNING,
                f"Emergency stop request suppressed (maintenance mode): {reason}",
            )
            return

        node_id = self._config.EMERGENCY_STOP_NODE_ID
        logger.critical("Executing belt emergency stop", extra={"reason": reason, "node_id": node_id})

        confirmed = False
        try:
            await self._opcua.write_node(node_id, True)
            confirmed = True
            logger.info("Belt emergency stop written successfully")
        except Exception as exc:
            logger.error("OPC-UA emergency stop write failed", extra={"error": str(exc)})
        finally:
            await self._publish_writeback(node_id, True, TargetProtocol.OPCUA, confirmed)

        await self._publish_alert(
            AlertSeverity.EMERGENCY,
            f"BELT EMERGENCY STOP ACTIVATED: {reason}",
        )

    # ------------------------------------------------------------------
    # Speed setpoint
    # ------------------------------------------------------------------

    async def set_speed_setpoint(self, speed_ms: float) -> None:
        """
        Write a new speed setpoint to the belt drive.

        Enforces a hard upper limit of :const:`_MAX_SPEED_SETPOINT_MS`.
        """
        if speed_ms > _MAX_SPEED_SETPOINT_MS:
            logger.warning(
                "Speed setpoint clamped to maximum",
                extra={"requested_ms": speed_ms, "max_ms": _MAX_SPEED_SETPOINT_MS},
            )
            speed_ms = _MAX_SPEED_SETPOINT_MS

        if speed_ms < 0.0:
            raise ValueError("Speed setpoint cannot be negative")

        node_id = self._config.SPEED_SETPOINT_NODE_ID
        confirmed = False
        try:
            await self._opcua.write_node(node_id, float(speed_ms))
            confirmed = True
            logger.info("Belt speed setpoint written", extra={"speed_ms": speed_ms})
        except Exception as exc:
            logger.error("OPC-UA speed setpoint write failed", extra={"error": str(exc)})
        finally:
            await self._publish_writeback(node_id, speed_ms, TargetProtocol.OPCUA, confirmed)

        await self._publish_alert(
            AlertSeverity.INFO,
            f"Belt speed setpoint changed to {speed_ms:.2f} m/s",
        )

    # ------------------------------------------------------------------
    # Alarm acknowledgement
    # ------------------------------------------------------------------

    async def acknowledge_alarm(self, alarm_id: str) -> None:
        """Write an alarm acknowledgement to the OPC-UA server."""
        node_id = _ALARM_ACK_NODE
        confirmed = False
        try:
            await self._opcua.write_node(node_id, alarm_id)
            confirmed = True
            logger.info("Alarm acknowledged via OPC-UA", extra={"alarm_id": alarm_id})
        except Exception as exc:
            logger.error("OPC-UA alarm ack write failed", extra={"error": str(exc), "alarm_id": alarm_id})
        finally:
            await self._publish_writeback(node_id, alarm_id, TargetProtocol.OPCUA, confirmed)

    # ------------------------------------------------------------------
    # Resume belt
    # ------------------------------------------------------------------

    async def resume_belt(self) -> None:
        """
        Clear the emergency stop and allow the belt to restart.

        Refused when maintenance mode is active.
        """
        if self._config.MAINTENANCE_MODE:
            logger.warning("Belt resume refused – maintenance mode active")
            await self._publish_alert(
                AlertSeverity.WARNING,
                "Belt resume refused: maintenance mode is active. "
                "Disable maintenance mode before resuming.",
            )
            return

        node_id = self._config.EMERGENCY_STOP_NODE_ID
        confirmed = False
        try:
            await self._opcua.write_node(node_id, False)
            confirmed = True
            logger.info("Belt emergency stop cleared – belt may resume")
        except Exception as exc:
            logger.error("OPC-UA belt resume write failed", extra={"error": str(exc)})
        finally:
            await self._publish_writeback(node_id, False, TargetProtocol.OPCUA, confirmed)

        await self._publish_alert(
            AlertSeverity.INFO,
            "Belt emergency stop cleared – belt authorised to resume",
        )

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
        writeback_topic = (
            f"fluxot/{self._config.SITE_ID}/{self._config.SKID_TYPE}"
            f"/{self._config.SKID_ID}/writeback"
        )
        await self._mqtt.publish(writeback_topic, req.model_dump(mode="json"), qos=2)

    async def _publish_alert(self, severity: AlertSeverity, message: str) -> None:
        alert = Alert(
            site_id=self._config.SITE_ID,
            skid_id=self._config.SKID_ID,
            skid_type=self._config.SKID_TYPE,
            severity=severity,
            message=message,
            tag_name="belt_control",
        )
        await self._mqtt.publish_alert(alert.model_dump(mode="json"))
