"""
Road Crossing edge service – main service class.

Wires together:
  - CrossingSensorReader    – OPC-UA / simulated sensor reading
  - CrossingDecisionEngine  – autonomous safety FSM
  - CrossingController      – OPC-UA barrier / light writeback
  - BaseSkidService         – polling, heartbeat, command handling, MQTT

Entry point: ``main()``
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from edge.common.base_skid import BaseSkidService
from edge.common.data_models import (
    Alert,
    AlertSeverity,
    Command,
    CommandStatus,
    DataQuality,
    TelemetryBatch,
    TelemetryPoint,
)
from edge.common.logging_config import get_logger

from .config import RoadCrossingConfig
from .control import CrossingController
from .decision_engine import CrossingAction, CrossingDecision, CrossingDecisionEngine, CrossingState
from .sensors import CrossingSensorData, CrossingSensorReader

logger = get_logger(__name__)

# Operational modes
_MODE_AUTO = "AUTO"
_MODE_MANUAL = "MANUAL"
_MODE_MAINTENANCE = "MAINTENANCE"


class RoadCrossingService(BaseSkidService):
    """
    Full road crossing control edge service.

    Inherits infrastructure from :class:`BaseSkidService` and implements the
    crossing-specific polling, decision execution, and command handling.
    """

    def __init__(self, config: RoadCrossingConfig) -> None:
        super().__init__(config)
        self._crossing_config = config

        self._sensor_reader = CrossingSensorReader(config, self._opcua)
        self._decision_engine = CrossingDecisionEngine(config)
        self._controller: Optional[CrossingController] = None  # set in start()

        self._last_decision: Optional[CrossingDecision] = None
        self._last_sensor_data: Optional[CrossingSensorData] = None
        self._mode: str = _MODE_AUTO
        self._last_state: Optional[CrossingState] = None

        # Crossing event tracking
        self._crossing_start_time: Optional[datetime] = None
        self._crossings_total: int = 0

    # ------------------------------------------------------------------
    # Lifecycle overrides
    # ------------------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._controller = CrossingController(
            self._crossing_config,
            self._opcua,
            self._mqtt,
        )
        logger.info(
            "RoadCrossingService started",
            extra={"crossing_id": self._crossing_config.CROSSING_ID},
        )

    # ------------------------------------------------------------------
    # Poll implementation
    # ------------------------------------------------------------------

    async def poll(self) -> None:
        """One complete sensor-read → decision → execute → publish cycle."""
        cfg = self._crossing_config

        # 1. Read sensors
        sensor_data = await self._sensor_reader.read_all()
        self._last_sensor_data = sensor_data

        # 2. Evaluate decision (only in AUTO mode)
        if self._mode == _MODE_AUTO:
            decision = self._decision_engine.evaluate(sensor_data)
        else:
            # In MANUAL/MAINTENANCE mode – still evaluate for telemetry but do not act
            decision = self._decision_engine.evaluate(sensor_data)
            # Override action to NONE
            from dataclasses import replace
            decision = CrossingDecision(
                timestamp=decision.timestamp,
                state=decision.state,
                action=CrossingAction.NONE,
                reason=f"Action suppressed: mode={self._mode}",
                estimated_wait_s=decision.estimated_wait_s,
                safe_to_cross=decision.safe_to_cross,
                next_state_transition_s=decision.next_state_transition_s,
            )

        self._last_decision = decision

        # 3. Execute action
        if self._mode == _MODE_AUTO and self._controller:
            await self._execute_action(decision, sensor_data)

        # 4. Build and publish telemetry batch
        batch = self._build_telemetry_batch(sensor_data, decision)
        await self._mqtt.publish_telemetry(batch.model_dump(mode="json"))

        # 5. State-change alerts
        if self._last_state != decision.state:
            await self._publish_state_change_alert(self._last_state, decision.state, decision.reason)
            self._last_state = decision.state

        # 6. Track crossing events
        await self._track_crossing_event(decision)

        # 7. Sync simulation state if applicable
        if cfg.SIMULATE and self._controller:
            self._sensor_reader.update_sim_state(
                self._controller.barrier_states,
                self._controller.light_states,
            )

    # ------------------------------------------------------------------
    # Action execution
    # ------------------------------------------------------------------

    async def _execute_action(
        self,
        decision: CrossingDecision,
        sensor_data: CrossingSensorData,
    ) -> None:
        action = decision.action
        if action == CrossingAction.OPEN_BARRIER:
            await self._controller.open_barriers(decision.reason)

        elif action == CrossingAction.CLOSE_BARRIER:
            await self._controller.close_barriers(decision.reason)

        elif action == CrossingAction.SET_TRAFFIC_LIGHT:
            # Green for approaching direction, red for belt-side
            state = "GREEN" if decision.safe_to_cross else "RED"
            for zone in range(self._crossing_config.TRAFFIC_LIGHT_COUNT):
                await self._controller.set_traffic_light(zone, state)

        elif action == CrossingAction.EMERGENCY_STOP:
            await self._controller.trigger_emergency_stop(decision.reason)

        elif action == CrossingAction.WAIT:
            pass  # No physical action; just waiting

        elif action == CrossingAction.NONE:
            pass  # Intentional no-op

    # ------------------------------------------------------------------
    # Command handler
    # ------------------------------------------------------------------

    async def handle_command(self, command: Command) -> None:
        """Dispatch inbound operator / cloud commands."""
        cmd_type = command.command_type.upper()
        params = command.parameters

        try:
            if cmd_type == "MANUAL_OPEN":
                if self._mode not in (_MODE_AUTO, _MODE_MANUAL):
                    await self._ack_command(
                        command.command_id,
                        CommandStatus.REJECTED,
                        f"Cannot open barriers in {self._mode} mode",
                    )
                    return
                reason = params.get("reason", "Operator manual open")
                await self._controller.open_barriers(reason)
                await self._ack_command(command.command_id, CommandStatus.COMPLETED, "Barriers opened")

            elif cmd_type == "MANUAL_CLOSE":
                reason = params.get("reason", "Operator manual close")
                await self._controller.close_barriers(reason)
                await self._ack_command(command.command_id, CommandStatus.COMPLETED, "Barriers closed")

            elif cmd_type == "EMERGENCY_STOP":
                reason = params.get("reason", "Operator emergency stop")
                await self._controller.trigger_emergency_stop(reason)
                self._decision_engine.force_emergency(reason)
                await self._ack_command(command.command_id, CommandStatus.COMPLETED, "Emergency stop activated")

            elif cmd_type == "RESET_EMERGENCY":
                await self._controller.reset_emergency()
                self._decision_engine.reset_emergency()
                await self._ack_command(command.command_id, CommandStatus.COMPLETED, "Emergency reset")

            elif cmd_type == "REQUEST_STATUS":
                await self._publish_status()
                await self._ack_command(command.command_id, CommandStatus.COMPLETED, "Status published")

            elif cmd_type == "SET_MODE":
                new_mode = params.get("mode", "AUTO").upper()
                if new_mode not in (_MODE_AUTO, _MODE_MANUAL, _MODE_MAINTENANCE):
                    await self._ack_command(
                        command.command_id,
                        CommandStatus.REJECTED,
                        f"Unknown mode: {new_mode}",
                    )
                    return
                old_mode = self._mode
                self._mode = new_mode
                logger.info("Crossing mode changed", extra={"old": old_mode, "new": new_mode})
                await self._ack_command(
                    command.command_id,
                    CommandStatus.COMPLETED,
                    f"Mode changed from {old_mode} to {new_mode}",
                )

            else:
                logger.warning("Unknown crossing command", extra={"command_type": cmd_type})
                await self._ack_command(
                    command.command_id,
                    CommandStatus.REJECTED,
                    f"Unknown command type: {cmd_type}",
                )

        except Exception as exc:
            logger.error("Crossing command failed", extra={"command_type": cmd_type, "error": str(exc)})
            await self._ack_command(command.command_id, CommandStatus.FAILED, str(exc))

    # ------------------------------------------------------------------
    # Telemetry builder
    # ------------------------------------------------------------------

    def _build_telemetry_batch(
        self,
        sensor_data: CrossingSensorData,
        decision: CrossingDecision,
    ) -> TelemetryBatch:
        cfg = self._crossing_config
        ts = sensor_data.timestamp

        def _pt(tag: str, value: object, unit: str = "") -> TelemetryPoint:
            return TelemetryPoint(
                timestamp=ts,
                site_id=cfg.SITE_ID,
                skid_id=cfg.SKID_ID,
                skid_type=cfg.SKID_TYPE,
                tag_name=tag,
                value=value,
                unit=unit,
                quality=DataQuality.GOOD,
            )

        data = {}

        # Loop detectors
        for i, v in enumerate(sensor_data.loop_detectors):
            data[f"loop_detector_{i+1}"] = _pt(f"loop_detector_{i+1}", v)

        # Vehicle sensing
        data["vehicle_speed_kmh"] = _pt("vehicle_speed_kmh", sensor_data.vehicle_speed_kmh, "km/h")
        data["vehicle_class"] = _pt("vehicle_class", sensor_data.vehicle_class)
        data["waiting_vehicles"] = _pt("waiting_vehicles", sensor_data.waiting_vehicles_count)
        data["crossing_clear"] = _pt("crossing_clear", sensor_data.crossing_clear)

        # Lidar
        for i, v in enumerate(sensor_data.lidar_readings):
            data[f"lidar_{i+1}_distance_m"] = _pt(f"lidar_{i+1}_distance_m", v, "m")

        # Belt
        data["belt_speed_ms"] = _pt("belt_speed_ms", sensor_data.belt_speed_ms, "m/s")
        data["belt_running"] = _pt("belt_running", sensor_data.belt_running)

        # Barriers and lights
        for i, v in enumerate(sensor_data.barrier_states):
            data[f"barrier_{i+1}_open"] = _pt(f"barrier_{i+1}_open", v)
        for i, v in enumerate(sensor_data.traffic_light_states):
            data[f"traffic_light_{i+1}"] = _pt(f"traffic_light_{i+1}", v)

        # Decision
        data["fsm_state"] = _pt("fsm_state", decision.state)
        data["fsm_action"] = _pt("fsm_action", decision.action)
        data["safe_to_cross"] = _pt("safe_to_cross", decision.safe_to_cross)
        data["estimated_wait_s"] = _pt("estimated_wait_s", decision.estimated_wait_s, "s")
        data["crossing_mode"] = _pt("crossing_mode", self._mode)
        data["crossings_total"] = _pt("crossings_total", self._crossings_total)

        return TelemetryBatch(
            timestamp=ts,
            site_id=cfg.SITE_ID,
            skid_id=cfg.SKID_ID,
            skid_type=cfg.SKID_TYPE,
            data=data,
            sequence_number=self._next_sequence(),
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _track_crossing_event(self, decision: CrossingDecision) -> None:
        """Track start/end of crossing events for statistics."""
        if (
            decision.state == CrossingState.CROSSING_IN_PROGRESS
            and self._crossing_start_time is None
        ):
            self._crossing_start_time = datetime.now(tz=timezone.utc)
            logger.info("Crossing event started")

        elif (
            decision.state == CrossingState.CROSSING_COMPLETE
            and self._crossing_start_time is not None
        ):
            duration = (
                datetime.now(tz=timezone.utc) - self._crossing_start_time
            ).total_seconds()
            self._crossings_total += 1
            self._crossing_start_time = None
            logger.info(
                "Crossing event completed",
                extra={
                    "duration_s": round(duration, 1),
                    "crossings_total": self._crossings_total,
                },
            )

    async def _publish_state_change_alert(
        self,
        old_state: Optional[CrossingState],
        new_state: CrossingState,
        reason: str,
    ) -> None:
        cfg = self._crossing_config
        severity = AlertSeverity.INFO

        if new_state == CrossingState.EMERGENCY:
            severity = AlertSeverity.EMERGENCY
        elif new_state == CrossingState.CROSSING_IN_PROGRESS:
            severity = AlertSeverity.INFO
        elif old_state == CrossingState.EMERGENCY:
            severity = AlertSeverity.WARNING

        alert = Alert(
            site_id=cfg.SITE_ID,
            skid_id=cfg.SKID_ID,
            skid_type=cfg.SKID_TYPE,
            severity=severity,
            message=f"Crossing state: {old_state} -> {new_state}: {reason}",
            tag_name="fsm_state",
            value=str(new_state),
        )
        await self._mqtt.publish_alert(alert.model_dump(mode="json"))

    async def _publish_status(self) -> None:
        cfg = self._crossing_config
        status_topic = f"fluxot/{cfg.SITE_ID}/{cfg.SKID_TYPE}/{cfg.SKID_ID}/status"
        payload: dict = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "crossing_id": cfg.CROSSING_ID,
            "crossing_name": cfg.CROSSING_NAME,
            "site_id": cfg.SITE_ID,
            "skid_id": cfg.SKID_ID,
            "uptime_s": self.uptime_s,
            "mode": self._mode,
            "crossings_total": self._crossings_total,
            "mqtt_connected": self._mqtt.is_connected,
            "opcua_connected": self._opcua.is_connected if self._opcua else False,
            "mqtt_buffer_depth": self._mqtt.buffer_depth,
        }
        if self._last_decision:
            payload["last_decision"] = {
                "state": str(self._last_decision.state),
                "action": str(self._last_decision.action),
                "safe_to_cross": self._last_decision.safe_to_cross,
                "reason": self._last_decision.reason,
            }
        await self._mqtt.publish(status_topic, payload, qos=1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Start the Road Crossing service."""
    config = RoadCrossingConfig()
    service = RoadCrossingService(config)
    service.run()


if __name__ == "__main__":
    main()
