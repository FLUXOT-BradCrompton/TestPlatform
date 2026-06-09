"""
Belt Rip Monitor edge service – main service class.

Wires together:
  - BeltSensorReader   – OPC-UA / simulated sensor reading
  - BeltRipDetector    – local heuristic anomaly detection
  - AIInferenceClient  – remote AI service (with local fallback)
  - BeltWritebackController – OPC-UA writeback for stop / speed commands
  - BaseSkidService    – polling, heartbeat, command handling, MQTT

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

from .ai_client import AIInferenceClient
from .config import BeltRipConfig
from .detector import BeltRipDetector, DetectionResult
from .sensors import BeltSensorData, BeltSensorReader
from .writeback import BeltWritebackController

logger = get_logger(__name__)


class BeltRipMonitorService(BaseSkidService):
    """
    Full belt rip monitoring edge service.

    Inherits the poll / heartbeat / command infrastructure from
    :class:`BaseSkidService` and implements the belt-specific logic.
    """

    def __init__(self, config: BeltRipConfig) -> None:
        super().__init__(config)
        self._belt_config = config

        self._sensor_reader = BeltSensorReader(config, self._opcua)
        self._detector = BeltRipDetector(config)
        self._ai_client = AIInferenceClient(config)
        self._writeback: Optional[BeltWritebackController] = None  # set in start()

        self._last_detection: Optional[DetectionResult] = None
        self._last_sensor_data: Optional[BeltSensorData] = None
        self._emergency_active: bool = False

    # ------------------------------------------------------------------
    # Lifecycle overrides
    # ------------------------------------------------------------------

    async def start(self) -> None:
        await super().start()
        self._writeback = BeltWritebackController(
            self._belt_config,
            self._opcua,
            self._mqtt,
        )
        logger.info("BeltRipMonitorService started", extra={"belt_id": self._belt_config.BELT_ID})

    async def stop(self) -> None:
        await self._ai_client.close()
        await super().stop()

    # ------------------------------------------------------------------
    # Poll implementation
    # ------------------------------------------------------------------

    async def poll(self) -> None:
        """One complete sensor-read → detect → publish cycle."""
        cfg = self._belt_config

        # 1. Read sensors
        sensor_data = await self._sensor_reader.read_all()
        self._last_sensor_data = sensor_data

        # 2. Local detection (always runs)
        local_result = self._detector.detect(sensor_data)
        self._detector.update_baseline(sensor_data)

        # 3. AI inference (if enabled; falls back to local)
        if cfg.AI_ENABLED:
            detection = await self._ai_client.predict(sensor_data)
            if detection is None:
                detection = local_result
        else:
            detection = local_result

        self._last_detection = detection

        # 4. Build telemetry batch
        batch = self._build_telemetry_batch(sensor_data, detection)

        # 5. Publish telemetry
        await self._mqtt.publish_telemetry(batch.model_dump(mode="json"))

        # 6. Check for alert / action conditions
        await self._evaluate_alerts(sensor_data, detection)

        # 7. Automated writeback if critical
        if cfg.EMERGENCY_STOP_ENABLED and not cfg.MAINTENANCE_MODE and not self._emergency_active:
            if detection.recommendation == "STOP":
                logger.critical(
                    "Auto-triggering emergency stop",
                    extra={
                        "composite_score": round(detection.composite_score, 3),
                        "confidence": round(detection.confidence, 3),
                    },
                )
                self._emergency_active = True
                await self._writeback.execute_emergency_stop(
                    f"Rip detection score={detection.composite_score:.3f} "
                    f"confidence={detection.confidence:.3f}"
                )

    # ------------------------------------------------------------------
    # Command handler
    # ------------------------------------------------------------------

    async def handle_command(self, command: Command) -> None:
        """Dispatch inbound operator / cloud commands."""
        cmd_type = command.command_type.upper()
        params = command.parameters

        try:
            if cmd_type == "EMERGENCY_STOP":
                reason = params.get("reason", "Operator commanded emergency stop")
                await self._writeback.execute_emergency_stop(reason)
                self._emergency_active = True
                await self._ack_command(command.command_id, CommandStatus.COMPLETED, "Emergency stop executed")

            elif cmd_type == "SET_SPEED":
                speed = float(params.get("speed_ms", self._belt_config.BELT_SPEED_MS))
                await self._writeback.set_speed_setpoint(speed)
                await self._ack_command(command.command_id, CommandStatus.COMPLETED, f"Speed set to {speed} m/s")

            elif cmd_type == "RESUME":
                await self._writeback.resume_belt()
                self._emergency_active = False
                await self._ack_command(command.command_id, CommandStatus.COMPLETED, "Belt resume executed")

            elif cmd_type == "ACK_ALARM":
                alarm_id = params.get("alarm_id", "")
                if not alarm_id:
                    await self._ack_command(command.command_id, CommandStatus.REJECTED, "alarm_id required")
                    return
                await self._writeback.acknowledge_alarm(alarm_id)
                await self._ack_command(command.command_id, CommandStatus.COMPLETED, f"Alarm {alarm_id} acknowledged")

            elif cmd_type == "MAINTENANCE_MODE":
                enabled = bool(params.get("enabled", True))
                self._belt_config.MAINTENANCE_MODE = enabled
                mode_str = "ENABLED" if enabled else "DISABLED"
                logger.info("Maintenance mode changed", extra={"enabled": enabled})
                await self._ack_command(
                    command.command_id,
                    CommandStatus.COMPLETED,
                    f"Maintenance mode {mode_str}",
                )

            elif cmd_type == "REQUEST_STATUS":
                await self._publish_status()
                await self._ack_command(command.command_id, CommandStatus.COMPLETED, "Status published")

            else:
                logger.warning("Unknown command type", extra={"command_type": cmd_type})
                await self._ack_command(
                    command.command_id,
                    CommandStatus.REJECTED,
                    f"Unknown command type: {cmd_type}",
                )

        except Exception as exc:
            logger.error("Command execution failed", extra={"command_type": cmd_type, "error": str(exc)})
            await self._ack_command(command.command_id, CommandStatus.FAILED, str(exc))

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _build_telemetry_batch(
        self,
        sensor_data: BeltSensorData,
        detection: DetectionResult,
    ) -> TelemetryBatch:
        cfg = self._belt_config
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

        # Magnetic sensors
        for i, v in enumerate(sensor_data.magnetic_sensors):
            data[f"magnetic_sensor_{i+1}"] = _pt(f"magnetic_sensor_{i+1}", v, "mT")

        # Acoustic sensors
        for i, v in enumerate(sensor_data.acoustic_sensors):
            data[f"acoustic_sensor_{i+1}"] = _pt(f"acoustic_sensor_{i+1}", v, "dB")

        # Load cells
        for i, v in enumerate(sensor_data.load_cells):
            data[f"load_cell_{i+1}"] = _pt(f"load_cell_{i+1}", v, "kN")

        # Belt kinematics
        data["belt_speed"] = _pt("belt_speed", sensor_data.belt_speed, "m/s")
        data["belt_tension"] = _pt("belt_tension", sensor_data.belt_tension, "kN")
        data["motor_current"] = _pt("motor_current", sensor_data.motor_current, "A")
        data["belt_temperature"] = _pt("belt_temperature", sensor_data.belt_temperature, "°C")
        data["belt_running"] = _pt("belt_running", sensor_data.running, "")

        # Detection results
        data["rip_composite_score"] = _pt("rip_composite_score", round(detection.composite_score, 4), "")
        data["rip_confidence"] = _pt("rip_confidence", round(detection.confidence, 4), "")
        data["rip_recommendation"] = _pt("rip_recommendation", detection.recommendation, "")
        data["rip_magnetic_anomaly"] = _pt("rip_magnetic_anomaly", detection.magnetic_anomaly, "")
        data["rip_acoustic_anomaly"] = _pt("rip_acoustic_anomaly", detection.acoustic_anomaly, "")
        data["rip_load_anomaly"] = _pt("rip_load_anomaly", detection.load_anomaly, "")
        data["rip_speed_anomaly"] = _pt("rip_speed_anomaly", detection.speed_anomaly, "")
        data["emergency_active"] = _pt("emergency_active", self._emergency_active, "")

        return TelemetryBatch(
            timestamp=ts,
            site_id=cfg.SITE_ID,
            skid_id=cfg.SKID_ID,
            skid_type=cfg.SKID_TYPE,
            data=data,
            sequence_number=self._next_sequence(),
        )

    async def _evaluate_alerts(
        self,
        sensor_data: BeltSensorData,
        detection: DetectionResult,
    ) -> None:
        cfg = self._belt_config

        severity: Optional[AlertSeverity] = None
        message: Optional[str] = None

        if detection.recommendation == "STOP":
            severity = AlertSeverity.EMERGENCY
            message = (
                f"BELT RIP DETECTED – score={detection.composite_score:.3f} "
                f"confidence={detection.confidence:.3f}. EMERGENCY STOP REQUIRED."
            )
        elif detection.recommendation == "SLOW_DOWN":
            severity = AlertSeverity.CRITICAL
            message = (
                f"Belt rip probable – score={detection.composite_score:.3f}. "
                "Reduce speed immediately."
            )
        elif detection.recommendation == "MONITOR":
            severity = AlertSeverity.WARNING
            message = f"Belt rip anomaly detected – score={detection.composite_score:.3f}. Monitor closely."

        # Belt temperature alarm
        if sensor_data.belt_temperature > 80.0:
            await self._mqtt.publish_alert(
                Alert(
                    site_id=cfg.SITE_ID,
                    skid_id=cfg.SKID_ID,
                    skid_type=cfg.SKID_TYPE,
                    severity=AlertSeverity.ALARM,
                    message=f"High belt temperature: {sensor_data.belt_temperature:.1f}°C",
                    tag_name="belt_temperature",
                    value=sensor_data.belt_temperature,
                    threshold=80.0,
                ).model_dump(mode="json")
            )

        if severity and message:
            await self._mqtt.publish_alert(
                Alert(
                    site_id=cfg.SITE_ID,
                    skid_id=cfg.SKID_ID,
                    skid_type=cfg.SKID_TYPE,
                    severity=severity,
                    message=message,
                    tag_name="rip_composite_score",
                    value=detection.composite_score,
                    threshold=cfg.RIP_DETECTION_THRESHOLD,
                ).model_dump(mode="json")
            )

    async def _publish_status(self) -> None:
        """Publish a detailed status snapshot to the status topic."""
        cfg = self._belt_config
        status_topic = (
            f"fluxot/{cfg.SITE_ID}/{cfg.SKID_TYPE}/{cfg.SKID_ID}/status"
        )
        payload: dict = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "belt_id": cfg.BELT_ID,
            "site_id": cfg.SITE_ID,
            "skid_id": cfg.SKID_ID,
            "uptime_s": self.uptime_s,
            "emergency_active": self._emergency_active,
            "maintenance_mode": cfg.MAINTENANCE_MODE,
            "mqtt_connected": self._mqtt.is_connected,
            "opcua_connected": self._opcua.is_connected if self._opcua else False,
            "mqtt_buffer_depth": self._mqtt.buffer_depth,
        }
        if self._last_detection:
            payload["last_detection"] = {
                "composite_score": round(self._last_detection.composite_score, 4),
                "recommendation": self._last_detection.recommendation,
                "confidence": round(self._last_detection.confidence, 4),
            }
        await self._mqtt.publish(status_topic, payload, qos=1)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Start the Belt Rip Monitor service."""
    config = BeltRipConfig()
    service = BeltRipMonitorService(config)
    service.run()


if __name__ == "__main__":
    main()
