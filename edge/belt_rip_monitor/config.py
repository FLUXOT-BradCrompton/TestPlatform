"""
Belt Rip Monitor specific configuration.

Extends :class:`EdgeConfig` with all belt-rip-specific settings.
"""

from typing import List, Optional
import json
from pydantic import Field, field_validator
from pydantic_settings import SettingsConfigDict

from edge.common.config import EdgeConfig


class BeltRipConfig(EdgeConfig):
    """Configuration for the belt rip detection edge service."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Belt identity
    BELT_ID: str = Field(default="BELT_001", description="Belt conveyor identifier")

    # Belt geometry / kinematics
    BELT_WIDTH_MM: float = Field(default=1200.0, description="Belt width in millimetres")
    BELT_SPEED_MS: float = Field(default=3.5, description="Nominal belt speed in m/s")
    BELT_LENGTH_M: float = Field(default=500.0, description="Belt length in metres")

    # Sensor counts
    SENSOR_COUNT: int = Field(default=8, description="Number of magnetic/acoustic sensors")
    ACOUSTIC_SENSOR_COUNT: int = Field(default=4, description="Number of acoustic sensors")
    LOAD_CELL_COUNT: int = Field(default=2, description="Number of load cells")

    # Optional vision integration
    VISION_ENABLED: bool = Field(default=False, description="Enable vision-based rip detection")

    # Detection thresholds
    RIP_DETECTION_THRESHOLD: float = Field(
        default=0.75,
        description="Composite confidence threshold (0-1) above which a rip is declared",
    )

    # Safety actions
    EMERGENCY_STOP_ENABLED: bool = Field(
        default=True,
        description="Automatically trigger emergency stop when rip detected",
    )

    # OPC-UA node IDs for belt control
    EMERGENCY_STOP_NODE_ID: str = Field(
        default="ns=2;s=BeltControl.EmergencyStop",
        description="OPC-UA node ID for emergency stop coil",
    )
    SPEED_SETPOINT_NODE_ID: str = Field(
        default="ns=2;s=BeltControl.SpeedSetpoint",
        description="OPC-UA node ID for speed setpoint",
    )
    STATUS_NODE_ID: str = Field(
        default="ns=2;s=BeltStatus.Running",
        description="OPC-UA node ID for running status",
    )

    # AI service
    AI_SERVICE_URL: str = Field(
        default="http://ai-service:8080/predict/belt-rip",
        description="URL of the AI inference service",
    )
    AI_ENABLED: bool = Field(default=True, description="Enable AI service inference")

    # Optional alert delivery
    ALARM_EMAIL_ENABLED: bool = Field(default=False, description="Send email on ALARM/CRITICAL alerts")

    # Operational modes
    MAINTENANCE_MODE: bool = Field(
        default=False,
        description="Suppress automated actions when in maintenance mode",
    )

    # OPC-UA namespace for sensor nodes
    OPCUA_SENSOR_NAMESPACE: int = Field(default=2, description="OPC-UA namespace index for sensor nodes")

    # JSON-encoded list of OPC-UA node IDs for all sensors
    # Example: '["ns=2;s=Sensor.Mag1", "ns=2;s=Sensor.Mag2", ...]'
    NODE_IDS: str = Field(
        default=(
            '["ns=2;s=Sensor.Mag1","ns=2;s=Sensor.Mag2","ns=2;s=Sensor.Mag3",'
            '"ns=2;s=Sensor.Mag4","ns=2;s=Sensor.Mag5","ns=2;s=Sensor.Mag6",'
            '"ns=2;s=Sensor.Mag7","ns=2;s=Sensor.Mag8",'
            '"ns=2;s=Sensor.Acoustic1","ns=2;s=Sensor.Acoustic2",'
            '"ns=2;s=Sensor.Acoustic3","ns=2;s=Sensor.Acoustic4",'
            '"ns=2;s=Sensor.LoadCell1","ns=2;s=Sensor.LoadCell2",'
            '"ns=2;s=Sensor.BeltSpeed","ns=2;s=Sensor.BeltTension",'
            '"ns=2;s=Sensor.MotorCurrent","ns=2;s=Sensor.BeltTemperature",'
            '"ns=2;s=BeltStatus.Running"]'
        ),
        description="JSON array of OPC-UA node IDs to read each poll cycle",
    )

    # Simulation mode (set SIMULATE=true to generate synthetic data)
    SIMULATE: bool = Field(default=False, description="Use simulated sensor data")

    # Skid type override
    SKID_TYPE: str = Field(default="BELT_RIP", description="Skid type identifier")

    @field_validator("NODE_IDS", mode="before")
    @classmethod
    def _validate_node_ids(cls, v: str) -> str:
        try:
            parsed = json.loads(v)
            if not isinstance(parsed, list):
                raise ValueError("NODE_IDS must be a JSON array")
        except json.JSONDecodeError as exc:
            raise ValueError(f"NODE_IDS is not valid JSON: {exc}") from exc
        return v

    def get_node_ids(self) -> List[str]:
        """Return the NODE_IDS as a Python list."""
        return json.loads(self.NODE_IDS)
