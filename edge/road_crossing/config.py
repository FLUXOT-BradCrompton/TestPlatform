"""
Road Crossing Monitor specific configuration.

Extends :class:`EdgeConfig` with all road-crossing-specific settings.
"""

from pydantic import Field
from pydantic_settings import SettingsConfigDict

from edge.common.config import EdgeConfig


class RoadCrossingConfig(EdgeConfig):
    """Configuration for the road crossing control edge service."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # Crossing identity
    CROSSING_ID: str = Field(default="CROSSING_001", description="Road crossing identifier")
    CROSSING_NAME: str = Field(default="Main Haul Road Crossing", description="Human-readable crossing name")

    # Belt parameters (read from OPC-UA)
    BELT_SPEED_MS: float = Field(default=3.5, description="Nominal belt speed m/s")
    BELT_WIDTH_MM: float = Field(default=1200.0, description="Belt width in millimetres")

    # Sensor counts
    LIDAR_COUNT: int = Field(default=2, description="Number of LIDAR units")
    RADAR_COUNT: int = Field(default=2, description="Number of radar speed sensors")
    CAMERA_COUNT: int = Field(default=4, description="Number of cameras")
    LOOP_DETECTOR_COUNT: int = Field(default=4, description="Number of inductive loop detectors")
    BARRIER_COUNT: int = Field(default=2, description="Number of road barriers")
    TRAFFIC_LIGHT_COUNT: int = Field(default=4, description="Number of traffic light heads")

    # Safety timing
    SAFE_CROSSING_WINDOW_S: float = Field(
        default=30.0,
        description="Minimum safe gap in belt (seconds) required to open barriers",
    )
    VEHICLE_CLEARANCE_TIME_S: float = Field(
        default=15.0,
        description="Time (seconds) allowed for a vehicle to clear the crossing",
    )
    VEHICLE_DETECTION_CONFIDENCE: float = Field(
        default=0.85,
        description="Minimum AI confidence required to classify a vehicle (0-1)",
    )

    # OPC-UA control nodes
    BARRIER_OPEN_NODE_ID: str = Field(
        default="ns=2;s=CrossingControl.BarrierOpen",
        description="OPC-UA node to open barriers (write True)",
    )
    BARRIER_CLOSE_NODE_ID: str = Field(
        default="ns=2;s=CrossingControl.BarrierClose",
        description="OPC-UA node to close barriers (write True)",
    )
    TRAFFIC_LIGHT_NODE_ID: str = Field(
        default="ns=2;s=CrossingControl.TrafficLight",
        description="OPC-UA node for traffic light state",
    )
    BELT_STATUS_NODE_ID: str = Field(
        default="ns=2;s=BeltStatus.Running",
        description="OPC-UA node ID for belt running status",
    )
    BELT_SPEED_NODE_ID: str = Field(
        default="ns=2;s=BeltStatus.Speed",
        description="OPC-UA node ID for belt speed",
    )
    EMERGENCY_STOP_CROSSING_NODE_ID: str = Field(
        default="ns=2;s=CrossingControl.EmergencyStop",
        description="OPC-UA node for crossing emergency stop",
    )

    # AI service
    AI_SERVICE_URL: str = Field(
        default="http://ai-service:8080/predict/road-crossing",
        description="URL of the AI inference service for vehicle classification",
    )

    # Traffic constraints
    HAUL_TRUCK_MAX_SPEED_KMH: float = Field(
        default=40.0,
        description="Maximum allowed haul truck speed at crossing (km/h)",
    )

    # Simulation
    SIMULATE: bool = Field(default=False, description="Use simulated sensor data")

    # Skid type override
    SKID_TYPE: str = Field(default="ROAD_CROSSING", description="Skid type identifier")
