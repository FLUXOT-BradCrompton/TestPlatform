"""
Pydantic v2 data models shared across all edge skid services.
These define the canonical wire format for telemetry, alerts, commands, and
writeback requests published over MQTT.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class AlertSeverity(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ALARM = "ALARM"
    CRITICAL = "CRITICAL"
    EMERGENCY = "EMERGENCY"


class CommandStatus(str, Enum):
    ACCEPTED = "ACCEPTED"
    REJECTED = "REJECTED"
    EXECUTING = "EXECUTING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TargetProtocol(str, Enum):
    OPCUA = "OPCUA"
    MODBUS = "MODBUS"
    MQTT = "MQTT"


class DataQuality(str, Enum):
    GOOD = "GOOD"
    BAD = "BAD"
    UNCERTAIN = "UNCERTAIN"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def _new_uuid() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Telemetry
# ---------------------------------------------------------------------------


class TelemetryPoint(BaseModel):
    """Single tag/value measurement."""

    timestamp: datetime = Field(default_factory=_now_utc)
    site_id: str
    skid_id: str
    skid_type: str
    tag_name: str
    value: Any
    unit: str = ""
    quality: DataQuality = DataQuality.GOOD

    model_config = {"use_enum_values": True}


class TelemetryBatch(BaseModel):
    """Collection of tag readings captured at the same poll cycle."""

    timestamp: datetime = Field(default_factory=_now_utc)
    site_id: str
    skid_id: str
    skid_type: str
    # key: tag name, value: TelemetryPoint
    data: Dict[str, TelemetryPoint] = Field(default_factory=dict)
    sequence_number: int = 0

    model_config = {"use_enum_values": True}


# ---------------------------------------------------------------------------
# Alerts
# ---------------------------------------------------------------------------


class Alert(BaseModel):
    """An alarm or notification raised by the edge service."""

    timestamp: datetime = Field(default_factory=_now_utc)
    site_id: str
    skid_id: str
    skid_type: str
    alert_id: str = Field(default_factory=_new_uuid)
    severity: AlertSeverity
    message: str
    tag_name: str = ""
    value: Optional[Any] = None
    threshold: Optional[Any] = None
    acknowledged: bool = False

    model_config = {"use_enum_values": True}


# ---------------------------------------------------------------------------
# Commands  (cloud → edge)
# ---------------------------------------------------------------------------


class Command(BaseModel):
    """A command sent from cloud/operator to the edge service."""

    timestamp: datetime = Field(default_factory=_now_utc)
    site_id: str
    skid_id: str
    command_id: str = Field(default_factory=_new_uuid)
    command_type: str
    parameters: Dict[str, Any] = Field(default_factory=dict)
    source: str = "CLOUD"
    requires_ack: bool = True

    model_config = {"use_enum_values": True}


class CommandAck(BaseModel):
    """Acknowledgement sent back to the cloud after processing a command."""

    timestamp: datetime = Field(default_factory=_now_utc)
    command_id: str
    status: CommandStatus
    message: str = ""

    model_config = {"use_enum_values": True}


# ---------------------------------------------------------------------------
# Heartbeat
# ---------------------------------------------------------------------------


class Heartbeat(BaseModel):
    """Periodic liveness signal published by each edge service."""

    timestamp: datetime = Field(default_factory=_now_utc)
    site_id: str
    skid_id: str
    skid_type: str
    status: str = "RUNNING"
    uptime_s: int = 0
    # e.g. {"mqtt": "CONNECTED", "opcua": "CONNECTED", "modbus": "DISCONNECTED"}
    connection_states: Dict[str, str] = Field(default_factory=dict)

    model_config = {"use_enum_values": True}


# ---------------------------------------------------------------------------
# Writeback
# ---------------------------------------------------------------------------


class WritebackRequest(BaseModel):
    """
    Audit record published to MQTT whenever the edge service writes back to
    a field device (OPC-UA, Modbus, etc.).
    """

    timestamp: datetime = Field(default_factory=_now_utc)
    command_id: str = Field(default_factory=_new_uuid)
    target_protocol: TargetProtocol
    target_node: str
    value: Any
    confirmed: bool = False

    model_config = {"use_enum_values": True}

    @field_validator("value", mode="before")
    @classmethod
    def _coerce_value(cls, v: Any) -> Any:
        # Allow any JSON-serialisable primitive; leave complex objects as-is
        return v
