"""
SQLAlchemy 2.0 async ORM models and Pydantic v2 API schemas for Flux OT Platform.
"""
from __future__ import annotations

import enum
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    Enum,
    Float,
    ForeignKey,
    Index,
    String,
    Text,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# SQLAlchemy enums
# ---------------------------------------------------------------------------


class SkidType(str, enum.Enum):
    BELT_RIP_MONITOR = "BELT_RIP_MONITOR"
    ROAD_CROSSING = "ROAD_CROSSING"
    GENERIC = "GENERIC"


class SkidStatus(str, enum.Enum):
    ONLINE = "ONLINE"
    OFFLINE = "OFFLINE"
    MAINTENANCE = "MAINTENANCE"
    ALARM = "ALARM"


class CommandStatus(str, enum.Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"


class AlertSeverity(str, enum.Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    ALARM = "ALARM"
    CRITICAL = "CRITICAL"


# ---------------------------------------------------------------------------
# Declarative base
# ---------------------------------------------------------------------------


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------


class Site(Base):
    __tablename__ = "sites"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)
    location: Mapped[Optional[str]] = mapped_column(String(500))
    timezone: Mapped[str] = mapped_column(String(64), default="Australia/Perth")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    skids: Mapped[List["Skid"]] = relationship(
        "Skid", back_populates="site", cascade="all, delete-orphan"
    )


class Skid(Base):
    __tablename__ = "skids"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    site_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("sites.id", ondelete="CASCADE"), nullable=False
    )
    skid_type: Mapped[SkidType] = mapped_column(
        Enum(SkidType, name="skid_type_enum"), nullable=False
    )
    skid_name: Mapped[str] = mapped_column(String(255), nullable=False)
    firmware_version: Mapped[Optional[str]] = mapped_column(String(64))
    last_seen: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    status: Mapped[SkidStatus] = mapped_column(
        Enum(SkidStatus, name="skid_status_enum"),
        default=SkidStatus.OFFLINE,
        nullable=False,
    )
    config_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    site: Mapped["Site"] = relationship("Site", back_populates="skids")
    telemetry: Mapped[List["TelemetryRecord"]] = relationship(
        "TelemetryRecord", back_populates="skid", cascade="all, delete-orphan"
    )
    alerts: Mapped[List["AlertRecord"]] = relationship(
        "AlertRecord", back_populates="skid", cascade="all, delete-orphan"
    )
    commands: Mapped[List["CommandRecord"]] = relationship(
        "CommandRecord", back_populates="skid", cascade="all, delete-orphan"
    )
    writeback_audits: Mapped[List["WritebackAudit"]] = relationship(
        "WritebackAudit", back_populates="skid", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("ix_skids_site_id", "site_id"),
        Index("ix_skids_status", "status"),
    )


class TelemetryRecord(Base):
    __tablename__ = "telemetry_records"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    skid_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("skids.id", ondelete="CASCADE"),
        nullable=False,
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    tag_name: Mapped[str] = mapped_column(String(255), nullable=False)
    value_float: Mapped[Optional[float]] = mapped_column(Float)
    value_bool: Mapped[Optional[bool]] = mapped_column(Boolean)
    value_str: Mapped[Optional[str]] = mapped_column(String(1024))
    quality: Mapped[str] = mapped_column(String(32), default="GOOD")
    unit: Mapped[Optional[str]] = mapped_column(String(32))

    skid: Mapped["Skid"] = relationship("Skid", back_populates="telemetry")

    __table_args__ = (
        # TimescaleDB hypertable hint — actual conversion in database.py
        Index("ix_telemetry_skid_ts", "skid_id", "timestamp"),
        Index("ix_telemetry_tag_ts", "tag_name", "timestamp"),
        {"timescaledb_hypertable": True},
    )


class AlertRecord(Base):
    __tablename__ = "alert_records"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    skid_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("skids.id", ondelete="CASCADE"),
        nullable=False,
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    alert_id: Mapped[str] = mapped_column(String(128), nullable=False)
    severity: Mapped[AlertSeverity] = mapped_column(
        Enum(AlertSeverity, name="alert_severity_enum"), nullable=False
    )
    message: Mapped[str] = mapped_column(Text, nullable=False)
    tag_name: Mapped[Optional[str]] = mapped_column(String(255))
    value_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    acknowledged: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    acknowledged_by: Mapped[Optional[str]] = mapped_column(String(255))
    acknowledged_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))
    resolved_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True))

    skid: Mapped["Skid"] = relationship("Skid", back_populates="alerts")

    __table_args__ = (
        Index("ix_alerts_skid_id", "skid_id"),
        Index("ix_alerts_severity", "severity"),
        Index("ix_alerts_acknowledged", "acknowledged"),
    )


class CommandRecord(Base):
    __tablename__ = "command_records"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    skid_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("skids.id", ondelete="CASCADE"),
        nullable=False,
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    command_type: Mapped[str] = mapped_column(String(128), nullable=False)
    parameters_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    source: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[CommandStatus] = mapped_column(
        Enum(CommandStatus, name="command_status_enum"),
        default=CommandStatus.PENDING,
        nullable=False,
    )
    result_json: Mapped[Optional[Dict[str, Any]]] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    skid: Mapped["Skid"] = relationship("Skid", back_populates="commands")
    writeback_audit: Mapped[Optional["WritebackAudit"]] = relationship(
        "WritebackAudit", back_populates="command", uselist=False
    )

    __table_args__ = (
        Index("ix_commands_skid_id", "skid_id"),
        Index("ix_commands_status", "status"),
    )


class WritebackAudit(Base):
    __tablename__ = "writeback_audit"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    skid_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("skids.id", ondelete="CASCADE"),
        nullable=False,
    )
    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, index=True
    )
    command_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("command_records.id", ondelete="SET NULL"),
    )
    target_protocol: Mapped[str] = mapped_column(String(64), nullable=False)
    target_node: Mapped[str] = mapped_column(String(255), nullable=False)
    value_json: Mapped[Dict[str, Any]] = mapped_column(JSONB, nullable=False)
    confirmed: Mapped[bool] = mapped_column(Boolean, default=False)
    operator: Mapped[Optional[str]] = mapped_column(String(255))

    skid: Mapped["Skid"] = relationship("Skid", back_populates="writeback_audits")
    command: Mapped[Optional["CommandRecord"]] = relationship(
        "CommandRecord", back_populates="writeback_audit"
    )

    __table_args__ = (Index("ix_writeback_skid_id", "skid_id"),)


# ---------------------------------------------------------------------------
# Pydantic v2 schemas
# ---------------------------------------------------------------------------


class _BaseSchema(BaseModel):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)


# -- Site schemas ------------------------------------------------------------


class SiteCreate(_BaseSchema):
    name: str = Field(..., max_length=255)
    location: Optional[str] = Field(None, max_length=500)
    timezone: str = Field("Australia/Perth", max_length=64)


class SiteResponse(_BaseSchema):
    id: uuid.UUID
    name: str
    location: Optional[str]
    timezone: str
    created_at: datetime
    updated_at: datetime


# -- Skid schemas ------------------------------------------------------------


class SkidCreate(_BaseSchema):
    skid_type: SkidType
    skid_name: str = Field(..., max_length=255)
    firmware_version: Optional[str] = Field(None, max_length=64)
    config_json: Optional[Dict[str, Any]] = None


class SkidResponse(_BaseSchema):
    id: uuid.UUID
    site_id: uuid.UUID
    skid_type: SkidType
    skid_name: str
    firmware_version: Optional[str]
    last_seen: Optional[datetime]
    status: SkidStatus
    config_json: Optional[Dict[str, Any]]
    created_at: datetime


class SkidStatusUpdate(_BaseSchema):
    status: SkidStatus


# -- Telemetry schemas -------------------------------------------------------


class TelemetryPointResponse(_BaseSchema):
    id: int
    skid_id: uuid.UUID
    timestamp: datetime
    tag_name: str
    value_float: Optional[float]
    value_bool: Optional[bool]
    value_str: Optional[str]
    quality: str
    unit: Optional[str]


class TelemetryResponse(_BaseSchema):
    items: List[TelemetryPointResponse]
    total: int
    limit: int
    offset: int


# -- Alert schemas -----------------------------------------------------------


class AlertResponse(_BaseSchema):
    id: uuid.UUID
    skid_id: uuid.UUID
    timestamp: datetime
    alert_id: str
    severity: AlertSeverity
    message: str
    tag_name: Optional[str]
    value_json: Optional[Dict[str, Any]]
    acknowledged: bool
    acknowledged_by: Optional[str]
    acknowledged_at: Optional[datetime]
    resolved_at: Optional[datetime]


class AlertAcknowledge(_BaseSchema):
    operator: str = Field(..., max_length=255)
    note: Optional[str] = Field(None, max_length=1024)


# -- Command schemas ---------------------------------------------------------


class CommandCreate(_BaseSchema):
    command_type: str = Field(..., max_length=128)
    parameters_json: Optional[Dict[str, Any]] = None
    source: str = Field(..., max_length=255, description="Operator or system identifier")


class CommandResponse(_BaseSchema):
    id: uuid.UUID
    skid_id: uuid.UUID
    timestamp: datetime
    command_type: str
    parameters_json: Optional[Dict[str, Any]]
    source: str
    status: CommandStatus
    result_json: Optional[Dict[str, Any]]
    created_at: datetime


# -- Writeback schemas -------------------------------------------------------


class WritebackCreate(_BaseSchema):
    command_type: str = Field(..., max_length=128)
    target_protocol: str = Field(..., max_length=64, description="e.g. MODBUS, OPCUA, MQTT")
    target_node: str = Field(..., max_length=255, description="Node address or register")
    value_json: Dict[str, Any] = Field(..., description="Value(s) to write")
    operator: Optional[str] = Field(None, max_length=255)


class WritebackResponse(_BaseSchema):
    id: uuid.UUID
    skid_id: uuid.UUID
    timestamp: datetime
    command_id: Optional[uuid.UUID]
    target_protocol: str
    target_node: str
    value_json: Dict[str, Any]
    confirmed: bool
    operator: Optional[str]


# -- Dashboard schemas -------------------------------------------------------


class SkidHealthSummary(_BaseSchema):
    skid_id: uuid.UUID
    skid_name: str
    skid_type: SkidType
    status: SkidStatus
    last_seen: Optional[datetime]
    active_alarm_count: int


class DashboardSummary(_BaseSchema):
    site_id: uuid.UUID
    site_name: str
    total_skids: int
    skids_online: int
    skids_offline: int
    skids_in_alarm: int
    skids_in_maintenance: int
    active_alarms_total: int
    critical_alarms: int
    warning_alarms: int
    skid_summaries: List[SkidHealthSummary]
    generated_at: datetime
