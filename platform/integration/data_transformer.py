"""
Data transformation and enrichment for inbound telemetry and alert messages.

Responsibilities:
  - Normalise engineering units to SI
  - Validate required fields via Pydantic
  - Enrich payloads with site/skid metadata
  - Parse MQTT topic strings into structured objects
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Pydantic message schemas (inbound from edge)
# ---------------------------------------------------------------------------


class RawTelemetryMessage(BaseModel):
    """Schema for a single tag value arriving from an edge skid."""

    skid_id: str
    site_id: Optional[str] = None
    skid_type: Optional[str] = None
    timestamp: str = Field(..., description="ISO-8601 or Unix epoch (int/float)")
    tag_name: str
    value_float: Optional[float] = None
    value_bool: Optional[bool] = None
    value_str: Optional[str] = None
    quality: str = "GOOD"
    unit: Optional[str] = None
    firmware_version: Optional[str] = None

    @field_validator("timestamp")
    @classmethod
    def parse_timestamp(cls, v: Any) -> str:
        """Normalise timestamp to UTC ISO-8601 string."""
        if isinstance(v, (int, float)):
            dt = datetime.fromtimestamp(float(v), tz=timezone.utc)
            return dt.isoformat()
        # Already a string — try parsing to validate
        try:
            datetime.fromisoformat(str(v))
        except ValueError:
            raise ValueError(f"Cannot parse timestamp: {v!r}")
        return str(v)

    @model_validator(mode="after")
    def at_least_one_value(self) -> "RawTelemetryMessage":
        if self.value_float is None and self.value_bool is None and self.value_str is None:
            raise ValueError(
                "At least one of value_float, value_bool, or value_str must be provided"
            )
        return self


class RawAlertMessage(BaseModel):
    """Schema for an alert arriving from an edge skid."""

    skid_id: str
    site_id: Optional[str] = None
    skid_type: Optional[str] = None
    timestamp: str
    alert_id: str
    severity: str = "ALARM"
    message: str
    tag_name: Optional[str] = None
    value_json: Optional[Dict[str, Any]] = None

    @field_validator("severity")
    @classmethod
    def validate_severity(cls, v: str) -> str:
        allowed = {"INFO", "WARNING", "ALARM", "CRITICAL"}
        upper = v.upper()
        if upper not in allowed:
            raise ValueError(f"severity must be one of {allowed}")
        return upper


# ---------------------------------------------------------------------------
# Parsed topic
# ---------------------------------------------------------------------------


@dataclass
class KafkaTopic:
    site_id: str
    skid_type: str
    skid_id: str
    message_type: str

    @property
    def kafka_name(self) -> str:
        return f"fluxot.{self.site_id}.{self.skid_type}.{self.skid_id}.{self.message_type}"

    @property
    def mqtt_name(self) -> str:
        return f"fluxot/{self.site_id}/{self.skid_type}/{self.skid_id}/{self.message_type}"


# ---------------------------------------------------------------------------
# Engineering unit conversion tables
# ---------------------------------------------------------------------------

# Conversion functions: value, source_unit -> (normalised_value, SI_unit)
_UNIT_CONVERSIONS: Dict[str, tuple[float, str]] = {
    # Speed
    "km/h": (1 / 3.6, "m/s"),
    "mph":  (0.44704, "m/s"),
    "ft/s": (0.3048, "m/s"),
    "m/min": (1 / 60.0, "m/s"),
    # Temperature
    "F":    (None, "degC"),   # Special case — formula conversion
    "K":    (None, "degC"),   # Special case
    # Pressure
    "psi":  (6894.757, "Pa"),
    "kPa":  (1000.0, "Pa"),
    "MPa":  (1_000_000.0, "Pa"),
    "mbar": (100.0, "Pa"),
    "bar":  (100_000.0, "Pa"),
    # Force / tension
    "kN":   (1000.0, "N"),
    "MN":   (1_000_000.0, "N"),
    "lbf":  (4.44822, "N"),
    # Magnetic field
    "T":    (1000.0, "mT"),
    "uT":   (0.001, "mT"),
    "G":    (0.1, "mT"),       # Gauss
    # Current — already SI (A)
    # Power
    "kW":   (1000.0, "W"),
    "MW":   (1_000_000.0, "W"),
    "HP":   (745.7, "W"),
}


def convert_to_si(value: float, unit: Optional[str]) -> tuple[float, Optional[str]]:
    """Convert a value in *unit* to its SI equivalent.

    Returns (converted_value, si_unit). If the unit is unknown or already SI,
    the value is returned unchanged.
    """
    if unit is None:
        return value, None

    unit_clean = unit.strip()

    # Temperature special cases
    if unit_clean in ("F", "degF", "°F"):
        return (value - 32) * 5 / 9, "degC"
    if unit_clean in ("K", "kelvin"):
        return value - 273.15, "degC"

    factor_si = _UNIT_CONVERSIONS.get(unit_clean)
    if factor_si and factor_si[0] is not None:
        factor, si_unit = factor_si
        return value * factor, si_unit

    return value, unit_clean


# ---------------------------------------------------------------------------
# DataTransformer
# ---------------------------------------------------------------------------


class DataTransformer:
    """Validates, normalises, and enriches raw messages from edge skids."""

    def __init__(self, skid_metadata: Optional[Dict[str, Dict[str, Any]]] = None) -> None:
        """
        Args:
            skid_metadata: Optional pre-loaded dict of skid_id -> metadata
                           (e.g. from DB). If provided, enrichment is possible
                           without a live DB connection.
        """
        self._skid_metadata: Dict[str, Dict[str, Any]] = skid_metadata or {}

    def register_skid(self, skid_id: str, metadata: Dict[str, Any]) -> None:
        """Cache skid metadata for enrichment."""
        self._skid_metadata[skid_id] = metadata

    # ------------------------------------------------------------------
    # Public transform methods
    # ------------------------------------------------------------------

    def transform_telemetry(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and normalise a raw telemetry dict.

        Steps:
          1. Pydantic validation
          2. SI unit normalisation
          3. Metadata enrichment (timezone, skid config)
        """
        msg = RawTelemetryMessage.model_validate(raw)

        # Unit conversion
        norm_value = msg.value_float
        norm_unit = msg.unit
        if msg.value_float is not None and msg.unit is not None:
            norm_value, norm_unit = convert_to_si(msg.value_float, msg.unit)

        result: Dict[str, Any] = {
            "skid_id": msg.skid_id,
            "site_id": msg.site_id,
            "skid_type": msg.skid_type,
            "timestamp": msg.timestamp,
            "tag_name": msg.tag_name,
            "value_float": norm_value,
            "value_bool": msg.value_bool,
            "value_str": msg.value_str,
            "quality": msg.quality,
            "unit": norm_unit,
        }

        # Enrich with cached skid metadata
        meta = self._skid_metadata.get(msg.skid_id, {})
        if meta:
            result["_meta"] = {
                "skid_name": meta.get("skid_name"),
                "site_name": meta.get("site_name"),
                "site_timezone": meta.get("timezone", "UTC"),
                "firmware_version": meta.get("firmware_version"),
            }

        return result

    def transform_alert(self, raw: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and enrich a raw alert dict."""
        msg = RawAlertMessage.model_validate(raw)

        result: Dict[str, Any] = {
            "skid_id": msg.skid_id,
            "site_id": msg.site_id,
            "skid_type": msg.skid_type,
            "timestamp": msg.timestamp,
            "alert_id": msg.alert_id,
            "severity": msg.severity,
            "message": msg.message,
            "tag_name": msg.tag_name,
            "value_json": msg.value_json,
        }

        # Enrich with skid and site names
        meta = self._skid_metadata.get(msg.skid_id, {})
        if meta:
            result["skid_name"] = meta.get("skid_name")
            result["site_name"] = meta.get("site_name")

        return result

    # ------------------------------------------------------------------
    # Topic normalisation
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_topic(mqtt_topic: str) -> KafkaTopic:
        """Parse an MQTT topic string into a structured KafkaTopic.

        Raises:
            ValueError: If the topic does not match the expected format.
        """
        pattern = re.compile(
            r"^fluxot/(?P<site>[^/]+)/(?P<type>[^/]+)/(?P<skid>[^/]+)/(?P<msg>[^/]+)$"
        )
        m = pattern.match(mqtt_topic)
        if not m:
            raise ValueError(
                f"MQTT topic '{mqtt_topic}' does not match expected format "
                f"fluxot/{{site}}/{{type}}/{{skid}}/{{message_type}}"
            )
        return KafkaTopic(
            site_id=m.group("site"),
            skid_type=m.group("type"),
            skid_id=m.group("skid"),
            message_type=m.group("msg"),
        )

    # ------------------------------------------------------------------
    # Batch processing
    # ------------------------------------------------------------------

    def transform_telemetry_batch(
        self, records: List[Dict[str, Any]]
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Transform a batch of telemetry dicts.

        Returns:
            (valid_records, invalid_records) tuple.
        """
        valid: List[Dict[str, Any]] = []
        invalid: List[Dict[str, Any]] = []

        for record in records:
            try:
                valid.append(self.transform_telemetry(record))
            except Exception as exc:
                logger.warning("Telemetry transform failed: %s — %s", record, exc)
                invalid.append({"record": record, "error": str(exc)})

        return valid, invalid

    def transform_alert_batch(
        self, records: List[Dict[str, Any]]
    ) -> tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Transform a batch of alert dicts."""
        valid: List[Dict[str, Any]] = []
        invalid: List[Dict[str, Any]] = []

        for record in records:
            try:
                valid.append(self.transform_alert(record))
            except Exception as exc:
                logger.warning("Alert transform failed: %s — %s", record, exc)
                invalid.append({"record": record, "error": str(exc)})

        return valid, invalid
