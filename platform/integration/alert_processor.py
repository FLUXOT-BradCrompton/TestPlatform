"""
Alert processing, rule evaluation, deduplication, and escalation for Flux OT Platform.

Alarm rule evaluation is run against every incoming telemetry message.
Alerts are deduplicated within a configurable cooldown window (default 5 min).
Unacknowledged ALARMs are automatically escalated to CRITICAL after 10 min.
"""
from __future__ import annotations

import asyncio
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

import redis.asyncio as aioredis

from platform.api.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Enums and dataclasses
# ---------------------------------------------------------------------------


class AlarmCondition(str, Enum):
    GT = "GT"   # value > threshold
    LT = "LT"   # value < threshold
    EQ = "EQ"   # value == threshold
    NEQ = "NEQ" # value != threshold
    GTE = "GTE" # value >= threshold
    LTE = "LTE" # value <= threshold
    TRUE = "TRUE"   # bool is True
    FALSE = "FALSE" # bool is False


@dataclass
class AlarmRule:
    """A single alarm evaluation rule."""

    rule_id: str
    tag_name: str
    condition: AlarmCondition
    threshold: Optional[float]          # Used for GT/LT/EQ/NEQ/GTE/LTE
    severity: str                        # INFO / WARNING / ALARM / CRITICAL
    message_template: str                # Can reference {value} and {threshold}
    cooldown_s: int = 300                # Deduplicate within this window
    escalation_after_s: int = 600        # Escalate to CRITICAL if not acked after this
    enabled: bool = True

    def evaluate(self, tag_name: str, value_float: Optional[float], value_bool: Optional[bool]) -> bool:
        """Return True if this rule fires for the given tag value."""
        if not self.enabled or tag_name != self.tag_name:
            return False

        if self.condition == AlarmCondition.TRUE:
            return value_bool is True
        if self.condition == AlarmCondition.FALSE:
            return value_bool is False

        if value_float is None:
            return False

        t = self.threshold
        if self.condition == AlarmCondition.GT:
            return value_float > t
        if self.condition == AlarmCondition.LT:
            return value_float < t
        if self.condition == AlarmCondition.GTE:
            return value_float >= t
        if self.condition == AlarmCondition.LTE:
            return value_float <= t
        if self.condition == AlarmCondition.EQ:
            return abs(value_float - t) < 1e-9
        if self.condition == AlarmCondition.NEQ:
            return abs(value_float - t) >= 1e-9
        return False

    def format_message(self, value: Any) -> str:
        try:
            return self.message_template.format(
                value=value, threshold=self.threshold
            )
        except (KeyError, ValueError):
            return self.message_template


# ---------------------------------------------------------------------------
# Default rule sets per skid type
# ---------------------------------------------------------------------------


def _belt_rip_rules() -> List[AlarmRule]:
    return [
        AlarmRule(
            rule_id="BRM_COMPOSITE_WARNING",
            tag_name="composite_score",
            condition=AlarmCondition.GT,
            threshold=0.60,
            severity="WARNING",
            message_template=(
                "Belt anomaly score {value:.2f} exceeds warning threshold {threshold:.2f}. "
                "Monitor closely."
            ),
            cooldown_s=300,
        ),
        AlarmRule(
            rule_id="BRM_COMPOSITE_ALARM",
            tag_name="composite_score",
            condition=AlarmCondition.GT,
            threshold=0.75,
            severity="ALARM",
            message_template=(
                "Belt anomaly score {value:.2f} exceeds alarm threshold {threshold:.2f}. "
                "Possible belt rip — inspect immediately."
            ),
            cooldown_s=180,
            escalation_after_s=600,
        ),
        AlarmRule(
            rule_id="BRM_COMPOSITE_CRITICAL",
            tag_name="composite_score",
            condition=AlarmCondition.GT,
            threshold=0.90,
            severity="CRITICAL",
            message_template=(
                "Belt rip likely confirmed — score {value:.2f} exceeds critical "
                "threshold {threshold:.2f}. EMERGENCY STOP recommended."
            ),
            cooldown_s=60,
            escalation_after_s=0,
        ),
        AlarmRule(
            rule_id="BRM_MAGNETIC_ANOMALY",
            tag_name="magnetic_anomaly",
            condition=AlarmCondition.GT,
            threshold=0.5,  # Treated as boolean flag encoded as float 0/1
            severity="ALARM",
            message_template="Magnetic anomaly flag set — possible belt rip detected.",
            cooldown_s=120,
        ),
        AlarmRule(
            rule_id="BRM_SPEED_LOW",
            tag_name="belt_speed",
            condition=AlarmCondition.LT,
            threshold=0.5,
            severity="WARNING",
            message_template="Belt speed {value:.2f} m/s is critically low (threshold: {threshold} m/s).",
            cooldown_s=300,
        ),
        AlarmRule(
            rule_id="BRM_MOTOR_OVERCURRENT",
            tag_name="motor_current_a",
            condition=AlarmCondition.GT,
            threshold=220.0,
            severity="ALARM",
            message_template="Motor current {value:.1f} A exceeds limit {threshold:.0f} A.",
            cooldown_s=120,
        ),
        AlarmRule(
            rule_id="BRM_TENSION_HIGH",
            tag_name="belt_tension_kn",
            condition=AlarmCondition.GT,
            threshold=150.0,
            severity="ALARM",
            message_template="Belt tension {value:.1f} kN exceeds safe limit {threshold:.0f} kN.",
            cooldown_s=120,
        ),
    ]


def _road_crossing_rules() -> List[AlarmRule]:
    return [
        AlarmRule(
            rule_id="RXS_EMERGENCY_STOP",
            tag_name="emergency_stop",
            condition=AlarmCondition.TRUE,
            threshold=None,
            severity="CRITICAL",
            message_template="Road crossing emergency stop activated.",
            cooldown_s=30,
            escalation_after_s=0,
        ),
        AlarmRule(
            rule_id="RXS_BARRIER_FAULT",
            tag_name="barrier_fault",
            condition=AlarmCondition.TRUE,
            threshold=None,
            severity="ALARM",
            message_template="Barrier fault detected — manual inspection required.",
            cooldown_s=180,
            escalation_after_s=600,
        ),
        AlarmRule(
            rule_id="RXS_HYDRAULIC_LOW",
            tag_name="hydraulic_pressure_bar",
            condition=AlarmCondition.LT,
            threshold=100.0,
            severity="WARNING",
            message_template="Hydraulic pressure {value:.1f} bar below minimum {threshold:.0f} bar.",
            cooldown_s=300,
        ),
        AlarmRule(
            rule_id="RXS_HYDRAULIC_CRITICAL",
            tag_name="hydraulic_pressure_bar",
            condition=AlarmCondition.LT,
            threshold=60.0,
            severity="CRITICAL",
            message_template="Hydraulic pressure {value:.1f} bar critically low — barrier may not operate.",
            cooldown_s=60,
        ),
        AlarmRule(
            rule_id="RXS_VEHICLE_OVERSTAY",
            tag_name="vehicle_detected",
            condition=AlarmCondition.TRUE,
            threshold=None,
            severity="WARNING",
            message_template="Vehicle detected in crossing zone — check traffic management.",
            cooldown_s=60,
        ),
    ]


def _generic_rules() -> List[AlarmRule]:
    return [
        AlarmRule(
            rule_id="GEN_TEMP_HIGH",
            tag_name="ambient_temp_c",
            condition=AlarmCondition.GT,
            threshold=60.0,
            severity="WARNING",
            message_template="Ambient temperature {value:.1f} °C exceeds {threshold:.0f} °C.",
            cooldown_s=600,
        ),
        AlarmRule(
            rule_id="GEN_TEMP_CRITICAL",
            tag_name="ambient_temp_c",
            condition=AlarmCondition.GT,
            threshold=75.0,
            severity="CRITICAL",
            message_template="Ambient temperature {value:.1f} °C critically high — equipment at risk.",
            cooldown_s=300,
        ),
    ]


_RULES_BY_SKID_TYPE: Dict[str, List[AlarmRule]] = {
    "BELT_RIP_MONITOR": _belt_rip_rules() + _generic_rules(),
    "ROAD_CROSSING": _road_crossing_rules() + _generic_rules(),
    "GENERIC": _generic_rules(),
}


def get_alarm_rules(skid_type: str) -> List[AlarmRule]:
    """Return the list of AlarmRule objects applicable to *skid_type*."""
    return _RULES_BY_SKID_TYPE.get(skid_type.upper(), _generic_rules())


# ---------------------------------------------------------------------------
# AlertProcessor
# ---------------------------------------------------------------------------


class AlertProcessor:
    """Evaluates alarm rules against telemetry and manages alert lifecycle."""

    def __init__(
        self,
        alert_callback: Optional[Callable] = None,
        redis_url: Optional[str] = None,
    ) -> None:
        """
        Args:
            alert_callback: Async callable(alert_dict) called when a new alert fires.
            redis_url: Redis URL for deduplication state. Falls back to settings.
        """
        self._alert_callback = alert_callback
        self._redis_url = redis_url or settings.REDIS_URL
        self._redis: Optional[aioredis.Redis] = None
        self._escalation_tasks: Dict[str, asyncio.Task] = {}

    async def connect(self) -> None:
        self._redis = aioredis.from_url(self._redis_url, decode_responses=True)

    async def disconnect(self) -> None:
        # Cancel pending escalation tasks
        for task in self._escalation_tasks.values():
            task.cancel()
        self._escalation_tasks.clear()

        if self._redis:
            await self._redis.aclose()

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def evaluate_alarm_rules(
        self, telemetry: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Apply all rules for the skid type and return fired alert dicts."""
        skid_type = telemetry.get("skid_type", "GENERIC")
        skid_id = telemetry.get("skid_id", "")
        tag_name = telemetry.get("tag_name", "")
        value_float = telemetry.get("value_float")
        value_bool = telemetry.get("value_bool")

        fired_alerts: List[Dict[str, Any]] = []

        for rule in get_alarm_rules(skid_type):
            if not rule.evaluate(tag_name, value_float, value_bool):
                continue

            value_display = value_float if value_float is not None else value_bool
            alert = _build_alert(rule, skid_id, telemetry, value_display)

            # Deduplication check
            if await self._is_in_cooldown(skid_id, rule.rule_id):
                logger.debug(
                    "Rule %s suppressed for skid %s (in cooldown)", rule.rule_id, skid_id
                )
                continue

            await self._set_cooldown(skid_id, rule.rule_id, rule.cooldown_s)
            fired_alerts.append(alert)

            # Schedule escalation if applicable
            if rule.escalation_after_s > 0 and rule.severity in ("ALARM", "WARNING"):
                task_key = f"{skid_id}:{rule.rule_id}"
                if task_key not in self._escalation_tasks or self._escalation_tasks[task_key].done():
                    self._escalation_tasks[task_key] = asyncio.create_task(
                        self._escalate_after(alert, rule.escalation_after_s, skid_id, rule.rule_id)
                    )

        return fired_alerts

    async def process_alert(self, alert: Dict[str, Any]) -> Dict[str, Any]:
        """Process an incoming alert dict — deduplicate and call callback.

        Returns the alert dict (possibly enriched), or an empty dict if suppressed.
        """
        skid_id = alert.get("skid_id", "")
        alert_id = alert.get("alert_id", "")
        cooldown = alert.get("cooldown_s", 300)

        if await self._is_in_cooldown(skid_id, alert_id):
            logger.debug("Alert %s suppressed for skid %s", alert_id, skid_id)
            return {}

        await self._set_cooldown(skid_id, alert_id, cooldown)

        if self._alert_callback:
            try:
                await self._alert_callback(alert)
            except Exception as exc:
                logger.error("Alert callback error: %s", exc)

        return alert

    # ------------------------------------------------------------------
    # Escalation
    # ------------------------------------------------------------------

    async def _escalate_after(
        self,
        original_alert: Dict[str, Any],
        delay_s: int,
        skid_id: str,
        rule_id: str,
    ) -> None:
        """Wait *delay_s* seconds, then escalate to CRITICAL if not acked."""
        try:
            await asyncio.sleep(delay_s)

            # Check if the alert was acknowledged in Redis
            ack_key = f"alert_acked:{skid_id}:{rule_id}"
            if self._redis and await self._redis.exists(ack_key):
                return

            escalated = {
                **original_alert,
                "severity": "CRITICAL",
                "alert_id": f"{rule_id}_ESCALATED",
                "message": (
                    f"[AUTO-ESCALATED] {original_alert.get('message', '')} "
                    f"(original severity {original_alert.get('severity', 'ALARM')} "
                    f"not acknowledged within {delay_s // 60} minutes)"
                ),
                "escalated_from": original_alert.get("alert_id"),
                "auto_escalated": True,
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            }

            logger.warning(
                "Alert escalated to CRITICAL for skid %s: %s", skid_id, rule_id
            )

            if self._alert_callback:
                await self._alert_callback(escalated)

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Escalation task error for %s/%s: %s", skid_id, rule_id, exc)

    async def mark_acknowledged(self, skid_id: str, rule_id: str) -> None:
        """Mark an alert as acknowledged, suppressing any pending escalation."""
        if self._redis:
            ack_key = f"alert_acked:{skid_id}:{rule_id}"
            await self._redis.set(ack_key, "1", ex=3600)

        task_key = f"{skid_id}:{rule_id}"
        task = self._escalation_tasks.pop(task_key, None)
        if task and not task.done():
            task.cancel()

    # ------------------------------------------------------------------
    # Cooldown helpers
    # ------------------------------------------------------------------

    async def _is_in_cooldown(self, skid_id: str, rule_id: str) -> bool:
        if not self._redis:
            return False
        key = _cooldown_key(skid_id, rule_id)
        return bool(await self._redis.exists(key))

    async def _set_cooldown(self, skid_id: str, rule_id: str, ttl_s: int) -> None:
        if not self._redis:
            return
        key = _cooldown_key(skid_id, rule_id)
        await self._redis.set(key, "1", ex=ttl_s)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _cooldown_key(skid_id: str, rule_id: str) -> str:
    return f"alarm_cooldown:{skid_id}:{rule_id}"


def _build_alert(
    rule: AlarmRule,
    skid_id: str,
    telemetry: Dict[str, Any],
    value_display: Any,
) -> Dict[str, Any]:
    return {
        "skid_id": skid_id,
        "site_id": telemetry.get("site_id"),
        "skid_type": telemetry.get("skid_type"),
        "timestamp": telemetry.get("timestamp", datetime.now(tz=timezone.utc).isoformat()),
        "alert_id": rule.rule_id,
        "severity": rule.severity,
        "message": rule.format_message(value_display),
        "tag_name": rule.tag_name,
        "value_json": {
            "value": value_display,
            "threshold": rule.threshold,
            "rule_id": rule.rule_id,
        },
        "cooldown_s": rule.cooldown_s,
    }
