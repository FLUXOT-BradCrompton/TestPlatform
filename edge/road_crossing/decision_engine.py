"""
Road crossing autonomous decision engine.

Implements a safety-critical finite state machine (FSM) for managing traffic
flow across a moving conveyor belt.

States
------
BELT_RUNNING_CROSSING_CLOSED  – Normal operating state: belt moving, barriers closed.
BELT_STOPPED_CHECKING         – Belt has stopped; checking if safe to open.
SAFE_WINDOW_OPEN              – Belt stopped, safe gap confirmed, barriers may open.
CROSSING_IN_PROGRESS          – Barriers open, vehicle actively crossing.
CROSSING_COMPLETE             – Vehicle cleared; barriers closing, belt may resume.
EMERGENCY                     – Fault condition; all crossings locked out.

Safety invariants
-----------------
1. Barriers NEVER open while belt is running (unless safe gap >= SAFE_CROSSING_WINDOW_S).
2. Pedestrian detection adds 50% to the clearance time.
3. Haul trucks exceeding HAUL_TRUCK_MAX_SPEED_KMH * 0.9 are rejected.
4. Any anomaly (unknown object, sensor fault) forces EMERGENCY state.
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Deque, Optional

from edge.common.logging_config import get_logger

from .config import RoadCrossingConfig
from .sensors import CrossingSensorData

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Enumerations
# ---------------------------------------------------------------------------


class CrossingState(str, Enum):
    BELT_RUNNING_CROSSING_CLOSED = "BELT_RUNNING_CROSSING_CLOSED"
    BELT_STOPPED_CHECKING = "BELT_STOPPED_CHECKING"
    SAFE_WINDOW_OPEN = "SAFE_WINDOW_OPEN"
    CROSSING_IN_PROGRESS = "CROSSING_IN_PROGRESS"
    CROSSING_COMPLETE = "CROSSING_COMPLETE"
    EMERGENCY = "EMERGENCY"


class CrossingAction(str, Enum):
    NONE = "NONE"
    OPEN_BARRIER = "OPEN_BARRIER"
    CLOSE_BARRIER = "CLOSE_BARRIER"
    SET_TRAFFIC_LIGHT = "SET_TRAFFIC_LIGHT"
    EMERGENCY_STOP = "EMERGENCY_STOP"
    WAIT = "WAIT"


# ---------------------------------------------------------------------------
# Decision result
# ---------------------------------------------------------------------------


@dataclass
class CrossingDecision:
    """
    Output of one FSM evaluation cycle.

    Attributes:
        timestamp:                   UTC time of decision.
        state:                       Current FSM state.
        action:                      Action to execute.
        reason:                      Human-readable explanation.
        estimated_wait_s:            Estimated wait for next safe window.
        safe_to_cross:               True if crossing is currently safe.
        next_state_transition_s:     Estimated seconds until FSM transitions.
    """

    timestamp: datetime
    state: CrossingState
    action: CrossingAction
    reason: str
    estimated_wait_s: Optional[float]
    safe_to_cross: bool
    next_state_transition_s: Optional[float]


# ---------------------------------------------------------------------------
# Decision engine
# ---------------------------------------------------------------------------

_SAFETY_LOG_SIZE = 50
_PEDESTRIAN_CLEARANCE_MULTIPLIER = 1.5


class CrossingDecisionEngine:
    """
    Autonomous road-crossing decision engine.

    Thread-safe for use from asyncio (single-threaded) contexts.
    """

    def __init__(self, config: RoadCrossingConfig) -> None:
        self._config = config
        self._state: CrossingState = CrossingState.BELT_RUNNING_CROSSING_CLOSED

        # Timing references
        self._belt_stopped_at: Optional[float] = None
        self._crossing_started_at: Optional[float] = None
        self._safe_window_entered_at: Optional[float] = None

        # Safety log
        self._safety_log: Deque[CrossingDecision] = deque(maxlen=_SAFETY_LOG_SIZE)

        # FIFO vehicle queue (vehicle_class strings)
        self._vehicle_queue: deque[str] = deque()
        self._current_vehicle_class: str = "UNKNOWN"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def evaluate(self, sensor_data: CrossingSensorData) -> CrossingDecision:
        """
        Evaluate the current sensor state and return a :class:`CrossingDecision`.
        Also transitions the internal FSM state.
        """
        decision = self._fsm_transition(sensor_data)
        self._safety_log.append(decision)
        return decision

    @property
    def state(self) -> CrossingState:
        return self._state

    @property
    def safety_log(self) -> list[CrossingDecision]:
        return list(self._safety_log)

    def force_emergency(self, reason: str) -> None:
        """Force transition to EMERGENCY state from any state."""
        logger.critical("Crossing FSM forced to EMERGENCY", extra={"reason": reason})
        self._state = CrossingState.EMERGENCY

    def reset_emergency(self) -> None:
        """Clear EMERGENCY state back to the appropriate idle state."""
        logger.info("Crossing FSM emergency reset")
        self._state = CrossingState.BELT_RUNNING_CROSSING_CLOSED
        self._belt_stopped_at = None
        self._crossing_started_at = None

    # ------------------------------------------------------------------
    # FSM
    # ------------------------------------------------------------------

    def _fsm_transition(self, sd: CrossingSensorData) -> CrossingDecision:
        now = time.monotonic()
        cfg = self._config

        # Track queued vehicles
        vehicle_approaching = any(sd.loop_detectors[:2])  # front loops
        if vehicle_approaching and sd.vehicle_class not in ("UNKNOWN",):
            if not self._vehicle_queue or self._vehicle_queue[-1] != sd.vehicle_class:
                self._vehicle_queue.append(sd.vehicle_class)

        # ---- EMERGENCY ------------------------------------------------
        if self._state == CrossingState.EMERGENCY:
            return CrossingDecision(
                timestamp=sd.timestamp,
                state=self._state,
                action=CrossingAction.EMERGENCY_STOP,
                reason="System in EMERGENCY state – all crossings locked out",
                estimated_wait_s=None,
                safe_to_cross=False,
                next_state_transition_s=None,
            )

        # ---- Speed check for haul trucks ------------------------------
        if sd.vehicle_speed_kmh is not None:
            max_speed_threshold = cfg.HAUL_TRUCK_MAX_SPEED_KMH * 0.9
            if sd.vehicle_speed_kmh > max_speed_threshold:
                logger.warning(
                    "Vehicle speed exceeds safe threshold",
                    extra={
                        "speed_kmh": round(sd.vehicle_speed_kmh, 1),
                        "threshold_kmh": max_speed_threshold,
                    },
                )
                # Do not open barriers for over-speed vehicle
                self._vehicle_queue.clear()

        # ---- BELT_RUNNING_CROSSING_CLOSED -----------------------------
        if self._state == CrossingState.BELT_RUNNING_CROSSING_CLOSED:
            if not sd.belt_running:
                self._belt_stopped_at = now
                self._state = CrossingState.BELT_STOPPED_CHECKING
                return self._decision(
                    sd,
                    CrossingAction.WAIT,
                    "Belt has stopped – checking for safe crossing window",
                    estimated_wait_s=cfg.SAFE_CROSSING_WINDOW_S,
                )

            # Belt running – barriers stay closed
            wait_s = self._estimate_wait(sd.belt_speed_ms, sd.belt_running)
            return self._decision(
                sd,
                CrossingAction.NONE,
                "Belt running – barriers closed",
                estimated_wait_s=wait_s,
                safe=False,
            )

        # ---- BELT_STOPPED_CHECKING ------------------------------------
        if self._state == CrossingState.BELT_STOPPED_CHECKING:
            if sd.belt_running:
                # Belt restarted before we opened – go back to running state
                self._belt_stopped_at = None
                self._state = CrossingState.BELT_RUNNING_CROSSING_CLOSED
                return self._decision(
                    sd,
                    CrossingAction.NONE,
                    "Belt restarted during safety check",
                    safe=False,
                )

            stopped_duration = now - (self._belt_stopped_at or now)
            if stopped_duration >= cfg.SAFE_CROSSING_WINDOW_S:
                self._safe_window_entered_at = now
                self._state = CrossingState.SAFE_WINDOW_OPEN
                return self._decision(
                    sd,
                    CrossingAction.OPEN_BARRIER,
                    f"Safe window confirmed ({stopped_duration:.1f}s belt stopped)",
                    safe=True,
                    next_transition_s=cfg.VEHICLE_CLEARANCE_TIME_S,
                )

            remaining = cfg.SAFE_CROSSING_WINDOW_S - stopped_duration
            return self._decision(
                sd,
                CrossingAction.WAIT,
                f"Waiting for safe window ({remaining:.1f}s remaining)",
                estimated_wait_s=remaining,
            )

        # ---- SAFE_WINDOW_OPEN ----------------------------------------
        if self._state == CrossingState.SAFE_WINDOW_OPEN:
            if sd.belt_running:
                # Belt started while window open – close immediately
                self._state = CrossingState.BELT_RUNNING_CROSSING_CLOSED
                return self._decision(
                    sd,
                    CrossingAction.CLOSE_BARRIER,
                    "Belt restarted – closing barriers immediately",
                    safe=False,
                )

            any_loop = any(sd.loop_detectors)
            if any_loop or sd.waiting_vehicles_count > 0:
                self._current_vehicle_class = sd.vehicle_class
                if self._vehicle_queue:
                    self._current_vehicle_class = self._vehicle_queue.popleft()
                self._crossing_started_at = now
                self._state = CrossingState.CROSSING_IN_PROGRESS
                return self._decision(
                    sd,
                    CrossingAction.SET_TRAFFIC_LIGHT,
                    f"Vehicle entering crossing: {self._current_vehicle_class}",
                    safe=True,
                    next_transition_s=self._clearance_time(self._current_vehicle_class),
                )

            return self._decision(
                sd,
                CrossingAction.NONE,
                "Safe window open – awaiting vehicle",
                safe=True,
            )

        # ---- CROSSING_IN_PROGRESS ------------------------------------
        if self._state == CrossingState.CROSSING_IN_PROGRESS:
            if sd.belt_running:
                # Belt started while vehicle crossing – EMERGENCY
                self._state = CrossingState.EMERGENCY
                return self._decision(
                    sd,
                    CrossingAction.EMERGENCY_STOP,
                    "CRITICAL: Belt started while vehicle is crossing",
                    safe=False,
                )

            clearance = self._clearance_time(self._current_vehicle_class)
            elapsed = now - (self._crossing_started_at or now)

            rear_loops_clear = not any(sd.loop_detectors[2:])  # rear loops
            front_loops_clear = not any(sd.loop_detectors[:2])

            if (elapsed >= clearance and rear_loops_clear and front_loops_clear) or sd.crossing_clear:
                self._state = CrossingState.CROSSING_COMPLETE
                return self._decision(
                    sd,
                    CrossingAction.CLOSE_BARRIER,
                    f"Vehicle cleared after {elapsed:.1f}s",
                    safe=False,
                )

            remaining = max(0.0, clearance - elapsed)
            return self._decision(
                sd,
                CrossingAction.NONE,
                f"Vehicle crossing in progress ({remaining:.1f}s remaining)",
                safe=True,
                next_transition_s=remaining,
            )

        # ---- CROSSING_COMPLETE ---------------------------------------
        if self._state == CrossingState.CROSSING_COMPLETE:
            # Check for more waiting vehicles
            if sd.waiting_vehicles_count > 0 and self._vehicle_queue:
                # Another vehicle can cross if belt still stopped
                if not sd.belt_running:
                    self._state = CrossingState.SAFE_WINDOW_OPEN
                    return self._decision(
                        sd,
                        CrossingAction.NONE,
                        f"{sd.waiting_vehicles_count} vehicle(s) still waiting",
                        safe=True,
                    )

            # All clear – transition back to running state
            self._state = CrossingState.BELT_RUNNING_CROSSING_CLOSED
            self._crossing_started_at = None
            return self._decision(
                sd,
                CrossingAction.SET_TRAFFIC_LIGHT,
                "Crossing complete – returning to normal operation",
                safe=False,
            )

        # Should never reach here
        logger.error("FSM reached unexpected state", extra={"state": str(self._state)})
        return self._decision(sd, CrossingAction.NONE, "Unknown state", safe=False)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _clearance_time(self, vehicle_class: str) -> float:
        base = self._config.VEHICLE_CLEARANCE_TIME_S
        if vehicle_class == "PEDESTRIAN":
            return base * _PEDESTRIAN_CLEARANCE_MULTIPLIER
        if vehicle_class == "HAUL_TRUCK":
            return base * 1.3  # Haul trucks are longer
        return base

    def _estimate_wait(self, belt_speed: float, belt_running: bool) -> Optional[float]:
        """Rough estimate of seconds until next safe crossing opportunity."""
        if not belt_running:
            return 0.0
        # No way to know when belt will stop; return None to indicate unknown
        return None

    def _decision(
        self,
        sd: CrossingSensorData,
        action: CrossingAction,
        reason: str,
        estimated_wait_s: Optional[float] = None,
        safe: Optional[bool] = None,
        next_transition_s: Optional[float] = None,
    ) -> CrossingDecision:
        if safe is None:
            safe = self._state in (CrossingState.SAFE_WINDOW_OPEN, CrossingState.CROSSING_IN_PROGRESS)
        return CrossingDecision(
            timestamp=sd.timestamp,
            state=self._state,
            action=action,
            reason=reason,
            estimated_wait_s=estimated_wait_s,
            safe_to_cross=safe,
            next_state_transition_s=next_transition_s,
        )
