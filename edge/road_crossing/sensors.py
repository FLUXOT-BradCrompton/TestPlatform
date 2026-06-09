"""
Road crossing sensor reader.

Reads all crossing sensors from OPC-UA (or generates realistic simulation data
using a Poisson vehicle-arrival process).
"""

from __future__ import annotations

import math
import os
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import List, Optional

from edge.common.logging_config import get_logger
from edge.common.opcua_client import OPCUAClient

from .config import RoadCrossingConfig

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Sensor data structure
# ---------------------------------------------------------------------------


@dataclass
class CrossingSensorData:
    """
    Complete sensor snapshot for one crossing poll cycle.

    Attributes:
        timestamp:               UTC timestamp.
        loop_detectors:          4 booleans – True when vehicle present.
        vehicle_speed_kmh:       Radar-measured vehicle speed (None if clear).
        vehicle_class:           Classified vehicle type string.
        lidar_readings:          2 floats – distance to nearest object (m).
        belt_speed_ms:           Current belt speed in m/s (from OPC-UA).
        belt_running:            True when belt is running.
        crossing_clear:          Composite 'all clear' flag.
        barrier_states:          2 booleans – True when barrier is open.
        traffic_light_states:    4 strings – "RED" / "AMBER" / "GREEN".
        waiting_vehicles_count:  Number of vehicles queued at barriers.
    """

    timestamp: datetime
    loop_detectors: List[bool]            # 4 elements
    vehicle_speed_kmh: Optional[float]
    vehicle_class: str                    # HAUL_TRUCK / LIGHT_VEHICLE / PEDESTRIAN / UNKNOWN
    lidar_readings: List[float]           # 2 elements, metres
    belt_speed_ms: float
    belt_running: bool
    crossing_clear: bool
    barrier_states: List[bool]            # 2 elements: [open, open]
    traffic_light_states: List[str]       # 4 elements
    waiting_vehicles_count: int


# ---------------------------------------------------------------------------
# OPC-UA node definitions
# ---------------------------------------------------------------------------

_LOOP_NODES = [f"ns=2;s=Crossing.Loop{i}" for i in range(1, 5)]
_LIDAR_NODES = [f"ns=2;s=Crossing.Lidar{i}" for i in range(1, 3)]
_VEHICLE_SPEED_NODE = "ns=2;s=Crossing.VehicleSpeed"
_VEHICLE_CLASS_NODE = "ns=2;s=Crossing.VehicleClass"
_BELT_SPEED_NODE = "ns=2;s=BeltStatus.Speed"
_BELT_RUNNING_NODE = "ns=2;s=BeltStatus.Running"
_CROSSING_CLEAR_NODE = "ns=2;s=Crossing.Clear"
_BARRIER_NODES = [f"ns=2;s=CrossingControl.Barrier{i}Open" for i in range(1, 3)]
_TRAFFIC_NODES = [f"ns=2;s=CrossingControl.TrafficLight{i}" for i in range(1, 5)]
_WAITING_COUNT_NODE = "ns=2;s=Crossing.WaitingCount"


# ---------------------------------------------------------------------------
# Simulation helpers
# ---------------------------------------------------------------------------

_SIM_T0 = time.monotonic()


class _PoissonTrafficSim:
    """
    Simulates vehicle arrivals using a Poisson process.

    Average arrival rate is configurable (vehicles per minute).
    """

    def __init__(self, arrival_rate_per_min: float = 2.0) -> None:
        self._rate_per_s = arrival_rate_per_min / 60.0
        self._last_arrival: float = time.monotonic()
        self._in_crossing: bool = False
        self._crossing_start: Optional[float] = None
        self._vehicle_class: str = "UNKNOWN"
        self._vehicle_speed: Optional[float] = None
        self._waiting: int = 0
        self._clearance_time_s: float = 12.0

    def tick(self, barriers_open: bool) -> dict:
        now = time.monotonic()
        elapsed = now - self._last_arrival

        # Probabilistic arrival
        arrival_prob = 1.0 - math.exp(-self._rate_per_s * elapsed)
        if random.random() < arrival_prob and not self._in_crossing:
            self._last_arrival = now
            classes = ["HAUL_TRUCK"] * 5 + ["LIGHT_VEHICLE"] * 4 + ["PEDESTRIAN"] * 1
            self._vehicle_class = random.choice(classes)
            if self._vehicle_class == "HAUL_TRUCK":
                self._vehicle_speed = random.uniform(15.0, 38.0)
            elif self._vehicle_class == "LIGHT_VEHICLE":
                self._vehicle_speed = random.uniform(20.0, 50.0)
            else:
                self._vehicle_speed = None
            if barriers_open:
                self._in_crossing = True
                self._crossing_start = now
                self._waiting = max(0, self._waiting - 1)
            else:
                self._waiting = min(self._waiting + 1, 8)

        # Clear crossing after clearance time
        if self._in_crossing and self._crossing_start:
            if now - self._crossing_start > self._clearance_time_s:
                self._in_crossing = False
                self._crossing_start = None
                self._vehicle_class = "UNKNOWN"
                self._vehicle_speed = None

        vehicle_present = self._in_crossing or self._waiting > 0
        loop_1 = self._in_crossing
        loop_2 = self._in_crossing
        loop_3 = self._waiting > 0
        loop_4 = self._waiting > 1

        return {
            "loops": [loop_3, loop_4, loop_1, loop_2],
            "vehicle_speed": self._vehicle_speed if vehicle_present else None,
            "vehicle_class": self._vehicle_class if vehicle_present else "UNKNOWN",
            "waiting": self._waiting,
        }


class BeltSimHelper:
    """Simulates belt running / speed for the road crossing context."""

    def __init__(self, nominal_speed: float = 3.5) -> None:
        self._nominal = nominal_speed

    def tick(self) -> tuple[float, bool]:
        t = time.monotonic() - _SIM_T0
        running = True
        # Occasional simulated stops
        if 300 < t % 600 < 330:
            running = False
        speed = self._nominal + 0.05 * math.sin(t / 30.0) + random.gauss(0, 0.01)
        return (max(0.0, speed), running)


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------


class CrossingSensorReader:
    """
    Reads all crossing sensors via OPC-UA, or uses simulation when
    configured or when OPC-UA is unavailable.
    """

    def __init__(
        self,
        config: RoadCrossingConfig,
        opcua: Optional[OPCUAClient] = None,
    ) -> None:
        self._config = config
        self._opcua = opcua
        self._simulate = config.SIMULATE or os.environ.get("SIMULATE", "false").lower() == "true"

        self._traffic_sim = _PoissonTrafficSim(arrival_rate_per_min=3.0)
        self._belt_sim = BeltSimHelper(config.BELT_SPEED_MS)

        # Track simulated barrier state (starts closed)
        self._sim_barriers: List[bool] = [False, False]
        # Track simulated traffic light state
        self._sim_lights: List[str] = ["RED", "RED", "RED", "RED"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def read_all(self) -> CrossingSensorData:
        """Read all sensors; falls back to simulation on OPC-UA failure."""
        if self._simulate or (self._opcua is None) or (not self._opcua.is_connected):
            return self._simulate_data()
        try:
            return await self._read_from_opcua()
        except Exception as exc:
            logger.warning("Crossing OPC-UA read failed, simulating", extra={"error": str(exc)})
            return self._simulate_data()

    # ------------------------------------------------------------------
    # OPC-UA reader
    # ------------------------------------------------------------------

    async def _read_from_opcua(self) -> CrossingSensorData:
        all_nodes = (
            _LOOP_NODES
            + _LIDAR_NODES
            + [
                _VEHICLE_SPEED_NODE,
                _VEHICLE_CLASS_NODE,
                _BELT_SPEED_NODE,
                _BELT_RUNNING_NODE,
                _CROSSING_CLEAR_NODE,
                _WAITING_COUNT_NODE,
            ]
            + _BARRIER_NODES
            + _TRAFFIC_NODES
        )
        values = await self._opcua.read_nodes(all_nodes)

        def _b(nid: str, default: bool = False) -> bool:
            v = values.get(nid)
            return bool(v) if v is not None else default

        def _f(nid: str, default: float = 0.0) -> float:
            v = values.get(nid)
            try:
                return float(v) if v is not None else default
            except (TypeError, ValueError):
                return default

        def _s(nid: str, default: str = "UNKNOWN") -> str:
            v = values.get(nid)
            return str(v) if v is not None else default

        loops = [_b(n) for n in _LOOP_NODES]
        lidar = [_f(n, default=100.0) for n in _LIDAR_NODES]
        barriers = [_b(n, default=False) for n in _BARRIER_NODES]
        lights = [_s(n, default="RED") for n in _TRAFFIC_NODES]

        vehicle_speed_raw = values.get(_VEHICLE_SPEED_NODE)
        vehicle_speed = float(vehicle_speed_raw) if vehicle_speed_raw is not None else None

        any_loop = any(loops)
        return CrossingSensorData(
            timestamp=datetime.now(tz=timezone.utc),
            loop_detectors=loops,
            vehicle_speed_kmh=vehicle_speed,
            vehicle_class=_s(_VEHICLE_CLASS_NODE),
            lidar_readings=lidar,
            belt_speed_ms=_f(_BELT_SPEED_NODE),
            belt_running=_b(_BELT_RUNNING_NODE, default=True),
            crossing_clear=_b(_CROSSING_CLEAR_NODE, default=not any_loop),
            barrier_states=barriers,
            traffic_light_states=lights,
            waiting_vehicles_count=int(_f(_WAITING_COUNT_NODE, 0.0)),
        )

    # ------------------------------------------------------------------
    # Simulation
    # ------------------------------------------------------------------

    def _simulate_data(self) -> CrossingSensorData:
        barriers_open = self._sim_barriers[0]
        traffic = self._traffic_sim.tick(barriers_open)
        belt_speed, belt_running = self._belt_sim.tick()

        loops: List[bool] = traffic["loops"]
        any_loop = any(loops)

        # Simulated lidar: random distances reflecting vehicle presence
        lidar: List[float] = [
            (random.uniform(2.0, 8.0) if loops[0] or loops[1] else random.uniform(20.0, 50.0)),
            (random.uniform(2.0, 8.0) if loops[2] or loops[3] else random.uniform(20.0, 50.0)),
        ]

        return CrossingSensorData(
            timestamp=datetime.now(tz=timezone.utc),
            loop_detectors=loops,
            vehicle_speed_kmh=traffic["vehicle_speed"],
            vehicle_class=traffic["vehicle_class"],
            lidar_readings=lidar,
            belt_speed_ms=belt_speed,
            belt_running=belt_running,
            crossing_clear=(not any_loop) and (not belt_running),
            barrier_states=list(self._sim_barriers),
            traffic_light_states=list(self._sim_lights),
            waiting_vehicles_count=traffic["waiting"],
        )

    def update_sim_state(self, barriers: List[bool], lights: List[str]) -> None:
        """Called by the controller to keep simulation state consistent."""
        self._sim_barriers = list(barriers)
        self._sim_lights = list(lights)
