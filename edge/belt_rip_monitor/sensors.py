"""
Belt sensor reader for the Flux OT belt rip monitor.

Reads all physical sensors via OPC-UA (or generates simulation data).
Provides a clean :class:`BeltSensorData` dataclass for downstream use.
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

from .config import BeltRipConfig

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Sensor data structure
# ---------------------------------------------------------------------------


@dataclass
class BeltSensorData:
    """
    Snapshot of all belt sensor readings taken in a single poll cycle.

    Attributes:
        timestamp:          UTC timestamp of the poll.
        magnetic_sensors:   8 floats – magnetic field strength per channel (mT).
        acoustic_sensors:   4 floats – acoustic energy per channel (dB).
        load_cells:         2 floats – load cell forces (kN).
        belt_speed:         Belt surface speed in m/s.
        belt_tension:       Belt tension in kN.
        motor_current:      Drive motor current in A.
        belt_temperature:   Belt surface temperature in °C.
        running:            True when belt is running.
    """

    timestamp: datetime
    magnetic_sensors: List[float]   # 8 channels
    acoustic_sensors: List[float]   # 4 channels
    load_cells: List[float]         # 2 channels
    belt_speed: float               # m/s
    belt_tension: float             # kN
    motor_current: float            # A
    belt_temperature: float         # °C
    running: bool


# ---------------------------------------------------------------------------
# OPC-UA node mapping
# ---------------------------------------------------------------------------

# Mapping of short sensor names to OPC-UA node-ID suffixes.
# Actual node IDs are constructed from OPCUA_SENSOR_NAMESPACE.
_MAG_NODES = [f"ns={{ns}};s=Sensor.Mag{i}" for i in range(1, 9)]
_ACOU_NODES = [f"ns={{ns}};s=Sensor.Acoustic{i}" for i in range(1, 5)]
_LOAD_NODES = [f"ns={{ns}};s=Sensor.LoadCell{i}" for i in range(1, 3)]

_BELT_SPEED_NODE = "ns={ns};s=Sensor.BeltSpeed"
_BELT_TENSION_NODE = "ns={ns};s=Sensor.BeltTension"
_MOTOR_CURRENT_NODE = "ns={ns};s=Sensor.MotorCurrent"
_BELT_TEMP_NODE = "ns={ns};s=Sensor.BeltTemperature"
_RUNNING_NODE = "ns={ns};s=BeltStatus.Running"


def _fmt(template: str, ns: int) -> str:
    return template.format(ns=ns)


# ---------------------------------------------------------------------------
# Simulation helpers
# ---------------------------------------------------------------------------

_SIM_T0 = time.monotonic()


def _sim_value(
    base: float,
    amplitude: float,
    period_s: float,
    noise_scale: float,
    phase: float = 0.0,
) -> float:
    """Return a realistic simulated sensor reading."""
    t = time.monotonic() - _SIM_T0
    sine = amplitude * math.sin(2 * math.pi * t / period_s + phase)
    noise = random.gauss(0.0, noise_scale)
    return max(0.0, base + sine + noise)


# ---------------------------------------------------------------------------
# Reader
# ---------------------------------------------------------------------------


class BeltSensorReader:
    """
    Reads all belt sensors via OPC-UA, or generates simulated values when
    ``config.SIMULATE`` is ``True`` or the OPC-UA client is disconnected.
    """

    def __init__(self, config: BeltRipConfig, opcua: Optional[OPCUAClient] = None) -> None:
        self._config = config
        self._opcua = opcua
        self._ns = config.OPCUA_SENSOR_NAMESPACE
        self._simulate = config.SIMULATE or os.environ.get("SIMULATE", "false").lower() == "true"

        # Build the full list of node IDs to batch-read
        self._mag_node_ids = [_fmt(t, self._ns) for t in _MAG_NODES]
        self._acou_node_ids = [_fmt(t, self._ns) for t in _ACOU_NODES]
        self._load_node_ids = [_fmt(t, self._ns) for t in _LOAD_NODES]
        self._all_node_ids = (
            self._mag_node_ids
            + self._acou_node_ids
            + self._load_node_ids
            + [
                _fmt(_BELT_SPEED_NODE, self._ns),
                _fmt(_BELT_TENSION_NODE, self._ns),
                _fmt(_MOTOR_CURRENT_NODE, self._ns),
                _fmt(_BELT_TEMP_NODE, self._ns),
                _fmt(_RUNNING_NODE, self._ns),
            ]
        )

    async def read_all(self) -> BeltSensorData:
        """
        Read all sensors and return a :class:`BeltSensorData` snapshot.

        Falls back to simulation mode if OPC-UA is unavailable.
        """
        if self._simulate or (self._opcua is None) or (not self._opcua.is_connected):
            return self._simulate_data()

        try:
            values = await self._opcua.read_nodes(self._all_node_ids)
            return self._parse_opcua_values(values)
        except Exception as exc:
            logger.warning(
                "OPC-UA sensor read failed, using simulated fallback",
                extra={"error": str(exc)},
            )
            return self._simulate_data()

    def _parse_opcua_values(self, values: dict) -> BeltSensorData:
        def _f(node_id: str, default: float = 0.0) -> float:
            v = values.get(node_id)
            try:
                return float(v) if v is not None else default
            except (TypeError, ValueError):
                return default

        def _b(node_id: str, default: bool = False) -> bool:
            v = values.get(node_id)
            if v is None:
                return default
            if isinstance(v, bool):
                return v
            return bool(v)

        ns = self._ns
        return BeltSensorData(
            timestamp=datetime.now(tz=timezone.utc),
            magnetic_sensors=[_f(_fmt(_MAG_NODES[i], ns)) for i in range(8)],
            acoustic_sensors=[_f(_fmt(_ACOU_NODES[i], ns)) for i in range(4)],
            load_cells=[_f(_fmt(_LOAD_NODES[i], ns)) for i in range(2)],
            belt_speed=_f(_fmt(_BELT_SPEED_NODE, ns)),
            belt_tension=_f(_fmt(_BELT_TENSION_NODE, ns)),
            motor_current=_f(_fmt(_MOTOR_CURRENT_NODE, ns)),
            belt_temperature=_f(_fmt(_BELT_TEMP_NODE, ns)),
            running=_b(_fmt(_RUNNING_NODE, ns), default=True),
        )

    def _simulate_data(self) -> BeltSensorData:
        """
        Generate realistic synthetic sensor data using sine waves + Gaussian
        noise.  Useful for development and integration testing without
        physical hardware.
        """
        cfg = self._config
        t = time.monotonic() - _SIM_T0

        # Magnetic: 8 channels, nominal ~50 mT, slight drift
        mag = [
            _sim_value(50.0, 3.0, 30.0, 0.5, phase=i * 0.4)
            for i in range(8)
        ]

        # Acoustic: 4 channels, nominal ~60 dB, short bursts
        acou = [
            _sim_value(60.0, 5.0, 10.0, 1.0, phase=i * 0.7)
            for i in range(4)
        ]

        # Load cells: 2 channels, nominal 20 kN each
        load = [
            _sim_value(20.0, 1.0, 60.0, 0.2, phase=i * 1.5)
            for i in range(2)
        ]

        belt_speed = max(0.0, _sim_value(cfg.BELT_SPEED_MS, 0.1, 120.0, 0.02))
        belt_tension = _sim_value(150.0, 10.0, 90.0, 1.0)
        motor_current = _sim_value(85.0, 5.0, 60.0, 0.5)
        belt_temperature = _sim_value(45.0, 2.0, 300.0, 0.1)
        running = True  # Simulated belt is always running

        return BeltSensorData(
            timestamp=datetime.now(tz=timezone.utc),
            magnetic_sensors=mag,
            acoustic_sensors=acou,
            load_cells=load,
            belt_speed=belt_speed,
            belt_tension=belt_tension,
            motor_current=motor_current,
            belt_temperature=belt_temperature,
            running=running,
        )
