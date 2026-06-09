"""
OPC-UA simulation server for Flux OT edge development.

Creates a realistic OPC-UA address space mirroring the belt rip monitor and
road crossing skid layouts, then continuously updates node values with
sine-wave + Gaussian-noise data every 500 ms.

Run with::

    python -m edge.opcua_simulator.server

or via Docker (see Dockerfile).

Server endpoint: opc.tcp://0.0.0.0:4840
"""

from __future__ import annotations

import asyncio
import logging
import math
import os
import random
import signal
import time
from typing import Any, Dict, Tuple

from asyncua import Server, ua
from asyncua.server.nodes import Node

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ---------------------------------------------------------------------------
# Simulation helpers
# ---------------------------------------------------------------------------

_T0 = time.monotonic()


def _t() -> float:
    return time.monotonic() - _T0


def _wave(base: float, amp: float, period_s: float, noise: float, phase: float = 0.0) -> float:
    return max(0.0, base + amp * math.sin(2 * math.pi * _t() / period_s + phase) + random.gauss(0.0, noise))


# ---------------------------------------------------------------------------
# Node registry: name -> (Node object, update_fn)
# ---------------------------------------------------------------------------

NodeRegistry = Dict[str, Tuple[Node, Any]]


# ---------------------------------------------------------------------------
# Server
# ---------------------------------------------------------------------------


class FluxOTSimServer:
    """Async OPC-UA simulation server."""

    ENDPOINT = os.environ.get("OPCUA_ENDPOINT", "opc.tcp://0.0.0.0:4840")
    NAMESPACE_URI = "urn:fluxot:simulator"
    UPDATE_INTERVAL_S = 0.5

    def __init__(self) -> None:
        self._server = Server()
        self._ns: int = 0
        self._registry: NodeRegistry = {}
        self._running = False

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def init(self) -> None:
        await self._server.init()
        self._server.set_endpoint(self.ENDPOINT)
        self._server.set_server_name("Flux OT Simulator")
        self._ns = await self._server.register_namespace(self.NAMESPACE_URI)

        objects = self._server.get_objects_node()

        # ---- Belt Rip Monitor nodes ----------------------------------
        belt_folder = await objects.add_folder(self._ns, "BeltRipMonitor")
        sensor_folder = await belt_folder.add_folder(self._ns, "Sensor")
        belt_status_folder = await belt_folder.add_folder(self._ns, "BeltStatus")
        belt_control_folder = await belt_folder.add_folder(self._ns, "BeltControl")
        belt_alarms_folder = await belt_folder.add_folder(self._ns, "BeltAlarms")

        # Magnetic sensors (8 channels)
        for i in range(1, 9):
            n = await sensor_folder.add_variable(self._ns, f"Mag{i}", 50.0)
            await n.set_writable()
            self._registry[f"Sensor.Mag{i}"] = (n, lambda _i=i: _wave(50.0, 3.0, 30.0, 0.5, _i * 0.4))

        # Acoustic sensors (4 channels)
        for i in range(1, 5):
            n = await sensor_folder.add_variable(self._ns, f"Acoustic{i}", 60.0)
            await n.set_writable()
            self._registry[f"Sensor.Acoustic{i}"] = (n, lambda _i=i: _wave(60.0, 5.0, 10.0, 1.0, _i * 0.7))

        # Load cells (2 channels)
        for i in range(1, 3):
            n = await sensor_folder.add_variable(self._ns, f"LoadCell{i}", 20.0)
            await n.set_writable()
            self._registry[f"Sensor.LoadCell{i}"] = (n, lambda _i=i: _wave(20.0, 1.0, 60.0, 0.2, _i * 1.5))

        # Belt kinematics
        belt_speed_n = await sensor_folder.add_variable(self._ns, "BeltSpeed", 3.5)
        await belt_speed_n.set_writable()
        self._registry["Sensor.BeltSpeed"] = (belt_speed_n, lambda: _wave(3.5, 0.1, 120.0, 0.02))

        belt_tension_n = await sensor_folder.add_variable(self._ns, "BeltTension", 150.0)
        await belt_tension_n.set_writable()
        self._registry["Sensor.BeltTension"] = (belt_tension_n, lambda: _wave(150.0, 10.0, 90.0, 1.0))

        motor_current_n = await sensor_folder.add_variable(self._ns, "MotorCurrent", 85.0)
        await motor_current_n.set_writable()
        self._registry["Sensor.MotorCurrent"] = (motor_current_n, lambda: _wave(85.0, 5.0, 60.0, 0.5))

        belt_temp_n = await sensor_folder.add_variable(self._ns, "BeltTemperature", 45.0)
        await belt_temp_n.set_writable()
        self._registry["Sensor.BeltTemperature"] = (belt_temp_n, lambda: _wave(45.0, 2.0, 300.0, 0.1))

        # Belt status
        running_n = await belt_status_folder.add_variable(self._ns, "Running", True)
        await running_n.set_writable()
        self._registry["BeltStatus.Running"] = (running_n, lambda: True)

        speed_status_n = await belt_status_folder.add_variable(self._ns, "Speed", 3.5)
        await speed_status_n.set_writable()
        self._registry["BeltStatus.Speed"] = (speed_status_n, lambda: _wave(3.5, 0.1, 120.0, 0.02))

        # Belt control (writeback targets)
        estop_n = await belt_control_folder.add_variable(self._ns, "EmergencyStop", False)
        await estop_n.set_writable()
        self._registry["BeltControl.EmergencyStop"] = (estop_n, None)  # no auto-update

        speed_sp_n = await belt_control_folder.add_variable(self._ns, "SpeedSetpoint", 3.5)
        await speed_sp_n.set_writable()
        self._registry["BeltControl.SpeedSetpoint"] = (speed_sp_n, None)

        ack_n = await belt_alarms_folder.add_variable(self._ns, "Acknowledge", "")
        await ack_n.set_writable()
        self._registry["BeltAlarms.Acknowledge"] = (ack_n, None)

        # ---- Road Crossing nodes ------------------------------------
        crossing_folder = await objects.add_folder(self._ns, "Crossing")
        crossing_ctrl_folder = await objects.add_folder(self._ns, "CrossingControl")

        # Loop detectors (4)
        for i in range(1, 5):
            n = await crossing_folder.add_variable(self._ns, f"Loop{i}", False)
            await n.set_writable()
            # Occasionally true (simulate vehicle presence)
            self._registry[f"Crossing.Loop{i}"] = (
                n,
                lambda _i=i: random.random() < 0.05,
            )

        # Lidar (2 channels)
        for i in range(1, 3):
            n = await crossing_folder.add_variable(self._ns, f"Lidar{i}", 50.0)
            await n.set_writable()
            self._registry[f"Crossing.Lidar{i}"] = (n, lambda: _wave(50.0, 5.0, 20.0, 0.5))

        # Vehicle speed / class
        v_speed_n = await crossing_folder.add_variable(self._ns, "VehicleSpeed", 0.0)
        await v_speed_n.set_writable()
        self._registry["Crossing.VehicleSpeed"] = (v_speed_n, lambda: _wave(0.0, 15.0, 30.0, 1.0))

        v_class_n = await crossing_folder.add_variable(self._ns, "VehicleClass", "UNKNOWN")
        await v_class_n.set_writable()
        classes = ["HAUL_TRUCK", "LIGHT_VEHICLE", "UNKNOWN"]
        self._registry["Crossing.VehicleClass"] = (v_class_n, lambda: random.choice(classes))

        # Crossing clear (derived)
        clear_n = await crossing_folder.add_variable(self._ns, "Clear", True)
        await clear_n.set_writable()
        self._registry["Crossing.Clear"] = (clear_n, lambda: random.random() > 0.1)

        # Waiting count
        wait_n = await crossing_folder.add_variable(self._ns, "WaitingCount", 0)
        await wait_n.set_writable()
        self._registry["Crossing.WaitingCount"] = (wait_n, lambda: random.randint(0, 3))

        # Crossing control (writeback targets)
        barrier_open_n = await crossing_ctrl_folder.add_variable(self._ns, "BarrierOpen", False)
        await barrier_open_n.set_writable()
        self._registry["CrossingControl.BarrierOpen"] = (barrier_open_n, None)

        barrier_close_n = await crossing_ctrl_folder.add_variable(self._ns, "BarrierClose", False)
        await barrier_close_n.set_writable()
        self._registry["CrossingControl.BarrierClose"] = (barrier_close_n, None)

        for i in range(1, 5):
            n = await crossing_ctrl_folder.add_variable(self._ns, f"TrafficLight{i}", "RED")
            await n.set_writable()
            self._registry[f"CrossingControl.TrafficLight{i}"] = (n, None)

        for i in range(1, 3):
            n = await crossing_ctrl_folder.add_variable(self._ns, f"Barrier{i}Open", False)
            await n.set_writable()
            self._registry[f"CrossingControl.Barrier{i}Open"] = (n, None)

        estop_crossing_n = await crossing_ctrl_folder.add_variable(self._ns, "EmergencyStop", False)
        await estop_crossing_n.set_writable()
        self._registry["CrossingControl.EmergencyStop"] = (estop_crossing_n, None)

        logger.info(
            "OPC-UA simulator address space created",
            extra={"node_count": len(self._registry), "endpoint": self.ENDPOINT},
        )

    # ------------------------------------------------------------------
    # Update loop
    # ------------------------------------------------------------------

    async def _update_loop(self) -> None:
        """Continuously update all auto-updating nodes with simulated values."""
        while self._running:
            for name, (node, update_fn) in self._registry.items():
                if update_fn is None:
                    continue
                try:
                    new_value = update_fn()
                    await node.write_value(new_value)
                except Exception as exc:
                    logger.debug("Node update error", extra={"node": name, "error": str(exc)})
            await asyncio.sleep(self.UPDATE_INTERVAL_S)

    # ------------------------------------------------------------------
    # Main
    # ------------------------------------------------------------------

    async def run(self) -> None:
        await self.init()
        self._running = True

        async with self._server:
            logger.info("OPC-UA simulator running on %s", self.ENDPOINT)
            update_task = asyncio.ensure_future(self._update_loop())

            stop_event = asyncio.Event()

            def _stop(_sig: int) -> None:
                logger.info("Shutdown signal received")
                stop_event.set()

            loop = asyncio.get_running_loop()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    loop.add_signal_handler(sig, _stop, sig)
                except NotImplementedError:
                    pass

            await stop_event.wait()
            self._running = False
            update_task.cancel()
            try:
                await update_task
            except asyncio.CancelledError:
                pass

        logger.info("OPC-UA simulator stopped")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def _main() -> None:
    server = FluxOTSimServer()
    await server.run()


if __name__ == "__main__":
    asyncio.run(_main())
