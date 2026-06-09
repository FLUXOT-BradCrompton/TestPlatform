"""
Abstract base class for all edge skid services.

Provides:
  - Lifecycle management (start / stop)
  - Polling loop driven by POLL_INTERVAL_MS
  - Heartbeat loop driven by HEARTBEAT_INTERVAL_S
  - MQTT command listener (subscribes to the commands topic)
  - Connection health tracking
  - Prometheus metrics: polls_total, errors_total, last_poll_duration_ms
  - Signal handling (SIGINT / SIGTERM) for graceful shutdown
  - Sequence number tracking for telemetry batches
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
import time
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Optional

from prometheus_client import Counter, Gauge, start_http_server

from .config import EdgeConfig
from .data_models import Command, CommandAck, CommandStatus, Heartbeat
from .logging_config import get_logger, setup_logging
from .modbus_adapter import ModbusAdapter
from .mqtt_publisher import MQTTPublisher
from .opcua_client import OPCUAClient

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Prometheus metrics (module-level singletons)
# ---------------------------------------------------------------------------

_POLLS_TOTAL = Counter(
    "edge_polls_total",
    "Total number of poll cycles executed",
    ["site_id", "skid_id", "skid_type"],
)
_ERRORS_TOTAL = Counter(
    "edge_errors_total",
    "Total number of errors during poll cycles",
    ["site_id", "skid_id", "skid_type"],
)
_LAST_POLL_DURATION_MS = Gauge(
    "edge_last_poll_duration_ms",
    "Duration of the most recent poll cycle in milliseconds",
    ["site_id", "skid_id", "skid_type"],
)


class BaseSkidService(ABC):
    """
    Abstract base for all edge skid services.

    Subclasses must implement:
      - :meth:`poll`
      - :meth:`handle_command`
    """

    def __init__(self, config: EdgeConfig) -> None:
        self._config = config
        setup_logging(config.LOG_LEVEL)

        self._mqtt = MQTTPublisher(config)
        self._opcua: Optional[OPCUAClient] = OPCUAClient(config)
        self._modbus: Optional[ModbusAdapter] = (
            ModbusAdapter(config) if config.MODBUS_HOST else None
        )

        self._sequence_number: int = 0
        self._start_time: float = time.monotonic()
        self._running: bool = False
        self._shutdown_event: asyncio.Event = asyncio.Event()

        # Per-instance metric labels
        self._metric_labels = [config.SITE_ID, config.SKID_ID, config.SKID_TYPE]

        # Metrics references
        self._polls_total = _POLLS_TOTAL.labels(*self._metric_labels)
        self._errors_total = _ERRORS_TOTAL.labels(*self._metric_labels)
        self._last_poll_duration_ms = _LAST_POLL_DURATION_MS.labels(*self._metric_labels)

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def mqtt(self) -> MQTTPublisher:
        return self._mqtt

    @property
    def opcua(self) -> Optional[OPCUAClient]:
        return self._opcua

    @property
    def modbus(self) -> Optional[ModbusAdapter]:
        return self._modbus

    @property
    def uptime_s(self) -> int:
        return int(time.monotonic() - self._start_time)

    def _next_sequence(self) -> int:
        self._sequence_number += 1
        return self._sequence_number

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def poll(self) -> None:
        """
        Perform one poll cycle: read sensors, process data, publish telemetry.
        Called every POLL_INTERVAL_MS milliseconds.
        """

    @abstractmethod
    async def handle_command(self, command: Command) -> None:
        """
        Process an inbound command from the MQTT commands topic.
        Subclasses should publish a :class:`CommandAck` via MQTT.
        """

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Connect all clients and start background tasks."""
        logger.info(
            "Starting skid service",
            extra={
                "site_id": self._config.SITE_ID,
                "skid_id": self._config.SKID_ID,
                "skid_type": self._config.SKID_TYPE,
            },
        )
        self._running = True
        self._start_time = time.monotonic()

        # Connect MQTT first (buffers telemetry if not yet connected)
        await self._mqtt.connect()

        # Connect OPC-UA
        try:
            await self._opcua.connect()
            await self._opcua.start_reconnect_loop()
        except Exception as exc:
            logger.warning("OPC-UA initial connect failed, will retry", extra={"error": str(exc)})

        # Connect Modbus (optional)
        if self._modbus:
            try:
                await self._modbus.connect()
            except Exception as exc:
                logger.warning("Modbus initial connect failed, will retry", extra={"error": str(exc)})

        # Start Prometheus metrics HTTP server on port 9090
        try:
            start_http_server(9090)
            logger.info("Prometheus metrics server started on :9090")
        except Exception as exc:
            logger.warning("Could not start Prometheus metrics server", extra={"error": str(exc)})

        # Start background tasks
        asyncio.ensure_future(self._poll_loop())
        asyncio.ensure_future(self._heartbeat_loop())
        asyncio.ensure_future(self._command_listener())

        logger.info("Skid service started successfully")

    async def stop(self) -> None:
        """Gracefully shut down all clients and tasks."""
        logger.info("Stopping skid service")
        self._running = False
        self._shutdown_event.set()

        # Disconnect clients
        try:
            await self._mqtt.disconnect()
        except Exception as exc:
            logger.warning("Error disconnecting MQTT", extra={"error": str(exc)})

        try:
            await self._opcua.disconnect()
        except Exception as exc:
            logger.warning("Error disconnecting OPC-UA", extra={"error": str(exc)})

        if self._modbus:
            try:
                await self._modbus.disconnect()
            except Exception as exc:
                logger.warning("Error disconnecting Modbus", extra={"error": str(exc)})

        logger.info("Skid service stopped")

    # ------------------------------------------------------------------
    # Background loops
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """Drive the :meth:`poll` method every POLL_INTERVAL_MS milliseconds."""
        interval_s = self._config.POLL_INTERVAL_MS / 1000.0
        while self._running:
            t0 = time.monotonic()
            try:
                await self.poll()
                self._polls_total.inc()
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._errors_total.inc()
                logger.error("Poll cycle error", extra={"error": str(exc)}, exc_info=True)

            elapsed_ms = (time.monotonic() - t0) * 1000
            self._last_poll_duration_ms.set(elapsed_ms)

            # Sleep for the remaining interval (skip if poll took too long)
            remaining = interval_s - (time.monotonic() - t0)
            if remaining > 0:
                try:
                    await asyncio.sleep(remaining)
                except asyncio.CancelledError:
                    break

    async def _heartbeat_loop(self) -> None:
        """Publish a heartbeat every HEARTBEAT_INTERVAL_S seconds."""
        while self._running:
            try:
                await asyncio.sleep(self._config.HEARTBEAT_INTERVAL_S)
                heartbeat = Heartbeat(
                    site_id=self._config.SITE_ID,
                    skid_id=self._config.SKID_ID,
                    skid_type=self._config.SKID_TYPE,
                    status="RUNNING" if self._running else "STOPPING",
                    uptime_s=self.uptime_s,
                    connection_states={
                        "mqtt": "CONNECTED" if self._mqtt.is_connected else "DISCONNECTED",
                        "opcua": "CONNECTED" if self._opcua.is_connected else "DISCONNECTED",
                        "modbus": (
                            "CONNECTED"
                            if self._modbus and self._modbus.is_connected
                            else ("DISABLED" if not self._modbus else "DISCONNECTED")
                        ),
                    },
                )
                await self._mqtt.publish_heartbeat()
                await self._mqtt.publish(
                    self._mqtt._make_topic("heartbeat"),
                    heartbeat.model_dump(mode="json"),
                    qos=1,
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Heartbeat loop error", extra={"error": str(exc)})

    async def _command_listener(self) -> None:
        """
        Subscribe to the MQTT commands topic and dispatch inbound commands
        to :meth:`handle_command`.
        """
        cfg = self._config
        command_topic = (
            f"fluxot/{cfg.SITE_ID}/{cfg.SKID_TYPE}/{cfg.SKID_ID}/commands"
        )

        async def _on_command(topic: str, payload: dict) -> None:
            try:
                cmd = Command(**payload)
                logger.info(
                    "Received command",
                    extra={
                        "command_id": cmd.command_id,
                        "command_type": cmd.command_type,
                        "source": cmd.source,
                    },
                )
                await self.handle_command(cmd)
            except Exception as exc:
                logger.error("Command dispatch error", extra={"error": str(exc)}, exc_info=True)

        # Wait for MQTT to be connected before subscribing
        while self._running and not self._mqtt.is_connected:
            await asyncio.sleep(1.0)

        if self._running:
            await self._mqtt.subscribe(command_topic, _on_command)
            logger.info("Command listener subscribed", extra={"topic": command_topic})

    # ------------------------------------------------------------------
    # Helper: publish CommandAck
    # ------------------------------------------------------------------

    async def _ack_command(
        self,
        command_id: str,
        status: CommandStatus,
        message: str = "",
    ) -> None:
        ack = CommandAck(command_id=command_id, status=status, message=message)
        ack_topic = (
            f"fluxot/{self._config.SITE_ID}/{self._config.SKID_TYPE}"
            f"/{self._config.SKID_ID}/command_acks"
        )
        await self._mqtt.publish(ack_topic, ack.model_dump(mode="json"), qos=1)

    # ------------------------------------------------------------------
    # Sync entry point
    # ------------------------------------------------------------------

    def run(self) -> None:
        """
        Synchronous entry point.

        Creates the asyncio event loop, installs SIGINT / SIGTERM handlers
        and runs the service until a shutdown signal is received.
        """
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        async def _main() -> None:
            await self.start()
            # Block until shutdown is requested
            await self._shutdown_event.wait()
            await self.stop()

        def _signal_handler(sig: int) -> None:
            logger.info("Shutdown signal received", extra={"signal": sig})
            loop.call_soon_threadsafe(self._shutdown_event.set)

        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, _signal_handler, sig)
        except NotImplementedError:
            # Windows does not support add_signal_handler
            pass

        try:
            loop.run_until_complete(_main())
        finally:
            loop.close()
