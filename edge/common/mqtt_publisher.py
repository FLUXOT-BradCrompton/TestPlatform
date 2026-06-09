"""
Async MQTT publisher / subscriber for edge skid services.

Uses paho-mqtt v2 in a thread-safe manner driven by an asyncio event loop.
Key features:
  - TLS mutual-auth support
  - Exponential-backoff reconnection (capped at 60 s)
  - Offline buffer (ring buffer, BUFFER_SIZE limit)
  - Automatic flush of buffered messages on reconnect
  - Topic helpers for the fluxot/{site}/{type}/{skid}/... hierarchy
"""

from __future__ import annotations

import asyncio
import json
import logging
import ssl
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Deque, Dict, Optional, Tuple

import paho.mqtt.client as mqtt

from .config import EdgeConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal types
# ---------------------------------------------------------------------------

_BufferEntry = Tuple[str, bytes, int]  # (topic, payload_bytes, qos)


class MQTTPublisher:
    """
    Async-friendly MQTT client wrapping paho-mqtt v2.

    The paho network loop runs in a dedicated background thread managed by
    paho itself (``loop_start()``). All public coroutines are safe to call
    from the asyncio event loop.
    """

    _BACKOFF_BASE: float = 1.0
    _BACKOFF_MAX: float = 60.0

    def __init__(self, config: EdgeConfig) -> None:
        self._config = config
        self._client: mqtt.Client = self._build_client()
        self._connected: bool = False
        self._reconnect_task: Optional[asyncio.Task] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None

        # Ring buffer for messages that arrive while disconnected
        self._buffer: Deque[_BufferEntry] = deque(maxlen=config.BUFFER_SIZE)

        # Callbacks registered by subscribers: topic -> coroutine callback
        self._subscriptions: Dict[str, Callable] = {}

        # Event to signal that a connection has been established
        self._connected_event: asyncio.Event = asyncio.Event()

    # ------------------------------------------------------------------
    # Paho callbacks (called from paho's network thread)
    # ------------------------------------------------------------------

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        if reason_code == 0 or (hasattr(reason_code, "value") and reason_code.value == 0):
            logger.info(
                "MQTT connected",
                extra={"broker": self._config.MQTT_BROKER_HOST, "port": self._config.MQTT_BROKER_PORT},
            )
            self._connected = True
            if self._loop and not self._loop.is_closed():
                self._loop.call_soon_threadsafe(self._connected_event.set)
            # Re-subscribe to all registered topics
            for topic in self._subscriptions:
                client.subscribe(topic, qos=self._config.MQTT_QOS)
            # Schedule buffer flush
            if self._loop and not self._loop.is_closed():
                asyncio.run_coroutine_threadsafe(self._flush_buffer(), self._loop)
        else:
            rc = reason_code.value if hasattr(reason_code, "value") else reason_code
            logger.warning("MQTT connect failed", extra={"reason_code": rc})

    def _on_disconnect(
        self,
        client: mqtt.Client,
        userdata: Any,
        disconnect_flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        rc = reason_code.value if hasattr(reason_code, "value") else reason_code
        logger.warning("MQTT disconnected", extra={"reason_code": rc})
        self._connected = False
        if self._loop and not self._loop.is_closed():
            self._loop.call_soon_threadsafe(self._connected_event.clear)

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        topic = msg.topic
        try:
            payload = json.loads(msg.payload.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            payload = msg.payload

        callback = self._subscriptions.get(topic)
        if callback is None:
            # Check wildcard subscriptions (simple prefix match)
            for sub_topic, cb in self._subscriptions.items():
                if sub_topic.endswith("#"):
                    prefix = sub_topic[:-1]
                    if topic.startswith(prefix):
                        callback = cb
                        break
                elif sub_topic.endswith("+"):
                    prefix = sub_topic.rsplit("/", 1)[0]
                    if topic.startswith(prefix):
                        callback = cb
                        break

        if callback and self._loop and not self._loop.is_closed():
            asyncio.run_coroutine_threadsafe(callback(topic, payload), self._loop)

    # ------------------------------------------------------------------
    # Client construction
    # ------------------------------------------------------------------

    def _build_client(self) -> mqtt.Client:
        cfg = self._config
        client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=f"{cfg.SITE_ID}-{cfg.SKID_ID}-{cfg.SKID_TYPE}",
            clean_session=True,
        )

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message

        if cfg.MQTT_USERNAME:
            client.username_pw_set(cfg.MQTT_USERNAME, cfg.MQTT_PASSWORD)

        if cfg.MQTT_USE_TLS:
            tls_ctx = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            if cfg.MQTT_CA_CERT:
                tls_ctx.load_verify_locations(cafile=cfg.MQTT_CA_CERT)
            if cfg.MQTT_CLIENT_CERT and cfg.MQTT_CLIENT_KEY:
                tls_ctx.load_cert_chain(certfile=cfg.MQTT_CLIENT_CERT, keyfile=cfg.MQTT_CLIENT_KEY)
            client.tls_set_context(tls_ctx)

        # Last Will and Testament so the broker can notify subscribers if we die
        lwt_topic = self._make_topic("status")
        lwt_payload = json.dumps({"status": "OFFLINE", "site_id": cfg.SITE_ID, "skid_id": cfg.SKID_ID})
        client.will_set(lwt_topic, payload=lwt_payload, qos=1, retain=True)

        return client

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """
        Connect to the MQTT broker with exponential-backoff retry.
        Returns once the connection is established.
        """
        self._loop = asyncio.get_running_loop()
        backoff = self._BACKOFF_BASE

        while True:
            try:
                self._client.connect_async(
                    self._config.MQTT_BROKER_HOST,
                    self._config.MQTT_BROKER_PORT,
                    keepalive=60,
                )
                self._client.loop_start()
                # Wait up to (backoff * 2) seconds for the connection event
                try:
                    await asyncio.wait_for(self._connected_event.wait(), timeout=backoff * 2)
                except asyncio.TimeoutError:
                    pass

                if self._connected:
                    logger.info("MQTT connection established")
                    return

                logger.warning(
                    "MQTT connection attempt timed out, retrying",
                    extra={"backoff_s": backoff},
                )
            except Exception as exc:
                logger.error("MQTT connect error", extra={"error": str(exc), "backoff_s": backoff})

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, self._BACKOFF_MAX)

    async def disconnect(self) -> None:
        """Gracefully disconnect from the broker."""
        if self._reconnect_task and not self._reconnect_task.done():
            self._reconnect_task.cancel()
        self._client.loop_stop()
        self._client.disconnect()
        self._connected = False
        logger.info("MQTT disconnected cleanly")

    # ------------------------------------------------------------------
    # Publishing
    # ------------------------------------------------------------------

    def _make_topic(self, suffix: str) -> str:
        cfg = self._config
        return f"fluxot/{cfg.SITE_ID}/{cfg.SKID_TYPE}/{cfg.SKID_ID}/{suffix}"

    def _serialise(self, payload: dict) -> bytes:
        def _default(obj: Any) -> Any:
            if isinstance(obj, datetime):
                return obj.isoformat()
            raise TypeError(f"Not serialisable: {type(obj)}")

        return json.dumps(payload, default=_default).encode("utf-8")

    async def publish(self, topic: str, payload: dict, qos: int = 1) -> None:
        """
        Serialise *payload* to JSON and publish to *topic*.
        If currently disconnected the message is buffered for later delivery.
        """
        raw = self._serialise(payload)

        if not self._connected:
            logger.debug("MQTT offline – buffering message", extra={"topic": topic})
            self._buffer.append((topic, raw, qos))
            return

        info = self._client.publish(topic, payload=raw, qos=qos)
        if info.rc != mqtt.MQTT_ERR_SUCCESS:
            logger.warning(
                "MQTT publish failed, buffering",
                extra={"topic": topic, "rc": info.rc},
            )
            self._buffer.append((topic, raw, qos))

    async def publish_telemetry(self, data: dict) -> None:
        """Publish telemetry to the canonical telemetry topic."""
        await self.publish(self._make_topic("telemetry"), data, qos=self._config.MQTT_QOS)

    async def publish_alert(self, alert: dict) -> None:
        """Publish an alert to the canonical alerts topic."""
        await self.publish(self._make_topic("alerts"), alert, qos=2)  # QoS 2 for alerts

    async def publish_heartbeat(self) -> None:
        """Publish a heartbeat to the canonical heartbeat topic."""
        payload = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "site_id": self._config.SITE_ID,
            "skid_id": self._config.SKID_ID,
            "skid_type": self._config.SKID_TYPE,
            "status": "ALIVE",
        }
        await self.publish(self._make_topic("heartbeat"), payload, qos=1)

    # ------------------------------------------------------------------
    # Subscription
    # ------------------------------------------------------------------

    async def subscribe(self, topic: str, callback: Callable) -> None:
        """
        Subscribe to *topic* and invoke *callback(topic, payload)* for each
        received message.  Callback must be an async function.
        """
        self._subscriptions[topic] = callback
        if self._connected:
            self._client.subscribe(topic, qos=self._config.MQTT_QOS)
            logger.debug("MQTT subscribed", extra={"topic": topic})

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _flush_buffer(self) -> None:
        """Drain the offline buffer, publishing all held messages."""
        flushed = 0
        while self._buffer:
            topic, raw, qos = self._buffer.popleft()
            info = self._client.publish(topic, payload=raw, qos=qos)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                # Put it back and give up for now
                self._buffer.appendleft((topic, raw, qos))
                logger.warning("MQTT buffer flush stalled", extra={"remaining": len(self._buffer)})
                break
            flushed += 1

        if flushed:
            logger.info("MQTT buffer flushed", extra={"count": flushed})

    @property
    def is_connected(self) -> bool:
        return self._connected

    @property
    def buffer_depth(self) -> int:
        return len(self._buffer)
