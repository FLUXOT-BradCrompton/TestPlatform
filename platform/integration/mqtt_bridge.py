"""
MQTT-to-Kafka bidirectional bridge service for Flux OT Platform.

Responsibilities:
  Edge -> Cloud:  Subscribe to fluxot/# MQTT topics, validate payloads,
                  deduplicate (5 s window), publish to matching Kafka topics.
  Cloud -> Edge:  Subscribe to Kafka command topics, publish to MQTT command
                  topics so edge skids receive their commands over MQTT.

Topic mapping (MQTT <-> Kafka):
  MQTT  fluxot/{site}/{type}/{skid}/telemetry
  Kafka fluxot.{site}.{type}.{skid}.telemetry
  (Same substitution for alerts, heartbeat, writeback, commands)
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional, Tuple

import paho.mqtt.client as mqtt
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
import redis.asyncio as aioredis

from platform.api.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MQTT_QOS_EXACTLY_ONCE = 2
MQTT_QOS_AT_LEAST_ONCE = 1
DEDUP_WINDOW_S = 5
DEDUP_KEY_PREFIX = "mqtt_dedup:"
BRIDGE_CLIENT_ID = "fluxot-mqtt-kafka-bridge"

# Topics that flow Edge -> Platform (subscribed on MQTT, produced to Kafka)
INBOUND_SUFFIXES = {"telemetry", "alerts", "heartbeat", "writeback"}

# Topics that flow Platform -> Edge (consumed from Kafka, published to MQTT)
OUTBOUND_SUFFIXES = {"commands"}

# ---------------------------------------------------------------------------
# Topic parsing
# ---------------------------------------------------------------------------


@dataclass
class ParsedTopic:
    site_id: str
    skid_type: str
    skid_id: str
    message_type: str

    @property
    def kafka_topic(self) -> str:
        return f"fluxot.{self.site_id}.{self.skid_type}.{self.skid_id}.{self.message_type}"

    @property
    def mqtt_topic(self) -> str:
        return f"fluxot/{self.site_id}/{self.skid_type}/{self.skid_id}/{self.message_type}"


def parse_mqtt_topic(topic: str) -> Optional[ParsedTopic]:
    """Parse an MQTT topic string into a structured object.

    Expected format: ``fluxot/{site_id}/{skid_type}/{skid_id}/{message_type}``
    """
    parts = topic.split("/")
    if len(parts) != 5 or parts[0] != "fluxot":
        return None
    return ParsedTopic(
        site_id=parts[1],
        skid_type=parts[2],
        skid_id=parts[3],
        message_type=parts[4],
    )


def parse_kafka_topic(topic: str) -> Optional[ParsedTopic]:
    """Parse a Kafka topic string into a structured object.

    Expected format: ``fluxot.{site_id}.{skid_type}.{skid_id}.{message_type}``
    """
    parts = topic.split(".")
    if len(parts) != 5 or parts[0] != "fluxot":
        return None
    return ParsedTopic(
        site_id=parts[1],
        skid_type=parts[2],
        skid_id=parts[3],
        message_type=parts[4],
    )


# ---------------------------------------------------------------------------
# Metrics (simple counters — expose via Prometheus in production)
# ---------------------------------------------------------------------------


class BridgeMetrics:
    def __init__(self) -> None:
        self.mqtt_to_kafka: int = 0
        self.kafka_to_mqtt: int = 0
        self.errors: int = 0
        self.deduplicated: int = 0
        self.invalid_payloads: int = 0

    def to_dict(self) -> Dict[str, int]:
        return {
            "mqtt_to_kafka": self.mqtt_to_kafka,
            "kafka_to_mqtt": self.kafka_to_mqtt,
            "errors": self.errors,
            "deduplicated": self.deduplicated,
            "invalid_payloads": self.invalid_payloads,
        }


# ---------------------------------------------------------------------------
# Bridge class
# ---------------------------------------------------------------------------


class MQTTKafkaBridge:
    """Bidirectional bridge between MQTT broker and Apache Kafka."""

    def __init__(self) -> None:
        self._mqtt_client: Optional[mqtt.Client] = None
        self._kafka_producer: Optional[AIOKafkaProducer] = None
        self._kafka_consumer: Optional[AIOKafkaConsumer] = None
        self._redis: Optional[aioredis.Redis] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._running = False
        self._consume_task: Optional[asyncio.Task] = None
        self.metrics = BridgeMetrics()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Connect to MQTT, Kafka, and Redis; begin bridging."""
        self._loop = asyncio.get_running_loop()
        self._running = True

        # Redis for deduplication
        self._redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

        # Kafka producer (edge -> platform direction)
        producer_kwargs: Dict[str, Any] = {
            "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "enable_idempotence": True,
            "acks": "all",
            "compression_type": "snappy",
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
            "key_serializer": lambda k: k.encode("utf-8") if k else None,
        }
        _apply_kafka_security(producer_kwargs)
        self._kafka_producer = AIOKafkaProducer(**producer_kwargs)
        await self._kafka_producer.start()

        # Kafka consumer (platform -> edge direction): only command topics
        consumer_kwargs: Dict[str, Any] = {
            "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group_id": "fluxot-mqtt-bridge",
            "auto_offset_reset": "latest",
            "enable_auto_commit": True,
            "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
        }
        _apply_kafka_security(consumer_kwargs)
        self._kafka_consumer = AIOKafkaConsumer(**consumer_kwargs)
        self._kafka_consumer.subscribe(pattern=r"fluxot\..+\..+\..+\.commands")
        await self._kafka_consumer.start()

        # MQTT client
        self._mqtt_client = mqtt.Client(
            client_id=BRIDGE_CLIENT_ID,
            clean_session=True,
            protocol=mqtt.MQTTv311,
        )
        self._mqtt_client.on_connect = self._on_mqtt_connect
        self._mqtt_client.on_message = self._on_mqtt_message
        self._mqtt_client.on_disconnect = self._on_mqtt_disconnect

        if settings.MQTT_USERNAME:
            self._mqtt_client.username_pw_set(
                settings.MQTT_USERNAME, settings.MQTT_PASSWORD
            )

        if settings.MQTT_USE_TLS:
            self._mqtt_client.tls_set()

        self._mqtt_client.connect_async(
            settings.MQTT_BROKER_HOST, settings.MQTT_BROKER_PORT, keepalive=60
        )
        self._mqtt_client.loop_start()

        # Start Kafka -> MQTT forwarding task
        self._consume_task = asyncio.create_task(self._forward_kafka_to_mqtt())

        logger.info(
            "MQTT-Kafka bridge started — MQTT=%s:%s, Kafka=%s",
            settings.MQTT_BROKER_HOST,
            settings.MQTT_BROKER_PORT,
            settings.KAFKA_BOOTSTRAP_SERVERS,
        )

    async def stop(self) -> None:
        """Gracefully disconnect from all brokers."""
        self._running = False

        if self._consume_task:
            self._consume_task.cancel()
            try:
                await self._consume_task
            except asyncio.CancelledError:
                pass

        if self._mqtt_client:
            self._mqtt_client.loop_stop()
            self._mqtt_client.disconnect()

        if self._kafka_consumer:
            await self._kafka_consumer.stop()

        if self._kafka_producer:
            await self._kafka_producer.stop()

        if self._redis:
            await self._redis.aclose()

        logger.info("MQTT-Kafka bridge stopped. Metrics: %s", self.metrics.to_dict())

    # ------------------------------------------------------------------
    # MQTT callbacks
    # ------------------------------------------------------------------

    def _on_mqtt_connect(
        self, client: mqtt.Client, userdata: Any, flags: dict, rc: int
    ) -> None:
        if rc == 0:
            logger.info("MQTT connected — subscribing to fluxot/#")
            client.subscribe("fluxot/#", qos=MQTT_QOS_AT_LEAST_ONCE)
        else:
            logger.error("MQTT connection refused — return code %d", rc)

    def _on_mqtt_disconnect(
        self, client: mqtt.Client, userdata: Any, rc: int
    ) -> None:
        if rc != 0:
            logger.warning("MQTT disconnected unexpectedly (rc=%d), reconnecting…", rc)

    def _on_mqtt_message(
        self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage
    ) -> None:
        """Called by paho in the MQTT thread — schedule async handling."""
        if self._loop and self._running:
            asyncio.run_coroutine_threadsafe(
                self._handle_mqtt_message(message.topic, message.payload),
                self._loop,
            )

    # ------------------------------------------------------------------
    # MQTT -> Kafka forwarding
    # ------------------------------------------------------------------

    async def _handle_mqtt_message(self, topic: str, raw: bytes) -> None:
        parsed = parse_mqtt_topic(topic)
        if parsed is None:
            logger.debug("Ignoring MQTT message with unexpected topic: %s", topic)
            return

        if parsed.message_type not in INBOUND_SUFFIXES:
            return

        # Validate JSON
        try:
            payload: Dict[str, Any] = json.loads(raw)
        except json.JSONDecodeError as exc:
            logger.warning("Invalid JSON on topic %s: %s", topic, exc)
            self.metrics.invalid_payloads += 1
            return

        # Enrich payload with routing metadata
        payload.setdefault("site_id", parsed.site_id)
        payload.setdefault("skid_id", parsed.skid_id)
        payload.setdefault("skid_type", parsed.skid_type)

        # Deduplication check
        dedup_key = _build_dedup_key(parsed.kafka_topic, payload)
        if await self._is_duplicate(dedup_key):
            self.metrics.deduplicated += 1
            return

        # Publish to Kafka
        try:
            message_key = f"{parsed.skid_id}:{int(time.time() * 1000)}"
            await self._kafka_producer.send_and_wait(
                parsed.kafka_topic,
                value=payload,
                key=message_key,
            )
            self.metrics.mqtt_to_kafka += 1
            logger.debug("MQTT->Kafka: %s -> %s", topic, parsed.kafka_topic)
        except Exception as exc:
            logger.error("Failed to forward MQTT->Kafka [%s]: %s", topic, exc)
            self.metrics.errors += 1

    # ------------------------------------------------------------------
    # Kafka -> MQTT forwarding (commands)
    # ------------------------------------------------------------------

    async def _forward_kafka_to_mqtt(self) -> None:
        """Consume command messages from Kafka and publish them to MQTT."""
        try:
            async for msg in self._kafka_consumer:
                if not self._running:
                    break

                parsed = parse_kafka_topic(msg.topic)
                if parsed is None or parsed.message_type not in OUTBOUND_SUFFIXES:
                    continue

                mqtt_topic = parsed.mqtt_topic
                payload_str = json.dumps(msg.value) if isinstance(msg.value, dict) else str(msg.value)

                if self._mqtt_client and self._mqtt_client.is_connected():
                    result = self._mqtt_client.publish(
                        mqtt_topic,
                        payload=payload_str.encode("utf-8"),
                        qos=MQTT_QOS_AT_LEAST_ONCE,
                        retain=False,
                    )
                    if result.rc == mqtt.MQTT_ERR_SUCCESS:
                        self.metrics.kafka_to_mqtt += 1
                        logger.debug(
                            "Kafka->MQTT: %s -> %s", msg.topic, mqtt_topic
                        )
                    else:
                        logger.error(
                            "MQTT publish failed (rc=%d) for topic %s",
                            result.rc,
                            mqtt_topic,
                        )
                        self.metrics.errors += 1
                else:
                    logger.warning(
                        "MQTT client not connected — dropping command for %s", mqtt_topic
                    )
                    self.metrics.errors += 1

        except asyncio.CancelledError:
            pass
        except Exception as exc:
            logger.error("Kafka->MQTT forwarding error: %s", exc)

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    async def _is_duplicate(self, key: str) -> bool:
        """Return True if this message hash was seen within DEDUP_WINDOW_S."""
        redis_key = f"{DEDUP_KEY_PREFIX}{key}"
        result = await self._redis.set(
            redis_key, "1", nx=True, ex=DEDUP_WINDOW_S
        )
        # set(..., nx=True) returns True if the key was newly created
        return result is None  # None means key already existed


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_dedup_key(kafka_topic: str, payload: Dict[str, Any]) -> str:
    """Build a short hash key for dedup based on topic + skid_id + timestamp."""
    dedup_str = (
        f"{kafka_topic}:"
        f"{payload.get('skid_id', '')}:"
        f"{payload.get('timestamp', payload.get('ts', ''))}"
    )
    return hashlib.sha256(dedup_str.encode()).hexdigest()[:32]


def _apply_kafka_security(kwargs: Dict[str, Any]) -> None:
    """Mutate a Kafka config dict to add security settings if configured."""
    if settings.KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
        kwargs.update(
            {
                "security_protocol": settings.KAFKA_SECURITY_PROTOCOL,
                "sasl_mechanism": settings.KAFKA_SASL_MECHANISM,
                "sasl_plain_username": settings.KAFKA_SASL_USERNAME,
                "sasl_plain_password": settings.KAFKA_SASL_PASSWORD,
            }
        )
        if settings.KAFKA_CA_CERT:
            kwargs["ssl_cafile"] = settings.KAFKA_CA_CERT


# ---------------------------------------------------------------------------
# Standalone entry point (run as a microservice)
# ---------------------------------------------------------------------------


async def main() -> None:
    import signal

    bridge = MQTTKafkaBridge()
    await bridge.start()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    logger.info("Bridge running — press Ctrl+C to stop.")
    await stop_event.wait()
    await bridge.stop()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
