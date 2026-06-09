"""
Async Kafka producer for the Flux OT Platform API.

Publishes commands and writeback requests to the appropriate edge skid topics.
Uses idempotent producer settings to guarantee exactly-once delivery ordering.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError

from platform.api.config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Topic name builder
# ---------------------------------------------------------------------------


def _topic(site_id: str, skid_type: str, skid_id: str, message_type: str) -> str:
    """Construct a canonical Flux OT Kafka topic name."""
    return f"fluxot.{site_id}.{skid_type}.{skid_id}.{message_type}"


# ---------------------------------------------------------------------------
# Producer class
# ---------------------------------------------------------------------------


class FluxOTKafkaProducer:
    """Idempotent Kafka producer with automatic retry and serialisation."""

    def __init__(self) -> None:
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self) -> None:
        """Connect to Kafka brokers."""
        producer_kwargs: Dict[str, Any] = {
            "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "enable_idempotence": True,
            "acks": "all",
            "retries": 5,
            "retry_backoff_ms": 300,
            "max_in_flight_requests_per_connection": 5,
            "compression_type": "snappy",
            "value_serializer": lambda v: json.dumps(v).encode("utf-8"),
            "key_serializer": lambda k: k.encode("utf-8") if k else None,
            "request_timeout_ms": 30_000,
            "metadata_max_age_ms": 60_000,
        }

        if settings.KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
            producer_kwargs.update(
                {
                    "security_protocol": settings.KAFKA_SECURITY_PROTOCOL,
                    "sasl_mechanism": settings.KAFKA_SASL_MECHANISM,
                    "sasl_plain_username": settings.KAFKA_SASL_USERNAME,
                    "sasl_plain_password": settings.KAFKA_SASL_PASSWORD,
                }
            )
            if settings.KAFKA_CA_CERT:
                producer_kwargs["ssl_cafile"] = settings.KAFKA_CA_CERT

        self._producer = AIOKafkaProducer(**producer_kwargs)
        await self._producer.start()
        logger.info(
            "Kafka producer started — brokers=%s", settings.KAFKA_BOOTSTRAP_SERVERS
        )

    async def stop(self) -> None:
        """Flush pending messages and disconnect."""
        if self._producer:
            await self._producer.stop()
            logger.info("Kafka producer stopped.")

    # ------------------------------------------------------------------
    # Public send helpers
    # ------------------------------------------------------------------

    async def send_command(
        self,
        site_id: str,
        skid_id: str,
        skid_type: str,
        command: Dict[str, Any],
    ) -> None:
        """Publish a command message to the edge skid command topic.

        Topic: ``fluxot.{site_id}.{skid_type}.{skid_id}.commands``
        """
        topic = _topic(site_id, skid_type, skid_id, "commands")
        key = str(command.get("command_id", uuid.uuid4()))
        payload = {
            **command,
            "schema_version": "1.0",
            "platform_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }
        await self._send(topic, key, payload)
        logger.debug("Published command to %s: %s", topic, key)

    async def send_writeback(
        self,
        site_id: str,
        skid_id: str,
        skid_type: str,
        writeback: Dict[str, Any],
    ) -> None:
        """Publish a writeback request to the edge skid writeback topic.

        Topic: ``fluxot.{site_id}.{skid_type}.{skid_id}.writeback``
        """
        topic = _topic(site_id, skid_type, skid_id, "writeback")
        key = str(writeback.get("audit_id", uuid.uuid4()))
        payload = {
            **writeback,
            "schema_version": "1.0",
            "platform_timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }
        await self._send(topic, key, payload)
        logger.debug("Published writeback to %s: %s", topic, key)

    async def send_ai_prediction_request(
        self,
        site_id: str,
        skid_id: str,
        skid_type: str,
        prediction_request: Dict[str, Any],
    ) -> None:
        """Publish a prediction request for the AI service."""
        topic = _topic(site_id, skid_type, skid_id, "ai-predictions")
        key = str(prediction_request.get("request_id", uuid.uuid4()))
        await self._send(topic, key, prediction_request)

    # ------------------------------------------------------------------
    # Low-level send with retry logic
    # ------------------------------------------------------------------

    async def _send(
        self,
        topic: str,
        key: str,
        value: Dict[str, Any],
        partition: Optional[int] = None,
    ) -> None:
        """Send a message, raising KafkaError after exhausted retries."""
        if not self._producer:
            raise RuntimeError("Producer not started — call start() first")

        try:
            await self._producer.send_and_wait(
                topic,
                value=value,
                key=key,
                partition=partition,
            )
        except KafkaError as exc:
            logger.error("Failed to send message to %s: %s", topic, exc)
            raise


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_producer_instance: Optional[FluxOTKafkaProducer] = None


async def get_producer() -> FluxOTKafkaProducer:
    """Return the shared producer instance (must be started before use)."""
    global _producer_instance
    if _producer_instance is None:
        _producer_instance = FluxOTKafkaProducer()
    return _producer_instance


async def start_producer() -> None:
    global _producer_instance
    producer = await get_producer()
    await producer.start()


async def stop_producer() -> None:
    global _producer_instance
    if _producer_instance:
        await _producer_instance.stop()
        _producer_instance = None
