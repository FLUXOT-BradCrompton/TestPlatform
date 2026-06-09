"""
Async Kafka consumer for the Flux OT Platform API.

Subscribes to all fluxot.* topics and routes messages to:
  - TimescaleDB (telemetry)
  - PostgreSQL (alerts, heartbeat, writeback)
  - Redis pub/sub (real-time WebSocket fan-out)

Uses batch buffering: up to 100 messages or 500 ms before a bulk insert.
Failed messages are routed to the dead letter queue topic.
"""
from __future__ import annotations

import asyncio
import json
import logging
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from aiokafka import AIOKafkaConsumer, ConsumerRecord
from aiokafka.errors import KafkaError
import redis.asyncio as aioredis

from platform.api.config import settings
from platform.api.database import AsyncSessionLocal

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CONSUMER_GROUP = "fluxot-platform-api"
TOPIC_PATTERN = re.compile(r"^fluxot\.")
DLQ_TOPIC = "fluxot.dlq"
BATCH_SIZE = 100
BATCH_TIMEOUT_S = 0.5

# ---------------------------------------------------------------------------
# Internal data structures
# ---------------------------------------------------------------------------


@dataclass
class _MessageBuffer:
    telemetry: List[Dict[str, Any]] = field(default_factory=list)
    alerts: List[Dict[str, Any]] = field(default_factory=list)
    heartbeats: List[Dict[str, Any]] = field(default_factory=list)
    writebacks: List[Dict[str, Any]] = field(default_factory=list)

    def total(self) -> int:
        return (
            len(self.telemetry)
            + len(self.alerts)
            + len(self.heartbeats)
            + len(self.writebacks)
        )

    def clear(self) -> None:
        self.telemetry.clear()
        self.alerts.clear()
        self.heartbeats.clear()
        self.writebacks.clear()


# ---------------------------------------------------------------------------
# Consumer class
# ---------------------------------------------------------------------------


class FluxOTKafkaConsumer:
    """Async Kafka consumer that processes all Flux OT platform topics."""

    def __init__(self) -> None:
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._dlq_producer = None  # Lazy init
        self._redis: Optional[aioredis.Redis] = None
        self._buffer = _MessageBuffer()
        self._running = False
        self._flush_task: Optional[asyncio.Task] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Create the consumer, connect to Kafka and Redis, then begin consuming."""
        consumer_kwargs: Dict[str, Any] = {
            "bootstrap_servers": settings.KAFKA_BOOTSTRAP_SERVERS,
            "group_id": CONSUMER_GROUP,
            "auto_offset_reset": "latest",
            "enable_auto_commit": False,
            "max_poll_records": 200,
            "session_timeout_ms": 30_000,
            "heartbeat_interval_ms": 10_000,
            "value_deserializer": lambda v: json.loads(v.decode("utf-8")),
            "key_deserializer": lambda k: k.decode("utf-8") if k else None,
        }

        if settings.KAFKA_SECURITY_PROTOCOL != "PLAINTEXT":
            consumer_kwargs.update(
                {
                    "security_protocol": settings.KAFKA_SECURITY_PROTOCOL,
                    "sasl_mechanism": settings.KAFKA_SASL_MECHANISM,
                    "sasl_plain_username": settings.KAFKA_SASL_USERNAME,
                    "sasl_plain_password": settings.KAFKA_SASL_PASSWORD,
                }
            )
            if settings.KAFKA_CA_CERT:
                consumer_kwargs["ssl_cafile"] = settings.KAFKA_CA_CERT

        self._consumer = AIOKafkaConsumer(**consumer_kwargs)

        # Subscribe using regex to catch all dynamically-named site/skid topics
        self._consumer.subscribe(
            pattern=(
                r"fluxot\..+\..+\..+\.(telemetry|alerts|heartbeat|writeback)"
            )
        )

        await self._consumer.start()

        self._redis = aioredis.from_url(settings.REDIS_URL, decode_responses=True)
        self._running = True

        # Start a background timer to flush the buffer every BATCH_TIMEOUT_S
        self._flush_task = asyncio.create_task(self._periodic_flush())

        logger.info(
            "Kafka consumer started — group=%s, brokers=%s",
            CONSUMER_GROUP,
            settings.KAFKA_BOOTSTRAP_SERVERS,
        )

    async def stop(self) -> None:
        """Gracefully drain and shut down the consumer."""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass

        # Final flush before shutdown
        try:
            await self._flush_buffer()
        except Exception as exc:
            logger.error("Error during final buffer flush: %s", exc)

        if self._consumer:
            await self._consumer.stop()
        if self._redis:
            await self._redis.aclose()

        logger.info("Kafka consumer stopped.")

    # ------------------------------------------------------------------
    # Main consume loop
    # ------------------------------------------------------------------

    async def consume(self) -> None:
        """Main loop — call this as a background task from the FastAPI lifespan."""
        if not self._consumer:
            raise RuntimeError("Consumer not started — call start() first")

        try:
            async for msg in self._consumer:
                await self._route_message(msg)

                if self._buffer.total() >= BATCH_SIZE:
                    await self._flush_buffer()
                    await self._consumer.commit()
        except asyncio.CancelledError:
            pass
        except KafkaError as exc:
            logger.error("Kafka consumer error: %s", exc)
        finally:
            await self.stop()

    # ------------------------------------------------------------------
    # Message routing
    # ------------------------------------------------------------------

    async def _route_message(self, msg: ConsumerRecord) -> None:
        """Parse topic suffix and route message to the appropriate buffer."""
        topic: str = msg.topic
        payload: Dict[str, Any] = msg.value

        try:
            # Topic format: fluxot.{site_id}.{skid_type}.{skid_id}.{message_type}
            parts = topic.split(".")
            if len(parts) < 5:
                raise ValueError(f"Unexpected topic format: {topic}")

            message_type = parts[-1]

            if message_type == "telemetry":
                self._buffer.telemetry.append(
                    {**payload, "_topic": topic, "_offset": msg.offset}
                )
            elif message_type == "alerts":
                self._buffer.alerts.append(
                    {**payload, "_topic": topic, "_offset": msg.offset}
                )
            elif message_type == "heartbeat":
                self._buffer.heartbeats.append(
                    {**payload, "_topic": topic, "_offset": msg.offset}
                )
            elif message_type == "writeback":
                self._buffer.writebacks.append(
                    {**payload, "_topic": topic, "_offset": msg.offset}
                )
            else:
                logger.debug("Ignoring unhandled message type: %s", message_type)

        except Exception as exc:
            logger.error(
                "Failed to route message from topic %s offset %s: %s",
                topic,
                msg.offset,
                exc,
            )
            await self._send_to_dlq(msg, str(exc))

    # ------------------------------------------------------------------
    # Buffer flushing
    # ------------------------------------------------------------------

    async def _periodic_flush(self) -> None:
        """Wake up every BATCH_TIMEOUT_S and flush whatever is buffered."""
        while self._running:
            await asyncio.sleep(BATCH_TIMEOUT_S)
            if self._buffer.total() > 0:
                try:
                    await self._flush_buffer()
                    if self._consumer:
                        await self._consumer.commit()
                except Exception as exc:
                    logger.error("Periodic flush error: %s", exc)

    async def _flush_buffer(self) -> None:
        """Write all buffered messages to the database and Redis."""
        if self._buffer.total() == 0:
            return

        async with AsyncSessionLocal() as session:
            try:
                if self._buffer.telemetry:
                    await self._persist_telemetry(session, self._buffer.telemetry)
                if self._buffer.alerts:
                    await self._persist_alerts(session, self._buffer.alerts)
                if self._buffer.heartbeats:
                    await self._persist_heartbeats(session, self._buffer.heartbeats)
                if self._buffer.writebacks:
                    await self._persist_writebacks(session, self._buffer.writebacks)

                await session.commit()
            except Exception as exc:
                await session.rollback()
                logger.error("Buffer flush DB error: %s", exc)
                raise

        self._buffer.clear()

    # ------------------------------------------------------------------
    # Persistence handlers
    # ------------------------------------------------------------------

    async def _persist_telemetry(
        self, session, records: List[Dict[str, Any]]
    ) -> None:
        from platform.api.models import TelemetryRecord  # noqa: PLC0415

        orm_records = []
        redis_publishes = []

        for item in records:
            try:
                skid_id = uuid.UUID(str(item["skid_id"]))
                ts = _parse_timestamp(item["timestamp"])

                record = TelemetryRecord(
                    skid_id=skid_id,
                    timestamp=ts,
                    tag_name=str(item["tag_name"]),
                    value_float=item.get("value_float"),
                    value_bool=item.get("value_bool"),
                    value_str=item.get("value_str"),
                    quality=item.get("quality", "GOOD"),
                    unit=item.get("unit"),
                )
                orm_records.append(record)

                # Prepare Redis publish payload
                site_id = item.get("site_id", "unknown")
                redis_publishes.append(
                    (
                        f"telemetry:{site_id}:{skid_id}",
                        json.dumps(
                            {
                                "skid_id": str(skid_id),
                                "timestamp": ts.isoformat(),
                                "tag_name": item["tag_name"],
                                "value_float": item.get("value_float"),
                                "value_bool": item.get("value_bool"),
                                "value_str": item.get("value_str"),
                                "quality": item.get("quality", "GOOD"),
                                "unit": item.get("unit"),
                            }
                        ),
                    )
                )
            except (KeyError, ValueError) as exc:
                logger.warning("Malformed telemetry record, skipping: %s", exc)

        if orm_records:
            session.add_all(orm_records)

        # Publish to Redis for WebSocket fan-out
        if self._redis and redis_publishes:
            async with self._redis.pipeline(transaction=False) as pipe:
                for channel, message in redis_publishes:
                    pipe.publish(channel, message)
                await pipe.execute()

    async def _persist_alerts(
        self, session, records: List[Dict[str, Any]]
    ) -> None:
        from platform.api.models import AlertRecord, AlertSeverity  # noqa: PLC0415

        for item in records:
            try:
                skid_id = uuid.UUID(str(item["skid_id"]))
                alert = AlertRecord(
                    id=uuid.uuid4(),
                    skid_id=skid_id,
                    timestamp=_parse_timestamp(item["timestamp"]),
                    alert_id=str(item["alert_id"]),
                    severity=AlertSeverity(item.get("severity", "ALARM")),
                    message=str(item.get("message", "")),
                    tag_name=item.get("tag_name"),
                    value_json=item.get("value_json"),
                    acknowledged=False,
                )
                session.add(alert)

                # Publish alert to Redis
                if self._redis:
                    site_id = item.get("site_id", "unknown")
                    await self._redis.publish(
                        f"alerts:{site_id}:{skid_id}",
                        json.dumps(
                            {
                                "alert_id": str(alert.id),
                                "severity": item.get("severity"),
                                "message": alert.message,
                                "timestamp": alert.timestamp.isoformat(),
                            }
                        ),
                    )
            except (KeyError, ValueError) as exc:
                logger.warning("Malformed alert record, skipping: %s", exc)

    async def _persist_heartbeats(
        self, session, records: List[Dict[str, Any]]
    ) -> None:
        from sqlalchemy import update  # noqa: PLC0415
        from platform.api.models import Skid, SkidStatus  # noqa: PLC0415

        for item in records:
            try:
                skid_id = uuid.UUID(str(item["skid_id"]))
                reported_status = item.get("status", "ONLINE")
                # Validate status value
                try:
                    skid_status = SkidStatus(reported_status)
                except ValueError:
                    skid_status = SkidStatus.ONLINE

                await session.execute(
                    update(Skid)
                    .where(Skid.id == skid_id)
                    .values(
                        last_seen=_parse_timestamp(item["timestamp"]),
                        status=skid_status,
                        firmware_version=item.get("firmware_version"),
                    )
                )
            except (KeyError, ValueError) as exc:
                logger.warning("Malformed heartbeat record, skipping: %s", exc)

    async def _persist_writebacks(
        self, session, records: List[Dict[str, Any]]
    ) -> None:
        from platform.api.models import WritebackAudit  # noqa: PLC0415

        for item in records:
            try:
                skid_id = uuid.UUID(str(item["skid_id"]))
                command_id_raw = item.get("command_id")
                command_id = uuid.UUID(str(command_id_raw)) if command_id_raw else None

                audit = WritebackAudit(
                    id=uuid.uuid4(),
                    skid_id=skid_id,
                    timestamp=_parse_timestamp(item["timestamp"]),
                    command_id=command_id,
                    target_protocol=str(item.get("target_protocol", "UNKNOWN")),
                    target_node=str(item.get("target_node", "")),
                    value_json=item.get("value", {}),
                    confirmed=bool(item.get("confirmed", False)),
                    operator=item.get("operator"),
                )
                session.add(audit)
            except (KeyError, ValueError) as exc:
                logger.warning("Malformed writeback record, skipping: %s", exc)

    # ------------------------------------------------------------------
    # Dead letter queue
    # ------------------------------------------------------------------

    async def _send_to_dlq(self, msg: ConsumerRecord, error: str) -> None:
        """Send a failed message to the dead letter queue topic."""
        try:
            if self._dlq_producer is None:
                from platform.api.kafka_producer import FluxOTKafkaProducer  # noqa: PLC0415

                self._dlq_producer = FluxOTKafkaProducer()
                await self._dlq_producer.start()

            dlq_payload = {
                "original_topic": msg.topic,
                "original_offset": msg.offset,
                "original_partition": msg.partition,
                "error": error,
                "failed_at": datetime.now(tz=timezone.utc).isoformat(),
                "payload": msg.value,
            }
            await self._dlq_producer._producer.send_and_wait(
                DLQ_TOPIC,
                value=json.dumps(dlq_payload).encode(),
                key=msg.key,
            )
        except Exception as dlq_exc:
            logger.error("Failed to write to DLQ: %s", dlq_exc)


# ---------------------------------------------------------------------------
# Singleton accessor used by main.py
# ---------------------------------------------------------------------------

_consumer_instance: Optional[FluxOTKafkaConsumer] = None
_consume_task: Optional[asyncio.Task] = None


async def get_consumer() -> FluxOTKafkaConsumer:
    global _consumer_instance
    if _consumer_instance is None:
        _consumer_instance = FluxOTKafkaConsumer()
    return _consumer_instance


async def start_consumer() -> None:
    global _consumer_instance, _consume_task
    consumer = await get_consumer()
    await consumer.start()
    _consume_task = asyncio.create_task(consumer.consume())
    logger.info("Kafka consumer background task started.")


async def stop_consumer() -> None:
    global _consumer_instance, _consume_task
    if _consume_task:
        _consume_task.cancel()
        try:
            await _consume_task
        except asyncio.CancelledError:
            pass
    if _consumer_instance:
        await _consumer_instance.stop()
        _consumer_instance = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_timestamp(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    return datetime.fromisoformat(str(value)).replace(tzinfo=timezone.utc)
