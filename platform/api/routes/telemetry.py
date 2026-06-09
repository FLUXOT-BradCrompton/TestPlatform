"""
Telemetry routes — latest readings, historical queries, tag list, dashboard,
internal ingestion endpoint, and WebSocket real-time stream.
"""
from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import List, Optional

import redis.asyncio as aioredis
from fastapi import (
    APIRouter,
    Depends,
    Header,
    HTTPException,
    Query,
    WebSocket,
    WebSocketDisconnect,
    status,
)
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from platform.api.auth import User, get_current_user
from platform.api.config import settings
from platform.api.database import get_db
from platform.api.models import (
    AlertRecord,
    AlertSeverity,
    DashboardSummary,
    Skid,
    SkidHealthSummary,
    SkidStatus,
    Site,
    TelemetryRecord,
    TelemetryPointResponse,
    TelemetryResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/telemetry", tags=["Telemetry"])

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _require_internal_key(x_internal_key: Optional[str] = Header(default=None)) -> None:
    """Validate the X-Internal-Key header for internal-only endpoints."""
    if x_internal_key != settings.INTERNAL_API_KEY:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Invalid or missing X-Internal-Key",
        )


async def _get_redis() -> aioredis.Redis:
    """Return a Redis client instance (caller is responsible for closing)."""
    return aioredis.from_url(settings.REDIS_URL, decode_responses=True)


# ---------------------------------------------------------------------------
# Tag list
# ---------------------------------------------------------------------------


@router.get(
    "/sites/{site_id}/skids/{skid_id}/tags",
    response_model=List[str],
    summary="List distinct tag names for a skid",
)
async def list_tags(
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
) -> List[str]:
    await _assert_skid_belongs_to_site(db, site_id, skid_id)
    result = await db.execute(
        select(TelemetryRecord.tag_name)
        .where(TelemetryRecord.skid_id == skid_id)
        .distinct()
        .order_by(TelemetryRecord.tag_name)
    )
    return [row[0] for row in result.all()]


# ---------------------------------------------------------------------------
# Latest telemetry (one reading per tag)
# ---------------------------------------------------------------------------


@router.get(
    "/sites/{site_id}/skids/{skid_id}/latest",
    response_model=List[TelemetryPointResponse],
    summary="Return the latest reading for every tag of a skid",
)
async def get_latest_telemetry(
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
) -> List[TelemetryPointResponse]:
    await _assert_skid_belongs_to_site(db, site_id, skid_id)

    # Use a lateral / DISTINCT ON to get the most-recent row per tag
    stmt = (
        select(TelemetryRecord)
        .where(TelemetryRecord.skid_id == skid_id)
        .distinct(TelemetryRecord.tag_name)
        .order_by(TelemetryRecord.tag_name, TelemetryRecord.timestamp.desc())
    )
    result = await db.execute(stmt)
    rows = result.scalars().all()
    return [TelemetryPointResponse.model_validate(r) for r in rows]


# ---------------------------------------------------------------------------
# Historical telemetry (paginated)
# ---------------------------------------------------------------------------


@router.get(
    "/sites/{site_id}/skids/{skid_id}/history",
    response_model=TelemetryResponse,
    summary="Return paginated telemetry history for a skid",
)
async def get_telemetry_history(
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
    tag_name: Optional[str] = Query(None, description="Filter to a specific tag"),
    start_time: Optional[datetime] = Query(None, description="ISO-8601 start timestamp"),
    end_time: Optional[datetime] = Query(None, description="ISO-8601 end timestamp"),
    limit: int = Query(1000, ge=1, le=10000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
) -> TelemetryResponse:
    await _assert_skid_belongs_to_site(db, site_id, skid_id)

    filters = [TelemetryRecord.skid_id == skid_id]
    if tag_name:
        filters.append(TelemetryRecord.tag_name == tag_name)
    if start_time:
        filters.append(TelemetryRecord.timestamp >= start_time)
    if end_time:
        filters.append(TelemetryRecord.timestamp <= end_time)

    count_stmt = select(func.count()).select_from(TelemetryRecord).where(*filters)
    total: int = (await db.execute(count_stmt)).scalar_one()

    data_stmt = (
        select(TelemetryRecord)
        .where(*filters)
        .order_by(TelemetryRecord.timestamp.desc())
        .offset(offset)
        .limit(limit)
    )
    rows = (await db.execute(data_stmt)).scalars().all()

    return TelemetryResponse(
        items=[TelemetryPointResponse.model_validate(r) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


# ---------------------------------------------------------------------------
# Dashboard summary
# ---------------------------------------------------------------------------


@router.get(
    "/sites/{site_id}/dashboard",
    response_model=DashboardSummary,
    summary="Return aggregated health summary for a site",
)
async def get_dashboard(
    site_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
) -> DashboardSummary:
    site = await _get_site(db, site_id)

    # Fetch all skids for the site
    skids_result = await db.execute(select(Skid).where(Skid.site_id == site_id))
    skids = skids_result.scalars().all()

    # Build per-skid alarm counts in a single query
    skid_ids = [s.id for s in skids]
    alarm_counts: dict[uuid.UUID, int] = {}
    if skid_ids:
        ac_result = await db.execute(
            select(AlertRecord.skid_id, func.count(AlertRecord.id))
            .where(
                AlertRecord.skid_id.in_(skid_ids),
                AlertRecord.resolved_at.is_(None),
            )
            .group_by(AlertRecord.skid_id)
        )
        for skid_id_val, cnt in ac_result.all():
            alarm_counts[skid_id_val] = cnt

    # Severity-level counts
    severity_counts_result = await db.execute(
        select(AlertRecord.severity, func.count(AlertRecord.id))
        .where(
            AlertRecord.skid_id.in_(skid_ids) if skid_ids else text("FALSE"),
            AlertRecord.resolved_at.is_(None),
        )
        .group_by(AlertRecord.severity)
    )
    severity_map: dict[str, int] = {
        str(sev): cnt for sev, cnt in severity_counts_result.all()
    }

    skid_summaries = [
        SkidHealthSummary(
            skid_id=s.id,
            skid_name=s.skid_name,
            skid_type=s.skid_type,
            status=s.status,
            last_seen=s.last_seen,
            active_alarm_count=alarm_counts.get(s.id, 0),
        )
        for s in skids
    ]

    status_counter: dict[SkidStatus, int] = {st: 0 for st in SkidStatus}
    for s in skids:
        status_counter[s.status] += 1

    return DashboardSummary(
        site_id=site.id,
        site_name=site.name,
        total_skids=len(skids),
        skids_online=status_counter[SkidStatus.ONLINE],
        skids_offline=status_counter[SkidStatus.OFFLINE],
        skids_in_alarm=status_counter[SkidStatus.ALARM],
        skids_in_maintenance=status_counter[SkidStatus.MAINTENANCE],
        active_alarms_total=sum(alarm_counts.values()),
        critical_alarms=severity_map.get(AlertSeverity.CRITICAL.value, 0),
        warning_alarms=severity_map.get(AlertSeverity.WARNING.value, 0),
        skid_summaries=skid_summaries,
        generated_at=datetime.now(tz=timezone.utc),
    )


# ---------------------------------------------------------------------------
# Internal ingestion endpoint (used by Kafka consumer / bridge service)
# ---------------------------------------------------------------------------


@router.post(
    "/ingest",
    status_code=status.HTTP_201_CREATED,
    summary="Internal: write a batch of telemetry records",
    include_in_schema=False,
)
async def ingest_telemetry(
    payload: List[dict],
    db: AsyncSession = Depends(get_db),
    _key: None = Depends(_require_internal_key),
) -> dict:
    """Accept a list of raw telemetry dicts from the Kafka consumer."""
    records = []
    for item in payload:
        try:
            skid_id = uuid.UUID(item["skid_id"])
            record = TelemetryRecord(
                skid_id=skid_id,
                timestamp=datetime.fromisoformat(item["timestamp"]),
                tag_name=item["tag_name"],
                value_float=item.get("value_float"),
                value_bool=item.get("value_bool"),
                value_str=item.get("value_str"),
                quality=item.get("quality", "GOOD"),
                unit=item.get("unit"),
            )
            records.append(record)
        except (KeyError, ValueError) as exc:
            logger.warning("Skipping malformed telemetry item: %s — %s", item, exc)

    if records:
        db.add_all(records)

    return {"accepted": len(records), "rejected": len(payload) - len(records)}


# ---------------------------------------------------------------------------
# WebSocket real-time stream
# ---------------------------------------------------------------------------


@router.websocket("/ws/{site_id}/{skid_id}")
async def telemetry_websocket(
    websocket: WebSocket,
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
) -> None:
    """Stream live telemetry for a skid via Redis pub/sub."""
    await websocket.accept()

    redis_client = await _get_redis()
    channel = f"telemetry:{site_id}:{skid_id}"
    pubsub = redis_client.pubsub()
    await pubsub.subscribe(channel)

    logger.info("WebSocket client connected: channel=%s", channel)

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                data = message["data"]
                await websocket.send_text(data if isinstance(data, str) else data.decode())
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected: channel=%s", channel)
    except Exception as exc:
        logger.error("WebSocket error on channel %s: %s", channel, exc)
    finally:
        await pubsub.unsubscribe(channel)
        await pubsub.close()
        await redis_client.aclose()


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _get_site(db: AsyncSession, site_id: uuid.UUID) -> Site:
    result = await db.execute(select(Site).where(Site.id == site_id))
    site = result.scalar_one_or_none()
    if site is None:
        raise HTTPException(status_code=404, detail=f"Site {site_id} not found")
    return site


async def _assert_skid_belongs_to_site(
    db: AsyncSession, site_id: uuid.UUID, skid_id: uuid.UUID
) -> Skid:
    result = await db.execute(
        select(Skid).where(Skid.id == skid_id, Skid.site_id == site_id)
    )
    skid = result.scalar_one_or_none()
    if skid is None:
        raise HTTPException(
            status_code=404,
            detail=f"Skid {skid_id} not found in site {site_id}",
        )
    return skid
