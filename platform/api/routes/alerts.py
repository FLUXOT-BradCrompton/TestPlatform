"""
Alert management routes — active alerts, history, acknowledge, resolve.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from platform.api.auth import User, get_current_user, require_operator
from platform.api.database import get_db
from platform.api.models import (
    AlertAcknowledge,
    AlertRecord,
    AlertResponse,
    AlertSeverity,
    Skid,
    Site,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/alerts", tags=["Alerts"])

# ---------------------------------------------------------------------------
# Active alerts for a site
# ---------------------------------------------------------------------------


@router.get(
    "/sites/{site_id}/active",
    response_model=List[AlertResponse],
    summary="Return all unresolved alerts for a site",
)
async def get_active_alerts(
    site_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
) -> List[AlertResponse]:
    await _assert_site_exists(db, site_id)

    skid_ids = await _get_site_skid_ids(db, site_id)
    if not skid_ids:
        return []

    result = await db.execute(
        select(AlertRecord)
        .where(
            AlertRecord.skid_id.in_(skid_ids),
            AlertRecord.resolved_at.is_(None),
        )
        .order_by(AlertRecord.timestamp.desc())
    )
    return [AlertResponse.model_validate(r) for r in result.scalars().all()]


# ---------------------------------------------------------------------------
# Alert history for a site (paginated + filtered)
# ---------------------------------------------------------------------------


@router.get(
    "/sites/{site_id}/history",
    response_model=dict,
    summary="Return paginated alert history for a site",
)
async def get_alert_history(
    site_id: uuid.UUID,
    severity: Optional[AlertSeverity] = Query(None),
    skid_id: Optional[uuid.UUID] = Query(None),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
) -> dict:
    await _assert_site_exists(db, site_id)

    skid_ids = await _get_site_skid_ids(db, site_id)
    if not skid_ids:
        return {"items": [], "total": 0, "limit": limit, "offset": offset}

    filters = [AlertRecord.skid_id.in_(skid_ids)]
    if severity:
        filters.append(AlertRecord.severity == severity)
    if skid_id:
        if skid_id not in skid_ids:
            raise HTTPException(status_code=404, detail="Skid not found in this site")
        filters.append(AlertRecord.skid_id == skid_id)
    if start_time:
        filters.append(AlertRecord.timestamp >= start_time)
    if end_time:
        filters.append(AlertRecord.timestamp <= end_time)

    total: int = (
        await db.execute(
            select(func.count()).select_from(AlertRecord).where(*filters)
        )
    ).scalar_one()

    rows = (
        await db.execute(
            select(AlertRecord)
            .where(*filters)
            .order_by(AlertRecord.timestamp.desc())
            .offset(offset)
            .limit(limit)
        )
    ).scalars().all()

    return {
        "items": [AlertResponse.model_validate(r) for r in rows],
        "total": total,
        "limit": limit,
        "offset": offset,
    }


# ---------------------------------------------------------------------------
# Alerts for a specific skid
# ---------------------------------------------------------------------------


@router.get(
    "/sites/{site_id}/skids/{skid_id}/alerts",
    response_model=List[AlertResponse],
    summary="Return alerts for a specific skid",
)
async def get_skid_alerts(
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
    resolved: Optional[bool] = Query(None, description="Filter by resolved status"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
) -> List[AlertResponse]:
    await _assert_skid_in_site(db, site_id, skid_id)

    filters = [AlertRecord.skid_id == skid_id]
    if resolved is not None:
        if resolved:
            filters.append(AlertRecord.resolved_at.is_not(None))
        else:
            filters.append(AlertRecord.resolved_at.is_(None))

    result = await db.execute(
        select(AlertRecord)
        .where(*filters)
        .order_by(AlertRecord.timestamp.desc())
        .offset(offset)
        .limit(limit)
    )
    return [AlertResponse.model_validate(r) for r in result.scalars().all()]


# ---------------------------------------------------------------------------
# Acknowledge alert
# ---------------------------------------------------------------------------


@router.post(
    "/sites/{site_id}/alerts/{alert_id}/acknowledge",
    response_model=AlertResponse,
    summary="Acknowledge an alert",
)
async def acknowledge_alert(
    site_id: uuid.UUID,
    alert_id: uuid.UUID,
    body: AlertAcknowledge,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_operator),
) -> AlertResponse:
    alert = await _get_alert_in_site(db, site_id, alert_id)

    if alert.acknowledged:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Alert is already acknowledged",
        )

    alert.acknowledged = True
    alert.acknowledged_by = body.operator
    alert.acknowledged_at = datetime.now(tz=timezone.utc)

    # Append note to value_json if provided
    if body.note:
        val = dict(alert.value_json or {})
        val["acknowledge_note"] = body.note
        alert.value_json = val

    db.add(alert)
    return AlertResponse.model_validate(alert)


# ---------------------------------------------------------------------------
# Resolve alert
# ---------------------------------------------------------------------------


@router.post(
    "/sites/{site_id}/alerts/{alert_id}/resolve",
    response_model=AlertResponse,
    summary="Resolve (close) an alert",
)
async def resolve_alert(
    site_id: uuid.UUID,
    alert_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_operator),
) -> AlertResponse:
    alert = await _get_alert_in_site(db, site_id, alert_id)

    if alert.resolved_at is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Alert is already resolved",
        )

    alert.resolved_at = datetime.now(tz=timezone.utc)
    # Auto-acknowledge if not already done
    if not alert.acknowledged:
        alert.acknowledged = True
        alert.acknowledged_by = _user.username
        alert.acknowledged_at = datetime.now(tz=timezone.utc)

    db.add(alert)
    return AlertResponse.model_validate(alert)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


async def _assert_site_exists(db: AsyncSession, site_id: uuid.UUID) -> Site:
    result = await db.execute(select(Site).where(Site.id == site_id))
    site = result.scalar_one_or_none()
    if site is None:
        raise HTTPException(status_code=404, detail=f"Site {site_id} not found")
    return site


async def _get_site_skid_ids(
    db: AsyncSession, site_id: uuid.UUID
) -> list[uuid.UUID]:
    result = await db.execute(
        select(Skid.id).where(Skid.site_id == site_id)
    )
    return [row[0] for row in result.all()]


async def _assert_skid_in_site(
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


async def _get_alert_in_site(
    db: AsyncSession, site_id: uuid.UUID, alert_id: uuid.UUID
) -> AlertRecord:
    """Return an AlertRecord that belongs to one of the site's skids."""
    skid_ids = await _get_site_skid_ids(db, site_id)
    result = await db.execute(
        select(AlertRecord).where(
            AlertRecord.id == alert_id,
            AlertRecord.skid_id.in_(skid_ids),
        )
    )
    alert = result.scalar_one_or_none()
    if alert is None:
        raise HTTPException(
            status_code=404,
            detail=f"Alert {alert_id} not found for site {site_id}",
        )
    return alert
