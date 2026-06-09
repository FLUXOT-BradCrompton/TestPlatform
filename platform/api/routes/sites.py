"""
Site and skid management routes — CRUD operations and health summaries.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from platform.api.auth import User, get_current_user, require_admin, require_operator
from platform.api.database import get_db
from platform.api.models import (
    AlertRecord,
    AlertSeverity,
    DashboardSummary,
    Skid,
    SkidCreate,
    SkidHealthSummary,
    SkidResponse,
    SkidStatus,
    SkidStatusUpdate,
    Site,
    SiteCreate,
    SiteResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/sites", tags=["Sites & Skids"])

# ---------------------------------------------------------------------------
# Site CRUD
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=List[SiteResponse],
    summary="List all sites",
)
async def list_sites(
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
) -> List[SiteResponse]:
    result = await db.execute(select(Site).order_by(Site.name))
    return [SiteResponse.model_validate(s) for s in result.scalars().all()]


@router.post(
    "",
    response_model=SiteResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new site (admin only)",
)
async def create_site(
    body: SiteCreate,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_admin),
) -> SiteResponse:
    # Check uniqueness
    existing = await db.execute(select(Site).where(Site.name == body.name))
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Site with name '{body.name}' already exists",
        )

    site = Site(
        id=uuid.uuid4(),
        name=body.name,
        location=body.location,
        timezone=body.timezone,
    )
    db.add(site)
    await db.flush()
    logger.info("Created site: %s (%s)", site.name, site.id)
    return SiteResponse.model_validate(site)


@router.get(
    "/{site_id}",
    response_model=dict,
    summary="Get a site with its skids",
)
async def get_site(
    site_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
) -> dict:
    result = await db.execute(
        select(Site)
        .options(selectinload(Site.skids))
        .where(Site.id == site_id)
    )
    site = result.scalar_one_or_none()
    if site is None:
        raise HTTPException(status_code=404, detail=f"Site {site_id} not found")

    return {
        **SiteResponse.model_validate(site).model_dump(),
        "skids": [SkidResponse.model_validate(sk) for sk in site.skids],
    }


# ---------------------------------------------------------------------------
# Skid CRUD
# ---------------------------------------------------------------------------


@router.get(
    "/{site_id}/skids",
    response_model=List[SkidResponse],
    summary="List all skids for a site",
)
async def list_skids(
    site_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
) -> List[SkidResponse]:
    await _assert_site_exists(db, site_id)
    result = await db.execute(
        select(Skid).where(Skid.site_id == site_id).order_by(Skid.skid_name)
    )
    return [SkidResponse.model_validate(sk) for sk in result.scalars().all()]


@router.post(
    "/{site_id}/skids",
    response_model=SkidResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Register a new skid at a site",
)
async def register_skid(
    site_id: uuid.UUID,
    body: SkidCreate,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_operator),
) -> SkidResponse:
    await _assert_site_exists(db, site_id)

    # Prevent duplicate skid names within the same site
    existing = await db.execute(
        select(Skid).where(Skid.site_id == site_id, Skid.skid_name == body.skid_name)
    )
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Skid '{body.skid_name}' already exists in this site",
        )

    skid = Skid(
        id=uuid.uuid4(),
        site_id=site_id,
        skid_type=body.skid_type,
        skid_name=body.skid_name,
        firmware_version=body.firmware_version,
        status=SkidStatus.OFFLINE,
        config_json=body.config_json,
    )
    db.add(skid)
    await db.flush()
    logger.info("Registered skid: %s (%s) at site %s", skid.skid_name, skid.id, site_id)
    return SkidResponse.model_validate(skid)


@router.put(
    "/{site_id}/skids/{skid_id}",
    response_model=SkidResponse,
    summary="Update skid configuration or status",
)
async def update_skid(
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
    body: SkidCreate,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_operator),
) -> SkidResponse:
    skid = await _assert_skid_in_site(db, site_id, skid_id)

    skid.skid_type = body.skid_type
    skid.skid_name = body.skid_name
    if body.firmware_version is not None:
        skid.firmware_version = body.firmware_version
    if body.config_json is not None:
        skid.config_json = body.config_json

    db.add(skid)
    return SkidResponse.model_validate(skid)


@router.patch(
    "/{site_id}/skids/{skid_id}/status",
    response_model=SkidResponse,
    summary="Update skid operational status",
)
async def update_skid_status(
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
    body: SkidStatusUpdate,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_operator),
) -> SkidResponse:
    skid = await _assert_skid_in_site(db, site_id, skid_id)
    skid.status = body.status
    db.add(skid)
    return SkidResponse.model_validate(skid)


# ---------------------------------------------------------------------------
# Site health summary
# ---------------------------------------------------------------------------


@router.get(
    "/{site_id}/health",
    response_model=DashboardSummary,
    summary="Overall health summary for a site",
)
async def get_site_health(
    site_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(get_current_user),
) -> DashboardSummary:
    site = await _assert_site_exists(db, site_id)

    skid_result = await db.execute(select(Skid).where(Skid.site_id == site_id))
    skids = skid_result.scalars().all()
    skid_ids = [s.id for s in skids]

    # Active alarm counts per skid
    alarm_counts: dict[uuid.UUID, int] = {}
    severity_map: dict[str, int] = {}

    if skid_ids:
        ac_result = await db.execute(
            select(AlertRecord.skid_id, func.count(AlertRecord.id))
            .where(
                AlertRecord.skid_id.in_(skid_ids),
                AlertRecord.resolved_at.is_(None),
            )
            .group_by(AlertRecord.skid_id)
        )
        for sid, cnt in ac_result.all():
            alarm_counts[sid] = cnt

        sev_result = await db.execute(
            select(AlertRecord.severity, func.count(AlertRecord.id))
            .where(
                AlertRecord.skid_id.in_(skid_ids),
                AlertRecord.resolved_at.is_(None),
            )
            .group_by(AlertRecord.severity)
        )
        for sev, cnt in sev_result.all():
            severity_map[str(sev)] = cnt

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
# Shared helpers
# ---------------------------------------------------------------------------


async def _assert_site_exists(db: AsyncSession, site_id: uuid.UUID) -> Site:
    result = await db.execute(select(Site).where(Site.id == site_id))
    site = result.scalar_one_or_none()
    if site is None:
        raise HTTPException(status_code=404, detail=f"Site {site_id} not found")
    return site


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
