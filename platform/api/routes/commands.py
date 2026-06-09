"""
Command and writeback routes — issue commands to edge skids, track status,
acknowledge results, and perform direct writeback actions.
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from platform.api.auth import User, require_operator
from platform.api.config import settings
from platform.api.database import get_db
from platform.api.models import (
    CommandCreate,
    CommandRecord,
    CommandResponse,
    CommandStatus,
    Skid,
    SkidType,
    Site,
    WritebackAudit,
    WritebackCreate,
    WritebackResponse,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1/commands", tags=["Commands & Writeback"])

# ---------------------------------------------------------------------------
# Valid command sets per skid type
# ---------------------------------------------------------------------------

_BELT_RIP_COMMANDS = {
    "SET_SPEED",
    "EMERGENCY_STOP",
    "RESUME",
    "ACK_ALARM",
    "MAINTENANCE_MODE",
}

_ROAD_CROSSING_COMMANDS = {
    "MANUAL_OPEN",
    "MANUAL_CLOSE",
    "EMERGENCY_STOP",
    "RESET_EMERGENCY",
    "SET_MODE",
}

_GENERIC_COMMANDS = {
    "RESET",
    "REBOOT",
    "SET_PARAM",
    "DIAGNOSTIC",
}

_VALID_COMMANDS: dict[SkidType, set[str]] = {
    SkidType.BELT_RIP_MONITOR: _BELT_RIP_COMMANDS,
    SkidType.ROAD_CROSSING: _ROAD_CROSSING_COMMANDS,
    SkidType.GENERIC: _GENERIC_COMMANDS,
}


def _validate_command_type(skid: Skid, command_type: str) -> None:
    allowed = _VALID_COMMANDS.get(skid.skid_type, set())
    if command_type not in allowed:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=(
                f"Command '{command_type}' is not valid for skid type "
                f"'{skid.skid_type.value}'. "
                f"Allowed commands: {sorted(allowed)}"
            ),
        )


# ---------------------------------------------------------------------------
# Issue a command
# ---------------------------------------------------------------------------


@router.post(
    "/sites/{site_id}/skids/{skid_id}/commands",
    response_model=CommandResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Issue a command to a skid",
)
async def create_command(
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
    body: CommandCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_operator),
) -> CommandResponse:
    skid = await _assert_skid_in_site(db, site_id, skid_id)
    _validate_command_type(skid, body.command_type)

    now = datetime.now(tz=timezone.utc)
    command = CommandRecord(
        id=uuid.uuid4(),
        skid_id=skid_id,
        timestamp=now,
        command_type=body.command_type,
        parameters_json=body.parameters_json,
        source=body.source or user.username,
        status=CommandStatus.PENDING,
        created_at=now,
    )
    db.add(command)
    await db.flush()  # Populate id before publishing

    # Publish to Kafka (lazy import to avoid circular dependency at startup)
    try:
        from platform.api.kafka_producer import get_producer  # noqa: PLC0415

        producer = await get_producer()
        await producer.send_command(
            site_id=str(site_id),
            skid_id=str(skid_id),
            skid_type=skid.skid_type.value.lower(),
            command={
                "command_id": str(command.id),
                "command_type": command.command_type,
                "parameters": command.parameters_json or {},
                "source": command.source,
                "timestamp": now.isoformat(),
            },
        )
        command.status = CommandStatus.SENT
    except Exception as exc:
        logger.error("Failed to publish command to Kafka: %s", exc)
        command.status = CommandStatus.FAILED
        command.result_json = {"error": str(exc)}

    return CommandResponse.model_validate(command)


# ---------------------------------------------------------------------------
# Command history
# ---------------------------------------------------------------------------


@router.get(
    "/sites/{site_id}/skids/{skid_id}/commands",
    response_model=List[CommandResponse],
    summary="List command history for a skid",
)
async def list_commands(
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
    command_status: Optional[CommandStatus] = Query(None, alias="status"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_operator),
) -> List[CommandResponse]:
    await _assert_skid_in_site(db, site_id, skid_id)

    filters = [CommandRecord.skid_id == skid_id]
    if command_status:
        filters.append(CommandRecord.status == command_status)

    result = await db.execute(
        select(CommandRecord)
        .where(*filters)
        .order_by(CommandRecord.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    return [CommandResponse.model_validate(r) for r in result.scalars().all()]


# ---------------------------------------------------------------------------
# Get single command
# ---------------------------------------------------------------------------


@router.get(
    "/sites/{site_id}/skids/{skid_id}/commands/{command_id}",
    response_model=CommandResponse,
    summary="Get status of a specific command",
)
async def get_command(
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
    command_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_operator),
) -> CommandResponse:
    await _assert_skid_in_site(db, site_id, skid_id)
    command = await _get_command(db, skid_id, command_id)
    return CommandResponse.model_validate(command)


# ---------------------------------------------------------------------------
# Acknowledge command result from edge
# ---------------------------------------------------------------------------


@router.post(
    "/sites/{site_id}/skids/{skid_id}/commands/{command_id}/ack",
    response_model=CommandResponse,
    summary="Record acknowledgement of command execution from edge skid",
)
async def ack_command(
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
    command_id: uuid.UUID,
    result: Dict[str, Any],
    db: AsyncSession = Depends(get_db),
    _user: User = Depends(require_operator),
) -> CommandResponse:
    await _assert_skid_in_site(db, site_id, skid_id)
    command = await _get_command(db, skid_id, command_id)

    success = result.get("success", False)
    command.status = CommandStatus.COMPLETED if success else CommandStatus.FAILED
    command.result_json = result
    db.add(command)

    return CommandResponse.model_validate(command)


# ---------------------------------------------------------------------------
# Writeback (direct register / node write)
# ---------------------------------------------------------------------------


@router.post(
    "/sites/{site_id}/skids/{skid_id}/writeback",
    response_model=WritebackResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Issue a direct writeback to a field device node",
)
async def create_writeback(
    site_id: uuid.UUID,
    skid_id: uuid.UUID,
    body: WritebackCreate,
    db: AsyncSession = Depends(get_db),
    user: User = Depends(require_operator),
) -> WritebackResponse:
    skid = await _assert_skid_in_site(db, site_id, skid_id)

    # Create a companion command record for audit trail
    now = datetime.now(tz=timezone.utc)
    command = CommandRecord(
        id=uuid.uuid4(),
        skid_id=skid_id,
        timestamp=now,
        command_type=body.command_type,
        parameters_json=body.value_json,
        source=body.operator or user.username,
        status=CommandStatus.PENDING,
        created_at=now,
    )
    db.add(command)
    await db.flush()

    audit = WritebackAudit(
        id=uuid.uuid4(),
        skid_id=skid_id,
        timestamp=now,
        command_id=command.id,
        target_protocol=body.target_protocol,
        target_node=body.target_node,
        value_json=body.value_json,
        confirmed=False,
        operator=body.operator or user.username,
    )
    db.add(audit)
    await db.flush()

    # Publish writeback to Kafka
    try:
        from platform.api.kafka_producer import get_producer  # noqa: PLC0415

        producer = await get_producer()
        await producer.send_writeback(
            site_id=str(site_id),
            skid_id=str(skid_id),
            skid_type=skid.skid_type.value.lower(),
            writeback={
                "audit_id": str(audit.id),
                "command_id": str(command.id),
                "command_type": body.command_type,
                "target_protocol": body.target_protocol,
                "target_node": body.target_node,
                "value": body.value_json,
                "operator": audit.operator,
                "timestamp": now.isoformat(),
            },
        )
        command.status = CommandStatus.SENT
    except Exception as exc:
        logger.error("Failed to publish writeback to Kafka: %s", exc)
        command.status = CommandStatus.FAILED
        command.result_json = {"error": str(exc)}

    return WritebackResponse.model_validate(audit)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


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


async def _get_command(
    db: AsyncSession, skid_id: uuid.UUID, command_id: uuid.UUID
) -> CommandRecord:
    result = await db.execute(
        select(CommandRecord).where(
            CommandRecord.id == command_id,
            CommandRecord.skid_id == skid_id,
        )
    )
    command = result.scalar_one_or_none()
    if command is None:
        raise HTTPException(
            status_code=404,
            detail=f"Command {command_id} not found",
        )
    return command
