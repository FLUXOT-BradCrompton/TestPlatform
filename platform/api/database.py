"""
Async SQLAlchemy engine setup, session factory, and database initialisation helpers.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy import text

from platform.api.config import settings
from platform.api.models import Base

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Engine
# ---------------------------------------------------------------------------

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.ENVIRONMENT == "development",
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=3600,
    connect_args={
        "server_settings": {
            "application_name": "fluxot-platform-api",
            "jit": "off",  # Disable JIT for short-lived OLTP queries
        }
    },
)

# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------

AsyncSessionLocal: async_sessionmaker[AsyncSession] = async_sessionmaker(
    bind=engine,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)

# ---------------------------------------------------------------------------
# FastAPI dependency
# ---------------------------------------------------------------------------


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Yield an async database session, committing or rolling back on exit."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise


# Context manager variant for non-FastAPI usage (e.g. background tasks)
get_db_context = asynccontextmanager(get_db)

# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------


async def init_db() -> None:
    """Create all tables defined by the ORM models.

    In production this is superseded by Alembic migrations, but the function
    is kept for development convenience and integration tests.
    """
    async with engine.begin() as conn:
        # Ensure uuid-ossp and timescaledb are available
        try:
            await conn.execute(text('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"'))
            await conn.execute(text("CREATE EXTENSION IF NOT EXISTS pg_stat_statements"))
        except Exception as exc:
            logger.warning("Could not create extensions (non-fatal): %s", exc)

        # SQLAlchemy will ignore the custom __table_args__ dict key we added as
        # a hint; it does not affect DDL generation.
        await conn.run_sync(Base.metadata.create_all)
        logger.info("Database tables created / verified.")


async def create_timescale_hypertables() -> None:
    """Convert telemetry_records to a TimescaleDB hypertable.

    Safe to call multiple times — the IF NOT EXISTS guard prevents errors on
    subsequent starts.
    """
    sql_statements = [
        # Convert the base table
        """
        SELECT create_hypertable(
            'telemetry_records',
            'timestamp',
            chunk_time_interval => INTERVAL '1 day',
            if_not_exists => TRUE
        );
        """,
        # Continuous aggregate: hourly rollup
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS hourly_telemetry_summary
        WITH (timescaledb.continuous) AS
        SELECT
            skid_id,
            tag_name,
            time_bucket('1 hour', timestamp) AS bucket,
            AVG(value_float)   AS avg_value,
            MIN(value_float)   AS min_value,
            MAX(value_float)   AS max_value,
            STDDEV(value_float) AS stddev_value,
            COUNT(*)           AS sample_count,
            MODE() WITHIN GROUP (ORDER BY quality) AS dominant_quality
        FROM telemetry_records
        WHERE value_float IS NOT NULL
        GROUP BY skid_id, tag_name, bucket
        WITH NO DATA;
        """,
        # Continuous aggregate: daily rollup
        """
        CREATE MATERIALIZED VIEW IF NOT EXISTS daily_telemetry_summary
        WITH (timescaledb.continuous) AS
        SELECT
            skid_id,
            tag_name,
            time_bucket('1 day', timestamp) AS bucket,
            AVG(value_float)   AS avg_value,
            MIN(value_float)   AS min_value,
            MAX(value_float)   AS max_value,
            STDDEV(value_float) AS stddev_value,
            COUNT(*)           AS sample_count
        FROM telemetry_records
        WHERE value_float IS NOT NULL
        GROUP BY skid_id, tag_name, bucket
        WITH NO DATA;
        """,
        # Refresh policy: hourly aggregate
        """
        SELECT add_continuous_aggregate_policy(
            'hourly_telemetry_summary',
            start_offset => INTERVAL '3 hours',
            end_offset   => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 hour',
            if_not_exists => TRUE
        );
        """,
        # Refresh policy: daily aggregate
        """
        SELECT add_continuous_aggregate_policy(
            'daily_telemetry_summary',
            start_offset => INTERVAL '3 days',
            end_offset   => INTERVAL '1 day',
            schedule_interval => INTERVAL '1 day',
            if_not_exists => TRUE
        );
        """,
        # Data retention: 90 days on raw telemetry
        """
        SELECT add_retention_policy(
            'telemetry_records',
            INTERVAL '90 days',
            if_not_exists => TRUE
        );
        """,
        # Compression: compress chunks older than 7 days
        """
        ALTER TABLE telemetry_records SET (
            timescaledb.compress,
            timescaledb.compress_orderby = 'timestamp DESC',
            timescaledb.compress_segmentby = 'skid_id, tag_name'
        );
        """,
        """
        SELECT add_compression_policy(
            'telemetry_records',
            INTERVAL '7 days',
            if_not_exists => TRUE
        );
        """,
    ]

    async with engine.begin() as conn:
        for stmt in sql_statements:
            try:
                await conn.execute(text(stmt))
                await conn.execute(text("COMMIT"))  # Some TimescaleDB calls need explicit commit
            except Exception as exc:
                logger.warning(
                    "TimescaleDB setup statement skipped (non-fatal): %s — %s",
                    stmt.strip()[:80],
                    exc,
                )

    logger.info("TimescaleDB hypertable configuration complete.")


async def check_db_connection() -> bool:
    """Return True if the database is reachable."""
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return True
    except OperationalError:
        return False


async def close_db() -> None:
    """Dispose the engine connection pool on shutdown."""
    await engine.dispose()
    logger.info("Database connection pool closed.")
