"""
Flux OT Platform API — FastAPI application entry point.

Startup sequence:
  1. Initialise database tables and TimescaleDB hypertables
  2. Start Kafka producer (for command/writeback publishing)
  3. Start Kafka consumer background task
  4. Verify Redis connectivity

Shutdown sequence:
  1. Stop Kafka consumer (drain in-flight messages)
  2. Stop Kafka producer (flush)
  3. Close DB connection pool
"""
from __future__ import annotations

import logging
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

import redis.asyncio as aioredis
from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import ValidationError
from prometheus_fastapi_instrumentator import Instrumentator

from platform.api.config import settings
from platform.api.database import check_db_connection, close_db, create_timescale_hypertables, init_db
from platform.api.auth import router as auth_router
from platform.api.routes.alerts import router as alerts_router
from platform.api.routes.commands import router as commands_router
from platform.api.routes.sites import router as sites_router
from platform.api.routes.telemetry import router as telemetry_router

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Application lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """Manage startup and shutdown of background services."""
    # ---- Startup ----
    logger.info("Flux OT Platform API starting up (env=%s)…", settings.ENVIRONMENT)

    # Database
    try:
        await init_db()
        await create_timescale_hypertables()
        logger.info("Database initialised.")
    except Exception as exc:
        logger.error("Database init failed: %s", exc)

    # Kafka producer
    try:
        from platform.api.kafka_producer import start_producer  # noqa: PLC0415

        await start_producer()
        logger.info("Kafka producer started.")
    except Exception as exc:
        logger.error("Kafka producer start failed (non-fatal): %s", exc)

    # Kafka consumer
    try:
        from platform.api.kafka_consumer import start_consumer  # noqa: PLC0415

        await start_consumer()
        logger.info("Kafka consumer started.")
    except Exception as exc:
        logger.error("Kafka consumer start failed (non-fatal): %s", exc)

    # Redis connectivity check
    try:
        redis_client = aioredis.from_url(settings.REDIS_URL)
        await redis_client.ping()
        await redis_client.aclose()
        logger.info("Redis connection verified.")
    except Exception as exc:
        logger.error("Redis connectivity check failed (non-fatal): %s", exc)

    logger.info("Flux OT Platform API ready.")
    yield

    # ---- Shutdown ----
    logger.info("Flux OT Platform API shutting down…")

    try:
        from platform.api.kafka_consumer import stop_consumer  # noqa: PLC0415

        await stop_consumer()
    except Exception as exc:
        logger.error("Kafka consumer stop error: %s", exc)

    try:
        from platform.api.kafka_producer import stop_producer  # noqa: PLC0415

        await stop_producer()
    except Exception as exc:
        logger.error("Kafka producer stop error: %s", exc)

    await close_db()
    logger.info("Shutdown complete.")


# ---------------------------------------------------------------------------
# Application factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    app = FastAPI(
        title="Flux OT Platform API",
        version="1.0.0",
        description=(
            "Industrial IoT Integration Platform for Mining and Oil & Gas — "
            "central OpenShift-hosted service that receives data from edge skids, "
            "processes it, and provides REST/WebSocket APIs."
        ),
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
        lifespan=lifespan,
    )

    # ---- CORS ----
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ---- Prometheus metrics ----
    Instrumentator(
        should_group_status_codes=True,
        should_ignore_untemplated=True,
        should_respect_env_var=True,
        should_instrument_requests_inprogress=True,
        excluded_handlers=["/health", "/ready", "/metrics"],
        env_var_name="ENABLE_METRICS",
        inprogress_name="fluxot_http_requests_inprogress",
        inprogress_labels=True,
    ).instrument(app).expose(app, endpoint="/metrics", include_in_schema=False)

    # ---- Routers ----
    app.include_router(auth_router)
    app.include_router(sites_router)
    app.include_router(telemetry_router)
    app.include_router(alerts_router)
    app.include_router(commands_router)

    # ---- Exception handlers ----
    @app.exception_handler(ValidationError)
    async def validation_exception_handler(
        request: Request, exc: ValidationError
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": exc.errors(), "body": None},
        )

    @app.exception_handler(Exception)
    async def generic_exception_handler(
        request: Request, exc: Exception
    ) -> JSONResponse:
        logger.error(
            "Unhandled exception for %s %s: %s\n%s",
            request.method,
            request.url,
            exc,
            traceback.format_exc(),
        )
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={
                "detail": "An internal server error occurred.",
                "request_id": request.headers.get("x-request-id"),
            },
        )

    # ---- Health / readiness probes ----
    @app.get(
        "/health",
        tags=["Operations"],
        summary="Liveness probe",
        response_model=dict,
    )
    async def health() -> dict:
        return {
            "status": "healthy",
            "version": "1.0.0",
            "environment": settings.ENVIRONMENT,
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        }

    @app.get(
        "/ready",
        tags=["Operations"],
        summary="Readiness probe — checks DB and Kafka connectivity",
        response_model=dict,
    )
    async def ready() -> JSONResponse:
        checks: dict[str, Any] = {}

        # Database
        db_ok = await check_db_connection()
        checks["database"] = "ok" if db_ok else "unavailable"

        # Redis
        try:
            redis_client = aioredis.from_url(settings.REDIS_URL)
            await redis_client.ping()
            await redis_client.aclose()
            checks["redis"] = "ok"
        except Exception:
            checks["redis"] = "unavailable"

        # Kafka (check producer is initialised)
        try:
            from platform.api.kafka_producer import _producer_instance  # noqa: PLC0415

            checks["kafka"] = "ok" if _producer_instance is not None else "not started"
        except Exception:
            checks["kafka"] = "unavailable"

        all_ok = all(v == "ok" for v in checks.values())
        http_status = status.HTTP_200_OK if all_ok else status.HTTP_503_SERVICE_UNAVAILABLE

        return JSONResponse(
            status_code=http_status,
            content={
                "status": "ready" if all_ok else "not ready",
                "checks": checks,
                "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            },
        )

    return app


# ---------------------------------------------------------------------------
# ASGI application entry point
# ---------------------------------------------------------------------------

app = create_app()

# ---------------------------------------------------------------------------
# Development runner
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "platform.api.main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        workers=1,  # Use single worker in dev; set API_WORKERS in production
        reload=settings.ENVIRONMENT == "development",
        log_level=settings.LOG_LEVEL.lower(),
        access_log=True,
    )
