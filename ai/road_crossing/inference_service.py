"""
Road Crossing Safety AI Inference Service — Flux OT AI

FastAPI HTTP service providing:
  - POST /predict/crossing           inference endpoint
  - GET  /model/info                 current model metadata
  - POST /model/reload               hot-reload from registry (admin)
  - GET  /health                     liveness probe
  - GET  /ready                      readiness probe (checks model loaded)
  - GET  /metrics                    Prometheus metrics

Hard safety constraint (non-negotiable):
  belt_running=True AND belt_speed > 0 -> always UNSAFE, regardless of model output.

Background Kafka consumer:
  Subscribes to  fluxot.*.ROAD_CROSSING.*.telemetry
  Publishes to   fluxot.*.ROAD_CROSSING.*.ai-predictions
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
import uuid
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

import numpy as np
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, SettingsConfigDict

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s — %(message)s")
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", case_sensitive=False)

    MLFLOW_TRACKING_URI: str = Field(default="http://mlflow:5000")
    MODEL_NAME: str = Field(default="road_crossing_safety")
    MODEL_STAGE: str = Field(default="Production")
    LOCAL_MODEL_PATH: str = Field(default="/models/road_crossing")
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="kafka:9092")
    KAFKA_CONSUMER_GROUP: str = Field(default="fluxot-ai-road-crossing")
    KAFKA_TELEMETRY_TOPIC_PATTERN: str = Field(
        default="fluxot\\..*\\.ROAD_CROSSING\\..*\\.telemetry"
    )
    KAFKA_PREDICTIONS_TOPIC_SUFFIX: str = Field(default="ai-predictions")
    ADMIN_API_KEY: str = Field(default="changeme-in-production")
    DRIFT_WINDOW_SIZE: int = Field(default=500)


settings = Settings()


# ---------------------------------------------------------------------------
# Pydantic request/response schemas
# ---------------------------------------------------------------------------


class CrossingSensorData(BaseModel):
    """Sensor readings from a road crossing skid."""

    site_id: str = Field(..., description="Site identifier")
    skid_id: str = Field(..., description="Skid identifier")
    timestamp: Optional[str] = Field(default=None)

    # Belt state
    belt_speed: float = Field(..., ge=0.0, description="Belt speed (m/s)")
    belt_running: bool = Field(..., description="True if belt drive is running")

    # Vehicle detection
    vehicle_speed: float = Field(default=0.0, ge=0.0, description="Detected vehicle speed (km/h)")
    vehicle_class: int = Field(
        default=0,
        ge=0,
        le=3,
        description="Vehicle class: 0=none, 1=light, 2=heavy, 3=oversized",
    )

    # Loop detectors (inductive presence sensors)
    loop_detector_near: bool = Field(default=False, description="Near-side loop detector triggered")
    loop_detector_far: bool = Field(default=False, description="Far-side loop detector triggered")

    # LiDAR
    lidar_distance_m: float = Field(
        default=999.0, ge=0.0, description="Nearest object distance from LiDAR (m)"
    )
    lidar_count: int = Field(default=0, ge=0, description="Objects detected in zone")

    # Temporal context
    hour_of_day: float = Field(
        default=12.0, ge=0.0, le=23.0, description="Hour of day (0-23)"
    )
    day_of_week: float = Field(default=0.0, ge=0.0, le=6.0, description="Day of week (0=Mon)")

    # Weather (optional)
    weather_code: int = Field(
        default=0,
        ge=0,
        le=3,
        description="Weather: 0=clear, 1=rain, 2=fog, 3=snow",
    )


class CrossingPredictionResponse(BaseModel):
    request_id: str
    timestamp: str
    site_id: str
    skid_id: str
    safety_state: int
    state_label: str
    confidence: float
    estimated_wait_s: Optional[float]
    risk_factors: List[str]
    recommended_action: str
    human_review_required: bool
    latency_ms: float
    model_version: str
    hard_constraint_applied: bool = False


class ModelInfoResponse(BaseModel):
    model_name: str
    model_version: str
    model_stage: str
    classifier: str
    loaded_at: str
    accuracy: Optional[float] = None
    macro_f1: Optional[float] = None
    auc_roc_ovr: Optional[float] = None
    n_features: int = 0


# ---------------------------------------------------------------------------
# Application state
# ---------------------------------------------------------------------------


class _AppState:
    model: Optional[Any] = None
    model_version: str = "unknown"
    loaded_at: str = ""
    model_card: Dict[str, Any] = {}

    latency_samples: Deque[float] = deque(maxlen=1000)

    # Track safety state distribution for drift monitoring
    state_history: Deque[int] = deque(maxlen=500)

    kafka_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]


_state = _AppState()


# ---------------------------------------------------------------------------
# Model loader
# ---------------------------------------------------------------------------


def _load_model() -> None:
    """Load road crossing model from registry or local path."""
    from ai.road_crossing.model import RoadCrossingModel
    from ai.common.model_registry import ModelRegistry

    registry = ModelRegistry(tracking_uri=settings.MLFLOW_TRACKING_URI)
    local_path = Path(settings.LOCAL_MODEL_PATH)

    try:
        logger.info("Loading model '%s' (stage=%s) ...", settings.MODEL_NAME, settings.MODEL_STAGE)
        raw = registry.load_latest_model(settings.MODEL_NAME, stage=settings.MODEL_STAGE)
        if not isinstance(raw, RoadCrossingModel) and local_path.exists():
            raise RuntimeError("Registry returned non-RoadCrossingModel")
        _state.model = raw
        _state.model_version = "registry-latest"
    except Exception as exc:
        logger.warning("Registry load failed (%s) — trying local path %s", exc, local_path)
        if local_path.exists():
            _state.model = RoadCrossingModel.load(str(local_path))
            _state.model_version = _state.model.model_version
            logger.info("Loaded model from local path %s", local_path)
        else:
            logger.error("No model available — service will return 503 until model is loaded")
            return

    card_path = local_path / "model_card.json"
    if card_path.exists():
        _state.model_card = json.loads(card_path.read_text())

    _state.loaded_at = datetime.now(tz=timezone.utc).isoformat()
    logger.info("Road crossing model loaded (version=%s)", _state.model_version)


# ---------------------------------------------------------------------------
# Kafka consumer
# ---------------------------------------------------------------------------


async def _kafka_consumer_loop() -> None:
    """Background Kafka consumer for batch inference."""
    try:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
        import re
    except ImportError:
        logger.warning("aiokafka not available — Kafka consumer disabled")
        return

    pattern = settings.KAFKA_TELEMETRY_TOPIC_PATTERN
    bootstrap = settings.KAFKA_BOOTSTRAP_SERVERS
    logger.info("Starting Kafka consumer (bootstrap=%s)", bootstrap)

    try:
        consumer = AIOKafkaConsumer(
            bootstrap_servers=bootstrap,
            group_id=settings.KAFKA_CONSUMER_GROUP,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
        )
        producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

        await consumer.start()
        await producer.start()
        consumer.subscribe(pattern=re.compile(pattern.replace("\\.", ".")))
        logger.info("Kafka consumer started for road crossing")

        async for msg in consumer:
            if _state.model is None:
                continue
            try:
                await _process_kafka_message(msg.value, producer, msg.topic)
            except Exception as exc:
                logger.error("Kafka processing error: %s", exc, exc_info=True)

    except Exception as exc:
        logger.error("Kafka consumer fatal: %s", exc, exc_info=True)
    finally:
        try:
            await consumer.stop()
            await producer.stop()
        except Exception:
            pass


async def _process_kafka_message(
    payload: dict,
    producer: Any,
    source_topic: str,
) -> None:
    """Process telemetry message from Kafka, publish prediction."""
    data = payload.get("data", payload)

    def _get(key: str, default: Any = 0.0) -> Any:
        val = data.get(key, default)
        return val.get("value", default) if isinstance(val, dict) else val

    sensor_data = {
        "belt_speed": _get("belt_speed", 0.0),
        "belt_running": bool(_get("belt_running", False)),
        "vehicle_speed": _get("vehicle_speed", 0.0),
        "vehicle_class": int(_get("vehicle_class", 0)),
        "loop_detector_near": bool(_get("loop_detector_near", False)),
        "loop_detector_far": bool(_get("loop_detector_far", False)),
        "lidar_distance_m": _get("lidar_distance_m", 999.0),
        "lidar_count": int(_get("lidar_count", 0)),
        "hour_of_day": _get("hour_of_day", 12.0),
        "day_of_week": _get("day_of_week", 0.0),
        "weather_code": int(_get("weather_code", 0)),
    }

    result = _state.model.predict(sensor_data)
    _state.state_history.append(result.safety_state)

    parts = source_topic.rsplit(".", 1)
    pred_topic = f"{parts[0]}.{settings.KAFKA_PREDICTIONS_TOPIC_SUFFIX}"

    await producer.send_and_wait(pred_topic, {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "site_id": payload.get("site_id", "unknown"),
        "skid_id": payload.get("skid_id", "unknown"),
        "safety_state": result.safety_state,
        "state_label": result.state_label,
        "confidence": result.confidence,
        "risk_factors": result.risk_factors,
        "recommended_action": result.recommended_action,
        "human_review_required": result.human_review_required,
        "model_version": _state.model_version,
    })


# ---------------------------------------------------------------------------
# Prometheus
# ---------------------------------------------------------------------------


def _setup_prometheus(app: FastAPI) -> None:
    try:
        from prometheus_fastapi_instrumentator import Instrumentator
        Instrumentator(should_group_status_codes=True).instrument(app).expose(app, endpoint="/metrics")
        logger.info("Prometheus metrics at /metrics")
    except ImportError:
        logger.warning("prometheus_fastapi_instrumentator not installed")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: FastAPI):
    _load_model()
    _state.kafka_task = asyncio.create_task(_kafka_consumer_loop())
    yield
    if _state.kafka_task and not _state.kafka_task.done():
        _state.kafka_task.cancel()
        try:
            await _state.kafka_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="Road Crossing Safety AI Inference Service",
    description=(
        "Flux OT AI — Real-time road crossing safety classification for "
        "industrial conveyor belt crossings. Includes hard safety constraint enforcement."
    ),
    version="1.0.0",
    lifespan=_lifespan,
)

_setup_prometheus(app)


# ---------------------------------------------------------------------------
# Dependencies
# ---------------------------------------------------------------------------


def _require_model():
    if _state.model is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model not loaded. Check /health.",
        )
    return _state.model


def _require_admin(x_api_key: str = Header(...)):
    if x_api_key != settings.ADMIN_API_KEY:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Invalid API key")


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@app.get("/health", tags=["Operations"])
async def health():
    return {
        "status": "ok",
        "service": "road-crossing-inference",
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
    }


@app.get("/ready", tags=["Operations"])
async def ready():
    if _state.model is None:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"ready": False, "reason": "Model not loaded"},
        )
    return {"ready": True, "model_version": _state.model_version}


@app.post("/predict/crossing", response_model=CrossingPredictionResponse, tags=["Inference"])
async def predict_crossing(
    sensor_data: CrossingSensorData,
    model: Any = Depends(_require_model),
) -> CrossingPredictionResponse:
    """
    Run road crossing safety inference.

    Returns the safety state (0=SAFE_TO_CROSS, 1=WAIT, 2=UNSAFE, 3=EMERGENCY),
    confidence, risk factors, and a human-readable recommended action.

    NOTE: The hard safety constraint (belt running -> UNSAFE) is always
    enforced and takes precedence over the model output.
    """
    sensor_dict = {
        "belt_speed": sensor_data.belt_speed,
        "belt_running": sensor_data.belt_running,
        "vehicle_speed": sensor_data.vehicle_speed,
        "vehicle_class": sensor_data.vehicle_class,
        "loop_detector_near": sensor_data.loop_detector_near,
        "loop_detector_far": sensor_data.loop_detector_far,
        "lidar_distance_m": sensor_data.lidar_distance_m,
        "lidar_count": sensor_data.lidar_count,
        "hour_of_day": sensor_data.hour_of_day,
        "day_of_week": sensor_data.day_of_week,
        "weather_code": sensor_data.weather_code,
    }

    result = model.predict(sensor_dict)
    _state.latency_samples.append(result.latency_ms)
    _state.state_history.append(result.safety_state)

    hard_constraint = "HARD_SAFETY_CONSTRAINT_ACTIVE" in result.risk_factors

    return CrossingPredictionResponse(
        request_id=str(uuid.uuid4()),
        timestamp=sensor_data.timestamp or datetime.now(tz=timezone.utc).isoformat(),
        site_id=sensor_data.site_id,
        skid_id=sensor_data.skid_id,
        safety_state=result.safety_state,
        state_label=result.state_label,
        confidence=result.confidence,
        estimated_wait_s=result.estimated_wait_s,
        risk_factors=result.risk_factors,
        recommended_action=result.recommended_action,
        human_review_required=result.human_review_required,
        latency_ms=result.latency_ms,
        model_version=_state.model_version,
        hard_constraint_applied=hard_constraint,
    )


@app.get("/model/info", response_model=ModelInfoResponse, tags=["Model Management"])
async def model_info():
    """Return metadata about the currently loaded model."""
    metrics = _state.model_card.get("validation_metrics", {})
    return ModelInfoResponse(
        model_name=settings.MODEL_NAME,
        model_version=_state.model_version,
        model_stage=settings.MODEL_STAGE,
        classifier=_state.model_card.get("classifier", "gradient_boosting"),
        loaded_at=_state.loaded_at,
        accuracy=metrics.get("accuracy"),
        macro_f1=metrics.get("macro_f1"),
        auc_roc_ovr=metrics.get("auc_roc_ovr"),
        n_features=len(_state.model_card.get("feature_names", [])),
    )


@app.post("/model/reload", tags=["Model Management"])
async def reload_model(
    background_tasks: BackgroundTasks,
    _: None = Depends(_require_admin),
):
    """Trigger hot-reload of the model. Admin-only (X-API-Key header required)."""
    background_tasks.add_task(_load_model)
    return {
        "status": "reload_triggered",
        "model_version": _state.model_version,
        "message": "Model reload triggered. Check /model/info for updated version.",
    }


@app.get("/model/safety-stats", tags=["Model Management"])
async def safety_stats():
    """Return recent safety state distribution (last 500 predictions)."""
    if not _state.state_history:
        return {"window_size": 0, "distribution": {}}

    from collections import Counter
    label_map = {0: "SAFE_TO_CROSS", 1: "WAIT", 2: "UNSAFE", 3: "EMERGENCY"}
    counts = Counter(_state.state_history)
    total = len(_state.state_history)

    distribution = {
        label_map[k]: {
            "count": v,
            "rate": round(v / total, 4),
        }
        for k, v in sorted(counts.items())
    }

    # Alert if UNSAFE + EMERGENCY rate is high
    unsafe_rate = (counts.get(2, 0) + counts.get(3, 0)) / total
    alert = unsafe_rate > 0.30

    return {
        "window_size": total,
        "distribution": distribution,
        "unsafe_emergency_rate": round(unsafe_rate, 4),
        "alert": alert,
    }


@app.get("/model/latency", tags=["Model Management"])
async def latency_stats():
    """Return inference latency percentiles (p50, p95, p99)."""
    if not _state.latency_samples:
        return {"p50_ms": None, "p95_ms": None, "p99_ms": None, "n_samples": 0}
    samples = np.array(_state.latency_samples)
    return {
        "p50_ms": round(float(np.percentile(samples, 50)), 3),
        "p95_ms": round(float(np.percentile(samples, 95)), 3),
        "p99_ms": round(float(np.percentile(samples, 99)), 3),
        "mean_ms": round(float(np.mean(samples)), 3),
        "n_samples": len(samples),
    }


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "inference_service:app",
        host="0.0.0.0",
        port=8081,
        log_level="info",
        workers=1,
    )
