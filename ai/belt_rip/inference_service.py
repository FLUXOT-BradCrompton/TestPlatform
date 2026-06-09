"""
Belt Rip AI Inference Service — Flux OT AI

FastAPI HTTP service providing:
  - POST /predict/belt-rip           inference endpoint
  - GET  /model/info                 current model metadata
  - POST /model/reload               hot-reload from registry (admin)
  - GET  /health                     liveness probe
  - GET  /ready                      readiness probe (checks model loaded)
  - GET  /metrics                    Prometheus metrics

Background Kafka consumer:
  Subscribes to  fluxot.*.BELT_RIP_MONITOR.*.telemetry
  Publishes to   fluxot.*.BELT_RIP_MONITOR.*.ai-predictions
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

import numpy as np
from fastapi import BackgroundTasks, Depends, FastAPI, HTTPException, Header, status
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
    MODEL_NAME: str = Field(default="belt_rip_detector")
    MODEL_STAGE: str = Field(default="Production")
    LOCAL_MODEL_PATH: str = Field(default="/models/belt_rip")
    KAFKA_BOOTSTRAP_SERVERS: str = Field(default="kafka:9092")
    KAFKA_CONSUMER_GROUP: str = Field(default="fluxot-ai-belt-rip")
    KAFKA_TELEMETRY_TOPIC_PATTERN: str = Field(default="fluxot\\..*\\.BELT_RIP_MONITOR\\..*\\.telemetry")
    KAFKA_PREDICTIONS_TOPIC_SUFFIX: str = Field(default="ai-predictions")
    ADMIN_API_KEY: str = Field(default="changeme-in-production")
    DRIFT_WINDOW_SIZE: int = Field(default=500)
    DRIFT_ALERT_THRESHOLD: float = Field(default=0.25)


settings = Settings()


# ---------------------------------------------------------------------------
# Pydantic request/response schemas
# ---------------------------------------------------------------------------


class BeltSensorData(BaseModel):
    """Sensor readings from a belt rip monitoring skid."""

    site_id: str = Field(..., description="Site identifier")
    skid_id: str = Field(..., description="Skid identifier")
    timestamp: Optional[str] = Field(default=None, description="ISO 8601 UTC timestamp")

    # Magnetic sensor channels (mT)
    mag_ch1: float = Field(..., ge=0, description="Magnetic channel 1 (mT)")
    mag_ch2: float = Field(..., ge=0, description="Magnetic channel 2 (mT)")
    mag_ch3: float = Field(..., ge=0, description="Magnetic channel 3 (mT)")
    mag_ch4: float = Field(..., ge=0, description="Magnetic channel 4 (mT)")

    # Load cells (kg)
    load_cell_left: float = Field(..., ge=0, description="Left load cell (kg)")
    load_cell_right: float = Field(..., ge=0, description="Right load cell (kg)")

    # Acoustic (dB)
    acoustic_level: float = Field(..., ge=0, description="Acoustic sensor level (dB)")

    # Belt kinematics
    belt_speed: float = Field(..., ge=0, description="Belt speed (m/s)")

    # Motor
    motor_current: float = Field(..., ge=0, description="Motor current (A)")
    motor_vibration: float = Field(..., ge=0, description="Motor vibration RMS (mm/s)")


class PredictionResponse(BaseModel):
    request_id: str
    timestamp: str
    site_id: str
    skid_id: str
    prediction: int
    label: str
    confidence: float
    anomaly_score: float
    recommendation: str
    feature_importances: Dict[str, float] = Field(default_factory=dict)
    latency_ms: float
    model_tier: str
    model_version: str


class ModelInfoResponse(BaseModel):
    model_name: str
    model_version: str
    model_stage: str
    active_tier: str
    loaded_at: str
    accuracy: Optional[float] = None
    f1_score: Optional[float] = None
    auc_roc: Optional[float] = None
    n_features: int = 0


class ReloadResponse(BaseModel):
    status: str
    model_version: str
    message: str


# ---------------------------------------------------------------------------
# Application state
# ---------------------------------------------------------------------------


class _AppState:
    model: Optional[Any] = None
    model_version: str = "unknown"
    model_stage: str = settings.MODEL_STAGE
    loaded_at: str = ""
    model_card: Dict[str, Any] = {}

    # Latency tracking (rolling deque)
    latency_samples: Deque[float] = deque(maxlen=1000)

    # Drift monitoring — rolling window of positive class prediction rates
    prediction_history: Deque[int] = deque(maxlen=settings.DRIFT_WINDOW_SIZE)
    baseline_positive_rate: float = 0.08  # expected from training data rip_ratio
    drift_alert_active: bool = False

    # Kafka consumer task
    kafka_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]


_state = _AppState()


# ---------------------------------------------------------------------------
# Model loader
# ---------------------------------------------------------------------------


def _load_model() -> None:
    """Load model from MLflow registry or local path."""
    from ai.belt_rip.model import BeltRipModel
    from ai.common.model_registry import ModelRegistry

    registry = ModelRegistry(tracking_uri=settings.MLFLOW_TRACKING_URI)
    local_path = Path(settings.LOCAL_MODEL_PATH)

    try:
        logger.info("Loading model '%s' (stage=%s) from registry ...",
                    settings.MODEL_NAME, settings.MODEL_STAGE)
        raw = registry.load_latest_model(settings.MODEL_NAME, stage=settings.MODEL_STAGE)
        # If MLflow returns a pyfunc wrapper, extract underlying sklearn model
        if hasattr(raw, "_model_impl"):
            raw = raw._model_impl.python_model  # type: ignore[attr-defined]
        # Try local path if registry returned a non-BeltRipModel
        if not isinstance(raw, BeltRipModel) and local_path.exists():
            raise RuntimeError("Registry returned non-BeltRipModel — using local fallback")
        _state.model = raw
        _state.model_version = "registry-latest"
    except Exception as exc:
        logger.warning("Registry load failed (%s) — trying local path %s", exc, local_path)
        if local_path.exists():
            _state.model = BeltRipModel.load(str(local_path))
            _state.model_version = "local"
            logger.info("Loaded model from local path %s", local_path)
        else:
            logger.error("No model available — service will return 503 until model is loaded")
            return

    # Load model card for /model/info
    card_path = local_path / "model_card.json"
    if card_path.exists():
        _state.model_card = json.loads(card_path.read_text())

    _state.loaded_at = datetime.now(tz=timezone.utc).isoformat()
    logger.info("Model loaded successfully (version=%s)", _state.model_version)


# ---------------------------------------------------------------------------
# Kafka consumer
# ---------------------------------------------------------------------------


async def _kafka_consumer_loop() -> None:
    """
    Background async Kafka consumer.

    Reads telemetry batches from topic pattern, runs inference, publishes
    prediction results back to the AI predictions topic.
    """
    try:
        from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
        import re
    except ImportError:
        logger.warning("aiokafka not available — Kafka consumer disabled")
        return

    pattern = settings.KAFKA_TELEMETRY_TOPIC_PATTERN
    bootstrap = settings.KAFKA_BOOTSTRAP_SERVERS

    logger.info("Starting Kafka consumer (bootstrap=%s, pattern=%s)", bootstrap, pattern)

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

        # Subscribe by pattern
        consumer.subscribe(pattern=re.compile(pattern.replace("\\.", ".")))

        logger.info("Kafka consumer started")

        async for msg in consumer:
            if _state.model is None:
                continue
            try:
                payload = msg.value
                await _process_kafka_message(payload, producer, msg.topic)
            except Exception as exc:
                logger.error("Error processing Kafka message: %s", exc, exc_info=True)

    except Exception as exc:
        logger.error("Kafka consumer fatal error: %s", exc, exc_info=True)
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
    """Process a single telemetry message and publish prediction."""
    from ai.common.feature_engineering import build_feature_vector

    # Extract sensor readings from telemetry payload
    data_section = payload.get("data", payload)
    site_id = payload.get("site_id", "unknown")
    skid_id = payload.get("skid_id", "unknown")

    sensor_data = {}
    sensor_keys = [
        "mag_ch1", "mag_ch2", "mag_ch3", "mag_ch4",
        "load_cell_left", "load_cell_right",
        "acoustic_level", "belt_speed", "motor_current", "motor_vibration",
    ]
    for key in sensor_keys:
        if key in data_section:
            raw = data_section[key]
            sensor_data[key] = raw.get("value", 0.0) if isinstance(raw, dict) else float(raw)
        else:
            sensor_data[key] = 0.0

    features = build_feature_vector(sensor_data)
    result = _state.model.predict(features)

    # Update drift monitoring
    _state.prediction_history.append(result.prediction)
    _check_drift()

    # Derive prediction topic from source topic
    parts = source_topic.rsplit(".", 1)
    pred_topic = f"{parts[0]}.{settings.KAFKA_PREDICTIONS_TOPIC_SUFFIX}"

    prediction_msg = {
        "timestamp": datetime.now(tz=timezone.utc).isoformat(),
        "site_id": site_id,
        "skid_id": skid_id,
        "prediction": result.prediction,
        "label": result.label,
        "confidence": result.confidence,
        "anomaly_score": result.anomaly_score,
        "recommendation": result.recommendation,
        "model_version": _state.model_version,
        "latency_ms": result.latency_ms,
    }

    await producer.send_and_wait(pred_topic, prediction_msg)


def _check_drift() -> None:
    """Detect prediction distribution drift vs training baseline."""
    if len(_state.prediction_history) < 100:
        return
    current_positive_rate = sum(_state.prediction_history) / len(_state.prediction_history)
    drift_ratio = abs(current_positive_rate - _state.baseline_positive_rate) / max(
        _state.baseline_positive_rate, 0.01
    )
    if drift_ratio > settings.DRIFT_ALERT_THRESHOLD and not _state.drift_alert_active:
        logger.warning(
            "MODEL DRIFT DETECTED: current positive rate=%.3f vs baseline=%.3f (ratio=%.2f)",
            current_positive_rate,
            _state.baseline_positive_rate,
            drift_ratio,
        )
        _state.drift_alert_active = True
    elif drift_ratio <= settings.DRIFT_ALERT_THRESHOLD:
        _state.drift_alert_active = False


# ---------------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------------


def _setup_prometheus(app: FastAPI) -> None:
    try:
        from prometheus_fastapi_instrumentator import Instrumentator

        Instrumentator(
            should_group_status_codes=True,
            should_ignore_untemplated=True,
        ).instrument(app).expose(app, endpoint="/metrics")
        logger.info("Prometheus metrics enabled at /metrics")
    except ImportError:
        logger.warning("prometheus_fastapi_instrumentator not installed — /metrics unavailable")


# ---------------------------------------------------------------------------
# FastAPI app
# ---------------------------------------------------------------------------


@asynccontextmanager
async def _lifespan(app: FastAPI):
    # Startup
    _load_model()
    _state.kafka_task = asyncio.create_task(_kafka_consumer_loop())
    yield
    # Shutdown
    if _state.kafka_task and not _state.kafka_task.done():
        _state.kafka_task.cancel()
        try:
            await _state.kafka_task
        except asyncio.CancelledError:
            pass


app = FastAPI(
    title="Belt Rip AI Inference Service",
    description=(
        "Flux OT AI — Belt rip detection inference for industrial conveyor belts. "
        "Combines RandomForest (edge-compatible) and optional LSTM tiers."
    ),
    version="1.0.0",
    lifespan=_lifespan,
)

_setup_prometheus(app)


# ---------------------------------------------------------------------------
# Dependency: require loaded model
# ---------------------------------------------------------------------------


def _require_model():
    if _state.model is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Model not loaded. Check /health for service status.",
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
    return {"status": "ok", "service": "belt-rip-inference", "timestamp": datetime.now(tz=timezone.utc).isoformat()}


@app.get("/ready", tags=["Operations"])
async def ready():
    if _state.model is None:
        return JSONResponse(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            content={"ready": False, "reason": "Model not loaded"},
        )
    return {"ready": True, "model_version": _state.model_version}


@app.post("/predict/belt-rip", response_model=PredictionResponse, tags=["Inference"])
async def predict_belt_rip(
    sensor_data: BeltSensorData,
    model: Any = Depends(_require_model),
) -> PredictionResponse:
    """
    Run belt rip inference on a single sensor reading.

    Returns the predicted class (0=NORMAL, 1=RIP_DETECTED), confidence score,
    anomaly score, feature importances, and a human-readable recommendation.
    """
    from ai.common.feature_engineering import build_feature_vector
    import uuid

    sensor_dict = {
        "mag_ch1": sensor_data.mag_ch1,
        "mag_ch2": sensor_data.mag_ch2,
        "mag_ch3": sensor_data.mag_ch3,
        "mag_ch4": sensor_data.mag_ch4,
        "load_cell_left": sensor_data.load_cell_left,
        "load_cell_right": sensor_data.load_cell_right,
        "acoustic_level": sensor_data.acoustic_level,
        "belt_speed": sensor_data.belt_speed,
        "motor_current": sensor_data.motor_current,
        "motor_vibration": sensor_data.motor_vibration,
    }

    features = build_feature_vector(sensor_dict)
    result = model.predict(features)

    # Track latency
    _state.latency_samples.append(result.latency_ms)

    # Update drift window
    _state.prediction_history.append(result.prediction)
    _check_drift()

    return PredictionResponse(
        request_id=str(uuid.uuid4()),
        timestamp=sensor_data.timestamp or datetime.now(tz=timezone.utc).isoformat(),
        site_id=sensor_data.site_id,
        skid_id=sensor_data.skid_id,
        prediction=result.prediction,
        label=result.label,
        confidence=result.confidence,
        anomaly_score=result.anomaly_score,
        recommendation=result.recommendation,
        feature_importances=result.feature_importances,
        latency_ms=result.latency_ms,
        model_tier=result.model_tier,
        model_version=_state.model_version,
    )


@app.get("/model/info", response_model=ModelInfoResponse, tags=["Model Management"])
async def model_info():
    """Return metadata about the currently loaded model."""
    metrics = _state.model_card.get("validation_metrics", {})
    n_features = len(_state.model_card.get("feature_names", []))
    if _state.model and hasattr(_state.model, "_rf") and _state.model._rf is not None:
        n_features = n_features or getattr(_state.model._rf, "n_features_in_", 0)

    return ModelInfoResponse(
        model_name=settings.MODEL_NAME,
        model_version=_state.model_version,
        model_stage=_state.model_stage,
        active_tier=_state.model_card.get("active_tier", "random_forest"),
        loaded_at=_state.loaded_at,
        accuracy=metrics.get("accuracy"),
        f1_score=metrics.get("f1"),
        auc_roc=metrics.get("auc_roc"),
        n_features=n_features,
    )


@app.post("/model/reload", response_model=ReloadResponse, tags=["Model Management"])
async def reload_model(
    background_tasks: BackgroundTasks,
    _: None = Depends(_require_admin),
):
    """
    Trigger a hot-reload of the model from the registry.
    Admin-only (requires X-API-Key header).
    """
    background_tasks.add_task(_load_model)
    return ReloadResponse(
        status="reload_triggered",
        model_version=_state.model_version,
        message="Model reload triggered in background. Check /model/info for updated version.",
    )


@app.get("/model/drift", tags=["Model Management"])
async def drift_status():
    """Return current model drift monitoring status."""
    if len(_state.prediction_history) == 0:
        return {"drift_alert": False, "current_positive_rate": None, "window_size": 0}
    current_rate = sum(_state.prediction_history) / len(_state.prediction_history)
    return {
        "drift_alert": _state.drift_alert_active,
        "current_positive_rate": round(current_rate, 4),
        "baseline_positive_rate": _state.baseline_positive_rate,
        "window_size": len(_state.prediction_history),
        "threshold": settings.DRIFT_ALERT_THRESHOLD,
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
        port=8080,
        log_level="info",
        workers=1,
    )
