"""
HTTP client for the Flux OT AI inference service.

Sends sensor snapshots to the remote AI service and maps the response back to
a :class:`DetectionResult`.  Falls back to the local heuristic detector when
the service is unavailable (timeout / connection error / non-2xx response).
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Optional

import httpx

from edge.common.logging_config import get_logger

from .config import BeltRipConfig
from .detector import BeltRipDetector, DetectionResult
from .sensors import BeltSensorData

logger = get_logger(__name__)

_REQUEST_TIMEOUT_S = 2.0   # Keep well under poll interval
_CACHE_TTL_S = 2.0         # How long a cached response stays valid (1 cycle)


class AIInferenceClient:
    """
    Calls the remote AI inference service for belt-rip prediction.

    If the service is unavailable the client transparently falls back to the
    local :class:`BeltRipDetector`.  The last successful prediction is cached
    and returned for at most ``_CACHE_TTL_S`` seconds when the service times
    out mid-cycle.
    """

    def __init__(self, config: BeltRipConfig) -> None:
        self._config = config
        self._url = config.AI_SERVICE_URL
        self._enabled = config.AI_ENABLED
        self._fallback = BeltRipDetector(config)

        # Cache: (DetectionResult, timestamp)
        self._cache: Optional[tuple[DetectionResult, float]] = None

        self._http = httpx.AsyncClient(
            timeout=httpx.Timeout(connect=1.0, read=_REQUEST_TIMEOUT_S, write=1.0, pool=1.0),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )

    async def predict(self, sensor_data: BeltSensorData) -> Optional[DetectionResult]:
        """
        Request an AI inference prediction for *sensor_data*.

        Returns:
            A :class:`DetectionResult` (from AI or local fallback), or ``None``
            if both the AI service and the local fallback fail.
        """
        if not self._enabled:
            return self._local_predict(sensor_data)

        payload = self._build_payload(sensor_data)

        try:
            response = await self._http.post(self._url, json=payload)
            response.raise_for_status()
            result = self._parse_response(response.json(), sensor_data.timestamp)
            # Update local fallback baseline on every successful AI call
            self._fallback.update_baseline(sensor_data)
            self._cache = (result, time.monotonic())
            logger.debug("AI inference success", extra={"score": round(result.composite_score, 3)})
            return result

        except httpx.TimeoutException:
            logger.warning("AI service timeout, using cached/local result")
            return self._cached_or_local(sensor_data)

        except httpx.HTTPStatusError as exc:
            logger.warning(
                "AI service HTTP error",
                extra={"status": exc.response.status_code, "url": self._url},
            )
            return self._cached_or_local(sensor_data)

        except httpx.RequestError as exc:
            logger.warning("AI service unreachable", extra={"error": str(exc)})
            return self._cached_or_local(sensor_data)

        except Exception as exc:
            logger.error("AI inference unexpected error", extra={"error": str(exc)}, exc_info=True)
            return self._local_predict(sensor_data)

    async def close(self) -> None:
        """Release the underlying HTTP client resources."""
        await self._http.aclose()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _cached_or_local(self, sensor_data: BeltSensorData) -> DetectionResult:
        """Return the cached prediction if fresh enough, else run locally."""
        if self._cache:
            result, ts = self._cache
            if time.monotonic() - ts <= _CACHE_TTL_S:
                logger.debug("Using cached AI prediction")
                return result
        return self._local_predict(sensor_data)

    def _local_predict(self, sensor_data: BeltSensorData) -> DetectionResult:
        """Run the local heuristic detector and update its baseline."""
        result = self._fallback.detect(sensor_data)
        self._fallback.update_baseline(sensor_data)
        return result

    @staticmethod
    def _build_payload(sensor_data: BeltSensorData) -> dict:
        """Serialise :class:`BeltSensorData` into the AI service request schema."""
        return {
            "timestamp": sensor_data.timestamp.isoformat(),
            "magnetic_sensors": sensor_data.magnetic_sensors,
            "acoustic_sensors": sensor_data.acoustic_sensors,
            "load_cells": sensor_data.load_cells,
            "belt_speed": sensor_data.belt_speed,
            "belt_tension": sensor_data.belt_tension,
            "motor_current": sensor_data.motor_current,
            "belt_temperature": sensor_data.belt_temperature,
            "running": sensor_data.running,
        }

    @staticmethod
    def _parse_response(data: dict, fallback_ts: datetime) -> DetectionResult:
        """
        Parse the AI service JSON response into a :class:`DetectionResult`.

        Expected response schema::

            {
              "composite_score": 0.12,
              "magnetic_anomaly": false,
              "acoustic_anomaly": false,
              "load_anomaly": false,
              "speed_anomaly": false,
              "confidence": 0.95,
              "recommendation": "NORMAL"
            }
        """
        ts_str = data.get("timestamp")
        if ts_str:
            try:
                ts = datetime.fromisoformat(ts_str)
            except ValueError:
                ts = fallback_ts
        else:
            ts = fallback_ts

        recommendation = data.get("recommendation", "NORMAL")
        if recommendation not in ("NORMAL", "MONITOR", "SLOW_DOWN", "STOP"):
            recommendation = "MONITOR"

        return DetectionResult(
            timestamp=ts,
            composite_score=float(data.get("composite_score", 0.0)),
            magnetic_anomaly=bool(data.get("magnetic_anomaly", False)),
            acoustic_anomaly=bool(data.get("acoustic_anomaly", False)),
            load_anomaly=bool(data.get("load_anomaly", False)),
            speed_anomaly=bool(data.get("speed_anomaly", False)),
            confidence=float(data.get("confidence", 0.0)),
            recommendation=recommendation,
        )
