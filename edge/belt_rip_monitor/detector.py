"""
Local (edge-side) belt rip detection algorithm.

Runs entirely on the edge node with no network dependency.  Implements four
independent anomaly detectors that are combined into a composite confidence
score.

Detection methods:
  1. Magnetic field signature analysis  – drop in any channel signals rip
  2. Acoustic energy analysis           – transient spikes signal rip
  3. Load cell asymmetry check          – side-to-side imbalance
  4. Belt speed anomaly                 – unexpected speed drop

A rolling baseline of the last N samples is maintained for adaptive
thresholding.
"""

from __future__ import annotations

import statistics
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Deque, List, Optional

from edge.common.logging_config import get_logger

from .config import BeltRipConfig
from .sensors import BeltSensorData

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class DetectionResult:
    """
    Output of one detection cycle.

    Attributes:
        timestamp:         UTC time of the detection.
        composite_score:   0.0 – 1.0; higher means more likely a rip event.
        magnetic_anomaly:  True when magnetic signature anomaly detected.
        acoustic_anomaly:  True when acoustic transient spike detected.
        load_anomaly:      True when load-cell asymmetry exceeds threshold.
        speed_anomaly:     True when belt speed drops unexpectedly.
        confidence:        Calibrated confidence in the composite score (0–1).
        recommendation:    Action recommendation string.
    """

    timestamp: datetime
    composite_score: float          # 0–1
    magnetic_anomaly: bool
    acoustic_anomaly: bool
    load_anomaly: bool
    speed_anomaly: bool
    confidence: float               # 0–1
    recommendation: str             # "NORMAL" | "MONITOR" | "SLOW_DOWN" | "STOP"


# ---------------------------------------------------------------------------
# Rolling baseline helper
# ---------------------------------------------------------------------------

_BASELINE_WINDOW = 100  # number of samples to retain


class _RollingStats:
    """Maintains a rolling mean and standard deviation for one signal."""

    def __init__(self, window: int = _BASELINE_WINDOW) -> None:
        self._buf: Deque[float] = deque(maxlen=window)

    def update(self, value: float) -> None:
        self._buf.append(value)

    @property
    def mean(self) -> Optional[float]:
        if len(self._buf) < 2:
            return None
        return statistics.mean(self._buf)

    @property
    def stdev(self) -> Optional[float]:
        if len(self._buf) < 2:
            return None
        try:
            return statistics.stdev(self._buf)
        except statistics.StatisticsError:
            return 0.0

    @property
    def ready(self) -> bool:
        return len(self._buf) >= 10


# ---------------------------------------------------------------------------
# Detector
# ---------------------------------------------------------------------------


class BeltRipDetector:
    """
    Edge-local belt rip detector using heuristic signal analysis.

    The detector maintains a rolling baseline for each sensor channel and
    uses adaptive thresholds derived from that baseline.
    """

    # Weighting for each anomaly type in the composite score
    _WEIGHT_MAGNETIC: float = 0.40
    _WEIGHT_ACOUSTIC: float = 0.30
    _WEIGHT_LOAD: float = 0.20
    _WEIGHT_SPEED: float = 0.10

    # Fixed thresholds
    _MAG_DROP_RATIO: float = 0.30        # 30% drop from baseline triggers anomaly
    _ACOUSTIC_SIGMA: float = 3.0         # 3-sigma spike threshold
    _LOAD_ASYMMETRY_RATIO: float = 0.15  # 15% asymmetry threshold
    _SPEED_DROP_RATIO: float = 0.20      # 20% speed drop threshold

    def __init__(self, config: BeltRipConfig) -> None:
        self._config = config
        self._threshold = config.RIP_DETECTION_THRESHOLD

        # Per-channel rolling baselines
        self._mag_stats: List[_RollingStats] = [_RollingStats() for _ in range(8)]
        self._acou_stats: List[_RollingStats] = [_RollingStats() for _ in range(4)]
        self._speed_stats = _RollingStats()
        self._load_stats: List[_RollingStats] = [_RollingStats() for _ in range(2)]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def detect(self, sensor_data: BeltSensorData) -> DetectionResult:
        """
        Run all detectors against *sensor_data* and return a
        :class:`DetectionResult`.
        """
        mag_score, mag_anomaly = self._check_magnetic(sensor_data.magnetic_sensors)
        acou_score, acou_anomaly = self._check_acoustic(sensor_data.acoustic_sensors)
        load_score, load_anomaly = self._check_load_cells(sensor_data.load_cells)
        speed_score, speed_anomaly = self._check_speed(sensor_data.belt_speed, sensor_data.running)

        composite = (
            self._WEIGHT_MAGNETIC * mag_score
            + self._WEIGHT_ACOUSTIC * acou_score
            + self._WEIGHT_LOAD * load_score
            + self._WEIGHT_SPEED * speed_score
        )
        composite = min(1.0, max(0.0, composite))

        # Confidence: grows with baseline readiness
        n_ready = sum(
            [s.ready for s in self._mag_stats]
            + [s.ready for s in self._acou_stats]
            + [self._speed_stats.ready]
        )
        total_channels = 8 + 4 + 1
        confidence = n_ready / total_channels

        recommendation = self._recommend(composite, mag_anomaly, acou_anomaly, speed_anomaly)

        result = DetectionResult(
            timestamp=sensor_data.timestamp,
            composite_score=composite,
            magnetic_anomaly=mag_anomaly,
            acoustic_anomaly=acou_anomaly,
            load_anomaly=load_anomaly,
            speed_anomaly=speed_anomaly,
            confidence=confidence,
            recommendation=recommendation,
        )

        if composite > 0.5:
            logger.warning(
                "Belt rip anomaly detected",
                extra={
                    "composite_score": round(composite, 3),
                    "recommendation": recommendation,
                    "mag": mag_anomaly,
                    "acou": acou_anomaly,
                    "load": load_anomaly,
                    "speed": speed_anomaly,
                },
            )

        return result

    def update_baseline(self, sensor_data: BeltSensorData) -> None:
        """Incorporate a new sensor reading into the rolling baselines."""
        for i, v in enumerate(sensor_data.magnetic_sensors):
            self._mag_stats[i].update(v)
        for i, v in enumerate(sensor_data.acoustic_sensors):
            self._acou_stats[i].update(v)
        for i, v in enumerate(sensor_data.load_cells):
            self._load_stats[i].update(v)
        if sensor_data.running:
            self._speed_stats.update(sensor_data.belt_speed)

    # ------------------------------------------------------------------
    # Individual detectors
    # ------------------------------------------------------------------

    def _check_magnetic(self, channels: List[float]) -> tuple[float, bool]:
        """
        Detect rip via magnetic field drop.

        A rip opens the belt material and disrupts the magnetic sensor field.
        We flag any channel whose current value has dropped >30% below its
        rolling mean.
        """
        anomaly_count = 0
        for i, val in enumerate(channels):
            stats = self._mag_stats[i]
            if not stats.ready:
                continue
            baseline = stats.mean
            if baseline and baseline > 0.0:
                drop_ratio = (baseline - val) / baseline
                if drop_ratio > self._MAG_DROP_RATIO:
                    anomaly_count += 1
                    logger.debug(
                        "Magnetic drop anomaly",
                        extra={"channel": i, "baseline": round(baseline, 2), "current": round(val, 2)},
                    )

        # Score proportional to fraction of channels anomalous
        score = anomaly_count / len(channels) if channels else 0.0
        return score, anomaly_count > 0

    def _check_acoustic(self, channels: List[float]) -> tuple[float, bool]:
        """
        Detect rip via acoustic energy spikes.

        A longitudinal belt rip generates a characteristic acoustic transient.
        We flag any channel whose current value exceeds mean + N*sigma.
        """
        anomaly_count = 0
        for i, val in enumerate(channels):
            stats = self._acou_stats[i]
            if not stats.ready:
                continue
            mean = stats.mean
            stdev = stats.stdev
            if mean is not None and stdev is not None and stdev > 0:
                z_score = (val - mean) / stdev
                if z_score > self._ACOUSTIC_SIGMA:
                    anomaly_count += 1
                    logger.debug(
                        "Acoustic spike anomaly",
                        extra={"channel": i, "z_score": round(z_score, 2)},
                    )

        score = anomaly_count / len(channels) if channels else 0.0
        return score, anomaly_count > 0

    def _check_load_cells(self, load_cells: List[float]) -> tuple[float, bool]:
        """
        Detect belt asymmetry via load-cell comparison.

        If one side of the belt carries significantly more load than the other,
        it may indicate material spill caused by a rip.
        """
        if len(load_cells) < 2:
            return 0.0, False

        lc0, lc1 = load_cells[0], load_cells[1]
        total = lc0 + lc1
        if total <= 0:
            return 0.0, False

        asymmetry = abs(lc0 - lc1) / total
        anomaly = asymmetry > self._LOAD_ASYMMETRY_RATIO
        score = min(1.0, asymmetry / self._LOAD_ASYMMETRY_RATIO) if anomaly else 0.0
        return score, anomaly

    def _check_speed(self, belt_speed: float, running: bool) -> tuple[float, bool]:
        """
        Detect unexpected speed drop.

        A large rip can cause the belt to slow suddenly due to increased drag
        or motor protection tripping.
        """
        if not running:
            return 0.0, False
        if not self._speed_stats.ready:
            return 0.0, False

        baseline = self._speed_stats.mean
        if baseline and baseline > 0.0:
            drop_ratio = (baseline - belt_speed) / baseline
            if drop_ratio > self._SPEED_DROP_RATIO:
                score = min(1.0, drop_ratio / self._SPEED_DROP_RATIO)
                logger.debug(
                    "Speed drop anomaly",
                    extra={"baseline": round(baseline, 3), "current": round(belt_speed, 3)},
                )
                return score, True

        return 0.0, False

    # ------------------------------------------------------------------
    # Recommendation
    # ------------------------------------------------------------------

    def _recommend(
        self,
        score: float,
        magnetic: bool,
        acoustic: bool,
        speed: bool,
    ) -> str:
        """Map composite score and individual anomalies to an action string."""
        if score >= self._threshold or (magnetic and acoustic):
            return "STOP"
        if score >= 0.50 or speed:
            return "SLOW_DOWN"
        if score >= 0.25 or magnetic or acoustic:
            return "MONITOR"
        return "NORMAL"
