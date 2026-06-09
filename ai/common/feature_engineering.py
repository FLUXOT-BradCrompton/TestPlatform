"""
Feature engineering utilities for Flux OT AI services.

Provides time-domain, frequency-domain, and statistical feature extraction
from industrial sensor signals, plus helpers for feature vector construction
and normalization used by both belt-rip and road-crossing models.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Tuple

import numpy as np
from scipy import signal as scipy_signal
from scipy.stats import kurtosis as scipy_kurtosis
from scipy.stats import skew as scipy_skew


# ---------------------------------------------------------------------------
# Time-domain features
# ---------------------------------------------------------------------------


def extract_time_domain_features(sig: np.ndarray, fs: float) -> Dict[str, float]:
    """
    Compute standard time-domain statistical features from a 1-D signal.

    Parameters
    ----------
    sig : 1-D array of sensor samples (e.g. vibration, current, load).
    fs  : sampling frequency in Hz.

    Returns
    -------
    dict with keys:
        mean, std, rms, peak, crest_factor, kurtosis, skewness,
        peak_to_peak, shape_factor
    """
    if sig.ndim != 1 or len(sig) == 0:
        raise ValueError("sig must be a non-empty 1-D array")

    mean_val = float(np.mean(sig))
    std_val = float(np.std(sig, ddof=1)) if len(sig) > 1 else 0.0
    rms = float(np.sqrt(np.mean(sig**2)))
    peak = float(np.max(np.abs(sig)))
    crest_factor = float(peak / rms) if rms > 0 else 0.0
    kurt = float(scipy_kurtosis(sig, fisher=True))   # excess kurtosis (normal=0)
    skewness = float(scipy_skew(sig))
    peak_to_peak = float(np.max(sig) - np.min(sig))
    # Shape factor = RMS / mean absolute value
    mean_abs = float(np.mean(np.abs(sig)))
    shape_factor = float(rms / mean_abs) if mean_abs > 0 else 0.0

    return {
        "mean": mean_val,
        "std": std_val,
        "rms": rms,
        "peak": peak,
        "crest_factor": crest_factor,
        "kurtosis": kurt,
        "skewness": skewness,
        "peak_to_peak": peak_to_peak,
        "shape_factor": shape_factor,
    }


# ---------------------------------------------------------------------------
# Frequency-domain features
# ---------------------------------------------------------------------------


def extract_frequency_domain_features(sig: np.ndarray, fs: float) -> Dict[str, float]:
    """
    Compute frequency-domain features using Welch's PSD estimate.

    Parameters
    ----------
    sig : 1-D sensor signal array.
    fs  : sampling frequency in Hz.

    Returns
    -------
    dict with keys:
        dominant_frequency, spectral_centroid, spectral_bandwidth,
        spectral_rolloff, total_power, peak_frequency_bin
    """
    if sig.ndim != 1 or len(sig) < 4:
        raise ValueError("sig must be a 1-D array with at least 4 samples")

    nperseg = min(256, len(sig))
    freqs, psd = scipy_signal.welch(sig, fs=fs, nperseg=nperseg)

    total_power = float(np.trapz(psd, freqs))

    if total_power == 0:
        return {
            "dominant_frequency": 0.0,
            "spectral_centroid": 0.0,
            "spectral_bandwidth": 0.0,
            "spectral_rolloff": 0.0,
            "total_power": 0.0,
            "peak_frequency_bin": 0,
        }

    # Dominant frequency = frequency with highest PSD
    peak_bin = int(np.argmax(psd))
    dominant_freq = float(freqs[peak_bin])

    # Spectral centroid = weighted mean frequency
    spectral_centroid = float(np.sum(freqs * psd) / np.sum(psd))

    # Spectral bandwidth = weighted standard deviation around centroid
    spectral_bandwidth = float(
        np.sqrt(np.sum(((freqs - spectral_centroid) ** 2) * psd) / np.sum(psd))
    )

    # Spectral roll-off: frequency below which 85% of energy is contained
    cumulative_power = np.cumsum(psd)
    rolloff_threshold = 0.85 * cumulative_power[-1]
    rolloff_idx = np.searchsorted(cumulative_power, rolloff_threshold)
    spectral_rolloff = float(freqs[min(rolloff_idx, len(freqs) - 1)])

    return {
        "dominant_frequency": dominant_freq,
        "spectral_centroid": spectral_centroid,
        "spectral_bandwidth": spectral_bandwidth,
        "spectral_rolloff": spectral_rolloff,
        "total_power": total_power,
        "peak_frequency_bin": peak_bin,
    }


# ---------------------------------------------------------------------------
# Statistical features
# ---------------------------------------------------------------------------


def extract_statistical_features(values: List[float]) -> Dict[str, float]:
    """
    Compute distribution-level statistics for a list of scalar values.

    Returns
    -------
    dict with keys:
        p25, p50, p75, p95, p99, iqr, coefficient_of_variation
    """
    if not values:
        raise ValueError("values must be non-empty")

    arr = np.asarray(values, dtype=float)
    p25, p50, p75, p95, p99 = float(np.percentile(arr, 25)), float(np.percentile(arr, 50)), \
        float(np.percentile(arr, 75)), float(np.percentile(arr, 95)), float(np.percentile(arr, 99))
    iqr = p75 - p25
    mean_val = float(np.mean(arr))
    std_val = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
    cv = float(std_val / mean_val) if mean_val != 0 else 0.0

    return {
        "p25": p25,
        "p50": p50,
        "p75": p75,
        "p95": p95,
        "p99": p99,
        "iqr": iqr,
        "coefficient_of_variation": cv,
    }


# ---------------------------------------------------------------------------
# Rolling / windowed features
# ---------------------------------------------------------------------------


def compute_rolling_features(series: List[float], window: int) -> Dict[str, float]:
    """
    Compute rolling window statistics over a time series.

    The last *window* samples are used for all calculations. If the series
    is shorter than *window*, the entire series is used.

    Returns
    -------
    dict with keys:
        rolling_mean, rolling_std, rolling_min, rolling_max, trend_slope
    """
    if not series:
        raise ValueError("series must be non-empty")

    arr = np.asarray(series[-window:], dtype=float)

    rolling_mean = float(np.mean(arr))
    rolling_std = float(np.std(arr, ddof=1)) if len(arr) > 1 else 0.0
    rolling_min = float(np.min(arr))
    rolling_max = float(np.max(arr))

    # Trend slope via least-squares linear fit
    x = np.arange(len(arr), dtype=float)
    if len(arr) >= 2:
        coeffs = np.polyfit(x, arr, 1)
        trend_slope = float(coeffs[0])
    else:
        trend_slope = 0.0

    return {
        "rolling_mean": rolling_mean,
        "rolling_std": rolling_std,
        "rolling_min": rolling_min,
        "rolling_max": rolling_max,
        "trend_slope": trend_slope,
    }


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------


def normalize_features(features: Dict[str, float], scaler_params: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    """
    Min-max normalize a feature dictionary.

    Parameters
    ----------
    features     : {'feature_name': value, ...}
    scaler_params: {'feature_name': {'min': ..., 'max': ...}, ...}
                   Features not present in scaler_params are passed through.

    Returns
    -------
    dict with normalized values in [0, 1].
    """
    normalized: Dict[str, float] = {}
    for key, value in features.items():
        if key in scaler_params:
            fmin = scaler_params[key]["min"]
            fmax = scaler_params[key]["max"]
            denom = fmax - fmin
            normalized[key] = float((value - fmin) / denom) if denom != 0 else 0.0
        else:
            normalized[key] = value
    return normalized


# ---------------------------------------------------------------------------
# Feature vector builder
# ---------------------------------------------------------------------------

# Canonical ordering of features extracted from a sensor data dict.
# Each entry maps to a sub-extraction function.  The order is fixed so
# that the resulting numpy vector is deterministic across calls.
_BELT_SENSOR_CHANNELS = [
    "mag_ch1", "mag_ch2", "mag_ch3", "mag_ch4",
    "load_cell_left", "load_cell_right",
    "acoustic_level",
    "belt_speed",
    "motor_current",
    "motor_vibration",
]


def build_feature_vector(sensor_data: Dict[str, object]) -> np.ndarray:
    """
    Build a flat feature vector from a sensor data dict.

    Each channel in *sensor_data* may be either a scalar float (latest
    reading) or a list/array of float readings (a recent window).

    For scalar channels: the value is used directly.
    For array channels : time-domain features are extracted and appended.

    The output vector has a deterministic, fixed layout and can be fed
    directly to the trained classifiers.

    Parameters
    ----------
    sensor_data : dict mapping channel name to float or List[float].

    Returns
    -------
    1-D np.ndarray (float32).
    """
    parts: List[float] = []

    for channel in _BELT_SENSOR_CHANNELS:
        raw = sensor_data.get(channel, 0.0)

        if isinstance(raw, (list, np.ndarray)):
            arr = np.asarray(raw, dtype=float)
            if len(arr) == 0:
                arr = np.array([0.0])
            td = extract_time_domain_features(arr, fs=1000.0)
            parts.extend([
                td["mean"], td["std"], td["rms"],
                td["peak"], td["crest_factor"], td["kurtosis"],
            ])
        else:
            # scalar — append directly
            parts.append(float(raw))

    # Append cross-channel features
    load_left = float(sensor_data.get("load_cell_left", 0.0))  # type: ignore[arg-type]
    load_right = float(sensor_data.get("load_cell_right", 0.0))  # type: ignore[arg-type]
    load_sum = load_left + load_right
    load_asymmetry = abs(load_left - load_right) / load_sum if load_sum > 0 else 0.0
    parts.append(load_asymmetry)

    # Mean of magnetic channels
    mag_vals = [float(sensor_data.get(ch, 0.0)) for ch in ("mag_ch1", "mag_ch2", "mag_ch3", "mag_ch4")]  # type: ignore[arg-type]
    parts.append(float(np.mean(mag_vals)))
    parts.append(float(np.std(mag_vals)))

    return np.array(parts, dtype=np.float32)
