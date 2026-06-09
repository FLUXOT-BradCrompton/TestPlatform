"""
Data loading utilities for Flux OT AI training pipelines.

Provides helpers for:
  - Loading raw telemetry from TimescaleDB (or compatible PostgreSQL)
  - Loading labeled events from CSV annotation files
  - Sliding-window segmentation for time series classification
  - Temporal train/test splits (no future leakage)
  - Minority-class augmentation (SMOTE-inspired)
"""

from __future__ import annotations

import logging
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database loader
# ---------------------------------------------------------------------------


def load_telemetry_from_db(
    connection_string: str,
    skid_id: str,
    start_time: str,
    end_time: str,
    tags: Optional[List[str]] = None,
) -> pd.DataFrame:
    """
    Load raw telemetry for a skid from TimescaleDB.

    Parameters
    ----------
    connection_string : SQLAlchemy-compatible connection string.
                        e.g. "postgresql://user:pass@timescale:5432/fluxot"
    skid_id           : Skid identifier to filter on.
    start_time        : ISO 8601 start time string (UTC).
    end_time          : ISO 8601 end time string (UTC).
    tags              : Optional list of tag names to filter.  If None all
                        tags for the skid are returned.

    Returns
    -------
    pd.DataFrame with columns: timestamp, tag_name, value, quality
    Sorted by timestamp ascending.
    """
    try:
        from sqlalchemy import create_engine, text
    except ImportError as exc:
        raise ImportError("sqlalchemy is required for database loading") from exc

    tag_filter = ""
    params: dict = {
        "skid_id": skid_id,
        "start_time": start_time,
        "end_time": end_time,
    }

    if tags:
        placeholders = ", ".join(f":tag_{i}" for i in range(len(tags)))
        tag_filter = f"AND tag_name IN ({placeholders})"
        for i, t in enumerate(tags):
            params[f"tag_{i}"] = t

    query = text(
        f"""
        SELECT timestamp, tag_name, value::float, quality
        FROM telemetry
        WHERE skid_id = :skid_id
          AND timestamp >= :start_time::timestamptz
          AND timestamp <  :end_time::timestamptz
          {tag_filter}
        ORDER BY timestamp ASC
        """
    )

    engine = create_engine(connection_string)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params=params)
    finally:
        engine.dispose()

    logger.info(
        "Loaded %d rows for skid=%s [%s -> %s]", len(df), skid_id, start_time, end_time
    )
    return df


# ---------------------------------------------------------------------------
# CSV event loader
# ---------------------------------------------------------------------------


def load_labeled_events(csv_path: str) -> pd.DataFrame:
    """
    Load labeled rip/crossing events from a CSV annotation file.

    Expected CSV columns:
        timestamp    - ISO 8601 UTC time of event start
        skid_id      - Skid identifier
        label        - Integer class label (0=normal, 1=rip / 0-3 for crossing)
        event_type   - Human-readable event description (optional)
        duration_s   - Event duration in seconds (optional)
        notes        - Analyst notes (optional)

    Returns
    -------
    pd.DataFrame sorted by timestamp.
    """
    df = pd.read_csv(csv_path)
    required = {"timestamp", "skid_id", "label"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing required columns: {missing}")

    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df.sort_values("timestamp", inplace=True)
    df.reset_index(drop=True, inplace=True)

    logger.info("Loaded %d labeled events from %s", len(df), csv_path)
    return df


# ---------------------------------------------------------------------------
# Sliding-window segmentation
# ---------------------------------------------------------------------------


def create_windows(
    df: pd.DataFrame,
    window_size: int,
    step_size: int,
    label_col: str,
    feature_cols: Optional[List[str]] = None,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Segment a time series DataFrame into overlapping windows for classification.

    The label for each window is determined by majority vote over the label
    values within the window.  If >=1 rip/event sample is present in the window
    the window is labelled as the minority (positive) class to preserve recall.

    Parameters
    ----------
    df           : DataFrame with rows ordered by time.  Must contain label_col.
    window_size  : Number of samples per window.
    step_size    : Number of samples to advance between consecutive windows.
    label_col    : Column name containing integer class labels.
    feature_cols : Columns to include as features.  If None, all numeric columns
                   except label_col are used.

    Returns
    -------
    X : np.ndarray shape (n_windows, window_size, n_features)
    y : np.ndarray shape (n_windows,) of integer labels
    """
    if feature_cols is None:
        feature_cols = [
            c for c in df.select_dtypes(include=[np.number]).columns if c != label_col
        ]

    feature_arr = df[feature_cols].values.astype(np.float32)
    label_arr = df[label_col].values.astype(np.int32)

    n_samples = len(df)
    X_windows: List[np.ndarray] = []
    y_labels: List[int] = []

    start = 0
    while start + window_size <= n_samples:
        end = start + window_size
        window_features = feature_arr[start:end]  # (window_size, n_features)
        window_labels = label_arr[start:end]

        # Label: if any non-zero label exists in window -> take max label
        # (preserves signal for rare events such as EMERGENCY)
        label = int(np.max(window_labels))

        X_windows.append(window_features)
        y_labels.append(label)
        start += step_size

    if not X_windows:
        raise ValueError(
            f"No windows created. DataFrame has {n_samples} rows but window_size={window_size}."
        )

    X = np.stack(X_windows, axis=0)
    y = np.array(y_labels, dtype=np.int32)

    logger.info(
        "Created %d windows (size=%d, step=%d) from %d samples. Label distribution: %s",
        len(y),
        window_size,
        step_size,
        dict(zip(*np.unique(y, return_counts=True))),
    )
    return X, y


# ---------------------------------------------------------------------------
# Temporal train/test split
# ---------------------------------------------------------------------------


def train_test_split_temporal(
    X: np.ndarray,
    y: np.ndarray,
    test_ratio: float = 0.2,
) -> Tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """
    Temporally ordered train/test split that avoids future leakage.

    The last *test_ratio* fraction of samples is used as the test set.

    Parameters
    ----------
    X          : Feature array (n_samples, ...).
    y          : Label array (n_samples,).
    test_ratio : Fraction of samples to reserve for testing.

    Returns
    -------
    X_train, X_test, y_train, y_test
    """
    if len(X) != len(y):
        raise ValueError("X and y must have the same number of samples")
    if not 0 < test_ratio < 1:
        raise ValueError("test_ratio must be in (0, 1)")

    n = len(X)
    split_idx = int(n * (1.0 - test_ratio))

    X_train, X_test = X[:split_idx], X[split_idx:]
    y_train, y_test = y[:split_idx], y[split_idx:]

    logger.info(
        "Temporal split: train=%d samples, test=%d samples (%.0f%% test)",
        len(X_train),
        len(X_test),
        test_ratio * 100,
    )
    return X_train, X_test, y_train, y_test


# ---------------------------------------------------------------------------
# Minority-class augmentation (SMOTE-inspired)
# ---------------------------------------------------------------------------


def augment_minority_class(
    X_train: np.ndarray,
    y_train: np.ndarray,
    factor: int = 3,
    noise_scale: float = 0.02,
    random_state: int = 42,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Augment minority classes to reduce class imbalance.

    Strategy (SMOTE-inspired without requiring imbalanced-learn at runtime):
      1. Identify all minority classes (any class with count < majority count).
      2. For each minority sample, generate *factor* synthetic neighbours by
         linearly interpolating between the sample and a randomly chosen
         minority neighbour, then adding small Gaussian noise.
      3. If imbalanced-learn is available, use SMOTE directly for better
         synthetic sample quality.

    Parameters
    ----------
    X_train      : Training features, shape (n, d) — must be 2-D (flattened).
    y_train      : Training labels.
    factor       : Number of synthetic copies per minority sample.
    noise_scale  : Std dev of Gaussian noise added to synthetic samples
                   (as a fraction of each feature's std dev in the minority set).
    random_state : RNG seed for reproducibility.

    Returns
    -------
    X_aug, y_aug : Augmented arrays (original + synthetic samples, shuffled).
    """
    rng = np.random.default_rng(random_state)

    # Attempt imbalanced-learn SMOTE first
    try:
        from imblearn.over_sampling import SMOTE  # type: ignore

        # Only usable with 2-D arrays
        if X_train.ndim == 2:
            sm = SMOTE(sampling_strategy="auto", random_state=random_state, k_neighbors=min(5, factor))
            X_res, y_res = sm.fit_resample(X_train, y_train)
            logger.info(
                "SMOTE augmentation: %d -> %d samples", len(y_train), len(y_res)
            )
            return X_res, y_res
    except ImportError:
        pass
    except Exception as exc:
        logger.warning("SMOTE failed (%s), falling back to interpolation augmentation", exc)

    # Fallback: interpolation + noise
    classes, counts = np.unique(y_train, return_counts=True)
    majority_count = int(np.max(counts))

    X_aug_parts = [X_train]
    y_aug_parts = [y_train]

    orig_shape = X_train.shape
    X_flat = X_train.reshape(len(X_train), -1) if X_train.ndim > 2 else X_train

    for cls, cnt in zip(classes, counts):
        if cnt >= majority_count:
            continue  # skip majority class

        mask = y_train == cls
        X_minority = X_flat[mask]
        n_minority = len(X_minority)
        n_to_generate = min(factor * n_minority, majority_count - cnt)

        if n_minority < 2:
            # Can't interpolate with a single sample — just duplicate with noise
            noise = rng.normal(0, noise_scale * (np.std(X_minority) + 1e-6), (n_to_generate, X_minority.shape[1]))
            synthetic = np.tile(X_minority, (n_to_generate, 1)) + noise
        else:
            idx_a = rng.integers(0, n_minority, n_to_generate)
            idx_b = rng.integers(0, n_minority, n_to_generate)
            alpha = rng.uniform(0.2, 0.8, (n_to_generate, 1))
            synthetic = X_minority[idx_a] * alpha + X_minority[idx_b] * (1 - alpha)
            feat_std = np.std(X_minority, axis=0) + 1e-6
            synthetic += rng.normal(0, noise_scale * feat_std, synthetic.shape)

        # Restore original shape
        synthetic = synthetic.reshape((n_to_generate,) + orig_shape[1:])
        X_aug_parts.append(synthetic)
        y_aug_parts.append(np.full(n_to_generate, cls, dtype=y_train.dtype))
        logger.info("Augmented class %d: %d -> %d samples", cls, cnt, cnt + n_to_generate)

    X_aug = np.concatenate(X_aug_parts, axis=0)
    y_aug = np.concatenate(y_aug_parts, axis=0)

    # Shuffle
    idx = rng.permutation(len(y_aug))
    return X_aug[idx], y_aug[idx]
