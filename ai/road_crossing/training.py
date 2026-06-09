"""
Road Crossing Safety — Training Pipeline (Flux OT AI)

Generates realistic synthetic crossing scenarios and trains the
RoadCrossingModel multi-class classifier.

Scenario taxonomy
-----------------
  0 = SAFE_TO_CROSS  : belt stopped, adequate gap (>30m), no vehicles, good visibility
  1 = WAIT           : belt stopped but vehicle approaching (10-30m), or short gap
  2 = UNSAFE         : belt running OR vehicle <10m OR adverse weather + speed
  3 = EMERGENCY      : belt running + fast vehicle approaching, or multiple compounding hazards

Class balance:
  SAFE_TO_CROSS  ~40%  (nominal operations)
  WAIT           ~30%  (common traffic scenario)
  UNSAFE         ~22%  (belt operations)
  EMERGENCY       ~8%  (rare but critical — class weights compensate)
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Dict, Optional, Tuple

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Synthetic data generation
# ---------------------------------------------------------------------------


def generate_synthetic_training_data(
    n_samples: int = 12_000,
    random_state: int = 42,
) -> pd.DataFrame:
    """
    Generate synthetic road crossing scenarios.

    Returns a DataFrame with columns matching the feature set used by
    RoadCrossingModel plus a 'label' column (0-3) and 'scenario' description.
    """
    rng = np.random.default_rng(random_state)

    # Scenario proportions
    n_safe = int(n_samples * 0.40)
    n_wait = int(n_samples * 0.30)
    n_unsafe = int(n_samples * 0.22)
    n_emergency = n_samples - n_safe - n_wait - n_unsafe

    rows = []

    # ---- SAFE_TO_CROSS (label=0) ----
    # Belt stopped, adequate gap, no vehicles or slow/distant vehicles
    for _ in range(n_safe):
        rows.append({
            "belt_speed": 0.0,
            "belt_running": 0,
            "vehicle_speed": float(rng.uniform(0, 15)),
            "vehicle_class": int(rng.choice([0, 1, 2], p=[0.5, 0.35, 0.15])),
            "loop_detector_near": 0,
            "loop_detector_far": int(rng.choice([0, 1], p=[0.8, 0.2])),
            "lidar_distance_m": float(rng.uniform(50, 200)),
            "lidar_count": int(rng.choice([0, 1], p=[0.7, 0.3])),
            "hour_of_day": float(rng.integers(6, 20)),
            "day_of_week": float(rng.integers(0, 7)),
            "weather_code": int(rng.choice([0, 1], p=[0.85, 0.15])),
            "label": 0,
            "scenario": "safe_to_cross",
        })

    # ---- WAIT (label=1) ----
    # Belt stopped but vehicle approaching within 10-40m, or marginal gap
    for _ in range(n_wait):
        veh_speed = float(rng.uniform(5, 50))
        rows.append({
            "belt_speed": float(rng.choice([0.0, float(rng.uniform(0.1, 1.0))], p=[0.7, 0.3])),
            "belt_running": 0,
            "vehicle_speed": veh_speed,
            "vehicle_class": int(rng.choice([0, 1, 2, 3], p=[0.1, 0.4, 0.35, 0.15])),
            "loop_detector_near": int(rng.choice([0, 1], p=[0.3, 0.7])),
            "loop_detector_far": 1,
            "lidar_distance_m": float(rng.uniform(10, 40)),
            "lidar_count": int(rng.choice([1, 2], p=[0.7, 0.3])),
            "hour_of_day": float(rng.integers(0, 24)),
            "day_of_week": float(rng.integers(0, 7)),
            "weather_code": int(rng.choice([0, 1, 2], p=[0.6, 0.3, 0.1])),
            "label": 1,
            "scenario": "wait",
        })

    # ---- UNSAFE (label=2) ----
    # Belt running or vehicle very close, or severe weather + high speed
    for _ in range(n_unsafe):
        belt_on = rng.choice([True, False], p=[0.65, 0.35])
        rows.append({
            "belt_speed": float(rng.uniform(2.0, 6.0)) if belt_on else float(rng.uniform(0, 1.5)),
            "belt_running": int(belt_on),
            "vehicle_speed": float(rng.uniform(20, 80)),
            "vehicle_class": int(rng.choice([1, 2, 3], p=[0.3, 0.4, 0.3])),
            "loop_detector_near": 1,
            "loop_detector_far": 1,
            "lidar_distance_m": float(rng.uniform(2, 20)),
            "lidar_count": int(rng.choice([1, 2, 3], p=[0.4, 0.4, 0.2])),
            "hour_of_day": float(rng.integers(0, 24)),
            "day_of_week": float(rng.integers(0, 7)),
            "weather_code": int(rng.choice([0, 1, 2, 3], p=[0.4, 0.3, 0.2, 0.1])),
            "label": 2,
            "scenario": "unsafe",
        })

    # ---- EMERGENCY (label=3) ----
    # Belt running at speed + fast vehicle very close, or multiple compounding factors
    for _ in range(n_emergency):
        rows.append({
            "belt_speed": float(rng.uniform(3.5, 6.0)),
            "belt_running": 1,
            "vehicle_speed": float(rng.uniform(50, 120)),
            "vehicle_class": int(rng.choice([2, 3], p=[0.4, 0.6])),
            "loop_detector_near": 1,
            "loop_detector_far": 1,
            "lidar_distance_m": float(rng.uniform(1, 8)),
            "lidar_count": int(rng.choice([2, 3, 4], p=[0.4, 0.4, 0.2])),
            "hour_of_day": float(rng.choice(
                [float(rng.integers(0, 6)), float(rng.integers(20, 24))],  # night time
                p=[0.5, 0.5],
            )),
            "day_of_week": float(rng.integers(0, 7)),
            "weather_code": int(rng.choice([2, 3], p=[0.5, 0.5])),  # fog or snow
            "label": 3,
            "scenario": "emergency",
        })

    # Shuffle
    rng.shuffle(rows)
    df = pd.DataFrame(rows)
    df["sample_id"] = range(len(df))

    label_dist = df["label"].value_counts().sort_index().to_dict()
    logger.info(
        "Generated %d synthetic crossing samples. Distribution: %s",
        len(df), {SAFETY_LABELS[k]: v for k, v in label_dist.items()},
    )
    return df


SAFETY_LABELS = {0: "SAFE_TO_CROSS", 1: "WAIT", 2: "UNSAFE", 3: "EMERGENCY"}


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------


def _extract_features(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray]:
    """Extract feature matrix and label vector from synthetic DataFrame."""
    from ai.road_crossing.model import _build_crossing_features

    feature_rows = []
    for _, row in df.iterrows():
        sensor = row.to_dict()
        fv = _build_crossing_features(sensor)
        feature_rows.append(fv)

    X = np.vstack(feature_rows).astype(np.float32)
    y = df["label"].values.astype(np.int32)
    return X, y


# ---------------------------------------------------------------------------
# Training pipeline
# ---------------------------------------------------------------------------


class RoadCrossingTrainingPipeline:
    """
    End-to-end training pipeline for the road crossing safety model.

    Usage
    -----
    pipeline = RoadCrossingTrainingPipeline()
    pipeline.run(
        data_path=None,           # None = use synthetic
        output_path="/models/road_crossing",
        mlflow_experiment="road_crossing_safety",
    )
    """

    def run(
        self,
        data_path: Optional[str],
        output_path: str,
        mlflow_experiment: str = "road_crossing_safety",
        n_synthetic_samples: int = 12_000,
    ) -> None:
        """
        Orchestrate the full training pipeline.

        Parameters
        ----------
        data_path           : Path to labeled CSV (see data_loader.load_labeled_events).
                              If None, synthetic data is generated.
        output_path         : Directory to save trained model artifacts.
        mlflow_experiment   : MLflow experiment name.
        n_synthetic_samples : Number of synthetic samples when data_path is None.
        """
        from ai.common.data_loader import (
            load_labeled_events,
            train_test_split_temporal,
            augment_minority_class,
        )
        from ai.road_crossing.model import RoadCrossingModel
        from ai.common.model_registry import ModelRegistry

        pipeline_start = time.monotonic()
        logger.info("=" * 60)
        logger.info("Road Crossing Safety Training Pipeline — Flux OT AI")
        logger.info("=" * 60)

        # ------------------------------------------------------------------
        # 1. Load or generate data
        # ------------------------------------------------------------------
        if data_path and Path(data_path).exists():
            logger.info("Loading labeled data from %s", data_path)
            df = load_labeled_events(data_path)
        else:
            if data_path:
                logger.warning("Data path %s not found — generating synthetic data", data_path)
            else:
                logger.info("No data path provided — generating synthetic data")
            df = generate_synthetic_training_data(n_samples=n_synthetic_samples)

        label_dist = df["label"].value_counts().sort_index()
        logger.info("Dataset: %d samples\n  %s", len(df), label_dist.to_string())

        # ------------------------------------------------------------------
        # 2. Feature extraction
        # ------------------------------------------------------------------
        logger.info("Extracting features ...")
        X, y = _extract_features(df)
        logger.info("Feature matrix: %s", X.shape)

        # ------------------------------------------------------------------
        # 3. Temporal train/test split
        # ------------------------------------------------------------------
        X_train, X_test, y_train, y_test = train_test_split_temporal(X, y, test_ratio=0.2)

        # ------------------------------------------------------------------
        # 4. Augment minority classes (EMERGENCY)
        # ------------------------------------------------------------------
        logger.info("Augmenting minority classes ...")
        X_train_aug, y_train_aug = augment_minority_class(
            X_train, y_train, factor=4, random_state=42
        )
        logger.info(
            "Post-augmentation train: %d samples, distribution: %s",
            len(y_train_aug),
            dict(zip(*np.unique(y_train_aug, return_counts=True))),
        )

        # Train/val split within augmented training set
        split = int(len(X_train_aug) * 0.85)
        X_tr, X_val = X_train_aug[:split], X_train_aug[split:]
        y_tr, y_val = y_train_aug[:split], y_train_aug[split:]

        # ------------------------------------------------------------------
        # 5. Train model
        # ------------------------------------------------------------------
        logger.info("Training road crossing safety model ...")
        model = RoadCrossingModel(random_state=42)
        result = model.train(X_tr, y_tr, X_val, y_val)

        # ------------------------------------------------------------------
        # 6. Test set evaluation
        # ------------------------------------------------------------------
        logger.info("Evaluating on held-out test set ...")
        self._evaluate_on_test_set(model, X_test, y_test, output_path)

        # ------------------------------------------------------------------
        # 7. Save model
        # ------------------------------------------------------------------
        model.model_version = f"v{int(time.time())}"
        model.save(output_path)

        # ------------------------------------------------------------------
        # 8. Register with MLflow
        # ------------------------------------------------------------------
        registry = ModelRegistry()
        version = registry.register_model(
            name="road_crossing_safety",
            model=model._clf,
            metrics={
                "accuracy": result.accuracy,
                "macro_f1": result.macro_f1,
                "weighted_f1": result.weighted_f1,
                "auc_roc_ovr": result.auc_roc_ovr,
            },
            params=result.best_params,
            tags={
                "model_type": "RoadCrossingModel",
                "classifier": "xgboost" if model._use_xgboost else "gradient_boosting",
                "n_classes": "4",
                "service": "fluxot-road-crossing",
            },
        )
        logger.info("Registered crossing model as version %s", version)

        # ------------------------------------------------------------------
        # 9. Training report
        # ------------------------------------------------------------------
        elapsed = time.monotonic() - pipeline_start
        logger.info("")
        logger.info("=" * 60)
        logger.info("TRAINING COMPLETE")
        logger.info("  Total pipeline time : %.1fs", elapsed)
        logger.info("  Model version       : %s", version)
        logger.info("  Output path         : %s", output_path)
        logger.info("  Validation metrics:")
        logger.info("    Accuracy          : %.4f", result.accuracy)
        logger.info("    Macro F1          : %.4f", result.macro_f1)
        logger.info("    Weighted F1       : %.4f", result.weighted_f1)
        logger.info("    AUC-ROC (OvR)     : %.4f", result.auc_roc_ovr)
        logger.info("  Per-class F1:")
        for cls_name, f1_val in result.per_class_f1.items():
            logger.info("    %-20s: %.4f", cls_name, f1_val)
        logger.info("=" * 60)

    def _evaluate_on_test_set(
        self,
        model: "RoadCrossingModel",
        X_test: np.ndarray,
        y_test: np.ndarray,
        output_path: str,
    ) -> None:
        """Evaluate on the held-out test set and save metrics."""
        from sklearn.metrics import classification_report, confusion_matrix

        y_pred = model._clf.predict(X_test)
        report = classification_report(
            y_test,
            y_pred,
            target_names=[SAFETY_LABELS[i] for i in sorted(SAFETY_LABELS)],
            zero_division=0,
        )
        cm = confusion_matrix(y_test, y_pred, labels=sorted(SAFETY_LABELS)).tolist()

        logger.info("\nTest Set Classification Report:\n%s", report)
        logger.info("Test Confusion Matrix: %s", cm)

        out_dir = Path(output_path)
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "test_metrics.json").write_text(
            json.dumps(
                {
                    "test_classification_report": report,
                    "test_confusion_matrix": cm,
                    "test_n_samples": int(len(y_test)),
                    "class_names": list(SAFETY_LABELS.values()),
                },
                indent=2,
            )
        )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flux OT — Road Crossing Safety Model Training"
    )
    parser.add_argument(
        "--data-path",
        type=str,
        default=None,
        help="Path to labeled CSV file.  If omitted, synthetic data is generated.",
    )
    parser.add_argument(
        "--output-path",
        type=str,
        default="/models/road_crossing",
        help="Directory to save trained model artifacts.",
    )
    parser.add_argument(
        "--mlflow-experiment",
        type=str,
        default="road_crossing_safety",
        help="MLflow experiment name.",
    )
    parser.add_argument(
        "--n-synthetic-samples",
        type=int,
        default=12_000,
        help="Number of synthetic samples to generate when --data-path is not provided.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    pipeline = RoadCrossingTrainingPipeline()
    pipeline.run(
        data_path=args.data_path,
        output_path=args.output_path,
        mlflow_experiment=args.mlflow_experiment,
        n_synthetic_samples=args.n_synthetic_samples,
    )
