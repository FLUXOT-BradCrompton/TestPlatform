"""
Belt Rip Detection — Training Pipeline (Flux OT AI)

Orchestrates end-to-end model training:
  1. Load or generate data
  2. Feature engineering
  3. Temporal train/test split
  4. Model training with cross-validation
  5. Evaluation (confusion matrix, ROC, PR curves saved to output_path)
  6. Save model + register with MLflow
  7. Print training report

Synthetic data generator creates realistic belt-rip scenarios:
  - Normal operation with Gaussian noise around nominal values
  - Rip events: magnetic field drop, acoustic spike, load cell asymmetry
  - Pre-rip deterioration: gradual trend over 50-200 samples
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

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

# Nominal sensor baseline values (engineering units)
_NOMINAL = {
    "mag_ch1": 850.0,   # mT
    "mag_ch2": 845.0,
    "mag_ch3": 848.0,
    "mag_ch4": 852.0,
    "load_cell_left": 2500.0,   # kg
    "load_cell_right": 2480.0,
    "acoustic_level": 62.0,     # dB
    "belt_speed": 4.2,           # m/s
    "motor_current": 185.0,      # A
    "motor_vibration": 1.8,      # mm/s RMS
}

_NOISE_STD = {
    "mag_ch1": 15.0,
    "mag_ch2": 15.0,
    "mag_ch3": 15.0,
    "mag_ch4": 15.0,
    "load_cell_left": 50.0,
    "load_cell_right": 50.0,
    "acoustic_level": 3.0,
    "belt_speed": 0.15,
    "motor_current": 8.0,
    "motor_vibration": 0.3,
}


def generate_synthetic_training_data(
    n_samples: int = 10_000,
    rip_ratio: float = 0.08,
    pre_rip_ratio: float = 0.05,
    random_state: int = 42,
) -> pd.DataFrame:
    """
    Generate a realistic synthetic dataset for belt rip detection training.

    Class labels:
        0 = NORMAL
        1 = RIP_EVENT  (active rip detected)

    The dataset also encodes a "pre_rip" window (gradual deterioration) which
    is labelled as RIP_EVENT (class 1) to train the model to detect early signs.

    Columns
    -------
    One column per sensor channel, plus:
        label        : 0 or 1
        event_type   : "normal", "pre_rip", "rip"
        sample_id    : sequential index

    Parameters
    ----------
    n_samples      : Total number of samples to generate.
    rip_ratio      : Fraction of samples that are active rip events.
    pre_rip_ratio  : Fraction of samples in the pre-rip deterioration window.
    random_state   : NumPy RNG seed.
    """
    rng = np.random.default_rng(random_state)
    channels = list(_NOMINAL.keys())

    n_rip = int(n_samples * rip_ratio)
    n_pre_rip = int(n_samples * pre_rip_ratio)
    n_normal = n_samples - n_rip - n_pre_rip

    rows = []

    # ---- Normal operation ----
    for _ in range(n_normal):
        row = {ch: float(rng.normal(_NOMINAL[ch], _NOISE_STD[ch])) for ch in channels}
        row["label"] = 0
        row["event_type"] = "normal"
        rows.append(row)

    # ---- Pre-rip deterioration (gradual trend, labelled as positive) ----
    for i in range(n_pre_rip):
        progress = i / max(n_pre_rip - 1, 1)  # 0 -> 1 as deterioration worsens
        row: dict = {}
        for ch in channels:
            base = _NOMINAL[ch]
            noise = float(rng.normal(0, _NOISE_STD[ch]))
            if ch.startswith("mag_ch"):
                # Magnetic field gradually weakens (indicates belt delamination)
                drift = -progress * 0.25 * base
            elif ch == "acoustic_level":
                # Increasing acoustic noise
                drift = progress * 0.15 * base
            elif ch in ("load_cell_left", "load_cell_right"):
                # Growing asymmetry — one side drifts
                drift = progress * 0.10 * base * (1 if ch == "load_cell_left" else -1)
            elif ch == "motor_vibration":
                drift = progress * 0.20 * base
            else:
                drift = 0.0
            row[ch] = base + drift + noise
        row["label"] = 1
        row["event_type"] = "pre_rip"
        rows.append(row)

    # ---- Active rip event ----
    for _ in range(n_rip):
        row = {}
        for ch in channels:
            base = _NOMINAL[ch]
            noise = float(rng.normal(0, _NOISE_STD[ch]))
            if ch.startswith("mag_ch"):
                # Sudden large drop in magnetic field (rip passes sensor)
                drop = rng.uniform(0.4, 0.75) * base
                row[ch] = base - drop + noise
            elif ch == "acoustic_level":
                # Significant acoustic spike
                spike = rng.uniform(0.3, 0.8) * base
                row[ch] = base + spike + abs(noise)
            elif ch == "load_cell_left":
                # Heavy asymmetry
                row[ch] = base * rng.uniform(1.1, 1.4) + noise
            elif ch == "load_cell_right":
                row[ch] = base * rng.uniform(0.6, 0.85) + noise
            elif ch == "motor_current":
                # Reduced load after rip (belt tension lost)
                row[ch] = base * rng.uniform(0.75, 0.90) + noise
            elif ch == "motor_vibration":
                row[ch] = base * rng.uniform(1.5, 3.0) + abs(noise)
            else:
                row[ch] = float(rng.normal(base, _NOISE_STD[ch]))
        row["label"] = 1
        row["event_type"] = "rip"
        rows.append(row)

    # Shuffle and build DataFrame
    rng.shuffle(rows)
    df = pd.DataFrame(rows)
    df["sample_id"] = range(len(df))

    logger.info(
        "Generated %d synthetic samples: normal=%d, pre_rip=%d, rip=%d",
        n_samples, n_normal, n_pre_rip, n_rip,
    )
    return df


# ---------------------------------------------------------------------------
# Feature extraction from sensor DataFrame
# ---------------------------------------------------------------------------


def _extract_features_from_df(df: pd.DataFrame) -> Tuple[np.ndarray, np.ndarray, list]:
    """
    Build a (n_samples, n_features) matrix and label vector from the
    raw sensor DataFrame produced by generate_synthetic_training_data
    or loaded from database.
    """
    from ai.common.feature_engineering import (
        extract_statistical_features,
        build_feature_vector,
    )

    channels = [c for c in df.columns if c not in ("label", "event_type", "sample_id")]
    feature_rows = []
    feature_names: Optional[list] = None

    for _, row in df.iterrows():
        sensor_data = {ch: float(row[ch]) for ch in channels}
        fv = build_feature_vector(sensor_data)
        feature_rows.append(fv)
        if feature_names is None:
            # Build names to match build_feature_vector layout
            from ai.common.feature_engineering import _BELT_SENSOR_CHANNELS
            names = []
            for ch in _BELT_SENSOR_CHANNELS:
                val = sensor_data.get(ch, 0.0)
                if isinstance(val, (list, np.ndarray)):
                    for suffix in ("mean", "std", "rms", "peak", "crest", "kurtosis"):
                        names.append(f"{ch}_{suffix}")
                else:
                    names.append(ch)
            names += ["load_asymmetry", "mag_mean", "mag_std"]
            feature_names = names

    X = np.vstack(feature_rows).astype(np.float32)
    y = df["label"].values.astype(np.int32)
    return X, y, feature_names or []


# ---------------------------------------------------------------------------
# Training pipeline
# ---------------------------------------------------------------------------


class BeltRipTrainingPipeline:
    """
    End-to-end training pipeline for the belt rip detection model.

    Usage
    -----
    pipeline = BeltRipTrainingPipeline()
    pipeline.run(
        data_path="/data/belt_rip_labeled.csv",  # or None to use synthetic
        output_path="/models/belt_rip",
        mlflow_experiment="belt_rip_detection",
    )
    """

    def run(
        self,
        data_path: Optional[str],
        output_path: str,
        mlflow_experiment: str = "belt_rip_detection",
        n_synthetic_samples: int = 15_000,
        use_lstm: bool = False,
    ) -> None:
        """
        Orchestrate the full training pipeline.

        Parameters
        ----------
        data_path            : Path to labeled CSV (see data_loader.load_labeled_events).
                               If None, synthetic data is generated.
        output_path          : Directory to save trained model artifacts.
        mlflow_experiment    : MLflow experiment name for tracking.
        n_synthetic_samples  : Number of synthetic samples when data_path is None.
        use_lstm             : Enable LSTM tier training (requires PyTorch + GPU).
        """
        from ai.common.data_loader import (
            load_labeled_events,
            train_test_split_temporal,
            augment_minority_class,
        )
        from ai.belt_rip.model import BeltRipModel
        from ai.common.model_registry import ModelRegistry

        pipeline_start = time.monotonic()
        logger.info("=" * 60)
        logger.info("Belt Rip Training Pipeline — Flux OT AI")
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

        logger.info("Dataset: %d samples, label distribution: %s",
                    len(df), dict(df["label"].value_counts().to_dict()))

        # ------------------------------------------------------------------
        # 2. Feature engineering
        # ------------------------------------------------------------------
        logger.info("Extracting features ...")
        X, y, feature_names = _extract_features_from_df(df)
        logger.info("Feature matrix shape: %s, %d unique features", X.shape, len(feature_names))

        # ------------------------------------------------------------------
        # 3. Temporal train/test split
        # ------------------------------------------------------------------
        X_train, X_test, y_train, y_test = train_test_split_temporal(X, y, test_ratio=0.2)

        # ------------------------------------------------------------------
        # 4. Minority class augmentation on training set only
        # ------------------------------------------------------------------
        logger.info("Augmenting minority class ...")
        X_train_aug, y_train_aug = augment_minority_class(X_train, y_train, factor=3)
        logger.info(
            "Post-augmentation train set: %d samples, distribution: %s",
            len(y_train_aug),
            dict(zip(*np.unique(y_train_aug, return_counts=True))),
        )

        # Split augmented training set into train/val
        split = int(len(X_train_aug) * 0.85)
        X_tr, X_val = X_train_aug[:split], X_train_aug[split:]
        y_tr, y_val = y_train_aug[:split], y_train_aug[split:]

        # ------------------------------------------------------------------
        # 5. Model training
        # ------------------------------------------------------------------
        logger.info("Training model (use_lstm=%s) ...", use_lstm)
        model = BeltRipModel(use_lstm=use_lstm, feature_names=feature_names)
        result = model.train(X_tr, y_tr, X_val, y_val)

        # ------------------------------------------------------------------
        # 6. Hold-out test evaluation
        # ------------------------------------------------------------------
        logger.info("Evaluating on held-out test set ...")
        self._print_evaluation_report(model, X_test, y_test, output_path)

        # ------------------------------------------------------------------
        # 7. Save model
        # ------------------------------------------------------------------
        model.save(output_path)

        # ------------------------------------------------------------------
        # 8. Register with MLflow
        # ------------------------------------------------------------------
        registry = ModelRegistry()
        version = registry.register_model(
            name="belt_rip_detector",
            model=model._rf,
            metrics={
                "accuracy": result.accuracy,
                "precision": result.precision,
                "recall": result.recall,
                "f1": result.f1,
                "auc_roc": result.auc_roc,
            },
            params=result.best_params,
            tags={
                "model_type": "BeltRipModel",
                "active_tier": model._active_tier,
                "framework": "sklearn+pytorch",
                "service": "fluxot-belt-rip",
            },
        )
        logger.info("Registered model as version %s", version)

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
        logger.info("    Accuracy  : %.4f", result.accuracy)
        logger.info("    Precision : %.4f", result.precision)
        logger.info("    Recall    : %.4f", result.recall)
        logger.info("    F1-Score  : %.4f", result.f1)
        logger.info("    AUC-ROC   : %.4f", result.auc_roc)
        logger.info("=" * 60)

    def _print_evaluation_report(
        self,
        model: "BeltRipModel",
        X_test: np.ndarray,
        y_test: np.ndarray,
        output_path: str,
    ) -> None:
        """Evaluate model on test set and save plots + confusion matrix."""
        from sklearn.metrics import classification_report, confusion_matrix

        y_pred = model._predict_class(X_test)
        y_proba = model.predict_proba(X_test)[:, 1]

        report = classification_report(
            y_test, y_pred, target_names=["NORMAL", "RIP_DETECTED"]
        )
        cm = confusion_matrix(y_test, y_pred).tolist()

        logger.info("\nClassification Report (Test Set):\n%s", report)
        logger.info("Confusion Matrix: %s", cm)

        # Save metrics JSON
        out_dir = Path(output_path)
        out_dir.mkdir(parents=True, exist_ok=True)

        metrics = {
            "test_classification_report": report,
            "test_confusion_matrix": cm,
            "test_n_samples": int(len(y_test)),
            "test_positive_rate": float(y_test.mean()),
        }
        (out_dir / "test_metrics.json").write_text(json.dumps(metrics, indent=2))

        # Save ROC curve data
        try:
            from sklearn.metrics import roc_curve, auc as sk_auc, precision_recall_curve

            fpr, tpr, _ = roc_curve(y_test, y_proba)
            roc_auc = sk_auc(fpr, tpr)
            precision, recall, _ = precision_recall_curve(y_test, y_proba)
            pr_auc = sk_auc(recall, precision)

            curve_data = {
                "roc": {"fpr": fpr.tolist(), "tpr": tpr.tolist(), "auc": float(roc_auc)},
                "pr": {"precision": precision.tolist(), "recall": recall.tolist(), "auc": float(pr_auc)},
            }
            (out_dir / "evaluation_curves.json").write_text(json.dumps(curve_data, indent=2))
            logger.info("Test AUC-ROC=%.4f, PR-AUC=%.4f", roc_auc, pr_auc)
        except Exception as exc:
            logger.warning("Could not compute curves: %s", exc)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Flux OT — Belt Rip Detection Model Training"
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
        default="/models/belt_rip",
        help="Directory to save trained model artifacts.",
    )
    parser.add_argument(
        "--mlflow-experiment",
        type=str,
        default="belt_rip_detection",
        help="MLflow experiment name.",
    )
    parser.add_argument(
        "--n-synthetic-samples",
        type=int,
        default=15_000,
        help="Number of synthetic samples to generate when --data-path is not provided.",
    )
    parser.add_argument(
        "--use-lstm",
        action="store_true",
        default=False,
        help="Also train the LSTM tier (requires PyTorch; uses GPU if available).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    pipeline = BeltRipTrainingPipeline()
    pipeline.run(
        data_path=args.data_path,
        output_path=args.output_path,
        mlflow_experiment=args.mlflow_experiment,
        n_synthetic_samples=args.n_synthetic_samples,
        use_lstm=args.use_lstm,
    )
