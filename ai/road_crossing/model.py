"""
Road Crossing Safety AI Model — Flux OT AI

Multi-class classification for conveyor belt road crossing safety decisions.

Output states
-------------
  0  SAFE_TO_CROSS  — adequate gap, belt stopped, no hazards
  1  WAIT           — marginal conditions, short gap, vehicle approaching
  2  UNSAFE         — active belt hazard or insufficient gap
  3  EMERGENCY      — immediate danger (fast vehicle + belt running, system fault)

Hard safety constraint (non-negotiable, applied post-model)
-----------------------------------------------------------
  If belt_running == True AND belt_speed > 0 → output is always UNSAFE (state 2),
  regardless of model output.  Safety-critical systems must fail safe.

Uncertainty quantification
--------------------------
  If model confidence < 0.70 → LOW_CONFIDENCE added to risk_factors,
  which triggers human review flag in the recommendation.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional

import joblib
import numpy as np

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Safety state enumeration
# ---------------------------------------------------------------------------

SAFETY_STATE_LABELS = {
    0: "SAFE_TO_CROSS",
    1: "WAIT",
    2: "UNSAFE",
    3: "EMERGENCY",
}

SAFETY_STATE_REVERSE = {v: k for k, v in SAFETY_STATE_LABELS.items()}


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class CrossingSafetyPrediction:
    """Output of a single road crossing safety inference call."""

    safety_state: int                   # 0=SAFE_TO_CROSS, 1=WAIT, 2=UNSAFE, 3=EMERGENCY
    confidence: float                   # [0.0, 1.0]
    estimated_wait_s: Optional[float]   # Estimated seconds until safe if state=WAIT
    risk_factors: List[str]             # Human-readable risk factor list
    recommended_action: str             # Operator-facing instruction
    latency_ms: float = 0.0
    model_version: str = "unknown"

    @property
    def state_label(self) -> str:
        return SAFETY_STATE_LABELS.get(self.safety_state, "UNKNOWN")

    @property
    def human_review_required(self) -> bool:
        return "LOW_CONFIDENCE" in self.risk_factors or self.safety_state == 3

    def as_dict(self) -> dict:
        d = asdict(self)
        d["state_label"] = self.state_label
        d["human_review_required"] = self.human_review_required
        return d


@dataclass
class CrossingTrainingResult:
    """Summary metrics from a crossing model training run."""

    accuracy: float
    macro_f1: float
    weighted_f1: float
    per_class_precision: Dict[str, float]
    per_class_recall: Dict[str, float]
    per_class_f1: Dict[str, float]
    auc_roc_ovr: float
    training_time_s: float
    best_params: Dict[str, Any]
    cv_scores: List[float] = field(default_factory=list)
    confusion_matrix: List[List[int]] = field(default_factory=list)

    def __str__(self) -> str:
        return (
            f"CrossingTrainingResult("
            f"acc={self.accuracy:.4f}, macro_f1={self.macro_f1:.4f}, "
            f"weighted_f1={self.weighted_f1:.4f}, auc_roc={self.auc_roc_ovr:.4f}, "
            f"train_time={self.training_time_s:.1f}s)"
        )


# ---------------------------------------------------------------------------
# Feature extraction
# ---------------------------------------------------------------------------

_FEATURE_NAMES = [
    "belt_speed",
    "belt_running",
    "vehicle_speed",
    "vehicle_class",       # encoded: 0=none, 1=light, 2=heavy, 3=oversized
    "loop_detector_near",  # 0/1
    "loop_detector_far",   # 0/1
    "lidar_distance_m",    # nearest detected object distance
    "lidar_count",         # number of objects in detection zone
    "hour_of_day",         # 0-23 (encoded as sin/cos in feature vector)
    "day_of_week",         # 0-6
    "weather_code",        # 0=clear, 1=rain, 2=fog, 3=snow
    # Derived features
    "belt_running_x_speed",
    "vehicle_ttc_s",       # time-to-crossing = lidar_distance / max(vehicle_speed, 0.1)
    "hour_sin",
    "hour_cos",
]


def _build_crossing_features(sensor_data: Dict[str, Any]) -> np.ndarray:
    """
    Build a flat feature vector from road crossing sensor data.

    Parameters
    ----------
    sensor_data : dict with keys matching _FEATURE_NAMES (raw sensor values).

    Returns
    -------
    1-D np.ndarray (float32) of length len(_FEATURE_NAMES).
    """
    belt_speed = float(sensor_data.get("belt_speed", 0.0))
    belt_running = float(bool(sensor_data.get("belt_running", False)))
    vehicle_speed = float(sensor_data.get("vehicle_speed", 0.0))
    vehicle_class = float(sensor_data.get("vehicle_class", 0))
    loop_near = float(bool(sensor_data.get("loop_detector_near", False)))
    loop_far = float(bool(sensor_data.get("loop_detector_far", False)))
    lidar_dist = float(sensor_data.get("lidar_distance_m", 999.0))
    lidar_count = float(sensor_data.get("lidar_count", 0))
    hour = float(sensor_data.get("hour_of_day", 12))
    dow = float(sensor_data.get("day_of_week", 0))
    weather = float(sensor_data.get("weather_code", 0))

    # Derived
    belt_run_x_speed = belt_running * belt_speed
    ttc = lidar_dist / max(vehicle_speed, 0.1)
    hour_sin = np.sin(2 * np.pi * hour / 24.0)
    hour_cos = np.cos(2 * np.pi * hour / 24.0)

    return np.array(
        [
            belt_speed, belt_running, vehicle_speed, vehicle_class,
            loop_near, loop_far, lidar_dist, lidar_count,
            hour, dow, weather,
            belt_run_x_speed, ttc, hour_sin, hour_cos,
        ],
        dtype=np.float32,
    )


# ---------------------------------------------------------------------------
# RoadCrossingModel
# ---------------------------------------------------------------------------


class RoadCrossingModel:
    """
    Road crossing safety classifier.

    Primary: Gradient Boosting (sklearn or XGBoost if available).
    Hard safety constraint applied post-inference.
    """

    _GB_PARAM_GRID = [
        {
            "n_estimators": 300,
            "max_depth": 6,
            "learning_rate": 0.05,
            "subsample": 0.8,
            "min_samples_leaf": 5,
        },
        {
            "n_estimators": 200,
            "max_depth": 4,
            "learning_rate": 0.1,
            "subsample": 0.9,
            "min_samples_leaf": 3,
        },
        {
            "n_estimators": 400,
            "max_depth": 8,
            "learning_rate": 0.03,
            "subsample": 0.7,
            "min_samples_leaf": 8,
        },
    ]

    def __init__(self, random_state: int = 42) -> None:
        self.random_state = random_state
        self._clf: Optional[Any] = None
        self._use_xgboost: bool = False
        self._training_result: Optional[CrossingTrainingResult] = None
        self._model_card: dict = {}
        self.model_version: str = "untrained"

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        class_weights: Optional[Dict[int, float]] = None,
    ) -> CrossingTrainingResult:
        """
        Train the road crossing safety classifier.

        Parameters
        ----------
        X_train, y_train : Training data.
        X_val,   y_val   : Held-out validation data.
        class_weights    : Per-class weight dict.  If None, balanced weights
                           are computed from class frequencies.
        """
        import time as _time
        from sklearn.model_selection import StratifiedKFold
        from sklearn.metrics import f1_score

        start = _time.monotonic()

        if X_train.ndim > 2:
            X_train = X_train.reshape(len(X_train), -1)
        if X_val.ndim > 2:
            X_val = X_val.reshape(len(X_val), -1)

        # Compute balanced class weights if not provided
        if class_weights is None:
            class_weights = self._compute_class_weights(y_train)

        # Try XGBoost first; fall back to sklearn GradientBoosting
        self._use_xgboost = self._xgboost_available()

        best_clf, best_params, cv_scores = self._tune_classifier(
            X_train, y_train, class_weights
        )
        self._clf = best_clf

        result = self._evaluate(X_val, y_val, best_params, cv_scores, start)
        self._training_result = result

        self._model_card = {
            "model_type": "RoadCrossingModel",
            "classifier": "xgboost" if self._use_xgboost else "gradient_boosting",
            "best_params": best_params,
            "validation_metrics": {
                "accuracy": result.accuracy,
                "macro_f1": result.macro_f1,
                "weighted_f1": result.weighted_f1,
                "auc_roc_ovr": result.auc_roc_ovr,
            },
            "class_weights": class_weights,
            "n_train_samples": len(X_train),
            "n_val_samples": len(X_val),
            "feature_names": _FEATURE_NAMES,
        }

        logger.info("Crossing model training complete: %s", result)
        return result

    def _compute_class_weights(self, y: np.ndarray) -> Dict[int, float]:
        """Compute inverse-frequency class weights."""
        classes, counts = np.unique(y, return_counts=True)
        n_total = len(y)
        n_classes = len(classes)
        weights = {
            int(cls): float(n_total / (n_classes * cnt))
            for cls, cnt in zip(classes, counts)
        }
        # Extra weight for EMERGENCY (class 3) — rare but critical
        if 3 in weights:
            weights[3] *= 3.0
        logger.info("Class weights: %s", weights)
        return weights

    def _xgboost_available(self) -> bool:
        try:
            import xgboost  # noqa: F401
            return True
        except ImportError:
            return False

    def _tune_classifier(
        self,
        X: np.ndarray,
        y: np.ndarray,
        class_weights: Dict[int, float],
    ):
        from sklearn.model_selection import StratifiedKFold
        from sklearn.metrics import f1_score

        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=self.random_state)
        best_score = -1.0
        best_clf = None
        best_params: dict = {}
        best_cv_scores: list = []

        # Build sample_weight array for sklearn
        sample_weights = np.array([class_weights.get(int(lbl), 1.0) for lbl in y])

        for params in self._GB_PARAM_GRID:
            fold_scores = []
            for train_idx, val_idx in cv.split(X, y):
                clf = self._build_classifier(params)
                sw = sample_weights[train_idx]
                try:
                    clf.fit(X[train_idx], y[train_idx], sample_weight=sw)
                except TypeError:
                    clf.fit(X[train_idx], y[train_idx])
                fold_scores.append(
                    f1_score(y[val_idx], clf.predict(X[val_idx]), average="macro", zero_division=0)
                )
            mean_score = float(np.mean(fold_scores))
            logger.debug("Crossing params %s -> CV macro-F1=%.4f", params, mean_score)
            if mean_score > best_score:
                best_score = mean_score
                best_params = params
                best_cv_scores = fold_scores

        # Final fit on full training data
        final_clf = self._build_classifier(best_params)
        try:
            final_clf.fit(X, y, sample_weight=sample_weights)
        except TypeError:
            final_clf.fit(X, y)

        logger.info("Best crossing params: %s (CV macro-F1=%.4f)", best_params, best_score)
        return final_clf, best_params, best_cv_scores

    def _build_classifier(self, params: dict) -> Any:
        if self._use_xgboost:
            import xgboost as xgb
            return xgb.XGBClassifier(
                n_estimators=params["n_estimators"],
                max_depth=params["max_depth"],
                learning_rate=params["learning_rate"],
                subsample=params["subsample"],
                use_label_encoder=False,
                eval_metric="mlogloss",
                random_state=self.random_state,
                n_jobs=-1,
            )
        else:
            from sklearn.ensemble import GradientBoostingClassifier
            return GradientBoostingClassifier(
                n_estimators=params["n_estimators"],
                max_depth=params["max_depth"],
                learning_rate=params["learning_rate"],
                subsample=params["subsample"],
                min_samples_leaf=params.get("min_samples_leaf", 5),
                random_state=self.random_state,
            )

    def _evaluate(
        self,
        X_val: np.ndarray,
        y_val: np.ndarray,
        best_params: dict,
        cv_scores: list,
        start_time: float,
    ) -> CrossingTrainingResult:
        import time as _time
        from sklearn.metrics import (
            accuracy_score, classification_report, confusion_matrix,
            roc_auc_score, f1_score, precision_score, recall_score
        )
        from sklearn.preprocessing import label_binarize

        y_pred = self._clf.predict(X_val)
        y_proba = self._predict_proba_raw(X_val)

        acc = float(accuracy_score(y_val, y_pred))
        macro_f1 = float(f1_score(y_val, y_pred, average="macro", zero_division=0))
        weighted_f1 = float(f1_score(y_val, y_pred, average="weighted", zero_division=0))

        labels = sorted(SAFETY_STATE_LABELS.keys())
        per_precision = {
            SAFETY_STATE_LABELS[i]: float(v)
            for i, v in enumerate(precision_score(y_val, y_pred, average=None, labels=labels, zero_division=0))
        }
        per_recall = {
            SAFETY_STATE_LABELS[i]: float(v)
            for i, v in enumerate(recall_score(y_val, y_pred, average=None, labels=labels, zero_division=0))
        }
        per_f1 = {
            SAFETY_STATE_LABELS[i]: float(v)
            for i, v in enumerate(f1_score(y_val, y_pred, average=None, labels=labels, zero_division=0))
        }

        # AUC-ROC one-vs-rest
        try:
            y_bin = label_binarize(y_val, classes=labels)
            auc_roc = float(roc_auc_score(y_bin, y_proba, multi_class="ovr", average="macro"))
        except Exception:
            auc_roc = 0.0

        cm = confusion_matrix(y_val, y_pred, labels=labels).tolist()
        training_time = _time.monotonic() - start_time

        return CrossingTrainingResult(
            accuracy=acc,
            macro_f1=macro_f1,
            weighted_f1=weighted_f1,
            per_class_precision=per_precision,
            per_class_recall=per_recall,
            per_class_f1=per_f1,
            auc_roc_ovr=auc_roc,
            training_time_s=training_time,
            best_params=best_params,
            cv_scores=cv_scores,
            confusion_matrix=cm,
        )

    # ------------------------------------------------------------------
    # Inference
    # ------------------------------------------------------------------

    def predict(self, sensor_data: Dict[str, Any]) -> CrossingSafetyPrediction:
        """
        Run road crossing safety inference.

        Hard safety constraint is applied before returning:
          belt_running=True AND belt_speed > 0 -> UNSAFE (state 2)

        Parameters
        ----------
        sensor_data : dict of sensor channel values.

        Returns
        -------
        CrossingSafetyPrediction
        """
        if self._clf is None:
            raise RuntimeError("Model has not been trained or loaded")

        t0 = time.monotonic()

        # Apply hard safety constraint BEFORE model inference
        belt_running = bool(sensor_data.get("belt_running", False))
        belt_speed = float(sensor_data.get("belt_speed", 0.0))
        risk_factors: List[str] = []

        if belt_running and belt_speed > 0:
            # Hard override: belt is moving — crossing is always UNSAFE
            risk_factors.append("BELT_RUNNING")
            risk_factors.append("HARD_SAFETY_CONSTRAINT_ACTIVE")
            latency_ms = (time.monotonic() - t0) * 1000.0
            return CrossingSafetyPrediction(
                safety_state=2,
                confidence=1.0,
                estimated_wait_s=None,
                risk_factors=risk_factors,
                recommended_action=(
                    "STOP: Conveyor belt is running. Crossing is UNSAFE. "
                    "Wait for belt to stop and lockout/tagout before crossing."
                ),
                latency_ms=latency_ms,
                model_version=self.model_version,
            )

        # Build feature vector and run model
        features = _build_crossing_features(sensor_data)
        features_2d = features.reshape(1, -1)

        safety_state = int(self._clf.predict(features_2d)[0])
        proba = self._predict_proba_raw(features_2d)[0]
        confidence = float(proba[safety_state])

        # Uncertainty: low confidence triggers human review
        if confidence < 0.70:
            risk_factors.append("LOW_CONFIDENCE")

        # Identify risk factors from sensor values
        risk_factors.extend(self._identify_risk_factors(sensor_data, safety_state))

        # Estimated wait (only meaningful for WAIT state)
        estimated_wait_s = None
        if safety_state == 1:
            vehicle_speed = float(sensor_data.get("vehicle_speed", 0.0))
            lidar_dist = float(sensor_data.get("lidar_distance_m", 999.0))
            if vehicle_speed > 0:
                estimated_wait_s = round(lidar_dist / vehicle_speed + 3.0, 1)
            else:
                estimated_wait_s = 10.0

        recommended_action = self._build_recommendation(
            safety_state, confidence, risk_factors, estimated_wait_s
        )

        latency_ms = (time.monotonic() - t0) * 1000.0
        return CrossingSafetyPrediction(
            safety_state=safety_state,
            confidence=confidence,
            estimated_wait_s=estimated_wait_s,
            risk_factors=risk_factors,
            recommended_action=recommended_action,
            latency_ms=latency_ms,
            model_version=self.model_version,
        )

    def _predict_proba_raw(self, X: np.ndarray) -> np.ndarray:
        """Return raw probability array (n_samples, n_classes)."""
        if hasattr(self._clf, "predict_proba"):
            proba = self._clf.predict_proba(X)
        else:
            # Decision function fallback (e.g. some XGB configs)
            decision = self._clf.decision_function(X)
            # Softmax
            exp_d = np.exp(decision - np.max(decision, axis=1, keepdims=True))
            proba = exp_d / exp_d.sum(axis=1, keepdims=True)
        # Ensure output has 4 columns (one per safety state class)
        n_classes = len(SAFETY_STATE_LABELS)
        if proba.shape[1] < n_classes:
            pad = np.zeros((proba.shape[0], n_classes - proba.shape[1]))
            proba = np.hstack([proba, pad])
        return proba

    @staticmethod
    def _identify_risk_factors(sensor_data: Dict[str, Any], safety_state: int) -> List[str]:
        """Generate a list of human-readable risk factor strings."""
        factors = []
        vehicle_speed = float(sensor_data.get("vehicle_speed", 0.0))
        lidar_dist = float(sensor_data.get("lidar_distance_m", 999.0))
        weather = int(sensor_data.get("weather_code", 0))
        vehicle_class = int(sensor_data.get("vehicle_class", 0))
        hour = int(sensor_data.get("hour_of_day", 12))

        if vehicle_speed > 40:
            factors.append("HIGH_VEHICLE_SPEED")
        if lidar_dist < 20:
            factors.append("VEHICLE_CLOSE_PROXIMITY")
        if weather == 2:
            factors.append("FOG_REDUCED_VISIBILITY")
        elif weather == 3:
            factors.append("SNOW_ICE_CONDITIONS")
        elif weather == 1:
            factors.append("WET_ROAD_CONDITIONS")
        if vehicle_class == 3:
            factors.append("OVERSIZED_VEHICLE")
        elif vehicle_class == 2:
            factors.append("HEAVY_VEHICLE")
        if hour < 6 or hour >= 22:
            factors.append("LOW_LIGHT_CONDITIONS")
        if safety_state == 3:
            factors.append("EMERGENCY_CONDITIONS")

        return factors

    @staticmethod
    def _build_recommendation(
        state: int,
        confidence: float,
        risk_factors: List[str],
        estimated_wait_s: Optional[float],
    ) -> str:
        review_flag = " [HUMAN REVIEW REQUIRED]" if "LOW_CONFIDENCE" in risk_factors else ""

        if state == 0:
            return f"SAFE TO CROSS: All conditions nominal. Proceed with caution.{review_flag}"
        elif state == 1:
            wait_str = f" Estimated wait: {estimated_wait_s:.0f}s." if estimated_wait_s else ""
            return (
                f"WAIT: Vehicle approaching or marginal gap detected.{wait_str} "
                f"Hold position until SAFE_TO_CROSS signal.{review_flag}"
            )
        elif state == 2:
            reasons = ", ".join(r for r in risk_factors if r not in ("LOW_CONFIDENCE",)) or "hazard detected"
            return (
                f"UNSAFE: Do not cross. Active hazard: {reasons}. "
                f"Wait for clearance and verify conditions.{review_flag}"
            )
        else:  # EMERGENCY
            return (
                "EMERGENCY: Immediate danger. Do not cross under any circumstances. "
                "Activate emergency stop. Alert site safety officer immediately."
            )

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        """Save model and metadata to *path* directory."""
        save_dir = Path(path)
        save_dir.mkdir(parents=True, exist_ok=True)

        if self._clf is not None:
            joblib.dump(self._clf, save_dir / "crossing_model.joblib")
            logger.info("Saved crossing classifier to %s", save_dir / "crossing_model.joblib")

        card = {**self._model_card, "model_version": self.model_version}
        (save_dir / "model_card.json").write_text(json.dumps(card, indent=2))
        (save_dir / "feature_names.json").write_text(json.dumps(_FEATURE_NAMES, indent=2))
        logger.info("Crossing model saved to %s", save_dir)

    @classmethod
    def load(cls, path: str) -> "RoadCrossingModel":
        """Load a saved RoadCrossingModel from *path*."""
        load_dir = Path(path)
        instance = cls()

        clf_path = load_dir / "crossing_model.joblib"
        if clf_path.exists():
            instance._clf = joblib.load(clf_path)
            logger.info("Loaded crossing classifier from %s", clf_path)
        else:
            raise FileNotFoundError(f"No classifier artifact at {clf_path}")

        card_path = load_dir / "model_card.json"
        if card_path.exists():
            instance._model_card = json.loads(card_path.read_text())
            instance.model_version = instance._model_card.get("model_version", "unknown")

        # Detect if XGBoost was used
        if hasattr(instance._clf, "get_booster"):
            instance._use_xgboost = True

        return instance
