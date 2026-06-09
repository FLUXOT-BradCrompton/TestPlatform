"""
Belt Rip Detection Model — Flux OT AI

Two-tier detection approach:
  Tier 1 (primary)  : RandomForestClassifier — fast, edge-compatible, no GPU required.
  Tier 2 (optional) : LSTM (PyTorch) — sequence-aware deep detection for
                      when GPU resources are available and richer context is needed.

Both tiers output the same PredictionResult interface.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import joblib
import numpy as np
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    accuracy_score,
    auc,
    f1_score,
    precision_score,
    recall_score,
    roc_curve,
)
from sklearn.model_selection import StratifiedKFold
from sklearn.preprocessing import label_binarize

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass
class TrainingResult:
    """Summary of a completed training run."""

    accuracy: float
    precision: float
    recall: float
    f1: float
    auc_roc: float
    training_time_s: float
    best_params: Dict[str, Any]
    cv_scores: List[float] = field(default_factory=list)
    confusion_matrix: List[List[int]] = field(default_factory=list)

    def as_dict(self) -> dict:
        return asdict(self)

    def __str__(self) -> str:
        return (
            f"TrainingResult("
            f"acc={self.accuracy:.4f}, prec={self.precision:.4f}, "
            f"rec={self.recall:.4f}, f1={self.f1:.4f}, "
            f"auc_roc={self.auc_roc:.4f}, "
            f"train_time={self.training_time_s:.1f}s)"
        )


@dataclass
class PredictionResult:
    """Output of a single belt rip inference call."""

    prediction: int          # 0 = NORMAL, 1 = RIP_DETECTED
    confidence: float        # [0.0, 1.0] — probability for predicted class
    feature_importances: Dict[str, float] = field(default_factory=dict)
    anomaly_score: float = 0.0
    recommendation: str = ""
    latency_ms: float = 0.0
    model_tier: str = "random_forest"

    @property
    def label(self) -> str:
        return "RIP_DETECTED" if self.prediction == 1 else "NORMAL"

    def as_dict(self) -> dict:
        d = asdict(self)
        d["label"] = self.label
        return d


# ---------------------------------------------------------------------------
# LSTM model (optional, PyTorch)
# ---------------------------------------------------------------------------


def _build_lstm(input_size: int, hidden_size: int = 128, num_layers: int = 2) -> Any:
    """Build a PyTorch LSTM classifier for sequence classification."""
    try:
        import torch
        import torch.nn as nn

        class BeltRipLSTM(nn.Module):
            def __init__(self) -> None:
                super().__init__()
                self.lstm = nn.LSTM(
                    input_size=input_size,
                    hidden_size=hidden_size,
                    num_layers=num_layers,
                    batch_first=True,
                    dropout=0.3 if num_layers > 1 else 0.0,
                    bidirectional=False,
                )
                self.layer_norm = nn.LayerNorm(hidden_size)
                self.dropout = nn.Dropout(0.3)
                self.fc = nn.Linear(hidden_size, 2)  # binary: normal vs rip

            def forward(self, x: "torch.Tensor") -> "torch.Tensor":  # type: ignore[name-defined]
                # x: (batch, seq_len, input_size)
                out, (h_n, _) = self.lstm(x)
                # Use last hidden state
                last = h_n[-1]  # (batch, hidden_size)
                last = self.layer_norm(last)
                last = self.dropout(last)
                return self.fc(last)

        return BeltRipLSTM()
    except ImportError:
        return None


# ---------------------------------------------------------------------------
# BeltRipModel
# ---------------------------------------------------------------------------


class BeltRipModel:
    """
    Belt rip detection model with two inference tiers.

    Usage
    -----
    model = BeltRipModel()
    result = model.train(X_train, y_train, X_val, y_val)
    pred   = model.predict(feature_vector)
    model.save("/models/belt_rip_v1")
    """

    # Default RF hyperparameter search space
    _RF_PARAM_GRID = [
        {"n_estimators": 200, "max_depth": 15, "min_samples_leaf": 2, "class_weight": "balanced"},
        {"n_estimators": 300, "max_depth": 20, "min_samples_leaf": 1, "class_weight": "balanced"},
        {"n_estimators": 150, "max_depth": 10, "min_samples_leaf": 3, "class_weight": "balanced"},
    ]

    def __init__(
        self,
        use_lstm: bool = False,
        feature_names: Optional[List[str]] = None,
        random_state: int = 42,
    ) -> None:
        self.use_lstm = use_lstm
        self.feature_names = feature_names or []
        self.random_state = random_state

        self._rf: Optional[RandomForestClassifier] = None
        self._lstm: Optional[Any] = None  # PyTorch module
        self._active_tier: str = "random_forest"
        self._training_result: Optional[TrainingResult] = None
        self._model_card: dict = {}

    # ------------------------------------------------------------------
    # Training
    # ------------------------------------------------------------------

    def train(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
    ) -> TrainingResult:
        """
        Train the RandomForest classifier with cross-validated hyperparameter
        selection, then optionally fine-tune the LSTM tier.

        Parameters
        ----------
        X_train, y_train : Training data (2-D feature matrix + labels).
        X_val,   y_val   : Held-out validation data.

        Returns
        -------
        TrainingResult with evaluation metrics on the validation set.
        """
        start = time.monotonic()

        # Ensure 2-D (flatten windows if needed)
        if X_train.ndim > 2:
            X_train = X_train.reshape(len(X_train), -1)
        if X_val.ndim > 2:
            X_val = X_val.reshape(len(X_val), -1)

        # --- RandomForest with CV-based hyperparameter selection ---
        best_rf, best_params, cv_scores = self._tune_rf(X_train, y_train)
        self._rf = best_rf
        self._active_tier = "random_forest"

        # --- Optional LSTM tier ---
        if self.use_lstm:
            try:
                self._train_lstm(X_train, y_train, X_val, y_val)
                self._active_tier = "lstm"
            except Exception as exc:
                logger.warning("LSTM training failed (%s) — falling back to RF", exc)

        # --- Evaluate on validation set ---
        result = self._evaluate(X_val, y_val, best_params, cv_scores, start)
        self._training_result = result

        # Build model card
        self._model_card = {
            "model_type": "BeltRipModel",
            "active_tier": self._active_tier,
            "best_params": best_params,
            "validation_metrics": {
                "accuracy": result.accuracy,
                "precision": result.precision,
                "recall": result.recall,
                "f1": result.f1,
                "auc_roc": result.auc_roc,
            },
            "n_train_samples": len(X_train),
            "n_val_samples": len(X_val),
            "feature_names": self.feature_names,
        }

        logger.info("Training complete: %s", result)
        return result

    def _tune_rf(
        self,
        X: np.ndarray,
        y: np.ndarray,
    ) -> Tuple[RandomForestClassifier, dict, List[float]]:
        """Cross-validated RF hyperparameter selection."""
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=self.random_state)
        best_score = -1.0
        best_rf: Optional[RandomForestClassifier] = None
        best_params: dict = {}
        best_cv_scores: List[float] = []

        for params in self._RF_PARAM_GRID:
            fold_scores: List[float] = []
            for train_idx, val_idx in cv.split(X, y):
                rf = RandomForestClassifier(
                    random_state=self.random_state, n_jobs=-1, **params
                )
                rf.fit(X[train_idx], y[train_idx])
                fold_scores.append(f1_score(y[val_idx], rf.predict(X[val_idx]), average="binary"))

            mean_score = float(np.mean(fold_scores))
            logger.debug("RF params %s -> CV F1=%.4f", params, mean_score)

            if mean_score > best_score:
                best_score = mean_score
                best_cv_scores = fold_scores
                best_params = params

        # Retrain on full training set with best params
        best_rf = RandomForestClassifier(
            random_state=self.random_state, n_jobs=-1, **best_params
        )
        best_rf.fit(X, y)
        logger.info("Best RF params: %s (CV F1=%.4f)", best_params, best_score)
        return best_rf, best_params, best_cv_scores

    def _train_lstm(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_val: np.ndarray,
        y_val: np.ndarray,
        seq_len: int = 50,
        epochs: int = 30,
        lr: float = 1e-3,
    ) -> None:
        """Train LSTM tier (PyTorch).  Sequences are formed from flat feature arrays."""
        import torch
        import torch.nn as nn
        from torch.utils.data import DataLoader, TensorDataset

        device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        logger.info("LSTM training on device: %s", device)

        n_features = X_train.shape[1]
        self._lstm = _build_lstm(input_size=n_features)
        if self._lstm is None:
            raise RuntimeError("Could not build LSTM — PyTorch not available")

        self._lstm = self._lstm.to(device)

        # Reshape flat feature vector into (batch, 1, features) for LSTM
        Xt = torch.tensor(X_train, dtype=torch.float32).unsqueeze(1)
        yt = torch.tensor(y_train, dtype=torch.long)
        Xv = torch.tensor(X_val, dtype=torch.float32).unsqueeze(1)
        yv = torch.tensor(y_val, dtype=torch.long)

        # Class weight for imbalanced data
        classes, counts = np.unique(y_train, return_counts=True)
        weights = torch.tensor(
            [1.0 / c for c in counts], dtype=torch.float32
        ).to(device)

        dataset = TensorDataset(Xt, yt)
        loader = DataLoader(dataset, batch_size=64, shuffle=True)

        optimizer = torch.optim.Adam(self._lstm.parameters(), lr=lr, weight_decay=1e-4)
        scheduler = torch.optim.lr_scheduler.StepLR(optimizer, step_size=10, gamma=0.5)
        criterion = nn.CrossEntropyLoss(weight=weights)

        best_val_loss = float("inf")
        patience, patience_counter = 5, 0

        for epoch in range(epochs):
            self._lstm.train()
            for xb, yb in loader:
                xb, yb = xb.to(device), yb.to(device)
                optimizer.zero_grad()
                logits = self._lstm(xb)
                loss = criterion(logits, yb)
                loss.backward()
                torch.nn.utils.clip_grad_norm_(self._lstm.parameters(), 1.0)
                optimizer.step()
            scheduler.step()

            # Validation loss for early stopping
            self._lstm.eval()
            with torch.no_grad():
                val_logits = self._lstm(Xv.to(device))
                val_loss = criterion(val_logits, yv.to(device)).item()

            if val_loss < best_val_loss:
                best_val_loss = val_loss
                patience_counter = 0
            else:
                patience_counter += 1
                if patience_counter >= patience:
                    logger.info("LSTM early stopping at epoch %d", epoch + 1)
                    break

        logger.info("LSTM training complete. Best val_loss=%.4f", best_val_loss)

    def _evaluate(
        self,
        X_val: np.ndarray,
        y_val: np.ndarray,
        best_params: dict,
        cv_scores: List[float],
        start_time: float,
    ) -> TrainingResult:
        from sklearn.metrics import confusion_matrix as sk_cm

        y_pred = self._predict_class(X_val)
        y_proba = self.predict_proba(X_val)

        acc = float(accuracy_score(y_val, y_pred))
        prec = float(precision_score(y_val, y_pred, average="binary", zero_division=0))
        rec = float(recall_score(y_val, y_pred, average="binary", zero_division=0))
        f1 = float(f1_score(y_val, y_pred, average="binary", zero_division=0))

        # AUC-ROC (binary)
        try:
            fpr, tpr, _ = roc_curve(y_val, y_proba[:, 1])
            auc_val = float(auc(fpr, tpr))
        except Exception:
            auc_val = 0.0

        cm = sk_cm(y_val, y_pred).tolist()
        training_time = time.monotonic() - start_time

        return TrainingResult(
            accuracy=acc,
            precision=prec,
            recall=rec,
            f1=f1,
            auc_roc=auc_val,
            training_time_s=training_time,
            best_params=best_params,
            cv_scores=cv_scores,
            confusion_matrix=cm,
        )

    # ------------------------------------------------------------------
    # Inference
    # ------------------------------------------------------------------

    def predict(self, features: np.ndarray) -> PredictionResult:
        """
        Run inference on a feature vector.

        Parameters
        ----------
        features : 1-D array of length n_features, OR a 2-D (1, n_features)
                   array for a single sample.

        Returns
        -------
        PredictionResult
        """
        if self._rf is None and self._lstm is None:
            raise RuntimeError("Model has not been trained or loaded yet")

        t0 = time.monotonic()
        features_2d = np.atleast_2d(features).astype(np.float32)

        prediction = int(self._predict_class(features_2d)[0])
        proba = self.predict_proba(features_2d)[0]
        confidence = float(proba[prediction])

        # Anomaly score: deviation from class-0 baseline (higher = more anomalous)
        anomaly_score = float(proba[1]) if len(proba) > 1 else 0.0

        # Feature importances (RF tier only)
        feat_importances: Dict[str, float] = {}
        if self._rf is not None and self._active_tier == "random_forest":
            importances = self._rf.feature_importances_
            names = self.feature_names or [f"f{i}" for i in range(len(importances))]
            feat_importances = {
                n: float(v) for n, v in zip(names, importances)
            }

        recommendation = self._build_recommendation(prediction, confidence, anomaly_score)
        latency_ms = (time.monotonic() - t0) * 1000.0

        return PredictionResult(
            prediction=prediction,
            confidence=confidence,
            feature_importances=feat_importances,
            anomaly_score=anomaly_score,
            recommendation=recommendation,
            latency_ms=latency_ms,
            model_tier=self._active_tier,
        )

    def _predict_class(self, X: np.ndarray) -> np.ndarray:
        """Return integer class predictions (uses active tier)."""
        if self._active_tier == "lstm" and self._lstm is not None:
            return self._lstm_predict_class(X)
        if self._rf is not None:
            return self._rf.predict(X)
        raise RuntimeError("No trained model available")

    def predict_proba(self, features: np.ndarray) -> np.ndarray:
        """
        Return class probabilities.

        Returns
        -------
        np.ndarray shape (n_samples, n_classes)
        """
        features_2d = np.atleast_2d(features).astype(np.float32)
        if self._active_tier == "lstm" and self._lstm is not None:
            return self._lstm_predict_proba(features_2d)
        if self._rf is not None:
            return self._rf.predict_proba(features_2d)
        raise RuntimeError("No trained model available")

    def _lstm_predict_class(self, X: np.ndarray) -> np.ndarray:
        import torch

        device = next(self._lstm.parameters()).device
        Xt = torch.tensor(X, dtype=torch.float32).unsqueeze(1).to(device)
        self._lstm.eval()
        with torch.no_grad():
            logits = self._lstm(Xt)
            preds = torch.argmax(logits, dim=1).cpu().numpy()
        return preds

    def _lstm_predict_proba(self, X: np.ndarray) -> np.ndarray:
        import torch
        import torch.nn.functional as F

        device = next(self._lstm.parameters()).device
        Xt = torch.tensor(X, dtype=torch.float32).unsqueeze(1).to(device)
        self._lstm.eval()
        with torch.no_grad():
            logits = self._lstm(Xt)
            proba = F.softmax(logits, dim=1).cpu().numpy()
        return proba

    @staticmethod
    def _build_recommendation(prediction: int, confidence: float, anomaly_score: float) -> str:
        if prediction == 1:
            if confidence >= 0.90:
                return (
                    "CRITICAL: High-confidence belt rip detected. "
                    "Initiate emergency stop immediately. Dispatch maintenance team."
                )
            elif confidence >= 0.75:
                return (
                    "WARNING: Belt rip likely. Initiate controlled shutdown. "
                    "Inspect magnetic sensor channels and load cells."
                )
            else:
                return (
                    "CAUTION: Possible belt anomaly detected with moderate confidence. "
                    "Increase monitoring frequency. Consider precautionary inspection."
                )
        else:
            if anomaly_score > 0.35:
                return (
                    "INFO: Belt operating normally but anomaly score elevated. "
                    "Monitor trend over next 15 minutes."
                )
            return "OK: Belt operating within normal parameters."

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        """
        Save model to disk.

        Saves:
          <path>/rf_model.joblib       — RandomForest artifact
          <path>/lstm_model.pt         — PyTorch LSTM state dict (if trained)
          <path>/model_card.json       — Metadata / model card
          <path>/feature_names.json    — Feature name ordering
        """
        save_dir = Path(path)
        save_dir.mkdir(parents=True, exist_ok=True)

        if self._rf is not None:
            joblib.dump(self._rf, save_dir / "rf_model.joblib")
            logger.info("Saved RF model to %s", save_dir / "rf_model.joblib")

        if self._lstm is not None:
            try:
                import torch
                torch.save(self._lstm.state_dict(), save_dir / "lstm_model.pt")
                logger.info("Saved LSTM weights to %s", save_dir / "lstm_model.pt")
            except Exception as exc:
                logger.warning("Could not save LSTM weights: %s", exc)

        card = {**self._model_card, "active_tier": self._active_tier}
        (save_dir / "model_card.json").write_text(json.dumps(card, indent=2))
        (save_dir / "feature_names.json").write_text(
            json.dumps(self.feature_names, indent=2)
        )
        logger.info("Model saved to %s", save_dir)

    @classmethod
    def load(cls, path: str) -> "BeltRipModel":
        """Load a previously saved BeltRipModel from *path*."""
        load_dir = Path(path)

        # Load feature names
        feature_names: List[str] = []
        fn_path = load_dir / "feature_names.json"
        if fn_path.exists():
            feature_names = json.loads(fn_path.read_text())

        instance = cls(feature_names=feature_names)

        # Load model card to determine active tier
        card_path = load_dir / "model_card.json"
        if card_path.exists():
            instance._model_card = json.loads(card_path.read_text())
            instance._active_tier = instance._model_card.get("active_tier", "random_forest")

        # Load RF
        rf_path = load_dir / "rf_model.joblib"
        if rf_path.exists():
            instance._rf = joblib.load(rf_path)
            logger.info("Loaded RF model from %s", rf_path)

        # Load LSTM (optional)
        lstm_path = load_dir / "lstm_model.pt"
        if lstm_path.exists():
            try:
                import torch
                n_features = len(feature_names) if feature_names else 64
                instance._lstm = _build_lstm(input_size=n_features)
                state = torch.load(lstm_path, map_location="cpu")
                instance._lstm.load_state_dict(state)
                instance._lstm.eval()
                logger.info("Loaded LSTM weights from %s", lstm_path)
            except Exception as exc:
                logger.warning("Could not load LSTM weights (%s) — RF tier active", exc)
                instance._active_tier = "random_forest"

        if instance._rf is None and instance._lstm is None:
            raise RuntimeError(f"No model artifacts found in {load_dir}")

        return instance
