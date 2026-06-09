"""
MLflow model registry client with local filesystem fallback.
Handles model registration, versioning, promotion, and loading
for all Flux OT AI services.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

MLFLOW_TRACKING_URI: str = os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000")
_LOCAL_REGISTRY_ROOT: Path = Path(os.environ.get("LOCAL_MODEL_REGISTRY_PATH", "/tmp/fluxot_model_registry"))

# ---------------------------------------------------------------------------
# MLflow availability check (lazy import so the service still works edge-side
# without a full ML dependency set)
# ---------------------------------------------------------------------------

def _mlflow_available() -> bool:
    try:
        import mlflow  # noqa: F401
        return True
    except ImportError:
        return False


# ---------------------------------------------------------------------------
# Local filesystem registry (fallback / edge-deployable)
# ---------------------------------------------------------------------------

class _LocalModelRegistry:
    """Minimal model registry backed by the local filesystem."""

    def __init__(self, root: Path = _LOCAL_REGISTRY_ROOT) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def _model_dir(self, name: str) -> Path:
        d = self.root / name
        d.mkdir(parents=True, exist_ok=True)
        return d

    def _meta_path(self, name: str, version: int) -> Path:
        return self._model_dir(name) / f"v{version}.meta.json"

    def _artifact_path(self, name: str, version: int) -> Path:
        return self._model_dir(name) / f"v{version}.pkl"

    def _next_version(self, name: str) -> int:
        existing = list(self._model_dir(name).glob("v*.meta.json"))
        if not existing:
            return 1
        versions = [int(p.stem.split(".")[0][1:]) for p in existing]
        return max(versions) + 1

    def register(self, name: str, model: Any, metrics: dict, params: dict, tags: dict) -> int:
        import joblib

        version = self._next_version(name)
        joblib.dump(model, self._artifact_path(name, version))
        meta = {
            "name": name,
            "version": version,
            "stage": "None",
            "metrics": metrics,
            "params": params,
            "tags": tags,
        }
        self._meta_path(name, version).write_text(json.dumps(meta, indent=2))
        logger.info("Local registry: registered %s v%d", name, version)
        return version

    def load(self, name: str, version: int) -> Any:
        import joblib

        path = self._artifact_path(name, version)
        if not path.exists():
            raise FileNotFoundError(f"No artifact for {name} v{version} at {path}")
        return joblib.load(path)

    def latest_version(self, name: str, stage: str = "Production") -> Optional[int]:
        model_dir = self._model_dir(name)
        candidates = []
        for p in model_dir.glob("v*.meta.json"):
            meta = json.loads(p.read_text())
            if meta.get("stage", "None") == stage:
                candidates.append(meta["version"])
        if not candidates:
            # Fall back to the highest version regardless of stage
            all_meta = list(model_dir.glob("v*.meta.json"))
            if not all_meta:
                return None
            return max(int(p.stem.split(".")[0][1:]) for p in all_meta)
        return max(candidates)

    def promote(self, name: str, version: int, stage: str) -> None:
        path = self._meta_path(name, version)
        if not path.exists():
            raise FileNotFoundError(f"No metadata for {name} v{version}")
        meta = json.loads(path.read_text())
        meta["stage"] = stage
        path.write_text(json.dumps(meta, indent=2))
        logger.info("Local registry: promoted %s v%d -> %s", name, version, stage)

    def list_models(self) -> List[dict]:
        results = []
        for model_dir in self.root.iterdir():
            if not model_dir.is_dir():
                continue
            for meta_path in model_dir.glob("v*.meta.json"):
                results.append(json.loads(meta_path.read_text()))
        return results


# ---------------------------------------------------------------------------
# Public ModelRegistry — wraps MLflow or local fallback transparently
# ---------------------------------------------------------------------------

class ModelRegistry:
    """
    Unified model registry.

    Attempts to use MLflow as the backing store.  If MLflow is unavailable
    (not installed or tracking server unreachable) the registry falls back to
    the local filesystem so the service can still start and serve predictions.
    """

    def __init__(self, tracking_uri: str = MLFLOW_TRACKING_URI) -> None:
        self._tracking_uri = tracking_uri
        self._mlflow_ok: bool = False
        self._local = _LocalModelRegistry()

        if _mlflow_available():
            try:
                import mlflow
                mlflow.set_tracking_uri(tracking_uri)
                # Probe the server with a lightweight call
                mlflow.search_experiments()
                self._mlflow_ok = True
                logger.info("ModelRegistry: MLflow backend connected at %s", tracking_uri)
            except Exception as exc:
                logger.warning(
                    "ModelRegistry: MLflow unreachable (%s) — using local filesystem fallback", exc
                )
        else:
            logger.warning("ModelRegistry: mlflow not installed — using local filesystem fallback")

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register_model(
        self,
        name: str,
        model: Any,
        metrics: dict,
        params: dict,
        tags: Optional[dict] = None,
    ) -> str:
        """
        Register a trained model.

        Returns a version string (MLflow version number or local integer).
        """
        tags = tags or {}

        if self._mlflow_ok:
            return self._mlflow_register(name, model, metrics, params, tags)

        version = self._local.register(name, model, metrics, params, tags)
        return str(version)

    def _mlflow_register(
        self, name: str, model: Any, metrics: dict, params: dict, tags: dict
    ) -> str:
        import mlflow
        import mlflow.sklearn

        with mlflow.start_run(run_name=f"{name}_training"):
            mlflow.log_metrics(metrics)
            mlflow.log_params(params)
            for k, v in tags.items():
                mlflow.set_tag(k, v)
            mlflow.sklearn.log_model(model, artifact_path="model", registered_model_name=name)
            run_id = mlflow.active_run().info.run_id  # type: ignore[union-attr]

        client = mlflow.tracking.MlflowClient()
        versions = client.search_model_versions(f"name='{name}'")
        latest = max(versions, key=lambda v: int(v.version))
        logger.info("MLflow: registered %s version %s (run %s)", name, latest.version, run_id)
        return latest.version

    # ------------------------------------------------------------------
    # Loading
    # ------------------------------------------------------------------

    def load_latest_model(self, name: str, stage: str = "Production") -> Any:
        """Load the most recent model at the given stage."""
        if self._mlflow_ok:
            return self._mlflow_load(name, stage=stage)

        version = self._local.latest_version(name, stage)
        if version is None:
            raise RuntimeError(f"No model found for '{name}' in local registry")
        return self._local.load(name, version)

    def load_model_by_version(self, name: str, version: str) -> Any:
        """Load a specific model version."""
        if self._mlflow_ok:
            try:
                import mlflow.pyfunc
                model_uri = f"models:/{name}/{version}"
                return mlflow.pyfunc.load_model(model_uri)
            except Exception as exc:
                logger.warning("MLflow load failed (%s), falling back to local", exc)

        return self._local.load(name, int(version))

    def _mlflow_load(self, name: str, stage: str = "Production") -> Any:
        import mlflow.pyfunc

        model_uri = f"models:/{name}/{stage}"
        try:
            return mlflow.pyfunc.load_model(model_uri)
        except Exception:
            # Stage may not exist — try latest version
            import mlflow
            client = mlflow.tracking.MlflowClient()
            versions = client.search_model_versions(f"name='{name}'")
            if not versions:
                raise RuntimeError(f"No versions found for model '{name}' in MLflow")
            latest = max(versions, key=lambda v: int(v.version))
            return mlflow.pyfunc.load_model(f"models:/{name}/{latest.version}")

    # ------------------------------------------------------------------
    # Promotion
    # ------------------------------------------------------------------

    def promote_model(self, name: str, version: str, stage: str) -> None:
        """
        Promote a model version to a lifecycle stage.

        Stages: Staging, Production, Archived
        """
        if self._mlflow_ok:
            try:
                import mlflow
                client = mlflow.tracking.MlflowClient()
                client.transition_model_version_stage(
                    name=name, version=version, stage=stage, archive_existing_versions=True
                )
                logger.info("MLflow: promoted %s v%s -> %s", name, version, stage)
                return
            except Exception as exc:
                logger.warning("MLflow promote failed (%s), falling back to local", exc)

        self._local.promote(name, int(version), stage)

    # ------------------------------------------------------------------
    # Listing
    # ------------------------------------------------------------------

    def list_models(self) -> List[dict]:
        """Return metadata for all registered models."""
        if self._mlflow_ok:
            try:
                import mlflow
                client = mlflow.tracking.MlflowClient()
                registered = client.search_registered_models()
                results = []
                for rm in registered:
                    for mv in rm.latest_versions:
                        results.append(
                            {
                                "name": rm.name,
                                "version": mv.version,
                                "stage": mv.current_stage,
                                "creation_timestamp": mv.creation_timestamp,
                                "description": rm.description,
                            }
                        )
                return results
            except Exception as exc:
                logger.warning("MLflow list_models failed (%s), falling back to local", exc)

        return self._local.list_models()
