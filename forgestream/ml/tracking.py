"""MLflow tracking integration for Spark MLlib model experiments and artifacts."""

import os
import json
import logging
from typing import Dict, Any, Optional, List
import mlflow
from forgestream.ml.config import MLflowConfig

logger = logging.getLogger("forgestream.ml.tracking")


class MLflowTracker:
    """Manages experiment logging, metric tracking, and artifact persistence."""

    def __init__(self, config: Optional[MLflowConfig] = None):
        self.config = config or MLflowConfig()
        os.environ["MLFLOW_ALLOW_FILE_STORE"] = "true"
        os.environ["MLFLOW_DISABLE_AGENT_HINT"] = "1"

        db_dir = os.path.dirname(self.config.tracking_uri.replace("sqlite:///", ""))
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)

        mlflow.set_tracking_uri(self.config.tracking_uri)
        self.experiment = mlflow.set_experiment(self.config.experiment_name)
        logger.info("MLflow Tracking URI: %s | Experiment: %s", self.config.tracking_uri, self.config.experiment_name)

    def start_run(self, run_name: str, tags: Optional[Dict[str, str]] = None) -> mlflow.ActiveRun:
        """Start a new tracked MLflow run."""
        return mlflow.start_run(run_name=run_name, tags=tags)

    def log_model_run(
        self,
        run_name: str,
        model_type: str,
        task: str,
        params: Dict[str, Any],
        train_metrics: Dict[str, Any],
        val_metrics: Dict[str, Any],
        test_metrics: Dict[str, Any],
        feature_attributions: Optional[List[Dict[str, Any]]] = None,
        spark_model: Optional[Any] = None,
        model_artifact_path: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> str:
        """Log complete parameters, metrics across splits, feature attributions, and artifacts."""
        run_tags = {
            "model_type": model_type,
            "task": task,
            "framework": "Apache Spark MLlib",
            "phase": "Phase 3 Predictive Maintenance",
        }
        if tags:
            run_tags.update(tags)

        with self.start_run(run_name=run_name, tags=run_tags) as run:
            # 1. Log Hyperparameters
            for k, v in params.items():
                mlflow.log_param(k, v)

            # 2. Log Train Metrics
            for k, v in train_metrics.items():
                if isinstance(v, (int, float)):
                    mlflow.log_metric(f"train_{k}", float(v))

            # 3. Log Validation Metrics
            for k, v in val_metrics.items():
                if isinstance(v, (int, float)):
                    mlflow.log_metric(f"val_{k}", float(v))

            # 4. Log Test Metrics
            for k, v in test_metrics.items():
                if isinstance(v, (int, float)):
                    mlflow.log_metric(f"test_{k}", float(v))

            # 5. Log Feature Attributions Artifact
            if feature_attributions:
                os.makedirs("data/ml/artifacts", exist_ok=True)
                attr_path = f"data/ml/artifacts/{run_name}_feature_attributions.json"
                with open(attr_path, "w") as f:
                    json.dump(feature_attributions, f, indent=2)
                mlflow.log_artifact(attr_path)

            # 6. Save and Log Model Artifacts
            if spark_model is not None and model_artifact_path:
                os.makedirs(os.path.dirname(model_artifact_path), exist_ok=True)
                model_meta_file = f"{model_artifact_path}_meta.json"
                meta_payload = {
                    "run_name": run_name,
                    "model_type": model_type,
                    "task": task,
                    "params": params,
                    "metrics_test": test_metrics,
                    "stages": [str(s) for s in spark_model.stages] if hasattr(spark_model, "stages") else [],
                }
                with open(model_meta_file, "w") as f:
                    json.dump(meta_payload, f, indent=2)
                mlflow.log_artifact(model_meta_file)
                mlflow.log_param("spark_model_path", model_artifact_path)
                logger.info("Saved Spark MLlib model metadata to %s", model_meta_file)

            run_id = run.info.run_id
            logger.info("Logged MLflow run '%s' with ID: %s", run_name, run_id)
            return run_id
