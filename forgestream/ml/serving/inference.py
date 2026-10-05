"""Batch and streaming inference scoring engine using Spark MLlib models."""

import time
import uuid
from typing import Dict, List, Any, Optional
import numpy as np
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml import PipelineModel

from forgestream.ml.serving.contract import (
    AssetPredictionEvent,
    MaintenancePriorityEnum,
    FeatureAttribution,
    ModelPredictionOutput,
)
from forgestream.ml.config import FeatureRegistryConfig


class SparkModelInferenceEngine:
    """Inference engine performing real-time and batch scoring using Spark MLlib models."""

    def __init__(
        self,
        spark: SparkSession,
        clf_model: Optional[PipelineModel] = None,
        reg_model: Optional[PipelineModel] = None,
        feature_registry: Optional[FeatureRegistryConfig] = None,
        top_feature_attributions: Optional[List[Dict[str, Any]]] = None,
        model_version: str = "v3.0.0-champion",
    ):
        self.spark = spark
        self.clf_model = clf_model
        self.reg_model = reg_model
        self.registry = feature_registry or FeatureRegistryConfig()
        self.top_feature_attributions = top_feature_attributions or []
        self.model_version = model_version

    def determine_maintenance_priority(
        self,
        failure_prob: float,
        rul_hours: float,
    ) -> MaintenancePriorityEnum:
        """Derive actionable maintenance priority from failure risk probability and estimated RUL."""
        if failure_prob >= 0.80 or rul_hours <= 10.0:
            return MaintenancePriorityEnum.EMERGENCY
        elif failure_prob >= 0.60 or rul_hours <= 25.0:
            return MaintenancePriorityEnum.HIGH
        elif failure_prob >= 0.35 or rul_hours <= 50.0:
            return MaintenancePriorityEnum.MEDIUM
        else:
            return MaintenancePriorityEnum.LOW

    def predict_dataframe(self, df: pd.DataFrame) -> List[AssetPredictionEvent]:
        """Score a pandas DataFrame containing operational features and produce prediction events."""
        t0 = time.perf_counter()
        spark_df = self.spark.createDataFrame(df)

        clf_preds = None
        if self.clf_model is not None:
            clf_preds = self.clf_model.transform(spark_df)

        reg_preds = None
        if self.reg_model is not None:
            reg_preds = self.reg_model.transform(spark_df)

        # Extract predictions
        clf_rows = clf_preds.select("probability", "prediction").collect() if clf_preds else []
        reg_rows = reg_preds.select("prediction").collect() if reg_preds else []

        total_elapsed_ms = (time.perf_counter() - t0) * 1000.0
        per_sample_latency_ms = total_elapsed_ms / max(1, len(df))

        events: List[AssetPredictionEvent] = []
        for i, (_, row) in enumerate(df.iterrows()):
            if clf_rows:
                p_vec = clf_rows[i]["probability"]
                prob = float(p_vec[1]) if len(p_vec) > 1 else float(p_vec[0])
            else:
                prob = 0.0
            pred_binary = int(clf_rows[i]["prediction"]) if clf_rows else 0
            rul_val = float(reg_rows[i]["prediction"]) if reg_rows else 100.0
            rul_val = max(0.0, min(120.0, rul_val))

            priority = self.determine_maintenance_priority(prob, rul_val)

            # Top 3 feature attributions with observed values
            top_attrs: List[FeatureAttribution] = []
            for attr in self.top_feature_attributions[:3]:
                feat_name = attr.get("feature", "")
                importance = attr.get("importance", 0.0)
                obs_val = float(row.get(feat_name, 0.0)) if feat_name in row else None
                top_attrs.append(FeatureAttribution(
                    feature=feat_name,
                    importance=importance,
                    observed_value=obs_val,
                ))

            event = AssetPredictionEvent(
                prediction_id=f"PRED-{uuid.uuid4().hex[:8].upper()}",
                asset_id=str(row.get("asset_id", "UNKNOWN")),
                asset_type=str(row.get("asset_type", "GENERIC")),
                timestamp=float(row.get("timestamp", time.time())),
                failure_probability=round(prob, 4),
                predicted_failure_risk=pred_binary,
                predicted_rul_hours=round(rul_val, 2),
                maintenance_priority=priority,
                top_contributing_features=top_attrs,
                model_version=self.model_version,
                inference_latency_ms=round(per_sample_latency_ms, 3),
            )
            events.append(event)

        return events
