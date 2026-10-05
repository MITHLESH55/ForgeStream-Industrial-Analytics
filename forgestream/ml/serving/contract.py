"""Prediction event schemas and versioned serving contracts."""

import enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class MaintenancePriorityEnum(str, enum.Enum):
    """Operational maintenance priority levels."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    EMERGENCY = "EMERGENCY"


class FeatureAttribution(BaseModel):
    """Explainability feature attribution breakdown."""
    feature: str = Field(..., description="Engineering feature name")
    importance: float = Field(..., description="Normalized contribution or importance weight")
    observed_value: Optional[float] = Field(None, description="Observed feature value")


class ModelPredictionOutput(BaseModel):
    """Raw model output from classification and regression inference."""
    model_config = {"protected_namespaces": ()}
    failure_prob: float
    failure_risk_binary: int
    predicted_rul_hours: float
    maintenance_priority: MaintenancePriorityEnum
    top_features: List[FeatureAttribution] = Field(default_factory=list)
    inference_latency_ms: float = 0.0


class AssetPredictionEvent(BaseModel):
    """Downstream prediction event published to Kafka prediction stream."""
    model_config = {"protected_namespaces": ()}
    prediction_id: str = Field(..., description="Unique prediction event identifier")
    asset_id: str = Field(..., description="Physical asset identifier")
    asset_type: str = Field(..., description="Asset equipment class")
    timestamp: float = Field(..., description="Event timestamp in epoch seconds")
    failure_probability: float = Field(..., ge=0.0, le=1.0, description="Predicted probability of failure within horizon")
    predicted_failure_risk: int = Field(..., description="Binary failure risk label (0 or 1)")
    predicted_rul_hours: float = Field(..., ge=0.0, description="Estimated continuous Remaining Useful Life in hours")
    maintenance_priority: MaintenancePriorityEnum = Field(..., description="Derived operational maintenance priority")
    top_contributing_features: List[FeatureAttribution] = Field(default_factory=list, description="Top explainable contributing features")
    model_version: str = Field(default="v3.0.0-champion", description="Versioned champion model identifier")
    inference_latency_ms: float = Field(default=0.0, description="Inference scoring latency in milliseconds")
    schema_version: str = Field(default="3.0.0", description="Semantic contract schema version")
