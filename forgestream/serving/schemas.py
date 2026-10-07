"""
ForgeStream Phase 4 Serving Layer Schemas.
Defines versioned data models and operational entities for Lakehouse serving,
Trino analytical queries, and Grafana dashboard visualization.
"""

from datetime import datetime, timezone
import enum
from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


class HealthStateEnum(str, enum.Enum):
    """Operational health state categories."""
    HEALTHY = "HEALTHY"
    WATCH = "WATCH"
    DEGRADED = "DEGRADED"
    CRITICAL = "CRITICAL"


class MaintenancePriorityEnum(str, enum.Enum):
    """Operational maintenance urgency classifications."""
    LOW = "LOW"
    MEDIUM = "MEDIUM"
    HIGH = "HIGH"
    EMERGENCY = "EMERGENCY"


class AnomalySeverityEnum(str, enum.Enum):
    """Anomaly severity levels."""
    INFO = "INFO"
    WARNING = "WARNING"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class AssetCurrentState(BaseModel):
    """
    Real-time operational state snapshot for a single asset.
    Represents the unified view of latest telemetry, streaming health state,
    and ML prognostic predictions.
    """
    asset_id: str = Field(..., description="Unique physical asset identifier")
    asset_type: str = Field(..., description="Equipment category (e.g., MOTOR, PUMP, TURBINE)")
    operating_mode: str = Field(default="NORMAL", description="Current operating mode (NORMAL, HIGH_LOAD, TRANSIENT, DEGRADED)")
    health_state: HealthStateEnum = Field(default=HealthStateEnum.HEALTHY, description="Evaluated health state")
    health_score: float = Field(default=1.0, ge=0.0, le=1.0, description="Normalized continuous health index H(t) in [0.0, 1.0]")
    failure_probability: float = Field(default=0.0, ge=0.0, le=1.0, description="ML predicted failure probability within 24h horizon")
    predicted_failure_risk: int = Field(default=0, description="Binary failure risk prediction (0 or 1)")
    predicted_rul_hours: float = Field(default=120.0, ge=0.0, description="Predicted remaining useful life in hours")
    maintenance_priority: MaintenancePriorityEnum = Field(default=MaintenancePriorityEnum.LOW, description="Prescriptive maintenance priority")
    anomaly_level: int = Field(default=0, ge=0, le=3, description="Highest active anomaly level (0=None, 1=Bounds, 2=Deviation, 3=Correlation)")
    active_alert_count: int = Field(default=0, ge=0, description="Count of unacknowledged active alerts for this asset")
    temperature: float = Field(..., description="Latest temperature in °C")
    vibration: float = Field(..., description="Latest vibration RMS in mm/s")
    pressure: float = Field(..., description="Latest pressure in bar")
    load: float = Field(..., description="Latest load percentage in %")
    rpm: float = Field(..., description="Latest rotational speed in RPM")
    power: float = Field(..., description="Latest electrical power in kW")
    last_event_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Timestamp of latest sensor reading")
    last_prediction_time: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Timestamp of latest ML prediction")
    model_version: str = Field(default="v3.0.0-champion", description="Version of champion ML model used for scoring")
    schema_version: str = Field(default="4.0.0", description="Serving contract schema version")


class AssetPredictionHistory(BaseModel):
    """
    Historical ML prediction event record for trend analysis and auditability.
    Compatible with Phase 3 AssetPredictionEvent.
    """
    prediction_id: str = Field(..., description="Unique prediction event identifier")
    asset_id: str = Field(..., description="Physical asset identifier")
    asset_type: str = Field(..., description="Equipment category")
    timestamp: float = Field(..., description="Epoch timestamp of prediction")
    event_time: datetime = Field(..., description="UTC timestamp of the evaluated telemetry")
    failure_probability: float = Field(..., ge=0.0, le=1.0, description="Predicted failure probability")
    predicted_failure_risk: int = Field(..., description="Binary failure risk (0 or 1)")
    predicted_rul_hours: float = Field(..., ge=0.0, description="Estimated Remaining Useful Life in hours")
    maintenance_priority: MaintenancePriorityEnum = Field(..., description="Prescriptive maintenance priority")
    top_features_summary: str = Field(default="", description="JSON-encoded top explainable feature attributions")
    model_version: str = Field(default="v3.0.0-champion", description="Champion model version")
    inference_latency_ms: float = Field(default=0.0, description="Inference scoring latency in ms")


class AssetAlertHistory(BaseModel):
    """
    Historical alert event record for operational alerting and audit.
    Compatible with Phase 2 AssetAlertEvent.
    """
    alert_id: str = Field(..., description="Unique alert identifier")
    asset_id: str = Field(..., description="Physical asset identifier")
    asset_type: str = Field(..., description="Equipment category")
    severity: AnomalySeverityEnum = Field(..., description="Alert severity level")
    health_state: HealthStateEnum = Field(..., description="Current evaluated health state")
    health_score: float = Field(..., ge=0.0, le=1.0, description="Health score at alert trigger")
    is_state_transition: bool = Field(default=False, description="Whether alert marks a health state transition")
    is_recovery: bool = Field(default=False, description="Whether alert represents asset recovery")
    reason_codes: str = Field(default="", description="JSON-encoded reason codes")
    triggering_values: str = Field(default="", description="JSON-encoded sensor triggering values")
    maintenance_priority: MaintenancePriorityEnum = Field(..., description="Prescriptive maintenance priority")
    timestamp: float = Field(..., description="Epoch timestamp")
    event_time: datetime = Field(..., description="UTC timestamp of alert event")


class AssetTelemetrySummary(BaseModel):
    """
    Aggregated telemetry statistics over a time window for trend analysis.
    """
    summary_id: str = Field(..., description="Unique window summary identifier")
    asset_id: str = Field(..., description="Physical asset identifier")
    asset_type: str = Field(..., description="Equipment category")
    window_start: datetime = Field(..., description="Start of aggregation window (UTC)")
    window_end: datetime = Field(..., description="End of aggregation window (UTC)")
    event_count: int = Field(default=0, description="Count of telemetry samples in window")
    avg_temperature: float = Field(..., description="Mean temperature in °C")
    max_temperature: float = Field(..., description="Peak temperature in °C")
    avg_vibration: float = Field(..., description="Mean vibration RMS in mm/s")
    max_vibration: float = Field(..., description="Peak vibration RMS in mm/s")
    avg_pressure: float = Field(..., description="Mean pressure in bar")
    avg_load: float = Field(..., description="Mean load percentage in %")
    avg_power: float = Field(..., description="Mean electrical power in kW")
    thermal_rise_rate: float = Field(default=0.0, description="Rate of temperature change (°C/min)")
    vibration_slope: float = Field(default=0.0, description="Rate of vibration change (mm/s/min)")
    anomaly_count: int = Field(default=0, description="Number of detected anomalies in window")


class MaintenancePriorityItem(BaseModel):
    """
    Ranked maintenance work item prioritized by failure risk, RUL, and criticality.
    """
    ranking: int = Field(..., description="Priority rank (1 = highest urgency)")
    asset_id: str = Field(..., description="Physical asset identifier")
    asset_type: str = Field(..., description="Equipment category")
    criticality: str = Field(default="MEDIUM", description="Asset structural criticality (LOW, MEDIUM, HIGH, CRITICAL)")
    health_state: HealthStateEnum = Field(..., description="Current evaluated health state")
    health_score: float = Field(..., description="Health index in [0.0, 1.0]")
    failure_probability: float = Field(..., description="Failure probability within 24h")
    predicted_rul_hours: float = Field(..., description="Remaining useful life in hours")
    maintenance_priority: MaintenancePriorityEnum = Field(..., description="Operational priority tier")
    priority_score: float = Field(..., description="Composite priority index score (higher = more urgent)")
    recommended_action: str = Field(..., description="Prescriptive maintenance recommendation")
    last_evaluated: datetime = Field(..., description="Evaluation timestamp")


class FleetHealthSummary(BaseModel):
    """
    Fleet-wide operational summary aggregating all assets.
    """
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="Evaluation timestamp")
    total_assets: int = Field(default=0, description="Total number of registered assets")
    healthy_assets: int = Field(default=0, description="Assets in HEALTHY state")
    watch_assets: int = Field(default=0, description="Assets in WATCH state")
    degraded_assets: int = Field(default=0, description="Assets in DEGRADED state")
    critical_assets: int = Field(default=0, description="Assets in CRITICAL state")
    high_risk_assets: int = Field(default=0, description="Assets with failure_prob >= 0.50")
    near_failure_assets: int = Field(default=0, description="Assets with RUL < 24.0h")
    fleet_health_score: float = Field(default=1.0, ge=0.0, le=1.0, description="Mean fleet health score")
    avg_predicted_rul_hours: float = Field(default=120.0, ge=0.0, description="Mean predicted RUL across fleet")
    min_predicted_rul_hours: float = Field(default=120.0, ge=0.0, description="Minimum predicted RUL across fleet")
    failure_risk_rate: float = Field(default=0.0, ge=0.0, le=1.0, description="Proportion of fleet at high risk")
    active_alerts_total: int = Field(default=0, description="Total active unacknowledged alerts")
    emergency_maintenance_count: int = Field(default=0, description="Assets requiring EMERGENCY maintenance")
    high_maintenance_count: int = Field(default=0, description="Assets requiring HIGH priority maintenance")
    schema_version: str = Field(default="4.0.0", description="Summary schema version")
