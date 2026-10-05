"""Pydantic schemas and event models for ForgeStream Phase 2 streaming."""

from enum import Enum
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field


class AnomalySeverity(str, Enum):
    """Severity levels for industrial asset anomalies."""
    INFO = "INFO"
    WARNING = "WARNING"
    HIGH = "HIGH"
    CRITICAL = "CRITICAL"


class HealthState(str, Enum):
    """Discrete operational health states for industrial assets."""
    HEALTHY = "HEALTHY"
    WATCH = "WATCH"
    DEGRADED = "DEGRADED"
    CRITICAL = "CRITICAL"


class MaintenancePriority(str, Enum):
    """Prescriptive maintenance urgency classifications."""
    NORMAL = "NORMAL"
    SCHEDULED = "SCHEDULED"
    URGENT = "URGENT"
    EMERGENCY = "EMERGENCY"


class AnomalyRecord(BaseModel):
    """Structured detail for an individual detected anomaly."""
    anomaly_id: str = Field(..., description="Unique identifier for the anomaly")
    anomaly_type: str = Field(..., description="Categorical anomaly classification slug")
    level: int = Field(..., description="Detection hierarchy level: 1 (Bounds), 2 (Deviation), 3 (Correlation)")
    severity: AnomalySeverity = Field(..., description="Severity classification")
    reason_code: str = Field(..., description="Standardized reason code")
    description: str = Field(..., description="Human-readable explanation of why this anomaly triggered")
    triggering_value: float = Field(..., description="Measured sensor value that breached threshold")
    threshold_value: float = Field(..., description="Baseline or threshold value compared against")
    timestamp: float = Field(..., description="Event timestamp of anomaly")


class AssetAlertEvent(BaseModel):
    """Standardized event emitted to Kafka topic asset-alerts."""
    alert_id: str = Field(..., description="Globally unique alert identifier")
    asset_id: str = Field(..., description="Target equipment identifier (e.g., MOTOR-001)")
    asset_type: str = Field(..., description="Equipment category (e.g., MOTOR, PUMP)")
    event_time: str = Field(..., description="ISO-8601 UTC timestamp of the telemetry event")
    timestamp: float = Field(..., description="Epoch timestamp of triggering event")
    alert_timestamp: float = Field(..., description="Epoch timestamp when alert was generated")
    severity: AnomalySeverity = Field(..., description="Alert severity level")
    previous_health_state: Optional[HealthState] = Field(None, description="Previous asset health state")
    current_health_state: HealthState = Field(..., description="Current evaluated health state")
    health_score: float = Field(..., ge=0.0, le=1.0, description="Normalized health score in [0.0, 1.0]")
    is_state_transition: bool = Field(default=False, description="True if alert marks a health state boundary transition")
    is_recovery: bool = Field(default=False, description="True if alert indicates asset recovery to a healthier state")
    anomaly_types: List[str] = Field(default_factory=list, description="Active anomaly classification codes")
    reason_codes: List[str] = Field(default_factory=list, description="Explainable diagnostic reason codes")
    triggering_values: Dict[str, float] = Field(default_factory=dict, description="Key sensor values at trigger time")
    supporting_features: Dict[str, float] = Field(default_factory=dict, description="Real-time rolling features at trigger time")
    maintenance_priority: MaintenancePriority = Field(..., description="Prescriptive maintenance urgency")
    schema_version: str = Field(default="2.0.0", description="Event schema contract version")


class SystemMetricEvent(BaseModel):
    """Operational health and performance metrics emitted to Kafka topic system-metrics."""
    metric_id: str = Field(..., description="Unique metric event ID")
    timestamp: float = Field(..., description="Epoch timestamp of metric evaluation")
    event_time: str = Field(..., description="ISO-8601 UTC timestamp")
    window_duration_sec: float = Field(default=1.0, description="Measurement window duration")
    events_ingested: int = Field(default=0, description="Count of raw telemetry events read from Kafka")
    events_processed: int = Field(default=0, description="Count of events fully evaluated")
    events_late: int = Field(default=0, description="Count of late events accepted within allowed lateness")
    events_excessively_late: int = Field(default=0, description="Count of events rejected past watermark")
    events_quarantined: int = Field(default=0, description="Count of invalid/corrupt records quarantined")
    anomalies_detected: int = Field(default=0, description="Total count of anomalies detected")
    alerts_emitted: int = Field(default=0, description="Count of alerts published to Kafka")
    active_assets_count: int = Field(default=0, description="Count of distinct assets actively tracked")
    current_watermark: float = Field(default=0.0, description="Current event-time watermark timestamp")
    watermark_lag_sec: float = Field(default=0.0, description="Time lag between max event time and watermark")
    processing_latency_p50_ms: float = Field(default=0.0, description="50th percentile processing latency in ms")
    processing_latency_p95_ms: float = Field(default=0.0, description="95th percentile processing latency in ms")
    processing_latency_p99_ms: float = Field(default=0.0, description="99th percentile processing latency in ms")
    throughput_events_per_sec: float = Field(default=0.0, description="Calculated event throughput rate")
    schema_version: str = Field(default="2.0.0", description="Metric schema contract version")


class WindowStateEvent(BaseModel):
    """Summary of a completed window evaluation for an asset."""
    window_id: str = Field(..., description="Unique window instance identifier")
    asset_id: str = Field(..., description="Asset identifier")
    window_type: str = Field(..., description="TUMBLING or SLIDING")
    window_start: float = Field(..., description="Window start epoch timestamp")
    window_end: float = Field(..., description="Window end epoch timestamp")
    sample_count: int = Field(..., description="Number of events in window")
    mean_temperature: float = Field(..., description="Mean temperature in window")
    std_temperature: float = Field(..., description="Temperature standard deviation")
    thermal_rise_rate: float = Field(..., description="Rate of temperature rise (°C/min)")
    mean_vibration: float = Field(..., description="Mean vibration RMS in window")
    max_vibration: float = Field(..., description="Peak vibration RMS in window")
    vibration_slope: float = Field(..., description="Vibration trend slope (mm/s/min)")
    mean_pressure: float = Field(..., description="Mean pressure in window")
    std_pressure: float = Field(..., description="Pressure standard deviation")
    anomalies_in_window: int = Field(default=0, description="Number of anomalies in window")
