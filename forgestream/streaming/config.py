"""Streaming configuration management for ForgeStream Phase 2."""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class StreamingConfig(BaseSettings):
    """Configuration for ForgeStream Real-Time Streaming and Flink Execution."""

    model_config = SettingsConfigDict(
        env_prefix="STREAMING_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Kafka Topic Bindings
    topic_telemetry_input: str = Field(default="industrial-telemetry", description="Input telemetry topic")
    topic_alerts_output: str = Field(default="asset-alerts", description="Output alert topic")
    topic_metrics_output: str = Field(default="system-metrics", description="Output system metrics topic")
    topic_model_events: str = Field(default="model-events", description="Output model events topic")
    consumer_group_id: str = Field(default="forgestream-streaming-group", description="Consumer group ID")

    # Event-Time & Watermarks
    watermark_delay_sec: float = Field(default=5.0, description="Bounded out-of-orderness watermark delay in seconds")
    allowed_lateness_sec: float = Field(default=30.0, description="Allowed lateness threshold in seconds")

    # Keyed State & Memory Bounds
    state_buffer_capacity: int = Field(default=120, description="Circular buffer sample capacity per asset (~2 min @ 1Hz)")
    state_ttl_hours: float = Field(default=24.0, description="Time-To-Live for inactive asset state in hours")

    # Window Durations
    window_short_tumbling_sec: float = Field(default=5.0, description="Short-term tumbling window in seconds")
    window_medium_sliding_sec: float = Field(default=30.0, description="Medium-term sliding window in seconds")
    window_medium_slide_step_sec: float = Field(default=5.0, description="Slide step for medium window in seconds")
    window_long_trend_sec: float = Field(default=120.0, description="Long-term trend window in seconds")
    window_long_slide_step_sec: float = Field(default=10.0, description="Slide step for long window in seconds")

    # Anomaly Thresholds (Level 1 Bounds)
    l1_temp_max_generic_c: float = Field(default=120.0, description="Level 1 maximum temperature trip for motors/pumps/compressors")
    l1_temp_max_turbine_c: float = Field(default=950.0, description="Level 1 maximum temperature trip for gas turbine")
    l1_vibration_critical_rms: float = Field(default=12.0, description="Level 1 ISO 10816 Zone D critical vibration trip (mm/s)")
    l1_overcurrent_ratio: float = Field(default=1.35, description="Level 1 overcurrent ratio above rated current")

    # Anomaly Thresholds (Level 2 Deviations)
    l2_thermal_rise_rate_threshold: float = Field(default=4.0, description="Level 2 thermal rise rate threshold (°C/min)")
    l2_vibration_zscore_threshold: float = Field(default=3.0, description="Level 2 vibration rolling z-score threshold")
    l2_pressure_instability_ratio: float = Field(default=2.5, description="Level 2 pressure instability threshold multiplier")

    # Health Scoring Weights (Sum = 1.0)
    health_weight_vibration: float = Field(default=0.35, description="Weight for vibration degradation")
    health_weight_temperature: float = Field(default=0.25, description="Weight for thermal stress")
    health_weight_pressure: float = Field(default=0.15, description="Weight for pressure instability")
    health_weight_electrical: float = Field(default=0.15, description="Weight for electrical discrepancy")
    health_weight_persistence: float = Field(default=0.10, description="Weight for persistent fault count")

    # Hysteresis Health Thresholds
    health_threshold_watch: float = Field(default=0.85, description="Trigger WATCH state when score drops below this")
    health_recover_healthy: float = Field(default=0.90, description="Recover to HEALTHY when score rises above this")
    health_threshold_degraded: float = Field(default=0.65, description="Trigger DEGRADED state when score drops below this")
    health_recover_watch: float = Field(default=0.72, description="Recover to WATCH when score rises above this")
    health_threshold_critical: float = Field(default=0.40, description="Trigger CRITICAL state when score drops below this")
    health_recover_degraded: float = Field(default=0.50, description="Recover to DEGRADED when score rises above this")

    # Alert Debouncing & Cooldown
    alert_cooldown_sec: float = Field(default=30.0, description="Alert cooldown period in seconds for same state")
    alert_escalation_enabled: bool = Field(default=True, description="Immediately emit alert on severity escalation")

    # Metrics & Checkpointing
    metric_reporting_interval_sec: float = Field(default=1.0, description="System metric calculation interval")
    checkpoint_interval_sec: float = Field(default=10.0, description="State snapshot checkpoint interval in seconds")
    parallelism: int = Field(default=1, description="Stream processing parallelism")
