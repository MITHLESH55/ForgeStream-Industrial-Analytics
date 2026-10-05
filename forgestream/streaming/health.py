"""Deterministic asset health model and hysteresis state machine for ForgeStream Phase 2."""

from typing import Dict, List, Optional, Tuple, Any
from pydantic import BaseModel, Field
from forgestream.streaming.config import StreamingConfig
from forgestream.streaming.schemas import (
    HealthState,
    MaintenancePriority,
    AnomalyRecord,
    AnomalySeverity,
)
from forgestream.streaming.features import StreamFeatures
from forgestream.streaming.state import AssetStreamingState


class HealthEvaluation(BaseModel):
    """Encapsulates evaluated asset health score and discrete state machine transition."""

    asset_id: str
    timestamp: float
    health_score: float = Field(..., ge=0.0, le=1.0, description="Normalized health score in [0.0, 1.0]")
    health_state: HealthState
    previous_health_state: Optional[HealthState] = None
    is_state_transition: bool = False
    is_recovery: bool = False
    sub_scores: Dict[str, float] = Field(default_factory=dict, description="Sub-index degradation components")
    maintenance_priority: MaintenancePriority
    degradation_reasons: List[str] = Field(default_factory=list)


class AssetHealthModel:
    """Computes deterministic health score and manages asymmetric hysteresis state transitions."""

    def __init__(self, config: Optional[StreamingConfig] = None):
        self.config = config or StreamingConfig()

    def evaluate_health(
        self,
        state: AssetStreamingState,
        features: StreamFeatures,
        anomalies: List[AnomalyRecord],
        telemetry: Dict[str, Any],
    ) -> HealthEvaluation:
        """Calculate composite health score and evaluate state machine transitions.

        Args:
            state: Keyed asset streaming state.
            features: Real-time stream features.
            anomalies: Active anomalies detected in current event.
            telemetry: Raw current sensor readings.

        Returns:
            HealthEvaluation: Comprehensive health score and state classification.
        """
        reasons: List[str] = []

        # 1. Vibration Degradation Component [0.0 - 1.0]
        vib = telemetry.get("vibration", 0.0)
        vib_limit = self.config.l1_vibration_critical_rms
        if vib <= 2.0:
            d_vib = 0.0
        elif vib <= 4.5:
            d_vib = 0.40 * ((vib - 2.0) / 2.5)
        elif vib < vib_limit:
            d_vib = 0.40 + 0.60 * ((vib - 4.5) / (vib_limit - 4.5))
            reasons.append(f"Elevated vibration RMS ({vib:.2f} mm/s)")
        else:
            d_vib = 1.0
            reasons.append(f"Critical ISO Zone D vibration breach ({vib:.2f} mm/s)")

        # 2. Thermal Degradation Component [0.0 - 1.0]
        temp = telemetry.get("temperature", 0.0)
        temp_limit = self.config.l1_temp_max_turbine_c if "TURBINE" in state.asset_id else self.config.l1_temp_max_generic_c
        temp_baseline = 65.0 if "TURBINE" not in state.asset_id else 650.0
        if temp <= temp_baseline:
            d_temp = 0.0
        elif temp < (temp_limit - 15.0):
            d_temp = 0.50 * ((temp - temp_baseline) / max(1.0, (temp_limit - 15.0 - temp_baseline)))
        else:
            d_temp = 0.50 + 0.50 * min(1.0, (temp - (temp_limit - 15.0)) / 15.0)
            reasons.append(f"High thermal stress ({temp:.1f}°C)")

        # Additional thermal penalty for high rise rate
        if features.thermal_rise_rate > 3.0:
            d_temp = min(1.0, d_temp + 0.30)
            reasons.append(f"Rapid thermal rise ({features.thermal_rise_rate:.1f}°C/min)")

        # 3. Pressure / Hydraulic Degradation Component [0.0 - 1.0]
        d_press = 0.0
        if features.pressure_instability_index > 0.08:
            d_press = min(1.0, features.pressure_instability_index * 4.0)
            reasons.append(f"Pressure hydraulic instability ({features.pressure_instability_index:.3f})")

        # 4. Electrical Degradation Component [0.0 - 1.0]
        d_elec = 0.0
        if features.current_ratio > 1.05:
            d_elec = min(1.0, (features.current_ratio - 1.0) * 3.0)
            if features.current_ratio > 1.20:
                reasons.append(f"Motor overcurrent draw ({features.current_ratio:.2f}x rated)")

        # 5. Fault Persistence Degradation Component [0.0 - 1.0]
        if anomalies:
            state.consecutive_fault_count += 1
            state.consecutive_healthy_samples = 0
            d_anomaly_penalty = min(0.40, len(anomalies) * 0.15)
        else:
            state.consecutive_fault_count = max(0, state.consecutive_fault_count - 1)
            state.consecutive_healthy_samples += 1
            d_anomaly_penalty = 0.0

        d_pers = min(1.0, state.consecutive_fault_count / 8.0)

        # Composite Health Formula
        total_penalty = (
            self.config.health_weight_vibration * d_vib
            + self.config.health_weight_temperature * d_temp
            + self.config.health_weight_pressure * d_press
            + self.config.health_weight_electrical * d_elec
            + self.config.health_weight_persistence * d_pers
            + d_anomaly_penalty
        )

        health_score = round(max(0.0, min(1.0, 1.0 - total_penalty)), 3)

        # Hysteresis State Evaluation
        prev_state = state.current_health_state
        new_state = self._evaluate_hysteresis_state(prev_state, health_score, anomalies)

        is_transition = (new_state != prev_state)
        severity_order = {HealthState.HEALTHY: 0, HealthState.WATCH: 1, HealthState.DEGRADED: 2, HealthState.CRITICAL: 3}
        is_recovery = is_transition and (severity_order[new_state] < severity_order[prev_state])

        # Update state object
        state.previous_health_state = prev_state
        state.current_health_state = new_state
        state.current_health_score = health_score

        # Determine Maintenance Priority
        if new_state == HealthState.CRITICAL:
            priority = MaintenancePriority.EMERGENCY
        elif new_state == HealthState.DEGRADED:
            priority = MaintenancePriority.URGENT
        elif new_state == HealthState.WATCH:
            priority = MaintenancePriority.SCHEDULED
        else:
            priority = MaintenancePriority.NORMAL

        return HealthEvaluation(
            asset_id=state.asset_id,
            timestamp=features.timestamp,
            health_score=health_score,
            health_state=new_state,
            previous_health_state=prev_state,
            is_state_transition=is_transition,
            is_recovery=is_recovery,
            sub_scores={
                "vibration_degradation": round(d_vib, 3),
                "thermal_degradation": round(d_temp, 3),
                "pressure_degradation": round(d_press, 3),
                "electrical_degradation": round(d_elec, 3),
                "persistence_degradation": round(d_pers, 3),
            },
            maintenance_priority=priority,
            degradation_reasons=reasons,
        )

    def _evaluate_hysteresis_state(
        self,
        current_state: HealthState,
        score: float,
        anomalies: List[AnomalyRecord],
    ) -> HealthState:
        """Asymmetric hysteresis state transition resolver."""
        # Critical trip override: any Level 1 Critical anomaly forces CRITICAL state immediately
        has_critical_l1 = any(a.level == 1 and a.severity == AnomalySeverity.CRITICAL for a in anomalies)
        if has_critical_l1 or score < self.config.health_threshold_critical:
            return HealthState.CRITICAL

        if current_state == HealthState.CRITICAL:
            if score >= self.config.health_recover_degraded:
                return HealthState.DEGRADED
            return HealthState.CRITICAL

        elif current_state == HealthState.DEGRADED:
            if score < self.config.health_threshold_critical:
                return HealthState.CRITICAL
            elif score >= self.config.health_recover_watch:
                return HealthState.WATCH
            return HealthState.DEGRADED

        elif current_state == HealthState.WATCH:
            if score < self.config.health_threshold_degraded:
                return HealthState.DEGRADED
            elif score >= self.config.health_recover_healthy:
                return HealthState.HEALTHY
            return HealthState.WATCH

        else:  # Currently HEALTHY
            if score < self.config.health_threshold_degraded:
                return HealthState.DEGRADED
            elif score < self.config.health_threshold_watch:
                return HealthState.WATCH
            return HealthState.HEALTHY
