"""Alert quality engine, suppression, cooldown, and lifecycle manager for ForgeStream Phase 2."""

import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from forgestream.streaming.config import StreamingConfig
from forgestream.streaming.schemas import (
    AssetAlertEvent,
    AnomalySeverity,
    HealthState,
    MaintenancePriority,
    AnomalyRecord,
)
from forgestream.streaming.features import StreamFeatures
from forgestream.streaming.health import HealthEvaluation
from forgestream.streaming.state import AssetStreamingState


class AlertQualityEngine:
    """Evaluates whether alerts should be emitted, suppressed by cooldown, or escalated."""

    def __init__(self, config: Optional[StreamingConfig] = None):
        self.config = config or StreamingConfig()
        self.total_alerts_evaluated: int = 0
        self.total_alerts_emitted: int = 0
        self.total_alerts_suppressed: int = 0
        self.total_state_transitions: int = 0
        self.total_recoveries: int = 0

    def evaluate_and_generate_alert(
        self,
        state: AssetStreamingState,
        health: HealthEvaluation,
        anomalies: List[AnomalyRecord],
        features: StreamFeatures,
        telemetry: Dict[str, Any],
    ) -> Optional[AssetAlertEvent]:
        """Determine if an alert should be emitted for the current health and anomaly state.

        Args:
            state: Keyed asset streaming state.
            health: Current health evaluation.
            anomalies: Active detected anomalies.
            features: Calculated real-time features.
            telemetry: Raw current telemetry readings.

        Returns:
            Optional[AssetAlertEvent]: Emitted alert event, or None if suppressed.
        """
        self.total_alerts_evaluated += 1
        current_time = features.timestamp

        # Determine highest severity among active anomalies or health state
        if health.health_state == HealthState.CRITICAL:
            severity = AnomalySeverity.CRITICAL
        elif health.health_state == HealthState.DEGRADED:
            severity = AnomalySeverity.HIGH
        elif health.health_state == HealthState.WATCH:
            severity = AnomalySeverity.WARNING
        else:
            severity = AnomalySeverity.INFO

        for a in anomalies:
            if a.severity == AnomalySeverity.CRITICAL:
                severity = AnomalySeverity.CRITICAL
                break
            elif a.severity == AnomalySeverity.HIGH and severity != AnomalySeverity.CRITICAL:
                severity = AnomalySeverity.HIGH
            elif a.severity == AnomalySeverity.WARNING and severity == AnomalySeverity.INFO:
                severity = AnomalySeverity.WARNING

        # -----------------------------------------------------------------
        # GATING POLICY:
        # 1. Healthy state with no anomalies -> No alert needed (unless recovery)
        # 2. State transition or recovery -> ALWAYS EMIT
        # 3. Escalation (severity increased) -> ALWAYS EMIT
        # 4. Same state & severity -> Suppress if within cooldown window
        # -----------------------------------------------------------------

        if health.health_state == HealthState.HEALTHY and not anomalies and not health.is_recovery:
            return None

        # Check if severity escalated
        severity_ranks = {AnomalySeverity.INFO: 0, AnomalySeverity.WARNING: 1, AnomalySeverity.HIGH: 2, AnomalySeverity.CRITICAL: 3}
        is_escalation = False
        if state.last_alert_severity is not None:
            if severity_ranks[severity] > severity_ranks[state.last_alert_severity]:
                is_escalation = True

        # Check cooldown condition
        time_since_last = current_time - state.last_alert_timestamp
        in_cooldown = time_since_last < self.config.alert_cooldown_sec

        should_emit = False

        if health.is_state_transition:
            should_emit = True
            self.total_state_transitions += 1
            if health.is_recovery:
                self.total_recoveries += 1
        elif is_escalation and self.config.alert_escalation_enabled:
            should_emit = True
        elif not in_cooldown and (anomalies or health.health_state != HealthState.HEALTHY):
            should_emit = True

        if not should_emit:
            self.total_suppressed_alerts = getattr(self, "total_suppressed_alerts", 0) + 1
            self.total_alerts_suppressed += 1
            return None

        # Update state alert trackers
        state.last_alert_timestamp = current_time
        state.last_alert_severity = severity
        alert_id = f"alert-{state.asset_id[:4]}-{int(current_time)}-{uuid.uuid4().hex[:6]}"
        state.active_alert_id = alert_id
        self.total_alerts_emitted += 1

        # Build triggering values map
        trigger_vals = {}
        for k in ["temperature", "vibration", "pressure", "current", "voltage", "power", "speed"]:
            if k in telemetry and telemetry[k] is not None:
                trigger_vals[k] = round(float(telemetry[k]), 2)

        # Build supporting features map
        supporting = {
            "thermal_rise_rate": features.thermal_rise_rate,
            "vibration_slope": features.vibration_slope,
            "pressure_instability": features.pressure_instability_index,
            "current_ratio": features.current_ratio,
            "vibration_zscore": features.vibration_zscore,
            "mean_vibration": features.mean_vibration,
            "mean_temperature": features.mean_temperature,
        }

        # Reason codes and anomaly types
        anomaly_types = [a.anomaly_type for a in anomalies]
        reason_codes = [a.reason_code for a in anomalies] + health.degradation_reasons

        # Format ISO timestamp
        dt = datetime.fromtimestamp(current_time, tz=timezone.utc)
        iso_event_time = dt.isoformat()

        return AssetAlertEvent(
            alert_id=alert_id,
            asset_id=state.asset_id,
            asset_type=state.asset_type,
            event_time=iso_event_time,
            timestamp=current_time,
            alert_timestamp=time.time(),
            severity=severity,
            previous_health_state=health.previous_health_state,
            current_health_state=health.health_state,
            health_score=health.health_score,
            is_state_transition=health.is_state_transition,
            is_recovery=health.is_recovery,
            anomaly_types=anomaly_types,
            reason_codes=reason_codes,
            triggering_values=trigger_vals,
            supporting_features=supporting,
            maintenance_priority=health.maintenance_priority,
            schema_version="2.0.0",
        )
