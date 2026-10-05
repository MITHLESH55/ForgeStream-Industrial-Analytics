"""3-Level explainable anomaly detection engine for ForgeStream Phase 2."""

import uuid
from typing import Dict, List, Optional, Any
from forgestream.streaming.config import StreamingConfig
from forgestream.streaming.schemas import AnomalyRecord, AnomalySeverity
from forgestream.streaming.features import StreamFeatures
from forgestream.streaming.state import AssetStreamingState


class StreamingAnomalyDetector:
    """Evaluates 3-level explainable anomaly rules against real-time features and state."""

    def __init__(self, config: Optional[StreamingConfig] = None):
        self.config = config or StreamingConfig()

    def evaluate(
        self,
        state: AssetStreamingState,
        features: StreamFeatures,
        telemetry: Dict[str, Any],
    ) -> List[AnomalyRecord]:
        """Run all 3 detection levels and return list of detected anomalies.

        Args:
            state: Keyed asset streaming state.
            features: Real-time calculated stream features.
            telemetry: Raw current telemetry reading dictionary.

        Returns:
            List[AnomalyRecord]: List of detected anomalies with explainable diagnostics.
        """
        anomalies: List[AnomalyRecord] = []
        ts = features.timestamp

        # Extract current sensor readings
        temp = telemetry.get("temperature", 0.0)
        vib = telemetry.get("vibration", 0.0)
        press = telemetry.get("pressure", 0.0)
        curr = telemetry.get("current", 0.0)
        volt = telemetry.get("voltage", 0.0)
        speed = telemetry.get("speed", 0.0)

        # ---------------------------------------------------------
        # LEVEL 1: Deterministic Physical / Engineering Bounds
        # ---------------------------------------------------------

        # L1.1: Maximum Temperature Trip
        temp_limit = self.config.l1_temp_max_turbine_c if "TURBINE" in state.asset_id else self.config.l1_temp_max_generic_c
        if temp > temp_limit:
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=f"ano-l1-temp-{uuid.uuid4().hex[:6]}",
                    anomaly_type="L1_TEMPERATURE_TRIP",
                    level=1,
                    severity=AnomalySeverity.CRITICAL,
                    reason_code="ERR_L1_TEMP_EXCEEDED",
                    description=f"Temperature {temp:.1f}°C breached absolute physical trip limit {temp_limit:.1f}°C",
                    triggering_value=round(temp, 2),
                    threshold_value=round(temp_limit, 2),
                    timestamp=ts,
                )
            )

        # L1.2: ISO 10816 Zone D Critical Vibration
        if vib > self.config.l1_vibration_critical_rms:
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=f"ano-l1-vib-{uuid.uuid4().hex[:6]}",
                    anomaly_type="L1_VIBRATION_CRITICAL_ISO",
                    level=1,
                    severity=AnomalySeverity.CRITICAL,
                    reason_code="ERR_L1_VIB_ZONE_D",
                    description=f"Vibration {vib:.2f} mm/s breached ISO 10816 Zone D critical trip {self.config.l1_vibration_critical_rms:.1f} mm/s",
                    triggering_value=round(vib, 2),
                    threshold_value=round(self.config.l1_vibration_critical_rms, 2),
                    timestamp=ts,
                )
            )

        # L1.3: Electrical Overcurrent Trip
        if features.current_ratio > self.config.l1_overcurrent_ratio:
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=f"ano-l1-curr-{uuid.uuid4().hex[:6]}",
                    anomaly_type="L1_OVERCURRENT_TRIP",
                    level=1,
                    severity=AnomalySeverity.HIGH,
                    reason_code="ERR_L1_OVERCURRENT",
                    description=f"Current draw {curr:.1f}A exceeded rated ceiling ({features.current_ratio:.2f}x rated)",
                    triggering_value=round(curr, 2),
                    threshold_value=round(state.baseline_current or 100.0, 2),
                    timestamp=ts,
                )
            )

        # ---------------------------------------------------------
        # LEVEL 2: Contextual Rolling Statistical Deviations
        # ---------------------------------------------------------

        # L2.1: Rapid Thermal Rise Rate (dT/dt)
        if features.thermal_rise_rate > self.config.l2_thermal_rise_rate_threshold:
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=f"ano-l2-thermal-{uuid.uuid4().hex[:6]}",
                    anomaly_type="L2_THERMAL_RISE_RATE",
                    level=2,
                    severity=AnomalySeverity.WARNING if features.thermal_rise_rate < 8.0 else AnomalySeverity.HIGH,
                    reason_code="WARN_L2_RAPID_HEATING",
                    description=f"Thermal rise rate {features.thermal_rise_rate:.2f}°C/min exceeded dynamic threshold {self.config.l2_thermal_rise_rate_threshold:.1f}°C/min",
                    triggering_value=round(features.thermal_rise_rate, 2),
                    threshold_value=round(self.config.l2_thermal_rise_rate_threshold, 2),
                    timestamp=ts,
                )
            )

        # L2.2: Vibration Rolling Z-Score Breach
        if abs(features.vibration_zscore) > self.config.l2_vibration_zscore_threshold and features.sample_count >= 5:
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=f"ano-l2-vibz-{uuid.uuid4().hex[:6]}",
                    anomaly_type="L2_VIBRATION_ZSCORE_DEVIATION",
                    level=2,
                    severity=AnomalySeverity.WARNING if abs(features.vibration_zscore) < 4.5 else AnomalySeverity.HIGH,
                    reason_code="WARN_L2_VIB_STATISTICAL_DEVIATION",
                    description=f"Vibration z-score {features.vibration_zscore:.2f} deviated from rolling baseline (mean: {features.mean_vibration:.2f} mm/s)",
                    triggering_value=round(features.vibration_zscore, 2),
                    threshold_value=round(self.config.l2_vibration_zscore_threshold, 2),
                    timestamp=ts,
                )
            )

        # L2.3: Pressure Instability Deviation
        if features.pressure_instability_index > 0.15 and features.sample_count >= 5:
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=f"ano-l2-press-{uuid.uuid4().hex[:6]}",
                    anomaly_type="L2_PRESSURE_INSTABILITY",
                    level=2,
                    severity=AnomalySeverity.WARNING,
                    reason_code="WARN_L2_PRESSURE_VARIANCE",
                    description=f"Pressure coefficient of variation {features.pressure_instability_index:.3f} indicates hydraulic oscillation",
                    triggering_value=round(features.pressure_instability_index, 3),
                    threshold_value=0.15,
                    timestamp=ts,
                )
            )

        # ---------------------------------------------------------
        # LEVEL 3: Multi-Signal Physical Correlation Engine
        # ---------------------------------------------------------

        # L3.1: Mechanical Bearing Degradation (High Vibration + Thermal Rise)
        if vib > 4.5 and features.thermal_rise_rate > 1.5:
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=f"ano-l3-bearing-{uuid.uuid4().hex[:6]}",
                    anomaly_type="L3_BEARING_DEGRADATION",
                    level=3,
                    severity=AnomalySeverity.HIGH,
                    reason_code="FAULT_L3_BEARING_FRICTION_COUPLING",
                    description=f"Coupled friction symptom: elevated vibration ({vib:.2f} mm/s) coinciding with thermal rise ({features.thermal_rise_rate:.2f}°C/min)",
                    triggering_value=round(vib, 2),
                    threshold_value=4.5,
                    timestamp=ts,
                )
            )

        # L3.2: Fluid Cavitation & Impeller Degradation (Pressure Instability + Vibration Spike)
        if features.pressure_instability_index > 0.12 and vib > 3.8:
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=f"ano-l3-cavitation-{uuid.uuid4().hex[:6]}",
                    anomaly_type="L3_FLUID_CAVITATION",
                    level=3,
                    severity=AnomalySeverity.HIGH,
                    reason_code="FAULT_L3_CAVITATION_SHOCKWAVES",
                    description=f"Cavitation signature: pressure instability ({features.pressure_instability_index:.3f}) coupled with high-frequency vibration ({vib:.2f} mm/s)",
                    triggering_value=round(features.pressure_instability_index, 3),
                    threshold_value=0.12,
                    timestamp=ts,
                )
            )

        # L3.3: Electrical Stator / Power Imbalance (High Current Ratio + Power Factor Drift)
        if features.current_ratio > 1.20 and features.power_factor_deviation > 0.05:
            anomalies.append(
                AnomalyRecord(
                    anomaly_id=f"ano-l3-electrical-{uuid.uuid4().hex[:6]}",
                    anomaly_type="L3_ELECTRICAL_STATOR_IMBALANCE",
                    level=3,
                    severity=AnomalySeverity.HIGH,
                    reason_code="FAULT_L3_WINDING_ASYMMETRY",
                    description=f"Electromagnetic degradation: overcurrent ({features.current_ratio:.2f}x rated) coupled with power factor distortion ({features.power_factor_deviation:.3f})",
                    triggering_value=round(features.current_ratio, 2),
                    threshold_value=1.20,
                    timestamp=ts,
                )
            )

        return anomalies
