"""
ForgeStream Phase 4 Serving Orchestration Service.
Unifies telemetry ingestion, streaming state (health, alerts, windows),
Phase 3 ML model prognostic inference (failure risk, RUL), and operational
KPI calculation into Lakehouse serving datasets.
"""

from datetime import datetime, timezone
import json
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple, Union
import numpy as np
import pandas as pd

from forgestream.observability.logging import get_logger
from forgestream.observability.metrics import metrics
from forgestream.schemas.telemetry_schema import TelemetryEvent
from forgestream.serving.schemas import (
    AnomalySeverityEnum,
    AssetAlertHistory,
    AssetCurrentState,
    AssetPredictionHistory,
    AssetTelemetrySummary,
    FleetHealthSummary,
    HealthStateEnum,
    MaintenancePriorityEnum,
    MaintenancePriorityItem,
)
from forgestream.serving.kpis import OperationalKPIEngine
from forgestream.serving.store import ServingStoreManager
from forgestream.streaming.job import StreamProcessingJob
from forgestream.streaming.schemas import (
    AssetAlertEvent,
    HealthState as StreamHealthState,
    MaintenancePriority as StreamMaintPriority,
)

logger = get_logger("serving.service")


def _map_maintenance_priority(val: Any) -> MaintenancePriorityEnum:
    if hasattr(val, "value"):
        val = val.value
    val_str = str(val).upper()
    mapping = {
        "NORMAL": MaintenancePriorityEnum.LOW,
        "LOW": MaintenancePriorityEnum.LOW,
        "SCHEDULED": MaintenancePriorityEnum.MEDIUM,
        "MEDIUM": MaintenancePriorityEnum.MEDIUM,
        "URGENT": MaintenancePriorityEnum.HIGH,
        "HIGH": MaintenancePriorityEnum.HIGH,
        "EMERGENCY": MaintenancePriorityEnum.EMERGENCY,
        "CRITICAL": MaintenancePriorityEnum.EMERGENCY,
    }
    return mapping.get(val_str, MaintenancePriorityEnum.LOW)


def _map_anomaly_severity(val: Any) -> AnomalySeverityEnum:
    if hasattr(val, "value"):
        val = val.value
    val_str = str(val).upper()
    mapping = {
        "INFO": AnomalySeverityEnum.INFO,
        "WARNING": AnomalySeverityEnum.WARNING,
        "HIGH": AnomalySeverityEnum.HIGH,
        "CRITICAL": AnomalySeverityEnum.CRITICAL,
    }
    return mapping.get(val_str, AnomalySeverityEnum.WARNING)


def _map_health_state(val: Any) -> HealthStateEnum:
    if hasattr(val, "value"):
        val = val.value
    val_str = str(val).upper()
    mapping = {
        "HEALTHY": HealthStateEnum.HEALTHY,
        "WATCH": HealthStateEnum.WATCH,
        "DEGRADED": HealthStateEnum.DEGRADED,
        "CRITICAL": HealthStateEnum.CRITICAL,
    }
    return mapping.get(val_str, HealthStateEnum.HEALTHY)


class LakehouseServingService:
    """
    Coordinates Lakehouse serving workflows, predictive scoring updates,
    and operational KPI materialization.
    """

    def __init__(
        self,
        store_manager: Optional[ServingStoreManager] = None,
        kpi_engine: Optional[OperationalKPIEngine] = None,
        stream_job: Optional[StreamProcessingJob] = None,
        model_version: str = "v3.0.0-champion",
    ):
        self.store = store_manager or ServingStoreManager()
        self.kpi_engine = kpi_engine or OperationalKPIEngine()
        self.stream_job = stream_job or StreamProcessingJob()
        self.model_version = model_version

    def process_telemetry_batch(
        self,
        events: List[Union[TelemetryEvent, Dict[str, Any]]],
        ml_inference_engine: Optional[Any] = None,
        asset_criticalities: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Processes a batch of industrial telemetry events:
          1. Evaluates streaming anomaly detection and health state machine via StreamProcessingJob
          2. Generates alerts for state transitions and threshold breaches
          3. Evaluates Phase 3 ML prognostic models (failure probability & RUL)
          4. Updates asset current state snapshots and history tables
          5. Computes operational KPIs and maintenance priority ranking
        """
        start_t = time.perf_counter()
        if not events:
            return {
                "events_processed": 0,
                "current_states_updated": 0,
                "predictions_generated": 0,
                "alerts_generated": 0,
                "fleet_summary": self.kpi_engine.compute_fleet_summary([]),
                "duration_ms": 0.0,
            }

        parsed_events: List[Dict[str, Any]] = []
        for e in events:
            if isinstance(e, TelemetryEvent):
                parsed_events.append(e.model_dump())
            elif isinstance(e, dict):
                parsed_events.append(e)

        # Sort by timestamp
        parsed_events.sort(key=lambda x: float(x.get("timestamp", 0.0)))

        generated_alerts: List[AssetAlertHistory] = []
        updated_states: List[AssetCurrentState] = []
        generated_predictions: List[AssetPredictionHistory] = []
        now_utc = datetime.now(timezone.utc)

        # Process each event through stream_job
        latest_event_per_asset: Dict[str, Dict[str, Any]] = {}
        stream_results_per_asset: Dict[str, Dict[str, Any]] = {}

        for evt in parsed_events:
            aid = str(evt.get("asset_id", "UNKNOWN"))
            latest_event_per_asset[aid] = evt

            res = self.stream_job.process_event(evt)
            stream_results_per_asset[aid] = res

            # If alert emitted, capture in historical record
            alert_dict = res.get("alert_emitted")
            if alert_dict:
                a_type = evt.get("asset_type")
                asset_type = a_type.value if hasattr(a_type, "value") else str(a_type or "MOTOR")
                severity_val = alert_dict.get("severity", "MEDIUM")
                if hasattr(severity_val, "value"):
                    severity_val = severity_val.value
                h_state_val = alert_dict.get("current_health_state", "HEALTHY")
                if hasattr(h_state_val, "value"):
                    h_state_val = h_state_val.value
                m_prio_val = alert_dict.get("maintenance_priority", "MEDIUM")
                if hasattr(m_prio_val, "value"):
                    m_prio_val = m_prio_val.value

                alert_history = AssetAlertHistory(
                    alert_id=str(alert_dict.get("alert_id", f"ALT-{uuid.uuid4().hex[:8].upper()}")),
                    asset_id=aid,
                    asset_type=asset_type,
                    severity=_map_anomaly_severity(severity_val),
                    health_state=_map_health_state(h_state_val),
                    health_score=float(alert_dict.get("health_score", 1.0)),
                    is_state_transition=bool(alert_dict.get("is_state_transition", False)),
                    is_recovery=bool(alert_dict.get("is_recovery", False)),
                    reason_codes=json.dumps(alert_dict.get("reason_codes", [])),
                    triggering_values=json.dumps(alert_dict.get("triggering_values", {})),
                    maintenance_priority=_map_maintenance_priority(m_prio_val),
                    timestamp=float(alert_dict.get("timestamp", time.time())),
                    event_time=now_utc,
                )
                generated_alerts.append(alert_history)

        # Compute Prognostics and Current State per asset
        for asset_id, latest_evt in latest_event_per_asset.items():
            res = stream_results_per_asset[asset_id]
            a_type = latest_evt.get("asset_type")
            asset_type = a_type.value if hasattr(a_type, "value") else str(a_type or "MOTOR")
            op_mode = str(latest_evt.get("operating_mode", "NORMAL"))
            evt_ts = float(latest_evt.get("timestamp", time.time()))

            current_health = float(res.get("health_score", 1.0))
            h_state_str = str(res.get("health_state", "HEALTHY"))
            try:
                h_state_enum = HealthStateEnum(h_state_str)
            except Exception:
                h_state_enum = HealthStateEnum.HEALTHY

            anomalies = res.get("anomalies", [])
            highest_anomaly_level = max([a.get("level", 0) for a in anomalies], default=0)

            temp_latest = float(latest_evt.get("temperature", 60.0))
            vib_latest = float(latest_evt.get("vibration", 1.5))
            press_latest = float(latest_evt.get("pressure", 4.0))
            load_latest = float(latest_evt.get("load", 75.0))
            rpm_latest = float(latest_evt.get("rpm", 1750.0))
            power_latest = float(latest_evt.get("power", 15.0))

            failure_prob = 0.0
            pred_risk = 0
            predicted_rul = 120.0
            top_feats_summary = ""
            inf_latency = 0.5

            if ml_inference_engine is not None and hasattr(ml_inference_engine, "predict_dataframe"):
                try:
                    feat_row = {
                        "asset_id": asset_id,
                        "asset_type": asset_type,
                        "timestamp": evt_ts,
                        "temperature": temp_latest,
                        "vibration": vib_latest,
                        "pressure": press_latest,
                        "load": load_latest,
                        "rpm": rpm_latest,
                        "power": power_latest,
                        "health_score": current_health,
                        "temperature_mean_30s": temp_latest,
                        "temperature_std_30s": 0.5,
                        "temperature_slope_30s": 0.0,
                        "vibration_mean_30s": vib_latest,
                        "vibration_std_30s": 0.05,
                        "vibration_slope_30s": 0.0,
                        "pressure_mean_30s": press_latest,
                        "pressure_std_30s": 0.1,
                        "load_mean_30s": load_latest,
                        "power_mean_30s": power_latest,
                        "anomaly_count_30s": len(anomalies),
                        "highest_anomaly_level": highest_anomaly_level,
                        "thermal_power_ratio": temp_latest / max(1.0, power_latest),
                        "vibration_load_ratio": vib_latest / max(1.0, load_latest),
                        "apparent_power_kba": power_latest,
                        "operating_mode": op_mode,
                    }
                    df_single = pd.DataFrame([feat_row])
                    preds = ml_inference_engine.predict_dataframe(df_single)
                    if preds:
                        p_evt = preds[0]
                        failure_prob = p_evt.failure_probability
                        pred_risk = p_evt.predicted_failure_risk
                        predicted_rul = p_evt.predicted_rul_hours
                        inf_latency = p_evt.inference_latency_ms
                        top_feats_summary = json.dumps([f.model_dump() for f in p_evt.top_contributing_features])
                except Exception as ex:
                    logger.warning(f"ML inference error on asset {asset_id}: {ex}")

            if ml_inference_engine is None or failure_prob == 0.0:
                failure_prob = round(float(np.clip(1.0 - current_health, 0.0, 1.0)), 4)
                if temp_latest > 90.0 or vib_latest > 5.0:
                    failure_prob = min(1.0, failure_prob + 0.25)
                pred_risk = 1 if failure_prob >= 0.50 else 0
                predicted_rul = round(float(np.clip(current_health * 120.0, 0.0, 120.0)), 2)
                top_feats_summary = json.dumps([
                    {"feature": "health_score", "importance": 0.38, "observed_value": current_health},
                    {"feature": "vibration", "importance": 0.28, "observed_value": vib_latest},
                    {"feature": "temperature", "importance": 0.22, "observed_value": temp_latest},
                ])

            maint_priority = MaintenancePriorityEnum.LOW
            if failure_prob >= 0.80 or predicted_rul <= 10.0 or h_state_enum == HealthStateEnum.CRITICAL:
                maint_priority = MaintenancePriorityEnum.EMERGENCY
            elif failure_prob >= 0.60 or predicted_rul <= 25.0 or h_state_enum == HealthStateEnum.DEGRADED:
                maint_priority = MaintenancePriorityEnum.HIGH
            elif failure_prob >= 0.35 or predicted_rul <= 50.0 or h_state_enum == HealthStateEnum.WATCH:
                maint_priority = MaintenancePriorityEnum.MEDIUM

            pred_history_item = AssetPredictionHistory(
                prediction_id=f"PRED-{uuid.uuid4().hex[:8].upper()}",
                asset_id=asset_id,
                asset_type=asset_type,
                timestamp=evt_ts,
                event_time=now_utc,
                failure_probability=failure_prob,
                predicted_failure_risk=pred_risk,
                predicted_rul_hours=predicted_rul,
                maintenance_priority=maint_priority,
                top_features_summary=top_feats_summary,
                model_version=self.model_version,
                inference_latency_ms=inf_latency,
            )
            generated_predictions.append(pred_history_item)

            curr_state = AssetCurrentState(
                asset_id=asset_id,
                asset_type=asset_type,
                operating_mode=op_mode,
                health_state=h_state_enum,
                health_score=round(current_health, 4),
                failure_probability=failure_prob,
                predicted_failure_risk=pred_risk,
                predicted_rul_hours=predicted_rul,
                maintenance_priority=maint_priority,
                anomaly_level=highest_anomaly_level,
                active_alert_count=len([a for a in generated_alerts if a.asset_id == asset_id and not a.is_recovery]),
                temperature=round(temp_latest, 2),
                vibration=round(vib_latest, 3),
                pressure=round(press_latest, 2),
                load=round(load_latest, 2),
                rpm=round(rpm_latest, 1),
                power=round(power_latest, 2),
                last_event_time=now_utc,
                last_prediction_time=now_utc,
                model_version=self.model_version,
            )
            updated_states.append(curr_state)

        # Persist batch updates to Serving Store
        for state in updated_states:
            self.store.upsert_current_state(state)

        self.store.record_predictions_batch(generated_predictions)
        self.store.record_alerts_batch(generated_alerts)

        # Compute Fleet Summary & Rank Maintenance Queue
        all_states = self.store.get_all_current_states()
        fleet_summary = self.kpi_engine.compute_fleet_summary(all_states, active_alert_count=len(generated_alerts))
        ranked_queue = self.kpi_engine.rank_maintenance_work_orders(all_states, asset_criticalities)
        self.store.update_maintenance_queue(ranked_queue)

        snapshot_id = f"SNAP-{uuid.uuid4().hex[:8].upper()}"
        self.store.record_fleet_kpi_snapshot(snapshot_id, fleet_summary)

        elapsed_ms = (time.perf_counter() - start_t) * 1000.0
        metrics.increment("serving_events_processed", len(events))
        metrics.record_latency("serving_pipeline_duration_ms", elapsed_ms)

        logger.info(
            f"Serving batch processed: {len(events)} events, {len(updated_states)} states updated, "
            f"{len(generated_predictions)} predictions, {len(generated_alerts)} alerts, "
            f"fleet_health={fleet_summary.fleet_health_score:.4f}, duration={elapsed_ms:.2f}ms"
        )
        return {
            "events_processed": len(events),
            "current_states_updated": len(updated_states),
            "predictions_generated": len(generated_predictions),
            "alerts_generated": len(generated_alerts),
            "fleet_summary": fleet_summary,
            "ranked_queue": ranked_queue,
            "duration_ms": elapsed_ms,
        }
