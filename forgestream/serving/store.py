"""
ForgeStream Phase 4 Serving Data Store.
Provides unified persistence and retrieval across:
  1. PostgreSQL operational repository (and SQLite fallback) for transactional state
  2. Apache Iceberg / Parquet Lakehouse for analytical history
"""

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, List, Optional
import pyarrow as pa
from sqlalchemy import text
from forgestream.config import settings
from forgestream.iceberg.catalog import IcebergCatalogManager
from forgestream.iceberg.tables import IcebergTableManager
from forgestream.observability.logging import get_logger
from forgestream.postgres.connection import DatabaseManager
from forgestream.serving.schemas import (
    AssetAlertHistory,
    AssetCurrentState,
    AssetPredictionHistory,
    AssetTelemetrySummary,
    FleetHealthSummary,
    HealthStateEnum,
    MaintenancePriorityEnum,
    MaintenancePriorityItem,
)

logger = get_logger("serving.store")


class ServingStoreManager:
    """
    Manages persistence and query access for all Phase 4 serving datasets.
    """

    def __init__(
        self,
        db_manager: Optional[DatabaseManager] = None,
        table_manager: Optional[IcebergTableManager] = None,
    ):
        self.db = db_manager or DatabaseManager()
        self.table_mgr = table_manager or IcebergTableManager()
        self._init_relational_schema()

    def _init_relational_schema(self) -> None:
        """Ensures relational tables for serving datasets exist in PostgreSQL / SQLite."""
        schema_ddl = """
        CREATE TABLE IF NOT EXISTS asset_current_state (
            asset_id VARCHAR(64) PRIMARY KEY,
            asset_type VARCHAR(32) NOT NULL,
            operating_mode VARCHAR(32) NOT NULL,
            health_state VARCHAR(32) NOT NULL,
            health_score DOUBLE PRECISION NOT NULL,
            failure_probability DOUBLE PRECISION NOT NULL,
            predicted_failure_risk INT NOT NULL,
            predicted_rul_hours DOUBLE PRECISION NOT NULL,
            maintenance_priority VARCHAR(32) NOT NULL,
            anomaly_level INT NOT NULL DEFAULT 0,
            active_alert_count INT NOT NULL DEFAULT 0,
            temperature DOUBLE PRECISION NOT NULL,
            vibration DOUBLE PRECISION NOT NULL,
            pressure DOUBLE PRECISION NOT NULL,
            load DOUBLE PRECISION NOT NULL,
            rpm DOUBLE PRECISION NOT NULL,
            power DOUBLE PRECISION NOT NULL,
            last_event_time TIMESTAMP WITH TIME ZONE NOT NULL,
            last_prediction_time TIMESTAMP WITH TIME ZONE NOT NULL,
            model_version VARCHAR(64) NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS asset_prediction_history (
            prediction_id VARCHAR(64) PRIMARY KEY,
            asset_id VARCHAR(64) NOT NULL,
            asset_type VARCHAR(32) NOT NULL,
            timestamp DOUBLE PRECISION NOT NULL,
            event_time TIMESTAMP WITH TIME ZONE NOT NULL,
            failure_probability DOUBLE PRECISION NOT NULL,
            predicted_failure_risk INT NOT NULL,
            predicted_rul_hours DOUBLE PRECISION NOT NULL,
            maintenance_priority VARCHAR(32) NOT NULL,
            top_features_summary TEXT,
            model_version VARCHAR(64) NOT NULL,
            inference_latency_ms DOUBLE PRECISION DEFAULT 0.0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS asset_alert_history (
            alert_id VARCHAR(64) PRIMARY KEY,
            asset_id VARCHAR(64) NOT NULL,
            asset_type VARCHAR(32) NOT NULL,
            severity VARCHAR(32) NOT NULL,
            health_state VARCHAR(32) NOT NULL,
            health_score DOUBLE PRECISION NOT NULL,
            is_state_transition BOOLEAN DEFAULT FALSE,
            is_recovery BOOLEAN DEFAULT FALSE,
            reason_codes TEXT,
            triggering_values TEXT,
            maintenance_priority VARCHAR(32) NOT NULL,
            timestamp DOUBLE PRECISION NOT NULL,
            event_time TIMESTAMP WITH TIME ZONE NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS asset_telemetry_summary (
            summary_id VARCHAR(64) PRIMARY KEY,
            asset_id VARCHAR(64) NOT NULL,
            asset_type VARCHAR(32) NOT NULL,
            window_start TIMESTAMP WITH TIME ZONE NOT NULL,
            window_end TIMESTAMP WITH TIME ZONE NOT NULL,
            event_count INT NOT NULL,
            avg_temperature DOUBLE PRECISION NOT NULL,
            max_temperature DOUBLE PRECISION NOT NULL,
            avg_vibration DOUBLE PRECISION NOT NULL,
            max_vibration DOUBLE PRECISION NOT NULL,
            avg_pressure DOUBLE PRECISION NOT NULL,
            avg_load DOUBLE PRECISION NOT NULL,
            avg_power DOUBLE PRECISION NOT NULL,
            thermal_rise_rate DOUBLE PRECISION DEFAULT 0.0,
            vibration_slope DOUBLE PRECISION DEFAULT 0.0,
            anomaly_count INT NOT NULL DEFAULT 0,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS maintenance_priority_queue (
            asset_id VARCHAR(64) PRIMARY KEY,
            ranking INT NOT NULL,
            asset_type VARCHAR(32) NOT NULL,
            criticality VARCHAR(32) NOT NULL,
            health_state VARCHAR(32) NOT NULL,
            health_score DOUBLE PRECISION NOT NULL,
            failure_probability DOUBLE PRECISION NOT NULL,
            predicted_rul_hours DOUBLE PRECISION NOT NULL,
            maintenance_priority VARCHAR(32) NOT NULL,
            priority_score DOUBLE PRECISION NOT NULL,
            recommended_action TEXT NOT NULL,
            evaluated_at TIMESTAMP WITH TIME ZONE NOT NULL
        );

        CREATE TABLE IF NOT EXISTS fleet_kpi_snapshots (
            snapshot_id VARCHAR(64) PRIMARY KEY,
            timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
            total_assets INT NOT NULL,
            healthy_assets INT NOT NULL,
            watch_assets INT NOT NULL,
            degraded_assets INT NOT NULL,
            critical_assets INT NOT NULL,
            high_risk_assets INT NOT NULL,
            near_failure_assets INT NOT NULL,
            fleet_health_score DOUBLE PRECISION NOT NULL,
            avg_predicted_rul_hours DOUBLE PRECISION NOT NULL,
            failure_risk_rate DOUBLE PRECISION NOT NULL,
            active_alerts_total INT NOT NULL,
            emergency_maintenance_count INT NOT NULL,
            high_maintenance_count INT NOT NULL
        );
        """
        with self.db.engine.begin() as conn:
            # Execute statement by statement
            for stmt in schema_ddl.strip().split(";"):
                clean = stmt.strip()
                if clean:
                    conn.execute(text(clean))

    # -------------------------------------------------------------------------
    # 1. Upsert Asset Current State
    # -------------------------------------------------------------------------
    def upsert_current_state(self, state: AssetCurrentState) -> None:
        """Upserts current state snapshot in operational DB."""
        now_str = datetime.now(timezone.utc).isoformat()
        evt_str = state.last_event_time.isoformat()
        pred_str = state.last_prediction_time.isoformat()

        with self.db.engine.begin() as conn:
            stmt_check = text("SELECT asset_id FROM asset_current_state WHERE asset_id = :asset_id")
            exists = conn.execute(stmt_check, {"asset_id": state.asset_id}).fetchone()

            params = {
                "asset_id": state.asset_id,
                "asset_type": state.asset_type,
                "operating_mode": state.operating_mode,
                "health_state": state.health_state.value if hasattr(state.health_state, "value") else str(state.health_state),
                "health_score": state.health_score,
                "failure_probability": state.failure_probability,
                "predicted_failure_risk": state.predicted_failure_risk,
                "predicted_rul_hours": state.predicted_rul_hours,
                "maintenance_priority": state.maintenance_priority.value if hasattr(state.maintenance_priority, "value") else str(state.maintenance_priority),
                "anomaly_level": state.anomaly_level,
                "active_alert_count": state.active_alert_count,
                "temperature": state.temperature,
                "vibration": state.vibration,
                "pressure": state.pressure,
                "load": state.load,
                "rpm": state.rpm,
                "power": state.power,
                "last_event_time": evt_str,
                "last_prediction_time": pred_str,
                "model_version": state.model_version,
                "updated_at": now_str,
            }

            if exists:
                stmt_update = text("""
                    UPDATE asset_current_state SET
                        asset_type = :asset_type,
                        operating_mode = :operating_mode,
                        health_state = :health_state,
                        health_score = :health_score,
                        failure_probability = :failure_probability,
                        predicted_failure_risk = :predicted_failure_risk,
                        predicted_rul_hours = :predicted_rul_hours,
                        maintenance_priority = :maintenance_priority,
                        anomaly_level = :anomaly_level,
                        active_alert_count = :active_alert_count,
                        temperature = :temperature,
                        vibration = :vibration,
                        pressure = :pressure,
                        load = :load,
                        rpm = :rpm,
                        power = :power,
                        last_event_time = :last_event_time,
                        last_prediction_time = :last_prediction_time,
                        model_version = :model_version,
                        updated_at = :updated_at
                    WHERE asset_id = :asset_id
                """)
                conn.execute(stmt_update, params)
            else:
                stmt_insert = text("""
                    INSERT INTO asset_current_state (
                        asset_id, asset_type, operating_mode, health_state, health_score,
                        failure_probability, predicted_failure_risk, predicted_rul_hours,
                        maintenance_priority, anomaly_level, active_alert_count,
                        temperature, vibration, pressure, load, rpm, power,
                        last_event_time, last_prediction_time, model_version, updated_at
                    ) VALUES (
                        :asset_id, :asset_type, :operating_mode, :health_state, :health_score,
                        :failure_probability, :predicted_failure_risk, :predicted_rul_hours,
                        :maintenance_priority, :anomaly_level, :active_alert_count,
                        :temperature, :vibration, :pressure, :load, :rpm, :power,
                        :last_event_time, :last_prediction_time, :model_version, :updated_at
                    )
                """)
                conn.execute(stmt_insert, params)

    def get_current_state(self, asset_id: str) -> Optional[AssetCurrentState]:
        """Retrieves a single asset's current state from relational store."""
        with self.db.engine.connect() as conn:
            stmt = text("SELECT * FROM asset_current_state WHERE asset_id = :asset_id")
            row = conn.execute(stmt, {"asset_id": asset_id}).mappings().fetchone()
            if not row:
                return None
            d = dict(row)
            if isinstance(d.get("last_event_time"), str):
                d["last_event_time"] = datetime.fromisoformat(d["last_event_time"])
            if isinstance(d.get("last_prediction_time"), str):
                d["last_prediction_time"] = datetime.fromisoformat(d["last_prediction_time"])
            return AssetCurrentState(**d)

    def get_all_current_states(self) -> List[AssetCurrentState]:
        """Retrieves all current asset states from relational store."""
        with self.db.engine.connect() as conn:
            stmt = text("SELECT * FROM asset_current_state ORDER BY asset_id ASC")
            rows = conn.execute(stmt).mappings().fetchall()
            results = []
            for r in rows:
                d = dict(r)
                if isinstance(d.get("last_event_time"), str):
                    d["last_event_time"] = datetime.fromisoformat(d["last_event_time"])
                if isinstance(d.get("last_prediction_time"), str):
                    d["last_prediction_time"] = datetime.fromisoformat(d["last_prediction_time"])
                results.append(AssetCurrentState(**d))
            return results

    # -------------------------------------------------------------------------
    # 2. Record Prediction History
    # -------------------------------------------------------------------------
    def record_predictions_batch(self, predictions: List[AssetPredictionHistory]) -> None:
        """Inserts batch of ML prediction history events."""
        if not predictions:
            return
        with self.db.engine.begin() as conn:
            stmt = text("""
                INSERT INTO asset_prediction_history (
                    prediction_id, asset_id, asset_type, timestamp, event_time,
                    failure_probability, predicted_failure_risk, predicted_rul_hours,
                    maintenance_priority, top_features_summary, model_version,
                    inference_latency_ms, created_at
                ) VALUES (
                    :prediction_id, :asset_id, :asset_type, :timestamp, :event_time,
                    :failure_probability, :predicted_failure_risk, :predicted_rul_hours,
                    :maintenance_priority, :top_features_summary, :model_version,
                    :inference_latency_ms, :created_at
                )
            """)
            for p in predictions:
                conn.execute(stmt, {
                    "prediction_id": p.prediction_id,
                    "asset_id": p.asset_id,
                    "asset_type": p.asset_type,
                    "timestamp": p.timestamp,
                    "event_time": p.event_time.isoformat() if hasattr(p.event_time, "isoformat") else str(p.event_time),
                    "failure_probability": p.failure_probability,
                    "predicted_failure_risk": p.predicted_failure_risk,
                    "predicted_rul_hours": p.predicted_rul_hours,
                    "maintenance_priority": p.maintenance_priority.value if hasattr(p.maintenance_priority, "value") else str(p.maintenance_priority),
                    "top_features_summary": p.top_features_summary,
                    "model_version": p.model_version,
                    "inference_latency_ms": p.inference_latency_ms,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })

    # -------------------------------------------------------------------------
    # 3. Record Alert History
    # -------------------------------------------------------------------------
    def record_alerts_batch(self, alerts: List[AssetAlertHistory]) -> None:
        """Inserts batch of streaming alert history events."""
        if not alerts:
            return
        with self.db.engine.begin() as conn:
            stmt = text("""
                INSERT INTO asset_alert_history (
                    alert_id, asset_id, asset_type, severity, health_state,
                    health_score, is_state_transition, is_recovery,
                    reason_codes, triggering_values, maintenance_priority,
                    timestamp, event_time, created_at
                ) VALUES (
                    :alert_id, :asset_id, :asset_type, :severity, :health_state,
                    :health_score, :is_state_transition, :is_recovery,
                    :reason_codes, :triggering_values, :maintenance_priority,
                    :timestamp, :event_time, :created_at
                )
            """)
            for a in alerts:
                conn.execute(stmt, {
                    "alert_id": a.alert_id,
                    "asset_id": a.asset_id,
                    "asset_type": a.asset_type,
                    "severity": a.severity.value if hasattr(a.severity, "value") else str(a.severity),
                    "health_state": a.health_state.value if hasattr(a.health_state, "value") else str(a.health_state),
                    "health_score": a.health_score,
                    "is_state_transition": bool(a.is_state_transition),
                    "is_recovery": bool(a.is_recovery),
                    "reason_codes": a.reason_codes,
                    "triggering_values": a.triggering_values,
                    "maintenance_priority": a.maintenance_priority.value if hasattr(a.maintenance_priority, "value") else str(a.maintenance_priority),
                    "timestamp": a.timestamp,
                    "event_time": a.event_time.isoformat() if hasattr(a.event_time, "isoformat") else str(a.event_time),
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })

    # -------------------------------------------------------------------------
    # 4. Record Telemetry Window Summary
    # -------------------------------------------------------------------------
    def record_summaries_batch(self, summaries: List[AssetTelemetrySummary]) -> None:
        """Inserts batch of window summaries."""
        if not summaries:
            return
        with self.db.engine.begin() as conn:
            stmt = text("""
                INSERT INTO asset_telemetry_summary (
                    summary_id, asset_id, asset_type, window_start, window_end,
                    event_count, avg_temperature, max_temperature, avg_vibration,
                    max_vibration, avg_pressure, avg_load, avg_power,
                    thermal_rise_rate, vibration_slope, anomaly_count, created_at
                ) VALUES (
                    :summary_id, :asset_id, :asset_type, :window_start, :window_end,
                    :event_count, :avg_temperature, :max_temperature, :avg_vibration,
                    :max_vibration, :avg_pressure, :avg_load, :avg_power,
                    :thermal_rise_rate, :vibration_slope, :anomaly_count, :created_at
                )
            """)
            for s in summaries:
                conn.execute(stmt, {
                    "summary_id": s.summary_id,
                    "asset_id": s.asset_id,
                    "asset_type": s.asset_type,
                    "window_start": s.window_start.isoformat(),
                    "window_end": s.window_end.isoformat(),
                    "event_count": s.event_count,
                    "avg_temperature": s.avg_temperature,
                    "max_temperature": s.max_temperature,
                    "avg_vibration": s.avg_vibration,
                    "max_vibration": s.max_vibration,
                    "avg_pressure": s.avg_pressure,
                    "avg_load": s.avg_load,
                    "avg_power": s.avg_power,
                    "thermal_rise_rate": s.thermal_rise_rate,
                    "vibration_slope": s.vibration_slope,
                    "anomaly_count": s.anomaly_count,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                })

    # -------------------------------------------------------------------------
    # 5. Update Maintenance Priority Queue
    # -------------------------------------------------------------------------
    def update_maintenance_queue(self, queue_items: List[MaintenancePriorityItem]) -> None:
        """Replaces maintenance priority queue with freshly computed ranked items."""
        with self.db.engine.begin() as conn:
            conn.execute(text("DELETE FROM maintenance_priority_queue"))
            if queue_items:
                stmt = text("""
                    INSERT INTO maintenance_priority_queue (
                        asset_id, ranking, asset_type, criticality, health_state,
                        health_score, failure_probability, predicted_rul_hours,
                        maintenance_priority, priority_score, recommended_action,
                        evaluated_at
                    ) VALUES (
                        :asset_id, :ranking, :asset_type, :criticality, :health_state,
                        :health_score, :failure_probability, :predicted_rul_hours,
                        :maintenance_priority, :priority_score, :recommended_action,
                        :evaluated_at
                    )
                """)
                for item in queue_items:
                    conn.execute(stmt, {
                        "asset_id": item.asset_id,
                        "ranking": item.ranking,
                        "asset_type": item.asset_type,
                        "criticality": item.criticality,
                        "health_state": item.health_state.value if hasattr(item.health_state, "value") else str(item.health_state),
                        "health_score": item.health_score,
                        "failure_probability": item.failure_probability,
                        "predicted_rul_hours": item.predicted_rul_hours,
                        "maintenance_priority": item.maintenance_priority.value if hasattr(item.maintenance_priority, "value") else str(item.maintenance_priority),
                        "priority_score": item.priority_score,
                        "recommended_action": item.recommended_action,
                        "evaluated_at": item.last_evaluated.isoformat() if hasattr(item.last_evaluated, "isoformat") else str(item.last_evaluated),
                    })

    def get_maintenance_queue(self) -> List[MaintenancePriorityItem]:
        """Retrieves the current ranked maintenance priority queue."""
        with self.db.engine.connect() as conn:
            stmt = text("SELECT * FROM maintenance_priority_queue ORDER BY ranking ASC")
            rows = conn.execute(stmt).mappings().fetchall()
            results = []
            for r in rows:
                d = dict(r)
                if isinstance(d.get("evaluated_at"), str):
                    d["last_evaluated"] = datetime.fromisoformat(d["evaluated_at"])
                elif "evaluated_at" in d:
                    d["last_evaluated"] = d["evaluated_at"]
                results.append(MaintenancePriorityItem(**d))
            return results

    # -------------------------------------------------------------------------
    # 6. Record Fleet KPI Snapshot
    # -------------------------------------------------------------------------
    def record_fleet_kpi_snapshot(self, snapshot_id: str, summary: FleetHealthSummary) -> None:
        """Stores a time-stamped fleet health snapshot."""
        with self.db.engine.begin() as conn:
            stmt = text("""
                INSERT INTO fleet_kpi_snapshots (
                    snapshot_id, timestamp, total_assets, healthy_assets,
                    watch_assets, degraded_assets, critical_assets,
                    high_risk_assets, near_failure_assets, fleet_health_score,
                    avg_predicted_rul_hours, failure_risk_rate,
                    active_alerts_total, emergency_maintenance_count,
                    high_maintenance_count
                ) VALUES (
                    :snapshot_id, :timestamp, :total_assets, :healthy_assets,
                    :watch_assets, :degraded_assets, :critical_assets,
                    :high_risk_assets, :near_failure_assets, :fleet_health_score,
                    :avg_predicted_rul_hours, :failure_risk_rate,
                    :active_alerts_total, :emergency_maintenance_count,
                    :high_maintenance_count
                )
            """)
            conn.execute(stmt, {
                "snapshot_id": snapshot_id,
                "timestamp": summary.timestamp.isoformat(),
                "total_assets": summary.total_assets,
                "healthy_assets": summary.healthy_assets,
                "watch_assets": summary.watch_assets,
                "degraded_assets": summary.degraded_assets,
                "critical_assets": summary.critical_assets,
                "high_risk_assets": summary.high_risk_assets,
                "near_failure_assets": summary.near_failure_assets,
                "fleet_health_score": summary.fleet_health_score,
                "avg_predicted_rul_hours": summary.avg_predicted_rul_hours,
                "failure_risk_rate": summary.failure_risk_rate,
                "active_alerts_total": summary.active_alerts_total,
                "emergency_maintenance_count": summary.emergency_maintenance_count,
                "high_maintenance_count": summary.high_maintenance_count,
            })
