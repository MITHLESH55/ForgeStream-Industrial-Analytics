"""
Typed Repository Layer for PostgreSQL / SQLite Operational Metadata.
Provides structured data access methods for asset registries, maintenance logs,
ingestion audit runs, data quality events, and quarantine records.
"""

from datetime import datetime, timezone
import json
from typing import Any, Dict, List, Optional
from sqlalchemy import text
from forgestream.postgres.connection import DatabaseManager
from forgestream.observability.logging import get_logger
from forgestream.validation.quarantine import QuarantinedRecord

logger = get_logger("postgres.repository")


class PostgresRepository:
    """Operational metadata repository providing typed persistence operations."""

    def __init__(self, db_manager: Optional[DatabaseManager] = None):
        self.db = db_manager or DatabaseManager()
        # Ensure schema is ready
        self.db.init_schema()

    # -------------------------------------------------------------------------
    # 1. Asset Registry
    # -------------------------------------------------------------------------

    def upsert_asset(
        self,
        asset_id: str,
        asset_type: str,
        model_name: str,
        rated_rpm: float,
        rated_load: float,
        rated_voltage: float,
        rated_current: float,
        baseline_temperature: float,
        baseline_vibration: float,
        baseline_pressure: float,
        criticality: str = "MEDIUM",
    ) -> None:
        """Inserts or updates an industrial asset record."""
        now_str = datetime.now(timezone.utc).isoformat()
        with self.db.engine.begin() as conn:
            # Check existence
            stmt_check = text("SELECT asset_id FROM assets WHERE asset_id = :asset_id")
            exists = conn.execute(stmt_check, {"asset_id": asset_id}).fetchone()

            if exists:
                stmt_update = text(
                    """
                    UPDATE assets SET
                        asset_type = :asset_type,
                        model_name = :model_name,
                        rated_rpm = :rated_rpm,
                        rated_load = :rated_load,
                        rated_voltage = :rated_voltage,
                        rated_current = :rated_current,
                        baseline_temperature = :baseline_temperature,
                        baseline_vibration = :baseline_vibration,
                        baseline_pressure = :baseline_pressure,
                        criticality = :criticality
                    WHERE asset_id = :asset_id
                    """
                )
                conn.execute(
                    stmt_update,
                    {
                        "asset_id": asset_id,
                        "asset_type": asset_type,
                        "model_name": model_name,
                        "rated_rpm": rated_rpm,
                        "rated_load": rated_load,
                        "rated_voltage": rated_voltage,
                        "rated_current": rated_current,
                        "baseline_temperature": baseline_temperature,
                        "baseline_vibration": baseline_vibration,
                        "baseline_pressure": baseline_pressure,
                        "criticality": criticality,
                    },
                )
            else:
                stmt_insert = text(
                    """
                    INSERT INTO assets (
                        asset_id, asset_type, model_name, rated_rpm, rated_load,
                        rated_voltage, rated_current, baseline_temperature,
                        baseline_vibration, baseline_pressure, criticality, created_at
                    ) VALUES (
                        :asset_id, :asset_type, :model_name, :rated_rpm, :rated_load,
                        :rated_voltage, :rated_current, :baseline_temperature,
                        :baseline_vibration, :baseline_pressure, :criticality, :created_at
                    )
                    """
                )
                conn.execute(
                    stmt_insert,
                    {
                        "asset_id": asset_id,
                        "asset_type": asset_type,
                        "model_name": model_name,
                        "rated_rpm": rated_rpm,
                        "rated_load": rated_load,
                        "rated_voltage": rated_voltage,
                        "rated_current": rated_current,
                        "baseline_temperature": baseline_temperature,
                        "baseline_vibration": baseline_vibration,
                        "baseline_pressure": baseline_pressure,
                        "criticality": criticality,
                        "created_at": now_str,
                    },
                )

    def get_asset(self, asset_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves a single asset by its ID."""
        with self.db.engine.connect() as conn:
            stmt = text("SELECT * FROM assets WHERE asset_id = :asset_id")
            row = conn.execute(stmt, {"asset_id": asset_id}).mappings().fetchone()
            return dict(row) if row else None

    def list_assets(self) -> List[Dict[str, Any]]:
        """Retrieves all registered industrial assets."""
        with self.db.engine.connect() as conn:
            stmt = text("SELECT * FROM assets ORDER BY asset_id ASC")
            rows = conn.execute(stmt).mappings().fetchall()
            return [dict(r) for r in rows]

    # -------------------------------------------------------------------------
    # 2. Maintenance History
    # -------------------------------------------------------------------------

    def record_maintenance(
        self,
        maintenance_id: str,
        asset_id: str,
        maintenance_type: str,
        technician_id: str,
        performed_at: datetime,
        scenario_id: Optional[str] = None,
        description: Optional[str] = None,
        cost_usd: float = 0.0,
    ) -> None:
        """Inserts a maintenance work-order record."""
        with self.db.engine.begin() as conn:
            stmt = text(
                """
                INSERT INTO maintenance_history (
                    maintenance_id, asset_id, maintenance_type, scenario_id,
                    description, technician_id, performed_at, cost_usd, created_at
                ) VALUES (
                    :maintenance_id, :asset_id, :maintenance_type, :scenario_id,
                    :description, :technician_id, :performed_at, :cost_usd, :created_at
                )
                """
            )
            conn.execute(
                stmt,
                {
                    "maintenance_id": maintenance_id,
                    "asset_id": asset_id,
                    "maintenance_type": maintenance_type,
                    "scenario_id": scenario_id,
                    "description": description,
                    "technician_id": technician_id,
                    "performed_at": performed_at.isoformat(),
                    "cost_usd": cost_usd,
                    "created_at": datetime.now(timezone.utc).isoformat(),
                },
            )

    def get_asset_maintenance_history(self, asset_id: str) -> List[Dict[str, Any]]:
        """Retrieves all maintenance records for a specified asset."""
        with self.db.engine.connect() as conn:
            stmt = text("SELECT * FROM maintenance_history WHERE asset_id = :asset_id ORDER BY performed_at DESC")
            rows = conn.execute(stmt, {"asset_id": asset_id}).mappings().fetchall()
            return [dict(r) for r in rows]

    # -------------------------------------------------------------------------
    # 3. Ingestion Runs
    # -------------------------------------------------------------------------

    def start_ingestion_run(
        self,
        run_id: str,
        seed: int,
        asset_count: int,
        duration_sec: int,
    ) -> None:
        """Records the start of a telemetry ingestion session."""
        now_str = datetime.now(timezone.utc).isoformat()
        with self.db.engine.begin() as conn:
            stmt = text(
                """
                INSERT INTO ingestion_runs (
                    run_id, seed, asset_count, duration_sec, events_generated,
                    events_persisted_iceberg, events_quarantined, status, started_at
                ) VALUES (
                    :run_id, :seed, :asset_count, :duration_sec, 0,
                    0, 0, 'RUNNING', :started_at
                )
                """
            )
            conn.execute(
                stmt,
                {
                    "run_id": run_id,
                    "seed": seed,
                    "asset_count": asset_count,
                    "duration_sec": duration_sec,
                    "started_at": now_str,
                },
            )

    def complete_ingestion_run(
        self,
        run_id: str,
        events_generated: int,
        events_persisted_iceberg: int,
        events_quarantined: int,
        status: str = "COMPLETED",
    ) -> None:
        """Finalizes an ingestion run with complete metrics."""
        now_str = datetime.now(timezone.utc).isoformat()
        with self.db.engine.begin() as conn:
            stmt = text(
                """
                UPDATE ingestion_runs SET
                    events_generated = :events_generated,
                    events_persisted_iceberg = :events_persisted_iceberg,
                    events_quarantined = :events_quarantined,
                    status = :status,
                    completed_at = :completed_at
                WHERE run_id = :run_id
                """
            )
            conn.execute(
                stmt,
                {
                    "run_id": run_id,
                    "events_generated": events_generated,
                    "events_persisted_iceberg": events_persisted_iceberg,
                    "events_quarantined": events_quarantined,
                    "status": status,
                    "completed_at": now_str,
                },
            )

    def get_ingestion_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves details of an ingestion run."""
        with self.db.engine.connect() as conn:
            stmt = text("SELECT * FROM ingestion_runs WHERE run_id = :run_id")
            row = conn.execute(stmt, {"run_id": run_id}).mappings().fetchone()
            return dict(row) if row else None

    # -------------------------------------------------------------------------
    # 4. Data Quality Events Audit
    # -------------------------------------------------------------------------

    def record_dq_event(
        self,
        rule_code: str,
        severity: str = "ERROR",
        run_id: Optional[str] = None,
        event_id: Optional[str] = None,
        asset_id: Optional[str] = None,
        details: Optional[str] = None,
    ) -> None:
        """Records an individual rule evaluation finding for audit."""
        now_str = datetime.now(timezone.utc).isoformat()
        with self.db.engine.begin() as conn:
            stmt = text(
                """
                INSERT INTO data_quality_events (
                    run_id, event_id, asset_id, rule_code, severity, details, evaluated_at
                ) VALUES (
                    :run_id, :event_id, :asset_id, :rule_code, :severity, :details, :evaluated_at
                )
                """
            )
            conn.execute(
                stmt,
                {
                    "run_id": run_id,
                    "event_id": event_id,
                    "asset_id": asset_id,
                    "rule_code": rule_code,
                    "severity": severity,
                    "details": details,
                    "evaluated_at": now_str,
                },
            )

    def get_dq_events_for_run(self, run_id: str) -> List[Dict[str, Any]]:
        """Retrieves all quality audit events associated with a run."""
        with self.db.engine.connect() as conn:
            stmt = text("SELECT * FROM data_quality_events WHERE run_id = :run_id ORDER BY id ASC")
            rows = conn.execute(stmt, {"run_id": run_id}).mappings().fetchall()
            return [dict(r) for r in rows]

    # -------------------------------------------------------------------------
    # 5. Defective Records Quarantine Store
    # -------------------------------------------------------------------------

    def record_quarantine(
        self,
        quarantine_record: QuarantinedRecord,
        run_id: Optional[str] = None,
    ) -> None:
        """Stores a quarantined defective payload with its validation failure causes."""
        with self.db.engine.begin() as conn:
            stmt = text(
                """
                INSERT INTO quarantine_events (
                    quarantine_id, run_id, event_id, asset_id, violated_rules,
                    reasons, raw_payload, quarantined_at
                ) VALUES (
                    :quarantine_id, :run_id, :event_id, :asset_id, :violated_rules,
                    :reasons, :raw_payload, :quarantined_at
                )
                """
            )
            conn.execute(
                stmt,
                {
                    "quarantine_id": quarantine_record.quarantine_id,
                    "run_id": run_id,
                    "event_id": quarantine_record.event_id,
                    "asset_id": quarantine_record.asset_id,
                    "violated_rules": json.dumps(quarantine_record.violated_rules),
                    "reasons": json.dumps(quarantine_record.reasons),
                    "raw_payload": json.dumps(quarantine_record.raw_payload),
                    "quarantined_at": quarantine_record.quarantined_at.isoformat(),
                },
            )

    def get_quarantined_records(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Retrieves quarantined defective records for investigation."""
        with self.db.engine.connect() as conn:
            stmt = text("SELECT * FROM quarantine_events ORDER BY quarantined_at DESC LIMIT :limit")
            rows = conn.execute(stmt, {"limit": limit}).mappings().fetchall()
            return [dict(r) for r in rows]

    # -------------------------------------------------------------------------
    # 6. Reference Experiment Runs & Benchmark Records
    # -------------------------------------------------------------------------

    def record_experiment(
        self,
        experiment_id: str,
        name: str,
        scenario_id: str,
        seed: int,
        metrics_summary: Dict[str, Any],
    ) -> None:
        """Records a benchmark or experimental test execution with summary metrics."""
        now_str = datetime.now(timezone.utc).isoformat()
        with self.db.engine.begin() as conn:
            stmt = text(
                """
                INSERT INTO experiment_runs (
                    experiment_id, name, scenario_id, seed, metrics_summary, created_at
                ) VALUES (
                    :experiment_id, :name, :scenario_id, :seed, :metrics_summary, :created_at
                )
                """
            )
            conn.execute(
                stmt,
                {
                    "experiment_id": experiment_id,
                    "name": name,
                    "scenario_id": scenario_id,
                    "seed": seed,
                    "metrics_summary": json.dumps(metrics_summary),
                    "created_at": now_str,
                },
            )

    def get_experiment(self, experiment_id: str) -> Optional[Dict[str, Any]]:
        """Retrieves experiment run details."""
        with self.db.engine.connect() as conn:
            stmt = text("SELECT * FROM experiment_runs WHERE experiment_id = :experiment_id")
            row = conn.execute(stmt, {"experiment_id": experiment_id}).mappings().fetchone()
            if row:
                res = dict(row)
                res["metrics_summary"] = json.loads(res["metrics_summary"])
                return res
            return None
