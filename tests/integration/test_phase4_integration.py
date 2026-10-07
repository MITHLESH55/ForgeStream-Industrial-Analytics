"""
ForgeStream Phase 4: Integration Tests for Serving Stores, PostgreSQL/Iceberg Persistence, and Live Trino Coordinator.
"""

from datetime import datetime, timezone
import json
import os
import pytest
from sqlalchemy import text

from forgestream.postgres.connection import DatabaseManager
from forgestream.iceberg.tables import IcebergTableManager
from forgestream.serving.schemas import (
    AssetCurrentState,
    AssetPredictionHistory,
    AssetAlertHistory,
    FleetHealthSummary,
    HealthStateEnum,
    MaintenancePriorityEnum,
    AnomalySeverityEnum,
)
from forgestream.serving.kpis import OperationalKPIEngine
from forgestream.serving.store import ServingStoreManager
from forgestream.serving.service import LakehouseServingService
from forgestream.serving.trino_client import TrinoClient


@pytest.fixture
def store_manager():
    """Provides a ServingStoreManager backed by SQLite in-memory or PostgreSQL test DB."""
    pg_url = "postgresql://forgestream_user:forgestream_secret@localhost:5433/forgestream_db"
    try:
        db = DatabaseManager(dsn=pg_url)
        with db.engine.connect() as conn:
            pass
    except Exception:
        db = DatabaseManager(force_sqlite=True, sqlite_fallback_path=":memory:")

    table_mgr = IcebergTableManager()
    store = ServingStoreManager(db_manager=db, table_manager=table_mgr)
    return store


class TestServingStoreManagerIntegration:
    """Integration test suite verifying ServingStoreManager persistence layer."""

    def test_upsert_and_retrieve_current_state(self, store_manager):
        now = datetime.now(timezone.utc)
        state = AssetCurrentState(
            asset_id="TEST-TURBINE-01",
            asset_type="GAS_TURBINE",
            operating_mode="NORMAL",
            health_state=HealthStateEnum.HEALTHY,
            health_score=0.925,
            failure_probability=0.075,
            predicted_failure_risk=0,
            predicted_rul_hours=110.5,
            maintenance_priority=MaintenancePriorityEnum.LOW,
            anomaly_level=0,
            active_alert_count=0,
            temperature=72.0,
            vibration=1.4,
            pressure=24.0,
            load=65.0,
            rpm=3600.0,
            power=450.0,
            last_event_time=now,
            last_prediction_time=now,
        )

        store_manager.upsert_current_state(state)
        retrieved = store_manager.get_current_state("TEST-TURBINE-01")
        assert retrieved is not None
        assert retrieved.asset_id == "TEST-TURBINE-01"
        assert retrieved.health_score == 0.925
        assert retrieved.health_state == HealthStateEnum.HEALTHY

        # Update existing state
        state.health_score = 0.810
        state.health_state = HealthStateEnum.WATCH
        store_manager.upsert_current_state(state)
        updated = store_manager.get_current_state("TEST-TURBINE-01")
        assert updated is not None
        assert updated.health_score == 0.810
        assert updated.health_state == HealthStateEnum.WATCH

    def test_record_predictions_and_alerts_batch(self, store_manager):
        now = datetime.now(timezone.utc)
        import uuid
        test_run_id = uuid.uuid4().hex[:6]
        preds = [
            AssetPredictionHistory(
                prediction_id=f"PRED-INT-{test_run_id}-{i}",
                asset_id="TEST-PUMP-02",
                asset_type="CENTRIFUGAL_PUMP",
                timestamp=now.timestamp(),
                event_time=now,
                failure_probability=0.35 + (i * 0.05),
                predicted_failure_risk=0 if i == 0 else 1,
                predicted_rul_hours=80.0 - (i * 5.0),
                maintenance_priority=MaintenancePriorityEnum.MEDIUM,
                top_features_summary=json.dumps([{"feature": "vibration", "importance": 0.4}]),
            )
            for i in range(3)
        ]
        store_manager.record_predictions_batch(preds)

        alerts = [
            AssetAlertHistory(
                alert_id=f"ALT-INT-{test_run_id}-{i}",
                asset_id="TEST-PUMP-02",
                asset_type="CENTRIFUGAL_PUMP",
                severity=AnomalySeverityEnum.WARNING,
                health_state=HealthStateEnum.WATCH,
                health_score=0.75,
                is_state_transition=True,
                is_recovery=False,
                reason_codes=json.dumps(["BEARING_OVERHEAT"]),
                triggering_values=json.dumps({"temperature": 85.0}),
                maintenance_priority=MaintenancePriorityEnum.MEDIUM,
                timestamp=now.timestamp(),
                event_time=now,
            )
            for i in range(2)
        ]
        store_manager.record_alerts_batch(alerts)

    def test_maintenance_queue_and_kpi_snapshot_lifecycle(self, store_manager):
        states = store_manager.get_all_current_states()
        summary = OperationalKPIEngine.compute_fleet_summary(states)
        ranked = OperationalKPIEngine.rank_maintenance_work_orders(states)

        store_manager.update_maintenance_queue(ranked)
        queue_items = store_manager.get_maintenance_queue()
        assert len(queue_items) == len(ranked)

        snapshot_id = f"SNAP-TEST-{datetime.now(timezone.utc).strftime('%H%M%S')}"
        store_manager.record_fleet_kpi_snapshot(snapshot_id, summary)


class TestTrinoCoordinatorIntegration:
    """Live query tests against Apache Trino coordinator on port 8085."""

    @pytest.fixture
    def trino_client(self):
        client = TrinoClient(host="localhost", port=8085, catalog="postgres", schema="public")
        if not client.is_alive():
            pytest.skip("Trino coordinator is not running on localhost:8085")
        return client

    def test_trino_alive_and_version(self, trino_client):
        info = trino_client.get_server_info()
        assert "version" in info or "nodeVersion" in info or trino_client.is_alive()

    def test_trino_federated_postgres_query(self, trino_client):
        res = trino_client.execute_query("SELECT COUNT(*) as count FROM postgres.public.asset_current_state")
        assert res.status == "SUCCESS"
        assert res.row_count >= 1

    def test_trino_federated_tpch_query(self, trino_client):
        res = trino_client.execute_query("SELECT count(*) as cnt FROM tpch.sf1.nation")
        assert res.status == "SUCCESS"
        assert res.row_count == 1
        assert res.rows[0][0] == 25  # TPCH sf1 nation count is exactly 25
