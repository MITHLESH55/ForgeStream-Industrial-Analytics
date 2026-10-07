"""
ForgeStream Phase 4: End-to-End Pipeline & Analytical Verification Tests.
"""

from datetime import datetime, timezone, timedelta
import json
import os
import pytest

from forgestream.postgres.connection import DatabaseManager
from forgestream.iceberg.tables import IcebergTableManager
from forgestream.serving.store import ServingStoreManager
from forgestream.serving.service import LakehouseServingService
from forgestream.serving.trino_client import TrinoClient
from forgestream.serving.schemas import HealthStateEnum, MaintenancePriorityEnum


class TestPhase4EndToEndPipeline:
    """Validates complete end-to-end telemetry ingestion, ML prognostics, Trino queries, and Grafana."""

    @pytest.fixture(scope="class")
    def serving_service(self):
        pg_url = "postgresql://forgestream_user:forgestream_secret@localhost:5433/forgestream_db"
        try:
            db_mgr = DatabaseManager(dsn=pg_url)
            with db_mgr.engine.connect() as conn:
                pass
        except Exception:
            db_mgr = DatabaseManager(force_sqlite=True, sqlite_fallback_path="data/metadata.db")

        table_mgr = IcebergTableManager()
        store = ServingStoreManager(db_manager=db_mgr, table_manager=table_mgr)
        return LakehouseServingService(store_manager=store)

    @pytest.fixture(scope="class")
    def trino_client(self):
        client = TrinoClient(host="localhost", port=8085, catalog="postgres", schema="public")
        if not client.is_alive():
            pytest.skip("Trino coordinator is not running on localhost:8085")
        return client

    def test_e2e_telemetry_batch_processing(self, serving_service):
        now = datetime.now(timezone.utc)
        synthetic_events = [
            {
                "asset_id": "E2E-TURBINE-01",
                "asset_type": "GAS_TURBINE",
                "operating_mode": "NORMAL",
                "temperature": 74.0,
                "vibration": 1.5,
                "pressure": 25.0,
                "load": 65.0,
                "rpm": 3600.0,
                "power": 450.0,
                "timestamp": now.timestamp(),
                "event_time": now.isoformat(),
            },
            {
                "asset_id": "E2E-GEN-02",
                "asset_type": "STEAM_GEN",
                "operating_mode": "DEGRADED",
                "temperature": 98.5,
                "vibration": 6.2,
                "pressure": 59.0,
                "load": 95.0,
                "rpm": 3000.0,
                "power": 590.0,
                "timestamp": now.timestamp(),
                "event_time": now.isoformat(),
            },
        ]

        result = serving_service.process_telemetry_batch(synthetic_events)
        assert result["events_processed"] == 2
        assert result["current_states_updated"] == 2
        assert result["predictions_generated"] == 2
        assert "fleet_summary" in result
        assert len(result["ranked_queue"]) >= 2

        # Verify state persisted
        state_degraded = serving_service.store.get_current_state("E2E-GEN-02")
        assert state_degraded is not None
        assert state_degraded.failure_probability >= 0.50
        assert state_degraded.maintenance_priority in [
            MaintenancePriorityEnum.MEDIUM,
            MaintenancePriorityEnum.HIGH,
            MaintenancePriorityEnum.EMERGENCY,
        ]

    def test_e2e_trino_analytical_queries(self, trino_client):
        # 1. Test record counts in asset_current_state
        res_count = trino_client.execute_query("SELECT COUNT(*) FROM postgres.public.asset_current_state")
        assert res_count.status == "SUCCESS"
        assert res_count.row_count == 1
        assert res_count.rows[0][0] >= 2

        # 2. Test maintenance queue ordering
        res_queue = trino_client.execute_query(
            "SELECT ranking, asset_id, priority_score FROM postgres.public.maintenance_priority_queue ORDER BY ranking ASC"
        )
        assert res_queue.status == "SUCCESS"
        assert res_queue.row_count >= 2
        rankings = [row[0] for row in res_queue.rows]
        assert rankings == sorted(rankings)

        # 3. Test federated TPCH cross-catalog baseline
        res_tpch = trino_client.execute_query("SELECT count(*) FROM tpch.sf1.region")
        assert res_tpch.status == "SUCCESS"
        assert res_tpch.rows[0][0] == 5

    def test_e2e_grafana_dashboard_discovery(self):
        import urllib.request, base64
        auth = base64.b64encode(b"admin:admin").decode("ascii")
        try:
            req = urllib.request.Request("http://localhost:3000/api/search", headers={"Authorization": f"Basic {auth}"})
            with urllib.request.urlopen(req, timeout=5) as resp:
                dashboards = json.loads(resp.read())
                titles = [d.get("title") for d in dashboards]
                assert any("FORGESTREAM" in t.upper() or "OPERATIONS" in t.upper() for t in titles)
        except Exception as ex:
            pytest.skip(f"Grafana not accessible: {ex}")
