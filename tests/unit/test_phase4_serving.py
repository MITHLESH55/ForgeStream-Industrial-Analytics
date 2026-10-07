"""
ForgeStream Phase 4: Unit Tests for Serving Layer, Operational KPIs, Schemas, and Analytical SQL.
"""

from datetime import datetime, timezone
import json
import os
import pytest
from pydantic import ValidationError

from forgestream.serving.schemas import (
    AssetCurrentState,
    AssetPredictionHistory,
    AssetAlertHistory,
    AssetTelemetrySummary,
    FleetHealthSummary,
    MaintenancePriorityItem,
    HealthStateEnum,
    MaintenancePriorityEnum,
    AnomalySeverityEnum,
)
from forgestream.serving.kpis import OperationalKPIEngine
from forgestream.serving.trino_client import TrinoClient, TrinoQueryResult


class TestServingSchemas:
    """Validates Phase 4 serving schemas, data integrity, and serialization contracts."""

    def test_asset_current_state_validation(self):
        now = datetime.now(timezone.utc)
        state = AssetCurrentState(
            asset_id="TURBINE-001",
            asset_type="GAS_TURBINE",
            operating_mode="NORMAL",
            health_state=HealthStateEnum.HEALTHY,
            health_score=0.952,
            failure_probability=0.048,
            predicted_failure_risk=0,
            predicted_rul_hours=114.2,
            maintenance_priority=MaintenancePriorityEnum.LOW,
            anomaly_level=0,
            active_alert_count=0,
            temperature=72.4,
            vibration=1.45,
            pressure=24.2,
            load=65.0,
            rpm=3600.0,
            power=450.0,
            last_event_time=now,
            last_prediction_time=now,
            model_version="v3.0.0-champion",
        )
        assert state.asset_id == "TURBINE-001"
        assert state.health_score == 0.952
        assert state.health_state == HealthStateEnum.HEALTHY
        dumped = state.model_dump(mode="json")
        assert dumped["asset_type"] == "GAS_TURBINE"
        assert dumped["maintenance_priority"] == "LOW"

    def test_asset_current_state_bounds_validation(self):
        now = datetime.now(timezone.utc)
        with pytest.raises(ValidationError):
            AssetCurrentState(
                asset_id="TURBINE-001",
                asset_type="GAS_TURBINE",
                operating_mode="NORMAL",
                health_state=HealthStateEnum.HEALTHY,
                health_score=1.5,  # Invalid: > 1.0
                failure_probability=0.05,
                predicted_failure_risk=0,
                predicted_rul_hours=100.0,
                maintenance_priority=MaintenancePriorityEnum.LOW,
                temperature=70.0,
                vibration=1.0,
                pressure=20.0,
                load=50.0,
                rpm=3000.0,
                power=400.0,
                last_event_time=now,
                last_prediction_time=now,
            )

    def test_asset_alert_history_schema(self):
        now = datetime.now(timezone.utc)
        alert = AssetAlertHistory(
            alert_id="ALT-1001",
            asset_id="PUMP-002",
            asset_type="CENTRIFUGAL_PUMP",
            severity=AnomalySeverityEnum.WARNING,
            health_state=HealthStateEnum.WATCH,
            health_score=0.82,
            is_state_transition=True,
            is_recovery=False,
            reason_codes=json.dumps(["VIBRATION_SPIKE"]),
            triggering_values=json.dumps({"vibration": 3.82}),
            maintenance_priority=MaintenancePriorityEnum.MEDIUM,
            timestamp=now.timestamp(),
            event_time=now,
        )
        assert alert.alert_id == "ALT-1001"
        assert alert.is_state_transition is True
        assert alert.severity == AnomalySeverityEnum.WARNING

    def test_fleet_health_summary_defaults(self):
        summary = FleetHealthSummary(
            total_assets=5,
            healthy_assets=3,
            watch_assets=1,
            degraded_assets=1,
            critical_assets=0,
            high_risk_assets=1,
            near_failure_assets=0,
            fleet_health_score=0.8528,
            avg_predicted_rul_hours=102.3,
            min_predicted_rul_hours=66.2,
            failure_risk_rate=0.20,
            active_alerts_total=12,
            emergency_maintenance_count=0,
            high_maintenance_count=1,
        )
        assert summary.total_assets == 5
        assert summary.healthy_assets == 3
        assert summary.min_predicted_rul_hours == 66.2


class TestOperationalKPIEngine:
    """Validates mathematical correctness of operational KPI formulations."""

    def _create_mock_states(self):
        now = datetime.now(timezone.utc)
        s1 = AssetCurrentState(
            asset_id="A1", asset_type="MOTOR", operating_mode="NORMAL",
            health_state=HealthStateEnum.HEALTHY, health_score=0.95,
            failure_probability=0.05, predicted_failure_risk=0, predicted_rul_hours=115.0,
            maintenance_priority=MaintenancePriorityEnum.LOW, temperature=65.0,
            vibration=1.2, pressure=4.0, load=70.0, rpm=1800.0, power=20.0,
            last_event_time=now, last_prediction_time=now,
        )
        s2 = AssetCurrentState(
            asset_id="A2", asset_type="PUMP", operating_mode="NORMAL",
            health_state=HealthStateEnum.WATCH, health_score=0.80,
            failure_probability=0.20, predicted_failure_risk=0, predicted_rul_hours=90.0,
            maintenance_priority=MaintenancePriorityEnum.MEDIUM, temperature=78.0,
            vibration=3.2, pressure=6.5, load=85.0, rpm=1750.0, power=45.0,
            last_event_time=now, last_prediction_time=now,
        )
        s3 = AssetCurrentState(
            asset_id="A3", asset_type="GENERATOR", operating_mode="DEGRADED",
            health_state=HealthStateEnum.DEGRADED, health_score=0.45,
            failure_probability=0.65, predicted_failure_risk=1, predicted_rul_hours=20.0,
            maintenance_priority=MaintenancePriorityEnum.HIGH, temperature=95.0,
            vibration=5.5, pressure=8.0, load=92.0, rpm=3000.0, power=500.0,
            last_event_time=now, last_prediction_time=now,
        )
        s4 = AssetCurrentState(
            asset_id="A4", asset_type="COMPRESSOR", operating_mode="CRITICAL",
            health_state=HealthStateEnum.CRITICAL, health_score=0.15,
            failure_probability=0.90, predicted_failure_risk=1, predicted_rul_hours=8.0,
            maintenance_priority=MaintenancePriorityEnum.EMERGENCY, temperature=108.0,
            vibration=7.8, pressure=12.0, load=98.0, rpm=1200.0, power=350.0,
            last_event_time=now, last_prediction_time=now,
        )
        return [s1, s2, s3, s4]

    def test_compute_fleet_summary_metrics(self):
        states = self._create_mock_states()
        summary = OperationalKPIEngine.compute_fleet_summary(states, active_alert_count=5)

        assert summary.total_assets == 4
        assert summary.healthy_assets == 1
        assert summary.watch_assets == 1
        assert summary.degraded_assets == 1
        assert summary.critical_assets == 1
        # High risk: s3 (0.65) and s4 (0.90) -> 2
        assert summary.high_risk_assets == 2
        # Near failure (<24h): s3 (20.0h) and s4 (8.0h) -> 2
        assert summary.near_failure_assets == 2
        # Fleet health score = (0.95 + 0.80 + 0.45 + 0.15) / 4 = 0.5875
        assert abs(summary.fleet_health_score - 0.5875) < 1e-4
        # Avg RUL = (115 + 90 + 20 + 8) / 4 = 58.25
        assert abs(summary.avg_predicted_rul_hours - 58.25) < 1e-2
        # Min RUL = 8.0
        assert summary.min_predicted_rul_hours == 8.0
        # Failure risk rate = 2 / 4 = 0.50
        assert summary.failure_risk_rate == 0.50

    def test_maintenance_priority_scoring_ordering(self):
        states = self._create_mock_states()
        criticalities = {"A1": "LOW", "A2": "MEDIUM", "A3": "HIGH", "A4": "CRITICAL"}
        ranked = OperationalKPIEngine.rank_maintenance_work_orders(states, criticalities)

        assert len(ranked) == 4
        assert ranked[0].asset_id == "A4"
        assert ranked[0].ranking == 1
        assert ranked[0].maintenance_priority == MaintenancePriorityEnum.EMERGENCY

        assert ranked[1].asset_id == "A3"
        assert ranked[1].ranking == 2
        assert ranked[1].maintenance_priority == MaintenancePriorityEnum.HIGH

        # Monotonically decreasing priority score
        for i in range(len(ranked) - 1):
            assert ranked[i].priority_score >= ranked[i + 1].priority_score


class TestTrinoClientUnit:
    """Unit tests for Trino query client execution wrappers and parsing."""

    def test_trino_query_result_formatting(self):
        res = TrinoQueryResult(
            query="SELECT 1",
            columns=["col1", "col2"],
            rows=[[10, "data"]],
            execution_time_ms=12.5,
            status="SUCCESS",
        )
        assert res.row_count == 1
        assert res.status == "SUCCESS"
        d = res.to_dict()
        assert d["row_count"] == 1
        assert d["column_count"] == 2

    def test_trino_client_initialization(self):
        client = TrinoClient(host="localhost", port=8085, catalog="postgres", schema="public")
        assert client.base_url == "http://localhost:8085"
        assert client.catalog == "postgres"
        assert client.schema == "public"


class TestAnalyticalSQLScripts:
    """Verifies existence, non-emptiness, and structural validity of the 7 Phase 4 SQL scripts."""

    def test_sql_files_exist(self):
        sql_dir = os.path.join(os.path.dirname(__file__), "..", "..", "sql", "phase4")
        expected_files = [
            "01_fleet_health.sql",
            "02_predictive_maintenance.sql",
            "03_asset_performance.sql",
            "04_cross_asset_analysis.sql",
            "05_scenario_analysis.sql",
            "06_time_based_analysis.sql",
            "07_operational_kpis.sql",
        ]
        for f in expected_files:
            p = os.path.join(sql_dir, f)
            assert os.path.exists(p), f"Missing expected SQL file: {f}"
            with open(p, "r", encoding="utf-8") as fh:
                content = fh.read()
            assert len(content) > 100, f"SQL file {f} is unexpectedly empty"
            assert "SELECT" in content.upper(), f"SQL file {f} contains no SELECT statements"
