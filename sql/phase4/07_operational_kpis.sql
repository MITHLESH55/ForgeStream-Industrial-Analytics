-- ============================================================================
-- FORGESTREAM ANALYTICAL SQL LAYER: Phase 4 Lakehouse Analytics
-- File: 07_operational_kpis.sql
-- Business Purpose: Executive Operational KPI queries driving the Grafana
--                   Operations Center dashboard and automated executive reporting.
-- Target Catalog / Schema: postgres.public (or lakehouse.forgestream)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: Executive KPI Panel 1-6 Queries (Row 1 Summary)
-- Direct queries matching Grafana single-stat panels.
-- ----------------------------------------------------------------------------
SELECT
    total_assets,
    healthy_assets,
    watch_assets,
    degraded_assets,
    critical_assets,
    high_risk_assets,
    near_failure_assets,
    ROUND(fleet_health_score, 4) AS fleet_health_score,
    ROUND(avg_predicted_rul_hours, 1) AS avg_predicted_rul_hours,
    ROUND(failure_risk_rate, 4) AS failure_risk_rate,
    active_alerts_total,
    emergency_maintenance_count,
    high_maintenance_count,
    timestamp AS snapshot_time
FROM postgres.public.fleet_kpi_snapshots
ORDER BY timestamp DESC
LIMIT 1;

-- ----------------------------------------------------------------------------
-- Query 2: Asset Health Score Ranking and Sensor Registry (Row 2 Table Panel)
-- Feeds the primary asset operational table with real-time sensor and health stats.
-- ----------------------------------------------------------------------------
SELECT
    asset_id,
    asset_type,
    operating_mode,
    health_state,
    ROUND(health_score, 3) AS health_score,
    ROUND(temperature, 1) AS temperature,
    ROUND(vibration, 2) AS vibration,
    ROUND(pressure, 1) AS pressure,
    ROUND(load, 1) AS load,
    ROUND(rpm, 0) AS rpm
FROM postgres.public.asset_current_state
ORDER BY health_score ASC;

-- ----------------------------------------------------------------------------
-- Query 3: Prognostics Ranking & Maintenance Priorities (Row 3 Panels)
-- Feeds the failure probability ranking and prescriptive priority work orders.
-- ----------------------------------------------------------------------------
SELECT
    m.ranking,
    m.asset_id,
    m.maintenance_priority,
    m.priority_score,
    ROUND(m.health_score, 3) AS health_score,
    ROUND(m.failure_probability, 3) AS failure_probability,
    ROUND(m.predicted_rul_hours, 1) AS predicted_rul_hours,
    m.recommended_action
FROM postgres.public.maintenance_priority_queue m
ORDER BY m.ranking ASC;
