-- ============================================================================
-- FORGESTREAM ANALYTICAL SQL LAYER: Phase 4 Lakehouse Analytics
-- File: 06_time_based_analysis.sql
-- Business Purpose: Temporal degradation trajectories, prognostic drift over time,
--                   and streaming alert velocity audit.
-- Target Catalog / Schema: postgres.public (or lakehouse.forgestream)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: Temporal Degradation & RUL Decay Trajectory
-- Tracks historical progression of failure probability and RUL decay across time windows.
-- ----------------------------------------------------------------------------
SELECT
    asset_id,
    date_trunc('minute', event_time) AS time_window,
    ROUND(AVG(failure_probability), 4) AS avg_failure_probability,
    ROUND(AVG(predicted_rul_hours), 1) AS avg_rul_hours,
    COUNT(*) AS prediction_events
FROM postgres.public.asset_prediction_history
GROUP BY asset_id, date_trunc('minute', event_time)
ORDER BY time_window ASC, asset_id ASC;

-- ----------------------------------------------------------------------------
-- Query 2: Streaming Alert Velocity and Severity Audit
-- Measures the arrival rate of critical vs warning alerts across time windows.
-- ----------------------------------------------------------------------------
SELECT
    severity,
    COUNT(*) AS total_alerts,
    COUNT(DISTINCT asset_id) AS distinct_assets_affected,
    MIN(event_time) AS first_alert_time,
    MAX(event_time) AS latest_alert_time
FROM postgres.public.asset_alert_history
GROUP BY severity
ORDER BY total_alerts DESC;

-- ----------------------------------------------------------------------------
-- Query 3: Fleet KPI Snapshot Temporal Trend
-- Analyzes the progression of executive fleet KPIs across historic snapshot evaluations.
-- ----------------------------------------------------------------------------
SELECT
    timestamp,
    total_assets,
    healthy_assets,
    critical_assets,
    high_risk_assets,
    ROUND(fleet_health_score, 4) AS fleet_health_score,
    ROUND(avg_predicted_rul_hours, 1) AS avg_predicted_rul_hours,
    active_alerts_total
FROM postgres.public.fleet_kpi_snapshots
ORDER BY timestamp ASC;
