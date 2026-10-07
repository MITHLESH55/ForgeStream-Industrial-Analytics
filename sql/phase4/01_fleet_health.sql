-- ============================================================================
-- FORGESTREAM ANALYTICAL SQL LAYER: Phase 4 Lakehouse Analytics
-- File: 01_fleet_health.sql
-- Business Purpose: Fleet-wide health score aggregation, operational state
--                   distributions, and immediate identification of critical assets.
-- Target Catalog / Schema: postgres.public (or lakehouse.forgestream)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: Executive Fleet Health Summary & State Counts
-- Computes the overall average health score and counts across all 4 discrete health states.
-- ----------------------------------------------------------------------------
SELECT
    COUNT(*) AS total_assets,
    ROUND(AVG(health_score), 4) AS fleet_avg_health_score,
    COUNT(CASE WHEN health_state = 'HEALTHY' THEN 1 END) AS healthy_count,
    COUNT(CASE WHEN health_state = 'WATCH' THEN 1 END) AS watch_count,
    COUNT(CASE WHEN health_state = 'DEGRADED' THEN 1 END) AS degraded_count,
    COUNT(CASE WHEN health_state = 'CRITICAL' THEN 1 END) AS critical_count,
    ROUND(100.0 * COUNT(CASE WHEN health_state = 'HEALTHY' THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS healthy_percentage
FROM postgres.public.asset_current_state;

-- ----------------------------------------------------------------------------
-- Query 2: Asset Health Distribution by Asset Type & Operating Mode
-- Identifies if specific equipment classes or operating regimes suffer disproportionate degradation.
-- ----------------------------------------------------------------------------
SELECT
    asset_type,
    operating_mode,
    COUNT(*) AS asset_count,
    ROUND(AVG(health_score), 4) AS avg_health_score,
    ROUND(MIN(health_score), 4) AS min_health_score,
    ROUND(MAX(health_score), 4) AS max_health_score,
    COUNT(CASE WHEN health_state IN ('DEGRADED', 'CRITICAL') THEN 1 END) AS at_risk_count
FROM postgres.public.asset_current_state
GROUP BY asset_type, operating_mode
ORDER BY avg_health_score ASC, asset_count DESC;

-- ----------------------------------------------------------------------------
-- Query 3: Critical and Watch Asset Detail Drilldown
-- Surfaces detailed operational metrics for assets requiring engineering attention.
-- ----------------------------------------------------------------------------
SELECT
    asset_id,
    asset_type,
    operating_mode,
    health_state,
    ROUND(health_score, 3) AS health_score,
    ROUND(failure_probability, 3) AS failure_probability,
    ROUND(predicted_rul_hours, 1) AS predicted_rul_hours,
    maintenance_priority,
    ROUND(temperature, 1) AS temp_c,
    ROUND(vibration, 2) AS vibration_mms,
    ROUND(pressure, 1) AS pressure_bar,
    last_event_time
FROM postgres.public.asset_current_state
WHERE health_state IN ('WATCH', 'DEGRADED', 'CRITICAL')
ORDER BY health_score ASC, failure_probability DESC;
