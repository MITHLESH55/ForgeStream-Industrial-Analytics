-- ============================================================================
-- FORGESTREAM ANALYTICAL SQL LAYER: Phase 4 Lakehouse Analytics
-- File: 04_cross_asset_analysis.sql
-- Business Purpose: Fleet-wide telemetry percentiles, asset class benchmarking,
--                   and cross-equipment variance comparisons.
-- Target Catalog / Schema: postgres.public (or lakehouse.forgestream)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: Statistical Distribution of Health & Prognostics Across Asset Types
-- Computes min, quartiles/percentiles, avg, and variance by equipment class.
-- ----------------------------------------------------------------------------
SELECT
    asset_type,
    COUNT(*) AS total_units,
    ROUND(AVG(health_score), 4) AS mean_health,
    ROUND(MIN(health_score), 4) AS min_health,
    ROUND(MAX(health_score), 4) AS max_health,
    ROUND(AVG(failure_probability), 4) AS mean_fail_prob,
    ROUND(AVG(predicted_rul_hours), 1) AS mean_rul_hours,
    ROUND(AVG(temperature), 2) AS mean_temp,
    ROUND(AVG(vibration), 3) AS mean_vib,
    ROUND(AVG(power), 2) AS mean_power
FROM postgres.public.asset_current_state
GROUP BY asset_type
ORDER BY mean_health ASC;

-- ----------------------------------------------------------------------------
-- Query 2: Cross-Asset Relative Reliability Index
-- Compares individual asset metrics against its specific asset type cohort average.
-- ----------------------------------------------------------------------------
WITH AssetTypeCohort AS (
    SELECT
        asset_type,
        AVG(health_score) AS cohort_avg_health,
        AVG(failure_probability) AS cohort_avg_fail_prob,
        AVG(predicted_rul_hours) AS cohort_avg_rul
    FROM postgres.public.asset_current_state
    GROUP BY asset_type
)
SELECT
    a.asset_id,
    a.asset_type,
    ROUND(a.health_score, 3) AS asset_health,
    ROUND(c.cohort_avg_health, 3) AS cohort_avg_health,
    ROUND(a.health_score - c.cohort_avg_health, 3) AS health_delta_from_cohort,
    ROUND(a.failure_probability, 3) AS asset_failure_prob,
    ROUND(a.failure_probability - c.cohort_avg_fail_prob, 3) AS fail_prob_delta_from_cohort,
    a.health_state
FROM postgres.public.asset_current_state a
JOIN AssetTypeCohort c ON a.asset_type = c.asset_type
ORDER BY health_delta_from_cohort ASC;

-- ----------------------------------------------------------------------------
-- Query 3: Fleet Telemetry Variance & Extreme Boundary Detection
-- Evaluates dispersion across vibration, temperature, and pressure.
-- ----------------------------------------------------------------------------
SELECT
    asset_type,
    ROUND(VARIANCE(temperature), 2) AS var_temp,
    ROUND(VARIANCE(vibration), 4) AS var_vib,
    ROUND(VARIANCE(pressure), 2) AS var_pressure,
    ROUND(VARIANCE(health_score), 4) AS var_health
FROM postgres.public.asset_current_state
GROUP BY asset_type;
