-- ============================================================================
-- FORGESTREAM ANALYTICAL SQL LAYER: Phase 4 Lakehouse Analytics
-- File: 03_asset_performance.sql
-- Business Purpose: Operational performance benchmarks, multi-sensor variance
--                   analysis, and anomaly level correlation across asset classes.
-- Target Catalog / Schema: postgres.public (or lakehouse.forgestream)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: Operating Mode vs Sensor Loading & Degradation Correlation
-- Analyzes the relationship between heavy duty cycles and physical sensor extremes.
-- ----------------------------------------------------------------------------
SELECT
    operating_mode,
    COUNT(*) AS total_samples,
    ROUND(AVG(load), 2) AS avg_load_pct,
    ROUND(AVG(temperature), 2) AS avg_temp_c,
    ROUND(AVG(vibration), 3) AS avg_vibration_mms,
    ROUND(AVG(pressure), 2) AS avg_pressure_bar,
    ROUND(AVG(rpm), 1) AS avg_rpm,
    ROUND(AVG(power), 2) AS avg_power_kw,
    ROUND(AVG(health_score), 3) AS avg_health_score
FROM postgres.public.asset_current_state
GROUP BY operating_mode
ORDER BY avg_load_pct DESC;

-- ----------------------------------------------------------------------------
-- Query 2: Anomalous Assets Filtered by Alert Severity and Anomaly Level
-- Flags equipment with multiple active alerts or elevated anomaly levels.
-- ----------------------------------------------------------------------------
SELECT
    asset_id,
    asset_type,
    operating_mode,
    anomaly_level,
    active_alert_count,
    health_state,
    ROUND(health_score, 3) AS health_score,
    ROUND(temperature, 1) AS temperature,
    ROUND(vibration, 2) AS vibration,
    ROUND(pressure, 1) AS pressure
FROM postgres.public.asset_current_state
WHERE anomaly_level > 0 OR active_alert_count > 0 OR health_state != 'HEALTHY'
ORDER BY anomaly_level DESC, active_alert_count DESC, health_score ASC;

-- ----------------------------------------------------------------------------
-- Query 3: Multi-Sensor Operational Outliers vs Fleet Mean
-- Identifies assets whose sensor metrics deviate significantly from fleet averages.
-- ----------------------------------------------------------------------------
WITH FleetAverages AS (
    SELECT
        AVG(temperature) AS mean_temp,
        STDDEV(temperature) AS std_temp,
        AVG(vibration) AS mean_vib,
        STDDEV(vibration) AS std_vib,
        AVG(pressure) AS mean_press,
        STDDEV(pressure) AS std_press
    FROM postgres.public.asset_current_state
)
SELECT
    a.asset_id,
    a.asset_type,
    ROUND(a.temperature, 1) AS temp_c,
    ROUND(f.mean_temp, 1) AS fleet_avg_temp,
    ROUND((a.temperature - f.mean_temp) / NULLIF(f.std_temp, 0), 2) AS temp_zscore,
    ROUND(a.vibration, 2) AS vib_mms,
    ROUND(f.mean_vib, 2) AS fleet_avg_vib,
    ROUND((a.vibration - f.mean_vib) / NULLIF(f.std_vib, 0), 2) AS vib_zscore
FROM postgres.public.asset_current_state a
CROSS JOIN FleetAverages f
WHERE ABS(a.temperature - f.mean_temp) > 1.5 * COALESCE(NULLIF(f.std_temp, 0), 1.0)
   OR ABS(a.vibration - f.mean_vib) > 1.5 * COALESCE(NULLIF(f.std_vib, 0), 1.0)
ORDER BY ABS(a.temperature - f.mean_temp) DESC;
