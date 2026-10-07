-- ============================================================================
-- FORGESTREAM ANALYTICAL SQL LAYER: Phase 4 Lakehouse Analytics
-- File: 02_predictive_maintenance.sql
-- Business Purpose: Prognostic failure risk analysis, Remaining Useful Life (RUL)
--                   stratification, and prescriptive maintenance work order ranking.
-- Target Catalog / Schema: postgres.public (or lakehouse.forgestream)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: High Failure Risk Assets (Failure Probability >= 0.50)
-- Identifies assets crossing the operational threshold for near-term failure.
-- ----------------------------------------------------------------------------
SELECT
    asset_id,
    asset_type,
    operating_mode,
    ROUND(failure_probability, 4) AS failure_probability,
    ROUND(predicted_rul_hours, 1) AS predicted_rul_hours,
    health_state,
    maintenance_priority,
    model_version,
    last_prediction_time
FROM postgres.public.asset_current_state
WHERE failure_probability >= 0.50
ORDER BY failure_probability DESC, predicted_rul_hours ASC;

-- ----------------------------------------------------------------------------
-- Query 2: Remaining Useful Life (RUL) Horizon Stratification
-- Groups fleet assets into actionable operational maintenance horizons (<24h, 24-72h, >72h).
-- ----------------------------------------------------------------------------
SELECT
    CASE
        WHEN predicted_rul_hours < 24.0 THEN 'IMMEDIATE (< 24h)'
        WHEN predicted_rul_hours BETWEEN 24.0 AND 72.0 THEN 'SHORT_TERM (24-72h)'
        WHEN predicted_rul_hours BETWEEN 72.0 AND 120.0 THEN 'MEDIUM_TERM (72-120h)'
        ELSE 'LONG_TERM (> 120h)'
    END AS rul_horizon,
    COUNT(*) AS asset_count,
    ROUND(AVG(health_score), 4) AS avg_health_score,
    ROUND(AVG(failure_probability), 4) AS avg_failure_probability,
    ROUND(MIN(predicted_rul_hours), 1) AS min_rul_hours,
    ROUND(MAX(predicted_rul_hours), 1) AS max_rul_hours
FROM postgres.public.asset_current_state
GROUP BY
    CASE
        WHEN predicted_rul_hours < 24.0 THEN 'IMMEDIATE (< 24h)'
        WHEN predicted_rul_hours BETWEEN 24.0 AND 72.0 THEN 'SHORT_TERM (24-72h)'
        WHEN predicted_rul_hours BETWEEN 72.0 AND 120.0 THEN 'MEDIUM_TERM (72-120h)'
        ELSE 'LONG_TERM (> 120h)'
    END
ORDER BY min_rul_hours ASC;

-- ----------------------------------------------------------------------------
-- Query 3: Prescriptive Maintenance Priority Queue
-- Retrieves the prioritized ranking of required maintenance interventions.
-- ----------------------------------------------------------------------------
SELECT
    ranking,
    asset_id,
    asset_type,
    criticality,
    health_state,
    maintenance_priority,
    priority_score,
    ROUND(health_score, 3) AS health_score,
    ROUND(failure_probability, 3) AS failure_probability,
    ROUND(predicted_rul_hours, 1) AS predicted_rul_hours,
    recommended_action,
    evaluated_at
FROM postgres.public.maintenance_priority_queue
ORDER BY ranking ASC;
