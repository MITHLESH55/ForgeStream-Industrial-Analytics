-- ============================================================================
-- FORGESTREAM ANALYTICAL SQL LAYER: Phase 4 Lakehouse Analytics
-- File: 05_scenario_analysis.sql
-- Business Purpose: What-if threshold sensitivity analysis, maintenance budget
--                   simulation, and fleet risk mitigation modeling.
-- Target Catalog / Schema: postgres.public (or lakehouse.forgestream)
-- ============================================================================

-- ----------------------------------------------------------------------------
-- Query 1: Sensitivity Analysis of Risk Thresholds on Maintenance Workload
-- Simulates the volume of work orders triggered under conservative (0.35), nominal (0.50),
-- and aggressive (0.75) failure probability thresholds.
-- ----------------------------------------------------------------------------
SELECT
    'Conservative (Risk >= 0.35)' AS scenario_name,
    COUNT(CASE WHEN failure_probability >= 0.35 THEN 1 END) AS triggered_work_orders,
    ROUND(100.0 * COUNT(CASE WHEN failure_probability >= 0.35 THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS fleet_pct_affected,
    ROUND(SUM(CASE WHEN failure_probability >= 0.35 THEN 1500.0 ELSE 0.0 END), 2) AS estimated_cost_usd
FROM postgres.public.asset_current_state
UNION ALL
SELECT
    'Nominal (Risk >= 0.50)' AS scenario_name,
    COUNT(CASE WHEN failure_probability >= 0.50 THEN 1 END) AS triggered_work_orders,
    ROUND(100.0 * COUNT(CASE WHEN failure_probability >= 0.50 THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS fleet_pct_affected,
    ROUND(SUM(CASE WHEN failure_probability >= 0.50 THEN 1500.0 ELSE 0.0 END), 2) AS estimated_cost_usd
FROM postgres.public.asset_current_state
UNION ALL
SELECT
    'Aggressive (Risk >= 0.75)' AS scenario_name,
    COUNT(CASE WHEN failure_probability >= 0.75 THEN 1 END) AS triggered_work_orders,
    ROUND(100.0 * COUNT(CASE WHEN failure_probability >= 0.75 THEN 1 END) / NULLIF(COUNT(*), 0), 2) AS fleet_pct_affected,
    ROUND(SUM(CASE WHEN failure_probability >= 0.75 THEN 1500.0 ELSE 0.0 END), 2) AS estimated_cost_usd
FROM postgres.public.asset_current_state;

-- ----------------------------------------------------------------------------
-- Query 2: What-If Fleet Health Impact of Servicing Top 3 High-Priority Assets
-- Simulates restored fleet health score if top ranked assets are repaired to health_score = 1.0.
-- ----------------------------------------------------------------------------
WITH Top3Urgent AS (
    SELECT asset_id
    FROM postgres.public.maintenance_priority_queue
    ORDER BY ranking ASC
    LIMIT 3
),
SimulatedFleet AS (
    SELECT
        a.asset_id,
        a.health_score AS current_health,
        CASE
            WHEN t.asset_id IS NOT NULL THEN 1.0
            ELSE a.health_score
        END AS simulated_post_maintenance_health
    FROM postgres.public.asset_current_state a
    LEFT JOIN Top3Urgent t ON a.asset_id = t.asset_id
)
SELECT
    ROUND(AVG(current_health), 4) AS baseline_fleet_health,
    ROUND(AVG(simulated_post_maintenance_health), 4) AS projected_fleet_health_after_top3_repairs,
    ROUND(AVG(simulated_post_maintenance_health) - AVG(current_health), 4) AS health_score_improvement
FROM SimulatedFleet;

-- ----------------------------------------------------------------------------
-- Query 3: RUL Buffer Risk Assessment Under Accelerated Degradation (1.5x)
-- Simulates operational survival time if equipment load or temperature accelerates wear by 50%.
-- ----------------------------------------------------------------------------
SELECT
    asset_id,
    asset_type,
    ROUND(predicted_rul_hours, 1) AS nominal_rul_hours,
    ROUND(predicted_rul_hours / 1.5, 1) AS stress_scenario_rul_hours,
    CASE
        WHEN (predicted_rul_hours / 1.5) < 24.0 THEN 'CRITICAL INTERVENTION NEEDED'
        ELSE 'MANAGEABLE BUFFER'
    END AS operational_recommendation
FROM postgres.public.asset_current_state
ORDER BY stress_scenario_rul_hours ASC;
