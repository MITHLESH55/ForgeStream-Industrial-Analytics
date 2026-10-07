# ForgeStream Phase 4: Modular Analytical SQL Layer

## Executive Overview
ForgeStream Phase 4 provides 7 specialized analytical SQL modules comprising 21 production queries executed across the Apache Trino MPP distributed query engine. These scripts analyze operational telemetry, prognostic failure trajectories, asset cohorts, hypothetical degradation scenarios, and executive KPIs.

---

## SQL Suite Architecture & Module Breakdown

| Module | File | Queries | Analytical Focus |
|---|---|---|---|
| **01** | `sql/phase4/01_fleet_health.sql` | 3 | Overall fleet health distribution, asset type grouping, degraded asset filtering. |
| **02** | `sql/phase4/02_predictive_maintenance.sql` | 3 | High failure probability thresholding ($P_{fail} \ge 0.50$), RUL horizon stratification, maintenance priority queue retrieval. |
| **03** | `sql/phase4/03_asset_performance.sql` | 3 | Operating regime sensor profiling, active anomaly ranking, $Z$-score statistical outlier detection. |
| **04** | `sql/phase4/04_cross_asset_analysis.sql` | 3 | Asset type cohort benchmarking, relative health delta ranking, inter-asset variance analysis. |
| **05** | `sql/phase4/05_scenario_analysis.sql` | 3 | What-if failure risk threshold sensitivity, emergency shutdown impact modeling, RUL stress testing under severe operating regimes. |
| **06** | `sql/phase4/06_time_based_analysis.sql` | 3 | Temporal RUL decay trajectories across time windows, alert velocity and severity audit, fleet KPI historical trends. |
| **07** | `sql/phase4/07_operational_kpis.sql` | 3 | Executive KPI panel queries, real-time sensor table feeds, prognostic maintenance priority ranking. |

---

## Key Query Implementations

### 1. Multi-Factor RUL Horizon Stratification (`02_predictive_maintenance.sql`)
```sql
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
```

### 2. $Z$-Score Statistical Sensor Outlier Detection (`03_asset_performance.sql`)
```sql
WITH FleetAverages AS (
    SELECT
        AVG(temperature) AS mean_temp,
        STDDEV(temperature) AS std_temp,
        AVG(vibration) AS mean_vib,
        STDDEV(vibration) AS std_vib
    FROM postgres.public.asset_current_state
)
SELECT
    a.asset_id,
    a.asset_type,
    ROUND(a.temperature, 1) AS temperature,
    ROUND((a.temperature - f.mean_temp) / NULLIF(f.std_temp, 0), 2) AS temp_z_score,
    ROUND(a.vibration, 2) AS vibration,
    ROUND((a.vibration - f.mean_vib) / NULLIF(f.std_vib, 0), 2) AS vib_z_score
FROM postgres.public.asset_current_state a
CROSS JOIN FleetAverages f
WHERE ABS((a.temperature - f.mean_temp) / NULLIF(f.std_temp, 0)) > 1.5
   OR ABS((a.vibration - f.mean_vib) / NULLIF(f.std_vib, 0)) > 1.5
ORDER BY temp_z_score DESC;
```

### 3. What-If Operating Regime RUL Stress Testing (`05_scenario_analysis.sql`)
```sql
SELECT
    asset_id,
    asset_type,
    ROUND(predicted_rul_hours, 1) AS nominal_rul_hours,
    ROUND(predicted_rul_hours * 0.70, 1) AS heavy_duty_rul_hours,
    ROUND(predicted_rul_hours * 0.40, 1) AS extreme_stress_rul_hours,
    CASE
        WHEN (predicted_rul_hours * 0.40) < 24.0 THEN 'CRITICAL UNDER STRESS'
        ELSE 'MANAGEABLE'
    END AS stress_status
FROM postgres.public.asset_current_state
WHERE failure_probability >= 0.20
ORDER BY nominal_rul_hours ASC;
```

---

## Verification Results
All 21 queries were verified on Apache Trino coordinator v438 with 100% success rate (`results/phase4_sql_verification.json`). Execution latencies averaged $180\text{ ms} - 320\text{ ms}$ per distributed query.
