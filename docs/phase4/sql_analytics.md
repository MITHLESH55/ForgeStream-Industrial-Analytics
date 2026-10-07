# ForgeStream Phase 4: Analytical SQL Layer Specification

## 1. Executive Summary

The ForgeStream Analytical SQL Layer (`sql/phase4/`) provides 21 executable, modular analytical queries organized across 7 functional files. These queries enable plant managers, reliability engineers, and data scientists to derive operational insights directly via Apache Trino.

```
sql/phase4/
├── 01_fleet_health.sql              <--- Fleet-wide health aggregations & distributions
├── 02_predictive_maintenance.sql     <--- Failure risk stratification & RUL ranking
├── 03_asset_performance.sql         <--- Multi-sensor z-scores & anomaly audits
├── 04_cross_asset_analysis.sql      <--- Cohort reliability & variance benchmarks
├── 05_scenario_analysis.sql         <--- What-if maintenance & stress simulations
├── 06_time_based_analysis.sql       <--- Temporal RUL decay & alert velocity
└── 07_operational_kpis.sql          <--- Executive KPI snapshot extraction
```

---

## 2. File-by-File Query Breakdown

### 2.1 `01_fleet_health.sql` — Fleet Health & State Distribution
- **Query 1 (Executive Summary)**: Aggregates total asset count, fleet mean health score, discrete counts across health states (`HEALTHY`, `WATCH`, `DEGRADED`, `CRITICAL`), and healthy asset percentage.
- **Query 2 (Asset Type & Mode Breakdown)**: Groups assets by equipment category and operating mode, revealing health score range and at-risk unit counts.
- **Query 3 (Drilldown on At-Risk Assets)**: Filters assets with non-healthy states, detailing calibrated temperature, vibration, pressure, and latest event timestamps.

### 2.2 `02_predictive_maintenance.sql` — Predictive Maintenance & Prognostics
- **Query 1 (High Failure Risk Assets)**: Identifies assets with $P_{fail} \ge 0.50$, ordered by failure probability descending and RUL ascending.
- **Query 2 (RUL Horizon Stratification)**: Stratifies assets into operational maintenance windows: `IMMEDIATE (< 24h)`, `SHORT_TERM (24-72h)`, `MEDIUM_TERM (72-120h)`, and `LONG_TERM (> 120h)`.
- **Query 3 (Prescriptive Maintenance Queue)**: Queries the fully ranked work order queue (`maintenance_priority_queue`) with prescriptive action text and urgency scores.

### 2.3 `03_asset_performance.sql` — Operational Performance & Anomaly Detection
- **Query 1 (Operating Mode vs Sensor Loading)**: Correlates heavy duty cycles (`HEAVY`, `DEGRADED`) with physical sensor readings (power, temperature, vibration, pressure).
- **Query 2 (Anomalous Assets by Alert Severity)**: Flags equipment exhibiting active anomalies (Level 1-3) or unacknowledged alerts.
- **Query 3 (Multi-Sensor Outliers vs Fleet Mean)**: Computes statistical $Z$-scores for temperature and vibration relative to fleet averages, surfacing units exceeding $1.5\sigma$.

### 2.4 `04_cross_asset_analysis.sql` — Cross-Asset Cohort Benchmarking
- **Query 1 (Statistical Health & Prognostics Distribution)**: Measures mean, min, max health score, failure probability, and mean RUL per equipment class.
- **Query 2 (Cohort Relative Reliability Index)**: Computes the delta between an individual asset's health score and its equipment class cohort average ($\Delta H_{cohort} = H_{asset} - \bar{H}_{cohort}$).
- **Query 3 (Fleet Telemetry Variance)**: Calculates statistical variance ($\sigma^2$) across temperature, vibration, and pressure to detect erratic behavior.

### 2.5 `05_scenario_analysis.sql` — What-If Simulations & Stress Testing
- **Query 1 (Sensitivity Analysis of Risk Thresholds)**: Simulates work order volumes and estimated repair costs under Conservative ($P_{fail} \ge 0.35$), Nominal ($P_{fail} \ge 0.50$), and Aggressive ($P_{fail} \ge 0.75$) threshold policies.
- **Query 2 (What-If Maintenance Impact Simulation)**: Simulates the fleet-wide health improvement if the top 3 urgent assets are immediately overhauled to $H = 1.0$.
- **Query 3 (Accelerated Degradation Stress Test)**: Simulates operational survival time under 1.5x accelerated wear conditions.

### 2.6 `06_time_based_analysis.sql` — Temporal Progression & Alert Velocity
- **Query 1 (Temporal RUL Decay Trajectory)**: Tracks historical progression of failure probability and RUL decay across time-truncated windows (`date_trunc('minute', event_time)`).
- **Query 2 (Streaming Alert Velocity Audit)**: Measures the arrival rate, volume, and affected asset count grouped by alert severity (`WARNING`, `HIGH`, `CRITICAL`).
- **Query 3 (Fleet KPI Snapshot Trend)**: Queries time-series snapshot records to track fleet health evolution over multiple evaluation cycles.

### 2.7 `07_operational_kpis.sql` — Executive KPI Extraction
- **Query 1 (Single-Stat KPI Summary)**: Extracts the latest fleet KPI snapshot to populate Row 1 Grafana stat cards.
- **Query 2 (Asset Health & Sensor Registry)**: Powers the primary asset overview table with real-time health and multi-sensor metrics.
- **Query 3 (Maintenance Urgency Ranking)**: Extracts the prioritized work order queue for operator dispatching.

---

## 3. SQL Execution Verification Summary

All 21 queries were executed against the live Apache Trino coordinator. Execution results are captured in `results/phase4_sql_verification.json`:

```json
{
  "total_sql_files": 7,
  "total_queries_evaluated": 21,
  "total_queries_passed": 21,
  "status": "VERIFIED"
}
```
