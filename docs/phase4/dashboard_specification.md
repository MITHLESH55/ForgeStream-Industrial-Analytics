# ForgeStream Phase 4: Dashboard Panel Specification & Metric Formulations

## 1. Executive Summary

This document specifies the exact query, data binding, visualization configuration, and threshold parameters for each of the 16 panels in the **FORGESTREAM INDUSTRIAL OPERATIONS CENTER** dashboard.

> **Important Regulatory & Industry Disclaimer**:  
> All thresholds, urgency weights, and scoring formulas described herein are **ForgeStream Project Policy Parameters** established for academic demonstration and operational research, not universal ISO/IEC industrial standards.

---

## 2. Parameter & Threshold Definitions

| Parameter Name | Value | Project Policy Interpretation |
|---|:---:|---|
| $\tau_{\text{risk}}$ (High Risk Threshold) | $0.50$ | Failure probability within 24h horizon $\ge 50\%$ triggers high risk classification. |
| $\tau_{\text{emergency}}$ (Emergency Threshold) | $0.80$ | Failure probability $\ge 80\%$ or $\text{RUL} \le 10\text{h}$ mandates immediate shutdown work order. |
| $T_{\text{near\_failure}}$ (Near-Failure Horizon) | $24.0\text{ h}$ | Assets with predicted RUL under 24 hours require urgent scheduling. |
| $T_{\text{max}}$ (Max Theoretical RUL) | $120.0\text{ h}$ | Phase 3 model prediction cap representing nominal wear horizon. |
| $C_{\text{asset}}$ (Criticality Weights) | `LOW`: 0.10, `MED`: 0.25, `HIGH`: 0.40, `CRIT`: 0.50 | Multiplier accounting for plant structural impact. |

---

## 3. Mathematical KPI Formulations

### 3.1 Fleet Health Score ($FHS$)
$$FHS = \frac{1}{N} \sum_{i=1}^N H_i(t), \quad H_i(t) \in [0.0, 1.0]$$
- **Interpretation**: Mean health index across all active fleet units. $1.0 = \text{Optimal}$, $< 0.70 = \text{Fleet-wide Degradation}$.

### 3.2 Failure Risk Rate ($FRR$)
$$FRR = \frac{\sum_{i=1}^N \mathbb{I}(P_{\text{fail}, i} \ge 0.50)}{N}$$
- **Interpretation**: Proportion of the fleet currently operating at elevated failure risk.

### 3.3 Prescriptive Priority Score ($S_{\text{priority}}$)
$$S_{\text{priority}} = 100 \times \left(0.35 \cdot (1.0 - H_i) + 0.35 \cdot P_{\text{fail}, i} + 0.20 \cdot \max\left(0, 1.0 - \frac{\text{RUL}_i}{120.0}\right) + 0.10 \cdot C_{\text{asset}}\right)$$
- **Range**: $[0.0, 100.0]$.
- **Interpretation**: Monotonically increasing urgency score used to sequence maintenance work orders.

---

## 4. Comprehensive Panel Specifications

### ROW 1: Executive Operational KPIs

#### Panel 1: Total Assets
- **Purpose**: Displays the total count of registered assets in the active fleet.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Stat Card (Single-stat)
- **Refresh Interval**: 10 seconds
- **SQL Query**:
  ```sql
  SELECT total_assets FROM fleet_kpi_snapshots ORDER BY timestamp DESC LIMIT 1;
  ```

#### Panel 2: Fleet Health Score
- **Purpose**: Real-time gauge of global fleet health index.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Gauge ($0.0 - 1.0$)
- **Color Thresholds**: Green ($> 0.85$), Yellow ($0.70 - 0.85$), Red ($< 0.70$)
- **SQL Query**:
  ```sql
  SELECT fleet_health_score FROM fleet_kpi_snapshots ORDER BY timestamp DESC LIMIT 1;
  ```

#### Panel 3: Healthy Assets
- **Purpose**: Count of assets currently operating in nominal `HEALTHY` state.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Stat Card (Green)
- **SQL Query**:
  ```sql
  SELECT healthy_assets FROM fleet_kpi_snapshots ORDER BY timestamp DESC LIMIT 1;
  ```

#### Panel 4: High Failure Risk Assets
- **Purpose**: Count of assets with failure probability $\ge 0.50$.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Stat Card (Red)
- **SQL Query**:
  ```sql
  SELECT high_risk_assets FROM fleet_kpi_snapshots ORDER BY timestamp DESC LIMIT 1;
  ```

#### Panel 5: Average Predicted RUL
- **Purpose**: Mean Remaining Useful Life across all operational units.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Stat Card (Unit: hours)
- **SQL Query**:
  ```sql
  SELECT avg_predicted_rul_hours FROM fleet_kpi_snapshots ORDER BY timestamp DESC LIMIT 1;
  ```

#### Panel 6: Active Alerts
- **Purpose**: Total count of active unacknowledged streaming alerts.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Stat Card (Orange)
- **SQL Query**:
  ```sql
  SELECT active_alerts_total FROM fleet_kpi_snapshots ORDER BY timestamp DESC LIMIT 1;
  ```

---

### ROW 2: Asset Health & Sensor Registry

#### Panel 7: Fleet Asset Health & Calibrated Sensor Registry
- **Purpose**: Comprehensive tabular view of every asset's operational parameters and state.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Table with cell color overrides for `health_state` and `maintenance_priority`.
- **SQL Query**:
  ```sql
  SELECT
      asset_id,
      asset_type,
      operating_mode,
      health_state,
      ROUND(health_score::numeric, 3) AS health_score,
      ROUND(temperature::numeric, 1) AS temp_c,
      ROUND(vibration::numeric, 2) AS vib_mms,
      ROUND(pressure::numeric, 1) AS press_bar,
      ROUND(load::numeric, 1) AS load_pct,
      ROUND(rpm::numeric, 0) AS rpm
  FROM asset_current_state
  ORDER BY health_score ASC;
  ```

#### Panel 8: Asset Health Distribution
- **Purpose**: Histogram of assets categorized across the 4 discrete health states.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Bar Chart
- **SQL Query**:
  ```sql
  SELECT
      health_state,
      COUNT(*) AS count
  FROM asset_current_state
  GROUP BY health_state
  ORDER BY count DESC;
  ```

---

### ROW 3: Predictive Maintenance & Prognostic Ranking

#### Panel 9: Prescriptive Maintenance Priority Queue
- **Purpose**: Actionable work order queue ranked by composite priority score.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Table with ranking column and highlighted priority badges.
- **SQL Query**:
  ```sql
  SELECT
      ranking,
      asset_id,
      maintenance_priority,
      ROUND(priority_score::numeric, 1) AS priority_score,
      ROUND(health_score::numeric, 3) AS health_score,
      ROUND(failure_probability::numeric, 3) AS failure_prob,
      ROUND(predicted_rul_hours::numeric, 1) AS rul_hours,
      recommended_action
  FROM maintenance_priority_queue
  ORDER BY ranking ASC;
  ```

#### Panel 10: Top Failure Probability Ranking
- **Purpose**: Visual ranking of assets with the highest failure risk.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Horizontal Bar Gauge ($0.0 - 1.0$)
- **SQL Query**:
  ```sql
  SELECT
      asset_id,
      failure_probability
  FROM asset_current_state
  ORDER BY failure_probability DESC
  LIMIT 10;
  ```

---

### ROW 4: Temporal Trajectories & Alert Dynamics

#### Panel 11: Fleet Health & Mean RUL Trajectory
- **Purpose**: Dual-axis time series tracking fleet health score alongside average RUL over time.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Time Series Chart
- **SQL Query**:
  ```sql
  SELECT
      timestamp AS time,
      fleet_health_score,
      avg_predicted_rul_hours
  FROM fleet_kpi_snapshots
  ORDER BY timestamp ASC;
  ```

#### Panel 12: Alert Volume by Severity
- **Purpose**: Stacked bar chart showing the arrival rate of warning, high, and critical alerts.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Bar Chart / Time Series
- **SQL Query**:
  ```sql
  SELECT
      severity,
      COUNT(*) AS alert_count
  FROM asset_alert_history
  GROUP BY severity
  ORDER BY alert_count DESC;
  ```

#### Panel 13: RUL Decay by Critical Asset
- **Purpose**: Time series illustrating the degradation curve of degrading assets.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Time Series Graph
- **SQL Query**:
  ```sql
  SELECT
      event_time AS time,
      asset_id,
      predicted_rul_hours
  FROM asset_prediction_history
  ORDER BY event_time ASC;
  ```

---

### ROW 5: Asset Diagnostics & Operating Regimes

#### Panel 14: Mean Load by Operating Mode
- **Purpose**: Evaluates mechanical loading across operating regimes (`NORMAL`, `HEAVY`, `DEGRADED`).
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Bar Chart
- **SQL Query**:
  ```sql
  SELECT
      operating_mode,
      ROUND(AVG(load)::numeric, 1) AS avg_load_pct
  FROM asset_current_state
  GROUP BY operating_mode;
  ```

#### Panel 15: Vibration vs Temperature Outliers
- **Purpose**: Scatter/point plot correlating vibration with temperature to detect thermal/mechanical coupling faults.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Point Series Chart
- **SQL Query**:
  ```sql
  SELECT
      asset_id,
      temperature AS temp_c,
      vibration AS vib_mms
  FROM asset_current_state;
  ```

#### Panel 16: Equipment Criticality Breakdown
- **Purpose**: Proportion of monitored fleet categorized by criticality class.
- **Datasource**: `ForgeStream-PostgreSQL`
- **Visualization**: Pie / Donut Chart
- **SQL Query**:
  ```sql
  SELECT
      criticality,
      COUNT(*) AS unit_count
  FROM maintenance_priority_queue
  GROUP BY criticality;
  ```
