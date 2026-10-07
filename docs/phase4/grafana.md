# ForgeStream Phase 4: Grafana Operational Dashboard Specification

## 1. Executive Summary

ForgeStream Phase 4 integrates **Grafana 10** to deliver a real-time, industrial-grade monitoring dashboard titled **FORGESTREAM INDUSTRIAL OPERATIONS CENTER**. The dashboard provides complete visibility from fleet-level KPIs down to single-sensor diagnostic traces.

```
+---------------------------------------------------------------------------------------------------------+
|                               FORGESTREAM INDUSTRIAL OPERATIONS CENTER                                  |
+---------------------------------------------------------------------------------------------------------+
| [ ROW 1: EXECUTIVE FLEET KPIS ]                                                                         |
|  +--------------+  +--------------+  +--------------+  +--------------+  +--------------+  +----------+ |
|  | Total Assets |  | Fleet Health |  | Healthy Units|  | High Risk    |  | Avg RUL (h)  |  | Active   | |
|  |     [ 28 ]   |  |   [ 0.866 ]  |  |    [ 16 ]    |  |    [ 3 ]     |  |   [ 104.5 ]  |  |  [ 12 ]  | |
|  +--------------+  +--------------+  +--------------+  +--------------+  +--------------+  +----------+ |
+---------------------------------------------------------------------------------------------------------+
| [ ROW 2: ASSET HEALTH & SENSOR REGISTRY ]                                                               |
|  +-------------------------------------------------------------------+  +-----------------------------+ |
|  | Live Asset Sensor Registry Table (ID, Mode, State, Temp, Vib, RPM)|  | Health State Distribution   | |
|  | (Colored badges: HEALTHY, WATCH, DEGRADED, CRITICAL)               |  | (Bar Chart / Histogram)     | |
|  +-------------------------------------------------------------------+  +-----------------------------+ |
+---------------------------------------------------------------------------------------------------------+
| [ ROW 3: PREDICTIVE MAINTENANCE & PROGNOSTIC RANKING ]                                                  |
|  +-------------------------------------------------------------------+  +-----------------------------+ |
|  | Prescriptive Maintenance Priority Work Order Queue                |  | Top Failure Probability Bar | |
|  | (Rank #1-28, Urgency Score, Recommended Action)                   |  | (Horizontal Gauge)          | |
|  +-------------------------------------------------------------------+  +-----------------------------+ |
+---------------------------------------------------------------------------------------------------------+
| [ ROW 4: TEMPORAL TRAJECTORIES & ALERT DYNAMICS ]                                                       |
|  +--------------------------------------+  +------------------------------------+  +------------------+ |
|  | Fleet Health & Mean RUL Trajectory   |  | Alert Volume by Severity           |  | RUL Decay Curve  | |
|  | (Dual-Axis Time Series)              |  | (Stacked Bar Time Series)          |  | (Degrading Units)| |
|  +--------------------------------------+  +------------------------------------+  +------------------+ |
+---------------------------------------------------------------------------------------------------------+
| [ ROW 5: ASSET PERFORMANCE & DIAGNOSTICS ]                                                              |
|  +--------------------------------------+  +------------------------------------+  +------------------+ |
|  | Load by Operating Mode (Bar Chart)   |  | Vibration vs Temperature Outliers  |  | Criticality Share| |
|  +--------------------------------------+  +------------------------------------+  +------------------+ |
+---------------------------------------------------------------------------------------------------------+
```

---

## 2. Declarative Provisioning Architecture

All Grafana resources are defined declaratively as code in the repository:

```
grafana/
├── provisioning/
│   ├── datasources/
│   │   └── datasources.yml      <--- PostgreSQL datasource configuration
│   └── dashboards/
│       └── dashboards.yml       <--- Automatic dashboard file provider
└── dashboards/
    └── forgestream_operations_center.json <--- 16-Panel Dashboard JSON Definition
```

### 2.1 Datasource Configuration (`grafana/provisioning/datasources/datasources.yml`)
```yaml
apiVersion: 1

datasources:
  - name: ForgeStream-PostgreSQL
    type: postgres
    access: proxy
    url: postgres:5432
    database: forgestream_db
    user: forgestream_user
    secureJsonData:
      password: forgestream_secret
    jsonData:
      sslmode: disable
      maxOpenConns: 10
      maxIdleConns: 5
      connMaxLifetime: 14400
      postgresVersion: 1600
      timescaledb: false
    isDefault: true
    editable: true
```

---

## 3. Panel Inventory (16 Panels across 5 Rows)

| Row | Panel ID | Panel Title | Visualization Type | Data Source Query Target |
|---|:---:|---|---|---|
| **Row 1** | `1` | Total Assets | Stat Card | `fleet_kpi_snapshots.total_assets` |
| **Row 1** | `2` | Fleet Health Score | Gauge ($0.0-1.0$) | `fleet_kpi_snapshots.fleet_health_score` |
| **Row 1** | `3` | Healthy Assets | Stat Card | `fleet_kpi_snapshots.healthy_assets` |
| **Row 1** | `4` | High Risk Assets ($P_{fail} \ge 0.50$) | Stat Card (Red) | `fleet_kpi_snapshots.high_risk_assets` |
| **Row 1** | `5` | Average Predicted RUL | Stat Card (Hours) | `fleet_kpi_snapshots.avg_predicted_rul_hours` |
| **Row 1** | `6` | Active Alerts | Stat Card (Orange) | `fleet_kpi_snapshots.active_alerts_total` |
| **Row 2** | `7` | Fleet Asset Health & Calibrated Sensor Registry | Table with Color Badges | `asset_current_state` (All columns) |
| **Row 2** | `8` | Asset Health Distribution | Bar Chart | `asset_current_state` grouped by `health_state` |
| **Row 3** | `9` | Prescriptive Maintenance Priority Queue | Table with Actions | `maintenance_priority_queue` ordered by `ranking ASC` |
| **Row 3** | `10`| Top Failure Probability Ranking | Horizontal Bar Gauge | `asset_current_state.failure_probability` |
| **Row 4** | `11`| Fleet Health & Mean RUL Trajectory | Time Series | `fleet_kpi_snapshots` time series |
| **Row 4** | `12`| Alert Volume by Severity | Stacked Bar Time Series | `asset_alert_history` grouped by `severity` |
| **Row 4** | `13`| RUL Decay by Critical Asset | Time Series | `asset_prediction_history.predicted_rul_hours` |
| **Row 5** | `14`| Mean Load by Operating Mode | Bar Chart | `asset_current_state` grouped by `operating_mode` |
| **Row 5** | `15`| Vibration vs Temperature Outliers | Scatter / Point Series | `asset_current_state` ($Z$-scores) |
| **Row 5** | `16`| Equipment Criticality Breakdown | Pie Chart | `maintenance_priority_queue.criticality` |

---

## 4. Operational Interpretation & Response Workflow

1. **Executive Triage (Row 1)**: The operator glances at the Fleet Health Score ($FHS$) and High Risk count. If $FHS < 0.80$ or High Risk $> 0$, investigation is triggered.
2. **Asset Health Drilldown (Row 2)**: The operator locates the degraded unit in the Sensor Registry table to inspect temperature, vibration, and operating mode.
3. **Prescriptive Action Dispatch (Row 3)**: The operator consults the Maintenance Priority Queue (Rank #1) and copies the recommended action directly into the plant Computerized Maintenance Management System (CMMS).
4. **Trajectory & Root Cause (Rows 4 & 5)**: The engineer reviews the RUL decay curve and multi-sensor correlation cross-plots to identify if thermal runaway or bearing wear is the primary failure mode.
