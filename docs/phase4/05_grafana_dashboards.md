# ForgeStream Phase 4: Operational Dashboard & Grafana Provisioning

## Executive Overview
ForgeStream Phase 4 deploys **Grafana 10** with declarative configuration provisioning to deliver an industrial Operations Center dashboard (`ForgeStream Operations Center - Phase 4 Lakehouse Analytics`).

---

## Provisioning Architecture

```
grafana/
├── provisioning/
│   ├── datasources/
│   │   └── postgres.yml        <--- Declarative PostgreSQL connection config
│   └── dashboards/
│       └── dashboards.yml      <--- Automatic dashboard JSON provider
└── dashboards/
    └── forgestream_operations_center.json <--- 16-Panel, 5-Row Dashboard Definition
```

### 1. Datasource Configuration (`grafana/provisioning/datasources/postgres.yml`)
```yaml
apiVersion: 1

datasources:
  - name: ForgeStream-PostgreSQL
    type: postgres
    access: proxy
    url: postgres:5432
    user: forgestream
    secureJsonData:
      password: "forgestream"
    jsonData:
      database: forgestream
      sslmode: "disable"
      maxOpenConns: 20
      maxIdleConns: 5
      connMaxLifetime: 14400
      postgresVersion: 1600
      timescaledb: false
    isDefault: true
    editable: false
```

---

## Dashboard Structure (5 Rows, 16 Monitoring Panels)

| Row | Focus Area | Panels Included |
|---|---|---|
| **Row 1** | **Executive Fleet Health Overview** | 1. Total Assets Active (Stat)<br>2. Fleet Health Score (Gauge, $0.0-1.0$)<br>3. High Failure Risk Assets (Stat, $P_{fail} \ge 0.50$)<br>4. Average Predicted RUL (Stat, hours)<br>5. Active Critical Alerts (Stat)<br>6. Emergency Work Orders (Stat) |
| **Row 2** | **Asset Health & Telemetry Registry** | 7. Fleet Asset Health & Calibrated Sensor Registry (Table with colored health states)<br>8. Asset Health Distribution (Bar Chart: Healthy, Watch, Degraded, Critical) |
| **Row 3** | **Prognostics & Maintenance Ranking** | 9. Failure Probability Ranking (Horizontal Bar Gauge)<br>10. Prescriptive Maintenance Priority Queue (Table with ranking, urgency score, action) |
| **Row 4** | **Temporal Health & Alert Dynamics** | 11. Fleet Health Score & Mean RUL Trajectory (Time-series graph)<br>12. Active Alert Volume by Severity (Time-series stacked bar chart)<br>13. RUL Decay by Critical Asset (Time-series graph) |
| **Row 5** | **Asset Performance & Operating Regimes** | 14. Mean Load by Operating Mode (Bar chart)<br>15. Vibration vs Temperature Cross-Plot (Scatter / Multi-series)<br>16. Equipment Criticality Breakdown (Pie chart) |

---

## Verification & Publication Artifacts

### 1. Live Grafana Verification
Grafana dashboard verified through live provisioning / REST API:
- Dashboard discovery: `GET /api/search` returned `uid: forgestream-ops-center-p4`.
- Panel count verified: Exactly 16 panels across 5 rows.
- Datasource connectivity: Verified query execution against `postgres:5432` with sub-second dashboard refresh.

### 2. Publication Figure
- **Figure 11 (`results/figures/fig11_fleet_health_dashboard_mockup.png`)**: Publication figure illustrating dashboard layout for report documentation (distinguished from the live provisioned Grafana instance).
