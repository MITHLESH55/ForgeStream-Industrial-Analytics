# ForgeStream Phase 4: Operational KPIs & Ranking Formulations

## Executive Overview
The Operational KPI and Ranking Engine (`forgestream.serving.kpis.OperationalKPIEngine`) evaluates deterministic mathematical formulations across fleet telemetry and ML prognostic outputs. These calculations provide real-time visibility into equipment health distributions, impending failure risks, and prioritized maintenance schedules.

---

## Mathematical Formulations

### 1. Fleet Health Score ($FHS$)
The continuous normalized mean health metric across all $N$ monitored assets:
$$FHS = \frac{1}{N} \sum_{i=1}^{N} H_i, \quad H_i \in [0.0, 1.0]$$

### 2. Failure Risk Rate ($FRR$)
The fraction of the fleet exhibiting imminent failure probability exceeding operational threshold $\tau_{risk} = 0.50$:
$$FRR = \frac{1}{N} \sum_{i=1}^{N} \mathbb{I}(P_{fail, i} \ge 0.50)$$

### 3. Assets Near Failure ($N_{near}$)
Asset count with Remaining Useful Life (RUL) under 24 operating hours:
$$N_{near} = \sum_{i=1}^{N} \mathbb{I}(\text{RUL}_i < 24.0\text{ hours})$$

### 4. Prescriptive Maintenance Urgency Score ($S_{priority}$)
To prioritize engineering resources, every asset is scored on a $[0, 100]$ scale incorporating failure probability, RUL decay, and asset operational criticality:
$$S_{priority} = 100 \times \left(0.45 \cdot P_{fail} + 0.35 \cdot \left(1.0 - \frac{\min(\text{RUL}, 120.0)}{120.0}\right) + 0.20 \cdot C_{asset}\right)$$

Where:
- $P_{fail} \in [0.0, 1.0]$: Prognostic failure probability from the ML inference engine.
- $\text{RUL} \in [0.0, \infty)$: Estimated remaining operating hours until functional failure.
- $C_{asset} \in [0.0, 1.0]$: Equipment criticality tier rating (`CRITICAL: 1.0`, `HIGH: 0.75`, `MEDIUM: 0.50`, `LOW: 0.25`).

---

## Maintenance Priority Tier Stratification

Based on the computed $S_{priority}$, assets are assigned deterministic maintenance priority tiers and actionable prescriptive work order recommendations:

| Priority Tier | Priority Score Range | Prescriptive Recommendation | Target SLA |
|---|---|---|---|
| **EMERGENCY** | $S_{priority} \ge 75.0$ | Immediate emergency shutdown and dispatch technician. | $< 2\text{ hours}$ |
| **HIGH** | $50.0 \le S_{priority} < 75.0$ | Schedule urgent inspection and replacement parts within 24h. | $< 24\text{ hours}$ |
| **MEDIUM** | $25.0 \le S_{priority} < 50.0$ | Monitor degradation trend and schedule routine maintenance window. | $< 7\text{ days}$ |
| **LOW** | $S_{priority} < 25.0$ | Nominal condition. Continue regular operational logging. | Next Planned Cycle |

---

## Verification & Accuracy
The KPI formulation is validated by unit tests in `tests/unit/test_phase4_serving.py` and continuous integration pipelines:
- Score boundedness: $\forall i, 0.0 \le S_{priority, i} \le 100.0$.
- Strict monotonicity: An increase in $P_{fail}$ or decrease in $\text{RUL}$ strictly increases $S_{priority}$.
- Criticality weighting: High-criticality equipment ranks higher than low-criticality equipment given identical degradation parameters.
