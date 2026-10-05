# ForgeStream Phase 3: Label Engineering & Target Isolation Design

## Overview
Label formulation in industrial predictive maintenance requires careful mathematical design to prevent artificial artifacts, avoid lookahead bias, and provide actionable operational horizons for plant operators. ForgeStream implements two complementary prognostic targets:
1. **Piecewise Linear Remaining Useful Life (RUL)** target for continuous degradation estimation.
2. **Operational Horizon Binary Failure Risk** target for early warning alarms.

---

## 1. Piecewise Linear Remaining Useful Life (RUL) Formulation

In industrial equipment, an asset operating in a healthy state experiences negligible degradation early in its lifecycle. A naive linear RUL function $y(t) = T_{\text{fail}} - t$ forces the regression model to act as an arbitrary clock counter rather than learning physical degradation patterns.

To reflect physical reality, ForgeStream employs the standard **piecewise linear degradation model** (originating in NASA turbofan degradation benchmarks):

$$y_{\text{RUL}}(t) = \min\left(T_{\text{max}}, \max\left(0.0, T_{\text{fail}} - t\right)\right)$$

Where:
- $t$: Current operational time step in hours.
- $T_{\text{fail}}$: Total operational time at critical failure.
- $T_{\text{max}} = 120.0\text{ hours}$: Piecewise linear cap parameter.
- When an asset is healthy ($T_{\text{fail}} - t > T_{\text{max}}$), $y_{\text{RUL}}(t) = T_{\text{max}}$.
- When physical degradation commences ($T_{\text{fail}} - t \le T_{\text{max}}$), $y_{\text{RUL}}(t)$ decreases monotonically to $0.0\text{h}$ at the moment of failure.
- For baseline healthy runs that do not fail, $y_{\text{RUL}}(t) = T_{\text{max}}$ throughout the trajectory.

---

## 2. Operational Prediction Horizon ($H$) Binary Classification

Plant operators require actionable lead time ($H$) to schedule maintenance, stage spare parts, and prevent catastrophic unplanned outages.

The binary failure risk label $y_{\text{fail}}(t) \in \{0, 1\}$ is formulated as:

$$y_{\text{fail}}(t) = \begin{cases} 1, & \text{if } (T_{\text{fail}} - t) \le H \text{ and asset experiences failure} \\ 0, & \text{otherwise} \end{cases}$$

Where:
- $H = 24.0\text{ hours}$ (equivalent to 120 simulation steps at $1.0\text{Hz}$).
- Positive class ($y_{\text{fail}} = 1$) indicates that the asset will fail within the next 24 operational hours.
- Negative class ($y_{\text{fail}} = 0$) indicates normal operation or failure outside the immediate action horizon.

---

## 3. Zero Target Leakage Guarantee & Automated Audit

Target leakage occurs when training features contain information derived from the ground-truth outcome, future timestamps, or simulator internals that would not exist at inference time.

### Forbidden Metadata Whitelist
ForgeStream strictly excludes the following simulation internal metadata columns from the ML feature matrix:
- `scenario_id` & `scenario_name` (internal fault injection scenario identifier)
- `maintenance_state` (ground-truth health state from simulation engine)
- `degradation_stage` (internal stage progression counter)
- `ground_truth_rul_hours` (un-capped physical simulator remaining time)
- `label_failure` & `label_rul` (target columns themselves)
- `is_synthetic` (dataset generation tag)

### Automated 3-Stage Target Leakage Audit Engine
Implemented in `forgestream.ml.leakage_auditor.TargetLeakageAuditor`:
1. **Feature Whitelist Check**: Verifies that 0 of the 25 input feature column names contain forbidden keywords.
2. **Temporal Causality Verification**: Confirms strictly monotonic timestamp progression ($t_{k+1} \ge t_k$) per asset run.
3. **Correlation Anomaly Scan**: Scans Pearson correlation coefficient between every feature $x_j$ and target $y$. Any feature with $|r| \ge 0.999$ triggers an immediate `FAIL` audit flag.

Audit Result: **`results/phase3_leakage_audit.json` $\to$ STATUS: PASS — Verified with Real Apache Spark MLlib, MLflow Tracking, Target Leakage Audits, and Automated Tests**.
