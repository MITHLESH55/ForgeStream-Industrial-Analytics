# ForgeStream Phase 3: Operational Evaluation, Explainability & Cross-Asset Validation

## Overview
Phase 3 establishes an industrial-grade prognostic evaluation framework. Beyond standard ML statistical metrics, models are evaluated against operational plant safety criteria, NASA PHM asymmetric penalties, feature importance attributions, and granular cross-asset / cross-scenario slices.

---

## 1. Operational & Prognostic Evaluation Metrics

### NASA PHM Asymmetric Scoring Function
In industrial maintenance, estimating that a machine has 30 hours of life remaining when it only has 10 hours (late estimation / overestimation) results in catastrophic unexpected downtime. Conversely, predicting 10 hours when it actually has 30 hours (early estimation / underestimation) merely triggers premature inspection.

To mathematically capture this operational asymmetry:

$$d_i = \hat{y}_i - y_i \quad (\text{Error in hours})$$

$$\text{Penalty}(d_i) = \begin{cases} \exp\left(-\frac{d_i}{13}\right) - 1, & \text{if } d_i < 0 \text{ (Early estimation / Conservative)} \\ \exp\left(\frac{d_i}{10}\right) - 1, & \text{if } d_i \ge 0 \text{ (Late estimation / Dangerous)} \end{cases}$$

### Operational Tolerance Envelopes ($\pm10\%$, $\pm25\%$)
Plant scheduling requires predictions within defined uncertainty bands:
- **$\text{Accuracy}_{\pm10\%}$**: Percentage of test predictions where $|\hat{y} - y| \le 12.0\text{ hours}$ ($0.10 \times 120\text{h}$).
- **$\text{Accuracy}_{\pm25\%}$**: Percentage of test predictions where $|\hat{y} - y| \le 30.0\text{ hours}$ ($0.25 \times 120\text{h}$).

---

## 2. Feature Explainability & Attribution Ranking

Feature importance analysis reveals the underlying physical drivers captured by the Random Forest tree ensemble:

### Top Feature Attributions (Classification Task)
| Rank | Feature Name | Mean Decrease Impurity (MDI) | Physical Significance |
| :-: | :--- | :---: | :--- |
| **1** | `rolling_mean_vib` | **$0.2481$** | Primary mechanical wear and imbalance indicator |
| **2** | `vibration` | **$0.1942$** | Instantaneous dynamic shock and bearing fault response |
| **3** | `vibration_slope` | **$0.1415$** | Acceleration of mechanical deterioration rate |
| **4** | `health_score` | **$0.0982$** | Composite multi-sensor degradation tracker |
| **5** | `rolling_mean_temp` | **$0.0754$** | Thermal friction accumulation |
| **6** | `thermal_rise_rate` | **$0.0621$** | Transient overheating slope |
| **7** | `rolling_std_vib` | **$0.0489$** | Vibration amplitude dispersion / instability |
| **8** | `current_load_ratio` | **$0.0385$** | Electrical efficiency degradation |

Artifact: **`results/phase3_explainability_attributions.json`** & **`results/figures/fig5_feature_importances_classification.png`**.

---

## 3. Cross-Asset Slicing Breakdown (5 Equipment Classes)

Evaluating the Champion Random Forest model across all 5 distinct industrial asset types on the held-out test partition:

| Asset Class | Test Sample Count | Classification PR-AUC | Classification Recall | RUL $R^2$ Score | RUL RMSE (Hours) |
| :--- | :---: | :---: | :---: | :---: | :---: |
| **Induction Motor** | $840$ | **$0.8120$** | **$0.8810$** | **$0.4320$** | $28.45\text{h}$ |
| **Centrifugal Pump** | $840$ | **$0.7850$** | **$0.8650$** | **$0.4110$** | $29.80\text{h}$ |
| **Reciprocating Compressor** | $840$ | **$0.7510$** | **$0.8420$** | **$0.3850$** | $31.20\text{h}$ |
| **Conveyor System** | $840$ | **$0.7420$** | **$0.8350$** | **$0.3790$** | $32.10\text{h}$ |
| **Gas/Steam Turbine** | $840$ | **$0.7430$** | **$0.8320$** | **$0.3830$** | $32.15\text{h}$ |

Artifact: **`results/phase3_cross_asset_evaluation.json`** & **`results/figures/fig7_cross_asset_performance.png`**.

---

## 4. Cross-Scenario Slicing Breakdown (8 Degradation Scenarios)

| Scenario Identifier | Fault Description | Classification Recall | Classification PR-AUC | RUL MAE (Hours) |
| :--- | :--- | :---: | :---: | :---: |
| `SCENARIO_001` | Normal Baseline Operation | $1.0000$ (Spec) | $1.0000$ | $4.20\text{h}$ |
| `SCENARIO_002` | Progressive Bearing Wear | **$0.8850$** | **$0.8120$** | $21.50\text{h}$ |
| `SCENARIO_003` | Thermal Runaway & Overheating | **$0.8720$** | **$0.7950$** | $22.40\text{h}$ |
| `SCENARIO_004` | Impeller Cavitation & Flow Loss | **$0.8410$** | **$0.7650$** | $24.80\text{h}$ |
| `SCENARIO_005` | Stator Winding & Voltage Sag | **$0.8520$** | **$0.7780$** | $23.90\text{h}$ |
| `SCENARIO_006` | Transient Load Shifting | $0.9850$ (Spec) | $0.9920$ | $8.50\text{h}$ |
| `SCENARIO_007` | Network Jitter & Delayed Events | **$0.8240$** | **$0.7410$** | $25.90\text{h}$ |
| `SCENARIO_008` | Out-of-Order Sensor Arrivals | **$0.8190$** | **$0.7350$** | $26.40\text{h}$ |

Artifact: **`results/phase3_scenario_evaluation.json`** & **`results/figures/fig8_scenario_performance.png`**.
