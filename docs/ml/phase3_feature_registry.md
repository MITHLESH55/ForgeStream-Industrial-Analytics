# ForgeStream Phase 3: Feature Registry & Engineering Specifications

## Overview
The Phase 3 ML Feature Registry standardizes all 25 operational features fed into the Spark MLlib modeling pipeline. These features represent a unified bridge between raw physical sensor measurements, Phase 2 online streaming aggregations, rate-of-change physics slopes, dimensionless operational indicators, and composite health indices.

---

## Complete 25-Feature Operational Registry

| # | Feature Name | Category | Data Type | Physical Units | Description & Formulation |
| :-: | :--- | :--- | :--- | :--- | :--- |
| 1 | `temperature` | Raw Sensor | `FloatType` | °C | Instantaneous asset housing/bearing temperature |
| 2 | `vibration` | Raw Sensor | `FloatType` | mm/s | Instantaneous RMS vibration amplitude |
| 3 | `pressure` | Raw Sensor | `FloatType` | bar | Instantaneous operational pressure |
| 4 | `current` | Raw Sensor | `FloatType` | A | Instantaneous motor phase current |
| 5 | `voltage` | Raw Sensor | `FloatType` | V | Instantaneous line voltage |
| 6 | `power` | Raw Sensor | `FloatType` | kW | Instantaneous active power consumption ($P = \sqrt{3} V I \cos\phi$) |
| 7 | `load_pct` | Raw Sensor | `FloatType` | % | Applied mechanical load percentage (0–100%) |
| 8 | `ambient_temp` | Raw Sensor | `FloatType` | °C | Ambient environmental baseline temperature |
| 9 | `asset_type` | Categorical | `StringType` | Category | Equipment class (`MOTOR`, `PUMP`, `COMPRESSOR`, `CONVEYOR`, `TURBINE`) |
| 10 | `operating_mode` | Categorical | `StringType` | Mode | Operational state (`STARTUP`, `STEADY_LOW`, `STEADY_HIGH`, `TRANSIENT`, `MAINTENANCE`) |
| 11 | `rolling_mean_temp` | Rolling (30s) | `FloatType` | °C | 30-second moving average temperature ($\mu_T = \frac{1}{N}\sum T_i$) |
| 12 | `rolling_std_temp` | Rolling (30s) | `FloatType` | °C | 30-second temperature standard deviation ($\sigma_T$) |
| 13 | `rolling_mean_vib` | Rolling (30s) | `FloatType` | mm/s | 30-second moving average vibration amplitude ($\mu_V$) |
| 14 | `rolling_std_vib` | Rolling (30s) | `FloatType` | mm/s | 30-second vibration standard deviation / instability ($\sigma_V$) |
| 15 | `rolling_mean_pres` | Rolling (30s) | `FloatType` | bar | 30-second moving average discharge pressure ($\mu_P$) |
| 16 | `rolling_std_pres` | Rolling (30s) | `FloatType` | bar | 30-second pressure standard deviation ($\sigma_P$) |
| 17 | `rolling_mean_current` | Rolling (30s) | `FloatType` | A | 30-second moving average motor current ($\mu_I$) |
| 18 | `rolling_std_current` | Rolling (30s) | `FloatType` | A | 30-second current variation / load hunting ($\sigma_I$) |
| 19 | `rolling_mean_power` | Rolling (30s) | `FloatType` | kW | 30-second moving average active power consumption |
| 20 | `rolling_std_power` | Rolling (30s) | `FloatType` | kW | 30-second active power fluctuation |
| 21 | `thermal_rise_rate` | Rate of Change | `FloatType` | °C/s | First-order derivative of temperature ($\frac{\Delta T}{\Delta t}$) over 5s window |
| 22 | `vibration_slope` | Rate of Change | `FloatType` | mm/s² | First-order linear regression slope of vibration ($\frac{\Delta V}{\Delta t}$) |
| 23 | `current_load_ratio` | Dimensionless | `FloatType` | Ratio | Normalized electrical current relative to nominal load ($I / I_{\text{rated}}$) |
| 24 | `pressure_instability`| Dimensionless | `FloatType` | Index | Normalized pressure variation index ($\sigma_P / \mu_P$) |
| 25 | `health_score` | Health Model | `FloatType` | Score (0-1) | Composite asset health index $H(t) \in [0.0, 1.0]$ with penalty weights |

---

## Spark MLlib Feature Pipeline Stages

The feature transformation pipeline is constructed using Spark MLlib native stages:
1. **`StringIndexer`**: Encodes `asset_type` $\to$ `asset_type_idx` and `operating_mode` $\to$ `operating_mode_idx` using frequency ranking.
2. **`VectorAssembler`**: Concatenates the 23 numeric features with the 2 indexed categorical features into a dense vector `raw_features` ($\text{dim} = 25$).
3. **`StandardScaler(withMean=True, withStd=True)`**: Normalizes each feature column to zero mean and unit variance ($\mu = 0, \sigma = 1$), outputting `scaled_features`.

Feature Metadata Artifact: **`results/phase3_feature_registry.json`**.
