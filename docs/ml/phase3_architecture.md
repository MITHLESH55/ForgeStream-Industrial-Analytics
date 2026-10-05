# ForgeStream Phase 3: Predictive Maintenance ML & Prognostics Architecture

## Executive Overview
Phase 3 establishes the predictive machine learning, prognostics, and model governance layer for the ForgeStream industrial intelligence platform. Building upon the Phase 1 deterministic simulation foundation and Phase 2 real-time streaming feature engine, Phase 3 transforms continuous physical sensor streams and Lakehouse historical partitions into:
1. **Binary Failure Risk Classification ($y_{\text{fail}} \in \{0, 1\}$)**: Detecting imminent equipment breakdown within an operational prediction horizon ($H = 24.0\text{h} / 120\text{ steps}$) under severe industrial class imbalance.
2. **Remaining Useful Life (RUL) Regression ($y_{\text{RUL}} \in \mathbb{R}^+$)**: Continuous estimation of remaining operating hours before catastrophic failure, bounded by a piecewise linear horizon cap ($T_{\text{max}} = 120.0\text{h}$).
3. **Model Explainability & Feature Attribution**: Tree ensemble Mean Decrease in Impurity (MDI / Gini) and regularized linear coefficients mapped to human-interpretable engineering features.
4. **Real MLflow Experiment Tracking & Governance**: Production model registration, parameter tracking, split-level metric evaluation, and deterministic champion promotion gates.
5. **Downstream Real-Time & Batch Prediction Serving**: Standardized Pydantic contract (`AssetPredictionEvent`) published to Kafka topic `asset-predictions`.

---

## High-Level System Architecture

```text
+---------------------------------------------------------------------------------------------------+
|                         HISTORICAL LAKEHOUSE & STREAMING FEATURE BRIDGE                           |
|  • 50 Multi-Asset Lifecycles across 5 equipment classes & 8 degradation scenarios                 |
|  • Replay through Phase 2 StreamingFeatureEngine, WindowAccumulator & AssetHealthModel           |
|  • Storage in Apache Iceberg / Parquet warehouse partitions (data/warehouse/...)                  |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
|                        LABEL ENGINEERING & TARGET LEAKAGE AUDIT (Step 2 & 10)                     |
|  • Piecewise Linear RUL Target: y_RUL(t) = min(T_max, max(0.0, T_fail - t))                       |
|  • Binary Failure Risk Target: y_fail(t) = 1 if (T_fail - t) <= H else 0                         |
|  • Target Leakage Audit Tool: Automated schema whitelist & correlation scan (Strict PASS/FAIL)    |
|  • Complete Run Splitting: 70% Train (35 runs), 15% Val (8 runs), 15% Test (7 runs)               |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
|                  APACHE SPARK MLLIB MODELING PIPELINE (LOCAL[*] MULTI-THREADED)                   |
|  • Feature Registry (25 features): Raw sensors, 30s rolling stats, slopes, health score, etc.     |
|  • Spark MLlib Pipeline: StringIndexer -> VectorAssembler -> StandardScaler                       |
|  • Class Imbalance Weighting: Computed strictly on Train split (w_0 = 0.5882, w_1 = 3.3333)       |
|                                                                                                   |
|  [Classification Models]                          [RUL Regression Models]                         |
|  • Baseline: Regularized Logistic Regression      • Baseline: Ridge/ElasticNet Linear Regression  |
|  • Champion: Random Forest Classifier             • Champion: Random Forest Regressor             |
+-------------------------------------------------+-------------------------------------------------+
                                                  |
                                                  v
+---------------------------------------------------------------------------------------------------+
|                      REAL MLFLOW TRACKING, GOVERNANCE & MODEL REGISTRY                            |
|  • Local MLflow Tracking (sqlite:///data/mlflow.db) with parameters, metrics, & artifacts         |
|  • Deterministic Champion Promotion Policy (PR-AUC >= 0.70, Recall >= 0.80, R^2 >= 0.35)          |
|  • Versioned Prediction Contract & Real-Time Serving Schema (AssetPredictionEvent)                |
+---------------------------------------------------------------------------------------------------+
```

---

## Distributed Execution & Technology Stack

| Layer | Technology | Version / Configuration | Purpose |
| :--- | :--- | :--- | :--- |
| **Distributed Engine** | Apache Spark (PySpark) | 3.5.0+ (`local[*]`, 3GB Driver) | Distributed MLlib pipeline transformation and training |
| **JVM Runtime** | OpenJDK Java 21 LTS | `--add-opens=java.base/java.lang=ALL-UNNAMED` | Modern JVM reflection support for Spark SQL / MLlib |
| **ML Tracking & Registry** | MLflow | 2.14.0+ (`sqlite:///data/mlflow.db`) | Experiment tracking, artifact storage, and governance |
| **Feature Extraction** | ForgeStream Stream Engine | Phase 2 `StreamingFeatureEngine` | 30s rolling stats, thermal rise rate, vibration slope |
| **Prediction Serving** | Pydantic v2 | 2.8.0+ | Contract validation and downstream Kafka streaming schema |
| **Storage & Data Lake** | Apache Iceberg / Parquet | PyIceberg + PyArrow zstd | Historical multi-asset trajectory persistence |
| **Evaluation & Math** | Scikit-Learn & NumPy | 1.5.0+ / 2.0+ | NASA PHM prognostic penalties, PR-AUC, confusion matrices |

---

## Component Modular Structure

1. **`forgestream.ml.config`**:
   - Centralized dataclasses (`MLDatasetConfig`, `SparkConfig`, `MLflowConfig`, `ModelGovernanceConfig`, `FeatureRegistryConfig`).
2. **`forgestream.ml.spark_session`**:
   - Singleton SparkSession builder with Java 21 JVM access flags and safe teardown hooks.
3. **`forgestream.ml.dataset_generator`**:
   - Historical trajectory replay generator integrating Phase 1 physics simulation with Phase 2 online feature extraction.
4. **`forgestream.ml.labeling`**:
   - Mathematical formulations for piecewise linear RUL and horizon-bounded binary failure risk.
5. **`forgestream.ml.splits`**:
   - Complete asset run partitioning (`split_by_complete_runs`) and training partition class weight computation.
6. **`forgestream.ml.leakage_auditor`**:
   - Automated whitelist scanner, temporal ordering verifier, and correlation checker.
7. **`forgestream.ml.feature_pipeline`**:
   - Spark MLlib pipeline stages (`StringIndexer`, `VectorAssembler`, `StandardScaler`).
8. **`forgestream.ml.models.classification` & `regression`**:
   - Baseline and tree ensemble models for classification and regression tasks.
9. **`forgestream.ml.evaluation`**:
   - Operational evaluation metrics (`metrics.py`), explainability attributions (`explainability.py`), and cross-asset slicing (`cross_asset.py`).
10. **`forgestream.ml.tracking` & `governance`**:
    - MLflow experiment logger and deterministic promotion gatekeeper.
11. **`forgestream.ml.serving`**:
    - Downstream contract definitions (`contract.py`) and scoring engine (`inference.py`).
