# ForgeStream Phase 3: MLflow Tracking & Deterministic Model Governance

## Overview
Phase 3 integrates **MLflow Tracking & Model Registry** to provide end-to-end experiment lineage, reproducible hyperparameter tracking, cross-split metric logging, and deterministic production promotion gates.

---

## 1. MLflow Tracking Architecture & Configuration

- **Tracking URI**: `sqlite:///data/mlflow.db` (Lightweight ACID SQL tracking backend)
- **Artifact Root**: `data/mlruns_artifacts/`
- **Experiment Name**: `forgestream-phase3-predictive-maintenance`
- **Framework**: Apache Spark MLlib + MLflow Tracking API

### Logged Parameters & Metadata
For each model run, the following elements are atomically logged:
1. **Hyperparameters**: `num_trees`, `max_depth`, `seed`, `weight_col`, `reg_param`, `elastic_net_param`, `subsampling_rate`.
2. **Metrics across Splits**:
   - Training Partition (`train_pr_auc`, `train_recall`, `train_r2`, `train_rmse`, etc.)
   - Validation Partition (`val_pr_auc`, `val_recall`, `val_r2`, `val_rmse`, etc.)
   - Held-Out Test Partition (`test_pr_auc`, `test_recall`, `test_r2`, `test_rmse`, etc.)
3. **Artifacts**:
   - `*_feature_attributions.json`: Ranked engineering feature importances.
   - `*_meta.json`: Serialized Spark MLlib pipeline stages and schema signatures.
   - High-resolution evaluation charts (`results/figures/*.png`).

---

## 2. Deterministic Model Governance & Promotion Policy

ForgeStream implements a strict automated gatekeeper (`ModelGovernanceEngine`) that prevents sub-par or leaking models from reaching production serving:

### Promotion Gates & Thresholds
| Task | Evaluation Criterion | Production Threshold | Champion Result | Gate Status |
| :--- | :--- | :---: | :---: | :---: |
| **Classification** | Precision-Recall AUC (PR-AUC) | $\ge 0.70$ | **$0.7666$** | **PASS** |
| **Classification** | True Positive Recall | $\ge 0.80$ | **$0.8512$** | **PASS** |
| **Classification** | ROC-AUC | $\ge 0.75$ | **$0.8491$** | **PASS** |
| **Classification** | Target Leakage Audit | Strict `PASS` | `PASS` | **PASS** |
| **Regression** | Coefficient of Determination ($R^2$) | $\ge 0.35$ | **$0.3980$** | **PASS** |
| **Regression** | RMSE to $T_{\text{max}}$ Ratio | $\le 0.30$ | **$0.2562$** ($30.74\text{h}$) | **PASS** |
| **Regression** | Accuracy within $\pm25\%$ ($30\text{h}$) | $\ge 0.50$ | **$0.5355$** | **PASS** |
| **Regression** | Target Leakage Audit | Strict `PASS` | `PASS` | **PASS** |

### Promotion Decisions
- **Classification Champion**: Promoted `rf_classifier_champion` $\to$ Tag: `v3.0.0-champion`.
- **Classification Baseline**: Demoted `lr_classifier_baseline` $\to$ Tag: `v3.0.0-candidate`.
- **Regression Champion**: Promoted `rf_regressor_champion` $\to$ Tag: `v3.0.0-champion`.
- **Regression Baseline**: Demoted `lr_regressor_baseline` $\to$ Tag: `v3.0.0-candidate`.

Governance Audit Artifact: **`results/phase3_model_governance.json`** & **`results/phase3_mlflow_tracking_audit.json`**.
