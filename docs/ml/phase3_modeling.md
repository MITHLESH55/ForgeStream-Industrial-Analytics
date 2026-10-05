# ForgeStream Phase 3: Apache Spark MLlib Modeling, Pipelines & Class Imbalance

## Overview
ForgeStream trains and benchmarks four models using **Apache Spark MLlib (local[*] multi-threaded execution)**:
1. **Baseline Classification**: Regularized Logistic Regression with ElasticNet penalty.
2. **Champion Classification**: Random Forest Classifier with tree ensemble feature importances (MDI).
3. **Baseline RUL Regression**: Regularized Linear Regression with ElasticNet penalty.
4. **Champion RUL Regression**: Random Forest Regressor for continuous piecewise linear prognostics.

---

## 1. Class Imbalance Mitigation Strategy

Industrial telemetry datasets are characterized by extreme class imbalance: healthy operating conditions represent $>85\%$ of lifecycle records, while imminent failure states ($y_{\text{fail}} = 1$) constitute $<15\%$.

### Strict Partition-Isolated Class Weight Formulation
To prevent data leakage, class weights are calculated **strictly on the Training partition** ($D_{\text{train}}$) and applied as sample weights during Spark MLlib loss optimization:

$$w_k = \frac{N_{\text{train}}}{2 \cdot N_k}$$

Where:
- $N_{\text{train}} = 21,000$ (35 complete asset runs $\times$ 600 steps).
- Negative class samples ($N_0$): $17,850$ ($85.0\%$).
- Positive class samples ($N_1$): $3,150$ ($15.0\%$).
- Computed weights:
  - $w_0 = \frac{21000}{2 \times 17850} = 0.5882$
  - $w_1 = \frac{21000}{2 \times 3150} = 3.3333$

In Spark MLlib, these weights are attached via `setWeightCol("sample_weight")`, ensuring the objective function penalizes false negatives $5.67\times$ more heavily than false positives.

---

## 2. Distributed Model Hyperparameters & Implementations

### Binary Classification Models
- **Logistic Regression (Baseline)**:
  - Algorithm: `pyspark.ml.classification.LogisticRegression`
  - Regularization parameter ($\lambda$): $0.01$
  - ElasticNet mixing ($\alpha$): $0.5$ (L1/L2 balanced penalty)
  - Max iterations: $100$
  - Weight column: `sample_weight`
- **Random Forest Classifier (Champion)**:
  - Algorithm: `pyspark.ml.classification.RandomForestClassifier`
  - Trees ($B$): $50$
  - Max Depth: $8$
  - Subsampling rate: $1.0$
  - Feature subset strategy: `"auto"` ($\sqrt{d}$)
  - Seed: $42$ (Deterministic reproducibility)
  - Weight column: `sample_weight`

### Remaining Useful Life (RUL) Regression Models
- **Linear Regression (Baseline)**:
  - Algorithm: `pyspark.ml.regression.LinearRegression`
  - Regularization parameter ($\lambda$): $0.01$
  - ElasticNet mixing ($\alpha$): $0.5$
  - Max iterations: $100$
- **Random Forest Regressor (Champion)**:
  - Algorithm: `pyspark.ml.regression.RandomForestRegressor`
  - Trees ($B$): $50$
  - Max Depth: $8$
  - Feature subset strategy: `"auto"` ($d / 3$)
  - Seed: $42$

---

## 3. Empirical Results & Performance Comparison on Unseen Test Partition

Evaluating on 7 completely held-out asset lifecycle runs (4,200 records):

### Binary Classification Comparison
| Metric | Baseline Logistic Regression | Champion Random Forest Classifier | Delta ($\Delta$) |
| :--- | :---: | :---: | :---: |
| **PR-AUC (Primary)** | $0.6698$ | **$0.7666$** | $+14.5\%$ |
| **Recall (Safety-Critical)** | $0.8041$ | **$0.8512$** | $+5.8\%$ |
| **ROC-AUC** | $0.8033$ | **$0.8491$** | $+5.7\%$ |
| **Precision** | $0.7820$ | **$0.8250$** | $+5.5\%$ |
| **F1-Score** | $0.7929$ | **$0.8379$** | $+5.7\%$ |
| **F2-Score** | $0.7996$ | **$0.8458$** | $+5.8\%$ |
| **False Alarm Rate (FAR)** | $0.0381$ | **$0.0307$** | $-19.4\%$ |
| **Missed Detection Rate (MDR)**| $0.1959$ | **$0.1488$** | $-24.0\%$ |

### RUL Continuous Regression Comparison
| Metric | Baseline Linear Regression | Champion Random Forest Regressor | Delta ($\Delta$) |
| :--- | :---: | :---: | :---: |
| **$R^2$ Score (Primary)** | $0.3012$ | **$0.3980$** | $+32.1\%$ |
| **RMSE (Hours)** | $33.12\text{h}$ | **$30.74\text{h}$** | $-7.2\%$ |
| **MAE (Hours)** | $26.45\text{h}$ | **$23.18\text{h}$** | $-12.4\%$ |
| **Accuracy within $\pm10\%$** | $31.20\%$ | **$35.10\%$** | $+12.5\%$ |
| **Accuracy within $\pm25\%$** | $48.30\%$ | **$53.55\%$** | $+10.9\%$ |
| **RMSE-to-Cap Ratio** | $0.2760$ | **$0.2562$** | $-7.2\%$ |

Machine-Readable Evidence: **`results/phase3_classification_comparison.json`** & **`results/phase3_regression_comparison.json`**.
