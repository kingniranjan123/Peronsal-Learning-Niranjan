# 🔥 Master Class: Linear Regression in Apache Spark

## Overview

Linear Regression is the foundational supervised learning algorithm that models a continuous response variable as a weighted linear combination of input features. In a single-machine context, the Ordinary Least Squares (OLS) solution is found analytically by inverting the Gram matrix `(XᵀX)⁻¹Xᵀy`. At scale, this matrix inversion is intractable — a dataset with `p` features produces a `p×p` matrix whose dense inversion costs `O(p³)` FLOPs and requires the entire design matrix to reside in driver memory. Spark's `LinearRegression` estimator solves this by distributing gradient computation across executors, materializing only compact sufficient statistics on the driver, and delegating the final parameter update to a high-performance numerical optimizer.

Spark's ML implementation (`org.apache.spark.ml.regression.LinearRegression`) supports three solving strategies: L-BFGS (default), normal equation (exact OLS for moderate `p`), and auto-selection. It natively supports L1 (Lasso), L2 (Ridge), and ElasticNet regularization, and computes a full suite of diagnostics — RMSE, MAE, R², and residual standard error — without a second pass over the data. Understanding *why* each solver exists, what it costs, and when it breaks is the difference between a practitioner who fits models and an engineer who ships reliable ML pipelines.

---

## 🏗️ Architectural Deep Dive

### How It Works Under the Hood

When `LinearRegression.fit()` is called, Spark first invokes the **Catalyst analyzer** to resolve column references in the `featuresCol` and `labelCol` inputs. The physical plan then emits a single `mapPartitions` stage where each executor independently computes its local gradient contribution and local sufficient statistics (`XᵀX` partition shard and `Xᵀy`). These are aggregated via a **treeAggregate** — a log-depth reduction tree that avoids funneling all gradient vectors to the driver in a flat reduce, cutting network overhead from `O(n)` to `O(log n)` messages.

When the **normal equation solver** is selected (`solver = "normal"`), the driver collects the aggregated `XᵀX` (a `p×p` dense matrix) and `Xᵀy` (a `p`-vector), then solves the system using LAPACK's `dposv` (Cholesky factorization) via Breeze. This is exact OLS in `O(p³)` time and is feasible only for low-to-moderate feature counts (Spark internally refuses this path when `p > 4096` or when regularization is L1). When **L-BFGS** is used, the driver holds a compact history of `m` (typically 10) curvature pairs `{sₖ, yₖ}` and approximates the inverse Hessian without ever materializing it, converging in `O(m·p)` work per iteration. Each L-BFGS iteration triggers one Spark job to recompute the gradient on the full dataset.

**Tungsten** accelerates the executor-side gradient computation via Whole-Stage Codegen: Spark fuses the `VectorAssembler` transformation, the dot product `wᵀxᵢ`, the residual `(wᵀxᵢ - yᵢ)`, and the gradient accumulation `xᵢ · residualᵢ` into a single tight JVM loop with no intermediate object allocation. Feature vectors are stored in **Tungsten's binary off-heap format** (UnsafeRow), eliminating Java object header overhead and GC pressure during the inner loop. The `StandardScaler` applied internally (when `standardization = true`) is fused into this same loop.

```
Driver JVM                              Executor JVM (×N)
┌──────────────────────────────┐        ┌──────────────────────────────────┐
│  LinearRegression.fit()      │        │  Partition [0..k]                │
│  ┌──────────────────────┐    │        │  ┌────────────────────────────┐  │
│  │ Catalyst Analyzer    │    │        │  │ mapPartitions (Tungsten)   │  │
│  │ (resolve cols, types)│    │        │  │  for row in partition:     │  │
│  └──────────┬───────────┘    │        │  │    pred = w · x_i          │  │
│             │                │        │  │    residual = pred - y_i   │  │
│  ┌──────────▼───────────┐    │  RDD   │  │    grad += x_i * residual  │  │
│  │  Physical Plan       │────┼───────▶│  │    XtX_local += x_i ⊗ x_i │  │
│  │  (mapPartitions job) │    │        │  └────────────┬───────────────┘  │
│  └──────────┬───────────┘    │        └───────────────┼──────────────────┘
│             │                │                        │ treeAggregate
│  ┌──────────▼───────────┐    │◀───────────────────────┘ (log-depth reduce)
│  │ Aggregated Gradient  │    │
│  │ g = Σ xᵢ(wᵀxᵢ - yᵢ) │    │
│  └──────────┬───────────┘    │
│             │                │
│  ┌──────────▼───────────┐    │
│  │  L-BFGS / Normal Eq  │    │
│  │  (Breeze / LAPACK)   │    │
│  │  w* = argmin L(w)    │    │
│  └──────────────────────┘    │
└──────────────────────────────┘
```

### Key Internal Components

- **`InstanceWeight` accumulator:** Each training row carries an optional `weightCol` value. Spark accumulates `Σwᵢ`, `Σwᵢ·yᵢ`, and `Σwᵢ·yᵢ²` alongside the gradient, enabling weighted OLS in a single pass without replication.
- **`MultivariateOnlineSummarizer`:** Computes per-feature mean and variance in a single streaming pass using Welford's numerically stable algorithm. The resulting statistics drive internal standardization and are surfaced in `LinearRegressionTrainingSummary`.
- **Breeze L-BFGS (`breeze.optimize.LBFGS`):** Holds `m` correction pairs on the driver. Each `valueAndGradient` call triggers a full Spark job over the dataset. Convergence is declared when `‖g‖ < tol` or `maxIter` is reached.
- **`LinearRegressionSummary`:** Computed on the training set post-fit via a single additional Spark job. Contains coefficient standard errors (via the diagonal of `(XᵀX)⁻¹σ²`), t-statistics, p-values, R², adjusted R², RMSE, and residuals as a `DataFrame`.

---

## ⚠️ Critical Concepts & Common Pitfalls

### Feature Collinearity and the Singular Gram Matrix

When two or more features are perfectly or near-perfectly linearly dependent, `XᵀX` becomes singular or nearly singular. In the normal equation path, LAPACK's Cholesky factorization will detect this and throw a `SingularMatrixException` (surfaced as a Spark `SparkException` wrapping a Breeze error). In the L-BFGS path, collinearity manifests more subtly: the loss surface becomes a flat ridge, the gradient norm never decreases below tolerance, and training hits `maxIter` without converging. The coefficients returned will be numerically arbitrary combinations of the collinear directions — individually meaningless but collectively predicting correctly.

The correct remediation is **not** simply adding more L2 regularization (though Ridge does stabilize `XᵀX` by adding `λI`). The correct approach is to detect collinearity via the Variance Inflation Factor (VIF) — any VIF above 10 signals a problem — remove redundant features, or use PCA to project into an orthogonal basis before fitting. Collinearity does not bias predictions but destroys the interpretability of individual coefficients and inflates their standard errors, making hypothesis tests unreliable.

### The `maxIter` Convergence Trap and Learning Rate Sensitivity

L-BFGS is a second-order method and does not have a user-controlled learning rate — it computes its own step size via Wolfe-condition line search. However, it is sensitive to the *scale* of features: if feature `x₁` ranges in `[0, 1]` and feature `x₂` ranges in `[0, 10⁶]`, the loss surface is a narrow ellipsoid and L-BFGS wastes iterations chasing curvature. Spark's internal standardization (`standardization = true`, the default) mitigates this by standardizing all features to zero mean and unit variance before optimization, then un-standardizing the final coefficients. Disabling `standardization` on heterogeneously-scaled features is a common anti-pattern that increases required iterations by 10–100× and can prevent convergence entirely within the default `maxIter = 100`.

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|-----------|---------|-------|
| Gradient computation (per iter) | O(n·p) | No | Pure mapPartitions, no shuffle; Tungsten codegen applies |
| treeAggregate of gradient | O(p·log N) | Yes (partial) | Log-depth reduce tree; `p` doubles = 2× network cost |
| Normal equation solve | O(p³) driver | No | LAPACK Cholesky; infeasible for p > ~4000 |
| L-BFGS update (per iter) | O(m·p) driver | No | m=10 correction pairs default; entirely in driver memory |
| `LinearRegressionSummary` | O(n·p) | No | One additional Spark job post-fit for residuals/diagnostics |
| VIF computation (manual) | O(p²·n) | No | Requires fitting p auxiliary regressions; expensive at scale |

---

## 💻 Code Examples

### Example 1: End-to-End Pipeline with Solver Selection and Diagnostic Summary

> **What this demonstrates:** How Spark internally routes between solvers, how to extract the full coefficient table, and why `standardization` is left enabled.

```python
from pyspark.sql import SparkSession
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml import Pipeline
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("lr-masterclass").getOrCreate()

# Load a dataset with heterogeneous feature scales.
# The CSV has: age (0-80), income (0-200000), credit_score (300-850) → loan_amount (target)
raw = spark.read.option("header", True).option("inferSchema", True) \
    .csv("dbfs:/data/loan_applications.csv")

feature_cols = ["age", "income", "credit_score"]

# VectorAssembler packs dense features into a single DenseVector stored
# in Tungsten binary format — no JVM object overhead during training.
assembler = VectorAssembler(inputCols=feature_cols, outputCol="raw_features")

# LinearRegression with standardization=True (default): Spark internally
# applies zero-mean/unit-variance scaling before L-BFGS, then
# back-transforms coefficients. This is critical here because 'income'
# is 1000× the scale of 'age', which would otherwise slow convergence severely.
lr = LinearRegression(
    featuresCol="raw_features",
    labelCol="loan_amount",
    maxIter=100,               # L-BFGS iteration budget
    regParam=0.01,             # λ for ElasticNet (L2 here since elasticNetParam=0)
    elasticNetParam=0.0,       # 0 = Ridge, 1 = Lasso, in-between = ElasticNet
    standardization=True,      # ALWAYS leave True unless features are pre-standardized
    solver="auto",             # Spark picks: normal eq if p<=4096 and no L1; else L-BFGS
    fitIntercept=True
)

pipeline = Pipeline(stages=[assembler, lr])
model = pipeline.fit(raw)

lr_model = model.stages[-1]  # Extract the fitted LinearRegressionModel

# Print the coefficient vector aligned to feature names — these are the
# un-standardized (original-scale) coefficients returned after back-transformation.
print("Coefficients:", dict(zip(feature_cols, lr_model.coefficients.toArray())))
print("Intercept:   ", lr_model.intercept)

# The training summary triggers one extra Spark job to compute residuals,
# RMSE, R², and per-coefficient standard errors via (XᵀX)⁻¹σ².
summary = lr_model.summary
print(f"RMSE:        {summary.rootMeanSquaredError:.4f}")
print(f"R²:          {summary.r2:.4f}")
print(f"Adj R²:      {summary.r2adj:.4f}")
print(f"Iterations:  {summary.totalIterations}")  # How many L-BFGS steps were needed

# Coefficient p-values — only valid when solver uses normal equation (no L1 reg).
# If solver was L-BFGS without normal equation, p-values approximate via asymptotic theory.
for feat, coef, se, t, p in zip(
    feature_cols,
    lr_model.coefficients,
    summary.coefficientStandardErrors[:-1],  # last entry is intercept SE
    summary.tValues[:-1],
    summary.pValues[:-1]
):
    print(f"  {feat:15s}  coef={coef:10.4f}  SE={se:.4f}  t={t:.3f}  p={p:.4f}")
```

> **Mastery Note:** The `solver="auto"` selection is non-trivial: Spark checks `p ≤ 4096` AND `elasticNetParam == 0.0` before allowing the normal equation path, because L1 regularization produces a non-differentiable objective that LAPACK cannot handle. When the normal equation is used, `(XᵀX)⁻¹` is retained in the model object and used directly to compute coefficient standard errors — no asymptotic approximation needed. When L-BFGS is used, Spark computes standard errors from the inverse-Hessian approximation, which is less accurate, especially with small datasets. The `summary.totalIterations` field is your convergence health check: if it equals `maxIter`, the solver did not converge and your coefficients are unreliable — increase `maxIter` or check feature scaling.

---

### Example 2: Detecting and Remediating Feature Collinearity via VIF

> **What this demonstrates:** How collinearity silently corrupts L-BFGS convergence and coefficients, and how to compute VIF in a distributed Spark context.

```python
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("vif-collinearity").getOrCreate()

df = spark.read.parquet("dbfs:/data/housing_features.parquet")
# Suppose: total_sqft, living_sqft, lot_sqft, num_rooms → price
# 'total_sqft' ≈ 'living_sqft' + 'lot_sqft': near-perfect collinearity expected.
feature_cols = ["total_sqft", "living_sqft", "lot_sqft", "num_rooms"]

def compute_vif(df, feature_cols):
    """
    VIF for feature j = 1 / (1 - R²_j), where R²_j is the R² of regressing
    feature j on all other features. High VIF (>10) signals problematic collinearity.
    We distribute work by fitting p separate LinearRegression models.
    """
    vifs = {}
    for target_feat in feature_cols:
        # Predict this feature from all others — measures how redundant it is.
        other_feats = [f for f in feature_cols if f != target_feat]
        assembler = VectorAssembler(inputCols=other_feats, outputCol="vif_features")
        assembled = assembler.transform(df)

        # Ridge regression (small λ) to handle potential sub-collinearity in auxiliary fit.
        lr = LinearRegression(
            featuresCol="vif_features",
            labelCol=target_feat,
            regParam=1e-6,           # near-zero regularization to approximate OLS
            standardization=True,
            maxIter=100
        )
        aux_model = lr.fit(assembled)
        r2 = aux_model.summary.r2

        # R² can be slightly negative with near-zero regularization on small data;
        # clamp to [0, 1] to avoid division by near-zero.
        r2 = max(0.0, min(r2, 1.0 - 1e-9))
        vif = 1.0 / (1.0 - r2)
        vifs[target_feat] = vif
        print(f"  VIF({target_feat}) = {vif:.2f}  (R²={r2:.4f})")

    return vifs

print("=== Variance Inflation Factors ===")
vifs = compute_vif(df, feature_cols)

# Identify and drop features with VIF > 10 (rule of thumb threshold).
safe_features = [f for f, v in vifs.items() if v <= 10.0]
dropped       = [f for f, v in vifs.items() if v > 10.0]
print(f"\nDropping collinear features: {dropped}")
print(f"Retaining: {safe_features}")

# Re-fit with only safe features — coefficients are now interpretable.
assembler_clean = VectorAssembler(inputCols=safe_features, outputCol="features_clean")
df_clean = assembler_clean.transform(df)

lr_final = LinearRegression(
    featuresCol="features_clean",
    labelCol="price",
    regParam=0.01,
    standardization=True
)
model_final = lr_final.fit(df_clean)
print(f"\nPost-VIF model R²: {model_final.summary.r2:.4f}")
print(f"Post-VIF RMSE:     {model_final.summary.rootMeanSquaredError:.2f}")
```

> **Mastery Note:** Each VIF auxiliary regression triggers a full Spark job — fitting VIF for `p` features costs `p` Spark jobs, which is acceptable for `p < 100` but prohibitive for `p > 1000`. At high dimensionality, prefer computing the **condition number** of `XᵀX` instead: condition number > 1000 is a reliable collinearity signal achievable in a single Spark aggregation without iterative solvers. Note also that Ridge regularization (`elasticNetParam=0, regParam=λ`) mathematically stabilizes `XᵀX` by adding `λI`, converting `(XᵀX + λI)⁻¹` which is always invertible — making Ridge coefficients defined even under perfect collinearity, though they will be biased toward zero.

---

### Example 3: ElasticNet Regularization for Automatic Feature Selection (Lasso Path)

> **What this demonstrates:** How `elasticNetParam=1.0` (Lasso) forces L-BFGS onto the proximal gradient path, driving coefficients to exactly zero for irrelevant features, performing embedded feature selection.

```python
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lasso-path").getOrCreate()

df = spark.read.parquet("dbfs:/data/marketing_features.parquet")
# 50 features, many irrelevant. Lasso will zero them out automatically.
feature_cols = df.columns[:-1]   # all columns except last (target)
target_col   = df.columns[-1]

assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
df_assembled = assembler.transform(df).select("features", target_col)

# ElasticNet with elasticNetParam=1.0 is pure Lasso.
# Lasso adds an L1 penalty λ·‖w‖₁, making the objective non-smooth.
# Spark uses coordinate descent (proximal gradient) internally when elasticNetParam > 0,
# NOT L-BFGS. The solver choice is automatic — 'normal' equation is rejected.
lr_lasso = LinearRegression(
    featuresCol="features",
    labelCol=target_col,
    elasticNetParam=1.0,       # 1.0 = pure Lasso; forces proximal gradient / coordinate descent
    standardization=True,
    maxIter=300,               # Coordinate descent may need more iterations than L-BFGS
    tol=1e-6
)

# Sweep λ values (regParam) to find the optimal sparsity-accuracy tradeoff.
param_grid = ParamGridBuilder() \
    .addGrid(lr_lasso.regParam, [0.001, 0.01, 0.05, 0.1, 0.5]) \
    .build()

evaluator = RegressionEvaluator(
    labelCol=target_col,
    predictionCol="prediction",
    metricName="rmse"     # Cross-validate on RMSE to pick optimal λ
)

cv = CrossValidator(
    estimator=lr_lasso,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=5,           # 5-fold CV: 5 Spark jobs per λ value = 25 total training jobs
    parallelism=4         # Run up to 4 parameter combinations simultaneously
)

cv_model = cv.fit(df_assembled)
best_lr   = cv_model.bestModel

# Count how many features Lasso zeroed out completely.
coefs = best_lr.coefficients.toArray()
nonzero = (coefs != 0.0).sum()
zeroed  = (coefs == 0.0).sum()
print(f"Best λ:          {best_lr.getRegParam()}")
print(f"Non-zero coefs:  {nonzero} / {len(coefs)}  ({zeroed} features zeroed by Lasso)")
print(f"Best CV RMSE:    {min(cv_model.avgMetrics):.4f}")

# The surviving non-zero features are exactly what Lasso selected as relevant.
selected = [feature_cols[i] for i, c in enumerate(coefs) if c != 0.0]
print(f"Selected features: {selected}")
```

> **Mastery Note:** When `elasticNetParam > 0`, Spark silently switches from L-BFGS to **iterative reweighted least squares with a proximal operator** (essentially coordinate descent), because L-BFGS requires smooth objectives. This is an internal implementation detail not documented in the public API — you will never see L-BFGS used when L1 is active, regardless of what `solver` you specify. The `parallelism` parameter in `CrossValidator` controls how many Spark `fit()` jobs run concurrently on the cluster; setting it too high causes resource contention between concurrent jobs. With 5 λ values and 5 folds, `parallelism=4` batches the 25 required fits into 7 parallel rounds, reducing wall-clock time by ~3× versus `parallelism=1`.

---

### Example 4: Diagnosing Convergence Failure and RMSE Degradation via Training Summary Metrics

> **What this demonstrates:** How to programmatically detect non-convergence, inspect per-iteration objective values, and diagnose whether model quality degradation is due to solver failure or genuine underfitting.

```python
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("convergence-diagnostics") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
    .getOrCreate()

df_train = spark.read.parquet("dbfs:/data/sensor_readings_train.parquet")
df_test  = spark.read.parquet("dbfs:/data/sensor_readings_test.parquet")

feature_cols = [c for c in df_train.columns if c.startswith("sensor_")]
assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
train_assembled = assembler.transform(df_train)
test_assembled  = assembler.transform(df_test)

# Deliberately set maxIter very low to simulate a non-converged model.
lr_bad = LinearRegression(
    featuresCol="features",
    labelCol="reading",
    maxIter=5,             # Far too few iterations for 200+ sensor features
    regParam=0.0,          # No regularization — makes convergence harder on collinear sensors
    standardization=True,
    solver="l-bfgs"        # Force L-BFGS explicitly to inspect iteration behavior
)
model_bad = lr_bad.fit(train_assembled)

# CONVERGENCE HEALTH CHECK:
# If totalIterations == maxIter, the solver hit the budget without converging.
# The returned weights are mid-optimization state — NOT the true minimum.
summary_bad = model_bad.summary
if summary_bad.totalIterations >= lr_bad.getMaxIter():
    print(f"⚠️  NON-CONVERGENCE DETECTED: hit maxIter={lr_bad.getMaxIter()} iterations")
    print(f"   Final gradient norm (objectiveHistory last delta): "
          f"{abs(summary_bad.objectiveHistory[-1] - summary_bad.objectiveHistory[-2]):.6f}")
else:
    print(f"✅ Converged in {summary_bad.totalIterations} iterations")

# objectiveHistory is the loss value at each L-BFGS iteration.
# A properly converging model shows monotonically decreasing values.
print("\nObjective history (should be decreasing):")
for i, loss in enumerate(summary_bad.objectiveHistory):
    print(f"  iter {i:3d}: loss = {loss:.6f}")

# Evaluate on held-out test set to detect train/test RMSE discrepancy.
# Large gap = overfitting (use regularization). Both large = underfitting.
from pyspark.ml.evaluation import RegressionEvaluator

evaluator_rmse = RegressionEvaluator(labelCol="reading", metricName="rmse")
evaluator_r2   = RegressionEvaluator(labelCol="reading", metricName="r2")

preds_bad  = model_bad.transform(test_assembled)
test_rmse  = evaluator_rmse.evaluate(preds_bad)
test_r2    = evaluator_r2.evaluate(preds_bad)
train_rmse = summary_bad.rootMeanSquaredError
train_r2   = summary_bad.r2

print(f"\nTrain RMSE: {train_rmse:.4f}  |  Test RMSE: {test_rmse:.4f}  "
      f"(gap={test_rmse - train_rmse:.4f})")
print(f"Train R²:   {train_r2:.4f}  |  Test R²:   {test_r2:.4f}")

# Now re-fit with sufficient iterations and add Ridge regularization
lr_good = LinearRegression(
    featuresCol="features",
    labelCol="reading",
    maxIter=500,           # Enough budget for 200 features under L-BFGS
    regParam=0.1,          # Ridge regularization: stabilizes XᵀX, reduces variance
    elasticNetParam=0.0,
    standardization=True,
    solver="l-bfgs",
    tol=1e-8               # Tight convergence tolerance
)
model_good = lr_good.fit(train_assembled)
summary_good = model_good.summary

preds_good     = model_good.transform(test_assembled)
test_rmse_good = evaluator_rmse.evaluate(preds_good)
test_r2_good   = evaluator_r2.evaluate(preds_good)

print(f"\n--- After fix (maxIter=500, regParam=0.1) ---")
print(f"Converged in: {summary_good.totalIterations} iterations")
print(f"Train RMSE:  {summary_good.rootMeanSquaredError:.4f}  |  Test RMSE: {test_rmse_good:.4f}")
print(f"Train R²:    {summary_good.r2:.4f}  |  Test R²:   {test_r2_good:.4f}")
```

> **Mastery Note:** The `objectiveHistory` list is populated by L-BFGS's loss evaluation at each iteration and is the most direct window into solver behavior available from the Spark API. A non-monotone `objectiveHistory` (loss increasing between iterations) indicates numerical issues — typically caused by features with extremely large magnitude that overflow the line search bracket; the fix is to enable `standardization` or pre-scale features to `[-1, 1]`. The gap between train RMSE and test RMSE is your bias-variance signal: a gap exceeding 20% of train RMSE typically indicates overfitting, and increasing `regParam` (strengthening Ridge/ElasticNet) is the correct lever. The `r2adj` metric in `summary` penalizes model complexity by `(n-1)/(n-p-1)` — when adding features *decreases* `r2adj`, those features add noise, not signal.

---

## 🎯 Mastery Checklist

To achieve true mastery of Linear Regression in Spark:
- [ ] Understand when Spark selects the normal equation vs. L-BFGS solver, and why L1 regularization prohibits the normal equation path
- [ ] Know how `treeAggregate` reduces gradient communication from `O(N)` to `O(p·log N)` and why this matters at 1000+ executor scale
- [ ] Know when `standardization=True` is critical (always with heterogeneous feature scales) and what the internal back-transformation does to reported coefficients
- [ ] Be able to diagnose non-convergence from `summary.totalIterations == maxIter` and `objectiveHistory` flatness in the Spark UI
- [ ] Understand the tradeoff between Ridge (reduces variance, keeps all features), Lasso (exact zeros, embedded selection), and ElasticNet (combines both)
- [ ] Know how collinearity manifests differently under L-BFGS (no convergence) vs. normal equation (LAPACK exception), and how VIF detects it pre-fit
- [ ] Be able to compute and interpret `r2adj` to distinguish genuine model improvement from feature overfitting
- [ ] Understand how `parallelism` in `CrossValidator` interacts with cluster resource allocation when sweeping `regParam`

---

## 📚 Summary

Spark's `LinearRegression` is not a simple least-squares fitter — it is a carefully orchestrated distributed optimization system. The Catalyst analyzer resolves the feature schema; Tungsten's Whole-Stage Codegen fuses gradient computation into a tight, allocation-free JVM loop over UnsafeRow binary data; `treeAggregate` ships gradient vectors up a logarithmic reduction tree rather than flooding the driver; and Breeze's L-BFGS or LAPACK's Cholesky solver closes the loop on the driver with compact, heap-resident state. Each design choice exists because the naïve centralized alternative — shipping all data to the driver for matrix inversion — fails catastrophically beyond a few GB.

The most consequential engineering decisions when deploying `LinearRegression` at production scale are regularization strategy and feature preprocessing. Failing to standardize features is the single most common cause of convergence failure, increasing required L-BFGS iterations by orders of magnitude. Feature collinearity silently corrupts coefficient interpretability and, in the normal equation path, causes hard numerical failures. Proper VIF analysis pre-fit and Ridge regularization post-detection are the correct mitigations. The `LinearRegressionSummary` object — `objectiveHistory`, `totalIterations`, `r2adj`, and coefficient p-values — is your complete diagnostic toolkit.

Mastery of Spark's `LinearRegression` means knowing not just the API but *when the API lies to you*: when convergence appears achieved but the model is at a saddle point, when R² is high but collinear coefficients are nonsense, when Lasso's sparse output is a sign of correct regularization versus over-penalization. These distinctions, rooted in the numerical linear algebra and distributed systems mechanics described in this chapter, are what separate production-grade ML engineers from practitioners who tune hyperparameters by intuition alone.
