# 🔥 Master Class: Logistic Regression in Apache Spark

## Overview
<div style='text-align: right; margin-top: -10px; margin-bottom: 20px; font-size: 0.85rem; color: #a0aec0;'><em>References: [Ref: 451](spark_book.pdf#page=451) [Ref: 456](spark_book.pdf#page=456) [Ref: 459](spark_book.pdf#page=459) [Ref: 463](spark_book.pdf#page=463) [Ref: 452](spark_book.pdf#page=452) [Ref: 457](spark_book.pdf#page=457) [Ref: 461](spark_book.pdf#page=461) [Ref: 464](spark_book.pdf#page=464) [Ref: 455](spark_book.pdf#page=455) [Ref: 458](spark_book.pdf#page=458) [Ref: 462](spark_book.pdf#page=462) [Ref: 469](spark_book.pdf#page=469)</em></div>

Logistic Regression is the workhorse of probabilistic classification in production ML systems. Unlike linear regression, which predicts continuous values, logistic regression maps a linear combination of features to a probability in the range (0, 1) using the **sigmoid function** (binary case) or the **softmax function** (multinomial case). Spark MLlib implements logistic regression as a distributed, in-memory optimizer that exploits the cluster's full parallelism, making it suitable for datasets with billions of rows and millions of sparse features — a regime where single-node scikit-learn simply fails.

Logistic regression exists in Spark's MLlib because the gradient of the logistic loss function is embarrassingly parallel across training samples. Each worker can compute its local gradient shard independently, and the driver aggregates them into a global gradient update — a pattern that maps perfectly onto Spark's RDD/DataFrame partitioning model. The result is a scalable, numerically stable, regularized classifier that Catalyst can optimize end-to-end through its physical planning phase.

The implementation lives in `org.apache.spark.ml.classification.LogisticRegression` and supports L1, L2, and ElasticNet regularization, threshold tuning, probability calibration, and per-class weighting — all on top of Spark's Tungsten binary memory format. Understanding these internals separates engineers who *use* logistic regression from those who *master* it. 

---

## 🏗️ Architectural Deep Dive 

### How It Works Under the Hood

When you call `LogisticRegression.fit(dataset)`, Spark does not immediately start iterating. First, the Catalyst optimizer's **Analysis phase** resolves the input schema — confirming that `featuresCol` is a `VectorUDT` and `labelCol` is numeric. The **Logical Optimization phase** then applies predicate pushdown and projection pruning to minimize the volume of data read from disk. If the features column originates from a Parquet file, Catalyst will push column selection into the Parquet reader, leveraging dictionary encoding to skip columns entirely — reducing I/O by up to 80% for wide feature tables.

Once the physical plan executes, each partition's data is loaded into **Tungsten's binary row format** (UnsafeRow) in off-heap memory. The gradient computation kernel reads these rows without JVM object deserialization, operating directly on contiguous byte arrays. This eliminates GC pressure — a critical property for the iterative gradient loop, where dozens of passes over the dataset would otherwise trigger full-heap GC pauses of 500ms–2s per iteration in a naive JVM implementation.

The optimizer itself is **L-BFGS** (Limited-memory Broyden–Fletcher–Goldfarb–Shanno) for L2/no regularization, and **OWLQN** (Orthant-Wise Limited-memory Quasi-Newton) when L1 regularization is active. OWLQN is a variant of L-BFGS that enforces sparsity-promoting L1 constraints while maintaining quasi-Newton convergence guarantees. Both optimizers run on the **driver** and call out to a distributed `gradient` and `loss` function computed across all executors. Each executor processes its local partition, computes a partial gradient vector, and the driver receives all partial gradients via **`treeAggregate`** — a log-depth reduce tree that avoids funneling all gradients through a single driver socket, cutting network bottleneck by ~60% at scale.

For **multinomial** (softmax) mode, the weight matrix grows from a single vector of size `numFeatures` to a matrix of shape `numClasses × numFeatures`. The memory footprint is proportional, and gradient aggregation cost scales linearly with `numClasses`. Spark automatically selects binary mode when `numClasses == 2` and multinomial otherwise, though you can force multinomial binary classification via `setFamily("multinomial")`.

```text
Training Data Partitions (Executors) Driver JVM
┌────────────────────────────────────┐ ┌─────────────────────────────────┐
│ Executor 1 │ │ L-BFGS / OWLQN Optimizer │
│ ┌──────────────────────────────┐ │ │ ┌───────────────────────────┐ │
│ │ UnsafeRow binary scan │ │ │ │ Global Weight Vector W │ │
│ │ local loss + ∂L/∂W (shard 1) │──┼──┐ │ │ Direction: d = -H⁻¹ ∇L │ │
│ └──────────────────────────────┘ │ │ │ └───────────┬───────────────┘ │
│ │ │ │ │ broadcast W_t │
│ Executor 2 │ │ └──────────────┼──────────────────┘
│ ┌──────────────────────────────┐ │ │ │
│ │ UnsafeRow binary scan │ │ │ treeAggregate │
│ │ local loss + ∂L/∂W (shard 2) │──┼──┤◀─────────────────┘
│ └──────────────────────────────┘ │ │
│ │ │ Aggregation Tree (depth = log N)
│ Executor N │ │ ┌──────┐ ┌──────┐
│ ┌──────────────────────────────┐ │ └──▶│ sum │──▶│ sum │──▶ ∇L (global)
│ │ local loss + ∂L/∂W (shard N) │──┘ └──────┘ └──────┘
│ └──────────────────────────────┘
└────────────────────────────────────┘
 Off-heap Tungsten memory (UnsafeRow) 
```

### Key Internal Components

- **`LogisticCostFun`:** The distributed cost function registered with L-BFGS/OWLQN. On each call, it broadcasts the current weight vector to all executors, triggers a distributed `mapPartitions` + `treeAggregate`, and returns the scalar loss and gradient vector to the optimizer on the driver.

- **`treeAggregate`:** A depth-limited reduce tree (default depth = 2) that aggregates partial gradients across partitions without routing all data through the driver. With 1,000 executors, a flat `reduce` would serialize 1,000 gradient vectors through one driver socket; `treeAggregate` reduces that to ~32 sequential aggregations along the tree.

- **Tungsten `UnsafeRow` Format:** Each training row is stored as a compact binary blob in off-heap memory. The feature vector — typically a sparse `SparseVector` — is accessed via pointer arithmetic rather than Java deserialization. This eliminates millions of short-lived `Object` allocations per iteration, keeping GC overhead below 5% of total training time.

- **Broadcast of Weight Vector:** At each optimizer iteration, the driver broadcasts the current weight vector `W_t` to all executors using Spark's `SparkContext.broadcast()`. For a model with 1M features, this vector is 8MB (doubles) — small enough to fit in executor memory and be efficiently cached via Torrent broadcast. 

---

## ⚠️ Critical Concepts & Common Pitfalls 

### The Feature Scaling Trap

Logistic regression with L2 regularization is **not scale-invariant**. A feature with values in the range [0, 1,000,000] will receive a much smaller regularization penalty than a feature in [0, 1], causing the optimizer to systematically under-regularize large-magnitude features and producing a model that overfits those dimensions. Spark's `LogisticRegression` has `standardization = true` by default, which internally standardizes features to zero mean and unit variance *before* computing gradients — but the reported coefficients are on the original scale. If you set `standardization = false` with L2 regularization on raw features, expect degraded accuracy and convergence in 3–5× more iterations (meaning 3–5× longer training time and proportionally higher cluster cost).

The less obvious trap is when using **sparse features** (e.g., TF-IDF vectors or one-hot encoded categoricals). Standardization computes `stddev` across all samples for each feature dimension. For a vocabulary of 500,000 terms where 99.9% of entries are zero, the mean is near-zero but the stddev computation still iterates over all non-zero entries — which is correct but can be slower than expected. The standardization pass adds roughly one full data scan on top of the gradient iterations. 

### Multinomial vs Binary Mode: Hidden Memory Cliff

When you switch from binary to multinomial logistic regression with `numClasses = 100`, the weight matrix jumps from `numFeatures` elements to `100 × numFeatures` elements. For a model with 500,000 features, this is 50M doubles = **400MB** of weight state on the driver. The L-BFGS optimizer stores `m` correction pairs (default `m = 10`), adding another `20 × 400MB = 8GB` of Hessian approximation state — all in the **driver's JVM heap**. A driver with `-Xmx4g` will throw `OutOfMemoryError: Java heap space` with no warning before training starts. The failure is non-obvious because Spark's UI shows executors healthy while the driver silently OOMs during the optimizer's line search. Always size your driver heap as `numClasses × numFeatures × 8 bytes × (2m + 3)` where `m` is `lbfgsNumCorrections`. 

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|-----------|---------|-------|
| Gradient computation (per iter) | O(N × F) | No | N = rows, F = features; pure map over partitions |
| `treeAggregate` (per iter) | O(P × F × log P) | Yes (network) | P = partitions; depth-limited reduce tree, not full shuffle |
| Model broadcast (per iter) | O(F) | No | Torrent broadcast; cached on executors after first iteration |
| Prediction (transform) | O(N × F) | No | Dot product per row; Tungsten vectorized; no shuffle needed |
| ROC AUC computation | O(N log N) | Yes | Requires global sort of (score, label) pairs across partitions |
| `classWeightCol` reweighting | O(N) | No | Applied as per-row multiplier during cost function evaluation | 

---

## 💻 Code Examples

### Example 1: Binary Classification with Threshold Tuning and Probability Output

> **What this demonstrates:** How Spark's logistic regression exposes raw probabilities alongside predictions, and how threshold tuning at inference time changes the precision-recall operating point without retraining — a critical production pattern for imbalanced datasets.

```scala
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.feature.{VectorAssembler, StandardScaler}
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("LR-BinaryDemo").getOrCreate()

// Load pre-featurized data. 'label' is 0/1, 'rawFeatures' is a dense vector.
val rawData = spark.read.parquet("/data/credit_features")

// StandardScaler ensures L2 regularization penalizes all coefficients equally.
// Without this, high-magnitude features get under-regularized (see Critical Concepts).
val scaler = new StandardScaler()
 .setInputCol("rawFeatures")
 .setOutputCol("features")
 .setWithMean(true) // centers data; valid only for dense vectors
 .setWithStd(true)

// Configure LR: L2 regularization, up to 100 iterations, explicit binary family.
// regParam = 0.01 is a starting point; tune via cross-validation.
val lr = new LogisticRegression()
 .setFeaturesCol("features")
 .setLabelCol("label")
 .setFamily("binomial") // explicit binary mode: single sigmoid output
 .setRegParam(0.01) // L2 penalty λ; larger = more regularization
 .setElasticNetParam(0.0) // 0.0 = pure L2; 1.0 = pure L1 (switches to OWLQN)
 .setMaxIter(100) // L-BFGS iteration budget
 .setStandardization(true) // internalize feature scaling during gradient computation
 .setProbabilityCol("probability") // output col: DenseVector([P(y=0), P(y=1)])
 .setRawPredictionCol("logits") // output col: raw log-odds before sigmoid

val pipeline = new Pipeline().setStages(Array(scaler, lr))
val model = pipeline.fit(rawData)

// Extract the logistic regression stage from the fitted pipeline.
val lrModel = model.stages(1).asInstanceOf[org.apache.spark.ml.classification.LogisticRegressionModel]

// Inspect model summary: available only after fit(), lives on the driver.
val summary = lrModel.binarySummary
println(f"Training AUC: ${summary.areaUnderROC}%.4f")
println(s"Iterations to convergence: ${summary.totalIterations}")

// SHIFT the decision threshold from default 0.5 to 0.3 to favor recall.
// This does NOT retrain — it merely changes the argmax on the probability vector.
// Useful for fraud detection where false negatives are far more costly than FPs.
lrModel.setThreshold(0.3)

val predictions = model.transform(rawData)
predictions.select("label", "probability", "prediction").show(10, truncate = false)
```

> **Mastery Note:** The `probability` column is a `DenseVector([P(y=0), P(y=1)])` produced by applying the sigmoid to the raw log-odds and normalizing. Changing `threshold` from 0.5 to 0.3 means Spark predicts class 1 whenever `P(y=1) > 0.3` — shifting the operating point left on the ROC curve (higher recall, lower precision). This threshold adjustment is applied in the `transform` physical plan as a simple conditional on the probability vector, adding zero computational cost. The `binarySummary.areaUnderROC` requires a global sort of (score, label) pairs across all partitions — a full shuffle — so only call it during evaluation, not in a hot path.

---

### Example 2: Multinomial Classification with Softmax and Per-Class Coefficient Inspection

> **What this demonstrates:** How multinomial logistic regression produces a weight matrix (not a vector), how Spark exposes per-class coefficients, and the memory implications of scaling to many classes.

```scala
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.linalg.{Matrix, Vector}
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().appName("LR-Multinomial").getOrCreate()

val data = spark.read.parquet("/data/news_tfidf_features")
// 'label' ranges from 0 to 19 (20 Newsgroups dataset), 'features' is sparse TF-IDF.

val lr = new LogisticRegression()
 .setFamily("multinomial") // forces softmax; Spark auto-selects this if numClasses > 2
 .setRegParam(0.1) // stronger regularization needed: weight matrix is 20x larger
 .setElasticNetParam(0.0) // L2; OWLQN is available for L1 by setting this to 1.0
 .setMaxIter(200)
 .setStandardization(false) // safe for sparse TF-IDF: mean is near-zero, no centering needed
 .setFitIntercept(true) // one intercept per class in multinomial mode

val model = lr.fit(data)

// coefficientMatrix: Matrix of shape (numClasses, numFeatures).
// Each row i is the weight vector for class i in the one-vs-rest softmax formulation.
// For 20 classes and 100K features: 20 * 100K * 8 bytes = 160MB on the DRIVER heap.
val coeffMatrix: Matrix = model.coefficientMatrix
val interceptVector: Vector = model.interceptVector

println(s"Coefficient matrix shape: ${coeffMatrix.numRows} x ${coeffMatrix.numCols}")
// Expected: 20 x 100000

// Find the top-5 most discriminative features for class 0 (e.g., 'alt.atheism').
// toArray flattens the matrix row-major; slice the first numFeatures elements.
val class0Coeffs = coeffMatrix.toArray.take(coeffMatrix.numCols)
val top5Indices = class0Coeffs.zipWithIndex
 .sortBy(-_._1) // descending by coefficient magnitude
 .take(5)
 .map(_._2)

println(s"Top feature indices for class 0: ${top5Indices.mkString(", ")}")

// Transform and show softmax probability distribution across 20 classes.
val predictions = model.transform(data)
predictions.select("label", "probability", "prediction").show(5, truncate = false)

// Compute multi-class accuracy manually to avoid BinaryClassificationEvaluator confusion.
val accuracy = predictions
 .filter($"prediction" === $"label")
 .count()
 .toDouble / predictions.count()
println(f"Accuracy: ${accuracy * 100}%.2f%%")
```

> **Mastery Note:** In multinomial mode, Spark's softmax does **not** reduce to `numClasses` independent binary classifiers (one-vs-rest). Instead, it optimizes a joint cross-entropy loss over all classes simultaneously, producing a proper probability distribution that sums to 1.0. This means the coefficient matrix is estimated jointly — class 5's weights inform class 12's weights through the shared softmax normalization denominator. The practical consequence is that multinomial is always at least as accurate as one-vs-rest, but costs `numClasses × numFeatures` memory on the driver. If you see `GC overhead limit exceeded` during training on a large multi-class problem, the first place to look is driver heap sizing, not executor memory.

---

### Example 3: Class Imbalance Handling with `classWeightCol`

> **What this demonstrates:** How per-sample instance weights via `classWeightCol` re-weight the logistic loss to compensate for severe class imbalance, and why this is superior to naive oversampling for large-scale Spark jobs.

```scala
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("LR-ClassWeight").getOrCreate()

val rawData = spark.read.parquet("/data/fraud_transactions")
// Dataset has 99.8% label=0 (legitimate) and 0.2% label=1 (fraud). ~10M rows.

// Step 1: Compute class counts and inverse-frequency weights.
// This is a full data scan — do it once and cache the result.
val classCounts = rawData
 .groupBy("label")
 .count()
 .collect()
 .map(r => r.getLong(0) -> r.getLong(1))
 .toMap

val totalCount = classCounts.values.sum.toDouble
val numClasses = classCounts.size.toDouble

// Inverse frequency weight: minority class gets weight = totalCount / (numClasses * minorityCount)
// This normalizes the effective sample size so both classes contribute equally to the gradient.
val weightMap = classCounts.map { case (label, count) =>
 label -> (totalCount / (numClasses * count))
}
// weightMap: {0 -> 0.501, 1 -> 250.0}
// Fraud samples now contribute 250x more to the loss gradient than legitimate ones.

println(s"Class weight map: $weightMap")

// Step 2: Add a 'sampleWeight' column by mapping each row's label to its class weight.
// This uses a Spark SQL expression — evaluated on executors with no shuffle.
val weightExpr = weightMap.foldLeft(lit(1.0)) { case (expr, (label, weight)) =>
 when($"label" === label, weight).otherwise(expr)
}

val weightedData = rawData
 .withColumn("sampleWeight", weightExpr)
 .repartition(200) // ensure balanced partitions after adding weight column
 .cache() // cache: LR will scan this DataFrame once per optimizer iteration

weightedData.groupBy("label").agg(
 count("*").alias("count"),
 avg("sampleWeight").alias("avgWeight")
).show()

// Step 3: Fit LR with classWeightCol pointing to the per-row weight column.
// The cost function scales each row's log-loss by its sampleWeight before aggregation.
// Effective: minority class loss is amplified 250x in the gradient signal.
val lr = new LogisticRegression()
 .setFeaturesCol("features")
 .setLabelCol("label")
 .setWeightCol("sampleWeight") // ← this is the classWeightCol equivalent for per-row weights
 .setFamily("binomial")
 .setRegParam(0.05)
 .setMaxIter(100)
 .setThreshold(0.5) // threshold can be tuned post-training on a validation set

val model = lr.fit(weightedData)

// Evaluate: for fraud detection, focus on AUPRC (area under precision-recall curve)
// not ROC AUC — ROC AUC is misleading when negatives outnumber positives 500:1.
val summary = model.binarySummary
println(f"ROC AUC (misleading for imbalanced): ${summary.areaUnderROC}%.4f")
println(s"Precision by threshold: ${summary.precisionByThreshold.show(5)}")
println(s"Recall by threshold: ${summary.recallByThreshold.show(5)}")
```

> **Mastery Note:** The `weightCol` multiplier is applied **inside** `LogisticCostFun` at gradient computation time — the weighted gradient for row `i` is `w_i × ∇loss_i`. This means weighted rows don't consume more memory or network bandwidth; the weight is just a scalar multiplier on the gradient contribution. This is fundamentally different from oversampling, which physically replicates minority-class rows and increases `N` (and thus partition sizes, shuffle volume, and training time). For a 10M-row dataset with 0.2% fraud, naive 500:1 oversampling would explode the dataset to 5B rows; `weightCol` keeps it at 10M rows with no shuffle cost. The tradeoff: `weightCol` has no effect on the intercept initialization or the regularization path — if you use very strong regularization, the high-weight minority gradients may still be dominated by the L2 penalty.

---

### Example 4: ROC AUC Evaluation, Threshold Sweep, and Model Persistence

> **What this demonstrates:** The full production evaluation pipeline — computing ROC AUC via `BinaryClassificationEvaluator`, sweeping thresholds to find the F1-optimal operating point, and saving/loading the model for serving — exposing the shuffle cost of AUC computation.

```scala
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().appName("LR-ROCEval").getOrCreate()

val Array(trainData, testData) = spark
 .read.parquet("/data/churn_features")
 .randomSplit(Array(0.8, 0.2), seed = 42L)

trainData.cache() // avoid re-reading Parquet on each gradient iteration
testData.cache() // avoid re-reading Parquet during evaluation

val lr = new LogisticRegression()
 .setFeaturesCol("features")
 .setLabelCol("label")
 .setFamily("binomial")
 .setRegParam(0.01)
 .setMaxIter(150)
 .setProbabilityCol("probability")
 .setRawPredictionCol("logits")

val model = lr.fit(trainData)

// --- ROC AUC via BinaryClassificationEvaluator ---
// This evaluator internally calls spark.mllib's BinaryClassificationMetrics,
// which performs a FULL SHUFFLE to sort (score, label) pairs globally.
// The shuffle volume = numTestRows × 16 bytes. For 1M test rows, that's ~16MB — acceptable.
// For 10B rows it is 160GB — use sampling before calling this.
val rocEvaluator = new BinaryClassificationEvaluator()
 .setLabelCol("label")
 .setRawPredictionCol("logits") // uses raw log-odds, not probability — more numerically stable
 .setMetricName("areaUnderROC") // alternative: "areaUnderPR" for imbalanced datasets

val testPredictions = model.transform(testData)
val rocAuc = rocEvaluator.evaluate(testPredictions)
println(f"Test ROC AUC: $rocAuc%.4f")

// --- Threshold Sweep for F1-Optimal Operating Point ---
// Iterate over 99 threshold candidates; each model.setThreshold() + transform() is O(N) — no shuffle.
// This is cheap: each transform is a pure map over the probability column.
val thresholdResults = (1 to 99).map { t =>
 val threshold = t / 100.0
 model.setThreshold(threshold) // mutates the model in-place; NOT thread-safe in concurrent pipelines

 val preds = model.transform(testData)

 // Precision and recall via MulticlassClassificationEvaluator at each threshold
 val tp = preds.filter($"label" === 1.0 && $"prediction" === 1.0).count().toDouble
 val fp = preds.filter($"label" === 0.0 && $"prediction" === 1.0).count().toDouble
 val fn = preds.filter($"label" === 1.0 && $"prediction" === 0.0).count().toDouble

 val precision = if (tp + fp > 0) tp / (tp + fp) else 0.0
 val recall = if (tp + fn > 0) tp / (tp + fn) else 0.0
 val f1 = if (precision + recall > 0) 2 * precision * recall / (precision + recall) else 0.0

 (threshold, precision, recall, f1)
}

val (bestThreshold, bestPrec, bestRec, bestF1) = thresholdResults.maxBy(_._4)
println(f"Optimal threshold: $bestThreshold%.2f Precision: $bestPrec%.4f Recall: $bestRec%.4f F1: $bestF1%.4f")

// Apply the optimal threshold permanently before saving.
model.setThreshold(bestThreshold)

// --- Model Persistence ---
// Saves the model's coefficientMatrix, interceptVector, threshold, and metadata to HDFS/S3.
// Does NOT save the training data or pipeline stages — only the fitted model weights.
model.write.overwrite().save("s3://my-bucket/models/churn-lr-v3")

// Reload for serving: the loaded model is identical to the saved one, including threshold.
val loadedModel = LogisticRegressionModel.load("s3://my-bucket/models/churn-lr-v3")
println(s"Loaded model threshold: ${loadedModel.getThreshold}")

// Demonstrate that the loaded model produces identical predictions.
val loadedPreds = loadedModel.transform(testData)
val diff = loadedPreds
 .join(testPredictions.select("prediction").withColumnRenamed("prediction", "origPred"), Seq("label"))
 .filter($"prediction" =!= $"origPred")
 .count()
println(s"Prediction differences after reload: $diff") // Expected: 0
```

> **Mastery Note:** The threshold sweep runs 99 Spark actions (one `count()` per threshold), which is expensive — each action triggers a full DAG re-execution on `testData`. A smarter production pattern is to collect all `(probability, label)` pairs to the driver with `.collect()` (safe when test set fits in driver memory, e.g., < 1M rows × 16 bytes = 16MB) and compute the entire precision-recall curve locally in Scala/Python. The `model.write.overwrite().save()` serializes the coefficient matrix as a Parquet file alongside JSON metadata — this means saved models can be inspected with any Parquet-compatible tool. The threshold is stored in the metadata JSON, so `setThreshold()` before saving is critical; a model deployed with the default 0.5 threshold after being tuned to 0.3 will silently miss 30% of positives in production.

---

## 🎯 Mastery Checklist

To achieve true mastery of Logistic Regression in Apache Spark:

- [ ] Understand why L-BFGS is used for L2 regularization and OWLQN for L1 — and know that the switch happens automatically based on `elasticNetParam`
- [ ] Know that `standardization = true` is essential for L2 but must be disabled for zero-mean sparse features to avoid incorrect centering
- [ ] Be able to size the driver heap for multinomial LR using `numClasses × numFeatures × 8 bytes × (2m + 3)` before submitting any job
- [ ] Know when `BinaryClassificationEvaluator.areaUnderROC` triggers a shuffle and how to avoid it for very large test sets using sampling
- [ ] Understand the difference between `weightCol` (per-row weight) and `classWeightCol` (per-label weight map) and why `weightCol` is preferred for fine-grained control
- [ ] Know that `setThreshold()` is applied post-sigmoid at predict time — it does not affect training gradients
- [ ] Understand why `treeAggregate` reduces driver network pressure versus `reduce`, and know the default depth is 2 (configurable via `spark.ml.gradient.aggregationDepth`)
- [ ] Be able to diagnose driver OOM during multinomial training from Spark UI (executor metrics healthy, driver GC time spiking) versus executor OOM (task failures with `ExecutorLostFailure`)
- [ ] Know that ROC AUC is misleading for severe class imbalance and prefer AUPRC (area under precision-recall curve) in production fraud/medical classification pipelines

---

## 📚 Summary

Logistic Regression in Apache Spark is a distributed, numerically optimized classifier built on top of Catalyst query planning, Tungsten binary memory, and quasi-Newton optimization. Its correctness depends on understanding three layers: the mathematical model (sigmoid for binary, softmax for multinomial), the distributed computation pattern (`treeAggregate` for gradient reduction, L-BFGS/OWLQN on the driver), and the JVM resource model (driver heap for coefficient matrices, off-heap Tungsten for training rows, Torrent broadcast for weight vectors). 

The most common production failures are all resource-related: driver OOM from large multinomial weight matrices, GC pauses from insufficient standardization (leading to more optimizer iterations), and misleading ROC AUC metrics on imbalanced datasets that mask poor minority-class recall. Each failure is diagnosable from the Spark UI — driver GC tab, task timeline, and shuffle read metrics respectively. 

Mastery of this algorithm means knowing not just how to call `LogisticRegression().fit()`, but how to choose the right regularizer for your feature distribution, size your cluster resources for the coefficient matrix dimensionality, handle class imbalance without exploding dataset size, and evaluate model quality with the right metric for your class distribution. These decisions, made correctly, are the difference between a model that works in a notebook and one that serves reliably in production at petabyte scale. 

