# 🚀 Master Class Assessment: Logistic Regression in Apache Spark

## Part 1: True/False Questions (10 Questions)

1. **Question:** Setting `standardization = false` when using L2 regularization on unscaled features will typically cause the optimizer to converge faster due to skipping the standardization data scan.
**Answer:** False.
**Mastery Explanation:** L2 regularization is not scale-invariant. Unscaled features with large magnitudes will be under-regularized, causing the optimizer to systematically overfit those dimensions. This degrades convergence, forcing L-BFGS to take 3-5x more iterations, meaning longer training time and higher cost.

2. **Question:** Spark's `treeAggregate` function prevents driver network bottlenecks by aggregating partial gradients in a log-depth tree rather than routing all gradients directly to a single driver socket.
**Answer:** True.
**Mastery Explanation:** `treeAggregate` uses a depth-limited reduce tree (default depth = 2). Instead of 1,000 executors sending gradients directly to the driver (causing a massive bottleneck), intermediate executors sum the gradients, cutting network bottlenecks by ~60% at scale.

3. **Question:** In multinomial logistic regression with 100 classes, Spark trains 100 independent binary classifiers using the one-vs-rest strategy to minimize driver memory usage.
**Answer:** False.
**Mastery Explanation:** Spark optimizes a joint cross-entropy loss over all classes simultaneously (softmax), which is always at least as accurate as one-vs-rest. However, this creates a weight matrix of `numClasses × numFeatures`, heavily consuming driver memory.

4. **Question:** When L1 regularization is activated via `elasticNetParam = 1.0`, Catalyst automatically switches the driver-side optimizer from L-BFGS to OWLQN.
**Answer:** True.
**Mastery Explanation:** L-BFGS cannot handle the non-differentiable L1 penalty properly. Spark detects the L1 penalty and automatically switches to OWLQN (Orthant-Wise Limited-memory Quasi-Newton) to enforce sparsity-promoting L1 constraints while maintaining convergence.

5. **Question:** During the iterative gradient computation, Spark deserializes Tungsten `UnsafeRow` objects into Java objects on the executors, leading to significant garbage collection overhead.
**Answer:** False.
**Mastery Explanation:** The gradient kernel reads Tungsten's binary row format (UnsafeRow) in off-heap memory using pointer arithmetic. This avoids JVM object deserialization and eliminates GC pressure, keeping GC overhead below 5%.

6. **Question:** Changing the decision threshold of a fitted `LogisticRegressionModel` via `setThreshold()` triggers a partial recomputation of the model weights to optimize for the new operating point.
**Answer:** False.
**Mastery Explanation:** `setThreshold()` merely changes the conditional argmax applied to the probability vector during the `transform` physical plan. It adds zero computational cost and does not alter the underlying model weights trained by the optimizer.

7. **Question:** Calculating `areaUnderROC` using `BinaryClassificationEvaluator` requires a full global shuffle across the cluster.
**Answer:** True.
**Mastery Explanation:** `areaUnderROC` requires sorting all `(score, label)` pairs globally across all partitions to calculate the True Positive and False Positive rates at all thresholds, which inherently forces a full network shuffle.

8. **Question:** Handling class imbalance via `weightCol` (per-row weights) physically replicates minority-class rows in memory, increasing network shuffle volume but improving model recall.
**Answer:** False.
**Mastery Explanation:** `weightCol` applies a scalar multiplier to the gradient contribution during the `LogisticCostFun` computation (`w_i × ∇loss_i`). It does not replicate rows, meaning there is zero increase in partition sizes or network shuffle volume compared to naive oversampling.

9. **Question:** When saving a `LogisticRegressionModel` to disk, Spark serializes the training data, pipeline stages, and model weights into a single Parquet file.
**Answer:** False.
**Mastery Explanation:** Model saving serializes only the fitted model weights (coefficient matrix, intercept) into Parquet format, alongside metadata (like the threshold) in JSON. It strictly does not save training data or upstream pipeline transformations.

10. **Question:** If a multinomial logistic regression job fails with `OutOfMemoryError: Java heap space` before training iterations begin, the primary culprit is insufficient executor memory.
**Answer:** False.
**Mastery Explanation:** The OOM occurs on the *driver* JVM heap due to the massive weight matrix and Hessian approximation state stored by the L-BFGS optimizer, which scales proportionally to `numClasses × numFeatures`.


## Part 2: Multiple Choice Questions (15 Questions)

11. **Question:** What is the correct formula to calculate the driver heap memory required for the L-BFGS optimizer state in multinomial logistic regression? (Where `m` is `lbfgsNumCorrections`)
A) `numClasses × numFeatures × 4 bytes × m`
B) `numFeatures × 8 bytes × (2m + 1)`
C) `numClasses × numFeatures × 8 bytes × (2m + 3)`
D) `numClasses × 8 bytes × m`
**Answer:** C
**Mastery Explanation:** The weight matrix requires `numClasses × numFeatures` elements. L-BFGS stores `m` correction pairs, resulting in state proportional to `(2m + 3)`. Each double precision float is 8 bytes, so the total driver heap required is `numClasses × numFeatures × 8 bytes × (2m + 3)`.

12. **Question:** Which component of Spark distributes the weight vector to all executors at the start of each optimizer iteration?
A) `treeAggregate`
B) Torrent Broadcast
C) Catalyst Logical Plan
D) Tungsten UnsafeRow
**Answer:** B
**Mastery Explanation:** At each optimizer iteration, the driver broadcasts the current weight vector `W_t` to all executors using Spark's `SparkContext.broadcast()`. This uses Torrent broadcast for efficient peer-to-peer distribution and executor caching.

13. **Question:** Why does the documentation recommend using AUPRC (Area Under Precision-Recall Curve) over ROC AUC for heavily imbalanced datasets (e.g., fraud detection)?
A) ROC AUC computation requires a full shuffle, while AUPRC does not.
B) ROC AUC can be misleadingly high because true negatives dominate the false positive rate.
C) AUPRC automatically tunes the decision threshold during gradient descent.
D) ROC AUC cannot be calculated for binary classification tasks.
**Answer:** B
**Mastery Explanation:** In severely imbalanced datasets (e.g., 500:1 negative to positive ratio), ROC AUC stays artificially high because the massive number of true negatives dilutes the false positive rate. AUPRC focuses strictly on the minority class predictions, making it a more rigorous metric.

14. **Question:** When `standardization = true` is applied to sparse features (e.g., TF-IDF vectors), what is the primary performance drawback?
A) The sparsity of the vectors is destroyed, leading to OOM errors on executors.
B) The mean computation throws a NullPointerException for implicit zero values.
C) Computing standard deviation still requires iterating over all non-zero entries, adding an upfront computational cost equivalent to one full data scan.
D) L-BFGS falls back to naive gradient descent because standardization breaks L2 convexity.
**Answer:** C
**Mastery Explanation:** While the mean of sparse features is near-zero (and doesn't strictly need centering), computing the standard deviation still requires a full pass over all non-zero entries. This standardization pass adds roughly one full data scan before iterations even begin.

15. **Question:** In Catalyst's Logical Optimization phase for Logistic Regression, how does it optimize reading features from Parquet?
A) By converting Parquet to Tungsten UnsafeRow directly on disk before reading.
B) By applying predicate pushdown and projection pruning to skip reading unused columns.
C) By serializing the L-BFGS optimizer into the Parquet reader format.
D) By caching the entire Parquet file in the driver heap.
**Answer:** B
**Mastery Explanation:** Catalyst optimizes the logical plan by pushing column selections into the Parquet reader, leveraging dictionary encoding and skipping unused columns entirely. This minimizes disk I/O significantly, saving up to 80% on wide tables.

16. **Question:** What happens to the gradient aggregation network cost in multinomial mode compared to binary mode?
A) It remains identical as long as `treeAggregate` is utilized.
B) It scales logarithmically with `numClasses`.
C) It scales linearly with `numClasses`.
D) It scales exponentially with `numFeatures`.
**Answer:** C
**Mastery Explanation:** In multinomial mode, the gradient vector grows to a matrix of `numClasses × numFeatures`. Therefore, the size of the partial gradients sent across the network via `treeAggregate` scales linearly with `numClasses`.

17. **Question:** Which of the following accurately describes the computational complexity of gradient computation per iteration in Spark's Logistic Regression?
A) O(N log N)
B) O(N × F)
C) O(P × F × log P)
D) O(F²)
**Answer:** B
**Mastery Explanation:** Gradient computation involves a pure map transformation over partitions without shuffles, computing the dot product for `N` rows and `F` features, resulting in a strict computational complexity of O(N × F).

18. **Question:** What is the default depth of the `treeAggregate` reduce tree?
A) 1
B) 2
C) 3
D) log(N)
**Answer:** B
**Mastery Explanation:** The default aggregation depth for `treeAggregate` is 2. Partial gradients are aggregated at an intermediate level before reaching the driver, drastically reducing driver socket bottlenecks while keeping latency low.

19. **Question:** How is the custom threshold setting persisted when a `LogisticRegressionModel` is saved to disk?
A) It is baked into the raw float values of the coefficient matrix in the Parquet file.
B) It is saved in the metadata JSON file alongside the weights.
C) It is stored as a Spark session configuration parameter (`spark.ml.threshold`).
D) It is not saved; it must be manually reset upon loading the model.
**Answer:** B
**Mastery Explanation:** Model saving serializes hyperparameters, including the tuned threshold, into a JSON metadata file. Failing to call `model.setThreshold()` before saving will persist the default (e.g., 0.5), silently ruining recall in production.

20. **Question:** Why is `weightCol` vastly preferred over naive oversampling for addressing class imbalance in Spark?
A) Oversampling changes the intercept initialization, whereas `weightCol` does not.
B) `weightCol` filters the physical dataset size to only include the minority class.
C) Oversampling physically replicates rows, drastically increasing partition sizes and shuffle volume, whereas `weightCol` acts as a scalar multiplier to the loss gradient.
D) Oversampling forces the use of OWLQN instead of L-BFGS.
**Answer:** C
**Mastery Explanation:** `weightCol` modifies `LogisticCostFun` by multiplying the row's gradient by its weight. This requires zero extra memory or shuffle bandwidth, unlike naive oversampling which explodes the dataset size (e.g., 10M rows expanding to 5B rows).

21. **Question:** What specifically triggers Catalyst to automatically choose the OWLQN optimizer?
A) Setting `standardization = false`.
B) Training a model where `numClasses > 2`.
C) Setting `elasticNetParam` to any value > 0.0 (activating L1 regularization).
D) Setting `regParam = 0.0`.
**Answer:** C
**Mastery Explanation:** `elasticNetParam` controls the L1/L2 mix. Any presence of L1 regularization introduces non-differentiability at zero, which L-BFGS cannot handle. Spark automatically falls back to OWLQN.

22. **Question:** If the `binarySummary.areaUnderROC` metric is evaluated on a dataset, what is the underlying physical operation?
A) A local sort entirely within the driver's JVM.
B) A pure map transformation over the probability column.
C) A depth-limited reduce tree execution.
D) A global sort of (score, label) pairs across partitions requiring a full network shuffle.
**Answer:** D
**Mastery Explanation:** ROC AUC requires sorting the prediction scores globally to calculate the True Positive Rate and False Positive Rate at every possible threshold. This inherently forces a massive O(N log N) network shuffle.

23. **Question:** What happens if you apply a very strong L2 regularization (`regParam = 0.5`) on an imbalanced dataset where you have configured `weightCol` to heavily amplify minority gradients?
A) The optimizer will ignore `weightCol` and default to oversampling.
B) The high-weight minority gradients may still be dominated by the strong L2 penalty, pulling coefficients toward zero.
C) The L2 penalty is automatically scaled down by the inverse of the class weights.
D) Spark throws an `IllegalArgumentException` because `weightCol` and `regParam` are mutually exclusive.
**Answer:** B
**Mastery Explanation:** The total gradient update is a combination of the weighted data loss and the regularization penalty. If the L2 penalty is excessively strong, it will overpower the weighted data loss, shrinking coefficients toward zero regardless of the row weights.

24. **Question:** How does `LogisticRegression.transform()` compute the final `probability` column in binary mode?
A) By calculating the softmax over the raw features directly.
B) By applying the sigmoid function to the raw log-odds (dot product of features and weights).
C) By running a single forward pass of OWLQN on the test row.
D) By looking up the threshold in the driver metadata and mapping it to 1.0.
**Answer:** B
**Mastery Explanation:** In binary classification, the model computes the raw log-odds (`logits`) via a dot product, applies the sigmoid function to map it to a [0,1] range, and outputs a `DenseVector([P(y=0), P(y=1)])`.

25. **Question:** When inspecting the `coefficientMatrix` of a multinomial model with 20 classes and 100K features, what is the shape of the matrix?
A) 1 x 100,000
B) 20 x 100,000
C) 100,000 x 20
D) 20 x 20
**Answer:** B
**Mastery Explanation:** In multinomial mode, Spark estimates a joint weight matrix where each row represents the weight vector for one class. For 20 classes and 100K features, the matrix shape is exactly 20 rows by 100,000 columns.


## Part 3: "Small Twist" Scenario Questions (15 Questions)

26. **Scenario:** You train a binary LR model with `elasticNetParam = 0.0` (L2). It converges in 20 iterations. You then change `elasticNetParam = 1.0` (L1) but explicitly leave `standardization = false`.
**Twist:** What unexpected side effect occurs due to this configuration change?
**Answer:** The L1 penalty affects unscaled features unevenly, failing to properly induce sparsity in large-magnitude features while zeroing out small-magnitude ones aggressively.
**Mastery Explanation:** L1 regularization induces feature selection. If features are not standardized, the L1 penalty magnitude is relative to the raw feature scale, destroying the intended feature selection mechanics and heavily biasing the model against small-magnitude dimensions.

27. **Scenario:** Your driver has 4GB of heap. You successfully train a binary LR model on 1 million features. You then apply the same code to predict 50 classes by running `.fit()` on a new label column.
**Twist:** The job instantly crashes before the Spark UI shows any executor tasks running. Why?
**Answer:** The driver OOMs instantly during the L-BFGS line search memory allocation.
**Mastery Explanation:** 50 classes * 1 million features * 8 bytes = 400MB for the weight matrix. The L-BFGS optimizer requires `(2m + 3)` copies (where default m=10, meaning 23 copies). 400MB * 23 = ~9.2GB. This drastically exceeds the 4GB driver heap, causing a silent OOM before executors do any work.

28. **Scenario:** You compute `classWeights` and create a `sampleWeight` column using a Spark SQL `when().otherwise()` expression. You then immediately call `lr.fit(weightedData)`.
**Twist:** The training time takes 10x longer compared to the unweighted model. What critical optimization step was missed?
**Answer:** You forgot to `.cache()` the `weightedData` DataFrame.
**Mastery Explanation:** `lr.fit()` is an iterative algorithm that scans the data dozens of times. Since `sampleWeight` is derived via an SQL projection, without caching, Spark re-evaluates the `when().otherwise()` expression (and re-reads the underlying Parquet files) on every single L-BFGS iteration.

29. **Scenario:** You use `BinaryClassificationEvaluator` to compute ROC AUC on a 10-billion row test set. The job hangs for hours and eventually fails with `ExecutorLostFailure`.
**Twist:** Why did a simple evaluation fail when the iterative training succeeded?
**Answer:** ROC AUC computation requires a full global sort of 10 billion pairs, exceeding shuffle limits.
**Mastery Explanation:** Training relies on `treeAggregate` (O(P * F * log P) network traffic). However, the ROC evaluator performs a global shuffle (O(N log N)). For 10 billion rows, this results in massive shuffle data (~160GB+), overwhelming executor memory and disk. You must sample before calling this evaluator.

30. **Scenario:** You evaluate 99 threshold values by calling `model.setThreshold(t)` and `transform(testData).count()` inside a Scala `for` loop.
**Twist:** The job takes 3 hours to evaluate thresholds. How can this exact mathematical evaluation be reduced to 30 seconds?
**Answer:** By using `.select("probability", "label").collect()` to bring the pairs to the driver, and computing the metrics locally.
**Mastery Explanation:** Calling `transform().count()` 99 times triggers 99 full DAG executions and data scans on the executors. Collecting the probabilities to the driver (if they fit in memory) allows you to compute all thresholds locally in milliseconds without re-scanning data.

31. **Scenario:** You train an LR model with `standardization = true` and `regParam = 0.1`. After training, you manually inspect `model.coefficients`.
**Twist:** Are the coefficients stored in `model.coefficients` based on the standardized feature scale or the original feature scale?
**Answer:** The original feature scale.
**Mastery Explanation:** Spark internalizes the standardization during the gradient computation for optimizer stability, but it transforms the resulting coefficients back to the *original* feature space before returning the model. This guarantees you can apply the coefficients directly to unscaled data during inference.

32. **Scenario:** You tune the threshold to 0.1 to maximize F1-score. You save the model using `model.write.save()`. Later, a real-time microservice loads this Parquet model to score rows.
**Twist:** The microservice starts predicting class 0 for rows that should be class 1 under the 0.1 threshold. Why?
**Answer:** The optimal threshold was not updated in the model object *before* saving.
**Mastery Explanation:** `model.write.save()` serializes the metadata JSON based on the model's current in-memory state. If you calculated the optimal threshold but failed to execute `model.setThreshold(0.1)` on the object prior to saving, the default 0.5 is written to the JSON, silently ruining production inference.

33. **Scenario:** You use Spark MLlib's `StandardScaler` to explicitly center (`setWithMean(true)`) your sparse TF-IDF features before feeding them into Logistic Regression.
**Twist:** The pipeline crashes instantly with an `IllegalArgumentException` during the scaling phase. Why?
**Answer:** `setWithMean(true)` cannot be applied to `SparseVector` objects.
**Mastery Explanation:** Centering a sparse vector by subtracting the mean turns all the implicit zeros into explicit non-zero values (dense vectors). Spark explicitly throws an exception when `setWithMean(true)` is applied to `SparseVector` to prevent immediate cluster-wide OutOfMemory errors from dense memory explosion.

34. **Scenario:** You switch from `setFamily("binomial")` to `setFamily("multinomial")` on a dataset that only contains 2 classes.
**Twist:** Does the memory footprint of the weights change, and by how much?
**Answer:** Yes, the weight matrix doubles in size.
**Mastery Explanation:** Even for 2 classes, forcing `multinomial` causes Spark to optimize a joint cross-entropy loss with a separate weight vector for *each* class (2 x numFeatures), rather than a single vector (1 x numFeatures) used in the binary sigmoid formulation.

35. **Scenario:** You are tracking execution time per iteration. Iteration 1 takes 45 seconds, but iterations 2 through 50 take 2 seconds each.
**Twist:** What internal Spark mechanism causes the first iteration to be drastically slower?
**Answer:** Torrent Broadcast lazy initialization and the initial Tungsten binary scan.
**Mastery Explanation:** During the first iteration, Spark triggers the initial disk read of the training data, caches it in Tungsten off-heap memory, and the driver performs the first Torrent broadcast of the weight vector. Subsequent iterations read directly from memory and reuse established broadcast infrastructure.

36. **Scenario:** You set `classWeightCol` to penalize false negatives 100x more. The test ROC AUC metric doesn't change by even 0.001 compared to the unweighted model.
**Twist:** Is the `classWeightCol` failing to apply internally?
**Answer:** No. ROC AUC is invariant to uniform threshold shifts caused by class weighting.
**Mastery Explanation:** Class weighting shifts the predicted probability distributions (pushing them higher for the minority class). However, ROC AUC measures the area under the curve across *all* thresholds. Since the rank ordering of predictions is largely preserved, the ROC AUC remains identical. You must evaluate Precision/Recall at a specific threshold.

37. **Scenario:** You are using `LogisticRegression` on a massive cluster with 10,000 executors. The driver CPU pegs at 100% and network times out during gradient aggregation.
**Twist:** How do you fix this without shrinking the cluster size or data size?
**Answer:** Increase `spark.ml.gradient.aggregationDepth` (e.g., to 3 or 4).
**Mastery Explanation:** The default `treeAggregate` depth is 2. With 10,000 executors, a depth of 2 means the driver still receives ~100 intermediate aggregation vectors simultaneously, choking its network card. Increasing the tree depth adds more intermediate reduce steps, vastly reducing the driver's bottleneck.

38. **Scenario:** A dataset has 500 million rows and only 10 features. A colleague suggests extracting it to a single node and using scikit-learn because the feature space fits in memory.
**Twist:** Why will Spark's `LogisticRegression` still vastly outperform scikit-learn in this regime?
**Answer:** Scikit-learn's gradient loop is strictly single-threaded and lacks data parallelism.
**Mastery Explanation:** Even with a small feature space, computing the loss and gradient over 500 million rows is a massive mathematical operation. Spark parallelizes the O(N x F) gradient computation across hundreds of cores via data partitioning. Scikit-learn will process all 500 million rows sequentially on a single CPU thread.

39. **Scenario:** You attempt to extract the top 5 most discriminative features for class 0 in a multinomial model using `coeffMatrix.toArray.take(coeffMatrix.numCols)`.
**Twist:** The features returned are completely irrelevant. What matrix flattening mistake was made?
**Answer:** Spark's `Matrix` type is heavily optimized and stored column-major by default.
**Mastery Explanation:** Because `DenseMatrix` is stored column-major, `toArray` flattens it by column. Taking the first `numCols` elements gives you the weights of feature 0 across *all classes*, not the weights of all features for class 0. You must extract the specific row using matrix row indexing.

40. **Scenario:** You configure LR with `elasticNetParam = 0.5` (ElasticNet), combining L1 and L2 penalties.
**Twist:** Which optimizer does Catalyst strictly select, and why?
**Answer:** OWLQN.
**Mastery Explanation:** Any non-zero amount of L1 regularization (`elasticNetParam > 0.0`) introduces mathematical non-differentiability at zero. L-BFGS requires a continuously differentiable objective function. Therefore, Catalyst dictates the use of the OWLQN optimizer to handle the L1 component safely.


## Part 4: Coding & Debugging Questions (10 Questions)

41. **Debugging Scenario:**
```scala
val lr = new LogisticRegression()
  .setRegParam(0.1)
  .setStandardization(false)
val pipeline = new Pipeline().setStages(Array(standardScaler, lr))
val model = pipeline.fit(rawData)
```
**Error:** The model takes 300 iterations to converge, but if you remove `standardScaler`, it converges in 50 iterations (assuming `setStandardization(true)`).
**Fix:** The logical flaw is wasting an entire pipeline stage to materialize dense scaled vectors, which destroys sparsity and increases memory. Spark's internal `setStandardization(true)` applies scaling dynamically during gradient calculation without materializing dense data, which is massively faster.

42. **Debugging Scenario:**
```scala
val rocAuc = new BinaryClassificationEvaluator()
  .setLabelCol("label")
  .setRawPredictionCol("probability")
  .setMetricName("areaUnderROC")
  .evaluate(predictions)
```
**Error:** The evaluator returns mathematically incorrect metrics or throws a schema error.
**Fix:** `setRawPredictionCol` expects the raw log-odds (the `logits` column). To evaluate the `probability` column, you must explicitly use `.setRawPredictionCol("probability")` — wait, the fix is that the default expects raw log-odds. If you point it to "probability", it may misinterpret the DenseVector. Best practice is to leave it pointed to `rawPrediction` (logits) for numerical stability.

43. **Coding Scenario:** Write the optimal Spark SQL expression to create a `sampleWeight` column for a dataframe `df` where `label=0` gets weight 1.0 and `label=1` gets weight 250.0.
**Answer:**
```scala
import org.apache.spark.sql.functions._
val weightedDf = df.withColumn("sampleWeight", when($"label" === 1.0, 250.0).otherwise(1.0))
```
**Mastery Explanation:** The `when().otherwise()` construct evaluates lazily as a Catalyst projection on executors, meaning no data is moved, shuffled, or physically duplicated.

44. **Debugging Scenario:**
```scala
val lrModel = model.stages(1).asInstanceOf[LogisticRegressionModel]
val summary = lrModel.binarySummary
println(summary.areaUnderROC)
```
**Error:** The code throws a runtime exception when accessing `binarySummary`. The model was trained with `setFamily("multinomial")`.
**Fix:** `binarySummary` is strictly only available when the model is trained as a binary classifier. If `setFamily("multinomial")` was explicitly used (or inferred >2 classes), the model generates a `multiclassSummary` instead. You must cast or check `hasSummary` before accessing.

45. **Debugging Scenario:**
```scala
val model = LogisticRegressionModel.load("/path/to/model")
model.setThreshold(0.2)
val preds = model.transform(testData)
```
**Error:** In a Structured Streaming or highly concurrent prediction pipeline, predictions randomly use threshold 0.5 instead of 0.2 on different batches.
**Fix:** `model.setThreshold()` mutates the model object in place and is **not thread-safe**. In a concurrent environment, mutating a shared model instance causes severe race conditions. The model should be instantiated with the correct threshold per thread, or the threshold must be set correctly *before* saving it to disk.

46. **Coding Scenario:** Extract the exact intercept value for class 2 in a fitted multinomial `LogisticRegressionModel`.
**Answer:**
```scala
val class2Intercept = model.interceptVector(2)
```
**Mastery Explanation:** In multinomial LR, `interceptVector` is not a scalar double, but a `Vector` of size `numClasses`. You access the specific intercept via 0-based class indexing.

47. **Debugging Scenario:**
```scala
val grid = new ParamGridBuilder()
  .addGrid(lr.regParam, Array(0.01, 0.1, 1.0))
  .build()
val cv = new CrossValidator().setEstimator(lr).setEstimatorParamMaps(grid).setParallelism(3)
```
**Error:** The driver instantly OOMs when `cv.fit()` is called on a large multinomial dataset.
**Fix:** `CrossValidator` trains models in parallel because `parallelism=3`. Each model's L-BFGS state requires massive driver heap (e.g., 4GB per model). The driver heap must be sized to hold the optimizer state of *all 3 concurrent models* (12GB total). You must either drop parallelism to 1 or heavily increase driver memory.

48. **Coding Scenario:** How do you instruct Spark to force binomial classification on a dataset that technically has labels 0, 1, and 2, but you only care about separating 0 from 1, and map 2 to 1 beforehand?
**Answer:**
```scala
// First map the labels
val mappedDf = df.withColumn("label", when($"label" === 2, 1).otherwise($"label"))
// Then configure LR
val lr = new LogisticRegression().setFamily("binomial")
```
**Mastery Explanation:** If the label column contains anything other than 0 and 1, Catalyst will throw a hard error during the Analysis phase if you force `binomial`. You must physically map the labels to 0 and 1 via SQL expressions first.

49. **Debugging Scenario:**
```scala
val lr = new LogisticRegression().setMaxIter(10)
val model = lr.fit(df)
println(model.summary.totalIterations) // Prints 10
```
**Error:** The model predictions are worse than random guessing.
**Fix:** `setMaxIter(10)` artificially chokes the L-BFGS optimizer. Logistic regression often requires 50-150 iterations to reach convergence, especially with unscaled or correlated features. The optimizer exited early due to the hard limit, leaving the weights far from the global minimum.

50. **Debugging Scenario:**
```scala
val classCounts = df.groupBy("label").count().collect()
val weightedDf = df.withColumn("weight", /* calculation */)
val model = lr.fit(weightedDf)
```
**Error:** The `total` calculation is correct, but the overall training pipeline takes 2 hours on a small 10M row dataset.
**Fix:** The `groupBy().count()` triggers a full DAG execution. If the user then does `df.withColumn...` without calling `.cache()`, the subsequent `lr.fit()` will re-trigger the entire Parquet scan *and* the `when().otherwise()` projection on *every single* L-BFGS iteration (e.g., 100 times). The fix is to heavily `.cache()` the dataframe immediately before calling `.fit()`.
