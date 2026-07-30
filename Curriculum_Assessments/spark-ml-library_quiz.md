# 50 Elite Technical Questions: Spark ML Library

## 1. True/False Questions

**Q1:** In `spark.ml`, pipeline stages must be fully materialized to disk before the next stage can begin processing.
**Answer:** False.
**Mastery Explanation:** Catalyst and Tungsten's Whole-Stage Code Generation (WSCG) can fuse adjacent row-level Transformer operations (e.g., VectorAssembler followed by a scaler) into a single compiled Java bytecode loop, avoiding intermediate materialization to disk or even the JVM heap.

**Q2:** `Pipeline.fit()` returns an immutable `Model` object, which implements the `Transformer` contract.
**Answer:** True.
**Mastery Explanation:** An `Estimator.fit()` always returns a `Model` (a subclass of `Transformer`). Because it is immutable, it is safely broadcastable to executors for scoring without re-serialization.

**Q3:** During hyperparameter tuning, `CrossValidator` computes evaluation metrics (like AUC) locally on the driver by collecting all predictions from the executors.
**Answer:** False.
**Mastery Explanation:** Spark ML evaluators compute metrics using distributed Spark SQL aggregations (e.g., distributed trapezoid integration for AUC-ROC over sorted scores), scaling linearly without pulling predictions to the driver.

**Q4:** Setting the parameter `withMean=True` in `StandardScaler` destroys the sparsity of `SparseVector` objects.
**Answer:** True.
**Mastery Explanation:** Subtracting a non-zero mean from a sparse vector assigns a non-zero value to previously zero elements, converting it into a dense vector (float array), which can cause massive memory inflation for high-cardinality OneHotEncoder outputs.

**Q5:** `mlflow.spark.autolog()` patches both Python PySpark API and underlying Scala Spark ML parameters.
**Answer:** False.
**Mastery Explanation:** It patches at the Python level via monkey-patching. If you use raw Scala classes or a mixed PySpark/Scala deployment, autolog will fail to capture parameters from the Scala-native objects.

**Q6:** Calling `df.cache()` immediately before `CrossValidator.fit()` guarantees performance improvements across all cluster setups.
**Answer:** False.
**Mastery Explanation:** If the DataFrame exceeds the executor's `spark.memory.storageFraction`, caching triggers aggressive block eviction during memory-intensive model training (like GBTs), causing repeated re-scans of source data and completely negating cache benefits.

**Q7:** Setting `spark.ml.parallelism` to a high number (e.g., 50) linearly speeds up `CrossValidator` across all clusters.
**Answer:** False.
**Mastery Explanation:** Parallelism > 1 submits concurrent Spark jobs from the driver. Too high a value materializes multiple folds concurrently in the executor BlockManagers, leading to `java.lang.OutOfMemoryError` or heavy shuffle spill.

**Q8:** `MLReader` lazily loads stage data (like GBT trees) in Parquet format only when executors perform their first `transform()` call.
**Answer:** True.
**Mastery Explanation:** The driver only eagerly reads the metadata JSON. The Parquet data is lazy-loaded by executor tasks on the first `transform()`, which is why the first scoring batch suffers high latency.

**Q9:** `spark.ml` models are stored by `MLWriter` as serialized Java object blobs.
**Answer:** False.
**Mastery Explanation:** Models are stored using a structured format: metadata as JSON and learned parameters (like tree nodes or coefficients) as typed Parquet files, enabling cross-version and cross-language interoperability.

**Q10:** `TrainValidationSplit` requires exactly `n` total fits, where `n` is the number of parameter grid combinations.
**Answer:** True.
**Mastery Explanation:** Unlike `CrossValidator` which requires `k * n` fits for `k`-folds, `TrainValidationSplit` performs a single randomized train/validation split, executing `n` fits total.

## 2. Multiple Choice Questions

**Q11:** Which action prevents data leakage when standardizing features for model training?
A) Fitting `StandardScaler` on the entire dataset, then calling `randomSplit`.
B) Fitting `StandardScaler` strictly on the `test` split.
C) Encapsulating `StandardScaler` inside a `Pipeline` passed to `CrossValidator`.
D) Using `StandardScaler(withMean=False)` on the full dataset.
**Answer:** C.
**Mastery Explanation:** By keeping it inside the pipeline, `CrossValidator` guarantees that `StandardScaler` is fitted only on the training fold of each CV iteration, leaving it blind to the validation fold.

**Q12:** How does `CrossValidator` with `parallelism > 1` achieve concurrency?
A) It uses multi-threading inside executor JVMs to evaluate multiple params per row.
B) It submits multiple independent model fits concurrently via Scala `Future`s on the driver's thread pool.
C) It allocates one Spark master node per parameter combination.
D) It relies on Tungsten WSCG to fuse parameter combinations into one bytecode block.
**Answer:** B.
**Mastery Explanation:** It uses a Scala `ExecutionContext` on the driver to submit multiple overlapping Spark jobs to the DAGScheduler.

**Q13:** What is the underlying architecture of a saved GBT model's tree nodes when written by `MLWriter`?
A) Pickled Python lists.
B) Custom Kryo serialized blobs.
C) JSON dictionaries of feature splits.
D) Parquet columnar rows where each node is a typed struct.
**Answer:** D.
**Mastery Explanation:** Spark ML leverages Parquet. Nodes are structs containing `id`, `gain`, `leftChild`, `rightChild`, etc., allowing vectorized Parquet scans when loading large forests.

**Q14:** Why should you avoid `collectSubModels=True` in a large `CrossValidator` grid?
A) It forces executors to spill intermediate sub-models to disk.
B) It triggers Python UDF deserialization overhead.
C) It retains all fitted models in the driver's heap, easily causing an OOM.
D) It prevents MLflow from autologging the best model.
**Answer:** C.
**Mastery Explanation:** A 5-fold CV over 20 params yields 100 models. Collecting them all sends them to the driver heap. Set to `False` to discard them after metric evaluation.

**Q15:** In the context of `mlflow.spark.autolog()`, what is the consequence of `log_model_signatures=True`?
A) It signs the Parquet files cryptographically.
B) It executes a `transform()` call on a small data sample to infer the schema, adding a small Spark job.
C) It restricts the model to the `pyfunc` flavor only.
D) It forces WSCG to be disabled.
**Answer:** B.
**Mastery Explanation:** Signature inference requires data to flow through the pipeline to determine output types, triggering an extra Spark job.

**Q16:** How does `spark.ml` handle hyperparameters to avoid instantiating new JVM objects for every CV parameter variation?
A) By utilizing global broadcast variables.
B) Through the typed `ParamMap` and `Params.copy()` method.
C) By modifying instance variables via Python reflection.
D) Using stateful `Estimator` classes.
**Answer:** B.
**Mastery Explanation:** Hyperparameters are stored in `Param[T]` structures. `Params.copy(newParamMap)` clones the estimator with a parameter overlay efficiently.

**Q17:** What happens when an unseen categorical value is encountered by `StringIndexer(handleInvalid="keep")` during `transform()`?
A) The pipeline throws an exception.
B) The row is silently dropped.
C) The value is assigned a new, separate index (vocabulary size + 1).
D) The value is mapped to the most frequent category.
**Answer:** C.
**Mastery Explanation:** `"keep"` safely assigns unseen labels to an extra bucket, preventing scoring jobs from crashing on dirty production data.

**Q18:** If a PySpark ML Pipeline is saved to disk, which MLflow flavor is capable of loading it without requiring a SparkSession?
A) `spark` flavor.
B) `mleap` flavor.
C) `python_function` (pyfunc) flavor.
D) None, Spark models always require a Spark JVM to execute.
**Answer:** C.
**Mastery Explanation:** The Pyfunc flavor wraps the Spark model and can operate on Pandas DataFrames (often spinning up a local SparkContext under the hood or via mlflow loading pyfunc), abstracting the raw Spark environment.

**Q19:** When `GBTClassifier` is trained with `subsamplingRate < 1.0`, how is the row sampling implemented?
A) By utilizing a global random seed on the driver.
B) Via row sampling on the executor using `TaskContext.partitionId()` as the random seed.
C) By taking the first N rows of each partition.
D) By performing a global shuffle.
**Answer:** B.
**Mastery Explanation:** It uses partition ID to seed the random sampler. Consequently, if the cluster's partition layout changes, the exact trees built will change, affecting reproducibility.

**Q20:** What is the time complexity of `MLWriter.save()` in relation to the dataset size `N`?
A) O(N)
B) O(N log N)
C) O(P) where P is the number of learned parameters.
D) O(1)
**Answer:** C.
**Mastery Explanation:** `MLWriter.save()` writes the metadata and parameters (e.g., GBT nodes). It does not pass over the training data `N`.

**Q21:** Which of the following best describes the difference between `spark.mllib` and `spark.ml`?
A) `mllib` uses DataFrames, `ml` uses Datasets.
B) `mllib` operates on low-level RDDs, while `ml` operates on DataFrames and Catalyst optimizations.
C) `mllib` only supports Python, `ml` only supports Scala.
D) `mllib` is stateful, `ml` is state-free.
**Answer:** B.
**Mastery Explanation:** The shift to DataFrames allows `spark.ml` to leverage Catalyst Query Optimization and Tungsten WSCG.

**Q22:** Which layer is responsible for fusing a `VectorAssembler` and `StandardScaler` into a single loop?
A) DAGScheduler
B) TaskScheduler
C) Tungsten WSCG
D) MLWriter
**Answer:** C.
**Mastery Explanation:** Tungsten's Whole-Stage Code Generation collapses adjacent row-wise projections to eliminate virtual dispatch overhead.

**Q23:** What is the primary reason an executor might throw a `SparkOutOfMemoryError` during `CrossValidator`?
A) `numFolds` is odd.
B) The `evaluator` metric requires all predictions simultaneously.
C) `parallelism` is too high, keeping too many fold DataFrames alive in the BlockManager.
D) The model's Parquet files cannot fit on the executor's disk.
**Answer:** C.
**Mastery Explanation:** Concurrent model fits materialize multiple folds simultaneously. Each takes up gigabytes in the block manager.

**Q24:** When does a `Transformer` execute Spark jobs?
A) During the `fit()` call.
B) During the `save()` call.
C) Never, `Transformer.transform()` is a pure DAG projection/scoring step until an action is called.
D) Only when WSCG is disabled.
**Answer:** C.
**Mastery Explanation:** Transformers simply add physical plans to the Catalyst DAG. No jobs fire until an action (like `.show()` or `.write()`) is invoked.

**Q25:** In the 2-phase save protocol of `MLWriter`, what is written first?
A) The Parquet data of the trees.
B) A metadata JSON directory with class name, UID, and parameter values.
C) A Kryo serialized bytecode file.
D) The MLflow experiment tracking logs.
**Answer:** B.
**Mastery Explanation:** It writes the JSON metadata first. This allows the driver to know exactly which Scala classes to instantiate before lazily loading Parquet data.

## 3. Small Twist Questions

**Q26:** You build a Pipeline: `assembler` -> `scaler` -> `lr`. You evaluate with `CrossValidator`.
*Twist*: You call `scaler.fit(df)` manually and pass the fitted scaler into the Pipeline instead of the unfitted scaler.
**Resulting Issue:** Data Leakage.
**Mastery Explanation:** Fitting the scaler on the full dataset observes the test set's mean/stddev. Evaluation metrics will be falsely inflated.

**Q27:** You run `CrossValidator` over a grid of 50 parameters with `numFolds=5` on 100M rows.
*Twist*: You swap `CrossValidator` for `TrainValidationSplit` with `trainRatio=0.8`.
**Resulting Effect:** Execution drops from 250 model fits to exactly 50 model fits.
**Mastery Explanation:** TVS does a single validation split, completely avoiding the `k` multiplier, sacrificing a bit of statistical variance for a massive speedup on large datasets.

**Q28:** You have a Pipeline with high-cardinality `OneHotEncoder` outputs.
*Twist*: You change `StandardScaler(withMean=False)` to `withMean=True`.
**Resulting Effect:** Executor OOM / Memory explosion.
**Mastery Explanation:** Mean subtraction converts compact `SparseVector` objects into massive dense float arrays (`DenseVector`), drastically increasing per-row memory.

**Q29:** You deploy a 500-tree `GBTClassifier` using `MLReader.load()`.
*Twist*: You instantly hit the model with max production traffic right after loading.
**Resulting Effect:** The first batch of requests experiences a severe latency spike.
**Mastery Explanation:** The Parquet node data is lazy-loaded by executors. You must warm up the model with a dummy `transform()` call before serving.

**Q30:** You run a hyperparameter sweep on a 500GB dataset using a 64-node cluster.
*Twist*: You call `train_df.cache()` to speed up CV folds, but executor memory is only 100GB total.
**Resulting Effect:** Performance plummets due to cache thrashing.
**Mastery Explanation:** The dataset exceeds `spark.memory.storageFraction`. The memory manager aggressively evicts blocks during GBT memory allocations, forcing repeated S3/HDFS re-scans.

**Q31:** You implement `mlflow.spark.autolog()`.
*Twist*: You place `mlflow.spark.autolog()` *after* `pipeline.fit()`.
**Resulting Effect:** No parameters or models are logged.
**Mastery Explanation:** The autologger monkey-patches the `.fit()` method. It must be called before execution begins to intercept the metrics.

**Q32:** You use PySpark to orchestrate a Pipeline containing a custom Scala Transformer.
*Twist*: You rely entirely on `mlflow.spark.autolog()` for lineage.
**Resulting Effect:** Scala-native parameters are silently ignored.
**Mastery Explanation:** Autolog operates at the Python wrapper level. If a parameter isn't exposed via a PySpark `Param` object, it won't be logged.

**Q33:** You run a huge GBT grid search on a driver node with 4GB of RAM.
*Twist*: You set `collectSubModels=True`.
**Resulting Effect:** The driver crashes with `java.lang.OutOfMemoryError`.
**Mastery Explanation:** The driver attempts to collect hundreds of large `GBTClassificationModel` objects into its local heap simultaneously.

**Q34:** You achieve exactly 85% AUC using GBT with `subsamplingRate=0.8` on Monday.
*Twist*: You re-run the exact same code and data on Tuesday, but the upstream team changed `spark.sql.shuffle.partitions` from 200 to 500.
**Resulting Effect:** The model produces slightly different predictions and a different AUC.
**Mastery Explanation:** GBT's subsampling relies on `TaskContext.partitionId()` as a random seed. Changing the partition layout alters the sampled rows per tree.

**Q35:** You use `StringIndexer` to encode customer regions.
*Twist*: You set `handleInvalid="error"` instead of `"keep"`. The model is deployed to streaming production. A user from a new region ("Mars") registers.
**Resulting Effect:** The Spark streaming micro-batch crashes.
**Mastery Explanation:** The Transformer throws an exception upon seeing an unseen categorical string. `"keep"` assigns it to an unseen bucket, allowing the pipeline to survive.

**Q36:** You are evaluating a regression model.
*Twist*: Instead of using `RegressionEvaluator`, you `collect()` predictions to the driver and use `sklearn.metrics.mean_squared_error`.
**Resulting Effect:** Driver OOM or severe bottleneck.
**Mastery Explanation:** Spark's native evaluators perform distributed aggregations. Collecting raw predictions to the driver scales terribly and breaks the distributed paradigm.

**Q37:** You delete the `metadata/part-00000` file from a saved model's directory on S3.
*Twist*: You attempt to load the model via `PipelineModel.load()`.
**Resulting Effect:** The driver immediately throws an exception on load.
**Mastery Explanation:** The driver requires the JSON metadata to identify the Scala classes (UIDs) and reconstruct the DAG structure before executors even see data.

**Q38:** You inspect the GBT model parameters after loading using `loaded_model.stages[-1].featureImportances`.
*Twist*: You assume this triggers a Parquet scan of the trees.
**Resulting Effect:** It returns instantly without scanning Parquet.
**Mastery Explanation:** `featureImportances` is a computed metadata array stored inside the JSON metadata on save, requiring no tree traversal on load.

**Q39:** You set `CrossValidator` `numFolds=10`.
*Twist*: You notice the Spark UI shows 10 distinct shuffle stages before any ML training begins.
**Resulting Effect:** Massive pre-training overhead.
**Mastery Explanation:** CV uses `randomSplit()` for each fold, and each split triggers a deterministic hash-partition shuffle. Large datasets suffer heavy I/O before fitting even starts.

**Q40:** You are logging thousands of models nightly with MLflow.
*Twist*: You set `log_model_signatures=False` in `autolog()`.
**Resulting Effect:** Pipeline execution time decreases by 5–10%.
**Mastery Explanation:** You avoid the automatic `transform()` pass over the sample data that MLflow uses to infer the schema, speeding up high-throughput batch training.

## 4. Coding & Debugging Questions

**Q41:** *Identify the architectural bug.*
```python
scaler = StandardScaler(inputCol="raw", outputCol="feat")
scaler_model = scaler.fit(full_dataset)
train, test = full_dataset.randomSplit([0.8, 0.2])
lr_model = LogisticRegression().fit(scaler_model.transform(train))
```
**Answer:** Data Leakage. The `StandardScaler` calculates mean/variance over `full_dataset`, thus learning information about the `test` set before splitting.
**Mastery Explanation:** `StandardScaler` must be wrapped in a `Pipeline` and fitted *only* on the training split.

**Q42:** *Diagnose the crash.*
```python
cv = CrossValidator(estimator=pipeline, evaluator=evaluator,
                    estimatorParamMaps=grid, parallelism=40)
cv.fit(large_df)
```
*Error: `SparkOutOfMemoryError: BlockManager exceeded memory limits on executor`*
**Answer:** `parallelism=40` is dangerously high.
**Mastery Explanation:** This submits 40 concurrent model fits, meaning 40 versions of intermediate folded DataFrames are materialized simultaneously in executor memory. Reduce to 2–4.

**Q43:** *Fix the memory explosion.*
```python
ohe = OneHotEncoder(inputCols=["cat_idx"], outputCols=["cat_ohe"])
assembler = VectorAssembler(inputCols=["cat_ohe"], outputCol="raw")
scaler = StandardScaler(inputCol="raw", outputCol="features", withMean=True)
```
**Answer:** Change `withMean=True` to `withMean=False`.
**Mastery Explanation:** OHE creates highly sparse vectors. Subtracting the mean forces them into DenseVectors, increasing memory usage by 10-50x.

**Q44:** *Eliminate the latency spike.*
```python
prod_model = PipelineModel.load("s3://models/v1")
def handle_api_request(df):
    return prod_model.transform(df) # First request takes 15 seconds!
```
**Answer:** Warm up the model before serving: `prod_model.transform(spark.range(1).toDF()).collect()`.
**Mastery Explanation:** GBT trees are saved as Parquet and are lazy-loaded by executors. A dummy transform forces the BlockManager to load and deserialize the data before real traffic hits.

**Q45:** *Fix the logging bug.*
```python
pipeline.fit(train_df)
mlflow.spark.autolog(log_models=True)
```
**Answer:** `autolog()` must be called *before* `pipeline.fit()`.
**Mastery Explanation:** Autolog uses Python monkey-patching on the `fit` method. If execution has already started, the patch is applied too late to intercept parameters.

**Q46:** *Optimize the slow CV run.*
```python
train_df = spark.read.parquet("s3://huge-10TB-dataset/")
train_df.cache()
cv = CrossValidator(..., parallelism=2)
cv.fit(train_df)
```
**Answer:** Remove `train_df.cache()` (or use `TrainValidationSplit`).
**Mastery Explanation:** A 10TB dataset far exceeds the executor `storageFraction`. Caching causes massive JVM garbage collection pauses and block evictions, making performance worse than just reading from S3.

**Q47:** *Prevent driver crash during hyperparameter tuning.*
```python
cv = CrossValidator(..., collectSubModels=True)
cv.fit(train_df) # Grid size is 200 combos.
```
**Answer:** Set `collectSubModels=False`.
**Mastery Explanation:** Collecting 200 large ML models (especially GBTs with deep trees) brings hundreds of megabytes into the driver's JVM heap, causing an immediate OOM.

**Q48:** *Fix the non-scalable metric evaluation.*
```python
preds = model.transform(test_df).select("label", "probability").collect()
y_true = [row.label for row in preds]
y_prob = [row.probability[1] for row in preds]
auc = sklearn.metrics.roc_auc_score(y_true, y_prob)
```
**Answer:** Use `BinaryClassificationEvaluator(metricName="areaUnderROC")`.
**Mastery Explanation:** `collect()` pulls all rows to the driver, crashing it on large datasets. The Spark evaluator computes metrics via distributed Spark SQL aggregations.

**Q49:** *Fix the reproducibility failure.*
```python
gbt = GBTClassifier(subsamplingRate=0.7, seed=42)
# Fails reproducibility tests when moving from a 10-node to 20-node cluster
```
**Answer:** `subsamplingRate` relies on `TaskContext.partitionId()`. To guarantee perfect reproducibility, either enforce a static partition layout (`df.repartition(100)`) or set `subsamplingRate=1.0`.
**Mastery Explanation:** Without a stable partition count across environments, the local random seed per partition changes, causing different tree splits.

**Q50:** *Correct the bad file format assumption.*
```python
import pickle
with open("s3://my-model/stages/1_GBT/data/part-0000.pkl", "rb") as f:
    trees = pickle.load(f)
```
**Answer:** Use `spark.read.parquet("s3://my-model/stages/1_GBT/data/")`.
**Mastery Explanation:** Spark ML does not use Python pickles or Java serialization blobs for stage data; it uses structured columnar Parquet files, allowing for standard SQL reads.
