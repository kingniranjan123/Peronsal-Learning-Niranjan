# Sparkling Water API - Elite Technical Assessment

## Part 1: True/False Questions

1. **Question**: `h2oContext.asH2OFrame(df)` performs a true zero-copy memory transfer between Spark and H2O because both frameworks share the same JVM process in the internal backend.
**Answer**: False.
**Mastery Explanation**: Data is transcoded from Spark's Tungsten `UnsafeRow` off-heap format into H2O's proprietary `NewChunk` columnar binary format. While it happens in-process and avoids network I/O (in internal mode), it requires CPU cycles and allocates memory, making it emphatically *not* zero-copy.

2. **Question**: H2OAutoML training triggers a complex DAG of Spark jobs that can be monitored in the Spark UI for task completion.
**Answer**: False.
**Mastery Explanation**: H2OAutoML training is invisible to the Spark DAGScheduler. It delegates to `water.automl.AutoML` running entirely on the H2O cluster. The Spark driver thread is simply blocked waiting for completion.

3. **Question**: In Structured Streaming, `H2OMOJOModel.transform()` requires an active `H2OContext` running in the background to score incoming micro-batches.
**Answer**: False.
**Mastery Explanation**: MOJO scoring relies only on `h2o-genmodel.jar` and uses a self-contained hand-optimized bytecode runtime. It has no dependency on the H2O cluster or `H2OContext`.

4. **Question**: Applying a `filter` operation after converting an H2OFrame back to a DataFrame via `asDataFrame(h2oFrame)` executes as a full table scan because Catalyst predicate pushdown does not apply.
**Answer**: True.
**Mastery Explanation**: Catalyst views the `H2ORDD` as an opaque data source. It cannot push down predicates or column projections into H2O's DKV, so the filter is evaluated only after all rows are materialized into Spark partitions.

5. **Question**: The dummy Spark job `_Sparkling_Water_H2O_Start_` is submitted to force H2O nodes to start only on the Spark driver node.
**Answer**: False.
**Mastery Explanation**: The dummy job forces `water.H2OApp.main()` to execute on *every* executor simultaneously via Spark's `TaskContext` machinery, forming the distributed H2O cloud.

6. **Question**: Enabling dynamic executor allocation (`spark.dynamicAllocation.enabled=true`) can cause `H2OContext.getOrCreate()` to form an H2O cloud with fewer nodes than maximum executors, silently reducing DKV parallelism.
**Answer**: True.
**Mastery Explanation**: If executors are not fully provisioned when the dummy start job runs, the H2O cloud forms only with the active executors. Missing executors joining later do not become part of the DKV ring.

7. **Question**: If a single Spark executor fails during H2OAutoML training, Spark will automatically recover the lost partition via RDD lineage recomputation without interrupting the AutoML job.
**Answer**: False.
**Mastery Explanation**: H2O's DKV has a default replication factor of 1. Losing a node causes a `water.exceptions.H2OAbortException: Cloud shrank`, terminating the training entirely, because H2O cannot rely on Spark's lineage for stateful training.

8. **Question**: The MOJO model bytes are broadcasted to all executors using Spark's `BlockManager` to avoid repeated deserialization per row.
**Answer**: True.
**Mastery Explanation**: `H2OMOJOModel` broadcasts the MOJO zip bytes once to each executor, where they are deserialized once per partition (or executor lifetime) into an `EasyPredictModelWrapper`.

9. **Question**: Stacked Ensemble MOJO models process data faster than single GBM MOJO models because they can evaluate base models in parallel across H2O threads.
**Answer**: False.
**Mastery Explanation**: Stacked Ensemble MOJOs chain multiple base models sequentially. They score significantly slower (e.g., 10K-20K rows/s/core) compared to single GBMs (50K-100K rows/s/core).

10. **Question**: To ensure stability during long H2OAutoML training sessions on Spark, `spark.network.timeout` should be configured to a value lower than `maxRuntimeSecs`.
**Answer**: False.
**Mastery Explanation**: `spark.network.timeout` must *exceed* `maxRuntimeSecs`. Because the Spark driver is blocked, heartbeat mechanisms may pause; a timeout lower than the training duration will cause Spark to prematurely declare executors as lost.

## Part 2: Multiple Choice Questions

11. **Question**: What is the primary architectural motivation for choosing the "external" backend over the "internal" backend in Sparkling Water?
a) To enable zero-copy data transfers via Apache Arrow
b) To isolate H2O's off-heap memory allocations and prevent GC storms in Spark executors
c) To allow the Catalyst optimizer to read directly from H2O's DKV
d) To enable real-time MOJO scoring in Structured Streaming
**Answer**: b
**Mastery Explanation**: In internal mode, H2O pre-allocates memory and shares the JVM heap with Spark. High memory pressure during conversions can cause GC storms and OOMs. The external backend isolates H2O into its own dedicated JVM cluster.

12. **Question**: During the conversion from Spark DataFrame to H2OFrame (`asH2OFrame`), how is the data format transformed?
a) Parquet to MOJO bytes
b) JVM objects to Apache Arrow columnar format
c) Tungsten `UnsafeRow` to H2O's `NewChunk` compressed columnar format
d) RDD objects to DKV UUID hashes
**Answer**: c
**Mastery Explanation**: Spark's memory format is Tungsten `UnsafeRow`. H2O's distributed format is `NewChunk`. The conversion transcodes row-wise Tungsten data into column-wise compressed chunks in the DKV.

13. **Question**: When invoking `H2OContext.getOrCreate()`, how does the Spark driver ensure H2O instances are launched on executors?
a) By deploying a sidecar container using YARN/Kubernetes APIs
b) By broadcasting a Spark UDF that starts an H2O daemon thread
c) By submitting a zero-duration dummy Spark job that runs `water.H2OApp.main()` on every executor
d) By using standard Scala reflection to inject H2O into the SparkContext
**Answer**: c
**Mastery Explanation**: The initialization uses Spark's own scheduling mechanism (a dummy job) to force execution on every active executor, forming a peer-to-peer cloud.

14. **Question**: What is the performance penalty of placing a `.filter()` operation *after* converting an H2OFrame to a DataFrame via `asDataFrame()`?
a) Catalyst cannot push down the predicate to H2O, resulting in a full table scan of the H2ORDD.
b) The Spark UI will show an extra shuffle stage.
c) The data will be serialized to disk before the filter is applied.
d) H2O will apply the filter using its own single-threaded engine.
**Answer**: a
**Mastery Explanation**: Catalyst treats the `H2ORDD` as an opaque source. It must materialize the entire dataset into Spark partitions before evaluating the filter row-by-row, wasting massive memory and CPU.

15. **Question**: Which of the following is TRUE about MOJO model scoring performance in a Spark environment?
a) Deep Learning MOJOs are strictly faster than GBM MOJOs.
b) MOJO models require `H2OContext` to deserialize on executors.
c) MOJO throughput scales with the number of trees/layers, not just the dataset size.
d) MOJOs use Java reflection internally, making them slower than native C++ code.
**Answer**: c
**Mastery Explanation**: The scoring engine (`h2o-genmodel.jar`) evaluates each tree or layer sequentially for a row. Therefore, latency and throughput depend directly on the complexity of the model (e.g., number of trees).

16. **Question**: How is H2O's Distributed Key-Value (DKV) store replicated by default?
a) Replication factor of 3, identical to HDFS.
b) Replication factor of 1, providing no redundancy.
c) Replicated asynchronously across Spark BlockManagers.
d) Replicated synchronously across all executors.
**Answer**: b
**Mastery Explanation**: H2O assumes a stable, in-memory cluster. A replication factor of 1 maximizes memory efficiency but sacrifices fault tolerance, which is why any lost executor aborts the cluster.

17. **Question**: What occurs if `spark.ext.h2o.sys.ai.h2o.mainDriver.memory` is not configured explicitly in internal mode?
a) Spark allocates all its memory to H2O.
b) H2O automatically spills to disk.
c) H2O's `MemoryManager` aggressively pre-allocates memory, competing with Spark's shuffle buffers and potentially causing OOM errors.
d) H2O uses the Spark execution memory pool dynamically.
**Answer**: c
**Mastery Explanation**: H2O operates its own memory heuristics separate from Spark's Unified Memory Manager. Without explicit limits, they collide and crash the JVM during intensive operations.

18. **Question**: In `H2OAutoML`, why is it critical to set `nfolds=5` for production scenarios?
a) To speed up the training process by distributing it across 5 nodes.
b) To yield unbiased leaderboard metrics, preventing the ranking from overfitting to the training data.
c) To force Catalyst to partition the DataFrame into 5 blocks.
d) To enable Stacked Ensembles to train in parallel.
**Answer**: b
**Mastery Explanation**: Without cross-validation, the leaderboard models are evaluated on the exact data they trained on, leading to severe overfitting. `nfolds=5` provides out-of-fold predictions to evaluate actual generalization.

19. **Question**: When configuring a Structured Streaming job with a MOJO model, what controls the maximum number of rows processed per micro-batch?
a) `maxModels` in the H2OAutoML config
b) The replication factor of the DKV
c) The `maxOffsetsPerTrigger` option on the streaming source
d) The number of Spark partition shuffle blocks
**Answer**: c
**Mastery Explanation**: In Structured Streaming (e.g., Kafka source), `maxOffsetsPerTrigger` bounds the micro-batch size. It must be tuned to match the MOJO's scoring throughput multiplied by the executor core count.

20. **Question**: Which class acts as the self-contained scoring engine wrapper for MOJO bytes on an executor?
a) `SparkSession`
b) `H2OFrameRDD`
c) `EasyPredictModelWrapper`
d) `ModelMetrics`
**Answer**: c
**Mastery Explanation**: `EasyPredictModelWrapper` is part of `h2o-genmodel.jar`. It accepts raw data objects and executes the compiled model bytecode to return predictions, independently of any cluster.

21. **Question**: What is the time complexity of `asDataFrame(h2oFrame)`?
a) O(n * c) where n is rows and c is columns.
b) O(1) setup, followed by O(n) scan when materialized.
c) O(n log n) due to sorting in the DKV.
d) O(c) because it only reads schema metadata.
**Answer**: b
**Mastery Explanation**: Creating the DataFrame is metadata-only (O(1)). Materializing it only requires a thin wrapper scan over the existing in-process DKV chunks (O(n) read).

22. **Question**: During the `asH2OFrame` conversion, what is the typical throughput limit per executor core?
a) 10-20 MB/s
b) 200-400 MB/s
c) 1-2 GB/s
d) Unlimited (Zero-copy)
**Answer**: b
**Mastery Explanation**: The CPU bottleneck of transcoding Tungsten binary formats into H2O chunk compression limits throughput to ~200-400 MB/s per core.

23. **Question**: If you restrict H2OAutoML using `include_algos=["GBM", "XGBoost"]`, what is the primary operational goal?
a) To reduce the DKV memory footprint.
b) To guarantee sub-100ms latency SLAs during downstream MOJO scoring by avoiding slow DL/Ensemble models.
c) To enable Catalyst predicate pushdown.
d) To allow Spark to compute the models instead of H2O.
**Answer**: b
**Mastery Explanation**: Tree-based algorithms score extremely fast (50K+ rows/s/core). Constraining AutoML ensures the winning model can meet stringent real-time streaming latency requirements.

24. **Question**: Why might an `asH2OFrame()` operation take significantly longer than a standard Spark `count()`?
a) DKV write contention caused by too many Spark partitions writing to too few H2O nodes simultaneously.
b) The MOJO model is compiling to C++.
c) Spark is writing data to HDFS first.
d) H2O is performing cross-validation during the conversion.
**Answer**: a
**Mastery Explanation**: If `spark.sql.shuffle.partitions` is extremely high, hundreds of Spark tasks try to write into a small number of H2O node chunk buffers simultaneously, creating lock contention.

25. **Question**: What is a MOJO fundamentally?
a) A Docker container running H2O.
b) A Python pickle file containing Spark RDDs.
c) A self-contained zip file containing model tree structures and scoring logic in H2O's portable binary format.
d) A shared library (.so or .dll) compiled natively.
**Answer**: c
**Mastery Explanation**: MOJO stands for Model ObJect, Optimized. It is a ZIP file storing binary parameters and metadata, entirely independent of the JVM that created it.

## Part 3: Small Twist Questions

26. **Scenario**: You deploy a cluster with `spark.executor.memory=8g` and default settings. The cluster frequently OOMs during `asH2OFrame`.
**Twist**: You change the setting to `spark.ext.h2o.backend.cluster.mode=external` and start a standalone H2O cluster, but leave executor memory at 8g. What happens to the OOMs?
**Answer**: The Spark OOMs during conversion disappear.
**Mastery Explanation**: By switching to external mode, H2O's heavy memory footprint is removed from the Spark executor JVM. Spark's 8g heap is now entirely dedicated to Spark overhead and Tungsten buffers, resolving the contention.

27. **Scenario**: You filter a DataFrame, then convert it: `hc.asH2OFrame(df.filter(col("x") > 0))`. This takes 2 minutes.
**Twist**: You change the code to: `hc.asDataFrame(hc.asH2OFrame(df)).filter(col("x") > 0)`. What happens to the conversion time and memory footprint?
**Answer**: Conversion time skyrockets and memory footprint increases drastically.
**Mastery Explanation**: In the twist, the entire un-filtered dataset is transcoded into H2O format in memory (wasting time and DKV space), and *then* filtered via a full table scan, bypassing Catalyst pushdown.

28. **Scenario**: You run `H2OContext.getOrCreate()` and the H2O cloud forms with 10 nodes matching your 10 executors.
**Twist**: You enable `spark.dynamicAllocation.enabled=true` with a minimum of 2 executors and max of 10. You immediately call `H2OContext.getOrCreate()`. What is the cloud size?
**Answer**: The cloud size is likely 2 (or slightly higher if it scales up mid-startup).
**Mastery Explanation**: `getOrCreate()` triggers immediately on the currently available executors. If dynamic allocation hasn't spun up the other 8 executors yet, the H2O cloud forms with only the pre-warmed nodes.

29. **Scenario**: A GBM MOJO model scores streaming Kafka data at 80,000 rows/second per core.
**Twist**: You retrain the model, and the new winner is a Stacked Ensemble of 5 models. You deploy the new MOJO to the exact same streaming pipeline. What happens to the Kafka lag?
**Answer**: Kafka lag builds up rapidly because throughput drops by roughly 5-10x.
**Mastery Explanation**: Stacked Ensembles execute base models sequentially. The throughput will drop to ~10,000-20,000 rows/s/core, which will throttle the micro-batch processing and cause the stream to fall behind.

30. **Scenario**: Your AutoML training takes 90 seconds. `spark.network.timeout` is the default 120s. It completes successfully.
**Twist**: You increase `maxRuntimeSecs` to 600 seconds to get better models. You leave `spark.network.timeout` at 120s. What happens?
**Answer**: The Spark job fails with a "Lost executor" or "Connection timeout" error before AutoML finishes.
**Mastery Explanation**: The driver thread blocks during AutoML. Spark's heartbeat mechanism assumes the executors/driver are dead if they don't communicate within the network timeout window, killing the context.

31. **Scenario**: You map a String column containing categories to an `H2OFrame`. It processes normally.
**Twist**: You cast the String column to an Integer type *before* conversion. What happens to H2O's treatment of the column during training?
**Answer**: H2O treats the feature as a continuous numerical variable instead of a categorical one, fundamentally changing the model's tree splits.
**Mastery Explanation**: H2O infers types. Strings become categoricals. Integers become numerics. You must explicitly call `.asfactor()` on the H2OFrame if you want an integer column treated as categorical.

32. **Scenario**: You run a 10-executor cluster with `spark.sql.shuffle.partitions=200`. `asH2OFrame()` takes 10 seconds.
**Twist**: You set `spark.sql.shuffle.partitions=10000` to handle data skew upstream, then call `asH2OFrame()`. What happens to conversion time?
**Answer**: Conversion time slows down drastically due to DKV write contention.
**Mastery Explanation**: 10,000 concurrent Spark tasks will attempt to write to 10 H2O nodes. The heavy locking and thread contention on H2O's `NewChunk.addNum()` will severely throttle throughput.

33. **Scenario**: You train an AutoML pipeline: `pipeline_model = Pipeline(stages=[automl]).fit(train_df)`. You deploy the pipeline.
**Twist**: You forgot to pass the validation data to AutoML, leaving `nfolds=0` (disabled). You check the leaderboard. What happens to the reported AUCs?
**Answer**: The leaderboard AUCs are absurdly high (near 1.0) due to massive overfitting.
**Mastery Explanation**: Without cross-validation or a validation frame, H2O evaluates the models on the training data. Complex models (like Deep Learning or deep trees) will memorize the training set, ruining out-of-sample accuracy.

34. **Scenario**: You use `H2OMOJOModel` in Structured Streaming. You include `H2OContext.getOrCreate()` at the top of your script.
**Twist**: You remove `H2OContext.getOrCreate()` entirely and rely only on `h2o-genmodel.jar`. What happens to the streaming job?
**Answer**: The streaming job runs perfectly and consumes significantly less memory.
**Mastery Explanation**: MOJO scoring does not use the H2O cluster or DKV. Removing `H2OContext` eliminates the heavy embedded H2O nodes from the executors, freeing up heap space.

35. **Scenario**: You set `spark.executor.memoryOverhead=512m` (default). You run a small dataset conversion, and it passes.
**Twist**: You run a large dataset that creates millions of H2O `Chunk` objects. What crashes?
**Answer**: The Spark executors are killed by the OS (YARN/K8s) for exceeding memory limits.
**Mastery Explanation**: H2O uses off-heap direct memory for its `H2O Store` (in modern versions) and network buffers. If `memoryOverhead` is too small, the JVM process expands beyond the container limits and is hard-killed by the OOMKiller.

36. **Scenario**: During internal backend execution, a worker node is preempted by the cloud provider. Spark lineage recomputes the lost RDD partitions.
**Twist**: The preempted node contained a segment of the H2O DKV. What happens to the H2O Context?
**Answer**: The H2O Context permanently crashes with `H2OAbortException: Cloud shrank`.
**Mastery Explanation**: H2O does not support lineage-based recovery. If a node holding DKV state is lost, the distributed state is corrupted, and the entire H2O cloud immediately aborts.

37. **Scenario**: You score a MOJO using `predict()` on a single row. It returns a `prediction` field.
**Twist**: The MOJO is a binary classifier, but you need the raw probabilities. How do you extract them in Spark?
**Answer**: You select the `p0` and `p1` fields from the output struct generated by `H2OMOJOModel.transform()`.
**Mastery Explanation**: For classification, the MOJO outputs a struct containing the class label (`prediction`) and the individual probabilities (`p0`, `p1`, etc.).

38. **Scenario**: You convert a dataset: `h2o_df = hc.asH2OFrame(df)`. The schema shows a `StringType`.
**Twist**: The DataFrame contains millions of unique UUID strings. What happens during conversion?
**Answer**: The conversion takes an exceptionally long time and consumes massive DKV memory.
**Mastery Explanation**: H2O dictionary-encodes strings. Millions of unique strings create a massive internal hash map and dictionary, blowing up the JVM heap and severely degrading transcoding performance.

39. **Scenario**: Your `Pipeline` includes a `VectorAssembler` before `H2OAutoML`.
**Twist**: You remove the `VectorAssembler` and pass the raw columns directly to `featuresCols` in `H2OAutoML`. What happens?
**Answer**: The model trains perfectly and potentially faster.
**Mastery Explanation**: H2O does not use Spark's `DenseVector` format. Passing raw columns directly is preferred; if a Vector is passed, H2O must unpack it back into columnar arrays internally anyway.

40. **Scenario**: In a streaming job, `maxOffsetsPerTrigger` is 100,000. Your GBM MOJO processes this in 1 second.
**Twist**: You upgrade to a 5-layer Deep Learning MOJO. What happens to the micro-batch processing time?
**Answer**: The micro-batch processing time spikes to 10-20 seconds.
**Mastery Explanation**: Matrix multiplication in DL MOJOs is computationally heavy compared to tree traversal. Throughput drops from ~80K to ~5K rows/s/core, causing processing times to skyrocket for the same batch size.

## Part 4: Coding & Debugging Questions

41. **Code Debugging**:
```python
df = spark.read.parquet("data/")
h2o_frame = hc.asH2OFrame(df)
train_frame = hc.asDataFrame(h2o_frame).filter("age > 18")
automl = H2OAutoML(labelCol="target", maxRuntimeSecs=300).fit(train_frame)
```
**Issue**: Identify the critical performance flaw.
**Fix/Explanation**: The filter is applied *after* `asH2OFrame()`. The entire dataset is transcoded to H2O format, wrapped back in a DataFrame, and then a full table scan evaluates the filter. The filter `df.filter("age > 18")` must be applied *before* `hc.asH2OFrame()` to leverage Catalyst pushdown and minimize conversion overhead.

42. **Code Debugging**:
```scala
val spark = SparkSession.builder()
  .config("spark.dynamicAllocation.enabled", "true")
  .getOrCreate()
val hc = H2OContext.getOrCreate()
val h2oFrame = hc.asH2OFrame(largeDf)
```
**Issue**: Explain the cluster topology risk here.
**Fix/Explanation**: Dynamic allocation means executors are added lazily. `H2OContext.getOrCreate()` forms the DKV cloud with whatever executors are alive at that exact millisecond. If only 2 out of 50 executors are up, H2O forms a 2-node cloud, severely crippling distributed memory and compute. Disable dynamic allocation or force full materialization.

43. **Code Debugging**:
```python
automl = H2OAutoML(
    labelCol="is_fraud",
    maxRuntimeSecs=3600,  # 1 hour
    include_algos=["DeepLearning", "StackedEnsemble"]
)
pipeline = Pipeline(stages=[automl]).fit(df)
```
**Issue**: If Spark is configured with default timeouts, what exception will occur at minute 2?
**Fix/Explanation**: The Spark driver blocks during `fit()`. If `spark.network.timeout` is at the 120s default, Spark will declare executors unresponsive and crash. Fix: Set `spark.network.timeout=4000s`.

44. **Code Debugging**:
```python
h2o_frame = hc.asH2OFrame(df)
h2o_frame["target"] = h2o_frame["target"].asnumeric()
# 'target' is 0 or 1 integers
automl = H2OAutoML(labelCol="target").fit(df)
```
**Issue**: What type of machine learning problem will H2O solve?
**Fix/Explanation**: H2O will treat this as a *regression* problem because the target is numeric. For binary classification, you must cast the label to categorical using `.asfactor()` instead of `.asnumeric()`.

45. **Code Debugging**:
```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.ext.h2o.backend.cluster.mode", "internal") \
    .getOrCreate()
```
**Issue**: What is the most likely error when calling `asH2OFrame` on 100GB of data?
**Fix/Explanation**: `java.lang.OutOfMemoryError: Java heap space`. 4GB is too small for Spark execution memory + H2O pre-allocations + H2O chunk ingestion buffers. Fix: Increase heap to 12g+ and explicitly set `spark.ext.h2o.sys.ai.h2o.mainDriver.memory`.

46. **Code Debugging**:
```python
scored_df = mojo_model.transform(df)
result = scored_df.select("prediction")
```
**Issue**: The user wants to write logic: `if probability > 0.9 then flag`. Why does this fail?
**Fix/Explanation**: The `prediction` column for a classifier only outputs the discrete class (e.g., 0 or 1). To get probabilities, the user must select `p1` (or `p0`) from the output struct: `scored_df.select(col("p1").alias("prob"))`.

47. **Code Debugging**:
```python
mojo_model = H2OMOJOModel.createFromMojo("model.zip")
stream = spark.readStream.format("kafka").load()
scored = mojo_model.transform(stream)
```
**Issue**: A developer is confused why there is no `H2OContext` initialized in this script. Is this a bug?
**Fix/Explanation**: No, this is architecturally correct. `H2OMOJOModel.transform()` uses `h2o-genmodel.jar` which evaluates the MOJO zip directly in the Spark executor JVM. It has absolutely no dependency on the H2O DKV or `H2OContext`.

48. **Code Debugging**:
```python
h2o_frame = hc.asH2OFrame(df.repartition(10000))
```
**Issue**: The cluster has 5 executors. What happens during conversion?
**Fix/Explanation**: Severe DKV write contention. 10,000 tasks will pound 5 H2O nodes simultaneously. `NewChunk` appending becomes heavily locked. Fix: Repartition to a smaller multiple of H2O nodes (e.g., `num_nodes * 4 = 20`) before conversion.

49. **Code Debugging**:
```scala
val spark = SparkSession.builder.getOrCreate()
val hc1 = H2OContext.getOrCreate()
// ... later in the code ...
val hc2 = H2OContext.getOrCreate()
```
**Issue**: Does `hc2` spawn a second independent H2O cluster in the executors?
**Fix/Explanation**: No. `H2OContext` is a singleton. `getOrCreate()` checks the cached instance and returns `hc1`. Only one H2O cluster can coexist within a single Spark JVM cluster.

50. **Code Debugging**:
```python
automl = H2OAutoML(labelCol="y", maxModels=50, nfolds=0)
pipeline = Pipeline(stages=[automl])
```
**Issue**: The leaderboard shows DL models winning, but in production, they perform terribly on new data. Why?
**Fix/Explanation**: `nfolds=0` disables cross-validation. The AutoML leaderboard evaluated models purely on training data. Deep Learning memorized the training set (overfitting). Fix: Set `nfolds=5` to force out-of-fold validation.
