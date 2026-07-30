# 🔥 Master Class: Sparkling Water API

## Overview

Sparkling Water is the official H2O.ai bridge library that lets Apache Spark and the H2O machine-learning runtime share the same JVM process and—critically—the same physical memory pages. Without Sparkling Water, a data scientist would be forced to serialize a Spark DataFrame to disk or over the network, deserialize it inside a separate H2O cluster, train a model, serialize the MOJO artifact back, and then reload it inside Spark for scoring. That round-trip costs minutes of wall-clock time and gigabytes of intermediate I/O. Sparkling Water collapses this pipeline: data lives in one place, and both runtimes read it directly.

The library ships in two flavors. **Internal backend** launches H2O worker nodes directly inside each Spark executor JVM—H2O and Spark threads share the same heap. **External backend** connects Spark to a separately managed H2O cluster, which is preferred when H2O's aggressive off-heap memory usage (H2O stores its `H2OFrame` data in a custom off-heap binary format called the *H2O Store*) would otherwise compete with Spark's execution memory pool and cause GC storms. Choosing the wrong backend for a workload is the single most common production failure mode for Sparkling Water deployments.

The API surface is intentionally minimal: `H2OContext`, the conversion implicits/methods between `H2OFrame` and Spark `DataFrame`, the `H2OAutoML` estimator that plugs into a `Pipeline`, and the `H2OMOJOModel` transformer for low-latency inference. Understanding each piece—and the JVM plumbing beneath each—is the difference between a proof-of-concept and a production ML platform.

---

## 🏗️ Architectural Deep Dive

### How It Works Under the Hood

When you call `H2OContext.getOrCreate(spark)`, the driver JVM starts an embedded H2O node via `water.H2OApp.main()`. This node joins an H2O cloud—either by forming one locally (internal backend) or by connecting to an external cluster via a flatfile of IP:port pairs. On each executor, a corresponding H2O node is started inside the executor's JVM through Spark's `TaskContext` machinery: a zero-duration dummy Spark job is submitted whose only purpose is to run `water.H2OApp.main()` on every executor simultaneously. The H2O cloud forms through multicast or the flatfile, and from that point forward both the Spark scheduler and the H2O DKV (Distributed Key-Value store) coexist in the same JVM cluster.

The DKV is H2O's distributed in-memory store. Every `H2OFrame` chunk is a `Chunk` object stored in the DKV under a UUID key, distributed across H2O nodes by consistent hashing. The data itself is stored in H2O's proprietary compressed columnar binary format (not Apache Arrow, not Parquet). When you call `h2oContext.asH2OFrame(dataFrame)`, Spark triggers an action that scans each partition; the `H2OFrameRDD` writer serializes each Spark `InternalRow` (already in Tungsten's off-heap binary format, using `UnsafeRow`) into H2O's `NewChunk` format column-by-column. This is emphatically **not** zero-copy for the initial import—data is transcoded—but once resident in the DKV, the reverse conversion `asDataFrame(h2oFrame)` wraps each DKV chunk in a thin Spark `Partition` and reads it with near-zero overhead because both the H2O node and the Spark executor are in the same JVM process.

The Catalyst optimizer has no visibility into H2O operations. When you call `h2oContext.asDataFrame(frame)`, the resulting `DataFrame` appears to Catalyst as an opaque scan on a custom `H2ORDD`. This means **predicate pushdown does not apply**: any `filter` you add after conversion will execute as a full table scan on the H2O-backed RDD before Spark prunes rows. For production workloads, apply all Spark-side filters on the raw `DataFrame` *before* calling `asH2OFrame`, so that Catalyst's logical optimization phase can push predicates down to the Parquet/Delta reader, and only the filtered result is transcoded into H2O format.

AutoML uses H2O's `AutoML` Java API under the hood, which runs entirely on the H2O cluster—not on the Spark DAGScheduler. The Spark driver thread blocks on `trainModels()` via H2O's Future mechanism while H2O's internal scheduler trains and cross-validates up to `maxModels` models. The resulting leaderboard `ModelMetrics` objects live in the DKV. When you call `getBestModel()`, the winning model is serialized into a MOJO (Model ObJect, Optimized)—a self-contained zip file containing the model tree structure and scoring logic in H2O's portable binary format. The MOJO has no JVM dependency: it is scored by the `h2o-genmodel.jar` runtime, which uses hand-optimized bytecode that bypasses reflection and achieves throughput within 2–5x of native compiled C++ inference code.

```
Driver JVM
┌────────────────────────────────────────────────────────────────┐
│  SparkSession                                                  │
│  ┌──────────────┐    asH2OFrame()     ┌──────────────────────┐│
│  │  DataFrame   │──── transcode ─────▶│  H2O DKV Node(Driver)││
│  │  (Tungsten   │                     │  (H2OFrame chunks)   ││
│  │  UnsafeRow)  │◀─── wrap RDD  ──────│                      ││
│  └──────────────┘    asDataFrame()    └──────────┬───────────┘│
│                                                  │ H2O Cloud  │
│  H2OContext ──── getOrCreate() ──▶ Embedded H2O  │            │
└──────────────────────────────────────────────────│────────────┘
                                                   │ DKV replication
Executor JVM #1                    Executor JVM #2 │
┌──────────────────────┐           ┌───────────────┴──────────┐
│ Spark ThreadPool     │           │ Spark ThreadPool         │
│ ┌──────────────────┐ │           │ ┌──────────────────────┐ │
│ │ Task (Partition N)│ │           │ │ Task (Partition M)   │ │
│ └──────────────────┘ │           │ └──────────────────────┘ │
│ H2O DKV Node         │◀─ gossip ▶│ H2O DKV Node            │
│ (Chunk storage,      │           │ (Chunk storage,          │
│  off-heap H2O Store) │           │  off-heap H2O Store)     │
└──────────────────────┘           └──────────────────────────┘

AutoML Flow (runs on H2O cluster, not Spark DAGScheduler):
┌───────────────────────────────────────────────────┐
│  H2OAutoML.train()                                │
│  ├─▶ GBM (5-fold CV) ──▶ ModelMetrics ──▶ DKV    │
│  ├─▶ XGBoost          ──▶ ModelMetrics ──▶ DKV    │
│  ├─▶ Deep Learning    ──▶ ModelMetrics ──▶ DKV    │
│  └─▶ Stacked Ensemble ──▶ ModelMetrics ──▶ DKV    │
│                                ↓                  │
│              getBestModel() → MOJO export         │
└───────────────────────────────────────────────────┘

Structured Streaming MOJO Scoring:
Kafka ──▶ readStream ──▶ H2OMOJOModel.transform() ──▶ writeStream
           (Micro-batch)   (h2o-genmodel.jar,            (Delta / Kafka)
                            no H2OContext needed)
```

### Key Internal Components

- **H2OContext:** The singleton bridge object that initializes H2O within Spark's JVM cluster. Internally it submits a `_water_start_` dummy job to force `H2OApp.main()` on all executors before returning, ensuring the H2O cloud is fully formed and the DKV ring is stable. Calling `getOrCreate` multiple times is safe—it checks `H2OContext._instances` and returns the cached context.

- **H2O DKV (Distributed Key-Value Store):** H2O's distributed memory layer. Every `H2OFrame`, model, and `ModelMetrics` object is a DKV value. Replication uses a consistent-hash ring; the replication factor is 1 by default (no redundancy), which is why losing a single H2O node during training causes `water.exceptions.H2OAbortException: Cloud shrank`.

- **MOJO Runtime (`h2o-genmodel.jar`):** The self-contained scoring engine for exported models. It implements `EasyPredictModelWrapper`, which accepts a `RowData` object and returns a `AbstractPrediction`. In Structured Streaming, `H2OMOJOModel` wraps this into a Spark `Transformer` that applies `EasyPredictModelWrapper.predict()` row-by-row inside a `mapPartitions` closure, broadcasting the MOJO bytes to each executor via Spark's `broadcast` variable mechanism—avoiding repeated deserialization per row.

- **`H2OAutoML` Spark Estimator:** Implements Spark's `Estimator[H2OMOJOModel]` interface, making it a first-class `Pipeline` stage. Internally it converts the training `DataFrame` to an `H2OFrame`, delegates to `water.automl.AutoML`, blocks the Spark driver thread until `maxRuntimeSecs` or `maxModels` is reached, exports the winning model as a MOJO, wraps it in `H2OMOJOModel`, and returns it. The entire H2O training computation is invisible to the Spark UI—no Spark jobs appear during training.

---

## ⚠️ Critical Concepts & Common Pitfalls

### The Internal Backend Memory War

In internal backend mode, H2O nodes and Spark executors compete for the same executor JVM heap. H2O's `MemoryManager` aggressively pre-allocates memory up to `sys.ai.h2o.heartbeat.benchmark.interval` and uses its own GC heuristics separate from the JVM GC. A common failure pattern is configuring `spark.executor.memory=8g` without accounting for H2O's overhead: H2O pre-allocates roughly 10% of the JVM heap for metadata plus allocates `NewChunk` buffers in the JVM heap during frame ingestion. With both Spark's shuffle buffers and H2O's frame ingestion active simultaneously, the executor hits `java.lang.OutOfMemoryError: Java heap space` during the `asH2OFrame` conversion on large partitions.

The production solution is to use external backend mode or to configure `spark.ext.h2o.sys.ai.h2o.mainDriver.memory` explicitly, set `spark.executor.memoryOverhead` to at least 15% of executor memory, and reduce `spark.sql.shuffle.partitions` to lower peak Spark memory pressure during the conversion window.

### MOJO Scoring Latency vs. Throughput Tradeoff

`H2OMOJOModel` in Structured Streaming applies `EasyPredictModelWrapper.predict()` inside a `mapPartitions` UDF. Each executor deserializes the MOJO bytes once per partition (from the broadcast variable) and reuses the `EasyPredictModelWrapper` instance across all rows in that partition—this is the `transformSchema`-safe, thread-local pattern. However, GBM and XGBoost MOJOs score at different rates: a GBM MOJO with 500 trees scores approximately 50,000–100,000 rows/second per executor core, while a deep learning MOJO is 10–20x slower due to matrix multiplication overhead. Stacked Ensemble MOJOs chain multiple base model scorers sequentially; with 5 base models, throughput drops to roughly 10,000–20,000 rows/second per core. For sub-100ms latency SLAs, constrain AutoML to `include_algos=["GBM", "XGBoost"]` and export only tree-based MOJOs.

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|-----------|---------|-------|
| `asH2OFrame(df)` | O(n × c) | No | Full data transcode from Tungsten UnsafeRow to H2O NewChunk; ~200–400 MB/s per executor core |
| `asDataFrame(frame)` | O(1) setup, O(n) scan | No | Wraps DKV chunks in thin Spark partitions; read throughput ~500–800 MB/s per core on local executor |
| `H2OAutoML.fit()` | O(models × folds × n) | No (H2O-internal) | Invisible to Spark DAGScheduler; all computation runs on H2O cluster; driver thread blocks |
| `H2OMOJOModel.transform()` | O(n × trees) per row | No | GBM: ~50K–100K rows/s/core; DL: ~5K–10K rows/s/core; Stacked Ensemble: ~10K–20K rows/s/core |

---

## 💻 Code Examples

### Example 1: H2OContext Initialization with Internal vs. External Backend Selection

> **What this demonstrates:** How `H2OContext.getOrCreate()` differs between internal and external backend configurations, and how memory partitioning must be adjusted at the `SparkConf` level before the context is created—not after.

```scala
import ai.h2o.sparkling._
import org.apache.spark.sql.SparkSession

// STEP 1: Configure Spark session BEFORE H2OContext is created.
// H2OContext reads these conf values at init time; changing them after getOrCreate() has no effect.
val spark = SparkSession.builder()
  .appName("SparklingWaterDemo")
  // Allocate enough executor memory to accommodate both Spark shuffle buffers
  // and H2O NewChunk ingestion buffers during asH2OFrame() conversion.
  .config("spark.executor.memory", "12g")
  // memoryOverhead covers H2O's off-heap metadata and direct ByteBuffer allocations.
  // H2O uses direct memory for its store on newer versions (H2O 3.36+).
  .config("spark.executor.memoryOverhead", "2048")   // 2 GB off-heap per executor
  // Internal backend: H2O runs inside each executor JVM.
  // Use "external" if H2O memory pressure causes GC storms (OldGen > 80%).
  .config("spark.ext.h2o.backend.cluster.mode", "internal")
  // Limit H2O's in-memory store to 60% of executor heap to leave room for Spark.
  .config("spark.ext.h2o.sys.ai.h2o.mainDriver.memory", "7g")
  // Log H2O cluster formation events; useful for diagnosing "cloud shrank" errors.
  .config("spark.ext.h2o.log.level", "WARN")
  .getOrCreate()

// STEP 2: Initialize H2OContext.
// Internally this submits a dummy Spark job to start water.H2OApp.main() on every executor.
// The call BLOCKS until all executor H2O nodes have joined the cloud.
// If any executor fails to start H2O within spark.ext.h2o.cluster.start.timeout (default 120s),
// it throws H2OClusterNotReachableException.
val hc: H2OContext = H2OContext.getOrCreate()

// STEP 3: Verify cluster formation — critical for debugging node count mismatches.
// H2O cloud size MUST equal the number of active Spark executors.
// A mismatch (e.g., lazy executor allocation) causes DKV replication gaps.
println(s"H2O cluster size: ${hc.getH2ONodes().length}")
println(s"Spark executors:  ${spark.sparkContext.getExecutorMemoryStatus.size - 1}") // subtract driver
println(s"H2O version:      ${hc.getH2OVersion()}")
println(s"Backend:          ${hc.getConf.backendClusterMode}")
```

> **Mastery Note:** The dummy job that starts H2O on executors is a critical but invisible step—it appears in the Spark UI as a zero-second job with description `_Sparkling_Water_H2O_Start_`. If dynamic executor allocation is enabled (`spark.dynamicAllocation.enabled=true`), Spark may not have all executors materialized when `getOrCreate()` runs, causing the H2O cloud to form with fewer nodes than expected. This produces a smaller-than-expected DKV ring, silently reducing parallelism without throwing an exception. Always set `spark.dynamicAllocation.enabled=false` or pre-warm all executors before calling `H2OContext.getOrCreate()` in production. A cloud size smaller than executor count is the root cause of 80% of "my AutoML is slow" support tickets.

---

### Example 2: Zero-Copy-Pattern DataFrame ↔ H2OFrame Conversion with Filter-Before-Convert

> **What this demonstrates:** The correct ordering of Spark-side filter operations relative to `asH2OFrame()` conversion, and why invoking filters after conversion is an invisible but catastrophic performance anti-pattern that Catalyst cannot detect.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, year
from pysparkling import H2OContext

spark = SparkSession.builder.appName("ConversionDemo").getOrCreate()
hc = H2OContext.getOrCreate()

# Load raw event data from Delta Lake.
# Catalyst's logical optimizer will read this lazily — no data movement yet.
raw_df = spark.read.format("delta").load("/data/events/")

# ── CORRECT PATTERN ──────────────────────────────────────────────────────────
# Apply ALL filters and projections BEFORE calling asH2OFrame().
# Catalyst will push these predicates into the Delta reader (file skipping,
# partition pruning, column projection), so only the required columns and rows
# are materialized into Tungsten UnsafeRow format.
# The resulting DataFrame that gets transcoded into H2O NewChunk format is
# as small as possible, minimizing conversion time and DKV memory usage.
filtered_df = (
    raw_df
    # Partition pruning: Delta log knows which partitions contain 2024 data.
    .filter(col("event_year") == 2024)
    # Column pruning: Catalyst's project pushdown removes unreferenced columns
    # from the Parquet/Delta scan entirely; they never enter the JVM heap.
    .select("user_id", "session_duration_s", "page_views", "converted")
    # Cast before conversion: H2O natively handles DoubleType and IntegerType;
    # StringType triggers an extra dictionary-encoding step inside NewChunk.
    .withColumn("session_duration_s", col("session_duration_s").cast("double"))
    .na.drop(subset=["converted"])  # H2O treats NA as valid; explicit drop is safer for labels
)

# ── ANTI-PATTERN (DO NOT DO THIS) ────────────────────────────────────────────
# BAD: Convert THEN filter. The filter runs as a Spark mapPartitions on the
# H2ORDD (Catalyst sees it as an opaque source), performing a full table scan
# AFTER all data has already been transcoded to H2O format. This wastes
# conversion time AND DKV memory for data you immediately discard.
# bad_frame = hc.asH2OFrame(raw_df)  # Full dataset transcoded — wasteful
# bad_df = hc.asDataFrame(bad_frame).filter(col("event_year") == 2024)  # Too late

# STEP: Convert filtered DataFrame to H2OFrame.
# This triggers a Spark action: all partitions are computed and written to the DKV.
# Each executor writes its partition's rows directly to the co-located H2O node.
# Conversion throughput: ~200-400 MB/s per executor core in internal backend mode.
h2o_frame = hc.asH2OFrame(filtered_df)
h2o_frame.set_names(["user_id", "session_s", "page_views", "label"])

# Mark 'label' as categorical for classification (tells H2O to train a classifier).
# Without this, H2O treats integer labels as a regression target.
h2o_frame["label"] = h2o_frame["label"].asfactor()

print(f"H2OFrame shape: {h2o_frame.shape}")         # (rows, cols)
print(f"H2OFrame in DKV: {h2o_frame.frame_id}")     # UUID key in the distributed KV store
print(f"Frame distribution: {h2o_frame.chunk_summary()}")  # Shows chunk distribution across nodes
```

> **Mastery Note:** The `asH2OFrame()` call triggers a Spark job visible in the Spark UI as a standard `collect`-like action — you will see tasks for each partition completing. The conversion throughput is bounded by the *slower* of Spark's partition read speed and H2O's `NewChunk.addNum()` ingestion speed. If you observe this job taking more than 2× longer than a simple `df.count()` on the same data, the bottleneck is DKV write contention caused by too many partitions writing to too few H2O nodes simultaneously. Tuning `spark.sql.shuffle.partitions` down (or calling `repartition(numH2ONodes * 4)` before conversion) resolves this. The resulting H2OFrame's chunk count will match the DataFrame's partition count exactly.

---

### Example 3: H2OAutoML as a Spark Pipeline Stage with Cross-Validation and Leaderboard Inspection

> **What this demonstrates:** Embedding `H2OAutoML` inside a Spark `Pipeline` alongside standard Spark ML transformers, and inspecting the AutoML leaderboard to understand which algorithm class won and why.

```python
from pysparkling.ml import H2OAutoML
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql import SparkSession
from pysparkling import H2OContext

spark = SparkSession.builder.appName("AutoMLPipeline").getOrCreate()
hc = H2OContext.getOrCreate()

# Load pre-filtered training data (filters applied BEFORE reaching the pipeline).
train_df = spark.read.parquet("/data/train/")
test_df  = spark.read.parquet("/data/test/")

# H2OAutoML accepts a featuresCols list directly; no VectorAssembler needed
# because H2O does NOT use Spark's DenseVector format internally.
# However, wrapping in a Pipeline stage allows MLflow autologging to capture
# the full transform graph.

# Configure H2OAutoML estimator.
# maxRuntimeSecs=600: H2O will train and cross-validate models for up to 10 minutes.
# nfolds=5: 5-fold stratified CV for each candidate model (doubles training time but
#           gives unbiased leaderboard metrics; without it, leaderboard overfits to train data).
# include_algos: Restricting to GBM/XGBoost ensures MOJO scoring throughput > 50K rows/s/core.
#                Remove this restriction only if latency is not a concern.
automl = H2OAutoML(
    featuresCols     = ["session_s", "page_views"],
    labelCol         = "label",
    maxRuntimeSecs   = 600,
    maxModels        = 20,
    nfolds           = 5,
    include_algos    = ["GBM", "XGBoost"],   # Exclude DL and GLM for latency-sensitive scoring
    seed             = 42,                    # Reproducibility: fixes H2O's internal RNG
    sortMetric       = "AUC",                 # Leaderboard sort key
    keepCrossValidationPredictions = False,   # Set True only if you need CV predictions for stacking
)

# Build the pipeline. H2OAutoML.fit() converts the DataFrame to H2OFrame internally,
# delegates to water.automl.AutoML, blocks the driver thread, and returns H2OMOJOModel.
# The Spark UI will show NO jobs during the AutoML training phase — all work is on H2O.
pipeline = Pipeline(stages=[automl])
pipeline_model = pipeline.fit(train_df)

# Retrieve the H2OMOJOModel from the fitted pipeline.
mojo_model = pipeline_model.stages[-1]

# Inspect the AutoML leaderboard — this is a Spark DataFrame backed by an H2OFrame.
leaderboard_df = mojo_model.getLeaderboard("ALL")
leaderboard_df.select("model_id", "auc", "logloss", "training_time_ms").show(10, truncate=False)

# The winning algorithm and its AUC reveal the dataset's signal complexity.
# A GBM winning with AUC > 0.95 at maxModels=20 suggests low-dimensional tabular signal;
# a Stacked Ensemble winning suggests complex feature interactions that no single algorithm captures.
best_algo = leaderboard_df.select("model_id").first()[0]
print(f"Best model: {best_algo}")   # e.g., "GBM_1_AutoML_20240101_120000"

# Score the test set through the pipeline.
predictions_df = pipeline_model.transform(test_df)
predictions_df.select("label", "prediction", "p0", "p1").show(5)
```

> **Mastery Note:** The `H2OAutoML` estimator is invisible to the Spark DAGScheduler during the training phase—the entire `water.automl.AutoML` computation runs on the H2O cluster, consuming DKV memory and H2O threads. The Spark driver thread is simply blocked on a Java `Future.get()`. This has an important implication: Spark's speculative execution, task retry, and heartbeat mechanisms are all paused from Spark's perspective during this blocking call. If `maxRuntimeSecs` exceeds `spark.network.timeout` (default 120s), the driver may receive a "Lost executor" message and terminate the context. Always set `spark.network.timeout` and `spark.executor.heartbeatInterval` to values greater than `maxRuntimeSecs` when running long AutoML jobs: `spark.network.timeout=700s` for a 600-second AutoML run.

---

### Example 4: Real-Time MOJO Scoring in Structured Streaming (No H2OContext Required)

> **What this demonstrates:** How `H2OMOJOModel` operates as a pure Spark transformer in Structured Streaming without requiring an active `H2OContext`—leveraging only `h2o-genmodel.jar`—and how MOJO broadcast and executor-local deserialization achieve sub-millisecond per-row scoring latency.

```python
from pysparkling.ml import H2OMOJOModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json
from pyspark.sql.types import StructType, StructField, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("MOJOStreamScoring") \
    # No H2OContext config needed for MOJO scoring — h2o-genmodel.jar is sufficient.
    # spark.jars must include both sparkling-water-package AND h2o-genmodel.
    .config("spark.jars.packages", "ai.h2o:sparkling-water-package_2.12:3.40.0.1-1-3.3") \
    .getOrCreate()

# STEP 1: Load the MOJO from disk (or cloud storage).
# The MOJO is a zip file containing model parameters in H2O's binary format.
# H2OMOJOModel.createFromMojo() reads the bytes and wraps them in a Spark Transformer.
# The MOJO bytes are serialized into a Spark broadcast variable so that each executor
# receives the model bytes ONCE via the BlockManager, not once per task.
mojo_model = H2OMOJOModel.createFromMojo(
    "/models/best_gbm.zip",
    # withPredictionCol: name of the output column containing the full prediction struct
    # (includes 'prediction', 'p0', 'p1' for binary classifiers).
)

# STEP 2: Define the incoming event schema.
# Kafka sends raw bytes; we parse JSON into typed columns matching the MOJO's expected features.
event_schema = StructType([
    StructField("user_id",        IntegerType(), nullable=False),
    StructField("session_s",      DoubleType(),  nullable=True),
    StructField("page_views",     IntegerType(), nullable=True),
])

# STEP 3: Build the streaming source.
# Each Kafka message is one user session event encoded as JSON.
raw_stream = (
    spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "broker:9092")
        .option("subscribe",               "user-events")
        .option("startingOffsets",         "latest")
        # Tune maxOffsetsPerTrigger to match MOJO scoring throughput.
        # GBM MOJO: ~80K rows/s/core. With 4 cores per executor and 3 executors = ~960K rows/s.
        # Set maxOffsetsPerTrigger = triggerIntervalSeconds * expectedRowsPerSecond.
        .option("maxOffsetsPerTrigger",    96000)  # 100ms trigger × 960K rows/s = 96K per batch
        .load()
)

# STEP 4: Deserialize Kafka value bytes to typed columns.
parsed_stream = (
    raw_stream
        .selectExpr("CAST(value AS STRING) AS json_str", "timestamp AS event_time")
        .withColumn("data", from_json(col("json_str"), event_schema))
        .select("event_time",
                col("data.user_id").alias("user_id"),
                col("data.session_s").alias("session_s"),
                col("data.page_views").alias("page_views"))
)

# STEP 5: Apply MOJO scoring.
# H2OMOJOModel.transform() internally calls EasyPredictModelWrapper.predict()
# inside a mapPartitions closure. The wrapper is instantiated ONCE per partition
# (executor-local, thread-safe) from the broadcast MOJO bytes.
# For binary classification the output struct contains: prediction, p0, p1.
# For regression: prediction only. For multinomial: prediction, p0..pN.
scored_stream = mojo_model.transform(parsed_stream)

# STEP 6: Flatten prediction struct and write to Delta Lake.
# Writing scored results back to Delta enables downstream batch reconciliation
# and model performance monitoring via Delta's time-travel capability.
output_stream = (
    scored_stream
        .select(
            col("event_time"),
            col("user_id"),
            col("prediction").alias("predicted_label"),
            col("p1").alias("conversion_probability"),   # P(converted=1)
        )
        .writeStream
            .format("delta")
            .option("checkpointLocation", "/checkpoints/mojo-scoring/")
            .outputMode("append")
            # ProcessingTime trigger aligns micro-batch cadence with maxOffsetsPerTrigger.
            .trigger(processingTime="100 milliseconds")
            .start("/data/scored-events/")
)

output_stream.awaitTermination()
```

> **Mastery Note:** The decisive advantage of MOJO scoring in Structured Streaming is that `H2OContext` is not required—no H2O cluster needs to be running. The `h2o-genmodel.jar` runtime is a self-contained scoring engine that requires only the MOJO zip bytes and the JVM. The MOJO bytes are broadcast to executors via Spark's `BlockManager` and stored in executor off-heap memory (if `spark.memory.offHeap.enabled=true`) or in executor JVM heap memory, deserialized exactly once per executor lifetime using `EasyPredictModelWrapper`. This means MOJO scoring latency scales with the number of trees (GBM) or layers (DL), not with the size of the dataset—a 500-tree GBM MOJO scores a single row in approximately 0.01–0.05ms, making end-to-end latency from Kafka ingestion to Delta write consistently under 100ms at 95th percentile for typical GBM models.

---

## 🎯 Mastery Checklist

To achieve true mastery of the Sparkling Water API:
- [ ] Understand why `H2OContext.getOrCreate()` submits a dummy Spark job and what happens if dynamic allocation is enabled when it runs
- [ ] Know when internal backend mode triggers GC storms and how to diagnose it from executor GC metrics in the Spark UI (OldGen > 80% during `asH2OFrame()`)
- [ ] Know that `asH2OFrame()` is a full data transcode, not a true zero-copy operation, and that the reverse `asDataFrame()` is near-zero-overhead on co-located nodes
- [ ] Be able to diagnose "H2O cloud shrank" (`H2OAbortException`) from executor logs and correlate it to executor preemption or network partition events
- [ ] Understand why applying Spark filters **after** `asH2OFrame()` bypasses Catalyst predicate pushdown and how to detect this anti-pattern using `df.explain(True)`
- [ ] Know that H2O AutoML training is invisible to the Spark DAGScheduler and that `spark.network.timeout` must exceed `maxRuntimeSecs` to prevent spurious executor loss events
- [ ] Understand the MOJO broadcast mechanism and why `H2OContext` is not required for Structured Streaming inference
- [ ] Know GBM vs. Deep Learning MOJO throughput differences (50K–100K vs. 5K–10K rows/s/core) and their implications for streaming backpressure tuning
- [ ] Be able to diagnose MOJO scoring bottlenecks from Structured Streaming's `processedRowsPerSecond` metric in the streaming query progress listener

---

## 📚 Summary

Sparkling Water's core value proposition is collapsing the Spark–H2O data pipeline from a distributed I/O problem into an in-process memory transcoding problem. By embedding H2O nodes inside Spark executor JVMs (internal backend) or connecting them on the same network (external backend), the library eliminates the serialization round-trip that would otherwise make iterative ML experimentation on large datasets prohibitively slow. The `asH2OFrame()` conversion is the critical boundary: it triggers a Spark action, transcodes Tungsten `UnsafeRow` data into H2O's columnar `NewChunk` format, and deposits the result into the DKV. All subsequent H2O operations—including AutoML's multi-model training and cross-validation—execute entirely on the H2O cluster with no Spark job overhead.

The `H2OAutoML` estimator's integration into Spark's `Pipeline` API provides MLOps-friendly model training: the same `Pipeline.fit()` / `Pipeline.transform()` interface used for Spark ML models now trains and selects from up to hundreds of H2O models, returning a `H2OMOJOModel` transformer that works in both batch and streaming contexts. The MOJO export format is the linchpin of production deployment: it decouples the scoring runtime from both Spark and H2O, requiring only `h2o-genmodel.jar` and enabling sub-millisecond per-row inference inside Structured Streaming micro-batches.

The two most consequential engineering decisions in any Sparkling Water deployment are backend selection (internal vs. external, driven by memory budget) and the filter-before-convert discipline (ensuring Catalyst optimizations run before `asH2OFrame()` is called). Both decisions are invisible at the API level—the code compiles and runs either way—but the performance difference between the anti-pattern and the correct pattern at production scale (hundreds of millions of rows, dozens of executors) is the difference between a 10-minute conversion and a 90-minute conversion, and between a stable cluster and one that OOMKills executors hourly.
