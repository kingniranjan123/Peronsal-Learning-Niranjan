# 🔥 Master Class: Regression and Classification with Deep Learning

## Overview

Deep neural networks (DNNs) have historically been associated with unstructured data—images, audio, text—but production Spark environments increasingly deploy DNNs against massive tabular datasets for regression and classification tasks where gradient-boosted trees plateau. The key insight is that when a dataset contains hundreds of millions of rows with high-cardinality categorical features, learned embeddings inside a DNN capture interaction effects that no hand-crafted feature engineering can match. Spark's role in this ecosystem is not to run backpropagation itself—that remains the domain of TensorFlow and PyTorch—but to orchestrate distributed data loading, feature transformation, parallel training, and model lifecycle management at petabyte scale.

The challenge is the impedance mismatch between Spark's batch-parallel, fault-tolerant data model and the synchronous, gradient-synchronization model that deep learning frameworks assume. Bridging this gap requires three subsystems working in concert: **Petastorm** (converting Spark DataFrames into a format that TensorFlow/PyTorch can stream from HDFS or S3), **Horovod** (coordinating all-reduce gradient synchronization across executors using MPI or Gloo), and **MLflow** (versioning trained models, logging hyperparameters, and serving registered artifacts to downstream consumers). Understanding how these three subsystems interact with Spark's DAGScheduler, BlockManager, and executor memory model is the difference between a prototype and a production system. [Ref: 451](spark_book.pdf#page=451)

--- [Ref: 455](spark_book.pdf#page=455)

## 🏗️ Architectural Deep Dive [Ref: 458](spark_book.pdf#page=458)

### How It Works Under the Hood

When a Spark job submits a Horovod training run via `HorovodRunner`, the driver's DAGScheduler emits a single Spark stage containing exactly `N` tasks—one per Horovod rank. Each task is pinned to a specific executor by Spark's `TaskScheduler` using locality preferences, ensuring the task runs on the machine that owns the Petastorm-written Parquet shards. Inside each executor JVM, a Python subprocess is forked via Py4J; this subprocess owns a TensorFlow or PyTorch process that binds a Gloo or NCCL rendezvous handle. The JVM executor heap is therefore not burdened with tensor memory—all model weights, activations, and gradients live in off-heap native memory or GPU VRAM, invisible to the JVM's garbage collector. This is critical: JVM GC pauses that would stall a shuffle operation do not stall backpropagation.

Petastorm's `make_batch_reader` opens Parquet files directly from the distributed file system using Arrow's native reader, bypassing Spark's RDD abstraction entirely. Each Horovod rank reads a non-overlapping shard of the dataset by partitioning the Parquet row groups by rank index modulo total ranks. The Arrow columnar format avoids row-level deserialization: an entire column of float32 values is read as a contiguous C buffer and passed to TensorFlow's `tf.data` pipeline without any Python-level object allocation. This eliminates the serialization bottleneck that makes naive `rdd.map(lambda row: ...)` approaches 10–40x slower than native readers for training workloads.

During the backward pass, Horovod intercepts each layer's gradient tensor immediately after it is computed (using framework hooks: `tf.GradientTape` callbacks or PyTorch's `register_hook`) and initiates an all-reduce operation across all ranks using the ring-all-reduce algorithm. Ring-all-reduce transmits `2 * (N-1) / N` times the gradient data per rank, making communication cost nearly constant regardless of cluster size—this is what enables linear scaling efficiency of 85–95% on clusters up to 128 GPUs. The reduced gradient is applied to the local model replica before the next mini-batch forward pass. Catalyst and Tungsten play no role in the training loop itself, but they are critical in the upstream feature engineering pipeline: Catalyst's Logical Optimization phase pushes `cast`, `fillna`, and `bucketize` transformations into a single fused physical plan, and Tungsten's Whole-Stage Codegen generates JIT-compiled bytecode that processes feature vectors at near-native speed before Petastorm serializes them to Parquet.

```
Spark Driver JVM
┌──────────────────────────────────────────────────────────────┐
│ SparkContext → DAGScheduler → TaskScheduler │
│ HorovodRunner.run(fn, np=N) ──► emits N pinned tasks │
└─────────────────────┬────────────────────────────────────────┘
 │ Task assignment (locality-aware)
 ┌───────────┼───────────┐
 ▼ ▼ ▼
 Executor 0 Executor 1 Executor N-1
 ┌──────────┐ ┌──────────┐ ┌──────────────┐
 │JVM Heap │ │JVM Heap │ │JVM Heap │
 │(feature │ │(feature │ │(feature pipe)│
 │pipeline) │ │pipeline) │ │ │
 │ │ │ │ │ │
 │ Py4J fork│ │ Py4J fork│ │ Py4J fork │
 │ ┌──────┐ │ │ ┌──────┐ │ │ ┌──────────┐ │
 │ │ TF/ │ │ │ │ TF/ │ │ │ │ TF/PyTch │ │
 │ │PyTch │ │ │ │PyTch │ │ │ │ Rank N-1 │ │
 │ │Rank 0│ │ │ │Rank 1│ │ │ └────┬─────┘ │
 │ └──┬───┘ │ │ └──┬───┘ │ │ │ │
 └────┼─────┘ └────┼─────┘ └──────┼────────┘
 │ │ │
 └───────────────┴─────────────────┘
 Horovod Ring-AllReduce (Gloo/NCCL)
 ∇W₀ + ∇W₁ + ... + ∇Wₙ → averaged ∇W
 │
 ▼
 Petastorm (Arrow/Parquet on HDFS/S3)
 ┌───────────────────────────────┐
 │ Row Group 0 │ Row Group 1 │ ...
 │ (Rank 0) │ (Rank 1) │
 └───────────────────────────────┘
 │
 ▼
 MLflow Tracking Server
 ┌──────────────────────┐
 │ run_id, params, │
 │ metrics, artifact │
 │ → Model Registry │
 │ (Staging→Prod) │
 └──────────────────────┘ [Ref: 462](spark_book.pdf#page=462)
```

### Key Internal Components

- **HorovodRunner:** A Spark-native launcher that wraps `horovod.spark.run`, submitting a closure as a distributed Spark action. It negotiates rendezvous addresses via the Spark driver and assigns one MPI rank per executor slot, respecting `spark.task.cpus` and GPU resource configuration.

- **Petastorm `make_batch_reader`:** Reads Parquet files written by `petastorm.spark.SparkDatasetConverter` using the Arrow IPC protocol. Each reader instance shard-filters row groups by `cur_shard` / `shard_count`, guaranteeing no data overlap between ranks without a shuffle.

- **Horovod `DistributedOptimizer`:** A wrapper around any Keras or PyTorch optimizer that intercepts `optimizer.apply_gradients` / `optimizer.step`, triggering `hvd.allreduce()` on each gradient tensor before the weight update. Compression codecs (FP16, 1-bit quantization) can reduce all-reduce bandwidth by 50–75%.

- **MLflow `log_model` / Model Registry:** The `mlflow.tensorflow.log_model` call serializes the `SavedModel` artifact, computes an MD5 fingerprint, and writes a `MLmodel` YAML descriptor. The Registry's `MlflowClient.transition_model_version_stage` API implements a promotion pipeline (`None → Staging → Production → Archived`) with atomic version tagging. [Ref: 469](spark_book.pdf#page=469)

--- [Ref: 452](spark_book.pdf#page=452)

## ⚠️ Critical Concepts & Common Pitfalls [Ref: 456](spark_book.pdf#page=456)

### Data Skew in Petastorm Sharding

Petastorm's shard assignment is based on Parquet row-group count, not row count. If your upstream Spark job produces unequal row group sizes—common when `spark.sql.files.maxPartitionBytes` is tuned aggressively—some ranks receive 3–4x more data than others. The slowest rank determines the epoch duration; fast ranks spin-wait at the Horovod barrier, wasting GPU time. This manifests as GPU utilization oscillating between 95% and 15% in YARN's Resource Manager UI. The fix is to repartition the DataFrame to a number of partitions exactly equal to `num_epochs * num_ranks` before writing, and set `parquet.block.size` equal to `target_rows_per_shard * avg_row_bytes`.

A more subtle issue: Petastorm's `make_batch_reader` holds file handles open for the entire training run. On clusters with HDFS NameNode lease timeouts set below 10 minutes (`dfs.datanode.socket.write.timeout`), long epochs trigger `LeaseExpiredException` mid-epoch. The fix is to set `options={'hdfs_driver': 'libhdfs3'}` and increase the lease timeout, or restructure training to close and reopen the reader every N steps. [Ref: 459](spark_book.pdf#page=459)

### Horovod Gradient Explosion at Scale

Horovod's default all-reduce averages gradients across all ranks, which means the effective learning rate scales with batch size. A common mistake when scaling from 4 to 32 GPUs is to keep the learning rate constant. With a per-GPU mini-batch of 512, scaling to 32 GPUs gives an effective batch of 16,384—the model sees gradients computed over a 32x larger batch without a learning rate adjustment, causing loss divergence within the first 200 steps. The standard fix is linear scaling: `lr_scaled = base_lr * num_ranks`, paired with a warm-up schedule for the first 5 epochs (Goyal et al., 2017). Horovod's `hvd.callbacks.LearningRateWarmupCallback` implements this automatically; failing to use it in multi-GPU tabular DNN training is the single most common cause of accuracy regression when scaling out. [Ref: 463](spark_book.pdf#page=463)

--- [Ref: 470](spark_book.pdf#page=470)

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|-----------|---------|-------|
| Petastorm Parquet write (N partitions) | O(N) | No | Arrow columnar serialization; ~2GB/s per executor with SSD |
| Horovod ring-all-reduce (M params) | O(M) | No | Network-only; scales to 128 ranks at 90%+ efficiency with NCCL |
| Feature pipeline (Catalyst fused plan) | O(rows) | No | Tungsten Whole-Stage Codegen; ~500M rows/min on 32-core executor |
| MLflow `log_model` (SavedModel) | O(model size) | No | Serializes to artifact store; 1–5s for 100MB model |
| Horovod broadcast (initial weights) | O(M) | No | Driver-to-rank fan-out; done once per run via `hvd.broadcast_variables` |
| Spark DataFrame repartition pre-write | O(rows) | Yes | Required for even shard distribution; triggers a full shuffle stage | [Ref: 453](spark_book.pdf#page=453)

--- [Ref: 457](spark_book.pdf#page=457)

## 💻 Code Examples [Ref: 461](spark_book.pdf#page=461)

### Example 1: Feature Engineering Pipeline with Catalyst-Optimized Transformations for Petastorm Ingestion

> **What this demonstrates:** How to build a production-grade feature engineering pipeline that produces a Petastorm-compatible Parquet dataset, leveraging Catalyst's predicate pushdown and Tungsten's fused codegen for maximum throughput before the data ever reaches a GPU.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType
from petastorm.spark import SparkDatasetConverter, make_spark_converter

spark = SparkSession.builder \
 .appName("DNN-FeaturePipeline") \
 # Enable Adaptive Query Execution so Catalyst can re-optimize
 # partition counts after each shuffle stage at runtime.
 .config("spark.sql.adaptive.enabled", "true") \
 # Tungsten off-heap memory for binary row format operations.
 .config("spark.memory.offHeap.enabled", "true") \
 .config("spark.memory.offHeap.size", "8g") \
 # Petastorm cache dir — must be accessible from all executors.
 .config(SparkDatasetConverter.PARENT_CACHE_DIR_URL_CONF,
 "hdfs:///tmp/petastorm_cache") \
 .getOrCreate()

# Read raw data from Delta Lake — Catalyst will push the filter
# `loan_status IS NOT NULL` into the Delta log scan, skipping
# entire file groups that cannot satisfy the predicate.
raw_df = spark.read.format("delta") \
 .load("hdfs:///data/loans/raw") \
 .filter(F.col("loan_status").isNotNull())

# Define a binary classification label: 1 = defaulted, 0 = paid.
# Using `when/otherwise` is a single Catalyst expression node —
# it compiles to a single JIT bytecode branch, not a UDF call.
labeled_df = raw_df.withColumn(
 "label",
 F.when(F.col("loan_status") == "Charged Off", 1.0).otherwise(0.0)
 .cast(FloatType())
)

# Log-transform skewed continuous features inline.
# Catalyst merges all `withColumn` calls into a single Project node,
# so there is no intermediate materialization between these transforms.
feature_df = labeled_df \
 .withColumn("log_annual_inc",
 F.log1p(F.col("annual_inc")).cast(FloatType())) \
 .withColumn("dti_norm",
 (F.col("dti") / F.lit(100.0)).cast(FloatType())) \
 .withColumn("fico_scaled",
 ((F.col("fico_range_high") - F.lit(300.0)) /
 F.lit(550.0)).cast(FloatType())) \
 .withColumn("emp_length_years",
 F.regexp_extract(F.col("emp_length"),
 r"(\d+)", 1)
 .cast(FloatType()).fillna(0.0))

# Select only the columns the DNN needs — projection pushdown
# prevents reading unused columns from Parquet entirely.
model_df = feature_df.select(
 "label",
 "log_annual_inc",
 "dti_norm",
 "fico_scaled",
 "emp_length_years",
 "int_rate",
 "installment",
 "revol_util"
).fillna(0.0)

# Repartition to exactly num_epochs * num_ranks partitions.
# This guarantees equal Parquet row groups per Horovod rank,
# preventing the data-skew barrier stall described in the pitfalls section.
NUM_RANKS = 8
NUM_EPOCHS = 10
model_df = model_df.repartition(NUM_RANKS * NUM_EPOCHS)

# SparkDatasetConverter writes Parquet with Arrow schema metadata
# that Petastorm uses for type-safe deserialization on the reader side.
converter = make_spark_converter(model_df)
print(f"Dataset written: {converter.dataset_size} rows") [Ref: 464](spark_book.pdf#page=464)
```

> **Mastery Note:** Every `withColumn` call here emits a `Project` node in Catalyst's Logical Plan; the Analyzer collapses all consecutive projections into a single `Project` during the Logical Optimization phase, producing one physical `ProjectExec` that Tungsten's Whole-Stage Codegen fuses into a single JIT-compiled Java class. The `regexp_extract` call is the only expression that cannot be fused into Whole-Stage Codegen (it calls back into the Scala regex engine), so isolating it early and casting to `FloatType` immediately prevents it from propagating through the rest of the plan. The `repartition` at the end triggers a hash-based shuffle—the only shuffle in this pipeline—which is unavoidable but must be budgeted as a one-time cost per training run.

---

### Example 2: Horovod All-Reduce DNN Training with Linear LR Scaling and Warm-Up

> **What this demonstrates:** How `HorovodRunner` executes a TensorFlow Keras DNN across Spark executors, with correct distributed optimizer wrapping, learning rate warm-up, and per-rank Petastorm shard reading to achieve linear scaling efficiency.

```python
import horovod.tensorflow.keras as hvd
import tensorflow as tf
from petastorm import make_batch_reader
from petastorm.tf_utils import make_petastorm_dataset
import mlflow

FEATURE_COLS = [
 "log_annual_inc", "dti_norm", "fico_scaled",
 "emp_length_years", "int_rate", "installment", "revol_util"
]
PETASTORM_URL = "hdfs:///tmp/petastorm_cache/loan_features"
BASE_LR = 1e-3 # learning rate for a single-GPU run
BATCH_SIZE = 512 # per-rank mini-batch size


def train_fn():
 # Step 1: Initialize Horovod — this binds the Gloo/NCCL
 # rendezvous handle and assigns this process its rank integer.
 hvd.init()

 # Step 2: Pin TensorFlow to the GPU corresponding to this rank.
 # On CPU-only clusters, remove this block.
 gpus = tf.config.list_physical_devices("GPU")
 if gpus:
 tf.config.set_visible_devices(gpus[hvd.local_rank()], "GPU")
 tf.config.experimental.set_memory_growth(
 gpus[hvd.local_rank()], True
 )

 # Step 3: Build the DNN. Batch normalization is critical for
 # tabular DNNs — it normalizes activations between layers,
 # dramatically accelerating convergence on non-stationary
 # feature distributions common in financial data.
 def build_model(input_dim):
 inputs = tf.keras.Input(shape=(input_dim,), name="features")
 x = tf.keras.layers.Dense(256, activation="relu")(inputs)
 x = tf.keras.layers.BatchNormalization()(x)
 x = tf.keras.layers.Dropout(0.3)(x)
 x = tf.keras.layers.Dense(128, activation="relu")(x)
 x = tf.keras.layers.BatchNormalization()(x)
 x = tf.keras.layers.Dropout(0.2)(x)
 x = tf.keras.layers.Dense(64, activation="relu")(x)
 # Sigmoid output for binary classification (loan default).
 output = tf.keras.layers.Dense(1, activation="sigmoid")(x)
 return tf.keras.Model(inputs, output)

 model = build_model(input_dim=len(FEATURE_COLS))

 # Step 4: Scale learning rate linearly with the number of ranks.
 # This preserves the gradient signal-to-noise ratio as effective
 # batch size grows proportionally with `hvd.size()`.
 scaled_lr = BASE_LR * hvd.size()

 # Step 5: Wrap the optimizer with Horovod's DistributedOptimizer.
 # This inserts an `hvd.allreduce()` call on each gradient tensor
 # between `tape.gradient()` and `optimizer.apply_gradients()`.
 opt = hvd.DistributedOptimizer(
 tf.keras.optimizers.Adam(learning_rate=scaled_lr),
 # Average gradients across all ranks before applying —
 # essential for consistent weight updates in data-parallel training.
 average_aggregated_gradients=True
 )

 model.compile(
 optimizer=opt,
 loss="binary_crossentropy",
 metrics=["AUC"],
 # experimental_run_tf_function=False is required when using
 # Horovod with tf.function tracing in TF 2.x.
 experimental_run_tf_function=False
 )

 # Step 6: Open a Petastorm reader shard for this rank only.
 # `cur_shard=hvd.rank()` and `shard_count=hvd.size()` filter
 # Parquet row groups by index, guaranteeing disjoint data splits.
 with make_batch_reader(
 PETASTORM_URL,
 cur_shard=hvd.rank(),
 shard_count=hvd.size(),
 num_epochs=None # Infinite iterator; epoch control via steps.
 ) as reader:
 dataset = make_petastorm_dataset(reader) \
 .batch(BATCH_SIZE) \
 .map(lambda x: (
 tf.stack([x[c] for c in FEATURE_COLS], axis=1),
 x["label"]
 ))

 callbacks = [
 # Broadcast initial weights from rank 0 to all other ranks
 # before training begins — ensures all replicas start
 # from the same random initialization.
 hvd.callbacks.BroadcastGlobalVariablesCallback(0),
 # Warm up LR from `scaled_lr / hvd.size()` to `scaled_lr`
 # over the first 5 epochs, preventing loss spikes at start.
 hvd.callbacks.LearningRateWarmupCallback(
 initial_lr=BASE_LR,
 warmup_epochs=5,
 verbose=1
 ),
 ]

 # Only rank 0 logs to MLflow — all other ranks produce
 # identical metrics due to synchronous all-reduce.
 if hvd.rank() == 0:
 mlflow.tensorflow.autolog()

 model.fit(
 dataset,
 steps_per_epoch=10000 // hvd.size(),
 epochs=10,
 callbacks=callbacks,
 verbose=1 if hvd.rank() == 0 else 0
 )

 # Only rank 0 saves the model — all replicas are weight-identical
 # at this point because all-reduce kept them synchronized.
 if hvd.rank() == 0:
 model.save("/tmp/dnn_loan_model")
 mlflow.tensorflow.log_model(
 model,
 artifact_path="loan_default_dnn",
 registered_model_name="LoanDefaultClassifier"
 )


# HorovodRunner emits `np` Spark tasks, one per rank.
# `use_gloo=True` enables the pure-Python Gloo backend, which works
# on CPU clusters without an MPI installation.
from horovod.spark import HorovodRunner
hr = HorovodRunner(np=8, use_gloo=True)
hr.run(train_fn)
```

> **Mastery Note:** The `hvd.callbacks.BroadcastGlobalVariablesCallback(0)` call is non-negotiable — without it, each rank initializes weights from a different random seed, and the first all-reduce averages incompatible weight landscapes, producing a model that is numerically equivalent to one trained with `hvd.size()` different initializations averaged together. The `average_aggregated_gradients=True` flag in `DistributedOptimizer` uses gradient accumulation internally to defer the all-reduce until all micro-batches are computed, reducing communication frequency and improving GPU utilization by 10–15% on high-latency networks. On a cluster with NCCL-capable GPUs, replacing `use_gloo=True` with the default NCCL backend reduces all-reduce latency by 3–5x for large gradient tensors (>10MB).

---

### Example 3: Transfer Learning from a Pre-Trained DNN with Petastorm and Layer Freezing

> **What this demonstrates:** How to load a pre-trained base DNN from the MLflow Model Registry, freeze its embedding layers, and fine-tune only the top classification head on a new target domain—reusing learned feature representations to cut training time by 60–80%.

```python
import mlflow.tensorflow
import tensorflow as tf
import horovod.tensorflow.keras as hvd
from petastorm import make_batch_reader
from petastorm.tf_utils import make_petastorm_dataset

# Load the pre-trained model from MLflow Model Registry.
# "Production" stage guarantees this is the version that passed
# evaluation gates and was explicitly promoted via the Registry API.
client = mlflow.tracking.MlflowClient()
prod_model_uri = client.get_latest_versions(
 "LoanDefaultClassifier", stages=["Production"]
)[0].source

base_model = mlflow.tensorflow.load_model(prod_model_uri)

# Inspect the layer structure to identify the embedding trunk
# (layers 0–5) vs. the classification head (layers 6–end).
for i, layer in enumerate(base_model.layers):
 print(f"[{i:2d}] {layer.name:35s} trainable={layer.trainable}")

# Freeze all layers up to and including the second BatchNorm block.
# Frozen layers produce zero gradients — they do NOT participate in
# all-reduce, reducing Horovod communication volume by ~60%.
FREEZE_UP_TO = 6
for layer in base_model.layers[:FREEZE_UP_TO]:
 layer.trainable = False

# Attach a new classification head for the new target task:
# mortgage default prediction (different label distribution).
x = base_model.layers[FREEZE_UP_TO - 1].output # last frozen layer output
x = tf.keras.layers.Dense(32, activation="relu",
 name="transfer_dense")(x)
x = tf.keras.layers.Dropout(0.1)(x)
new_output = tf.keras.layers.Dense(1, activation="sigmoid",
 name="mortgage_output")(x)

transfer_model = tf.keras.Model(
 inputs=base_model.input,
 outputs=new_output,
 name="MortgageDefaultTransfer"
)

# Verify the parameter count split: frozen vs. trainable.
total_params = transfer_model.count_params()
trainable_params = sum(
 tf.size(w).numpy() for w in transfer_model.trainable_weights
)
print(f"Trainable: {trainable_params:,} / Total: {total_params:,} "
 f"({100*trainable_params/total_params:.1f}%)")

# Only wrap the optimizer with Horovod after model construction —
# Horovod's DistributedOptimizer inspects `model.trainable_variables`
# at compile time to build the all-reduce communication plan.
hvd.init()
opt = hvd.DistributedOptimizer(
 # Use a lower LR for fine-tuning — the pre-trained trunk is
 # already in a good weight basin; aggressive updates destroy it.
 tf.keras.optimizers.Adam(learning_rate=1e-4 * hvd.size()),
 average_aggregated_gradients=True
)

transfer_model.compile(
 optimizer=opt,
 loss="binary_crossentropy",
 metrics=["AUC", "Precision", "Recall"]
)

MORTGAGE_URL = "hdfs:///tmp/petastorm_cache/mortgage_features"

with make_batch_reader(
 MORTGAGE_URL,
 cur_shard=hvd.rank(),
 shard_count=hvd.size(),
 num_epochs=None
) as reader:
 dataset = make_petastorm_dataset(reader).batch(256).map(
 lambda x: (
 tf.stack([x[c] for c in FEATURE_COLS], axis=1),
 x["mortgage_default_label"]
 )
 )

 with mlflow.start_run(run_name="transfer-mortgage-dnn"):
 if hvd.rank() == 0:
 # Log which layers were frozen as hyperparameters —
 # critical for experiment reproducibility in the Registry.
 mlflow.log_param("frozen_layers", FREEZE_UP_TO)
 mlflow.log_param("base_model_uri", prod_model_uri)
 mlflow.log_param("trainable_params", trainable_params)

 transfer_model.fit(
 dataset,
 steps_per_epoch=5000 // hvd.size(),
 epochs=5,
 callbacks=[
 hvd.callbacks.BroadcastGlobalVariablesCallback(0)
 ],
 verbose=1 if hvd.rank() == 0 else 0
 )

 if hvd.rank() == 0:
 mlflow.tensorflow.log_model(
 transfer_model,
 artifact_path="mortgage_transfer_dnn",
 registered_model_name="MortgageDefaultTransfer"
 )
```

> **Mastery Note:** Freezing layers in a Horovod-distributed setting has a multiplicative benefit: not only does backpropagation stop at the frozen boundary (reducing GPU FLOPS), but Horovod's all-reduce communication plan omits the gradients for frozen layers entirely, reducing per-step network traffic proportionally. On a model with 2M parameters where 1.2M are frozen, each all-reduce transmits only 800K × 4 bytes × 2 (ring factor) ≈ 6.4MB instead of 16MB—a 60% bandwidth reduction that translates directly to higher GPU utilization. The `mlflow.log_param("base_model_uri", ...)` call is essential for lineage tracking: the Model Registry records not just the new model's artifact, but the exact pre-trained checkpoint it derived from, enabling full audit trails for regulated industries.

---

### Example 4: MLflow Model Registry Promotion Pipeline with Automated Validation Gate

> **What this demonstrates:** How to implement a production-grade model promotion workflow that programmatically validates a newly registered model against a holdout test set before transitioning it from `Staging` to `Production`, with automatic rollback if AUC drops below threshold.

```python
import mlflow
from mlflow.tracking import MlflowClient
import tensorflow as tf
from petastorm import make_batch_reader
from petastorm.tf_utils import make_petastorm_dataset
import numpy as np

client = MlflowClient()
MODEL_NAME = "LoanDefaultClassifier"
AUC_THRESH = 0.82 # Minimum AUC required for Production promotion.
HOLDOUT_URL = "hdfs:///tmp/petastorm_cache/loan_holdout"


def evaluate_model_on_holdout(model_uri: str) -> dict:
 """Load a model from the Registry and evaluate on the holdout set.
 Returns a dict of metrics for the promotion decision gate."""
 model = mlflow.tensorflow.load_model(model_uri)

 # AUC metric is computed incrementally to avoid loading the
 # entire holdout set into driver memory — Petastorm streams it.
 auc_metric = tf.keras.metrics.AUC(name="auc", num_thresholds=200)
 pr_metric = tf.keras.metrics.AUC(curve="PR", name="pr_auc")
 total_rows = 0

 with make_batch_reader(HOLDOUT_URL, num_epochs=1) as reader:
 dataset = make_petastorm_dataset(reader).batch(1024).map(
 lambda x: (
 tf.stack([x[c] for c in FEATURE_COLS], axis=1),
 tf.cast(x["label"], tf.float32)
 )
 )
 for features, labels in dataset:
 preds = model(features, training=False) # No dropout at eval.
 auc_metric.update_state(labels, preds)
 pr_metric.update_state(labels, preds)
 total_rows += features.shape[0]

 return {
 "auc": float(auc_metric.result().numpy()),
 "pr_auc": float(pr_metric.result().numpy()),
 "rows_evaluated": total_rows
 }


def promote_or_rollback(model_name: str, auc_threshold: float):
 """
 Check all models in Staging stage.
 Promote to Production if AUC >= threshold.
 Archive the old Production version.
 Rollback (archive Staging) if validation fails.
 """
 staging_versions = client.get_latest_versions(
 model_name, stages=["Staging"]
 )
 if not staging_versions:
 print("No models in Staging. Nothing to promote.")
 return

 for version in staging_versions:
 model_uri = f"models:/{model_name}/{version.version}"
 print(f"Evaluating {model_name} v{version.version} from Staging…")

 metrics = evaluate_model_on_holdout(model_uri)
 print(f" AUC={metrics['auc']:.4f} PR-AUC={metrics['pr_auc']:.4f}"
 f" rows={metrics['rows_evaluated']:,}")

 # Log validation metrics back to the original training run
 # so they appear in the Registry's experiment lineage view.
 with mlflow.start_run(run_id=version.run_id):
 mlflow.log_metrics({
 "holdout_auc": metrics["auc"],
 "holdout_pr_auc": metrics["pr_auc"],
 "holdout_rows": metrics["rows_evaluated"]
 })

 if metrics["auc"] >= auc_threshold:
 # Archive the currently active Production version
 # before promoting — prevents two Production versions
 # from coexisting, which would cause serving ambiguity.
 current_prod = client.get_latest_versions(
 model_name, stages=["Production"]
 )
 for old_prod in current_prod:
 print(f" Archiving old Production v{old_prod.version}")
 client.transition_model_version_stage(
 name=model_name,
 version=old_prod.version,
 stage="Archived",
 # archive_existing_versions=True is idempotent and
 # prevents race conditions in concurrent promotions.
 archive_existing_versions=False
 )

 # Promote the validated Staging model to Production.
 client.transition_model_version_stage(
 name=model_name,
 version=version.version,
 stage="Production"
 )
 # Tag the version with the validation gate result for audit.
 client.set_model_version_tag(
 name=model_name,
 version=version.version,
 key="validation_auc",
 value=str(round(metrics["auc"], 5))
 )
 print(f" ✅ Promoted v{version.version} to Production "
 f"(AUC={metrics['auc']:.4f} ≥ {auc_threshold})")
 else:
 # Rollback: move failed Staging model to Archived.
 client.transition_model_version_stage(
 name=model_name,
 version=version.version,
 stage="Archived"
 )
 client.set_model_version_tag(
 name=model_name,
 version=version.version,
 key="rejection_reason",
 value=f"AUC {metrics['auc']:.4f} < threshold {auc_threshold}"
 )
 print(f" ❌ Archived v{version.version}: "
 f"AUC={metrics['auc']:.4f} below threshold {auc_threshold}")


# Execute the promotion pipeline — can be triggered from a
# Databricks Job, Airflow DAG, or GitHub Actions CI step.
promote_or_rollback(MODEL_NAME, AUC_THRESH)
```

> **Mastery Note:** The incremental AUC computation using `tf.keras.metrics.AUC.update_state` is architecturally significant: it streams the holdout data through the model without ever materializing more than one batch (1,024 rows × feature width × 4 bytes) in driver memory, making it safe for holdout sets with hundreds of millions of rows. The `archive_existing_versions=False` flag is deliberate—setting it to `True` would silently archive ALL non-Staging/Production versions, which in a CI pipeline can inadvertently archive versions that are being evaluated in parallel. Tagging the promoted version with `validation_auc` creates an immutable audit trail that satisfies SOC 2 and model governance requirements: every Production model version in the Registry carries a tag proving it passed quantitative evaluation before promotion.

---

## 🎯 Mastery Checklist

To achieve true mastery of Regression and Classification with Deep Learning in Spark:

- [ ] Understand how `HorovodRunner` maps Horovod ranks to Spark executor slots and why locality-aware task assignment prevents Petastorm I/O bottlenecks
- [ ] Know why `hvd.callbacks.LearningRateWarmupCallback` is mandatory when scaling from single-GPU to multi-GPU and what loss divergence looks like without it
- [ ] Be able to diagnose Petastorm shard imbalance from Spark UI stage timeline (look for barrier task straggler patterns where 1 task takes 3x longer than others)
- [ ] Understand the tradeoff between freezing more layers (faster training, less GPU communication, risks underfitting) vs. fine-tuning more layers (higher accuracy ceiling, more all-reduce traffic, risk of catastrophic forgetting)
- [ ] Know how Petastorm's `make_batch_reader` shard filtering interacts with Parquet row-group count, and how to pre-size partitions to guarantee balanced shards
- [ ] Be able to implement an MLflow promotion gate with rollback logic and explain why `transition_model_version_stage` must archive the old Production version atomically before promoting the new one
- [ ] Know how `hvd.DistributedOptimizer` with `average_aggregated_gradients=True` differs from vanilla gradient averaging and its impact on GPU utilization on high-latency networks
- [ ] Understand how TensorFlow's `training=False` flag during holdout evaluation disables BatchNormalization's running-mean updates and Dropout, and why omitting it causes AUC to be computed on artificially regularized predictions

---

## 📚 Summary

Deploying deep neural networks for regression and classification on Spark at scale is an exercise in distributed systems composition. The Spark DAGScheduler provides the skeleton—task placement, fault tolerance, resource negotiation—while Petastorm, Horovod, and MLflow provide the muscles that perform the actual deep learning work. The critical architectural invariant is that JVM executor heap and TensorFlow/PyTorch native memory never compete: Horovod training runs in forked Python subprocesses that own their own memory space, with gradient tensors living entirely in native or GPU memory that the JVM garbage collector cannot touch. 

Linear scaling efficiency in Horovod all-reduce depends on three factors that must all be correct simultaneously: learning rate scaling proportional to the number of ranks, warm-up scheduling to stabilize the early training landscape, and balanced Petastorm shard sizes that prevent any rank from becoming a barrier straggler. Failure in any one of these produces symptoms that are superficially similar—poor validation accuracy, slow convergence—but have entirely different root causes visible only in Spark UI task timelines, GPU utilization dashboards, and MLflow training curves combined. 

The MLflow Model Registry is not optional scaffolding—it is the governance contract between data scientists who train models and platform engineers who serve them. The promotion pipeline enforces that no model reaches Production without quantitative validation against a holdout set, with full metric provenance recorded against the exact run ID that produced the artifact. For regulated industries where model decisions carry legal weight, this audit chain—from raw Parquet shard through Horovod all-reduce to Registry version tag—is the complete evidentiary record of how a prediction was produced. 

