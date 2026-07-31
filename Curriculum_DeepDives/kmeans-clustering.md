# 🔥 Master Class: K-Means Clustering in Apache Spark

## Overview

K-Means is the most widely deployed unsupervised machine learning algorithm in distributed data systems, and Apache Spark's `MLlib` implementation exposes a battle-hardened, distributed version of Lloyd's algorithm capable of operating on datasets with billions of rows across thousands of partitions. The algorithm's goal is deceptively simple: partition *n* observations into *k* clusters such that each observation belongs to the cluster with the nearest mean (centroid), minimizing the within-cluster sum of squared distances (WCSS), also called inertia. Yet achieving this at petabyte scale requires deep architectural choices that span the Driver's DAG planning, executor-level memory management, and network-efficient centroid aggregation.

The reason K-Means exists as a first-class citizen in Spark MLlib (rather than a pure user-land implementation) is that its inner loop — the assignment step — requires broadcasting small centroid vectors to every partition, while the update step requires a distributed reduce. Without framework-level support for efficient broadcast variables and tree-based aggregation, a naive implementation would saturate the network with O(n·k·d) bytes per iteration. Spark's implementation collapses this to a single broadcast of k·d floating-point values followed by a shuffle-free treeReduce that computes new centroids in O(log P) rounds where P is the number of partitions.

The algorithm converges iteratively: in each round, every executor assigns its local data points to the nearest centroid using Euclidean distance, accumulates per-cluster sums and counts locally, and ships those partial aggregates to the Driver — never the raw data. The Driver recomputes centroids and broadcasts them back. This continues until either the centroid shift falls below `tol` (default 1e-4) or `maxIter` (default 20) is exhausted.

---

## 🏗️ Architectural Deep Dive

### How It Works Under the Hood

When you call `KMeans().fit(df)`, Spark MLlib triggers a sequence of coordinated Spark jobs, not a single monolithic action. The first job runs **k-means++ initialization**: it selects the first centroid uniformly at random, then iteratively selects subsequent centroids with probability proportional to D²(x) — the squared distance from each point to its nearest already-chosen centroid. This is implemented as a series of full-dataset passes (one per centroid), each of which is a separate Spark action that drives the cost from O(k) random initialization failures to a provably O(log k) approximation guarantee on the final WCSS. In practice, Spark uses a parallel k-means|| (k-means parallel) variant that over-samples in each pass and reduces the number of full-dataset passes from k to O(log n).

Once initialization completes, the main iteration loop begins. Each iteration is a single Spark Job containing one Stage with no shuffle. The centroids array — a tiny `k × d` double array — is wrapped in a `Broadcast[Array[Array[Double]]]` and shipped once to every executor via the `TorrentBroadcast` protocol, which uses a BitTorrent-like P2P mechanism to avoid driver bottlenecks when k·d is large (e.g., k=1000, d=512). Each Task running on a partition iterates over its `InternalRow` objects in Tungsten's binary off-heap format, computes the nearest centroid index using BLAS-accelerated `DDOT` operations (via `com.github.fommil.netlib`), and accumulates per-cluster sum vectors and counts into a local `Array[Array[Double]]`. This accumulation happens entirely in JVM heap memory, bypassing Tungsten's unsafe row format because centroids are mutable floating-point accumulators.

After all tasks complete, the partial aggregates — k pairs of (sum-vector, count) — are combined using `RDD.treeAggregate` with a merge depth of 2. `treeAggregate` differs critically from `aggregate` in that it performs a binary tree reduction on the executors before sending results to the Driver, reducing Driver ingestion from P messages to log₂(P) messages. The Driver divides each sum vector by its count to produce new centroids, computes the centroid shift (maximum L2 distance between old and new centroids), and repeats the loop if convergence has not been reached. The Catalyst optimizer is not involved in this loop — MLlib's K-Means operates below the DataFrame abstraction at the RDD level after the initial `fit` triggers a `dataset.rdd` conversion through the `RowToVector` internal transformer.

```
Driver JVM
┌──────────────────────────────────────────────────────┐
│  KMeans.fit()                                        │
│  ┌────────────────────┐   ┌─────────────────────┐   │
│  │ k-means|| Init     │   │  Iteration Loop      │   │
│  │ (k/2 Spark Jobs)   │──▶│  (1 Job / Iteration) │   │
│  └────────────────────┘   └──────────┬──────────┘   │
│                                       │              │
│  TorrentBroadcast(centroids: k×d)◀───┘              │
│         │                             ▲              │
│         ▼                             │              │
│  treeAggregate(depth=2) ─────────────┘              │
│  (partial sums from log₂(P) executor rounds)         │
└──────────────────────────────────────────────────────┘
         │ broadcast          │ partial aggregates
         ▼                    │
┌─────────────────────────────────────────────────────┐
│  Executor Pool (P partitions in parallel)           │
│  ┌──────────────────────────────────────────────┐   │
│  │ Task (Partition i)                           │   │
│  │  for row in partition:                       │   │
│  │    c = argmin BLAS·DDOT(row, centroid_j)     │   │
│  │    local_sums[c] += row                      │   │
│  │    local_counts[c] += 1                      │   │
│  │  return (local_sums, local_counts)           │   │
│  └──────────────────────────────────────────────┘   │
│  (No shuffle — assignment is embarrassingly          │
│   parallel; data never leaves its partition)         │
└─────────────────────────────────────────────────────┘
```

### Key Internal Components

- **`TorrentBroadcast`:** Serializes the centroid array using Java serialization (or Kryo if `spark.serializer=KryoSerializer`) and distributes it via P2P chunk exchange. For k=500, d=100, this is ~400KB — trivial. For k=5000, d=512, this is ~20MB per executor; enabling Kryo reduces this by ~40%.

- **`treeAggregate` (depth=2):** Performs a two-level binary-tree reduce on executors before shipping to the Driver. With 200 partitions, this reduces Driver receive operations from 200 to ~15, preventing the Driver from becoming a GC bottleneck when k×d partial aggregates are large.

- **BLAS `DDOT` / `DAXPY`:** MLlib delegates distance computation to native BLAS (OpenBLAS or MKL via `netlib-java`). If native libraries are absent, it falls back to pure-Java F2J, which is 3–10× slower. Verify with `com.github.fommil.netlib.BLAS.getInstance().getClass().getName()` at startup.

- **`KMeansModel.clusterCenters`:** An `Array[Vector]` on the Driver holding the final k centroids. `transform(df)` is a single map-side operation — it broadcasts centroids and assigns each row its nearest cluster, producing a `prediction` column with no shuffle.

---

## ⚠️ Critical Concepts & Common Pitfalls

### The Empty Cluster Problem and Degenerate Initialization

When using random initialization (not k-means++), it is common for two initial centroids to land in the same dense region, leaving a distant cluster entirely unclaimed. After the first assignment step, that centroid receives zero points. Spark's implementation handles this by retaining the centroid unchanged (it does not reinitialize or steal a point from the largest cluster, as some implementations do). The result is a model with effectively k-1 meaningful clusters, and the WCSS will be higher than optimal. This manifests silently — `KMeansModel` will report k cluster centers, but one will be a stale initialization point with a count of zero in the training summary. Always inspect `model.summary.clusterSizes` and assert that no cluster has zero members.

The k-means++ (`initMode = "k-means||"`, the default) reduces this risk dramatically but does not eliminate it. With k-means||, Spark runs `initSteps` (default 2) over-sampling passes, then runs a small K-Means on the O(k × initSteps) candidate centroids on the Driver to select the final k starting points. If `initSteps` is set to 1 on very non-spherical data, the over-sampling may still produce clumped candidates. Production pipelines should always run K-Means with at least 3 different random seeds and select the run with minimum WCSS.

### Choosing k: The Elbow Method vs. Silhouette Score

The most dangerous mistake in unsupervised learning is treating k as a hyperparameter to be guessed once and never validated. The elbow method — plotting WCSS against k and looking for a "kink" — is computationally cheap (one `KMeans.fit` per candidate k) but subjectively ambiguous; real-world datasets rarely produce a clean elbow. The silhouette score is a more principled metric, ranging from -1 (wrong cluster) to +1 (perfectly clustered), defined as `(b - a) / max(a, b)` where `a` is the mean intra-cluster distance and `b` is the mean nearest-cluster distance. Spark's `ClusteringEvaluator` computes a distributed silhouette using the squared Euclidean distance by default, which avoids the O(n²) all-pairs computation by exploiting the identity `‖x - y‖² = ‖x‖² - 2xᵀy + ‖y‖²`. This reduces the per-point computation from O(n) to O(k), making large-scale silhouette evaluation feasible. In practice, silhouette computation for n=100M with k=50 takes roughly the same wall-clock time as one K-Means iteration.

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|-----------|---------|-------|
| k-means++ Initialization | O(k · n · d) | No (broadcast + reduce) | k Spark jobs; each is one full dataset pass |
| Assignment Step (per iter) | O(n · k · d) | No | Embarrassingly parallel; BLAS-accelerated per partition |
| Centroid Update (treeAggregate) | O(n · d + k · d · log P) | No (tree reduce) | Driver never sees raw data; only k·d partial sums |
| `transform` (predict) | O(n · k · d) | No | Single broadcast + map; no shuffle whatsoever |
| Silhouette Score | O(n · k · d) | No | Cluster-stat broadcast + per-point O(k) computation |
| BisectingKMeans Fit | O(n · d · log k) | No per split (global shuffle between levels) | Divisive; each bisect is 2-means on a subset |

---

## 💻 Code Examples

### Example 1: Full K-Means Pipeline with k-means|| Initialization and Model Persistence

> **What this demonstrates:** The end-to-end production pipeline — feature assembly, K-Means fit with explicit k-means|| settings, cluster assignment, and saving/loading the model — illustrating how `KMeansModel.transform` is a zero-shuffle broadcast operation.

```python
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.clustering import KMeans
from pyspark.ml.pipeline import Pipeline

spark = SparkSession.builder \
    .appName("KMeans-MasterClass") \
    # Kryo serialization reduces broadcast size of centroid arrays by ~40%
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# Load a wide feature table — 50 numeric columns, ~10M rows
df = spark.read.parquet("s3a://datalake/user_features/")

# Step 1: Assemble raw columns into a single dense Vector column.
# VectorAssembler produces a DenseVector stored in Tungsten binary format.
assembler = VectorAssembler(
    inputCols=[f"feat_{i}" for i in range(50)],
    outputCol="raw_features"
)

# Step 2: StandardScaler — critical for K-Means because Euclidean distance
# is scale-sensitive. Without scaling, a feature with range [0, 10000]
# dominates features with range [0, 1], making clustering meaningless.
# withStd=True divides by stddev; withMean=True centers (zero-mean).
# centering requires a full-dataset pass (one Spark Job) to compute mean.
scaler = StandardScaler(
    inputCol="raw_features",
    outputCol="features",
    withStd=True,
    withMean=True
)

# Step 3: Configure K-Means.
# initMode="k-means||" is the parallel k-means++ variant (the default).
# initSteps=5 runs 5 over-sampling passes during initialization; higher
# values reduce WCSS variance across runs at the cost of 5 extra Spark jobs.
# tol=1e-4: stop if max centroid shift < 0.0001 (L2 norm).
# maxIter=50: hard cap to prevent runaway iteration on degenerate data.
kmeans = KMeans(
    k=25,
    featuresCol="features",
    predictionCol="cluster_id",
    initMode="k-means||",
    initSteps=5,
    maxIter=50,
    tol=1e-4,
    seed=42          # fix seed for reproducibility across runs
)

# Pipeline chains all stages; fit() triggers Spark jobs for scaler stats,
# k-means++ initialization, and then up to maxIter assignment+update jobs.
pipeline = Pipeline(stages=[assembler, scaler, kmeans])
model = pipeline.fit(df)

# transform() is a pure map operation — no shuffle, no aggregation.
# Spark broadcasts the k=25 centroids and assigns each row in parallel.
clustered_df = model.transform(df)
clustered_df.select("user_id", "cluster_id").write \
    .mode("overwrite") \
    .parquet("s3a://datalake/user_clusters/")

# Persist the full pipeline model for serving (scoring new users in batch).
model.write().overwrite().save("s3a://models/kmeans_user_v1/")
```

> **Mastery Note:** The `Pipeline.fit()` call here triggers at minimum `1 + initSteps + maxIter_actual` Spark Jobs in sequence — one for StandardScaler mean/stddev computation, five for k-means|| initialization passes, and up to 50 for the iteration loop. Inspecting the Spark UI's Jobs tab will show exactly how many iterations were required before convergence. If you see exactly 50 jobs for the K-Means phase, the algorithm hit `maxIter` without converging — a strong signal to increase `maxIter` or inspect for degenerate data (constant features, extreme outliers). Saving the `Pipeline` model (not just `KMeansModel`) preserves the `StandardScaler` statistics, ensuring that new data scored at inference time is normalized with the *training* mean and stddev, not recomputed stats — a critical correctness requirement.

---

### Example 2: Elbow Method + Silhouette Score Grid Search for Optimal k

> **What this demonstrates:** How to programmatically sweep candidate k values, compute both WCSS and distributed silhouette scores, and identify the optimal k — exposing how `ClusteringEvaluator` avoids O(n²) pairwise distance computation.

```python
from pyspark.ml.clustering import KMeans
from pyspark.ml.evaluation import ClusteringEvaluator
import pandas as pd

# Pre-assembled and scaled features DataFrame (reuse from Example 1 pipeline)
# Cache it: the feature DataFrame will be scanned once per candidate k value.
# Without caching, Spark recomputes the entire lineage for each KMeans.fit().
features_df = model.transform(raw_df) \
    .select("features") \
    .cache()

# Force materialization so the cache is warm before the sweep starts.
features_df.count()

candidate_ks = [5, 10, 15, 20, 25, 30, 40, 50]
results = []

# ClusteringEvaluator uses squared Euclidean distance by default.
# It computes per-cluster statistics (centroid, size) via broadcast and
# evaluates silhouette as (b - a) / max(a, b) per point in O(k) time —
# NOT O(n), making it feasible at n=100M scale.
evaluator = ClusteringEvaluator(
    featuresCol="features",
    predictionCol="prediction",
    metricName="silhouette",       # range [-1, +1]; higher is better
    distanceMeasure="squaredEuclidean"
)

for k in candidate_ks:
    km = KMeans(
        k=k,
        featuresCol="features",
        predictionCol="prediction",
        seed=42,
        maxIter=30
    )
    m = km.fit(features_df)

    # WCSS: sum of squared distances from each point to its assigned centroid.
    # Accessible via the training summary object — no extra Spark job needed.
    wcss = m.summary.trainingCost

    # Silhouette: requires transform (assign clusters) then evaluate.
    # transform() is a broadcast-map operation — zero shuffle.
    assigned = m.transform(features_df)

    # ClusteringEvaluator.evaluate() triggers one Spark Job:
    # it broadcasts centroid stats and computes per-point silhouette values
    # in a single mapPartitions pass, then reduces (average) with treeReduce.
    sil = evaluator.evaluate(assigned)

    results.append({"k": k, "wcss": wcss, "silhouette": sil})
    print(f"k={k:3d} | WCSS={wcss:,.0f} | Silhouette={sil:.4f}")

# Convert to Pandas for plotting (tiny result set — k candidates × 3 columns)
results_pdf = pd.DataFrame(results)
print(results_pdf.to_string(index=False))

# Unpersist to release executor memory for downstream jobs
features_df.unpersist()
```

> **Mastery Note:** The `features_df.cache()` call is not optional here — without it, each `KMeans.fit()` would re-read, re-parse, and re-scale the source Parquet files, multiplying I/O cost by `len(candidate_ks)`. With caching, the data is deserialized once into executor JVM heap memory (not off-heap Tungsten storage, because `cache()` defaults to `MEMORY_AND_DISK` with Java object serialization). For very wide feature vectors (d > 1000), consider `persist(StorageLevel.MEMORY_AND_DISK_SER)` with Kryo to halve the in-memory footprint. The `summary.trainingCost` property retrieves WCSS from an already-computed internal metric — it does not trigger an additional Spark job, making it essentially free. The silhouette score, by contrast, requires a full dataset scan and should be computed only for the final k candidates after the elbow method narrows the search space.

---

### Example 3: BisectingKMeans as a Scalable Alternative with Hierarchical Structure

> **What this demonstrates:** How `BisectingKMeans` uses a top-down divisive approach — recursively bisecting the largest cluster — producing a hierarchy that is more computationally efficient than standard K-Means for large k and naturally handles non-convex cluster shapes better than Lloyd's algorithm.

```python
from pyspark.ml.clustering import BisectingKMeans, BisectingKMeansModel
from pyspark.ml.evaluation import ClusteringEvaluator

# BisectingKMeans complexity: O(n · d · log k) vs K-Means O(n · k · d · iters).
# For k=100 and iters=20, K-Means does 2000 centroid comparisons per point,
# BisectingKMeans does ~7 (log₂ 100 ≈ 7). The difference is enormous at scale.
bkm = BisectingKMeans(
    k=50,                    # target number of leaf clusters
    featuresCol="features",
    predictionCol="cluster_id",
    maxIter=20,              # max iterations per bisection step (not total)
    minDivisibleClusterSize=20,  # clusters smaller than 20 points are not split
    # minDivisibleClusterSize can be a fraction [0,1] (treated as fraction of
    # total data) or integer (absolute count). Integer is safer in production.
    seed=42
)

bkm_model = bkm.fit(features_df)

# BisectingKMeansModel exposes a cluster hierarchy.
# clusterCenters() returns ALL intermediate + leaf centroids, not just leaves.
# The number of centers may be > k if the tree is unbalanced.
print(f"Number of leaf clusters: {len(bkm_model.clusterCenters())}")

# transform() assigns each point to its leaf cluster — identical API to KMeans.
# Internally, the model traverses the binary tree from root, choosing the
# nearest child at each level in O(d · log k) time per point.
assigned_bkm = bkm_model.transform(features_df)

evaluator = ClusteringEvaluator(
    featuresCol="features",
    predictionCol="cluster_id"
)
sil_bkm = evaluator.evaluate(assigned_bkm)
print(f"BisectingKMeans Silhouette: {sil_bkm:.4f}")

# Compare cluster size distribution — BisectingKMeans often produces more
# balanced clusters than K-Means because each split divides an existing cluster
# rather than competing for points globally.
cluster_sizes = assigned_bkm \
    .groupBy("cluster_id") \
    .count() \
    .orderBy("count", ascending=False)

cluster_sizes.show(10)

# Save the BisectingKMeans model for batch scoring
bkm_model.write().overwrite().save("s3a://models/bisecting_kmeans_v1/")
```

> **Mastery Note:** `BisectingKMeans` is dramatically more efficient than standard K-Means when k is large (k > 50) because each bisection step runs 2-means on only the *subset* of data belonging to the cluster being split, not the full dataset. This means the effective dataset per Spark Job shrinks exponentially as the tree deepens. The trade-off is that `BisectingKMeans` is a greedy divisive algorithm — once a cluster is split, the split is not reconsidered globally. This makes it sensitive to the order of splits and can produce suboptimal WCSS compared to fully iterated Lloyd's algorithm on the same k. In practice, for exploratory clustering with k > 30, BisectingKMeans is the recommended default in Spark MLlib because its runtime advantage is 5–20× while silhouette scores are typically within 5% of the Lloyd's optimum. Use standard K-Means only when you need guaranteed WCSS minimization and can afford the iteration cost.

---

### Example 4: Diagnosing Convergence Failures and Fixing Degenerate Clusters via Multiple Seeds

> **What this demonstrates:** Production-grade multi-seed K-Means training — running multiple independent fits with different random seeds, selecting the best by WCSS, and diagnosing pathological cluster size distributions that signal convergence failures or poor k selection.

```python
from pyspark.ml.clustering import KMeans
from pyspark.sql import functions as F
import math

# Production best practice: run K-Means with multiple random seeds and select
# the model with minimum WCSS. K-Means is not globally optimal — it converges
# to a local minimum whose quality depends heavily on initialization.
# With k-means|| and 3 seeds, the probability of all 3 runs landing in a
# poor local minimum is typically < 1% for well-separated clusters.

SEEDS = [42, 137, 2024]
K = 25
best_model = None
best_wcss = math.inf

for seed in SEEDS:
    km = KMeans(
        k=K,
        featuresCol="features",
        seed=seed,
        initMode="k-means||",
        initSteps=5,
        maxIter=50,
        tol=1e-4
    )
    m = km.fit(features_df)
    wcss = m.summary.trainingCost
    print(f"Seed={seed} | WCSS={wcss:,.2f}")

    if wcss < best_wcss:
        best_wcss = wcss
        best_model = m

print(f"\nBest WCSS: {best_wcss:,.2f} (selected model)")

# ── Diagnostic 1: Check for empty or near-empty clusters ──────────────────
# Empty clusters indicate initialization failure or k > true cluster count.
# Spark's KMeans does NOT raise an error for empty clusters — silent failure.
cluster_sizes = best_model.summary.clusterSizes  # List[Long], no Spark job
print("\nCluster size distribution:")
for idx, size in enumerate(cluster_sizes):
    status = "⚠️ EMPTY" if size == 0 else ("⚠️ TINY" if size < 100 else "OK")
    print(f"  Cluster {idx:3d}: {size:8,d} points  {status}")

# ── Diagnostic 2: Centroid spread — detect degenerate near-duplicate centroids
# Two centroids that are very close together indicate redundant clusters,
# a sign that k is too large or data is not well-separated.
centers = best_model.clusterCenters()  # List[Vector] on Driver
min_dist = math.inf
for i in range(len(centers)):
    for j in range(i + 1, len(centers)):
        # Squared Euclidean distance between centroid i and centroid j
        dist = sum((a - b) ** 2 for a, b in zip(centers[i], centers[j]))
        if dist < min_dist:
            min_dist = dist
            closest_pair = (i, j)

print(f"\nClosest centroid pair: {closest_pair}, dist²={min_dist:.6f}")
if min_dist < 0.01:
    print("⚠️  WARNING: Near-duplicate centroids detected. Consider reducing k.")

# ── Diagnostic 3: Intra-cluster variance per cluster ─────────────────────
# High variance in one cluster with low variance in others signals that
# cluster is a "catch-all" for noise — a common failure mode.
assigned = best_model.transform(features_df)

# Compute per-cluster point count and average squared distance to centroid
# using the model's transform output (prediction column already assigned).
# This requires one Spark Job (groupBy + agg).
intra_var = assigned \
    .groupBy("prediction") \
    .agg(
        F.count("*").alias("n"),
        # Avg squared L2 norm of features as a proxy for spread
        F.avg(F.aggregate(
            F.transform(F.col("features"), lambda x: x * x),
            F.lit(0.0).cast("double"),
            lambda acc, x: acc + x
        )).alias("avg_sq_norm")
    ) \
    .orderBy("avg_sq_norm", ascending=False)

intra_var.show(5, truncate=False)
```

> **Mastery Note:** The multi-seed approach is the single most impactful production improvement for K-Means quality. The cost is linear in the number of seeds — 3 seeds means 3× the wall-clock time of a single run — but with intelligent caching of the feature DataFrame (already done via `features_df.cache()`), the I/O cost is paid only once. The centroid distance diagnostic catches a failure mode that is invisible in the aggregate WCSS metric: when two centroids collapse onto the same dense region, WCSS may still look reasonable because the dense region is well-covered, but the cluster count is effectively k-1. If `min_dist² < 0.01` after StandardScaling (where all features have unit variance), the centroids are separated by less than 0.1 in normalized space — effectively the same centroid. The `clusterSizes` check in `model.summary` is free (computed during training as a side effect of the final aggregation step) and should be part of every production K-Means validation function.

---

## 🎯 Mastery Checklist

To achieve true mastery of K-Means Clustering in Apache Spark:

- [ ] Understand why `treeAggregate` (not `aggregate`) is used for centroid reduction and how its O(log P) message count prevents Driver GC pressure at P > 100 partitions
- [ ] Know that k-means|| over-samples candidates in O(log n) passes, not k passes like sequential k-means++, and what `initSteps` controls
- [ ] Be able to diagnose a convergence failure from the Spark UI by counting the number of K-Means phase jobs and comparing to `maxIter`
- [ ] Know when `BisectingKMeans` outperforms standard K-Means — specifically when k > 30 and runtime is more critical than WCSS optimality
- [ ] Understand the tradeoff between `MEMORY_AND_DISK` (Java serialized, fast deserialization) and `MEMORY_AND_DISK_SER` with Kryo (smaller footprint, slower deserialization) for caching the feature DataFrame during multi-k sweeps
- [ ] Know how `ClusteringEvaluator` computes silhouette in O(n·k) (not O(n²)) by exploiting the squared Euclidean identity, making it feasible at n > 10M
- [ ] Be able to detect near-duplicate centroids and empty clusters from `model.summary.clusterSizes` and `model.clusterCenters()` without triggering additional Spark jobs
- [ ] Understand why `StandardScaler` with `withMean=True` is a correctness requirement for K-Means, not merely a performance optimization

---

## 📚 Summary

Apache Spark's K-Means implementation is a masterclass in distributed algorithm design: it exploits the mathematical structure of Lloyd's algorithm (assignment is embarrassingly parallel; update is a simple aggregate) to achieve near-linear scaling with zero shuffle per iteration. The `TorrentBroadcast` mechanism distributes centroid arrays P2P across executors, and `treeAggregate` collapses the O(P) partial sums into O(log P) Driver-bound messages — two engineering decisions that together make the per-iteration cost dominated by raw compute, not network I/O. The k-means|| initialization provides an O(log k) approximation guarantee on WCSS with only O(log n) full-dataset passes, eliminating the main practical weakness of random seeding at the cost of `initSteps` additional Spark Jobs at startup. [[1]](spark_book.pdf#page=246)

The two most common production failure modes are silent empty clusters (detectable only via `model.summary.clusterSizes`) and convergence to a poor local minimum (mitigated by running 3+ random seeds and selecting minimum WCSS). `BisectingKMeans` is the preferred algorithm when k > 30 and the 5–20× runtime advantage outweighs the ~5% WCSS penalty relative to fully converged Lloyd's iteration. The distributed silhouette score from `ClusteringEvaluator` — running in O(n·k·d) rather than the naive O(n²) — makes programmatic k-selection feasible even at billion-row scale. [[2]](spark_book.pdf#page=274)

At the intersection of all these components lies a critical engineering insight: K-Means in Spark is not a single algorithm but a choreography of broadcast variables, treeReduce rounds, BLAS-accelerated inner loops, and JVM heap accumulators, coordinated by the DAGScheduler across a pipeline of sequentially dependent Spark Jobs. Understanding this choreography — not just the mathematical algorithm — is what separates a practitioner who can run K-Means from one who can tune, debug, and scale it in production. [[3]](spark_book.pdf#page=275)

