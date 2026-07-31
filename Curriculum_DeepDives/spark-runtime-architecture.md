# 🔥 Master Class: Spark Runtime Architecture

## Overview
<div style='text-align: right; margin-top: -10px; margin-bottom: 20px; font-size: 0.85rem; color: #a0aec0;'><em>References: [Ref: 451](spark_book.pdf#page=451) [Ref: 457](spark_book.pdf#page=457) [Ref: 461](spark_book.pdf#page=461) [Ref: 464](spark_book.pdf#page=464) [Ref: 452](spark_book.pdf#page=452) [Ref: 458](spark_book.pdf#page=458) [Ref: 462](spark_book.pdf#page=462) [Ref: 469](spark_book.pdf#page=469) [Ref: 455](spark_book.pdf#page=455) [Ref: 459](spark_book.pdf#page=459) [Ref: 463](spark_book.pdf#page=463) [Ref: 470](spark_book.pdf#page=470)</em></div>

Apache Spark's runtime architecture is a carefully layered distributed system built on the JVM, designed to execute DAGs of computation reliably across hundreds or thousands of machines. At the center of every Spark application is a strict separation between two roles: the **Driver** and the **Executor**. The Driver is the brain — it runs the user's main function, instantiates the `SparkContext`, analyzes the DAG via the Catalyst optimizer, and breaks that DAG into a sequence of `Stage`s and `Task`s through the `DAGScheduler`. The Executors are the muscle — JVM processes launched on worker nodes that receive serialized `Task` objects, execute them against partitioned data, and return results or shuffle output back through the cluster.

This architecture is not arbitrary. By centralizing scheduling and metadata in the Driver and pushing all data-plane computation to Executors, Spark achieves a clean fault-boundary: if an Executor dies, the Driver can re-schedule its Tasks on surviving Executors using lineage information from the RDD DAG. The Driver itself is a single point of failure, which is why high-availability Driver modes (e.g., Yarn cluster mode with AM failover, Kubernetes with restart policies) are mandatory for production deployments.

Understanding Spark's runtime deeply means understanding its JVM memory layout, how memory is partitioned between execution and storage within each Executor, how shuffle data flows through the `BlockManager`, and how `MapOutputTracker` coordinates the location of that shuffle data. Without this knowledge, you cannot correctly configure a production cluster, diagnose `OutOfMemoryError`s, or tune for throughput and latency simultaneously. 

---

## 🏗️ Architectural Deep Dive 

### How It Works Under the Hood

When a Spark application launches, the Driver JVM starts a `SparkContext` which registers with the cluster manager (YARN ResourceManager, Kubernetes API Server, or Mesos). The cluster manager allocates container resources and launches `CoarseGrainedExecutorBackend` processes on worker nodes — these are the Executor JVMs. Each Executor registers itself back with the Driver's `CoarseGrainedSchedulerBackend` over Netty RPC. From this point forward, all scheduling communication flows over this bidirectional RPC channel.

When the user calls an action (e.g., `.count()`), the `DAGScheduler` walks the RDD lineage graph backward, identifying `ShuffleDependency` boundaries as Stage breaks. Each `Stage` becomes a set of `Task`s — one per partition — serialized using Java serialization (or Kryo if `spark.serializer=org.apache.spark.serializer.KryoSerializer`) and sent to Executors via the `TaskScheduler` → `SchedulerBackend` → Netty pipeline. On the Executor side, the `TaskRunner` deserializes the Task and calls `Task.run()` inside a thread from the Executor's fixed-size thread pool (size = `spark.executor.cores`).

Inside the Executor JVM, memory is managed by the **Unified Memory Manager** (introduced in Spark 1.6), which divides the JVM heap into three logical regions. The **Reserved Memory** is a fixed 300MB held back for Spark internals and cannot be configured. The remainder is split between a **User Memory** fraction (default 40% of usable heap, controlled by `1 - spark.memory.fraction`) and a **Spark Memory Pool** (default 60% of usable heap, controlled by `spark.memory.fraction=0.6`). The Spark Memory Pool itself dynamically partitions between **Execution Memory** (used for sort buffers, hash tables, aggregation maps) and **Storage Memory** (used for RDD/DataFrame cache blocks). Either side can borrow from the other when idle, but Execution Memory can forcibly evict Storage Memory blocks to disk when under pressure — a critical asymmetry that means cached RDDs can be silently dropped during heavy aggregations.

The **Tungsten execution engine** operates primarily off-heap in the **DirectMemory** region (managed via `sun.misc.Unsafe`), storing binary-encoded rows in a compact format that avoids Java object overhead — a `String` field that costs 48+ bytes as a Java object costs exactly its character count in Tungsten binary format. Whole-Stage CodeGen collapses the entire operator pipeline into a single compiled JVM method, eliminating virtual dispatch and iterator overhead between operators, reducing the CPU cost of a query pipeline by 2-5x compared to interpreted execution.

```scala
Driver JVM Worker Node 1 (Executor JVM)
┌────────────────────────────────┐ ┌──────────────────────────────────────────────┐
│ SparkContext │ │ CoarseGrainedExecutorBackend │
│ ┌─────────────────────────┐ │ Netty │ ┌──────────────────────────────────────┐ │
│ │ DAGScheduler │◀──┼──RPC────▶│ │ Unified Memory Manager │ │
│ │ (Stage/Task creation) │ │ │ │ ┌─────────────┬──────────────────┐ │ │
│ └─────────────────────────┘ │ │ │ │ Reserved │ Spark Pool (60%)│ │ │
│ ┌─────────────────────────┐ │ │ │ │ 300MB │ ┌────────┬─────┐│ │ │
│ │ TaskScheduler │ │ │ │ │ │ │Exec Mem│Store││ │ │
│ │ SchedulerBackend │───┼──Tasks──▶│ │ │ │ │(fluid) │Cache││ │ │
│ └─────────────────────────┘ │ │ │ └─────────────┴──┴────────┴─────┘│ │ │
│ ┌─────────────────────────┐ │ │ │ User Memory (40%) │ Off-Heap(Tungsten)│ │
│ │ MapOutputTracker │◀──┼──shuffle─│ └──────────────────────────────────────┘ │
│ │ (Master) │ │ metadata│ ┌──────────────────────────────────────┐ │
│ └─────────────────────────┘ │ │ │ BlockManager (Slave) │ │
│ ┌─────────────────────────┐ │ │ │ ┌──────────┐ ┌──────────────────┐ │ │
│ │ BlockManagerMaster │◀──┼──block───│ │ │MemStore │ │ DiskStore │ │ │
│ │ (Driver-side registry) │ │ reports │ │ │(Heap/UMM)│ │(shuffle/persist) │ │ │
│ └─────────────────────────┘ │ │ └──────────────────────────────────────┘ │
└────────────────────────────────┘ │ ┌──────────────────────────────────────┐ │
 │ │ Thread Pool (spark.executor.cores) │ │
 │ │ Task 0 │ Task 1 │ Task 2 │ Task 3 │ │
 │ └──────────────────────────────────────┘ │
 └──────────────────────────────────────────────┘ 
```

### Key Internal Components

- **DAGScheduler:** Converts the RDD lineage DAG into `Stage`s by finding `ShuffleDependency` edges. Submits `TaskSet`s to the `TaskScheduler` and handles Stage failure/retry logic. Each `ResultStage` maps to a user action; each `ShuffleMapStage` produces shuffle output consumed by the next stage.

- **BlockManager:** A distributed key-value store embedded in every Driver and Executor JVM. Identified by a `BlockManagerId` (host, port, executor-id). Manages all block storage — RDD partitions, shuffle files, broadcast variables, and stream data — across `MemoryStore` (on-heap or off-heap) and `DiskStore`. Cross-Executor block fetches travel over Netty's `TransportServer`/`TransportClient` using zero-copy `FileRegion` transfers.

- **MapOutputTracker:** A fault-tolerant metadata service that tracks the location of every shuffle map output. The `MapOutputTrackerMaster` runs in the Driver; each Executor has a `MapOutputTrackerWorker` that fetches shuffle metadata via RPC. When a reduce Task starts, it queries the tracker to learn which Executor holds each mapper's output and opens direct Netty connections to fetch shuffle blocks — this is the **shuffle read** phase.

- **CoarseGrainedSchedulerBackend:** Maintains a long-lived RPC connection to each Executor. "Coarse-grained" means Executors are not released between Tasks — they hold their JVM process and memory allocation for the entire application lifetime, unlike fine-grained resource managers that deallocate between Tasks. This is the dominant model for all production cluster managers (YARN, K8s, Standalone). 

---

## ⚠️ Critical Concepts & Common Pitfalls 

### Executor Memory Misconfiguration and Silent OOM Eviction

The most destructive misconfiguration in Spark is setting `spark.memory.fraction` too high relative to the JVM heap, starving the garbage collector. The JVM heap is managed by the G1GC (default since JDK 9), which requires headroom to operate — typically 20-30% free heap. If `spark.executor.memory=8g` and `spark.memory.fraction=0.6`, then Spark claims 4.7GB (after subtracting 300MB reserved). If Execution Memory builds large sort buffers simultaneously across all cores, live JVM objects fill the remaining heap, triggering a `java.lang.OutOfMemoryError: GC overhead limit exceeded` — not the more diagnosable Spark OOM exception.

A subtler failure: when Execution Memory pressure evicts Storage Memory blocks, Spark does **not** log a warning at the default log level. You will see re-computation of cached DataFrames in the Spark UI (stages executing that should have been skipped), with no obvious error. The metric to watch is `Storage Memory Used` in the Executors tab — if it drops mid-job, eviction is occurring. Set `spark.memory.storageFraction=0.5` to reserve half the Spark pool for Storage and reduce eviction risk, accepting higher spill probability for aggregation. 

### Shuffle Fetch Failures and MapOutputTracker Inconsistency

When an Executor dies mid-shuffle, its map output files are lost. The `MapOutputTrackerMaster` detects the dead Executor (via the heartbeat timeout `spark.network.timeout`, default 120s) and invalidates its map output entries. The Stage that produced those outputs is re-submitted as a **FetchFailed** recovery — but only the specific map Tasks whose outputs were lost are re-run, not the entire Stage. This is controlled by `spark.stage.maxConsecutiveAttempts` (default 4).

The dangerous edge case is an Executor that is alive but under extreme GC pressure — it responds to heartbeats but fails to serve shuffle blocks within `spark.shuffle.io.connectionTimeout` (default 120s). The reducer receives a `FetchFailed` exception (`org.apache.spark.shuffle.FetchFailedException`), which the DAGScheduler interprets as a potential Executor loss. If the Executor is not actually dead, the Stage is retried unnecessarily, causing cascading delays. Monitoring GC time per Executor in the Spark UI (Executors tab → GC Time column) above 10% of task time is the diagnostic signal. 

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|-----------|---------|-------|
| `map` / `filter` / `select` | O(N/P) per partition | No | Pipelined by Tungsten; no data movement. Whole-Stage CodeGen fuses all operators into one JVM method. |
| `groupByKey` / `reduceByKey` | O(N log N) sort + O(N) aggregate | Yes | Sort-based shuffle (SortShuffleManager). Each mapper writes one sorted file + index. Reducer fetches and merges. |
| Broadcast Join | O(N) probe side, O(M) build side | No (for probe) | Driver collects small table, serializes via `TorrentBroadcast` (BitTorrent-like P2P), cached in Executor BlockManager. Threshold: `spark.sql.autoBroadcastJoinThreshold` default 10MB. |
| Sort-Merge Join | O(N log N + M log M) | Yes (both sides) | Both sides sorted by join key in shuffle. Requires two full shuffles. Avoidable with bucket tables that pre-sort data at write time. | 

---

## 💻 Code Examples

### Example 1: Inspecting Executor Memory Layout Programmatically

> **What this demonstrates:** How to read the actual Unified Memory Manager boundaries at runtime, confirming what fraction of JVM heap is allocated to each pool — critical for validating cluster configurations before production.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.memory.UnifiedMemoryManager

val spark = SparkSession.builder()
 // Allocate 4GB JVM heap per executor. The Unified Memory Manager
 // will reserve 300MB internally, then apply spark.memory.fraction=0.7
 // to the remaining ~3.7GB ≈ 2.59GB for Spark pool.
 .config("spark.executor.memory", "4g")
 // 70% of (heap - 300MB reserved) goes to Spark's unified pool.
 // The remaining 30% is user memory for UDFs, Python worker overhead,
 // and any data structures allocated outside Spark's memory tracking.
 .config("spark.memory.fraction", "0.7")
 // Within the Spark pool, guarantee at least 50% (≈1.3GB) for Storage.
 // Execution Memory can still use Storage's space when Storage is idle,
 // but Storage can reclaim this fraction before eviction occurs.
 .config("spark.memory.storageFraction", "0.5")
 .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
 // Kryo is 10x faster and 5x smaller than Java serialization for
 // task/closure serialization. Always enable in production.
 .getOrCreate()

val sc = spark.sparkContext

// Collect memory metrics from all executors via SparkContext.
// executorMemoryStatus returns (host:port -> (maxMem, remainingMem)) pairs.
// maxMem = the BlockManager's total Storage capacity (not full heap).
val memStatus = sc.getExecutorMemoryStatus
memStatus.foreach { case (executor, (maxMem, remainingMem)) =>
 val usedMem = maxMem - remainingMem
 val usedPct = (usedMem.toDouble / maxMem * 100).formatted("%.1f")
 // This reflects Storage Memory usage (cached RDD/DF blocks) only.
 // Execution Memory pressure is visible in the Spark UI Stages tab.
 println(s"Executor $executor: ${usedMem / 1024 / 1024}MB / ${maxMem / 1024 / 1024}MB used ($usedPct%)")
}

spark.stop()
```

> **Mastery Note:** `getExecutorMemoryStatus` reflects the `BlockManager`'s `maxMemory` — which is the Storage portion of the Spark pool, not the full executor heap. This is the `MemoryStore`'s maximum capacity as reported by `UnifiedMemoryManager.maxOnHeapStorageMemory`. Execution Memory (sort buffers, hash tables) is managed separately by `MemoryPool` objects and is not surfaced here. To see Execution Memory pressure, inspect `Task Metrics → Peak Execution Memory` in the Spark UI's Stages tab or via `SparkListener.onTaskEnd` callbacks. Enabling Kryo serialization is non-optional in production — default Java serialization of a simple case class with 10 fields generates ~3KB of bytes; Kryo produces ~200 bytes, directly reducing shuffle write volume.

---

### Example 2: Diagnosing BlockManager Behavior with Persistence Levels

> **What this demonstrates:** How different `StorageLevel` choices route data through different BlockManager subsystems — `MemoryStore` vs `DiskStore` vs off-heap — and the exact performance trade-offs each entails.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel

val spark = SparkSession.builder()
 .config("spark.executor.memory", "8g")
 // Enable off-heap memory for Tungsten's binary format and
 // MEMORY_AND_DISK_OFF_HEAP storage. Off-heap blocks are managed
 // via sun.misc.Unsafe and do not trigger JVM GC.
 .config("spark.memory.offHeap.enabled", "true")
 // 2GB of off-heap memory per executor, outside the JVM heap.
 // This memory is invisible to GC but counts toward container memory.
 // Always add 10-15% overhead: spark.executor.memoryOverhead = 2200m
 .config("spark.memory.offHeap.size", "2147483648")
 .getOrCreate()

val sc = spark.sparkContext

// Simulate a large reference dataset read repeatedly across many jobs.
val rawData = sc.textFile("hdfs:///data/reference/large_lookup.csv", minPartitions = 200)
 .map(line => line.split(","))
 .filter(fields => fields.length == 5)

// MEMORY_ONLY: Deserializes and stores JVM objects in the MemoryStore.
// Fast for access (no deserialization), but high GC pressure due to
// large object graphs. Evicted blocks are simply recomputed (no disk spill).
// BAD choice if the dataset barely fits in memory.
rawData.persist(StorageLevel.MEMORY_ONLY)

// MEMORY_AND_DISK_SER: Serializes rows to compact byte arrays (via Kryo)
// before storing in MemoryStore. If MemoryStore is full, spills to DiskStore.
// Reduces heap pressure by 3-5x vs MEMORY_ONLY for most datasets.
// Access cost: one deserialization step per partition read.
// rawData.persist(StorageLevel.MEMORY_AND_DISK_SER)

// OFF_HEAP: Stores serialized bytes in off-heap DirectMemory managed by
// Tungsten's MemoryAllocator. Completely GC-transparent. No disk fallback.
// Requires spark.memory.offHeap.enabled=true. Use for datasets that are
// frequently evicted from heap storage due to Execution Memory pressure.
// rawData.persist(StorageLevel.OFF_HEAP)

rawData.count() // triggers caching

// After the action, inspect which blocks are in which store.
// BlockManagerMaster on the Driver tracks all blocks across all Executors.
val blockIds = sc.getPersistentRDDs.flatMap { case (_, rdd) =>
 rdd.partitions.map(p => s"rdd_${rdd.id}_${p.index}")
}

println(s"Cached ${blockIds.size} partitions across the cluster.")

spark.stop()
```

> **Mastery Note:** The choice between `MEMORY_ONLY` and `MEMORY_AND_DISK_SER` is the single most impactful caching decision in production. `MEMORY_ONLY` stores fully deserialized Java objects, which cost 2-4x more heap than the raw data size due to JVM object headers (16 bytes per object), reference arrays, and padding. `MEMORY_AND_DISK_SER` with Kryo stores compact byte arrays that match compressed data sizes, reducing the GC root set by orders of magnitude. `OFF_HEAP` is the correct choice when the dataset must coexist with heavy aggregation workloads — Execution Memory cannot evict off-heap Storage blocks, providing true storage isolation. Always set `spark.executor.memoryOverhead` to at least 10% of `spark.memory.offHeap.size` when using off-heap to avoid container-level OOM kills from the cluster manager.

---

### Example 3: Controlling Shuffle Behavior and MapOutputTracker Metadata

> **What this demonstrates:** How shuffle partition count and shuffle manager configuration directly affect `MapOutputTracker` metadata size, shuffle file count, and the probability of `FetchFailed` exceptions — the most common cause of mysterious Stage retries in production.

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
 .config("spark.sql.shuffle.partitions", "400") \
 # 400 shuffle partitions means the MapOutputTrackerMaster stores
 # 400 × num_mappers entries (one per (mapper, reducer) pair).
 # For 200 executors × 4 cores = 800 map tasks, the tracker holds
 # 800 × 400 = 320,000 location records. At ~100 bytes each = 32MB
 # of Driver heap just for shuffle metadata. With 2000 map tasks,
 # this becomes 800MB — a common Driver OOM source at scale.
 .config("spark.shuffle.manager", "sort") \
 # SortShuffleManager (default) writes one sorted data file + one
 # index file per mapper per shuffle stage. Total shuffle files =
 # 2 × num_mappers. With BypassMergeSortShuffleHandle (triggered
 # when num_reducers < spark.shuffle.sort.bypassMergeThreshold=200),
 # it writes one file per reducer per mapper: num_mappers × num_reducers.
 # AVOID small bypassMergeThreshold with large partition counts.
 .config("spark.shuffle.sort.bypassMergeThreshold", "50") \
 .config("spark.reducer.maxSizeInFlight", "96m") \
 # Each reducer simultaneously fetches shuffle blocks from mappers.
 # maxSizeInFlight caps total in-flight bytes per reducer at 96MB.
 # Increasing this speeds up shuffle reads but raises Execution Memory
 # pressure. The fetched blocks are buffered in ExternalAppendOnlyMap
 # before being merged/aggregated.
 .config("spark.shuffle.io.retryWait", "10s") \
 # If a shuffle block fetch fails (FetchFailedException), Spark waits
 # 10s before retrying. Combined with spark.shuffle.io.maxRetries=3,
 # a degraded Executor has 30s to recover before a FetchFailed is
 # escalated to the DAGScheduler as an Executor loss event.
 .config("spark.shuffle.io.maxRetries", "3") \
 .getOrCreate()

# Simulate a wide transformation (groupBy + agg) that triggers a full shuffle.
df = spark.read.parquet("hdfs:///data/transactions/")

# groupBy forces a shuffle: each partition's rows are sorted by key and
# written to the shuffle file. The reducer fetches the relevant key-range
# from every mapper, then performs a local sort-merge aggregation.
result = df.groupBy("customer_id", "product_category") \
 .agg(
 F.sum("amount").alias("total_spend"),
 F.count("*").alias("transaction_count"),
 # approx_count_distinct uses HyperLogLog++ internally — a 16KB
 # sketch per group, serialized into the shuffle file as binary.
 # Far cheaper than exact count distinct which requires full shuffles
 # of deduplicated keys (O(N log N) sort vs O(N) sketch merge).
 F.approx_count_distinct("product_id", rsd=0.02).alias("unique_products")
 )

# Repartition to a smaller number AFTER aggregation to reduce output file count.
# Writing 400 output files from 400 reducers creates small files in HDFS/S3
# that hurt downstream read performance. Coalesce (not repartition) avoids
# a second shuffle — it just merges partitions in existing task boundaries.
result.coalesce(50).write.mode("overwrite").parquet("hdfs:///data/output/customer_summary/")

spark.stop()
```

> **Mastery Note:** The `spark.sql.shuffle.partitions` setting has a non-linear effect on both performance and Driver memory. Setting it too low (e.g., 10 partitions for 1TB of data) creates 100GB partitions that spill repeatedly to disk in `ExternalSorter`. Setting it too high (e.g., 10,000 for 10GB of data) creates 1MB shuffle files that overwhelm the `MapOutputTrackerMaster` with metadata and generate thousands of small Netty connections during the reduce phase. The empirically validated heuristic is **128MB per post-shuffle partition**: `num_partitions = total_shuffle_data_bytes / (128 * 1024 * 1024)`. Adaptive Query Execution (`spark.sql.adaptive.enabled=true`, default in Spark 3.x) solves this dynamically by coalescing small shuffle partitions at runtime, making manual tuning of `spark.sql.shuffle.partitions` largely obsolete for SQL workloads.

---

### Example 4: Network Topology-Aware Scheduling and Rack-Local Fallback

> **What this demonstrates:** How Spark's `TaskScheduler` uses HDFS block locality information and the cluster's network topology to prefer node-local task execution, and how to diagnose when locality degrades to rack-local or any-local — the difference between 1GB/s local reads and 125MB/s cross-rack reads.

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
 // locality.wait: Time the TaskScheduler waits for a node-local slot
 // before relaxing to rack-local. Default is 3 seconds. In a lightly
 // loaded cluster, increase to 10s to get better locality. In a heavily
 // loaded cluster, reduce to 0s to avoid Executor starvation.
 .config("spark.locality.wait", "10s")
 // Node-local: data and task on same Executor process. I/O speed: RAM or NVMe.
 .config("spark.locality.wait.node", "10s")
 // Rack-local: task on same network rack as data. I/O speed: ~1-10Gbps intra-rack.
 .config("spark.locality.wait.rack", "30s")
 // ANY: Cross-rack fetch. I/O speed: 1-10Gbps cross-rack but shared with all traffic.
 // A cross-rack read for a 1GB partition block adds ~8-80 seconds of pure network time.
 .config("spark.locality.wait.any", "0s") // don't wait once we've relaxed to ANY
 .config("spark.hadoop.dfs.replication", "3")
 // With 3 HDFS replicas, each block exists on 3 nodes. Spark's HDFS
 // InputFormat integration queries NameNode for block locations and
 // passes them to TaskScheduler via PreferredLocations on each Task.
 // The scheduler maps these hostnames to Executor IDs using the
 // ExecutorsByHost index maintained in CoarseGrainedSchedulerBackend.
 .getOrCreate()

val sc = spark.sparkContext
val sqlCtx = spark

// Read a large partitioned dataset. Spark calls InputFormat.getSplits(),
// which returns FileSplit objects each carrying a list of replica hosts.
// DAGScheduler.submitMissingTasks() calls rdd.preferredLocations() per
// partition, returning those replica hosts. TaskSetManager then schedules
// each Task to run on an Executor co-located with one of the replicas.
val events = sqlCtx.read
 .option("mergeSchema", "false") // disable schema merge to avoid extra NameNode round-trips
 .parquet("hdfs:///data/events/year=2024/")

// Force Task count to match HDFS block count for maximum node-locality.
// If numPartitions < numBlocks, some partitions span multiple blocks and
// lose locality guarantees. If numPartitions > numBlocks, excess Tasks
// have no preferred location and schedule as ANY-local immediately.
val blockAlignedEvents = events.repartition(events.rdd.partitions.length)

// Compute with a structured operation that Catalyst can push into the
// Parquet reader (predicate pushdown + column pruning):
// Only the `event_type` and `user_id` columns are read from disk (column pruning).
// The filter on event_type is pushed to the Parquet footer metadata reader,
// scanning only row groups where event_type min/max statistics overlap "purchase".
// On a 10TB dataset with 10% "purchase" events, this reduces I/O by ~90%.
val purchases = blockAlignedEvents
 .filter("event_type = 'purchase' AND amount > 100.0")
 .select("user_id", "amount", "event_ts")
 .groupBy("user_id")
 .agg(org.apache.spark.sql.functions.sum("amount").alias("total"))

purchases.write.mode("overwrite").parquet("hdfs:///data/output/high_value_users/")

// After the job, check locality level distribution via SparkListener.
// Locality levels are exposed per-task in TaskInfo.taskLocality.
// Target: >80% PROCESS_LOCAL or NODE_LOCAL in a well-configured cluster.
// If rack-local or any-local exceeds 20%, investigate Executor placement
// vs HDFS DataNode co-location (rack awareness in yarn-site.xml /
// spark.kubernetes.node.selector for K8s deployments).

spark.stop()
```

> **Mastery Note:** Task locality is the single most overlooked performance dimension in Spark deployments. A cluster where 50% of Tasks run at `ANY` locality is reading shuffle and HDFS data across the network at ~10Gbps shared bandwidth, while the same Tasks at `PROCESS_LOCAL` read from the OS page cache at 50GB/s+ effective throughput — a 5x difference in I/O speed that directly translates to wall-clock time. The `spark.locality.wait` defaults (3 seconds per level) are designed for lightly loaded clusters where preferred Executors become free quickly. In YARN clusters where Executors are time-shared across multiple applications, locality wait should be reduced to 0-1s to avoid Tasks sitting idle waiting for a preferred Executor that the cluster manager has allocated to another application. Always verify locality distribution in the Spark UI Stages tab under the "Locality Level" column, and correlate with the "Scheduler Delay" metric — high scheduler delay (>100ms) combined with poor locality indicates Executor resource contention.

---

## 🎯 Mastery Checklist

To achieve true mastery of Spark Runtime Architecture:

- [ ] Understand how `DAGScheduler` converts RDD lineage into `ShuffleMapStage` / `ResultStage` boundaries and how FetchFailed exceptions trigger selective Stage re-computation — not full job restarts
- [ ] Know when `MEMORY_AND_DISK_SER` outperforms `MEMORY_ONLY` in terms of GC pressure, and calculate the exact heap cost of storing 1GB of Parquet data at each `StorageLevel`
- [ ] Be able to diagnose Execution Memory evicting Storage Memory from the Spark UI (`Storage Memory Used` drop mid-job) vs. a genuine `OutOfMemoryError` in `ExternalSorter`
- [ ] Understand the tradeoff between `spark.sql.shuffle.partitions` (manual) and Adaptive Query Execution's dynamic partition coalescing — when AQE fails to coalesce (e.g., skewed joins) and why
- [ ] Know how `MapOutputTrackerMaster` Driver heap usage scales with `(num_map_tasks × shuffle_partitions)` and how to size Driver memory accordingly
- [ ] Understand how `TorrentBroadcast` distributes broadcast variables P2P through `BlockManager` rather than Driver→all-Executors, and why this matters at 500+ Executor scale
- [ ] Be able to interpret Locality Level distribution from the Spark UI and correlate it with Scheduler Delay, Task Duration, and `spark.locality.wait` configuration
- [ ] Know how Tungsten's off-heap binary format and Whole-Stage CodeGen interact with the JVM GC — specifically why off-heap storage eliminates GC pause correlation with dataset size

---

## 📚 Summary

Spark's runtime architecture is a precisely engineered contract between the Driver JVM and Executor JVMs: the Driver holds all metadata (DAG, stage graph, shuffle locations via `MapOutputTracker`, block registry via `BlockManagerMaster`) while Executors hold all data (partition bytes in `MemoryStore`/`DiskStore`, shuffle files on local disk). This separation of concerns enables the fundamental fault-tolerance guarantee: any Executor can be lost and its Tasks re-scheduled on surviving Executors using the immutable RDD lineage graph maintained in the Driver. 

The `UnifiedMemoryManager` is the most operationally significant internal component — its dynamic boundary between Execution and Storage Memory means that a heavy aggregation job and a cached reference dataset compete for the same pool of bytes, and the aggregation always wins (Execution can evict Storage; Storage cannot evict Execution). Understanding this asymmetry explains a class of production failures where cached DataFrames silently disappear under load, causing re-computation that looks like query regression. The Tungsten engine's off-heap binary format severs the link between dataset size and GC pause duration, which is why off-heap caching is the correct solution when both high-throughput aggregation and stable caching are required simultaneously. 

Network topology awareness and shuffle architecture are the final pillars. Every shuffle write produces exactly one sorted file + one index file per mapper (SortShuffleManager), and the `MapOutputTracker` must hold location records for every `(mapper, reducer)` pair in Driver heap. At 2,000 map tasks × 1,000 shuffle partitions, this is 2 million records — 200MB of Driver heap minimum. Designing Spark jobs means designing for memory at every layer: Driver metadata memory, Executor Execution Memory, Executor Storage Memory, off-heap Tungsten buffers, and the network shuffle — each with its own failure mode and its own configuration lever. 

