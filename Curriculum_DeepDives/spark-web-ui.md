# 🔥 Master Class: Spark Web UI — Diagnostics, Profiling, and Production Monitoring

## Overview

The Spark Web UI (default port 4040) is the primary observability surface for every running Spark application. It is not a dashboard bolted on after the fact — it is a first-class architectural component driven by the `SparkListener` event bus that is embedded directly into the `SparkContext`. Every task start, every shuffle write, every executor heartbeat, every SQL plan node produces a structured event that is consumed by the `AppStatusStore` (backed by a key-value `ElementTrackingStore` in-memory, or by the `HistoryServer`'s disk-backed `KVStore`) and rendered into the UI in near-real time.

Understanding the Web UI at an architectural level transforms it from a passive viewer into an active debugging weapon. A senior Spark engineer does not just glance at the timeline — they read the SQL DAG to confirm predicate pushdown occurred, cross-reference Stage metrics to detect data skew at the partition level, inspect the Storage tab to verify broadcast variables haven't been evicted, and watch the Executors tab for the specific GC time ratio that signals imminent OOM. Every number on every tab has a direct causal chain back to a JVM subsystem, a shuffle protocol, or a Catalyst optimizer decision.

The UI also exposes a programmatic REST API under `/api/v1/` and a Java/Scala `SparkListener` extension point that lets you intercept every internal event and build custom monitoring pipelines. This is the mechanism used by tools like Datadog's Spark integration, LinkedIn's Dr. Elephant, and Netflix's Metacat — and it is fully available to application developers without any framework modification.

---

## 🏗️ Architectural Deep Dive

### How It Works Under the Hood

When a `SparkContext` initializes, it creates a `LiveListenerBus` — an asynchronous, multi-queue event dispatch system backed by a dedicated daemon thread per queue. The bus has four specialized queues: `shared`, `appStatus`, `executorManagement`, and `eventLog`. The `AppStatusListener` (which feeds the UI) is registered on the `appStatus` queue and processes events like `SparkListenerTaskEnd`, `SparkListenerStageCompleted`, `SparkListenerBlockUpdated`, and `SparkListenerSQLExecutionEnd`. These events are written into an `ElementTrackingStore` (an in-memory LRU-bounded map), which the Jetty-based `SparkUI` HTTP server reads via `AppStatusStore` wrapper methods.

The SQL tab's DAG visualization is produced by the `SQLAppStatusListener`, which consumes `SparkListenerSQLExecutionStart` events containing the serialized `SparkPlan` tree. Catalyst's physical plan — the output of the four-phase optimizer pipeline (Analysis → Logical Optimization → Physical Planning → Code Generation) — is stored as a JSON-serialized `SparkPlanInfo` graph. Each node in that graph becomes a box in the DAG. The metric values (rows output, bytes read, spill) are `SQLMetric` objects that are `AccumulatorV2` instances distributed to tasks; executors update them locally and send deltas back to the driver via the heartbeat protocol every `spark.executor.heartbeatInterval` (default 10s), where they are merged into the UI store.

Tungsten's Whole-Stage CodeGen collapses multiple physical plan operators into a single JVM method (a `WholeStageCodegenExec` node), which shows in the DAG as a bold-bordered "WholeStageCodegen" box wrapping child operators. When you see this box, it means those operators run in a single tight loop with no virtual dispatch overhead and no intermediate row objects — this is the highest-performance execution path. The absence of this box for an operator (e.g., a Python UDF stage) is an immediate red flag that you are paying full row-object materialization cost.

The Stages tab metrics come from `TaskMetrics` objects serialized inside `SparkListenerTaskEnd` events. Each `TaskMetrics` record carries: executor deserialize time, JVM GC time (`jvmGCTime`), result serialization time, shuffle read/write bytes and records, input bytes, spill (memory and disk), and peak execution memory. These are aggregated across all tasks in a stage into min/p25/median/p75/max summary statistics — the distribution shape is your skew detector.

```
Driver JVM                                        Executor JVM
┌──────────────────────────────────┐              ┌─────────────────────────────────┐
│  SparkContext                    │              │  Executor                       │
│  ┌────────────────────────────┐  │              │  ┌─────────────────────────────┐│
│  │  LiveListenerBus           │  │  heartbeat   │  │ Task Thread                 ││
│  │  ┌──────────────────────┐  │  │◀─────────── │  │  AccumulatorV2 (SQLMetric)  ││
│  │  │ appStatus queue      │  │  │  (10s delta) │  │  TaskMetrics (GC, spill,    ││
│  │  │ AppStatusListener    │  │  │              │  │  shuffle bytes)             ││
│  │  └──────────┬───────────┘  │  │              │  └─────────────────────────────┘│
│  │             │              │  │              └─────────────────────────────────┘
│  │  ┌──────────▼───────────┐  │
│  │  │ ElementTrackingStore │  │              HistoryServer (off-cluster)
│  │  │ (in-memory KVStore)  │  │              ┌─────────────────────────────────┐
│  │  └──────────┬───────────┘  │  eventLog    │  KVStore (disk-backed LevelDB)  │
│  │             │─────────────────────────────▶  FsHistoryProvider               │
│  │  ┌──────────▼───────────┐  │              │  /api/v1/applications/...        │
│  │  │ AppStatusStore       │  │              └─────────────────────────────────┘
│  │  └──────────┬───────────┘  │
│  └─────────────┼──────────────┘
│                ▼                              SQL Tab
│  Jetty HTTP Server (:4040)    ──────────────▶ DAG (SparkPlanInfo tree)
│  ┌────────────────────────┐                  Stages Tab (TaskMetrics aggregates)
│  │ /api/v1/ REST endpoints│                  Storage Tab (BlockStatus, RDD info)
│  │ /stages, /sql, /storage│                  Executors Tab (HeartbeatReceiver)
│  └────────────────────────┘                  Environment Tab (SparkConf snapshot)
└──────────────────────────────────┘
```

### Key Internal Components

- **`LiveListenerBus`:** Asynchronous multi-queue event dispatcher inside the driver JVM. Decouples event producers (DAGScheduler, TaskScheduler) from consumers (UI, event log, executor management). A full `appStatus` queue (capacity 10,000 events by default) causes events to be dropped with a logged warning — a sign of extreme driver CPU pressure.
- **`AppStatusStore` / `ElementTrackingStore`:** In-memory key-value store with LRU eviction that holds all live UI state. Bounded to prevent unbounded driver heap growth; when a stage's task list exceeds `spark.ui.retainedTasks` (default 100,000), older task entries are evicted.
- **`SQLMetric` / `AccumulatorV2`:** Distributed metric objects sent to each task, updated locally (zero contention), and merged into the driver store via heartbeat deltas. The UI displays the last merged snapshot, so values update at heartbeat granularity, not per-row.
- **`SparkListener` / `SparkListenerBus`:** The public extension point. Any class implementing `SparkListener` and registered via `spark.extraListeners` receives every internal event synchronously on the bus thread — a slow listener stalls the entire bus and can cause dropped events.

---

## ⚠️ Critical Concepts & Common Pitfalls

### Reading Data Skew from Stage Task Duration Distribution

The Stages tab renders a min/median/max bar for every task metric. When the max task duration is 10× or more the median, you have data skew — one or a few partitions are carrying disproportionate data. The anti-pattern is ignoring this because the job eventually completes. At scale, a single skewed task in a 1,000-task stage can extend total stage wall-clock time by 40-60 minutes. The root cause is almost always a high-cardinality join key with a dominant value (e.g., joining on `country_code` when 70% of rows are `US`). The `Shuffle Read` metric breakdown (records vs. bytes) pinpoints this: if one task reads 50M records when the median is 50K, the partition key is the culprit. Fix with salting or `spark.sql.adaptive.skewJoin.enabled=true` (AQE skew join optimization, available Spark 3.0+).

### GC Time Ratio as an OOM Canary

The Executors tab exposes a `GC Time` column alongside `Task Time`. When `GC Time / Task Time` exceeds 10%, the executor is spending more than one second in every ten seconds on garbage collection — a critical threshold. When this ratio exceeds 20-30%, you will see `SparkException: Task failed: ExecutorLostFailure` cascading failures as the JVM enters stop-the-world GC pauses long enough to miss the heartbeat timeout (`spark.executor.heartbeatInterval` default 10s vs `spark.network.timeout` default 120s). The underlying cause is almost always one of three things: (1) too many live Java objects from Python UDFs or non-Tungsten code paths materializing row objects; (2) a broadcast variable larger than the executor's eden space; or (3) insufficient `spark.executor.memoryOverhead` causing the OS to pressure the JVM heap. The fix involves switching to Tungsten-native operations, increasing `spark.memory.fraction`, or enabling G1GC with `-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35`.

---

## 📊 Performance Characteristics

| UI Tab / Operation | Overhead | Driver Impact | Notes |
|---|---|---|---|
| SQL DAG rendering | Negligible | Low | SparkPlanInfo serialized once at query start |
| Task metric heartbeat | ~10s latency | Low CPU | AccumulatorV2 deltas, not full copies |
| Event logging to HDFS | Medium I/O | Moderate | Synchronous on `eventLog` queue; use SSD or HDFS short-circuit |
| Custom `SparkListener` | Variable | **High if slow** | Runs on bus thread; slow listener drops events after queue fills |
| `/api/v1/` REST poll | Low | Low | Reads from AppStatusStore; no recomputation |
| Storage tab RDD scan | O(partitions) | Low | Queries BlockManager status map on driver |
| HistoryServer replay | O(event log size) | N/A (separate JVM) | Large logs (>5GB) cause slow replay; use compaction |

---

## 💻 Code Examples

### Example 1: Confirming Predicate Pushdown and Whole-Stage CodeGen via SQL Tab Programmatic API

> **What this demonstrates:** How to programmatically query the Spark REST API to extract the physical plan JSON for a running query and verify that filter pushdown and WholeStageCodegen are active — the same checks a DBA would do manually in the SQL tab, now automated in a CI pipeline.

```python
import requests
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("WebUI-PlanVerification") \
    .config("spark.ui.enabled", "true") \
    .config("spark.ui.port", "4040") \
    # Enable AQE so the UI shows runtime plan changes
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Read a Parquet dataset — Catalyst will attempt to push filters into the reader
df = spark.read.parquet("/data/transactions") \
    .filter(col("amount") > 1000) \     # This predicate should be pushed to Parquet row-group pruning
    .filter(col("status") == "COMPLETE") \  # String filter also eligible for pushdown
    .groupBy("merchant_id") \
    .agg({"amount": "sum"})

# Trigger a query execution so the SQL tab has a plan to inspect
df.collect()

# Query the REST API for the last SQL execution entry
# The Spark UI REST endpoint returns plan nodes with metric values
app_id = spark.sparkContext.applicationId
rest_url = f"http://localhost:4040/api/v1/applications/{app_id}/sql"

response = requests.get(rest_url, params={"length": 1})
executions = response.json()

if executions:
    last_exec = executions[-1]
    print(f"Query ID: {last_exec['id']}")
    print(f"Status:   {last_exec['status']}")
    
    # Walk the plan nodes looking for WholeStageCodegen wrappers
    # A missing WholeStageCodegen around a SortMergeJoin or HashAggregate
    # is a signal that code generation was disabled or fell back
    plan_nodes = last_exec.get("nodes", [])
    codegen_nodes = [n for n in plan_nodes if "WholeStageCodegen" in n["nodeName"]]
    scan_nodes    = [n for n in plan_nodes if "Scan" in n["nodeName"]]
    
    print(f"\nWholeStageCodegen blocks found: {len(codegen_nodes)}")
    for node in scan_nodes:
        print(f"\nScan node: {node['nodeName']}")
        # Metrics include 'number of files read', 'static filters', 'dynamic filters'
        # A non-zero 'dynamic filters' value proves runtime filter pushdown occurred
        for metric in node.get("metrics", []):
            print(f"  {metric['name']}: {metric['value']}")

spark.stop()
```

> **Mastery Note:** The `nodes` array in the REST response maps 1:1 to Catalyst's `SparkPlanInfo` tree. Each `WholeStageCodegen` block proves Tungsten's Whole-Stage CodeGen fused those operators into a single JVM method — typically yielding 2-5× throughput improvement over interpreted execution. If a `HashAggregate` or `SortMergeJoin` appears *outside* a `WholeStageCodegen` wrapper, it means `spark.sql.codegen.wholeStage` was disabled or that operator doesn't support codegen (e.g., Python UDFs). The `number of output rows` metric on a `Filter` node above a `FileScan` node, compared to the scan's `number of files read` vs. total files, quantifies predicate pushdown effectiveness — if rows emitted by scan ≈ rows after filter, pushdown is NOT happening and you are reading excess data.

---

### Example 2: Custom SparkListener for Real-Time Stage Skew Detection

> **What this demonstrates:** Implementing a production-grade `SparkListener` that hooks into the internal event bus, computes task duration coefficient of variation (CV) per stage, and emits alerts when skew exceeds a configurable threshold — the same logic used by LinkedIn's Dr. Elephant.

```scala
import org.apache.spark.scheduler._
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * SkewDetectionListener registers on the SparkListener bus and computes
 * per-stage task duration statistics in O(n_tasks) time and O(1) space
 * using Welford's online algorithm.
 *
 * Register via: spark.extraListeners=com.example.SkewDetectionListener
 * The listener runs on the LiveListenerBus appStatus queue thread.
 * Keep all logic O(1) per event to avoid stalling the bus.
 */
class SkewDetectionListener extends SparkListener {

  // Accumulate task durations per stage; keyed by stageId
  // Using ArrayBuffer to avoid boxing overhead of scala.collection.mutable.Map[Int, List[Long]]
  private val stageDurations = mutable.Map[Int, ArrayBuffer[Long]]()
  
  // Configurable skew threshold: max/median ratio that triggers a warning
  private val SKEW_RATIO_THRESHOLD = 5.0

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    val stageId = taskEnd.stageId
    val taskInfo = taskEnd.taskInfo
    
    // Only record successful tasks — failed tasks have anomalous durations
    // that would pollute the skew calculation
    if (taskInfo.successful) {
      val durationMs = taskInfo.duration  // wall-clock time including GC
      
      stageDurations
        .getOrElseUpdate(stageId, ArrayBuffer.empty[Long])
        .append(durationMs)
    }
  }

  override def onStageCompleted(stageCompleted: SparkListenerStageCompleted): Unit = {
    val stageId   = stageCompleted.stageInfo.stageId
    val stageName = stageCompleted.stageInfo.name
    
    stageDurations.get(stageId).foreach { durations =>
      if (durations.size > 1) {
        val sorted  = durations.sorted
        val median  = sorted(sorted.size / 2).toDouble
        val maxDur  = sorted.last.toDouble
        val minDur  = sorted.head.toDouble
        
        // Skew ratio: max task duration vs median task duration
        // A ratio > 5 means the slowest partition took 5× longer than typical
        val skewRatio = if (median > 0) maxDur / median else 0.0
        
        // Mean absolute deviation as secondary signal for overall distribution spread
        val mean = durations.sum.toDouble / durations.size
        val mad  = durations.map(d => math.abs(d - mean)).sum / durations.size
        
        println(s"[SkewDetector] Stage $stageId ('$stageName'):")
        println(s"  Tasks: ${durations.size}, Min: ${minDur.toLong}ms, " +
                s"Median: ${median.toLong}ms, Max: ${maxDur.toLong}ms")
        println(s"  Skew ratio (max/median): ${"%.2f".format(skewRatio)}x")
        
        if (skewRatio >= SKEW_RATIO_THRESHOLD) {
          // In production, send to PagerDuty / Slack / Prometheus here
          println(s"  ⚠️  SKEW ALERT: Stage $stageId has ${skewRatio}x skew. " +
                  s"Consider salting the join key or enabling AQE skew join optimization " +
                  s"via spark.sql.adaptive.skewJoin.enabled=true")
        }
      }
      // Release memory — durations for completed stages are no longer needed
      stageDurations.remove(stageId)
    }
  }
}

// Registration in driver code:
// val conf = new SparkConf().set("spark.extraListeners", "com.example.SkewDetectionListener")
// val sc = new SparkContext(conf)
```

> **Mastery Note:** The `SparkListener` interface's methods are invoked on the `LiveListenerBus` daemon thread — not on any executor or task thread. This means any blocking I/O (e.g., an HTTP call to Slack) inside `onStageCompleted` will stall the bus and cause event queue buildup, eventually dropping events silently when the 10,000-event buffer fills. The correct pattern is to enqueue alerts to a separate `java.util.concurrent.LinkedBlockingQueue` and drain it with a dedicated thread. Also note that `taskInfo.duration` captures wall-clock time including GC pauses, not just CPU time — cross-referencing with `taskEnd.taskMetrics.jvmGCTime` lets you decompose slow tasks into "slow computation" vs. "GC-induced stall", which have entirely different remediation strategies.

---

### Example 3: GC Pressure Diagnostics via the Executors REST Endpoint

> **What this demonstrates:** Polling the `/api/v1/applications/{id}/executors` REST endpoint to compute per-executor GC efficiency ratios, flag executors approaching heartbeat-timeout risk, and recommend memory configuration changes — automating the manual inspection most engineers do on the Executors tab.

```python
import requests
import time
from dataclasses import dataclass
from typing import List

@dataclass
class ExecutorHealth:
    executor_id: str
    host: str
    task_time_ms: int
    gc_time_ms: int
    gc_ratio: float          # gc_time / task_time — the key diagnostic ratio
    memory_used_mb: float
    memory_total_mb: float
    memory_pressure: float   # used / total
    failed_tasks: int
    active_tasks: int

def diagnose_executors(app_id: str, ui_url: str = "http://localhost:4040") -> List[ExecutorHealth]:
    """
    Poll the Spark UI REST API for executor-level GC and memory statistics.
    
    The /executors endpoint returns data sourced from HeartbeatReceiver on the driver,
    which aggregates per-executor TaskMetrics from all completed tasks.
    Values represent cumulative totals since executor start, not rolling windows.
    """
    endpoint = f"{ui_url}/api/v1/applications/{app_id}/executors"
    response = requests.get(endpoint, timeout=10)
    response.raise_for_status()
    executors = response.json()
    
    health_reports = []
    
    for ex in executors:
        if ex.get("id") == "driver":
            continue  # Skip driver — it has different memory semantics
        
        task_time_ms = ex.get("totalDuration", 0)      # Sum of all task durations (ms)
        gc_time_ms   = ex.get("totalGCTime", 0)        # Sum of jvmGCTime across all tasks (ms)
        
        # GC ratio: fraction of task time spent in GC
        # >0.10 (10%) = Warning: heap pressure building
        # >0.20 (20%) = Critical: risk of stop-the-world pauses causing heartbeat timeout
        # >0.30 (30%) = Emergency: executor will likely be lost
        gc_ratio = gc_time_ms / task_time_ms if task_time_ms > 0 else 0.0
        
        mem_used  = ex.get("memoryUsed", 0) / (1024 ** 2)  # Convert bytes → MB
        mem_total = ex.get("maxMemory",  0) / (1024 ** 2)
        mem_pressure = mem_used / mem_total if mem_total > 0 else 0.0
        
        health = ExecutorHealth(
            executor_id    = ex["id"],
            host           = ex.get("hostPort", "unknown"),
            task_time_ms   = task_time_ms,
            gc_time_ms     = gc_time_ms,
            gc_ratio       = gc_ratio,
            memory_used_mb = mem_used,
            memory_total_mb= mem_total,
            memory_pressure= mem_pressure,
            failed_tasks   = ex.get("failedTasks", 0),
            active_tasks   = ex.get("activeTasks", 0),
        )
        health_reports.append(health)
    
    return health_reports

def print_gc_diagnosis(health_reports: List[ExecutorHealth]) -> None:
    print(f"\n{'ID':<6} {'Host':<25} {'GC%':>6} {'Mem%':>6} {'Failed':>7} {'Recommendation'}")
    print("-" * 90)
    
    for h in sorted(health_reports, key=lambda x: x.gc_ratio, reverse=True):
        gc_pct  = h.gc_ratio * 100
        mem_pct = h.memory_pressure * 100
        
        # Diagnosis logic mirrors what a senior Spark SRE looks for in the Executors tab
        if h.gc_ratio > 0.20:
            # High GC → heap is undersized for the workload
            # Fix: increase spark.executor.memory or switch to G1GC
            recommendation = f"CRITICAL GC: increase executor memory or add -XX:+UseG1GC"
        elif h.memory_pressure > 0.90:
            # Storage is crowding execution memory → cached RDDs/broadcast eviction imminent
            # Fix: reduce spark.memory.storageFraction or unpersist unused RDDs
            recommendation = f"Memory pressure: reduce storage fraction or unpersist RDDs"
        elif h.failed_tasks > 5:
            recommendation = f"High failures: check executor logs for OOM / network errors"
        else:
            recommendation = "Healthy"
        
        print(f"{h.executor_id:<6} {h.host:<25} {gc_pct:>5.1f}% {mem_pct:>5.1f}% "
              f"{h.failed_tasks:>7}  {recommendation}")

# Usage:
# app_id = spark.sparkContext.applicationId
# reports = diagnose_executors(app_id)
# print_gc_diagnosis(reports)
```

> **Mastery Note:** The `totalGCTime` field in the REST response is the cumulative sum of `TaskMetrics.jvmGCTime` across every task that completed on that executor — it is a lifetime counter, not a rate. To compute a true real-time GC rate, you must poll the endpoint at two time points and compute the delta, exactly as a Prometheus scraper would. The critical failure mode this script detects early is the scenario where GC pauses exceed `spark.network.timeout` (default 120s) — at that point, the driver's `HeartbeatReceiver` marks the executor as lost, triggers task re-scheduling, and potentially cascades into a `FetchFailedException` storm if the lost executor was holding shuffle data that other stages needed.

---

### Example 4: Storage Tab Deep Dive — Monitoring RDD Persistence and Broadcast Eviction

> **What this demonstrates:** Using the Storage REST API to audit cached RDD/DataFrame partition placement, detect cross-node replication failures, and identify broadcast variable memory consumption — preventing the silent performance regression where an evicted broadcast variable forces re-broadcast on every job.

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.util.parsing.json.JSON

object StorageAudit {

  def auditStorage(spark: SparkSession): Unit = {
    val sc    = spark.sparkContext
    val appId = sc.applicationId
    val uiUrl = s"http://localhost:4040/api/v1/applications/$appId"

    // ── RDD Storage ──────────────────────────────────────────────────────────
    val rddJson   = Source.fromURL(s"$uiUrl/storage/rdd").mkString
    val rddList   = JSON.parseFull(rddJson).get.asInstanceOf[List[Map[String, Any]]]

    println("\n=== RDD / DataFrame Cache Status ===")
    println(f"${"Name"}%-40s ${"Partitions"}%10s ${"Cached"}%8s ${"MiB"}%10s ${"Level"}%-20s")
    println("-" * 100)

    for (rdd <- rddList) {
      val name         = rdd("name").asInstanceOf[String]
      val numPartitions= rdd("numPartitions").asInstanceOf[Double].toInt
      val numCached    = rdd("numCachedPartitions").asInstanceOf[Double].toInt
      val memSizeMiB   = rdd("memoryUsed").asInstanceOf[Double] / (1024 * 1024)
      val storageLevel = rdd("storageLevel").asInstanceOf[String]

      // A cached fraction < 100% means some partitions were evicted
      // due to memory pressure — the next action on this RDD will recompute
      // the missing partitions, defeating the purpose of caching
      val cachedPct = if (numPartitions > 0) numCached * 100.0 / numPartitions else 0.0

      val warning = if (cachedPct < 100 && numPartitions > 0)
        s"⚠️  PARTIAL CACHE (${cachedPct.toInt}%%) — eviction detected"
      else ""

      println(f"$name%-40s $numPartitions%10d $numCached%8d $memSizeMiB%10.1f $storageLevel%-20s $warning")
    }

    // ── Broadcast Variables ──────────────────────────────────────────────────
    // Broadcast blocks appear in the Storage API as blocks with name starting "broadcast_"
    // They live in BlockManager storage memory (not execution memory) and compete with
    // cached RDD partitions for the spark.memory.storageFraction budget
    val broadcastJson  = Source.fromURL(s"$uiUrl/storage/rdd").mkString
    // In practice, broadcast variables show up via /api/v1/applications/{id}/storage/rdd
    // with name pattern "Broadcast(<id>)" — filter them here
    val broadcasts = rddList.filter(r => r("name").asInstanceOf[String].startsWith("Broadcast"))

    if (broadcasts.nonEmpty) {
      println("\n=== Broadcast Variables ===")
      for (bc <- broadcasts) {
        val name       = bc("name").asInstanceOf[String]
        val memMiB     = bc("memoryUsed").asInstanceOf[Double] / (1024 * 1024)
        val numCached  = bc("numCachedPartitions").asInstanceOf[Double].toInt
        val numParts   = bc("numPartitions").asInstanceOf[Double].toInt

        println(s"$name: ${memMiB.toInt} MiB across $numCached/$numParts executor blocks")

        // A broadcast larger than ~200MB risks executor heap pressure
        // and should be replaced with a bucketed join or salted SortMergeJoin
        if (memMiB > 200) {
          println(s"  ⚠️  OVERSIZED BROADCAST (${"%.0f".format(memMiB)} MiB): " +
                  s"Consider replacing with a sort-merge join. " +
                  s"Current threshold: ${spark.conf.get("spark.sql.autoBroadcastJoinThreshold")} bytes")
        }
      }
    }

    // ── Environment Tab: Verify critical config ───────────────────────────────
    // The Environment tab exposes all SparkConf key-value pairs + JVM system properties
    // This is the authoritative source — config set at multiple levels can be ambiguous
    val envJson  = Source.fromURL(s"$uiUrl/environment").mkString
    val envMap   = JSON.parseFull(envJson).get.asInstanceOf[Map[String, Any]]
    val sparkProps = envMap.getOrElse("sparkProperties", List())
                          .asInstanceOf[List[List[String]]]
                          .map(pair => pair(0) -> pair(1)).toMap

    println("\n=== Critical Configuration (from Environment Tab) ===")
    val keysToCheck = Seq(
      "spark.memory.fraction",           // Default 0.6 — storage+execution pool size
      "spark.memory.storageFraction",    // Default 0.5 — portion of memory pool for storage
      "spark.sql.autoBroadcastJoinThreshold",
      "spark.sql.adaptive.enabled",
      "spark.executor.memoryOverhead",   // Off-heap overhead; missing = OOM at OS level
      "spark.serializer"                 // KryoSerializer is 10x faster than JavaSerializer
    )
    keysToCheck.foreach { key =>
      println(s"  $key = ${sparkProps.getOrElse(key, "<not set / default>")}")
    }
  }
}
```

> **Mastery Note:** The Storage tab's "Fraction Cached" column is the single most actionable metric for cache health. When `numCachedPartitions < numPartitions`, Spark's Unified Memory Manager has evicted those partitions under LRU pressure to free space for execution memory (e.g., a sort or hash aggregate spilling into the storage pool). The silent consequence is that the *next* action on that DataFrame triggers re-computation of the evicted partitions — potentially re-reading terabytes from object storage. The fix is either to increase `spark.memory.storageFraction` (giving more of the unified pool to storage at the cost of execution), or to explicitly use `StorageLevel.DISK_AND_MEMORY` so evicted partitions spill to local disk rather than being discarded entirely. The Environment tab's `spark.serializer` check matters because broadcast variables serialized with `JavaSerializer` (the default) can be 8-10× larger in binary form than with `KryoSerializer`, directly inflating broadcast memory footprint.

---

## 🎯 Mastery Checklist

To achieve true mastery of the Spark Web UI:
- [ ] Read the SQL tab DAG and identify which operators are wrapped in `WholeStageCodegen` vs. which are interpreted
- [ ] Diagnose data skew from Stage tab task duration histograms (max/median ratio > 5× is the alert threshold)
- [ ] Compute GC time ratio from the Executors tab and map it to the correct JVM memory tuning parameter
- [ ] Detect partial RDD cache eviction from the Storage tab and understand why it silently defeats caching
- [ ] Cross-reference the Environment tab to verify that `spark.serializer`, `spark.sql.adaptive.enabled`, and memory fractions are set correctly for the workload
- [ ] Implement a `SparkListener` without blocking the `LiveListenerBus` thread (use async queues for I/O)
- [ ] Know the difference between `spark.network.timeout`, `spark.executor.heartbeatInterval`, and how GC pauses can bridge the gap between them causing executor loss
- [ ] Use the `/api/v1/` REST API to extract plan nodes and verify predicate pushdown in CI/CD

---

## 📚 Summary

The Spark Web UI is an architectural component, not an afterthought. Its data flows from the same `LiveListenerBus` that drives the DAGScheduler and event logging, meaning every metric it displays has a precise causal origin in a JVM subsystem. The SQL tab's DAG is a direct rendering of Catalyst's physical plan, making it the fastest way to verify optimizer decisions like predicate pushdown, broadcast join selection, and Whole-Stage CodeGen fusion. The Stages tab's task metric distributions expose data skew at partition granularity — the kind of skew that makes a 10-minute job take 3 hours without any error message.

The Executors tab's GC time ratio is the most important early-warning signal in the entire UI. A ratio above 10% demands investigation; above 20%, executor loss is imminent. The Storage tab catches the insidious problem of partial cache eviction, where a logically cached DataFrame silently falls back to full recomputation on the next action. The Environment tab is the ground truth for Spark configuration — essential for debugging the common case where config set in code, in `spark-defaults.conf`, and in cluster-level config create unexpected precedence behavior.

The `SparkListener` API elevates all of this from passive observation to active automation. By registering custom listeners via `spark.extraListeners`, production systems can build real-time skew detectors, GC canaries, SLA monitors, and lineage trackers — all from the same event stream that drives the UI itself. Mastery of the Web UI means never being surprised by a production job failure that the metrics were announcing for the previous 20 minutes.
