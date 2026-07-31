# 🔥 Master Class: Spark History Server

## Overview

The Spark History Server (SHS) is the post-mortem observability layer of the Spark ecosystem. While the Spark UI embedded in the Driver JVM provides a live view of a running application, the History Server reconstructs that same UI from persisted event logs after the application terminates. Every Spark application that has `spark.eventLog.enabled=true` writes a structured stream of JSON-encoded `SparkListenerEvent` objects to a configurable directory — this stream is the event log, and the History Server is its reader.

The SHS exists because production Spark workloads are ephemeral. Drivers die, YARN containers are reclaimed, and Kubernetes pods are deleted. Without a durable, queryable record of execution — stage timelines, task metrics, shuffle read/write sizes, GC times, SQL physical plans — diagnosing regressions and tail-latency problems becomes guesswork. The History Server turns the raw event-log stream into a fully interactive UI, including the SQL tab with physical plan visualization, the stages tab with task distribution histograms, and the environment tab listing every effective configuration parameter.

The SHS is not just a log viewer. It maintains its own in-process key-value store (KVStore), serves a REST API used by external monitoring systems, and supports pluggable backends for storing parsed application metadata. Understanding its internals is essential for operating Spark at scale, where thousands of completed applications must remain queryable without exhausting the History Server's heap. 

---

## 🏗️ Architectural Deep Dive 

### How It Works Under the Hood

When an application starts with `spark.eventLog.enabled=true`, the `EventLoggingListener` — a `SparkListener` registered on the Driver's `LiveListenerBus` — serializes every scheduler event (job start/end, stage submitted/completed, task start/end, executor added/removed, SQL execution start, environment update) to a JSON line in the event log file. The file is opened under a `.inprogress` suffix and renamed atomically on application completion. Each JSON line is a self-describing object with a `"Event"` discriminator field, e.g., `"org.apache.spark.scheduler.SparkListenerJobStart"`. Compression is applied at the codec level (LZ4, Snappy, or Zstd) before bytes are written to the underlying filesystem, which may be local POSIX, HDFS, or Amazon S3.

The History Server process runs a Jetty HTTP server (the same embedded server used by the Driver UI) and a background `FsHistoryProvider` that scans the event log directory. On startup and on a configurable polling interval (`spark.history.fs.update.interval`, default 10s), the provider lists the directory, detects new or modified log files, and submits them to a replay thread pool. Replay means reading the compressed JSON lines sequentially and re-firing each event through an `AppStatusListener` that rebuilds all application state into an in-process `KVStore`.

The KVStore is the critical internal component for scalability. By default it is a `InMemoryStore` (a `ConcurrentHashMap`-backed store living entirely on the JVM heap), but it can be switched to a `LevelDB`-backed disk store via `spark.history.store.path`. The LevelDB backend serializes application state to disk using a Kryo-based binary format, allowing the History Server to manage thousands of completed applications without proportionally increasing heap usage. Each application gets its own LevelDB instance under the store path. When the History Server evicts an application from memory (controlled by `spark.history.retainedApplications`, default 50), the LevelDB files remain on disk and are reloaded on demand, keeping eviction transparent to the UI user.

Rolling event logs, introduced to address the problem of enormous single-file event logs that can reach tens of GBs for long-running streaming jobs, partition the log stream into fixed-size files. When `spark.eventLog.rolling.enabled=true` and a file exceeds `spark.eventLog.rolling.maxFileSize` (default 128MB), the current file is closed and a new one opened in the same application directory. The History Server replays all rolling files in order, correctly reconstructing a unified application view across the file boundaries.


### Key Internal Components

- **`EventLoggingListener`:** Registered on the `LiveListenerBus`, it intercepts every `SparkListenerEvent` on the Driver and serializes them as newline-delimited JSON to the event log. It buffers writes to avoid I/O becoming a bottleneck on the driver's event dispatch thread. On application completion, it flushes, closes the codec stream, and triggers the atomic rename from `.inprogress` to the final filename.

- **`FsHistoryProvider`:** The core SHS backend class. It maintains a map of all discovered application log files, manages a `ThreadPoolExecutor` for parallel log replay, handles listing retries for eventually-consistent filesystems (critical for S3), and exposes the `ApplicationHistoryProvider` interface consumed by the Jetty servlet layer. It also implements the REST API handlers (`/api/v1/applications`, `/api/v1/applications/{id}/stages`, etc.).

- **`KVStore` / `ElementTrackingStore`:** The in-process database abstraction. `ElementTrackingStore` wraps either `InMemoryStore` or `LevelDBKVStore` and enforces per-entity retention limits (e.g., max 10,000 tasks per stage, controlled by `spark.ui.retainedTasks`). When a limit is breached, the oldest entries are evicted. For LevelDB, the underlying storage uses a column-family-like key prefix scheme with Kryo serialization, making range scans by stage ID or job ID extremely efficient.

- **`AppStatusListener`:** The replay listener that consumes a sequence of `SparkListenerEvent` objects (either live from the `LiveListenerBus` or replayed from a log file) and mutates the KVStore accordingly. It is shared between the live Driver UI and the SHS replay path, ensuring behavioral parity between live and historical views. 

---

## ⚠️ Critical Concepts & Common Pitfalls 

### S3 Event Log Consistency and Listing Latency

S3 is an eventually-consistent object store (prior to S3 Strong Consistency released in 2020, but `list` operations on prefixes can still exhibit high latency at scale). The `FsHistoryProvider` polls the event log directory using the configured filesystem's `listStatus` call. On HDFS this is a single NameNode RPC; on S3 with millions of completed application directories, a recursive `list` can take minutes and throttle other S3 operations due to request-rate limits. The fix is to use `spark.history.fs.eventLog.rolling.maxFilesToRetain` to bound directory size, enable S3 directory-level event log paths instead of flat files (`spark.eventLog.dir=s3a://bucket/spark-logs/`), and configure the S3A committer with `fs.s3a.list.version=2` for strongly-consistent listings.

A silent failure mode: if the History Server's IAM role or S3A credentials lack `s3:ListBucket` permission on the prefix, `listStatus` returns an empty array rather than throwing an exception. The SHS silently shows no applications. Always verify permissions with `hadoop fs -ls s3a://bucket/spark-logs/` from the SHS host before diagnosing log parsing issues. 

### KVStore Heap Explosion Under Default Configuration

With `InMemoryStore` (the default), every replayed application's full state — including all task metrics for every stage — lives in the JVM heap. A single application with 100 stages × 1,000 tasks per stage × 200 bytes of metrics per task consumes ~20MB of heap. With `spark.history.retainedApplications=50` (default), the SHS can hold 50 applications in memory simultaneously, consuming ~1GB of heap just for task metrics — before accounting for SQL plan objects, executor metrics, and the Jetty thread pool. Setting `spark.history.store.path` to a fast local NVMe directory and switching to the LevelDB backend reduces resident heap to less than 512MB regardless of how many applications have been loaded, because only the index (not the full data) is cached in memory. The LevelDB disk store adds ~50–100ms latency per application page load due to disk seeks, which is imperceptible to humans but must be considered in SHS REST API automation. 

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|-----------|---------|-------|
| Event log write (per event) | O(1) amortized | No | Buffered codec stream; codec flush on stage/job boundary |
| SHS directory scan (HDFS) | O(n) in app count | No | Single NameNode `listStatus` RPC; fast for <100k apps |
| SHS directory scan (S3) | O(n/1000) paged | No | S3 ListObjectsV2 returns 1000 keys/page; high latency at scale |
| Log replay (single app) | O(e) in event count | No | Sequential read; replay rate ~500k events/sec on modern hardware |
| KVStore range query (LevelDB) | O(log n + k) | No | LevelDB LSM-tree index; k = result set size |
| Rolling log file rotation | O(1) | No | New file opened; prior file closed and fsync'd atomically | 

---

## 💻 Code Examples

### Example 1: Full Production SHS Configuration for S3-Backed Event Logs with Rolling Files

> **What this demonstrates:** How to configure the History Server, the Spark application, and the S3A filesystem connector together for reliable event logging at scale, including rolling log files, LevelDB KVStore, and proper S3 credential handling.

```python
# spark_history_server_config.py
# Production-grade configuration generator for SHS + S3 event logs.
# Run on your cluster management host to emit spark-defaults.conf entries.

HISTORY_SERVER_CONF = {

 # ── Application-side event log settings ──────────────────────────────────
 # Enable the EventLoggingListener on every application.
 "spark.eventLog.enabled": "true",

 # S3A path for event logs. ALL applications write here.
 # Use a dedicated bucket prefix to isolate listing scope.
 "spark.eventLog.dir": "s3a://my-company-spark-logs/event-logs/",

 # Enable Zstd compression: ~40% smaller than LZ4, decompresses at 1GB/s+.
 # The SHS replay thread decompresses on the fly; Zstd is preferred for S3
 # because network transfer cost dominates CPU decompression cost.
 "spark.eventLog.compress": "true",
 "spark.eventLog.compression.codec": "zstd",

 # ── Rolling event log settings ────────────────────────────────────────────
 # Partition the log stream into 128MB files. This prevents the History Server
 # from having to read a single 50GB file to reconstruct a long-running app.
 "spark.eventLog.rolling.enabled": "true",

 # Each rolling file is closed and a new one opened when this threshold is reached.
 # Default is 128m. For streaming apps running >24h, use 256m.
 "spark.eventLog.rolling.maxFileSize": "128m",

 # ── History Server settings ───────────────────────────────────────────────
 # Point SHS to the same S3A prefix as the applications.
 "spark.history.fs.logDirectory": "s3a://my-company-spark-logs/event-logs/",

 # Poll every 30s instead of the 10s default to reduce S3 list API costs.
 # At 1000 apps/hour throughput, 30s polling is still sub-minute discovery.
 "spark.history.fs.update.interval": "30s",

 # Switch from InMemoryStore to LevelDB. The path must be on fast local NVMe.
 # Without this, each replayed app consumes ~20-50MB of JVM heap.
 "spark.history.store.path": "/mnt/nvme/shs-kvstore",

 # Retain 5000 applications on the listing page. LevelDB makes this cheap.
 "spark.history.retainedApplications": "5000",

 # Limit tasks retained per stage to prevent KVStore from growing unboundedly
 # for applications with hundreds of thousands of tasks.
 "spark.ui.retainedTasks": "100000",

 # ── S3A filesystem tuning for SHS event log listing ───────────────────────
 # Use the S3A list v2 API for strongly-consistent, paginated listings.
 "spark.hadoop.fs.s3a.list.version": "2",

 # Increase the S3A connection pool to allow parallel log replays.
 # The SHS replay thread pool defaults to CPU count; each thread needs a connection.
 "spark.hadoop.fs.s3a.connection.maximum": "200",

 # Use instance profile credentials on EC2; remove if using explicit keys.
 "spark.hadoop.fs.s3a.aws.credentials.provider":
 "com.amazonaws.auth.InstanceProfileCredentialsProvider",
}

for key, value in HISTORY_SERVER_CONF.items():
 print(f"{key}={value}")
```

> **Mastery Note:** The combination of `spark.eventLog.rolling.enabled=true` and `spark.history.store.path` (LevelDB) is the critical production pairing. Without rolling, a single long-running Spark Structured Streaming application can produce a 50–100GB event log that takes 20+ minutes to replay and exhausts the SHS heap. Without LevelDB, `spark.history.retainedApplications=5000` is effectively impossible because the InMemoryStore would require 100–250GB of heap. Together, they allow a single SHS instance to serve thousands of concurrent queries against thousands of completed applications with a 4GB heap. Note also that `spark.hadoop.fs.s3a.list.version=2` is non-negotiable for correctness: the v1 API returns stale listings when `PUT` and `DELETE` operations race with `LIST` on the same prefix.

---

### Example 2: Parsing the Event Log JSON Stream Programmatically

> **What this demonstrates:** How to read and decode the raw event log format directly — a technique used by external monitoring pipelines, SLA auditors, and custom cost-allocation tools that need application metrics without running a full SHS instance.

```python
# parse_event_log.py
# Reads a Spark event log file (compressed or uncompressed) and extracts
# per-stage shuffle metrics for cost/performance analysis pipelines.

import json
import lz4.frame # pip install lz4
import zstandard # pip install zstandard
import sys
from pathlib import Path
from collections import defaultdict

def open_event_log(path: str):
 """
 Detect codec from filename extension and return a text stream.
 Spark appends the codec name to the filename: app_..._.lz4, app_..._.zstd.
 Uncompressed logs have no codec suffix.
 """
 p = Path(path)
 if p.suffix == ".lz4":
 # LZ4 frame format; lz4.frame handles multi-frame files correctly.
 raw = open(p, "rb")
 return lz4.frame.open(raw, mode="rt", encoding="utf-8")
 elif p.suffix in (".zst", ".zstd"):
 # Zstandard streaming decompression; very fast even at high compression.
 dctx = zstandard.ZstdDecompressor()
 raw = open(p, "rb")
 return dctx.stream_reader(raw, closefd=True).__enter__().__iter__()
 else:
 return open(p, "r", encoding="utf-8")

def extract_stage_shuffle_metrics(event_log_path: str) -> dict:
 """
 Replay the event log stream, capturing shuffle bytes read/written per stage.
 This mirrors what AppStatusListener does inside the History Server JVM,
 but in a lightweight Python process with no KVStore overhead.
 """
 stage_metrics = defaultdict(lambda: {
 "shuffleWriteBytes": 0,
 "shuffleReadBytes": 0,
 "executorCpuTime": 0,
 "taskCount": 0,
 })

 with open_event_log(event_log_path) as f:
 for line in f:
 line = line.strip()
 if not line:
 continue # Skip blank separator lines between rolling file sections

 try:
 event = json.loads(line)
 except json.JSONDecodeError:
 # Truncated log (e.g., driver crashed mid-write); skip and continue.
 continue

 event_type = event.get("Event", "")

 # SparkListenerTaskEnd carries per-task metric accumulators.
 # This is the most granular and most numerous event type in the log.
 if event_type == "SparkListenerTaskEnd":
 stage_id = event.get("Stage ID", -1)
 metrics = event.get("Task Metrics", {})

 # Shuffle write metrics: bytes written to the shuffle block store
 # by this map task. These bytes cross the network during reduce.
 sw = metrics.get("Shuffle Write Metrics", {})
 stage_metrics[stage_id]["shuffleWriteBytes"] += (
 sw.get("Shuffle Bytes Written", 0)
 )

 # Shuffle read metrics: bytes fetched from remote BlockManagers
 # by this reduce task. Remote reads trigger network I/O.
 sr = metrics.get("Shuffle Read Metrics", {})
 stage_metrics[stage_id]["shuffleReadBytes"] += (
 sr.get("Remote Bytes Read", 0)
 )

 # Executor CPU time in nanoseconds (excludes scheduler overhead,
 # GC, and I/O wait). Useful for distinguishing CPU-bound vs I/O-bound stages.
 stage_metrics[stage_id]["executorCpuTime"] += (
 metrics.get("Executor CPU Time", 0)
 )
 stage_metrics[stage_id]["taskCount"] += 1

 return dict(stage_metrics)

if __name__ == "__main__":
 path = sys.argv[1] # Pass event log path as CLI argument
 metrics = extract_stage_shuffle_metrics(path)
 for stage_id, m in sorted(metrics.items()):
 print(
 f"Stage {stage_id:3d}: "
 f"tasks={m['taskCount']:6d} "
 f"shuffleWrite={m['shuffleWriteBytes']/1e9:.2f}GB "
 f"shuffleRead={m['shuffleReadBytes']/1e9:.2f}GB "
 f"cpuTime={m['executorCpuTime']/1e9:.1f}s"
 )
```

> **Mastery Note:** The event log stream is a complete, ordered record of application execution — every metric that the Spark UI displays is derived from it. The key architectural insight here is that `SparkListenerTaskEnd` events contain `Task Metrics` accumulators that are the final, rolled-up values from the executor's `TaskContext`, sent to the driver over RPC at task completion. These are the same metrics that `AppStatusListener` aggregates into stage-level summaries in the KVStore. By parsing the log directly, you bypass the entire SHS replay infrastructure and can process terabytes of historical logs in a Spark job itself, treating each event log file as an input partition and using `json_tuple` or `from_json` to extract specific event types at the executor level — a meta-pattern used by Databricks Cost Attribution and LinkedIn's Dr. Elephant.

---

### Example 3: Configuring NFS-Backed Event Logs for On-Premises Clusters

> **What this demonstrates:** The SHS configuration for POSIX NFS mounts, including the critical `spark.history.fs.inProgressOptimization.enabled` flag and the interaction between NFS client-side caching and the `.inprogress` file detection mechanism.

```bash
#!/usr/bin/env bash
# configure_shs_nfs.sh
# Mounts an NFS export and configures SHS for on-premises clusters.
# Run as root on the History Server node.

# ── Step 1: Mount the NFS export ─────────────────────────────────────────────
# Use noatime to prevent read operations from updating inode access times,
# which would generate spurious NFS SETATTR RPCs on every log read.
# Use hard,intr to retry NFS operations on network blips rather than failing.
# Use rsize/wsize=1048576 for 1MB read/write chunks (NFS default is 4KB).
mount -t nfs \
 -o noatime,hard,intr,rsize=1048576,wsize=1048576,nfsvers=4.1 \
 nfs-server.internal:/exports/spark-logs \
 /mnt/nfs/spark-logs

# ── Step 2: Write SHS configuration to spark-defaults.conf ───────────────────
cat >> /opt/spark/conf/spark-defaults.conf << 'EOF'

# NFS mount point for event logs. All applications write here.
spark.eventLog.enabled=true
spark.eventLog.dir=file:///mnt/nfs/spark-logs

# Compress with LZ4 for lowest CPU overhead on the Driver.
# NFS bandwidth is the bottleneck here, not CPU.
spark.eventLog.compress=true
spark.eventLog.compression.codec=lz4

# Enable rolling logs to limit maximum file size replayed by SHS.
spark.eventLog.rolling.enabled=true
spark.eventLog.rolling.maxFileSize=128m

# ── History Server ────────────────────────────────────────────────────────────
spark.history.fs.logDirectory=file:///mnt/nfs/spark-logs

# CRITICAL: NFS client-side caching (attribute cache) means that a file
# written by the Driver may not be visible to the SHS listStatus call for
# up to actimeo seconds (default: 60s). Set acregmax=5 on the NFS mount
# to reduce this to 5 seconds, OR rely on the SHS inprogress optimization:
# When enabled, SHS reads .inprogress files by seeking to the last known
# offset rather than replaying from byte 0 on every poll. This reduces
# NFS read RPC count by orders of magnitude for long-running applications.
spark.history.fs.inProgressOptimization.enabled=true

# Poll interval should be > NFS acregmax to avoid stale-cache reads.
spark.history.fs.update.interval=15s

# LevelDB for metadata persistence across SHS restarts.
spark.history.store.path=/var/lib/spark/shs-store

# Increase number of replay threads to saturate NFS throughput.
# Each thread opens a separate NFS file descriptor and reads sequentially.
spark.history.fs.numReplayThreads=8

EOF

echo "SHS NFS configuration written. Restart History Server to apply."

# ── Step 3: Verify NFS event log visibility ───────────────────────────────────
# List the 5 most recently modified files to confirm NFS mount freshness.
ls -lt /mnt/nfs/spark-logs/ | head -6
```

> **Mastery Note:** The `spark.history.fs.inProgressOptimization.enabled` flag is one of the most impactful and least-documented SHS settings. Without it, the `FsHistoryProvider` replays an in-progress application's entire event log from byte 0 on every poll cycle. For a 6-hour Spark Structured Streaming job that has already written 2GB of events, this means the SHS reads 2GB from NFS every 10 seconds — 720MB/min of pointless I/O. With the optimization enabled, SHS records the last-replayed byte offset in the KVStore and seeks directly to that position on subsequent polls, reading only the delta. The NFS `actimeo` interaction is subtle: if the NFS client caches the file's `st_size` attribute, the SHS may not see new bytes even after the Driver writes them, making the optimization appear broken. Setting `acregmax=5` on the mount options resolves this by forcing attribute revalidation every 5 seconds.

---

### Example 4: Automating SHS REST API for Application Performance Auditing

> **What this demonstrates:** Using the SHS REST API (served by the embedded Jetty server) to programmatically extract stage-level metrics across hundreds of completed applications — the same data shown in the Spark UI, but accessible without a browser.

```python
# shs_rest_audit.py
# Queries the SHS REST API to identify applications with pathological shuffle ratios.
# A shuffle amplification ratio > 10x (read >> write) indicates join skew or
# a missing broadcast join optimization that Catalyst failed to apply.

import requests
import time
from dataclasses import dataclass, field
from typing import List, Optional

SHS_BASE_URL = "http://history-server.internal:18080" # Default SHS port is 18080

@dataclass
class StageShuffleProfile:
 app_id: str
 app_name: str
 stage_id: int
 num_tasks: int
 shuffle_write_bytes: int
 shuffle_read_bytes: int
 executor_cpu_time_ms: int
 gc_time_ms: int

 @property
 def shuffle_amplification(self) -> float:
 """Ratio of shuffle read to shuffle write. > 10x indicates skew or fanout."""
 if self.shuffle_write_bytes == 0:
 return 0.0
 return self.shuffle_read_bytes / self.shuffle_write_bytes

def get_applications(limit: int = 100, status: str = "completed") -> List[dict]:
 """
 GET /api/v1/applications?status=completed&limit=N
 Returns application summaries. Each entry includes appId, name, and attemptId.
 The SHS REST API is versioned at /api/v1; do not use undocumented internal endpoints.
 """
 resp = requests.get(
 f"{SHS_BASE_URL}/api/v1/applications",
 params={"status": status, "limit": limit},
 timeout=30,
 )
 resp.raise_for_status()
 return resp.json()

def get_stages(app_id: str, attempt_id: str = "1") -> List[dict]:
 """
 GET /api/v1/applications/{appId}/{attemptId}/stages
 Returns all stages with aggregate metrics for the specified application attempt.
 The attemptId is "1" for single-attempt apps; YARN retries increment it.
 """
 resp = requests.get(
 f"{SHS_BASE_URL}/api/v1/applications/{app_id}/{attempt_id}/stages",
 timeout=60, # Large apps with 10k stages may take >10s to serialize
 )
 resp.raise_for_status()
 return resp.json()

def audit_shuffle_amplification(app_limit: int = 50) -> List[StageShuffleProfile]:
 """
 Scan recent completed applications and collect shuffle amplification ratios.
 Stages with amplification > 10x are candidates for broadcast join tuning
 or repartition() before the join to eliminate skew.
 """
 apps = get_applications(limit=app_limit)
 problematic_stages: List[StageShuffleProfile] = []

 for app in apps:
 app_id = app["id"]
 app_name = app.get("name", "unknown")
 # Use the last attempt (highest index) to get the final execution metrics.
 attempts = app.get("attempts", [{"attemptId": "1"}])
 attempt_id = attempts[-1].get("attemptId", "1")

 try:
 stages = get_stages(app_id, attempt_id)
 except requests.HTTPError as e:
 # 404 can occur if the app was evicted from KVStore and LevelDB files
 # were deleted. Log and skip.
 print(f"WARN: Could not fetch stages for {app_id}: {e}")
 continue

 for stage in stages:
 # The REST API returns camelCase keys matching the SparkUI JSON contract.
 shuffle_write = stage.get("shuffleWriteBytes", 0)
 shuffle_read = stage.get("shuffleReadBytes", 0)
 num_tasks = stage.get("numTasks", 0)

 profile = StageShuffleProfile(
 app_id=app_id,
 app_name=app_name,
 stage_id=stage["stageId"],
 num_tasks=num_tasks,
 shuffle_write_bytes=shuffle_write,
 shuffle_read_bytes=shuffle_read,
 # executorCpuTime is in nanoseconds in the event log but
 # the REST API returns it in milliseconds for convenience.
 executor_cpu_time_ms=stage.get("executorCpuTime", 0),
 gc_time_ms=stage.get("jvmGcTime", 0),
 )

 # Flag stages where shuffle read is 10x the shuffle write.
 # This pattern indicates either a massive fanout join or data skew
 # where a single reducer fetches disproportionate data.
 if profile.shuffle_amplification > 10.0 and shuffle_write > 100_000_000:
 problematic_stages.append(profile)

 # Rate-limit API calls to avoid saturating the SHS Jetty thread pool.
 # Default Jetty thread pool size is 200; 100ms sleep keeps throughput
 # to ~10 RPS, well within SHS capacity.
 time.sleep(0.1)

 return sorted(problematic_stages, key=lambda s: s.shuffle_amplification, reverse=True)

if __name__ == "__main__":
 results = audit_shuffle_amplification(app_limit=100)
 print(f"\n{'App ID':<30} {'Stage':>6} {'Tasks':>6} {'WriteGB':>8} {'ReadGB':>8} {'Amp':>6}")
 print("-" * 72)
 for s in results[:20]: # Print top 20 most amplified stages
 print(
 f"{s.app_id[:29]:<30} "
 f"{s.stage_id:>6} "
 f"{s.num_tasks:>6} "
 f"{s.shuffle_write_bytes/1e9:>8.2f} "
 f"{s.shuffle_read_bytes/1e9:>8.2f} "
 f"{s.shuffle_amplification:>6.1f}x"
 )
```

> **Mastery Note:** The SHS REST API at `/api/v1/applications` is the same API consumed by the Apache Ambari and Cloudera Manager monitoring integrations, by Spark's own `spark-submit --status` command, and by third-party tools like Dr. Elephant and Sparklens. A critical operational detail: the API returns data from the KVStore, not by re-reading the event log on each request. This means response latency is O(1) for indexed lookups (a single application's stage list) but can reach O(n) for the application listing endpoint when `n` is very large — the SHS serializes the entire `applications` list into a single JSON response. At 10,000+ applications, this response can be 50MB+ and cause Jetty worker thread starvation. Mitigate this by always using the `limit` and `minDate`/`maxDate` query parameters to paginate, and by setting `spark.history.ui.maxApplications` to cap the listing size at the SHS level.

---

## 🎯 Mastery Checklist

To achieve true mastery of the Spark History Server:

- [ ] Understand why `.inprogress` files exist and the atomic rename semantics that signal application completion to `FsHistoryProvider`
- [ ] Know when LevelDB KVStore outperforms `InMemoryStore` and how to quantify the heap savings using `jmap -histo` on the SHS JVM
- [ ] Be able to diagnose a "no applications shown" SHS symptom as either a credentials failure, an NFS `actimeo` staleness issue, or an S3 `ListBucket` permission error — using only `hadoop fs -ls` and SHS logs at `DEBUG` level
- [ ] Understand the tradeoff between `spark.eventLog.rolling.maxFileSize` (smaller = faster SHS replay, more S3 PUT requests) and `spark.eventLog.compression.codec` (Zstd vs LZ4 for network-dominated vs CPU-dominated environments)
- [ ] Know how `spark.history.fs.inProgressOptimization.enabled` interacts with NFS attribute caching (`actimeo`, `acregmax`) and when to disable it for correctness
- [ ] Be able to use the SHS REST API `/api/v1/applications/{id}/stages` to identify broadcast join candidates from shuffle amplification ratios without opening a browser
- [ ] Know how `AppStatusListener` is shared between the live Driver UI and the SHS replay path, and why this guarantees UI metric parity between live and historical views

---

## 📚 Summary

The Spark History Server is a purpose-built observability system whose correctness and scalability depend on a precise interplay of its three subsystems: the `EventLoggingListener` on the Driver (writing), the `FsHistoryProvider` with its replay thread pool (reading and parsing), and the `KVStore` abstraction (serving). The event log format — newline-delimited, codec-compressed JSON — is simple enough to parse with a 50-line Python script, yet rich enough to reconstruct a complete application timeline including physical SQL plans, task-level CPU and GC metrics, and shuffle I/O breakdown per stage. 

The two most common production failure modes — KVStore heap exhaustion and S3 listing latency — both have well-defined solutions: the LevelDB KVStore backend and rolling event logs, respectively. These are not optional optimizations; they are prerequisites for operating SHS in any environment with more than a few hundred completed applications per day. Failing to configure them produces a system that appears functional under load testing but degrades catastrophically in production, either through `OutOfMemoryError` in the SHS JVM or through multi-minute listing delays that cause the SHS UI to show stale or empty application lists. 

The SHS REST API elevates the History Server from a human-facing UI to a machine-queryable metrics store. Integrating it into CI/CD pipelines enables regression detection — an automated job can compare the shuffle bytes written by the current build against the 7-day median and flag a 2x regression before it reaches production. This is the mental model that separates reactive Spark debugging from proactive Spark performance engineering: the History Server is not a post-mortem tool, it is an always-on performance database.

---

<div style="font-size: 0.82rem; color: #64748b; border-top: 1px solid #1e3a5f; padding-top: 12px; margin-top: 24px; line-height: 1.8;">
<strong style="color: #94a3b8;">📚 Book References (Spark in Action, 2nd Ed.):</strong>&nbsp;
<a href="spark_book.pdf#page=1" style="color: #60a5fa; text-decoration: none; margin-right: 10px;" title="Introduction">p.1</a> <a href="spark_book.pdf#page=5" style="color: #60a5fa; text-decoration: none; margin-right: 10px;" title="Core Concepts">p.5</a> <a href="spark_book.pdf#page=10" style="color: #60a5fa; text-decoration: none; margin-right: 10px;" title="Implementation">p.10</a>
</div>
