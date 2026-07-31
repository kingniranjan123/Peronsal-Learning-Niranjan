# 🔥 Master Class: Amazon EC2 Deployment for Apache Spark

## Overview

Running Apache Spark on Amazon EC2 is not simply a matter of provisioning virtual machines and copying a JAR file. Every architectural decision — instance family selection, storage topology, cluster management strategy, and network configuration — has a direct, measurable impact on job throughput, fault tolerance, and monthly cloud spend. EC2 gives you the full spectrum from fully managed EMR clusters to raw self-managed deployments, and understanding what happens beneath each abstraction is what separates engineers who tolerate Spark from engineers who master it.

Spark's execution model maps directly onto EC2's resource hierarchy. The Driver JVM runs on the master node, coordinating the DAGScheduler and TaskScheduler. Executor JVMs run on worker nodes, each consuming a configurable slice of instance vCPUs and memory. EC2's instance types determine the physical ceiling for JVM heap, off-heap (Tungsten) memory, local NVMe throughput for shuffle spill, and network bandwidth for shuffle data movement. Choosing an `r6g.4xlarge` (64 GiB, Graviton3) versus an `m5d.4xlarge` (64 GiB, Intel Xeon) is not equivalent — core clock speeds, memory bandwidth, NVMe latency, and per-hour pricing all differ, and each dimension hits a different Spark subsystem.

The economic dimension is equally critical. EC2 On-Demand pricing is the floor; Spot Instances can reduce compute costs by 60–90%, but they introduce interruption risk that must be handled at the cluster level. AWS EMR's Instance Fleet mode was specifically designed to absorb Spot interruptions by maintaining a pool of diversified instance types and automatic replacement. Understanding how EMR's YARN Resource Manager interacts with Spot interruption notices — via the 2-minute EC2 termination signal — and how Spark's speculative execution and task retry logic responds to that signal is foundational knowledge for any production deployment.

---

## 🏗️ Architectural Deep Dive

### How It Works Under the Hood

When EMR provisions a cluster, it bootstraps each EC2 instance with a specific set of daemons: the YARN ResourceManager and Spark History Server on the master, and YARN NodeManagers on every core and task node. EMR exposes each NodeManager's total capacity (vCPUs and memory) to Spark's YARN client. Spark's `YarnAllocator` inside the Driver negotiates with the ResourceManager to request executor containers. These containers are bounded by `spark.executor.memory` + `spark.executor.memoryOverhead`; if you set overhead below 10% of executor memory, YARN's container memory check will hard-kill executors with a `Container killed by YARN for exceeding memory limits` error, one of the most common production failures.

Tungsten's off-heap memory manager operates outside the JVM heap within the executor container's total memory envelope. The `spark.memory.offHeap.enabled=true` + `spark.memory.offHeap.size` settings tell Tungsten to allocate a native memory region via `sun.misc.Unsafe`, bypassing the JVM's garbage collector entirely. This is critical on memory-intensive workloads (wide aggregations, sort-merge joins) where GC pauses on large heaps can stall executor heartbeats, causing the Driver's `HeartbeatReceiver` to mark executors as lost after `spark.network.timeout` (default 120s) expires. On Graviton3 instances, ARM's memory subsystem delivers ~40% higher memory bandwidth than equivalent x86 Xeon instances, which directly accelerates Tungsten's sequential binary format operations.

Shuffle data is the central bottleneck in EC2 deployments. Spark's `SortShuffleManager` writes shuffle files to the local disk of each executor's EC2 instance. On instance types with NVMe storage (e.g., `m5d`, `c5d`, `r6id`), shuffle write throughput can reach 3+ GB/s, while EBS `gp3` volumes are capped at 1,000 MB/s (16,000 IOPS). The `ExternalShuffleService`, enabled by default in EMR, decouples shuffle file serving from executor JVMs, allowing YARN to reclaim executor containers while shuffle data remains readable — critical for dynamic allocation on Spot clusters. S3A's `fs.s3a.fast.upload` multipart upload pipeline (controlled by `fs.s3a.multipart.size` and `fs.s3a.fast.upload.buffer`) determines the throughput ceiling for writing Parquet/ORC output to S3, and misconfiguration is the single largest source of slow write performance in production.

```
EMR Master Node (m5.xlarge)                  EMR Core / Task Nodes
┌───────────────────────────────┐             ┌────────────────────────────────────┐
│  YARN ResourceManager         │◀────────────│  YARN NodeManager                  │
│  Spark Driver (YarnClient)    │  Container  │  ┌──────────────────────────────┐  │
│  ┌───────────────────────┐    │  Heartbeat  │  │ Executor JVM                 │  │
│  │ DAGScheduler          │    │────────────▶│  │  Heap: spark.executor.memory │  │
│  │ TaskScheduler         │    │             │  │  Off-Heap: Tungsten UnsafeRow│  │
│  │ YarnAllocator         │    │             │  │  Shuffle: NVMe / EBS gp3     │  │
│  │ HeartbeatReceiver     │    │             │  └──────────────────────────────┘  │
│  └───────────────────────┘    │             │  ExternalShuffleService (port 7337)│
│  SparkHistoryServer           │             └────────────────────────────────────┘
│  S3A Committer (staging dir)  │                          │
└───────────────────────────────┘                          │ S3A multipart upload
                                                           ▼
                                              ┌─────────────────────┐
                                              │  Amazon S3 Bucket   │
                                              │  (output Parquet /  │
                                              │   ORC / Delta Lake) │
                                              └─────────────────────┘
             EC2 Spot Interruption Notice (2-min warning)
             ┌──────────────────────────────────────────────────────┐
             │  Instance Fleet: replace interrupted instance with   │
             │  next available type from diversified pool           │
             │  EMR → resubmit YARN container → Spark task retry   │
             └──────────────────────────────────────────────────────┘
```

### Key Internal Components

- **YarnAllocator (Driver JVM):** Negotiates executor container placement with the YARN ResourceManager. Tracks blacklisted nodes (via `spark.blacklist.enabled`) and avoids scheduling tasks on hosts that have returned repeated task failures — essential for handling degraded Spot instances before the 2-minute termination signal arrives.
- **ExternalShuffleService (NodeManager plugin):** A long-lived daemon on each worker node that serves shuffle blocks independently of executor JVM lifetime. Enabled by `spark.shuffle.service.enabled=true` and required for dynamic executor allocation on YARN; without it, executor deallocation destroys shuffle files and forces full-stage recomputation.
- **S3AFileSystem + Magic Committer:** The `hadoop-aws` S3A connector implements a `FileOutputCommitter` replacement called the Magic Committer (`fs.s3a.committer.name=magic`), which writes task output directly to the final S3 path using multipart upload, eliminating the two-phase rename-on-commit pattern that caused O(n) LIST + COPY + DELETE operations and severe job commit latency at scale.
- **EMR Instance Fleet:** An EMR cluster provisioning strategy that specifies target capacity in vCPU-hours and allows multiple instance types (e.g., `r6g.2xlarge`, `r6gd.2xlarge`, `r6i.2xlarge`) to satisfy that capacity. The Fleet controller bids on Spot pools with the lowest interruption probability across multiple Availability Zones, providing the interruption rate and price stability unavailable from a single instance type.

---

## ⚠️ Critical Concepts & Common Pitfalls

### Spot Interruption and Task Retry Asymmetry

Spark's default task retry count (`spark.task.maxFailures=4`) is designed for transient executor failures, not for the coordinated loss of multiple executors simultaneously. When a Spot interruption hits an entire Spot fleet of the same instance type (a correlated interruption event during an AZ capacity crunch), tens of executors can vanish within seconds. If all executors holding shuffle map output for a stage disappear, Spark must recompute the entire upstream stage — not just retry the downstream tasks. This is a fetch failure cascade: the DAGScheduler receives `FetchFailed` exceptions, marks the shuffle map stage as failed, and resubmits it. With `spark.stage.maxConsecutiveAttempts=4`, a large cluster hit by three consecutive interruption waves will abort the job with `Job aborted due to stage failure`.

The correct mitigation is multi-layered. First, use Instance Fleet with at least four distinct instance families across two Availability Zones, reducing the probability of correlated interruption below 1%. Second, enable `spark.shuffle.service.enabled=true` so that shuffle files survive individual executor terminations. Third, configure `spark.task.maxFailures=8` for Spot-heavy clusters. Fourth, for shuffle-heavy pipelines exceeding 500 GB of shuffle write, consider enabling Delta Lake's `OPTIMIZE` or persisting intermediate DataFrames to S3 as Parquet checkpoints to truncate lineage depth and eliminate multi-stage recomputation risk.

### EBS-Optimized Storage vs. Instance NVMe: The Shuffle Spill Decision

EBS `gp3` volumes deliver deterministic IOPS (up to 16,000) and throughput (up to 1,000 MB/s) via a dedicated EBS-optimized network path, but this path competes with S3A network traffic on instances without sufficient network bandwidth. On a `m5.4xlarge` (10 Gbps), saturating EBS at 1 GB/s leaves only 2.5 Gbps for S3A reads and writes. Instances with NVMe (instance store) bypass the network entirely: `r6id.4xlarge` ships with 950 GB NVMe delivering 2.5 GB/s sequential write with sub-100µs latency. For workloads with shuffle spill exceeding 10 GB per executor, instance store NVMe reduces shuffle sort time by 60–70% compared to `gp3`.

The failure mode is subtle: EBS volumes can throttle silently. When an executor's shuffle write exceeds the provisioned IOPS, EBS queues I/O and the executor's task thread blocks. The Spark UI shows tasks in the "RUNNING" state with zero input/output rates — misleadingly suggesting computation rather than I/O stall. Identifying this requires correlating CloudWatch's `VolumeQueueLength` metric (target: below 1) with Spark UI task duration. If `VolumeQueueLength` spikes above 10 during shuffle write phases, the volume is saturated and you should migrate the shuffle directory to instance NVMe or increase `gp3` provisioned throughput.

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|------------|---------|-------|
| S3A Parquet Read (predicate pushdown) | O(selected row groups) | No | Catalyst pushes filters to Parquet column statistics; skips unneeded row groups at the reader level |
| Wide Aggregation (groupBy + agg) | O(n log n) per partition | Yes | SortShuffleManager writes sorted shuffle files; Tungsten UnsafeRow binary format reduces serialization cost vs Java |
| Sort-Merge Join (large-large) | O(n log n) per side | Yes | Both sides sorted and merged; shuffle write is 2× total input size; network is the bottleneck above 1 TB |
| Broadcast Join (small-large) | O(n) broadcast + O(n) probe | No | Driver collects small table, serializes via Kryo, pushes to all executors via BlockManager; fails above 8–10 GB |
| Spark SQL Write to S3 (Magic Committer) | O(tasks) | No | Eliminates O(n) LIST+RENAME; task commit is a single S3 CompleteMultipartUpload call; 10–100× faster job commit |
| Dynamic Partition Insert (S3 output) | O(partitions × files) | Yes | Each partition key triggers a separate output stream; high-cardinality partitioning causes S3 throttling (3,500 PUT/s limit per prefix) |

---

## 💻 Code Examples

### Example 1: EMR Instance Fleet Configuration with Spot Diversification (Boto3)

> **What this demonstrates:** How to construct an EMR Instance Fleet that spans multiple instance types and Availability Zones to achieve Spot interruption resilience without sacrificing cost savings, and how this maps to Spark's executor container allocation model.

```python
import boto3

emr = boto3.client("emr", region_name="us-east-1")

response = emr.run_job_flow(
    Name="spark-production-fleet",
    ReleaseLabel="emr-7.1.0",  # Includes Spark 3.5, Hadoop 3.3.6, hadoop-aws 3.3.6
    # InstanceFleets replaces InstanceGroups — cannot mix both
    InstanceFleets=[
        {
            "Name": "MasterFleet",
            "InstanceFleetType": "MASTER",
            # Master is always On-Demand to avoid Driver loss
            "TargetOnDemandCapacity": 1,
            "TargetSpotCapacity": 0,
            "InstanceTypeConfigs": [
                {
                    "InstanceType": "m6g.xlarge",  # Graviton3: lower cost, higher mem bandwidth
                    "WeightedCapacity": 1,
                },
                {
                    "InstanceType": "m5.xlarge",   # x86 fallback if Graviton capacity unavailable
                    "WeightedCapacity": 1,
                }
            ],
        },
        {
            "Name": "CoreFleet",
            "InstanceFleetType": "CORE",
            # Core nodes hold HDFS blocks (if used) — keep some On-Demand for stability
            "TargetOnDemandCapacity": 4,   # 4 vCPU-units On-Demand (fault anchor)
            "TargetSpotCapacity": 20,      # 20 vCPU-units from Spot (80% cost savings)
            # WeightedCapacity lets Fleet count capacity in vCPU-hours, not instance count
            "InstanceTypeConfigs": [
                {
                    "InstanceType": "r6g.2xlarge",    # 8 vCPU, 64 GiB — Graviton3, memory-opt
                    "WeightedCapacity": 8,
                    "BidPriceAsPercentageOfOnDemandPrice": 80,  # Max bid = 80% of OD price
                },
                {
                    "InstanceType": "r6gd.2xlarge",   # Same vCPU/RAM + 474 GB NVMe for shuffle
                    "WeightedCapacity": 8,
                    "BidPriceAsPercentageOfOnDemandPrice": 80,
                },
                {
                    "InstanceType": "r6i.2xlarge",    # Intel fallback: same RAM, x86
                    "WeightedCapacity": 8,
                    "BidPriceAsPercentageOfOnDemandPrice": 80,
                },
                {
                    "InstanceType": "r5.2xlarge",     # Older gen x86 — broader Spot pool
                    "WeightedCapacity": 8,
                    "BidPriceAsPercentageOfOnDemandPrice": 80,
                },
            ],
            # AllocationStrategy=CAPACITY_OPTIMIZED picks the Spot pool with lowest interruption
            # probability rather than lowest price — critical for long-running Spark jobs
            "LaunchSpecifications": {
                "SpotSpecification": {
                    "TimeoutDurationMinutes": 10,      # Abort if no Spot capacity in 10 min
                    "TimeoutAction": "SWITCH_TO_ON_DEMAND",  # Fallback instead of failing cluster
                    "AllocationStrategy": "CAPACITY_OPTIMIZED",
                }
            },
        },
    ],
    Configurations=[
        {
            "Classification": "spark-defaults",
            "Properties": {
                # Allow YARN to reclaim idle executor containers (requires ExternalShuffleService)
                "spark.dynamicAllocation.enabled": "true",
                "spark.dynamicAllocation.minExecutors": "2",
                "spark.dynamicAllocation.maxExecutors": "200",
                # ExternalShuffleService: shuffle files survive executor deallocation
                "spark.shuffle.service.enabled": "true",
                # Increase task failure tolerance for Spot interruptions
                "spark.task.maxFailures": "8",
                # Tungsten off-heap: bypass GC for UnsafeRow binary operations
                "spark.memory.offHeap.enabled": "true",
                "spark.memory.offHeap.size": "4g",
            },
        },
        {
            "Classification": "hadoop-env",
            "Configurations": [
                {
                    "Classification": "export",
                    "Properties": {
                        # Graviton JVM flag: enable ARM-optimized AES intrinsics
                        "HADOOP_OPTS": "-XX:+UseAES -XX:+UseAESIntrinsics",
                    },
                }
            ],
        },
    ],
    Applications=[{"Name": "Spark"}, {"Name": "Hadoop"}],
    JobFlowRole="EMR_EC2_DefaultRole",
    ServiceRole="EMR_DefaultRole",
    LogUri="s3://my-emr-logs/clusters/",
    # Use subnet spanning multiple AZs for Spot pool diversification
    Ec2SubnetIds=[
        "subnet-aaa111",  # us-east-1a
        "subnet-bbb222",  # us-east-1b
        "subnet-ccc333",  # us-east-1c
    ],
)

print(f"Cluster ID: {response['JobFlowId']}")
```

> **Mastery Note:** The `CAPACITY_OPTIMIZED` allocation strategy instructs EC2's Spot API to select the pool with the deepest available capacity rather than the lowest spot price. For long-running Spark jobs (>30 minutes), this reduces interruption probability by 3–5× compared to `LOWEST_PRICE` strategy, at the cost of a 5–15% price premium — a worthwhile tradeoff since a single Spot interruption on a 2-hour job triggers full-stage recomputation, costing more in wall-clock time than the price savings. The `WeightedCapacity` model means Fleet honors vCPU-hour targets regardless of which instance type it picks, so Spark's executor count stays stable across the heterogeneous pool. The `SWITCH_TO_ON_DEMAND` fallback ensures the cluster bootstraps even during AZ capacity crunches.

---

### Example 2: S3A Connector Tuning for High-Throughput Parquet I/O

> **What this demonstrates:** The exact `SparkSession` configuration parameters that govern S3A read/write throughput, the Magic Committer pipeline, and how misconfigurations manifest as silent performance cliffs.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

spark = (
    SparkSession.builder
    .appName("S3A-Tuned-Parquet-ETL")
    # ── S3A Connection Pool ──────────────────────────────────────────────────
    # Default connection pool (50) is too small for parallel multipart uploads
    # across 200 executors; increase to prevent "Timeout waiting for connection"
    .config("spark.hadoop.fs.s3a.connection.maximum", "500")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
    # ── S3A Read Optimization ────────────────────────────────────────────────
    # readahead: bytes prefetched from S3 per GET request.
    # Parquet row group size is typically 128MB; align readahead to row group size
    # to fetch exactly one row group per S3 GET, minimizing wasted bandwidth.
    .config("spark.hadoop.fs.s3a.readahead.range", "134217728")   # 128 MiB
    # async drain: when a Parquet reader discards tail bytes of a GET response,
    # async drain keeps the TCP connection alive for reuse instead of closing it.
    .config("spark.hadoop.fs.s3a.input.async.drain.threshold", "134217728")
    # ── S3A Write / Multipart Upload Optimization ────────────────────────────
    # fast.upload=true: executor streams data to S3 in parallel multipart parts
    # without buffering the entire file to disk first (critical for large output)
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    # disk buffer: parts staged to local disk before upload — more reliable than
    # array (in-memory) buffer under memory pressure on executors
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "disk")
    # Each multipart part = 64 MiB; S3 allows up to 10,000 parts per object,
    # so max object size = 640 GiB per file (sufficient for Parquet part files)
    .config("spark.hadoop.fs.s3a.multipart.size", "67108864")     # 64 MiB
    # parallel uploads per stream: 5 concurrent PUT operations per executor file
    .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "5")
    # ── Magic Committer (CRITICAL for correctness + performance) ─────────────
    # Without Magic Committer, Spark uses FileOutputCommitter v1/v2 which does:
    #   Phase 1: write to _temporary/taskAttemptId/ path
    #   Phase 2: LIST + COPY + DELETE to final path (O(n) S3 API calls)
    # With Magic Committer, tasks write directly to final path via multipart;
    # job commit = CompleteMultipartUpload calls only — O(tasks) not O(files)
    .config("spark.hadoop.fs.s3a.committer.name", "magic")
    .config("spark.sql.sources.commitProtocolClass",
            "org.apache.spark.internal.io.cloud.PathOutputCommitProtocol")
    .config("spark.sql.parquet.output.committer.class",
            "org.apache.spark.internal.io.cloud.BindingParquetOutputCommitter")
    # ── Parquet Vectorized Reader ─────────────────────────────────────────────
    # Tungsten's vectorized Parquet reader decodes columnar data in batches of
    # 4096 rows (one ColumnarBatch) using SIMD-optimized arrow-style layout,
    # bypassing row-by-row deserialization into Java objects entirely.
    .config("spark.sql.parquet.enableVectorizedReader", "true")
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "4096")
    .getOrCreate()
)

# ── Read with aggressive predicate pushdown ──────────────────────────────────
# Catalyst's Analysis phase resolves column references; Logical Optimization
# phase applies PushDownPredicate rule to move the filter below the scan.
# The Parquet reader evaluates column statistics (min/max per row group) and
# page-level dictionary filters BEFORE decoding, skipping entire row groups.
df = (
    spark.read
    .option("mergeSchema", "false")     # Disable schema merge: avoids full S3 LIST scan
    .option("basePath", "s3a://my-data-lake/events/")
    .parquet("s3a://my-data-lake/events/year=2024/month=*/")
    .filter(col("event_type") == "purchase")   # Pushed to Parquet row group filter
    .filter(col("amount") > 100.0)             # Pushed to Parquet page dictionary filter
    .select("user_id", "event_type", "amount", "ts")  # Column pruning: only 4 of 40 columns read
)

# ── Write with partition layout that avoids S3 throttling ────────────────────
# S3 throttles at 3,500 PUT/s per prefix. High-cardinality partitioning
# (e.g., by user_id with 10M users) creates millions of S3 prefixes,
# each receiving one PUT — guaranteed throttling. Partition by low-cardinality
# date fields instead; use coalesce to control files per partition.
(
    df
    .withColumn("year", year("ts"))
    .withColumn("month", month("ts"))
    .coalesce(8)                         # 8 files per partition: ~128 MiB each at 1 GiB/partition
    .write
    .mode("overwrite")
    .partitionBy("year", "month")        # Low-cardinality: 12×N_YEARS prefixes only
    .parquet("s3a://my-output/purchases/")
)
```

> **Mastery Note:** The Magic Committer is the single highest-impact configuration change for S3-backed Spark deployments. Without it, a job writing 10,000 Parquet files performs ~30,000 S3 API calls during the commit phase (LIST + COPY + DELETE per file), which at S3's default 5,500 GET/s and 3,500 PUT/s rate limits can take 5–15 minutes for the commit alone — after all computation is complete. The Magic Committer reduces this to exactly `N_tasks` `CompleteMultipartUpload` calls. The `readahead.range` aligned to Parquet row group size (128 MiB) is equally important: misalignment causes S3 to serve partial GETs, wasting bandwidth on bytes that are immediately discarded by the Parquet reader before the next row group boundary.

---

### Example 3: Graviton3 vs x86 Spark Configuration Tuning

> **What this demonstrates:** The JVM and Spark configuration adjustments required to fully exploit Graviton3 (ARM64) instances on EMR, including GC tuning differences, SIMD-aware shuffle settings, and Kryo serializer registration for reduced network overhead.

```python
from pyspark.sql import SparkSession

# Graviton3 (r6g/m6g/c6g) characteristics vs x86 (m5/r5/c5):
# - 25% higher memory bandwidth (DDR5 on r6g vs DDR4 on r5)
# - Lower single-thread clock (2.6 GHz vs 3.1 GHz) — benefits parallel workloads
# - No AVX-512 (x86-only), but AWS Graviton3 has custom SVE SIMD for vectorized ops
# - ~20% cheaper per vCPU-hour at On-Demand, ~40% cheaper at Spot

spark = (
    SparkSession.builder
    .appName("Graviton3-Optimized-Spark")
    # ── JVM GC: G1GC tuned for large executor heaps on Graviton ──────────────
    # G1GC is preferred over ZGC on EMR 7.x for workloads with high allocation
    # rates (shuffle-heavy ETL). G1's concurrent marking keeps pause < 200ms
    # even on 48 GiB heaps — critical to avoid heartbeat timeouts.
    .config("spark.executor.extraJavaOptions",
            "-XX:+UseG1GC "
            "-XX:G1HeapRegionSize=32m "       # Large regions reduce GC overhead for big objects
            "-XX:+G1UseAdaptiveIHOP "         # Adaptive IHOP avoids premature Full GC
            "-XX:InitiatingHeapOccupancyPercent=35 "  # Start concurrent mark at 35% heap
            "-XX:+UseStringDeduplication "    # Dedup identical String objects (join keys)
            "-XX:+UseAES -XX:+UseAESIntrinsics "  # ARM AES hardware acceleration
            "-Djdk.nio.maxCachedBufferSize=262144")   # Cap NIO buffer cache per thread
    # ── Executor memory layout for r6g.4xlarge (64 GiB, 16 vCPU) ─────────────
    # Total container memory = executor.memory + memoryOverhead
    # Leave 8 GiB for OS, NodeManager, ExternalShuffleService
    # Container = 48 GiB heap + 6 GiB overhead = 54 GiB YARN container
    .config("spark.executor.memory", "48g")
    .config("spark.executor.memoryOverhead", "6144")   # 6 GiB: covers off-heap + Netty buffers
    .config("spark.executor.cores", "4")               # 4 cores: 4 tasks × 12 GiB unified memory each
    .config("spark.executor.instances", "32")          # 4 executors per r6g.4xlarge × 8 nodes
    # ── Tungsten off-heap for UnsafeRow operations ────────────────────────────
    # Off-heap memory is outside the JVM heap counted in memoryOverhead budget.
    # Must be accounted for in YARN container total memory:
    # YARN container = executor.memory + memoryOverhead + offHeap.size (if managed externally)
    .config("spark.memory.offHeap.enabled", "true")
    .config("spark.memory.offHeap.size", "2g")
    # ── Kryo serializer: faster than Java, critical for shuffle network I/O ──
    # Kryo serializes UnsafeRow spill buffers and broadcast variables.
    # Registration eliminates per-class header bytes (saves 10-15% shuffle I/O).
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryo.registrationRequired", "false")  # Fallback for unregistered classes
    .config("spark.kryo.unsafe", "true")   # Use sun.misc.Unsafe for zero-copy Kryo buffers
    # ── Shuffle tuning for Graviton's higher memory bandwidth ─────────────────
    # Larger sort buffer exploits Graviton's 25% higher memory bandwidth:
    # SortShuffleManager merges more records before spilling to disk,
    # reducing the number of spill files and subsequent merge I/O passes.
    .config("spark.shuffle.sort.bypassMergeThreshold", "200")  # Bypass sort for < 200 partitions
    .config("spark.shuffle.file.buffer", "1m")     # 1 MiB write buffer per shuffle output file
    .config("spark.reducer.maxSizeInFlight", "96m")  # Fetch 96 MiB of shuffle data per reducer
    # ── Adaptive Query Execution (AQE): critical on heterogeneous Spot fleets ─
    # AQE re-optimizes the physical plan at runtime after each shuffle stage.
    # On Spot fleets with mixed instance types, partition sizes vary unpredictably;
    # AQE's coalesce rule merges small post-shuffle partitions automatically.
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "134217728")  # 128 MiB target
    .config("spark.sql.adaptive.skewJoin.enabled", "true")  # Split skewed partitions at runtime
    .getOrCreate()
)

# Verify Graviton architecture at runtime
import subprocess
arch = subprocess.check_output(["uname", "-m"]).decode().strip()
print(f"Executor architecture: {arch}")  # Expected: aarch64 on Graviton

# Simple benchmark: wide aggregation to stress memory bandwidth
from pyspark.sql.functions import sum as _sum, count, avg

result = (
    spark.range(0, 500_000_000, numPartitions=800)  # 500M rows, 800 partitions
    .selectExpr(
        "id % 10000 as group_key",        # 10k groups: medium cardinality aggregation
        "cast(rand() * 1000 as double) as value"
    )
    .groupBy("group_key")
    .agg(
        _sum("value").alias("total"),
        count("*").alias("cnt"),
        avg("value").alias("mean_value"),
    )
    .count()
)
print(f"Groups aggregated: {result}")
```

> **Mastery Note:** Graviton3's DDR5 memory subsystem provides ~60 GB/s aggregate memory bandwidth per socket versus ~45 GB/s on equivalent m5 instances — a 33% advantage that directly benefits Tungsten's UnsafeRow hash aggregation, which is fundamentally a memory bandwidth-bound operation. However, Graviton's lower single-core clock speed means workloads with long sequential critical paths (e.g., a single-partition `orderBy` followed by a Python UDF on the Driver) will be slower than on x86. The JVM flag `-XX:+UseAES` enables ARM's hardware AES-NI instructions, which accelerates Spark's encrypted shuffle (`spark.authenticate=true`) by 3–5× versus software AES — important for compliance workloads. Kryo with `unsafe=true` eliminates intermediate byte array allocation during shuffle write by writing directly from off-heap memory, reducing GC allocation pressure by 15–25% on shuffle-intensive stages.

---

### Example 4: Cost-Optimized Spot Job with Checkpointing and Automatic Restart

> **What this demonstrates:** A production-grade pattern for running long-running Spark ETL on 100% Spot instances with S3-backed checkpointing, lineage truncation, and automated EMR Step retry — enabling 70–85% cost reduction with fault tolerance at the application layer.

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, to_date, current_timestamp
import boto3
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CHECKPOINT_BASE = "s3a://my-pipeline-checkpoints/daily-etl/"
OUTPUT_PATH = "s3a://my-data-lake/processed/daily-etl/"

spark = (
    SparkSession.builder
    .appName("CostOptimized-Spot-ETL")
    # Checkpoint directory: Spark truncates RDD lineage here, preventing
    # recomputation cascades that span dozens of shuffles on Spot interruption.
    # SparkContext.setCheckpointDir triggers a reliable write to S3 via S3A.
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.task.maxFailures", "8")          # Tolerate 8 failures per task (Spot retries)
    .config("spark.stage.maxConsecutiveAttempts", "8")
    # Speculation: relaunch slow tasks on other nodes before they become blockers.
    # On mixed Spot fleets with heterogeneous instance types, task duration variance
    # is high; speculation reduces the impact of stragglers on stage completion time.
    .config("spark.speculation", "true")
    .config("spark.speculation.multiplier", "2.0")   # Launch speculative if 2× median duration
    .config("spark.speculation.quantile", "0.9")     # After 90% of tasks complete
    .getOrCreate()
)

# Set checkpoint directory: must be a reliable, durable store (not local disk)
# S3 via S3A satisfies this — checkpointed RDD data survives executor loss.
spark.sparkContext.setCheckpointDir(f"{CHECKPOINT_BASE}rdd-checkpoints/")


def checkpoint_exists(path: str) -> bool:
    """Check if a stage output checkpoint exists on S3 to enable idempotent restarts."""
    s3 = boto3.client("s3")
    bucket, key = path.replace("s3a://", "").split("/", 1)
    try:
        s3.head_object(Bucket=bucket, Key=f"{key}/_SUCCESS")
        return True
    except s3.exceptions.ClientError:
        return False


def read_or_checkpoint(
    stage_name: str,
    compute_fn,
    checkpoint_path: str,
    schema=None
) -> DataFrame:
    """
    Idempotent stage execution pattern:
    1. If checkpoint exists on S3: read it (skip recomputation entirely)
    2. If not: compute, write to checkpoint path, return result
    This pattern limits blast radius of Spot interruption to the current stage only.
    On restart after interruption, completed stages reload from S3 in seconds.
    """
    stage_checkpoint = f"{checkpoint_path}{stage_name}/"
    
    if checkpoint_exists(stage_checkpoint):
        logger.info(f"Stage '{stage_name}': loading from checkpoint at {stage_checkpoint}")
        return spark.read.parquet(stage_checkpoint)
    
    logger.info(f"Stage '{stage_name}': computing and checkpointing to {stage_checkpoint}")
    result_df = compute_fn()
    
    # Write checkpoint: this is a blocking action (triggers computation).
    # After this write, Spark lineage for this DataFrame is "cut" — recomputation
    # on restart reads Parquet, not the entire upstream DAG.
    (
        result_df
        .write
        .mode("overwrite")
        .parquet(stage_checkpoint)
    )
    return spark.read.parquet(stage_checkpoint)


# ── Stage 1: Raw ingestion and type casting ───────────────────────────────────
# Checkpoint after stage 1 so a Spot interruption during stage 2 or 3
# does not force re-reading and re-casting 10 TB of raw S3 data.
stage1_df = read_or_checkpoint(
    stage_name="stage1-raw-cast",
    compute_fn=lambda: (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")          # Never inferSchema on large datasets: O(full scan)
        .schema("user_id STRING, amount DOUBLE, ts STRING, product_id STRING")
        .csv("s3a://raw-data/transactions/2024/")
        .filter(col("amount").isNotNull())
        .filter(col("amount") > 0)
        .withColumn("date", to_date(col("ts"), "yyyy-MM-dd'T'HH:mm:ss"))
        .drop("ts")
    ),
    checkpoint_path=CHECKPOINT_BASE,
)
logger.info(f"Stage 1 complete. Estimated rows: {stage1_df.count():,}")


# ── Stage 2: Heavy aggregation (shuffle-intensive) ────────────────────────────
# This stage produces large shuffle write; checkpoint its output so Stage 3
# does not need to re-execute the full groupBy if interrupted mid-shuffle.
from pyspark.sql.functions import sum as _sum, count, max as _max

stage2_df = read_or_checkpoint(
    stage_name="stage2-daily-agg",
    compute_fn=lambda: (
        stage1_df
        .groupBy("user_id", "date")
        .agg(
            _sum("amount").alias("daily_spend"),
            count("*").alias("txn_count"),
            _max("amount").alias("max_txn"),
        )
    ),
    checkpoint_path=CHECKPOINT_BASE,
)


# ── Stage 3: Final enrichment and output ──────────────────────────────────────
(
    stage2_df
    .filter(col("daily_spend") > 50)          # Filter after aggregation (AQE will coalesce partitions)
    .withColumn("processed_at", current_timestamp())
    .write
    .mode("overwrite")
    .partitionBy("date")
    .parquet(OUTPUT_PATH)
)

logger.info(f"Pipeline complete. Output written to {OUTPUT_PATH}")
spark.stop()
```

> **Mastery Note:** The `read_or_checkpoint` pattern transforms a monolithic Spark job into a series of independently restartable stages. Without it, a Spot interruption at hour 3 of a 4-hour pipeline restarts from scratch — a 75% work loss. With S3-backed checkpoints, the restarted job resumes from the last completed stage, reducing repeat work to at most one stage worth of computation. The `_SUCCESS` sentinel file check is critical: it uses S3's strongly consistent metadata (GA since Dec 2020) to verify that the checkpoint write completed atomically, preventing partial checkpoint reads after interrupted writes. Speculative execution (`spark.speculation=true`) with a `2.0` multiplier is essential on heterogeneous Spot fleets because `r6g.2xlarge` and `r5.2xlarge` instances running the same task will complete in different wall-clock times, creating artificial stragglers that inflate stage duration by 20–40% without speculation.

---

## 🎯 Mastery Checklist

To achieve true mastery of Amazon EC2 Deployment for Apache Spark:

- [ ] Understand how `YarnAllocator` negotiates executor containers and how `spark.executor.memoryOverhead` relates to YARN's container memory enforcement — and the exact error message when it is breached
- [ ] Know when Instance Fleet with `CAPACITY_OPTIMIZED` outperforms `LOWEST_PRICE` strategy and why correlated Spot interruptions cascade into full-stage recomputation via `FetchFailed`
- [ ] Be able to diagnose EBS I/O throttling from CloudWatch `VolumeQueueLength` and correlate it with Spark UI task durations showing zero I/O rate on "RUNNING" tasks
- [ ] Understand the Magic Committer's `CompleteMultipartUpload` mechanism vs FileOutputCommitter v2's LIST+COPY+DELETE and when each is appropriate (Magic Committer requires S3 path consistency)
- [ ] Know how Graviton3 memory bandwidth advantages manifest in Tungsten hash aggregation throughput and why they do not help single-threaded Driver-side operations
- [ ] Understand how `ExternalShuffleService` decouples shuffle file lifetime from executor container lifetime and why disabling it makes dynamic allocation unsafe on Spot clusters
- [ ] Be able to tune `fs.s3a.readahead.range` to align with Parquet row group size and explain the bandwidth waste from misalignment
- [ ] Know the tradeoff between Spot cost savings (60–90%) and the checkpoint overhead (5–10% extra S3 write I/O) and when pipeline checkpointing is worth the cost

---

## 📚 Summary

Apache Spark on Amazon EC2 is a multi-dimensional optimization problem where the execution engine, the cloud infrastructure, and the storage layer must be co-tuned. The Tungsten execution engine's off-heap binary format, Catalyst's predicate pushdown to Parquet row groups, and the S3A connector's multipart upload pipeline all operate independently but interact through shared resources: network bandwidth, JVM heap, and YARN container memory. A misconfiguration in any single layer — an undersized `memoryOverhead`, a misaligned `readahead.range`, or a missing Magic Committer — creates a performance cliff that is invisible in unit tests and only manifests under production load at scale. [Ref: 451](spark_book.pdf#page=451) [Ref: 455](spark_book.pdf#page=455) [Ref: 458](spark_book.pdf#page=458) [Ref: 462](spark_book.pdf#page=462) [Ref: 469](spark_book.pdf#page=469)

The Spot Instances and Instance Fleet pairing is the highest-leverage cost optimization available in EC2 deployments, but it requires application-level fault tolerance that goes beyond Spark's default task retry. The `ExternalShuffleService`, `FetchFailed`-aware stage retry limits, S3-backed stage checkpoints, and speculative execution must all be configured as a coherent system. Each component addresses a different failure mode: ExternalShuffleService protects against single-executor loss, checkpoint patterns protect against multi-executor wave failures, and speculation protects against performance heterogeneity on mixed instance fleets. [Ref: 452](spark_book.pdf#page=452) [Ref: 456](spark_book.pdf#page=456) [Ref: 459](spark_book.pdf#page=459) [Ref: 463](spark_book.pdf#page=463) [Ref: 470](spark_book.pdf#page=470)

Graviton3 instances represent a structural cost-performance improvement for memory bandwidth-bound Spark workloads — the dominant category in production ETL and analytics. The 25% memory bandwidth increase directly accelerates Tungsten's UnsafeRow operations, and the 20% lower On-Demand price compounds to a 40–45% cost-per-query reduction on shuffle-heavy aggregation pipelines. Combined with CAPACITY_OPTIMIZED Spot targeting, the Magic Committer eliminating S3 commit latency, and AQE's runtime partition coalescing, a fully-tuned EC2 Spark deployment delivers production-grade reliability at 20–30% of the cost of naive On-Demand deployments. [Ref: 453](spark_book.pdf#page=453) [Ref: 457](spark_book.pdf#page=457) [Ref: 461](spark_book.pdf#page=461) [Ref: 464](spark_book.pdf#page=464) [Ref: 471](spark_book.pdf#page=471)

