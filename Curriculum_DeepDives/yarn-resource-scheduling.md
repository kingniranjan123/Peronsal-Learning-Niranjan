# 🔥 Master Class: YARN Resource Scheduling

## Overview
<div style='text-align: right; margin-top: -10px; margin-bottom: 20px; font-size: 0.85rem; color: #a0aec0;'><em>References: [Ref: 451](spark_book.pdf#page=451) [Ref: 455](spark_book.pdf#page=455) [Ref: 458](spark_book.pdf#page=458) [Ref: 463](spark_book.pdf#page=463) [Ref: 470](spark_book.pdf#page=470) [Ref: 452](spark_book.pdf#page=452) [Ref: 456](spark_book.pdf#page=456) [Ref: 459](spark_book.pdf#page=459) [Ref: 464](spark_book.pdf#page=464) [Ref: 453](spark_book.pdf#page=453) [Ref: 457](spark_book.pdf#page=457) [Ref: 461](spark_book.pdf#page=461) [Ref: 469](spark_book.pdf#page=469)</em></div>

YARN (Yet Another Resource Negotiator) is the cluster operating system layer that sits beneath Apache Spark in every Hadoop-based deployment. When a Spark application launches, the Driver submits a request to the YARN ResourceManager, which allocates Containers — bounded execution environments with a guaranteed slice of CPU (vcores) and memory — across NodeManagers on worker nodes. The ResourceManager does not simply hand out whatever a job asks for; it runs a scheduling pipeline that enforces multi-tenancy fairness, hierarchical capacity guarantees, and access-controlled node placement, all simultaneously.

Understanding YARN scheduling is not optional for production Spark engineers. A misconfigured queue capacity leads to silent starvation where executors never materialise, causing `SparkContext` initialisation to hang until `spark.yarn.am.waitTime` (default 100 seconds) expires with the cryptic error `Application application_XXXX_XXXX failed 2 times`. An incorrect node label expression causes every Container request to be rejected because no labelled node is present in the queue's `accessible-node-labels`. A missing External Shuffle Service causes dynamic allocation to destroy executor Containers mid-job, making shuffle blocks unreachable and triggering a cascade of `FetchFailedException` errors that abort stages.

The five mechanisms covered here — DominantResourceCalculator, node labels, queue hierarchy, preemption policy, and dynamic allocation with ESS — are not independent features. They form an interlocking system: queue hierarchy defines where Containers land, DominantResourceCalculator decides how many are fair, node labels constrain placement, preemption enforces capacity contracts under load, and dynamic allocation determines the Container lifecycle. Mastery of all five is required to reliably operate Spark at scale. 

---

## 🏗️ Architectural Deep Dive 

### How It Works Under the Hood

When a Spark executor requests resources, the ApplicationMaster (running in its own Container on a NodeManager) sends `ResourceRequest` objects to the ResourceManager's Scheduler via RPC. Each `ResourceRequest` encodes a requested resource profile (`<memory, vcores>`), a locality preference (rack, node, or ANY), a node label expression, and a priority. The ResourceManager's active scheduler — almost universally the **Capacity Scheduler** in production — processes these requests on every heartbeat cycle (default every second, controlled by `yarn.resourcemanager.scheduler.client.thread-count`).

The Capacity Scheduler evaluates queue hierarchy depth-first. It checks whether the target queue has headroom — the difference between its current usage and its `capacity` or `maximum-capacity` ceiling expressed as a percentage of total cluster resources. This headroom calculation is where **DominantResourceCalculator** (DRC) enters. DRC replaces the legacy DefaultResourceCalculator which only considered memory. DRC computes each queue's dominant resource: if a queue has consumed 40% of cluster memory but only 20% of cluster vcores, its dominant resource share is 40%. Scheduling decisions are made by comparing dominant shares, not absolute values, ensuring CPU-heavy and memory-heavy workloads coexist fairly without one dimension becoming a hidden bottleneck. Without DRC enabled (`yarn.scheduler.capacity.resource-calculator=org.apache.hadoop.yarn.util.resource.DominantResourceCalculator`), a vcore-intensive Spark job that under-requests memory will appear cheaper than it truly is, enabling it to crowd out memory-heavy workloads.

Node labels partition cluster nodes into disjoint (exclusive) or shared (non-exclusive) partitions. Each NodeManager registers with zero or one node partition label. A queue is then configured with `accessible-node-labels` and a separate capacity allocation per label partition. Spark can target specific partitions by setting `spark.yarn.executor.nodeLabelExpression`, embedding a label expression into every Container request. The ResourceManager's `NodeLabelManager` maintains the label-to-node mapping in ZooKeeper under `/yarn/node-labels`, making label assignments highly available. When a Container request carries a label that no queue can satisfy, it remains in `PENDING` state indefinitely — this is the single most common cause of executors that "never appear" in production GPU or high-memory cluster deployments.

Preemption is the mechanism by which the Capacity Scheduler reclaims Containers from over-capacity queues to give them to under-served queues. When `yarn.scheduler.capacity.preemption.enabled=true`, the `PreemptionManager` runs a background thread (period set by `yarn.resourcemanager.monitor.capacity.preemption.monitoring_interval`, default 3 seconds). It calculates each queue's ideal share, identifies queues exceeding it, and marks excess Containers for reclamation. Marked Containers are first given a grace period (`yarn.resourcemanager.monitor.capacity.preemption.max_wait_before_kill`, default 15 seconds) during which the ApplicationMaster can voluntarily release them. Spark's AM does not implement voluntary preemption, so YARN always kills the Container hard after the grace period, causing the executor to be lost and all in-flight tasks and cached shuffle data on it to be invalidated.

```scala
ResourceManager JVM
┌──────────────────────────────────────────────────────────────────┐
│ RPC Server (Client/AM/RM protocols) │
│ ┌────────────────────────┐ ┌──────────────────────────────┐ │
│ │ ApplicationManager │ │ Capacity Scheduler │ │
│ │ (App lifecycle, AM │ │ ┌──────────────────────┐ │ │
│ │ tracking, history) │ │ │ Queue Hierarchy │ │ │
│ └────────────────────────┘ │ │ root │ │ │
│ │ │ ├─ prod (60%) │ │ │
│ ┌────────────────────────┐ │ │ │ ├─ spark (40%) │ │ │
│ │ NodeLabelManager │ │ │ │ └─ hive (20%) │ │ │
│ │ (ZK-backed label map) │ │ │ └─ dev (40%) │ │ │
│ └───────────┬────────────┘ │ └──────────────────────┘ │ │
│ │ label lookup │ ┌──────────────────────┐ │ │
│ ▼ │ │DominantResourceCalc │ │ │
│ ┌────────────────────────┐ │ │ dom_share = max( │ │ │
│ │ PreemptionManager │ │ │ mem_used/mem_total, │ │ │
│ │ (monitors every 3s, │ │ │ cpu_used/cpu_total) │ │ │
│ │ kills after 15s) │ │ └──────────────────────┘ │ │
│ └────────────────────────┘ └──────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘
 │ allocate() RPC heartbeat (every ~1s)
 ▼
NodeManager JVM (×N worker nodes)
┌──────────────────────────────────────────────────────────────────┐
│ ContainerManager │
│ ┌──────────────────────┐ ┌──────────────────────────────┐ │
│ │ Executor Container │ │ External Shuffle Service │ │
│ │ (Task Runners, │ │ (AuxService: retains shuffle│ │
│ │ BlockManager, │ │ data after Container dies; │ │
│ │ NettyRpcEnv) │ │ serves to other executors) │ │
│ └──────────────────────┘ └──────────────────────────────┘ │
└──────────────────────────────────────────────────────────────────┘ 
```

### Key Internal Components

- **DominantResourceCalculator:** Computes each user/queue's fractional share across all resource dimensions (memory, vcores, GPU) and uses the maximum dimension as the dominant share for scheduling comparisons. Activated per-scheduler via `yarn.scheduler.capacity.resource-calculator`. Without it, the scheduler is blind to vcore consumption, causing silent CPU starvation.

- **NodeLabelManager:** Maintains a ZooKeeper-persisted mapping of node hostnames to partition labels. Validated on every Container allocation. Label mismatches between a queue's `accessible-node-labels` and the Container's requested label expression immediately reject the Container with `REJECTED` status, not a graceful queue.

- **PreemptionManager (`ProportionalCapacityPreemptionPolicy`):** Runs continuously in the ResourceManager, computing delta between ideal and actual queue shares. Triggers kill signals to the NodeManager's `ContainerManager` after a configurable grace period. Spark executors receiving a SIGTERM from YARN log `ExecutorLostFailure (executor X exited caused by 'Container killed by the ApplicationMaster'`.

- **External Shuffle Service (ESS):** An `AuxiliaryService` running inside each NodeManager JVM (class `org.apache.spark.network.yarn.YarnShuffleService`). ESS registers shuffle file metadata in a `LevelDB` store (`spark.shuffle.service.db.enabled=true`) so shuffle data persists even after the executor Container is terminated by dynamic allocation or preemption. Without ESS, dynamic allocation is unsafe: a removed executor takes its shuffle blocks with it. 

---

## ⚠️ Critical Concepts & Common Pitfalls 

### Queue Capacity vs. Maximum-Capacity and the Silent Starvation Trap

The Capacity Scheduler distinguishes between a queue's `capacity` (the guaranteed minimum share of cluster resources) and its `maximum-capacity` (the absolute ceiling it can elastically expand to). A Spark job targeting a queue with `capacity=10%` and `maximum-capacity=80%` can burst to 80% when the cluster is idle, but the moment other queues reclaim their guaranteed capacity, the Spark job is preempted back toward 10%. The trap is that Spark's AM interprets a Container kill as an executor failure, counts it against `spark.executor.maxNumFailures`, and aborts the application if too many kills happen in a short window.

Operators frequently set `maximum-capacity=100%` thinking it is purely an upper bound that improves utilisation. In practice, this allows a single queue to monopolise the cluster, leaving zero headroom for guaranteed-capacity queues to even start their ApplicationMasters. The AM itself requires one Container, and if the cluster is at 100% utilisation, that Container never starts. The ResourceManager logs `Queue root.prod is at 100% capacity and cannot accept new applications` and the `spark-submit` process hangs waiting for AM launch. 

### Dynamic Allocation Executor Idle Timeout and Shuffle Data Loss

Dynamic allocation (`spark.dynamicAllocation.enabled=true`) uses `ExecutorAllocationManager` inside the Driver to request and release executors based on pending task count and executor idle time (`spark.dynamicAllocation.executorIdleTimeout`, default 60 seconds). The AM sends a `releaseContainer` RPC to the ResourceManager when an executor is idle, and the NodeManager immediately terminates the Container. If ESS is not configured, the shuffle files written by that executor — stored in the local filesystem under `yarn.nodemanager.local-dirs` — are deleted when the Container's working directory is cleaned up by the NodeManager's `DeletionService`.

Any subsequent stage that depends on those shuffle blocks will issue a `BlockManagerMasterEndpoint` lookup, receive a negative response because the executor's `BlockManager` is no longer registered, and throw `org.apache.spark.shuffle.FetchFailedException`. After `spark.stage.maxConsecutiveAttempts` (default 4) consecutive fetch failures on the same stage, the entire job aborts. In clusters processing petabytes daily, this failure mode kills dozens of jobs per hour when ESS is missing. Enabling ESS requires three coordinated changes: YARN-side `auxiliary-services`, NodeManager classpath inclusion of the Spark shuffle JAR, and `spark.shuffle.service.enabled=true` on the Spark side. 

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|------------|----------|-------|
| Container allocation (DRC) | O(Q × R) per heartbeat | No | Q = queue depth, R = resource dimensions; runs every ~1s in RM |
| Node label filtering | O(N) per request | No | N = nodes in cluster; done by NodeLabelManager before scheduler |
| Preemption scan | O(Q × C) per interval | No | C = containers per queue; default 3s interval, can be tuned |
| Dynamic alloc scale-up | O(pending tasks) | No | `ExecutorAllocationManager` polls every `spark.dynamicAllocation.schedulerBacklogTimeout` (1s) |
| ESS shuffle read (remote) | O(shuffle blocks) | Yes | Served from NM local disk via Netty; no Container overhead |
| ESS shuffle write (local) | O(partition count) | No | Written to `yarn.nodemanager.local-dirs` by executor before release | 

---

## 💻 Code Examples 

### Example 1: Capacity Scheduler Queue Configuration with DominantResourceCalculator

> **What this demonstrates:** How to wire up a two-level queue hierarchy with DRC enabled, GPU resource type support, and per-label capacities — the foundational YARN configuration every Spark cluster needs before any application runs.

```xml
<!-- capacity-scheduler.xml — deployed to all ResourceManager nodes -->
<configuration>

 <!-- CRITICAL: Switch from DefaultResourceCalculator (memory-only) to DRC.
 Without this, vcore allocation is invisible to the fairness algorithm.
 A Spark job requesting 4 vcores/8GB will appear identical to one
 requesting 1 vcore/8GB, causing CPU oversubscription. -->
 <property>
 <name>yarn.scheduler.capacity.resource-calculator</name>
 <value>org.apache.hadoop.yarn.util.resource.DominantResourceCalculator</value>
 </property>

 <!-- Root queue: must sum to 100 across children -->
 <property>
 <name>yarn.scheduler.capacity.root.queues</name>
 <value>prod,dev,default</value>
 </property>

 <!-- Production queue: 60% guaranteed, can burst to 90% -->
 <property>
 <name>yarn.scheduler.capacity.root.prod.capacity</name>
 <value>60</value>
 </property>
 <property>
 <name>yarn.scheduler.capacity.root.prod.maximum-capacity</name>
 <value>90</value>
 </property>

 <!-- Sub-queues within prod: spark gets 70% of prod's 60% = 42% cluster -->
 <property>
 <name>yarn.scheduler.capacity.root.prod.queues</name>
 <value>spark,hive</value>
 </property>
 <property>
 <name>yarn.scheduler.capacity.root.prod.spark.capacity</name>
 <value>70</value>
 </property>
 <property>
 <name>yarn.scheduler.capacity.root.prod.hive.capacity</name>
 <value>30</value>
 </property>

 <!-- Node label: GPU-equipped nodes are labelled "gpu".
 prod.spark queue can access both the default (unlabelled) and "gpu" partitions. -->
 <property>
 <name>yarn.scheduler.capacity.root.prod.spark.accessible-node-labels</name>
 <value>gpu,*</value> <!-- * means unlabelled (default partition) -->
 </property>
 <!-- 100% of the gpu-labelled partition is available to this queue -->
 <property>
 <name>yarn.scheduler.capacity.root.prod.spark.accessible-node-labels.gpu.capacity</name>
 <value>100</value>
 </property>

 <!-- Dev queue: lower capacity, can be fully preempted by prod -->
 <property>
 <name>yarn.scheduler.capacity.root.dev.capacity</name>
 <value>30</value>
 </property>
 <property>
 <name>yarn.scheduler.capacity.root.dev.maximum-capacity</name>
 <value>60</value>
 </property>

 <!-- Default catch-all queue: 10% guaranteed, never goes to zero -->
 <property>
 <name>yarn.scheduler.capacity.root.default.capacity</name>
 <value>10</value>
 </property>

 <!-- Enable preemption globally: PreemptionManager scans every 3s -->
 <property>
 <name>yarn.resourcemanager.scheduler.monitor.enable</name>
 <value>true</value>
 </property>
 <property>
 <name>yarn.resourcemanager.scheduler.monitor.policies</name>
 <value>org.apache.hadoop.yarn.server.resourcemanager.monitor.capacity.ProportionalCapacityPreemptionPolicy</value>
 </property>
 <!-- Executor gets 15s to finish in-flight tasks before hard kill -->
 <property>
 <name>yarn.resourcemanager.monitor.capacity.preemption.max_wait_before_kill</name>
 <value>15000</value>
 </property>

</configuration>
```

> **Mastery Note:** The `accessible-node-labels` property is validated at Container allocation time, not at queue definition time. A queue that lists label `gpu` but has no nodes registered with that label will silently reject every Container carrying `nodeLabelExpression=gpu`, leaving executors in `PENDING` state with no error logged to the Spark Driver. Always verify label registration with `yarn node -list -showDetails` and confirm the label appears under `Node-Labels` before deploying label-targeted Spark jobs. The per-label capacity (100% here) is independent of the queue's overall cluster capacity — it means "100% of the gpu partition's resources are available to this queue," not "this queue gets 100% of the cluster."

---

### Example 2: Spark Submit with Node Labels, Queue Targeting, and Resource Profiles

> **What this demonstrates:** How to target a specific YARN queue and node partition from `spark-submit`, and how Spark 3.x Resource Profiles allow heterogeneous executor sizing within a single application — a pattern critical for ML pipelines that mix CPU preprocessing with GPU inference.

```bash
#!/usr/bin/env bash
# submit_ml_pipeline.sh — Production ML pipeline submission script

spark-submit \
 --master yarn \
 --deploy-mode cluster \

 # Target the prod.spark queue explicitly.
 # Without this, Spark lands in root.default (10% capacity) and starves.
 --queue prod.spark \

 # ApplicationMaster Container: 2GB overhead above spark.driver.memory.
 # YARN enforces: AM memory = spark.driver.memory + spark.driver.memoryOverhead
 # spark.driver.memoryOverhead default = max(384MB, 10% of driver memory)
 --driver-memory 4g \
 --driver-cores 2 \

 # Default executor profile: 8 executors for CPU-bound ETL stages
 --num-executors 8 \
 --executor-memory 16g \
 --executor-cores 4 \

 # Memory overhead: must cover off-heap Tungsten allocations + native libs.
 # Rule: set to at least 10% of executor-memory, or 384MB, whichever is larger.
 # For pandas UDFs (Arrow IPC), add ~2GB for the Python worker heap.
 --conf spark.executor.memoryOverhead=2048 \

 # Target unlabelled nodes for the default executor profile.
 # Label expression "" means default partition (nodes with no label).
 --conf spark.yarn.executor.nodeLabelExpression="" \

 # GPU Resource Profile for inference executors (Spark 3.x ResourceProfile API)
 # These are submitted as separate ContainerRequests with nodeLabelExpression=gpu
 --conf spark.executor.resource.gpu.amount=1 \
 --conf spark.executor.resource.gpu.discoveryScript=/opt/spark/scripts/getGpusResources.sh \
 --conf spark.yarn.executor.nodeLabelExpression.gpu="gpu" \

 # Disable speculative execution: GPU tasks are expensive to duplicate
 --conf spark.speculation=false \

 # Kryo serialization: 3-5x smaller serialized closures vs Java default serializer.
 # Critical for broadcast variables on GPU inference models (often 500MB+).
 --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
 --conf spark.kryo.registrationRequired=false \

 --class com.company.ml.InferencePipeline \
 hdfs:///apps/ml-pipeline-3.2.0.jar
```

> **Mastery Note:** The `spark.executor.resource.gpu.discoveryScript` is executed inside each executor Container at startup. It must output a JSON blob like `{"name":"gpu","addresses":["0","1"]}` to stdout. If it fails or produces malformed output, the executor registers with zero GPU resources, and any task requesting `TaskContext.resources()("gpu")` throws `SparkException: task's resource requests could not be met`. The label expression targeting GPU nodes (`nodeLabelExpression=gpu`) must match exactly the label registered in YARN's `NodeLabelManager` — including case sensitivity. Confirm the exact label string with `yarn rmadmin -listClusterNodeLabels` before submission.

---

### Example 3: External Shuffle Service Configuration and Dynamic Allocation

> **What this demonstrates:** The complete three-part configuration — YARN NodeManager, `yarn-site.xml`, and Spark application — required to safely enable dynamic allocation. All three must be consistent or shuffle data loss will occur silently.

```xml
<!-- PART 1: yarn-site.xml on ALL NodeManager nodes -->
<configuration>

 <!-- Register ESS as an AuxiliaryService inside the NodeManager JVM.
 The service starts with the NodeManager and outlives any executor Container. -->
 <property>
 <name>yarn.nodemanager.aux-services</name>
 <value>spark_shuffle</value> <!-- name must match spark.shuffle.service.name -->
 </property>
 <property>
 <name>yarn.nodemanager.aux-services.spark_shuffle.class</name>
 <value>org.apache.spark.network.yarn.YarnShuffleService</value>
 </property>

 <!-- ESS JAR must be on NM classpath. Symlink from Spark distribution: -->
 <!-- ln -s $SPARK_HOME/yarn/spark-3.x.x-yarn-shuffle.jar $HADOOP_HOME/share/hadoop/yarn/lib/ -->

 <!-- LevelDB persistence: ESS persists shuffle metadata across NM restarts.
 Without this, a NodeManager restart during a long Spark job orphans all
 shuffle blocks written before the restart, causing FetchFailedException. -->
 <property>
 <name>spark.shuffle.service.db.enabled</name>
 <value>true</value>
 </property>
 <property>
 <name>spark.shuffle.service.db.backend</name>
 <value>LEVELDB</value>
 </property>

</configuration>
```

```python
# PART 2: Spark application configuration — pyspark_dynamic_alloc.py

from pyspark.sql import SparkSession

spark = SparkSession.builder \
 .appName("DynamicAllocDemo") \
 .config("spark.master", "yarn") \

 # ESS must be enabled on the Spark side to match YARN-side aux-service name.
 # These two values must be identical: yarn.nodemanager.aux-services entry
 # and spark.shuffle.service.name (default: "spark_shuffle").
 .config("spark.shuffle.service.enabled", "true") \
 .config("spark.shuffle.service.name", "spark_shuffle") \ # must match yarn-site.xml

 # Dynamic allocation: AM requests/releases executors based on workload.
 .config("spark.dynamicAllocation.enabled", "true") \
 .config("spark.dynamicAllocation.minExecutors", "2") \ # never scale below 2
 .config("spark.dynamicAllocation.maxExecutors", "100") \ # hard ceiling per queue
 .config("spark.dynamicAllocation.initialExecutors", "5") \ # start with 5

 # Scale up aggressively: if tasks are pending for >1s, request more executors.
 .config("spark.dynamicAllocation.schedulerBacklogTimeout", "1s") \

 # Scale down conservatively: executor must be idle for 120s before release.
 # Default is 60s — too aggressive for jobs with uneven stage boundaries.
 .config("spark.dynamicAllocation.executorIdleTimeout", "120s") \

 # Shuffle tracking (Spark 3.x alternative to ESS for structured streaming):
 # DO NOT use with batch jobs that have large shuffles — it disables decommission.
 # .config("spark.dynamicAllocation.shuffleTracking.enabled", "false") \

 # Executor decommission: gives executor time to migrate shuffle blocks before kill.
 # Requires ESS or shuffle tracking. Extends graceful shutdown window.
 .config("spark.executor.decommission.enabled", "true") \
 .config("spark.storage.decommission.shuffleBlocks.enabled", "true") \
 .config("spark.storage.decommission.rddBlocks.enabled", "true") \

 .getOrCreate()

# Simulate workload with two stages to test scale-up and scale-down
import time

# Stage 1: Wide shuffle — triggers executor scale-up to maxExecutors
df = spark.range(1_000_000_000).repartition(200)
result = df.groupBy((df.id % 1000).alias("bucket")).count()
result.write.mode("overwrite").parquet("hdfs:///output/dynamic-alloc-test/")

# Pause: ExecutorAllocationManager will scale down idle executors after 120s
time.sleep(130)

# Stage 2: Narrow operation — runs on minimal executor count
summary = spark.read.parquet("hdfs:///output/dynamic-alloc-test/").describe()
summary.show()

spark.stop()
```

> **Mastery Note:** The executor decommission protocol (`spark.executor.decommission.enabled`) is the correct way to handle dynamic scale-down in Spark 3.1+. When the AM decides to release an executor, instead of immediately sending `releaseContainer`, it first sends a decommission signal. The executor's `BlockManagerDecommissioner` migrates shuffle blocks to other live executors or to ESS storage before acknowledging the release. This eliminates the FetchFailedException race condition entirely. The older approach — relying solely on ESS to retain blocks — still works but does not migrate RDD cache blocks, which are lost permanently on executor removal.

---

### Example 4: Programmatic Queue Introspection and Preemption-Aware Job Submission

> **What this demonstrates:** How a Spark application can query YARN's REST API at runtime to inspect queue headroom, detect preemption risk, and adaptively tune `spark.dynamicAllocation.maxExecutors` to avoid breaching queue capacity and triggering preemption of its own Containers.

```python
# preemption_aware_submission.py
# Pattern: Query YARN RM REST API before submission to compute safe executor ceiling.

import requests
import json
import subprocess
import sys

YARN_RM_HOST = "http://resourcemanager-active:8088"
TARGET_QUEUE = "root.prod.spark"
EXECUTOR_MEMORY_MB = 16384 # 16GB per executor
EXECUTOR_VCORES = 4
OVERHEAD_MB = 2048 # spark.executor.memoryOverhead

def get_queue_info(queue_name: str) -> dict:
 """
 Calls YARN ResourceManager REST API to retrieve Capacity Scheduler queue metrics.
 Returns the raw queue JSON node from the scheduler response tree.
 """
 url = f"{YARN_RM_HOST}/ws/v1/cluster/scheduler"
 resp = requests.get(url, timeout=10)
 resp.raise_for_status()
 scheduler = resp.json()["scheduler"]["schedulerInfo"]

 # Recursively traverse the queue tree to find the target queue by name.
 def find_queue(node: dict, target: str) -> dict | None:
 if node.get("queueName") == target.split(".")[-1]:
 return node
 for child in node.get("queues", {}).get("queue", []):
 result = find_queue(child, target)
 if result:
 return result
 return None

 queue = find_queue(scheduler, queue_name)
 if not queue:
 raise ValueError(f"Queue {queue_name} not found in scheduler response")
 return queue

def compute_safe_max_executors(queue: dict) -> int:
 """
 Computes the maximum number of executors that can be safely requested without
 pushing the queue past its guaranteed capacity (triggering preemption by peers).

 Key insight: we target 90% of guaranteed capacity, not maximum-capacity.
 Staying within guaranteed capacity means our Containers are immune to preemption
 from other queues. Exceeding it means elastic usage that can be killed at any time.
 """
 # absoluteCapacityMB = guaranteed memory for this queue in MB
 # absoluteUsedCapacityMB = currently consumed memory
 guaranteed_mb = queue["absoluteCapacity"] * queue["resourcesUsed"]["memory"] / \
 max(queue["usedCapacity"], 0.01) # avoid division by zero

 # Simpler direct calculation if cluster total memory is available:
 cluster_url = f"{YARN_RM_HOST}/ws/v1/cluster/metrics"
 metrics = requests.get(cluster_url, timeout=10).json()["clusterMetrics"]
 total_cluster_memory_mb = metrics["totalMB"]

 # guaranteed_mb = absolute capacity fraction × total cluster memory
 abs_capacity_fraction = queue["absoluteCapacity"] / 100.0
 guaranteed_mb = abs_capacity_fraction * total_cluster_memory_mb

 # Already consumed by this queue
 used_mb = queue["resourcesUsed"]["memory"]
 available_guaranteed_mb = guaranteed_mb - used_mb

 # Each executor needs memory + overhead
 executor_total_mb = EXECUTOR_MEMORY_MB + OVERHEAD_MB

 # Stay at 90% of available guaranteed headroom to buffer against accounting lag
 safe_executors = int((available_guaranteed_mb * 0.9) / executor_total_mb)

 print(f"Queue: {TARGET_QUEUE}")
 print(f"Guaranteed cluster memory: {guaranteed_mb:.0f} MB")
 print(f"Currently used: {used_mb:.0f} MB")
 print(f"Available headroom (90%): {available_guaranteed_mb * 0.9:.0f} MB")
 print(f"Safe max executors: {safe_executors}")

 return max(safe_executors, 2) # always allow at least 2 executors

if __name__ == "__main__":
 queue_info = get_queue_info(TARGET_QUEUE)
 max_exec = compute_safe_max_executors(queue_info)

 # Inject the computed ceiling into spark-submit via --conf
 # This prevents Spark from bursting into elastic capacity and becoming preemptible.
 cmd = [
 "spark-submit",
 "--master", "yarn",
 "--deploy-mode", "cluster",
 "--queue", TARGET_QUEUE,
 "--executor-memory", f"{EXECUTOR_MEMORY_MB}m",
 "--executor-cores", str(EXECUTOR_VCORES),
 "--conf", "spark.dynamicAllocation.enabled=true",
 "--conf", "spark.shuffle.service.enabled=true",
 "--conf", f"spark.dynamicAllocation.maxExecutors={max_exec}",
 "--conf", "spark.dynamicAllocation.minExecutors=2",
 "--conf", f"spark.executor.memoryOverhead={OVERHEAD_MB}m",
 "--class", "com.company.etl.MainJob",
 "hdfs:///apps/etl-job.jar"
 ]

 print(f"\nLaunching with maxExecutors={max_exec}")
 print("Command:", " ".join(cmd))
 result = subprocess.run(cmd, check=True)
 sys.exit(result.returncode)
```

> **Mastery Note:** The YARN RM REST API (`/ws/v1/cluster/scheduler`) returns `absoluteCapacity` as a percentage of total cluster resources, not as an absolute MB value. The conversion `absoluteCapacity / 100 × totalMB` is necessary to derive the actual memory ceiling. The `resourcesUsed.memory` field in the response reflects the ResourceManager's committed view of allocations — it includes memory for Containers that have been allocated but whose tasks may not yet have started. This means headroom calculations will always be slightly conservative, which is the safe direction. Teams that skip this check and blindly set `maxExecutors=500` on a queue with a 1,000-node cluster end up monopolising elastic capacity, triggering mass preemption of peer queues and causing cluster-wide SLA violations.

---

## 🎯 Mastery Checklist

To achieve true mastery of YARN Resource Scheduling:

- [ ] Understand how `DominantResourceCalculator` differs from `DefaultResourceCalculator` and why vcore-only jobs are systematically over-scheduled without it
- [ ] Know when to use node labels (exclusive hardware partitioning) vs. node attributes (flexible affinity constraints without hard partitioning)
- [ ] Be able to diagnose Container-pending failures from the YARN RM UI's "Scheduler" tab — specifically distinguishing between `QUEUE_NOT_MAPPABLE`, `NODE_PARTITION_INVALID`, and `QUEUE_CAPACITY_EXCEEDED` rejection reasons
- [ ] Understand the tradeoff between `spark.dynamicAllocation.executorIdleTimeout` (scale-down speed vs. shuffle safety) and `spark.storage.decommission.shuffleBlocks.enabled` (safe migration vs. added latency)
- [ ] Know how preemption interacts with `spark.task.maxFailures` and `spark.stage.maxConsecutiveAttempts` to cause job aborts when grace periods are shorter than task runtimes
- [ ] Be able to configure ESS with LevelDB persistence and verify it is running with `yarn node -status <nodeId>` (look for `spark_shuffle` in `Auxiliary Service running`)
- [ ] Understand the difference between `maximum-capacity` (elastic burst, preemptible) and `capacity` (guaranteed, preemption-immune) in the context of SLA-bound production jobs

---

## 📚 Summary

YARN Resource Scheduling is the contract layer between Spark's computational demands and the physical cluster's finite resources. The Capacity Scheduler's queue hierarchy defines the multi-tenant resource contract: guaranteed capacity is SLA-level protection, maximum capacity is opportunity. DominantResourceCalculator enforces this contract across all resource dimensions simultaneously — without it, the fairness guarantees apply only to memory, and CPU becomes a hidden shared resource with no enforcement, leading to throughput collapses under mixed-workload conditions. Node labels add a hardware-partitioning dimension that allows GPU, high-memory, or SSD-backed nodes to be reserved for specific workloads without running separate clusters. 

Preemption is the enforcement mechanism for the capacity contract. When an elastic burst from one queue prevents another queue from reaching its guaranteed minimum, the PreemptionManager reclaims Containers — hard kills after a configurable grace period. For Spark, this means executors can disappear at any time when operating in elastic burst territory, and the application must be architected to tolerate it: ESS retains shuffle data, executor decommission migrates blocks before release, and `maxNumFailures` must be sized to absorb preemption events without aborting. The External Shuffle Service is not optional in preemption-enabled clusters — it is the mechanism that makes the entire dynamic executor lifecycle safe. 

The interaction between these five mechanisms — DRC, labels, hierarchy, preemption, and ESS — means that cluster operators and Spark engineers must share a unified mental model. A queue configuration change in `capacity-scheduler.xml` by a YARN administrator can silently change whether a Spark job's executors are preemptible, which in turn determines whether ESS is load-bearing or merely advisory. Production Spark engineering requires understanding YARN as deeply as Spark's own internals, because the boundary between them is where the most costly and least-debuggable failures originate. 

