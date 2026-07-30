# 🔥 Master Class: Mesos Resource Scheduling Quiz

## 1. True/False Questions

**Q1: Mesos operates on a request-based allocation model exactly like Hadoop YARN.**
**Answer:** False
**Mastery Explanation:** Mesos uses a revolutionary two-level scheduling mechanism based on "resource offers" inversion of control, unlike YARN's request-based model.

**Q2: The Mesos Master uses the Dominant Resource Fairness (DRF) algorithm to determine which framework gets the next resource offer.**
**Answer:** True
**Mastery Explanation:** DRF ensures mathematical fairness across the cluster by prioritizing the framework with the lowest share of its dominant resource (CPU or memory).

**Q3: Spark's Tungsten engine strictly allocates memory within the JVM heap to avoid Linux cgroup OOM Killer.**
**Answer:** False
**Mastery Explanation:** Tungsten relies heavily on off-heap memory, direct byte buffers, and memory-mapped files. If this off-heap memory combined with heap exceeds the cgroup `memory.limit_in_bytes`, the Linux kernel triggers an ungraceful OOM kill (Exit 137).

**Q4: Setting `spark.executor.memory=8g` and `spark.executor.memoryOverhead=1g` creates a hard kernel cgroup boundary at exactly 9GB.**
**Answer:** True
**Mastery Explanation:** Mesos maps these parameters directly to the cgroup memory limit. Breaching 9GB for even a microsecond results in a SIGKILL.

**Q5: Offer Starvation occurs when a Spark application rejects small resource offers indefinitely while waiting for a massive chunk of contiguous resources that never materializes.**
**Answer:** True
**Mastery Explanation:** This happens in highly concurrent clusters, leading to a deadlock-like scenario where the Driver spins without launching executors.

**Q6: Mesos provides dedicated network bandwidth guarantees for Spark executors by default.**
**Answer:** False
**Mastery Explanation:** Mesos allocates CPU and memory but does not guarantee dedicated network bandwidth by default, making aggressive serialization (like Kryo) essential.

**Q7: Spark's Dynamic Allocation on Mesos requires the External Shuffle Service to be enabled.**
**Answer:** True
**Mastery Explanation:** If an executor is reclaimed by Mesos, shuffle data is lost unless the independent External Shuffle Service is running to preserve map outputs.

**Q8: Using the Docker containerizer on Mesos for Spark requires writing shuffle files directly into the Docker overlay filesystem.**
**Answer:** False
**Mastery Explanation:** Writing shuffle files to the Docker overlay filesystem crushes disk I/O. Host volumes (e.g., NVMe drives) must be mounted to `/tmp` for maximum throughput.

**Q9: The `MesosCoarseGrainedSchedulerBackend` bypasses Mesos entirely when dispatching tasks once executors are up.**
**Answer:** True
**Mastery Explanation:** Once Mesos allocates resources and executors launch, Spark's TaskScheduler dispatches tasks directly via Akka/Netty to the executors.

**Q10: Mesos constraints allow Catalyst optimizer to natively plan physical execution strategies based on hardware topology.**
**Answer:** False
**Mastery Explanation:** Catalyst and DAGScheduler remain completely oblivious to Mesos constraints. The `MesosSchedulerBackend` handles physical isolation, guaranteeing execution on designated hardware without Catalyst's awareness.

## 2. Multiple Choice Questions

**Q11: Which Mesos component tracks memory and CPU capacity on physical nodes and reports to the Master?**
A) Mesos Master
B) Dominant Resource Fairness (DRF)
C) Mesos Agent (Slave)
D) MesosCoarseGrainedSchedulerBackend
**Answer:** C
**Mastery Explanation:** The Mesos Agent is the background daemon on physical nodes monitoring and reporting capacity, and executing tasks inside isolated cgroups.

**Q12: What is the primary cause of an Exit 137 (SIGKILL) error in Spark on Mesos?**
A) Unhandled Java NullPointerException in Executor
B) Tungsten off-heap memory exceeding the Linux cgroup `memory.limit_in_bytes` boundary
C) Mesos Master failing to send resource offers
D) Network timeout during shuffle phase
**Answer:** B
**Mastery Explanation:** The Linux kernel's OOM killer instantly terminates the executor via SIGKILL if the JVM + off-heap allocations breach the cgroup limit dictated by `spark.executor.memory` + `spark.executor.memoryOverhead`.

**Q13: How does the Dominant Resource Fairness (DRF) algorithm prioritize frameworks?**
A) By looking at the framework with the highest priority score configuration
B) By identifying the framework with the longest wait time
C) By analyzing the share of the dominant resource each framework consumes and prioritizing the one with the lowest share
D) By randomly assigning offers
**Answer:** C
**Mastery Explanation:** DRF ensures fair sharing by calculating the dominant resource (CPU or memory) for each framework and offering resources to the one currently consuming the smallest percentage of its dominant resource.

**Q14: What configuration is critical to prevent Offer Starvation in a busy multi-tenant Mesos cluster?**
A) `spark.executor.cores`
B) `spark.dynamicAllocation.enabled`
C) `spark.mesos.role` and reservations
D) `spark.task.maxFailures`
**Answer:** C
**Mastery Explanation:** Configuring Mesos roles and utilizing reservations guarantees a minimum floor of resources for the Spark framework, preventing starvation by competing short-lived tasks.

**Q15: Why is Kryo serialization heavily recommended for Spark on Mesos?**
A) Mesos strictly requires Kryo to launch executors
B) Kryo prevents cgroup OOM kills
C) Mesos doesn't guarantee dedicated network bandwidth, and Kryo minimizes the TCP footprint of shared networks
D) Kryo is required for the Docker containerizer
**Answer:** C
**Mastery Explanation:** Because executors share network bandwidth with other apps (like Redis), internal RPCs, broadcasts, and shuffles must be aggressively serialized to reduce network congestion.

**Q16: When scaling down executors in Spark on Mesos, what preserves the map output files?**
A) Mesos Master State
B) Spark Driver BlockManager
C) Mesos External Shuffle Service
D) Mesos Containerizer
**Answer:** C
**Mastery Explanation:** The external shuffle service decouples map outputs from the executor lifecycle, running as an independent process on the Agent to serve shuffle data after the executor is killed.

**Q17: What time complexity defines Mesos Offer Processing?**
A) O(1)
B) O(N) where N is active frameworks
C) O(E) where E is executors
D) O(T) where T is tasks
**Answer:** B
**Mastery Explanation:** The Master evaluates N active frameworks using DRF per allocation cycle, implementing this efficiently in C++.

**Q18: What handles the physical execution of a Spark executor on a Mesos node?**
A) YARN NodeManager
B) Spark TaskScheduler
C) Mesos Containerizer
D) Catalyst Optimizer
**Answer:** C
**Mastery Explanation:** The Mesos Containerizer utilizes Linux cgroups and namespaces to physically isolate and execute the Spark Executor JVM.

**Q19: Which configuration is used to ensure Spark grabs whatever resources it can rather than waiting for massive chunks?**
A) `spark.mesos.coarse=true`
B) `spark.mesos.fine.grained=true`
C) `spark.dynamicAllocation.minExecutors`
D) `spark.cores.max`
**Answer:** A
**Mastery Explanation:** Setting `spark.mesos.coarse=true` (the default) ensures the framework accepts available offers and holds them, preventing gridlock.

**Q20: What happens if `spark.executor.memoryOverhead` is NOT properly padded for Tungsten workloads on Mesos?**
A) The task fails over to YARN
B) The Spark UI gracefully shows an out of memory error
C) The Linux kernel OOM killer triggers an ungraceful executor death (Lost executor)
D) Spark dynamically shrinks the heap
**Answer:** C
**Mastery Explanation:** Without sufficient padding, Tungsten's off-heap allocations push total memory over the cgroup limit, resulting in a sudden, ungraceful SIGKILL by the OS.

**Q21: What is the complexity of Executor Launch via cgroups in Mesos?**
A) O(1)
B) O(N)
C) O(E)
D) O(T)
**Answer:** A
**Mastery Explanation:** Process isolation via Linux namespaces is near-instantaneous (O(1)), though pulling Docker images may add I/O overhead.

**Q22: How does Spark's `TaskScheduler` dispatch tasks to Mesos executors?**
A) By sending resource requests to the Mesos Master
B) Directly via Akka/Netty, bypassing Mesos entirely
C) Through the Mesos Agent Daemon
D) Using the Mesos External Shuffle Service
**Answer:** B
**Mastery Explanation:** Once executors are launched, the `TaskScheduler` communicates directly with them via Akka/Netty to dispatch tasks, bypassing Mesos for per-task scheduling.

**Q23: Which metric does DRF use to allocate resources?**
A) Absolute CPU cores requested
B) Maximum memory requested
C) The share of the dominant resource currently consumed
D) The number of tasks in the queue
**Answer:** C
**Mastery Explanation:** DRF calculates the percentage of total cluster resources each framework consumes for its most heavily used resource (dominant resource) and prioritizes the lowest.

**Q24: What is the primary risk of writing shuffle data to a Docker overlay filesystem in Mesos?**
A) Network timeout
B) Memory leak
C) Crushing disk I/O performance
D) OOM Killer invocation
**Answer:** C
**Mastery Explanation:** Overlay filesystems are not optimized for heavy I/O. Host volumes must be mounted for fast NVMe storage access during shuffles to maintain throughput.

**Q25: What configuration enforces strict physical isolation for Spark workloads on specific hardware in Mesos?**
A) `spark.mesos.role`
B) `spark.mesos.constraints`
C) `spark.mesos.principal`
D) `spark.dynamicAllocation.enabled`
**Answer:** B
**Mastery Explanation:** Constraints allow filtering offers based on agent attributes (e.g., `hardware_type:gpu`), ensuring execution only occurs on designated hardware.

## 3. Small Twist Questions

**Q26: Scenario:** You have `spark.executor.memory=8g` and `spark.executor.memoryOverhead=1g`. Your executor is consistently killed with Exit 137.
**Twist:** You change `spark.executor.memory=7g` and `spark.executor.memoryOverhead=2g` (total still 9g).
Does the executor still die?
A) Yes, total memory limit is the same.
B) No, Tungsten now has more off-heap space before hitting the cgroup limit.
C) Yes, because the heap is too small now.
D) No, the OS ignores memoryOverhead.
**Answer:** B
**Mastery Explanation:** While the hard cgroup limit remains 9GB, reducing the JVM heap and increasing the overhead gives Tungsten's off-heap allocations more breathing room, preventing it from breaching the 9GB boundary.

**Q27: Scenario:** Spark application is waiting endlessly for resources.
**Twist:** You change `spark.cores.max` from `1000` to `100`. The application instantly gets resources. Why?
A) Mesos limits max cores to 100 per framework.
B) The application stopped suffering from offer starvation by lowering its resource ceiling, allowing it to accept smaller, available resource chunks.
C) DRF prioritizes frameworks with lower `spark.cores.max`.
D) Mesos Agents can only offer 100 cores max.
**Answer:** B
**Mastery Explanation:** By lowering the ceiling, the framework stops rejecting smaller offers while waiting for a massive chunk, resolving the offer starvation deadlock.

**Q28: Scenario:** Dynamic allocation is enabled, and executors scale down. Shuffle data is lost, causing recomputation.
**Twist:** You enable `spark.shuffle.service.enabled=true`.
Does data loss still occur?
A) Yes, because Mesos kills the node.
B) No, the Mesos External Shuffle Service preserves map output files on the Agent's disk independently of the executor JVM.
C) Yes, because dynamic allocation doesn't support Mesos.
D) No, the Driver stores the shuffle data.
**Answer:** B
**Mastery Explanation:** The external shuffle service decouples map outputs from the executor lifecycle, preventing data loss when Mesos reclaims resources.

**Q29: Scenario:** You deploy a PySpark job using Docker containerizer but shuffle performance is terrible.
**Twist:** You add `.set("spark.mesos.executor.docker.volumes", "/data/spark-scratch:/tmp:rw")`. Performance skyrockets. Why?
A) The overlay filesystem is bypassed, mapping host NVMe directly to the container's `/tmp` directory where Spark writes shuffle files.
B) Docker caches shuffle data in RAM.
C) The cgroup memory limit is increased.
D) Mesos allocates more network bandwidth.
**Answer:** A
**Mastery Explanation:** Mapping host volumes prevents Spark from writing high-volume shuffle data into the slow Docker overlay filesystem, vastly improving disk I/O.

**Q30: Scenario:** You want to run a Spark job on GPU nodes.
**Twist:** You add `.set("spark.mesos.constraints", "hardware_type:cpu")` by mistake. What happens?
A) Spark crashes immediately.
B) Spark runs on CPU nodes, but Catalyst optimizes for GPU anyway, causing failure.
C) Catalyst optimization is unaffected, but the job is physically constrained to CPU-only agents, wasting time if GPU was expected.
D) Mesos overrides the constraint based on the code.
**Answer:** C
**Mastery Explanation:** The `MesosSchedulerBackend` filters offers strictly based on constraints. It will only accept CPU nodes. Catalyst is oblivious to physical hardware and just executes the physical plan wherever the executor is placed.

**Q31: Scenario:** A cluster has 1000 CPU cores and 1000GB RAM. Framework A uses 200 cores and 100GB RAM. Framework B uses 50 cores and 400GB RAM.
**Twist:** Mesos needs to make a new offer. Who gets it under DRF?
A) Framework A, because 200 cores > 50 cores.
B) Framework B, because 400GB RAM > 100GB RAM.
C) Framework A, because its dominant share (20% CPU) is lower than Framework B's dominant share (40% RAM).
D) They get equal offers.
**Answer:** C
**Mastery Explanation:** DRF calculates the dominant share (A: 200/1000 = 20% CPU, B: 400/1000 = 40% RAM). It prioritizes the framework with the lowest dominant share, which is A (20%).

**Q32: Scenario:** You configure `.set("spark.mesos.coarse", "false")` (Fine-grained mode, historically available).
**Twist:** Job latency increases massively. Why?
A) Fine-grained mode launches a Mesos task for every Spark task, adding O(1) cgroup isolation overhead per task instead of per executor.
B) Mesos rejects the configuration.
C) Fine-grained mode disables Tungsten.
D) Fine-grained mode forces Kryo serialization off.
**Answer:** A
**Mastery Explanation:** Coarse-grained mode launches executors once (O(1)) and dispatches tasks directly (O(T)). Fine-grained mode requests resources per task, adding massive Mesos offer and cgroup overhead to every single task.

**Q33: Scenario:** Your Spark job requires massive shuffle bandwidth.
**Twist:** You change `.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")`. What is the impact?
A) Performance improves due to native Java optimization.
B) Performance degrades because standard Java serialization has a massive TCP footprint, congesting the non-dedicated Mesos shared network.
C) Mesos kills the executor with Exit 137.
D) The Driver crashes.
**Answer:** B
**Mastery Explanation:** Mesos environments don't guarantee network bandwidth. Java serialization produces bloated byte streams compared to Kryo, causing severe network contention and shuffle slowdowns.

**Q34: Scenario:** Spark driver keeps losing connection to executors in a highly utilized Mesos cluster.
**Twist:** You set `.set("spark.mesos.role", "critical-etl")` and configure Mesos to reserve resources for this role.
A) The Driver runs out of memory.
B) Executors are pinned to specific hardware constraints.
C) Mesos guarantees a minimum resource floor for this role, preventing starvation and executor disconnects due to lack of resources.
D) Dynamic allocation is disabled.
**Answer:** C
**Mastery Explanation:** Roles and reservations ensure that competing frameworks (like Jenkins) don't consume all cluster resources, guaranteeing Spark has enough capacity to maintain its executors.

**Q35: Scenario:** You use `.set("spark.mesos.executor.docker.forcePullImage", "false")`.
**Twist:** The job runs with an outdated dependency on some nodes but not others. Why?
A) Mesos agents cache Docker images locally; without forcePull, agents with old cached versions won't update.
B) Mesos Master caches images.
C) Spark Driver failed to broadcast the dependency.
D) The containerizer crashed.
**Answer:** A
**Mastery Explanation:** If `forcePullImage` is false, Mesos agents will use their locally cached Docker image. If the image was updated in the registry but the tag remained the same, nodes with the old cache will run outdated code.

**Q36: Scenario:** `spark.executor.memory=16g`, `spark.executor.memoryOverhead=3276`.
**Twist:** You change `spark.executor.memoryOverhead=1000`. The job immediately fails with Exit 137 on a `groupBy` operation. Why?
A) `groupBy` requires external shuffle service.
B) `groupBy` triggers Tungsten off-heap sorting, which exceeds the reduced memory overhead, hitting the cgroup limit and triggering OOM Killer.
C) The JVM heap was reduced.
D) Mesos constraints were violated.
**Answer:** B
**Mastery Explanation:** `groupBy` relies on Tungsten's off-heap memory for sorting. Reducing the overhead padding causes this off-heap allocation to breach the hard cgroup boundary, resulting in a SIGKILL.

**Q37: Scenario:** You have Dynamic Allocation enabled but `spark.dynamicAllocation.schedulerBacklogTimeout=60s`.
**Twist:** You change it to `1s`. What is the result?
A) Executors are killed immediately.
B) Spark requests new executors from Mesos almost immediately when a backlog forms, scaling up much faster.
C) The Driver disconnects from Mesos.
D) The External Shuffle Service is disabled.
**Answer:** B
**Mastery Explanation:** The timeout controls how long tasks must wait in the backlog before Spark requests more resources. Lowering it makes scale-up highly aggressive and responsive.

**Q38: Scenario:** Your cluster runs Spark and Cassandra on Mesos.
**Twist:** You notice Spark tasks are slow despite plenty of CPU. You realize Cassandra is consuming massive disk I/O. How to fix?
A) Increase `spark.executor.memory`
B) Use Mesos constraints to isolate Spark to non-Cassandra nodes or mount separate physical disks.
C) Increase `spark.cores.max`
D) Enable Dynamic Allocation
**Answer:** B
**Mastery Explanation:** Mesos does not isolate disk I/O natively like it does CPU/RAM via cgroups. Co-locating I/O heavy frameworks causes contention. Constraints ensure physical isolation of workloads.

**Q39: Scenario:** `spark.dynamicAllocation.minExecutors=0`.
**Twist:** You change it to `10`.
A) Spark will never relinquish its last 10 executors back to Mesos, holding those resources even when idle.
B) Mesos Master will reserve 10 executors for other frameworks.
C) The job fails if exactly 10 nodes aren't available.
D) The external shuffle service stores 10 copies of data.
**Answer:** A
**Mastery Explanation:** Setting a minimum ensures that Dynamic Allocation scales down to a floor, but never below it, guaranteeing base capacity at the cost of hoarding cluster resources when idle.

**Q40: Scenario:** You run Catalyst transformations aggressively pushing down filters.
**Twist:** You remove the filters. Job runtime triples. Why?
A) Mesos allocated fewer cores.
B) The volume of data entering the shuffle phase exploded, congesting the shared Mesos network since dedicated bandwidth isn't guaranteed.
C) The Docker containerizer crashed.
D) DRF penalized the job.
**Answer:** B
**Mastery Explanation:** Because Mesos environments fiercely contest network bandwidth, pushing down filters is critical to minimize data volume before the shuffle phase.

## 4. Coding & Debugging Questions

**Q41: Debug this configuration:**
```scala
val conf = new SparkConf()
  .setAppName("FaultyMesosApp")
  .setMaster("mesos://master:5050")
  .set("spark.executor.memory", "32g")
  .set("spark.executor.memoryOverhead", "384m")
```
**Error:** Job fails with Exit 137 during a heavy Parquet read. What is the bug?
**Answer:** The `spark.executor.memoryOverhead` is vastly under-provisioned (only ~384MB for a 32GB heap).
**Mastery Explanation:** Tungsten's vectorized Parquet reader uses extensive off-heap memory. 384MB is easily breached, triggering the Linux cgroup OOM killer. It should be padded to at least 10-20% of the heap (e.g., 3-6GB).

**Q42: Identify the logic error:**
```scala
val conf = new SparkConf()
  .set("spark.dynamicAllocation.enabled", "true")
  .set("spark.shuffle.service.enabled", "false")
```
**Answer:** Enabling dynamic allocation without the external shuffle service.
**Mastery Explanation:** When Mesos scales down executors to satisfy DRF, local shuffle data is destroyed because `shuffle.service.enabled` is false. This leads to continuous `FetchFailedExceptions` and massive DAG recomputations.

**Q43: What is wrong with this Docker configuration on Mesos?**
```scala
val conf = new SparkConf()
  .set("spark.mesos.executor.docker.image", "my-spark:v1")
  .set("spark.mesos.executor.docker.volumes", "/var/lib/docker/overlay2:/tmp:rw")
```
**Answer:** Mapping the host's Docker overlay filesystem to the container's `/tmp`.
**Mastery Explanation:** Spark writes shuffle files to `/tmp`. Pointing this back to the Docker overlay filesystem defeats the purpose of volume mapping and completely crushes disk I/O. It should map to a raw host path (e.g., `/data/nvme`).

**Q44: Why does this app never launch executors?**
```scala
val conf = new SparkConf()
  .setAppName("StarvingApp")
  .set("spark.cores.max", "10000")
  .set("spark.mesos.coarse", "true")
// Cluster has 5000 cores total, highly contested.
```
**Answer:** Offer Starvation due to impossibly high `spark.cores.max` expectation.
**Mastery Explanation:** The framework will continually reject small resource offers while waiting to fulfill the massive 10,000 core request, causing a deadlock.

**Q45: Fix the hardware constraint bug:**
```scala
val conf = new SparkConf()
  .set("spark.mesos.constraints", "gpu_type:nvidia,rack:1a")
```
// Intended to require BOTH nvidia GPUs AND rack 1a.
**Answer:** The syntax for multiple constraints is typically semi-colon separated in Mesos, or provided as a list depending on Spark version, but comma separation often maps to a single invalid key-value pair.
**Mastery Explanation:** Mesos constraint parsing requires strict formatting (usually `attribute:value;attribute2:value2`). A malformed constraint string results in zero matching offers, causing the job to hang indefinitely in the `ACCEPTED` state.

**Q46: Identify the serialization bottleneck:**
```scala
val conf = new SparkConf()
  .set("spark.executor.memory", "16g")
  .set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
```
**Answer:** Using `JavaSerializer` instead of `KryoSerializer`.
**Mastery Explanation:** In a Mesos environment where network bandwidth is shared and fiercely contested, Java serialization's huge byte footprint causes severe network bottlenecks during RPCs and shuffles.

**Q47: Why does this dynamic allocation config fail to scale up quickly?**
```scala
val conf = new SparkConf()
  .set("spark.dynamicAllocation.enabled", "true")
  .set("spark.shuffle.service.enabled", "true")
  .set("spark.dynamicAllocation.schedulerBacklogTimeout", "300s")
```
**Answer:** The backlog timeout is set to 5 minutes (300s).
**Mastery Explanation:** Spark will wait 5 minutes with pending tasks before requesting additional executors from the Mesos Master. This causes agonizingly slow scale-up. It should be typically 1-5 seconds.

**Q48: Debug this memory config:**
```scala
val conf = new SparkConf()
  .set("spark.executor.memory", "8g")
  // default memoryOverhead is 10% (800MB)
```
// The cgroup limit is 8.8GB. The JVM heap is 8GB.
**Answer:** The JVM heap and off-heap leave virtually no room for OS overhead or Tungsten.
**Mastery Explanation:** By default, Spark allocates `spark.executor.memory` entirely to the JVM heap. With only 800MB left for off-heap, network buffers, thread stacks, and Tungsten, the cgroup limit will be breached almost instantly under load.

**Q49: What is the issue with this Mesos connection string?**
```scala
val conf = new SparkConf()
  .setMaster("mesos://10.0.0.1:5050,10.0.0.2:5050")
```
**Answer:** Missing Zookeeper integration for High Availability.
**Mastery Explanation:** Hardcoding Mesos master IPs fails if the leading master changes. The robust format uses Zookeeper: `mesos://zk://10.0.0.1:2181,10.0.0.2:2181/mesos` to allow the driver to dynamically find the elected master.

**Q50: Identify the DRF abuse:**
```scala
val conf = new SparkConf()
  .set("spark.executor.cores", "1")
  .set("spark.executor.memory", "100g")
```
**Answer:** Requesting extreme memory-to-core ratios distorts the DRF calculation.
**Mastery Explanation:** Requesting 100GB of RAM per 1 CPU core makes memory the massive dominant resource. DRF will severely penalize this framework, offering it very few resources because its dominant share climbs extraordinarily fast per executor launched.
