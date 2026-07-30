# Master Class Assessment: Job and Resource Scheduling in Apache Spark

## Section 1: True/False Questions

**1. True/False:** When Dynamic Resource Allocation (DRA) scales down an executor, all shuffle files stored on that executor's local disk are immediately lost unless an External Shuffle Service (ESS) or Shuffle Tracking is enabled.
* **Answer:** True
* **Mastery Explanation:** Spark tasks write intermediate shuffle data to local disk. If an executor is terminated by DRA for being idle, that data goes with it. The External Shuffle Service (or Spark 3.x Shuffle Tracking) detaches the lifecycle of shuffle data from the executor JVM, allowing downstream reducers to fetch it even after the executor is decommissioned.

**2. True/False:** Spark's `ResourceProfileBuilder` allows you to dynamically resize the JVM heap memory of an active executor mid-stage if a task requires more memory.
* **Answer:** False
* **Mastery Explanation:** Executor JVM heaps are fixed at launch. ResourceProfiles in Spark 3+ allow you to request entirely new executor configurations (shapes) from the cluster manager (like YARN or K8s) or restrict which tasks run on which existing executors based on task-level resources (like GPUs), but they cannot change the JVM heap of a running executor.

**3. True/False:** The FAIR scheduler allows preemption at the task level, meaning if a high-priority pool is starved, Spark will kill running tasks in a low-priority pool to free up CPU cores.
* **Answer:** False
* **Mastery Explanation:** Spark scheduling is non-preemptive. The FAIR scheduler multiplexes resources by assigning newly freed cores to starved pools, but it will never kill an actively running task to achieve fairness. 

**4. True/False:** Setting `spark.locality.wait` to `0s` ensures maximum CPU utilization by forcing tasks to immediately launch on any available executor, sacrificing data locality (e.g., `PROCESS_LOCAL`) to avoid idle cores.
* **Answer:** True
* **Mastery Explanation:** Delay scheduling waits for a localized executor to become free. A wait time of `0s` bypasses this delay entirely. The scheduler instantly degrades to `ANY` locality, maximizing CPU utilization but potentially incurring heavy network I/O.

**5. True/False:** Speculative Execution (`spark.speculation`) is an effective mechanism for mitigating Out-Of-Memory (OOM) errors caused by data skew in a single task.
* **Answer:** False
* **Mastery Explanation:** Speculation strictly monitors task *duration*, not memory usage. If a task fails due to OOM, the speculative copy will process the exact same skewed data partition and also fail. Speculation is for mitigating hardware/network stragglers, not logical data skew or memory bloat.

**6. True/False:** A Spark application configured with `spark.scheduler.mode=FAIR` applies FAIR scheduling rules across all concurrent applications running on the YARN cluster.
* **Answer:** False
* **Mastery Explanation:** `spark.scheduler.mode` only configures the micro-level task/job scheduling *within* a single SparkContext. Macro-level scheduling across different applications is handled by the Cluster Manager (e.g., YARN Capacity/Fair Scheduler).

**7. True/False:** If `spark.dynamicAllocation.schedulerBacklogTimeout` is reached, Spark requests a new executor. If the backlog persists, subsequent requests grow exponentially.
* **Answer:** True
* **Mastery Explanation:** To avoid overwhelming the cluster manager but quickly satisfy large backlogs, DRA requests executors exponentially (1, 2, 4, 8...). It scales up rapidly but scales down gracefully.

**8. True/False:** Applying `withResources()` on an RDD breaks the Catalyst Optimizer's ability to perform predicate pushdown and whole-stage code generation on that specific data flow.
* **Answer:** True
* **Mastery Explanation:** `withResources()` is an RDD API feature. Moving from the DataFrame/Dataset API to RDDs bypasses Catalyst entirely. You lose logical optimizations, Tungsten memory management, and physical planning for that segment of the pipeline.

**9. True/False:** Setting `spark.task.resource.gpu.amount` to `0.5` allows exactly two tasks to run concurrently on a single GPU.
* **Answer:** True
* **Mastery Explanation:** Fractional task resources are supported. A requirement of 0.5 means each task consumes half a GPU, perfectly allowing 2 tasks to multiplex a single GPU, which is useful for lightweight ML inference.

**10. True/False:** Under FIFO scheduling (the default), if Job A is submitted before Job B, Job B cannot start executing any tasks until every single task in Job A has completed.
* **Answer:** False
* **Mastery Explanation:** FIFO means Job A gets *priority* for all resources it requests. However, if Job A does not need all the cluster's CPU cores (e.g., it only has 10 tasks but there are 100 cores), Job B's tasks will utilize the remaining 90 idle cores.

## Section 2: Multiple Choice Questions

**11. Which internal Spark component is directly responsible for evaluating data locality preferences (`PROCESS_LOCAL`, `NODE_LOCAL`, etc.) and assigning tasks to specific Executors?**
A) Catalyst Optimizer
B) DAGScheduler
C) TaskScheduler
D) BlockManager
* **Answer:** C
* **Mastery Explanation:** The DAGScheduler splits the logical plan into Stages and computes optimal locations based on RDD lineage, but the *TaskScheduler* actually assigns the tasks to executors and implements Delay Scheduling to honor those locality preferences.

**12. When configuring a FAIR scheduler pool in `fairscheduler.xml`, which parameter determines the behavior when a pool falls below its minimum guaranteed resources?**
A) weight
B) minShare
C) schedulingMode
D) preemptionTimeout
* **Answer:** B
* **Mastery Explanation:** `minShare` sets the minimum CPU cores a pool is guaranteed. If active pools are below their `minShare`, Spark allocates available resources to them first before looking at `weight`.

**13. In a Kubernetes environment without an External Shuffle Service, what is the safest way to enable Dynamic Resource Allocation (DRA)?**
A) Mount a distributed file system (e.g., HDFS) to `/tmp`
B) Enable Spark 3.0+ Dynamic Shuffle Tracking (`spark.dynamicAllocation.shuffleTracking.enabled`)
C) Increase the `executorIdleTimeout` to 24 hours
D) Set `spark.shuffle.compress` to false
* **Answer:** B
* **Mastery Explanation:** Spark 3.0 introduced shuffle tracking, which prevents DRA from decommissioning executors that hold active shuffle data, eliminating the strict dependency on the ESS in containerized environments like K8s.

**14. What occurs when `spark.speculation.quantile` is set to `0.9` and `spark.speculation.multiplier` is set to `2.0`?**
A) Speculation checks start after 90% of the time has elapsed, duplicating tasks twice.
B) Tasks are speculated if they take 90% longer than 2.0x the average task.
C) Spark waits for 90% of tasks in a stage to finish successfully, then speculates any remaining task taking 2x longer than the median time.
D) Spark kills 10% of the slowest tasks and restarts them with 2.0x resources.
* **Answer:** C
* **Mastery Explanation:** The `quantile` (0.9) means 90% of the tasks must succeed before speculation heuristics are even evaluated. The `multiplier` (2.0) checks if the remaining tasks are taking twice as long as the median of the completed ones.

**15. If an executor is lost due to an OOM error during a shuffle map stage, how does the DAGScheduler react?**
A) It marks the entire Job as failed immediately.
B) It triggers a `FetchFailedException` in the downstream reduce stage, forcing the re-execution of the lost map tasks.
C) The TaskScheduler silently masks the failure by reading the shuffle data from a backup node.
D) It re-runs the Catalyst optimizer to generate a memory-efficient physical plan.
* **Answer:** B
* **Mastery Explanation:** Executors write shuffle data locally. If one dies, downstream reducers cannot fetch the data, throwing a `FetchFailedException`. The DAGScheduler catches this, resubmits the missing map tasks (often as a new attempt for the stage), and then resumes the reduce tasks.

**16. Which locality level indicates that a task is running on an executor on the same physical machine as the data, but the data is NOT cached in the executor's JVM memory?**
A) PROCESS_LOCAL
B) NODE_LOCAL
C) RACK_LOCAL
D) ANY
* **Answer:** B
* **Mastery Explanation:** `PROCESS_LOCAL` means the data is in the executor's BlockManager (memory). `NODE_LOCAL` means the data is on the same machine (e.g., local HDFS DataNode or OS page cache), but requires IPC or local disk I/O to read into the executor JVM.

**17. What is the primary purpose of the `Barrier Execution Mode` introduced in Spark?**
A) To isolate concurrent jobs in completely separate JVMs.
B) To prevent network shuffles from crossing rack boundaries.
C) To gang-schedule all tasks in a stage simultaneously, ensuring they all start at the same time for distributed Deep Learning training (e.g., MPI/Horovod).
D) To block speculative execution on GPU-bound workloads.
* **Answer:** C
* **Mastery Explanation:** Distributed ML frameworks require all worker processes to communicate synchronously. If one task fails, they all must restart. Barrier Execution Mode ensures all tasks in a stage are launched concurrently (gang scheduling) rather than independently.

**18. In Spark's memory architecture, what component manages the off-heap execution memory used heavily by Tungsten for sorting and aggregations?**
A) The Cluster Manager (YARN/K8s)
B) Spark's internal MemoryManager (Unified Memory Management)
C) Java Garbage Collector
D) The External Shuffle Service
* **Answer:** B
* **Mastery Explanation:** Since Spark 1.6, the Unified Memory Manager governs both Execution and Storage memory. Tungsten leverages it to allocate off-heap memory (if configured) directly via `sun.misc.Unsafe`, bypassing the JVM GC for execution buffers.

**19. How do you assign a specific DataFrame action (e.g., `df.count()`) to a FAIR scheduler pool named "ad-hoc" in PySpark?**
A) `df.withPool("ad-hoc").count()`
B) `spark.config("spark.scheduler.pool", "ad-hoc"); df.count()`
C) `spark.sparkContext.setLocalProperty("spark.scheduler.pool", "ad-hoc"); df.count()`
D) `df.count(pool="ad-hoc")`
* **Answer:** C
* **Mastery Explanation:** Scheduler pools are thread-local properties. You must use `setLocalProperty` on the `SparkContext` in the thread that triggers the Spark Action.

**20. A Spark job with DRA enabled is processing a massive dataset. The backlog timeout triggers, requesting 100 new executors. The cluster only has capacity for 20. What happens?**
A) The Spark application crashes with an `InsufficientResourcesException`.
B) Spark provisions the 20 executors, and the remaining 80 requests sit in a pending state with the Cluster Manager.
C) Spark automatically scales down the executor memory requirements by 80% to fit 100 executors.
D) The TaskScheduler degrades the locality wait to 0s to compensate.
* **Answer:** B
* **Mastery Explanation:** DRA sends the request to the cluster manager (YARN/K8s). The cluster manager fulfills what it can. The outstanding requests remain queued and will be fulfilled as cluster resources become available.

**21. When tuning Delay Scheduling, what does `spark.locality.wait.rack` govern?**
A) The time Spark waits for a free core on the same rack before degrading to ANY.
B) The time Spark waits for a rack to power on.
C) The maximum latency tolerated when fetching shuffle data across racks.
D) The time Spark waits to switch from NODE_LOCAL to RACK_LOCAL.
* **Answer:** A
* **Mastery Explanation:** Delay scheduling is a cascading fallback. `spark.locality.wait.rack` specifically defines how long to wait for a slot on the same rack before completely giving up on locality and assigning the task to `ANY` node in the cluster.

**22. You have a long-running streaming query and an hourly batch job in the same SparkContext. The streaming query uses the default pool. The batch job is assigned to a FAIR pool. Both pools have `weight=1` and `minShare=0`. What is the outcome?**
A) The streaming query gets 100% of resources because the default pool always has strict priority.
B) They share resources equally.
C) The batch job is queued until the streaming job stops.
D) Spark throws an exception because streaming and batch cannot share a Context.
* **Answer:** B
* **Mastery Explanation:** Under FAIR scheduling mode, if pools have equal weights and no minShares, the scheduler uses a round-robin approach to distribute active tasks, achieving roughly a 50/50 CPU split between the two pools.

**23. Why is Speculative Execution generally discouraged for jobs that perform writes directly to external databases (e.g., JDBC sink)?**
A) Databases cannot handle the connection overhead.
B) It causes Spark's DAG to deadlock.
C) Speculative execution can cause duplicate data writes if the external sink does not support idempotent operations or transactional commits.
D) It forces the JDBC driver to serialize the entire dataset.
* **Answer:** C
* **Mastery Explanation:** Speculation runs identical copies of a task. If task A and speculative task A' both execute an `INSERT` statement, and both succeed before the scheduler can kill the slower one, duplicate records are committed unless the sink is perfectly idempotent.

**24. Which of the following best describes how Spark discovers GPU resources on a worker node?**
A) Spark natively queries the NVIDIA NVML library.
B) The user must provide a discovery script (e.g., a bash script) configured via `spark.worker.resource.gpu.discoveryScript` that outputs JSON mapping GPU addresses.
C) The DAGScheduler assigns random GPU IDs.
D) GPUs are automatically abstracted as CPU cores by YARN.
* **Answer:** B
* **Mastery Explanation:** Spark relies on user-provided or environment-provided discovery scripts that run on startup. These scripts detect the hardware and output a JSON array of addresses (e.g., `["0", "1"]`), which Spark then manages.

**25. If an executor is decommissioned by DRA because of the `executorIdleTimeout`, what happens to the cached (persisted) RDD blocks stored in its memory?**
A) They are automatically migrated to other active executors.
B) They are written to the External Shuffle Service.
C) They are permanently lost, and any future tasks needing them will recompute them from the lineage.
D) The executor refuses to shut down.
* **Answer:** C
* **Mastery Explanation:** Unless Block Replication (`StorageLevel.MEMORY_ONLY_2`) or explicit RDD migration (in newer Spark versions under specific decommission flags) is configured, standard decommissioning simply kills the executor and its memory cache. Downstream tasks requiring those blocks will trigger a recomputation via the DAG's lineage.

## Section 3: "Small Twist" Questions

**26. Scenario:** You set `spark.locality.wait = 3s`. 
**Twist A:** Data is in HDFS, cached on Node X. Node X has 0 free cores. Spark waits 3s, then runs the task on Node Y.
**Twist B:** Data is loaded via JDBC from an external Postgres database. Node X has 0 free cores. How long does Spark wait for Node X before running on Node Y?
* **Answer:** 0 seconds (Immediate degradation to ANY).
* **Mastery Explanation:** JDBC data sources do not expose data locality information to Spark (unlike HDFS which provides block locations). All JDBC tasks instantly resolve to `ANY` locality, completely bypassing Delay Scheduling timeouts.

**27. Scenario:** You have two FAIR pools: Pool A (`weight=2`) and Pool B (`weight=1`).
**Twist A:** Both pools have 100 pending tasks. Pool A gets ~66% of the cores.
**Twist B:** You change Pool A to `weight=100` and Pool B to `weight=1`. How many cores does Pool B get if the cluster has 100 cores?
* **Answer:** ~1 core.
* **Mastery Explanation:** The FAIR scheduler divides resources proportionally based on weight if no `minShare` is specified. With a ratio of 100:1 on a 100-core cluster, Pool A gets ~99 cores, and Pool B gets ~1 core. 

**28. Scenario:** You enable DRA (`dynamicAllocation.enabled=true`).
**Twist A:** Spark 2.4. You forget to configure ESS. The application fails to start or throws an error.
**Twist B:** Spark 3.2. You forget to configure ESS, but you enable `spark.dynamicAllocation.shuffleTracking.enabled=true`. What happens to idle executors?
* **Answer:** In Twist B, executors are allowed to scale down, *except* those that are storing active shuffle files which have not been fully consumed yet.
* **Mastery Explanation:** Shuffle Tracking allows safe decommissioning without an ESS by smartly keeping executors alive only if they hold unconsumed shuffle data, whereas Spark 2.x would strictly require an ESS or disable DRA entirely.

**29. Scenario:** A stage has 100 tasks. 
**Twist A:** `spark.speculation.quantile = 0.75`. Speculation begins evaluating after 75 tasks finish.
**Twist B:** `spark.speculation.quantile = 1.0`. When does speculation evaluation begin?
* **Answer:** Never (effectively).
* **Mastery Explanation:** Setting the quantile to 1.0 means 100% of tasks must complete before Spark considers speculating. If 100% are complete, the stage is finished. Therefore, 1.0 completely disables speculation.

**30. Scenario:** You configure task GPUs.
**Twist A:** `spark.task.resource.gpu.amount = 1`. 1 task per GPU.
**Twist B:** `spark.task.resource.gpu.amount = 0.33`. How many tasks run concurrently on a single GPU?
* **Answer:** 3 tasks.
* **Mastery Explanation:** Spark divides the GPU address by the fractional amount and takes the floor. 1 / 0.33 = 3 tasks per GPU. 

**31. Scenario:** You configure `minShare` for your FAIR pools.
**Twist A:** Pool A has `minShare=10`. It receives 10 cores immediately when tasks queue up.
**Twist B:** Pool A has `minShare=10`, but the entire Spark application was only granted 5 cores by YARN. How many cores does Pool A get?
* **Answer:** 5 cores.
* **Mastery Explanation:** Micro-level scheduling (TaskScheduler) cannot exceed macro-level allocations (YARN). The `minShare` is a soft guarantee bounded by the physical resources granted to the SparkContext.

**32. Scenario:** An executor goes idle.
**Twist A:** `executorIdleTimeout=60s`. Executor dies after 60s.
**Twist B:** The executor computed a cached RDD block (`df.cache()`). `spark.dynamicAllocation.cachedExecutorIdleTimeout` is set to `infinity`. What happens?
* **Answer:** The executor is never decommissioned.
* **Mastery Explanation:** Spark distinguishes between completely idle executors and executors holding cached data. If `cachedExecutorIdleTimeout` is infinity (the default in some distributions), DRA will never kill an executor storing cached blocks to prevent expensive recomputations.

**33. Scenario:** Delay scheduling is active.
**Twist A:** `spark.locality.wait = 3s`. Fallback is PROCESS -> NODE -> RACK -> ANY. Max total wait is 9 seconds.
**Twist B:** You set `spark.locality.wait.node = 0s`. How does the fallback behave?
* **Answer:** Spark waits 3s for PROCESS_LOCAL. If it fails, it instantly skips NODE_LOCAL (0s wait) and immediately begins waiting 3s for RACK_LOCAL. 
* **Mastery Explanation:** Specific locality waits override the base `spark.locality.wait`. Setting one to 0s explicitly removes that rung from the delay scheduling ladder.

**34. Scenario:** A task is reading a heavily skewed partition.
**Twist A:** The task takes 10 minutes. No speculation is enabled. Stage takes 10 minutes.
**Twist B:** The task takes 10 minutes. Speculation is enabled with `multiplier=1.5`. The median time of other tasks is 1 minute. What is the stage duration?
* **Answer:** Still ~10 minutes (or slightly longer).
* **Mastery Explanation:** Speculation launches a *duplicate* task. Since the skew is in the *data* (the partition itself is massive), the speculative task will process the exact same massive partition. Both tasks will take ~10 minutes. Speculation does not fix data skew.

**35. Scenario:** Two Spark Actions are called asynchronously.
**Twist A:** You use Python's `threading.Thread` to launch them. Both use the default pool. They run sequentially.
**Twist B:** You set `spark.scheduler.mode=FAIR`, use `threading.Thread`, but *forget* to set `setLocalProperty("spark.scheduler.pool", "my_pool")`. What happens?
* **Answer:** Both queries run concurrently in the `default` FAIR pool.
* **Mastery Explanation:** If no pool is explicitly set, tasks default to the `default` pool. Since `scheduler.mode` is FAIR, the default pool itself schedules jobs in a round-robin FAIR manner, allowing concurrent execution even without custom pools.

**36. Scenario:** Executor memory configuration.
**Twist A:** You request `spark.executor.memory=4g`.
**Twist B:** You request `spark.executor.memory=4g` AND `spark.executor.memoryOverhead=2g` in a YARN cluster. What is the total container size requested from YARN?
* **Answer:** 6 GB.
* **Mastery Explanation:** The Cluster Manager allocates containers based on the sum of `executor.memory` (JVM Heap) and `memoryOverhead` (Off-heap, OS network buffers, Python processes). YARN will kill the container if its total footprint exceeds 6GB.

**37. Scenario:** Using ResourceProfiles.
**Twist A:** You apply `withResources(profile)` to an RDD map partition logic.
**Twist B:** You attempt to apply `withResources(profile)` to a DataFrame `groupBy().count()`. What happens?
* **Answer:** Compilation or Runtime Error.
* **Mastery Explanation:** As of Spark 3.x, `ResourceProfiles` are strictly exposed via the RDD API. You cannot apply them directly to DataFrame/SQL operations. You must convert to RDD, apply resources, and convert back if necessary.

**38. Scenario:** External Shuffle Service (ESS) port binding.
**Twist A:** You run 1 Spark App on a worker node. ESS binds to port 7337.
**Twist B:** You run 5 different Spark Apps on the same worker node. How many ESS instances run on this node?
* **Answer:** 1.
* **Mastery Explanation:** The ESS is a *Node-level* daemon, not an application-level service. A single ESS instance manages the shuffle files for all executors across all Spark applications running on that physical worker machine.

**39. Scenario:** FetchFailedException occurs.
**Twist A:** A map task is lost, FetchFailedException occurs, DAGScheduler resubmits the map task.
**Twist B:** The resubmitted map task fails again, causing a second FetchFailedException, up to 4 times (default `spark.stage.maxConsecutiveAttempts`). What happens?
* **Answer:** The entire Spark Application is aborted.
* **Mastery Explanation:** If a stage fails due to FetchFailedExceptions 4 consecutive times, the DAGScheduler assumes unrecoverable cluster corruption or systemic failure and fails the entire job/application.

**40. Scenario:** Configuring `minExecutors`.
**Twist A:** DRA is off. `spark.executor.instances=10`. You get 10 executors.
**Twist B:** DRA is on. `spark.dynamicAllocation.minExecutors=10`, but you also leave `spark.executor.instances=20` in the spark-submit arguments. How many executors does the application start with?
* **Answer:** 20 executors.
* **Mastery Explanation:** `spark.executor.instances` overrides the initial allocation. DRA will start with 20 executors. It will only scale down to a minimum of 10 if they become idle, but the initial boot will request 20.

## Section 4: Coding & Debugging Questions

**41. Identify the Bug:**
```python
spark.conf.set("spark.scheduler.mode", "FAIR")
def run_query():
    spark.sparkContext.setLocalProperty("spark.scheduler.pool", "fast_pool")
    df.count()

import threading
t = threading.Thread(target=run_query)
t.start()
```
* **Answer/Mastery Explanation:** `spark.conf.set()` does not change the scheduler mode at runtime. `spark.scheduler.mode` is a static configuration that MUST be set during the `SparkSession.builder...getOrCreate()` phase or in `spark-defaults.conf`. Changing it dynamically has no effect; the cluster will silently remain in FIFO mode.

**42. Identify the Performance Blocker:**
```python
spark = SparkSession.builder.config("spark.locality.wait", "60s").getOrCreate()
df = spark.read.parquet("hdfs://...")
df.filter(df.id > 100).count()
```
* **Answer/Mastery Explanation:** Setting `spark.locality.wait` to a massive 60 seconds will cause severe performance degradation if the node containing the HDFS data block is busy. The TaskScheduler will literally sit idle for a full minute, doing zero work, waiting for a core on that specific node to free up, instead of just transferring the data over the network (which takes milliseconds).

**43. Identify the Resource Leak:**
A user submits a Spark job with `spark.dynamicAllocation.enabled=true`. They are processing data using a PySpark UDF that relies on a heavy C++ library. After 5 minutes, DRA scales down 10 executors. Shortly after, the job crashes with YARN killing containers due to memory limits.
* **Answer/Mastery Explanation:** The PySpark UDF executes in Python worker processes, completely outside the JVM heap. When DRA scales executors down, it doesn't clean up zombie Python processes if the OS handles them poorly. Furthermore, the memory overhead calculation likely didn't account for the massive C++ library. The user needs to drastically increase `spark.executor.memoryOverhead` to prevent YARN from killing the remaining executors.

**44. Debug the Logical Error in Speculation:**
```scala
spark.conf.set("spark.speculation", "true")
rdd.foreachPartition { partition =>
  val dbConnection = getDbConnection()
  partition.foreach(row => dbConnection.execute(s"INSERT INTO table VALUES (${row.id})"))
}
```
* **Answer/Mastery Explanation:** Speculation launches duplicate tasks. If a task is slow, a second task will start executing the exact same `INSERT` statements. If both tasks finish at roughly the same time, or if one commits partially before being killed, duplicate rows are inserted into the database. External writes with speculation require idempotent operations (e.g., `UPSERT` or staging tables).

**45. Fix the DRA Configuration:**
```properties
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=10
spark.dynamicAllocation.schedulerBacklogTimeout=100ms
```
* **Symptom:** The cluster manager is being DDOS'd with executor requests.
* **Answer/Mastery Explanation:** `schedulerBacklogTimeout=100ms` is aggressively low. Spark will request new executors almost instantly for every minor queue of tasks. It should be set to a reasonable duration (e.g., `1s` or `5s`) to allow existing executors a chance to finish micro-tasks before begging the cluster manager for more hardware.

**46. Debug the Locality Issue:**
You look at the Spark UI for a massive Join operation. All tasks in the shuffle read stage show a locality of `ANY`.
* **Answer/Mastery Explanation:** This is normal and not a bug. After a shuffle, data is distributed across the cluster based on the partitioner (Hash/Range). Because reduce tasks must pull data from *all* map tasks across *all* nodes, there is no single preferred location. Thus, reduce tasks naturally have a locality of `ANY`.

**47. Write the snippet:**
Write the exact Python PySpark syntax to clear the local thread property so that subsequent queries in the same thread fall back to the default scheduler pool.
* **Answer/Mastery Explanation:**
```python
spark.sparkContext.setLocalProperty("spark.scheduler.pool", None)
```
Setting it to `None` removes the thread-local override, returning the thread's scheduling routing back to the default pool.

**48. Identify the misconfiguration in GPU scheduling:**
```python
spark = SparkSession.builder \
    .config("spark.executor.resource.gpu.amount", "1") \
    .config("spark.task.resource.gpu.amount", "2") \
    .getOrCreate()
```
* **Answer/Mastery Explanation:** The config requests 1 GPU per executor, but demands 2 GPUs per task. A task can never be scheduled because its resource requirements exceed the maximum capacity of a single executor. Tasks will remain in a `PENDING` state indefinitely.

**49. Debug the Straggler Heuristic:**
`spark.speculation.multiplier=10.0`, `spark.speculation.quantile=0.5`. 
Symptom: A task takes 1 hour while the others take 2 minutes, but speculation never triggers.
* **Answer/Mastery Explanation:** The `multiplier` is way too high. The median time is 2 minutes. For speculation to trigger, a task must take 2 x 10.0 = 20 minutes. While it eventually will trigger at the 20-minute mark, you wasted 18 minutes of idle time. The multiplier should be closer to `1.5` or `2.0` for rapid straggler mitigation.

**50. Identify the cause of "Lost Executor" logs during normal DRA operation:**
You are using Spark on Kubernetes. You see standard logs showing executors exiting with `Exit Code 137` every few minutes, but job progress is fine.
* **Answer/Mastery Explanation:** Exit Code 137 is a SIGKILL. When DRA decides to decommission an executor (scale down), it tells K8s to delete the pod. Kubernetes sends a SIGTERM, and if it doesn't exit fast enough, a SIGKILL (137). Because DRA requested this scale down, the DAGScheduler expects it and ignores the "lost" executor, meaning this is completely normal operational logging for dynamic scale-down.
