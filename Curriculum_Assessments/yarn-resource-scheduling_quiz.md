# YARN Resource Scheduling - Assessment Quiz

## Part 1: True/False Questions

1. **Question**: Spark's Dynamic Resource Allocation on YARN always requires an external shuffle service to be enabled to prevent shuffle data loss when executors are scaled down.
**Answer**: False.
**Mastery Explanation**: While historically true, Spark 3.0 introduced shuffle tracking (`spark.dynamicAllocation.shuffleTracking.enabled`), which allows Spark to safely scale down executors without an external shuffle service by tracking which executors hold active shuffle data.

2. **Question**: In YARN cluster mode, the driver runs inside the ApplicationMaster, meaning `spark.driver.memory` dictates the AM's container size.
**Answer**: True.
**Mastery Explanation**: In cluster mode, the driver runs within the ApplicationMaster container on a worker node. The memory requested for this container from YARN is driven by `spark.driver.memory` plus `spark.driver.memoryOverhead`.

3. **Question**: Tungsten's off-heap memory allocation (`spark.memory.offHeap.enabled`) is automatically accounted for within `spark.executor.memory`.
**Answer**: False.
**Mastery Explanation**: Off-heap memory is outside the JVM heap (`spark.executor.memory`). It must be explicitly configured and accounted for in `spark.yarn.executor.memoryOverhead` (or `spark.memory.offHeap.size` in newer versions, which adds to the total container size requested from YARN).

4. **Question**: Setting `spark.executor.memory` higher than `yarn.scheduler.maximum-allocation-mb` will cause the Spark application to hang in the ACCEPTED state indefinitely.
**Answer**: False.
**Mastery Explanation**: The application will fail immediately at submission time with an `IllegalArgumentException` or similar YARN rejection, rather than hanging in the ACCEPTED state.

5. **Question**: The Catalyst optimizer's physical planning phase evaluates YARN's current resource queue availability to choose between a SortMergeJoin and a BroadcastHashJoin.
**Answer**: False.
**Mastery Explanation**: Catalyst optimization is completely decoupled from the cluster manager (YARN). It uses table statistics and configurations (like `spark.sql.autoBroadcastJoinThreshold`) to choose physical plans, not real-time YARN queue states.

6. **Question**: YARN's Fair Scheduler can preempt Spark executor containers if a higher priority queue demands resources, potentially causing task recomputation.
**Answer**: True.
**Mastery Explanation**: YARN supports container preemption to satisfy fair share guarantees. Spark is resilient to this and will simply recompute lost tasks on other executors, though it impacts performance.

7. **Question**: If a Spark task suffers an OutOfMemoryError (OOM) in the JVM heap, YARN's NodeManager will report "Container killed for exceeding memory limits".
**Answer**: False.
**Mastery Explanation**: A JVM OOM causes the executor process to crash internally, which Spark reports as a JVM exit/failure. YARN's "Container killed" message happens when the *total physical memory* (including off-heap and overhead) exceeds the YARN container allocation limit, enforced by cgroups or OS polling.

8. **Question**: `spark.yarn.maxAppAttempts` limits the number of times the YARN ResourceManager will retry a failed Spark ApplicationMaster.
**Answer**: True.
**Mastery Explanation**: This controls AM retries. If the AM fails (e.g., due to a node crash), YARN can restart it up to this limit, allowing the Spark job to recover without failing the entire application.

9. **Question**: When dynamic allocation is enabled, `spark.dynamicAllocation.minExecutors` strictly guarantees that YARN will always grant at least this many executors immediately upon submission.
**Answer**: False.
**Mastery Explanation**: It dictates how many executors Spark *requests* as a minimum, but YARN only grants them if resources are actually available. If the cluster is full, the application may start with fewer or remain queued.

10. **Question**: In Spark 3.x, increasing YARN container sizes linearly increases the efficiency of garbage collection (GC) for all workloads.
**Answer**: False.
**Mastery Explanation**: Extremely large JVM heaps (e.g., > 64GB) can lead to massive GC pause times (Stop-The-World). It is often better to use moderately sized executors (e.g., 5 vCores, 16-32GB RAM) to balance parallel execution and GC overhead.

## Part 2: Multiple Choice Questions

11. **Question**: Which parameter determines the memory requested for the ApplicationMaster in YARN **client** mode?
A) spark.driver.memory
B) spark.yarn.am.memory
C) spark.executor.memory
D) spark.yarn.driver.memory
**Answer**: B
**Mastery Explanation**: In client mode, the driver runs on the submitting machine. The ApplicationMaster only orchestrates resources, so its memory is configured separately via `spark.yarn.am.memory` (default 512MB).

12. **Question**: Why is it typically recommended to set `spark.executor.cores` to 5 in a large YARN cluster, rather than 1 or 32?
A) 5 is the maximum supported by Hadoop YARN.
B) It strikes a balance between JVM concurrency overhead (HDFS client limits) and maximizing memory sharing without severe GC pauses.
C) Tungsten engine cannot compile code for more than 5 threads.
D) YARN NodeManagers reserve the remaining cores for the OS.
**Answer**: B
**Mastery Explanation**: Single-core executors fail to leverage shared JVM memory. Fat executors (>5 cores) suffer from HDFS I/O bottlenecks (concurrent connections) and severe garbage collection penalties. 5 vCores is the empirical sweet spot.

13. **Question**: When a Spark executor reads broadcast data, where is this data stored in the YARN container's memory model?
A) Off-heap memory exclusively.
B) The YARN NodeManager's local cache.
C) The execution memory region of the JVM heap.
D) The storage memory region of the JVM heap.
**Answer**: D
**Mastery Explanation**: Broadcast variables are cached in the Storage Memory region of the unified memory manager, competing with RDD/DataFrame caching.

14. **Question**: A Spark container is killed by YARN with exit code 137. Which memory configuration should you tune first?
A) spark.executor.memory
B) spark.yarn.executor.memoryOverhead
C) spark.driver.memory
D) spark.sql.shuffle.partitions
**Answer**: B
**Mastery Explanation**: Exit code 137 (SIGKILL) usually means the OS or YARN killed the container for exceeding its total physical memory footprint. Increasing `memoryOverhead` provides more room for non-heap structures like thread stacks, NIO buffers, and Python worker processes (if PySpark).

15. **Question**: What happens to Catalyst's `BroadcastHashJoin` if the executor container's overhead memory is too small during the broadcast phase?
A) The physical plan changes to SortMergeJoin dynamically.
B) The executor may be killed by YARN (OOM) due to high off-heap memory usage during Netty block transfers.
C) Catalyst ignores the broadcast threshold.
D) YARN dynamically expands the container memory.
**Answer**: B
**Mastery Explanation**: Broadcasting large tables uses Netty for network transfers, which heavily relies on off-heap DirectByteBuffers. If `memoryOverhead` is insufficient, YARN kills the container before the JVM throws an OOM.

16. **Question**: How does `spark.locality.wait` impact YARN scheduling?
A) It delays the YARN ResourceManager from allocating containers.
B) It tells Spark to wait before falling back to scheduling tasks on nodes that don't have the data locally.
C) It configures the timeout for connecting to the external shuffle service.
D) It delays garbage collection during shuffle reads.
**Answer**: B
**Mastery Explanation**: Task scheduling in Spark is data-aware. `spark.locality.wait` pauses task assignment to achieve better data locality (NODE_LOCAL vs ANY), balancing compute delay against network transfer costs.

17. **Question**: You have a 100-node YARN cluster, each with 16 vCores. You submit a job with `spark.executor.cores=16` and `spark.executor.instances=100`. Why might this perform poorly?
A) YARN cannot allocate 100 containers.
B) Catalyst Optimizer cannot handle 1600 partitions.
C) JVM GC pauses will be severe, and throughput per core will drop due to thread contention.
D) Spark requires an odd number of cores per executor.
**Answer**: C
**Mastery Explanation**: 16 cores per JVM leads to excessive thread contention (e.g., HDFS connections) and massive heap requirements, causing Stop-The-World GC pauses that devastate performance.

18. **Question**: Which Spark component is responsible for negotiating resources directly with YARN's ResourceManager?
A) DAGScheduler
B) TaskScheduler
C) ApplicationMaster
D) NodeManager
**Answer**: C
**Mastery Explanation**: The ApplicationMaster (running on a cluster node) negotiates resource containers from the ResourceManager, which it then uses to launch Executors via the NodeManagers.

19. **Question**: What is the primary role of the YARN External Shuffle Service in Spark?
A) To compress shuffle files.
B) To serve shuffle files to reducers even if the executor that wrote them has been shut down.
C) To bypass the Catalyst optimizer during shuffle reads.
D) To execute Tungsten code generation.
**Answer**: B
**Mastery Explanation**: The external shuffle service runs independently on the NodeManager. It serves shuffle data, allowing Spark to dynamically deallocate idle executors without losing the intermediate data they computed.

20. **Question**: If a Spark Application requests a container of 10GB, but `yarn.scheduler.minimum-allocation-mb` is 1024 and `yarn.scheduler.increment-allocation-mb` is 1024, what happens if Spark requests 10.5GB?
A) YARN denies the request.
B) YARN allocates exactly 10.5GB.
C) YARN allocates 11GB.
D) Spark crashes.
**Answer**: C
**Mastery Explanation**: YARN rounds up resource requests to the nearest multiple of `increment-allocation-mb` to prevent fragmentation. 10.5GB rounds up to 11GB.

21. **Question**: Which execution model guarantees that the Spark Driver will continue running even if the machine that submitted the job is disconnected?
A) Client Mode
B) Local Mode
C) Cluster Mode
D) Standalone Mode
**Answer**: C
**Mastery Explanation**: In Cluster mode, the driver runs inside the ApplicationMaster within the YARN cluster. The submitting client can safely disconnect.

22. **Question**: In PySpark on YARN, what primarily consumes the `spark.yarn.executor.memoryOverhead`?
A) Catalyst plan caching.
B) Tungsten row formats.
C) The Python worker processes executing UDFs.
D) Broadcast variables.
**Answer**: C
**Mastery Explanation**: PySpark spins up separate Python processes (workers) outside the JVM to run Python UDFs. These processes consume memory from the YARN container's overhead, not the JVM heap.

23. **Question**: How does YARN handle a NodeManager failure running an active Spark Executor?
A) YARN kills the entire Spark application.
B) The ApplicationMaster detects the loss, requests a new container, and the TaskScheduler recomputes lost tasks.
C) YARN automatically migrates the running JVM state to another node.
D) The Driver attempts to reconnect to the failed NodeManager indefinitely.
**Answer**: B
**Mastery Explanation**: Spark is fault-tolerant. The AM notifies the Driver (TaskScheduler) of the lost executor, asks YARN for a new one, and replays the lineage to recover lost RDD/DataFrame partitions.

24. **Question**: What does `spark.sql.shuffle.partitions` control in relation to YARN?
A) The number of YARN containers requested.
B) The number of reduce tasks, dictating how many concurrent executor threads will be utilized during a shuffle.
C) The amount of memory allocated for shuffle operations.
D) The number of NodeManagers involved in the job.
**Answer**: B
**Mastery Explanation**: It controls the number of partitions after wide transformations (joins/aggregations), which directly translates to the number of tasks in the next stage. This dictates how well the job utilizes the allocated YARN vCores.

25. **Question**: Which of the following is NOT managed by Tungsten's memory manager?
A) Shuffle buffers
B) Broadcast variables
C) Java Object heap allocations (e.g., standard String objects)
D) Execution memory for sorting
**Answer**: C
**Mastery Explanation**: Tungsten uses explicit memory management (unsafe arrays/off-heap) for tabular data (DataFrames/SQL). Standard Java objects created by user code bypass Tungsten and are subject to standard JVM GC.

## Part 3: "Small Twist" Questions

26. **Question**: You configure `spark.executor.memory=8g` and `spark.executor.cores=4`. A week later, you change to `spark.executor.cores=8` without changing memory. What happens to the memory per core?
**Answer**: It halves.
**Mastery Explanation**: The 8GB heap is now shared by 8 concurrent task threads instead of 4. Each task effectively gets 1GB instead of 2GB, highly increasing the risk of OOM errors during complex aggregations.

27. **Question**: In client mode, you set `spark.yarn.am.memory=4g` and `spark.driver.memory=8g`. You switch to cluster mode without changing configs. Which setting actually determines the driver container size now?
**Answer**: `spark.driver.memory`
**Mastery Explanation**: In cluster mode, the driver and the ApplicationMaster are the same JVM. Spark ignores `spark.yarn.am.memory` in cluster mode and uses `spark.driver.memory` to size the AM container.

28. **Question**: A job running fine suddenly starts failing with YARN container kills when you switch from a pure Spark SQL pipeline to one that heavily uses PySpark UDFs. What changed?
**Answer**: PySpark UDFs spawn Python processes outside the JVM.
**Mastery Explanation**: Python processes consume native OS memory (accounted in YARN overhead). Without increasing `spark.yarn.executor.memoryOverhead`, the total container memory exceeds YARN limits, triggering a kill.

29. **Question**: Your cluster has nodes with 64GB RAM. You request `spark.executor.memory=64g`. The application hangs in ACCEPTED. Why?
**Answer**: Container overhead makes the request exceed node capacity.
**Mastery Explanation**: A 64GB request plus the default 10% overhead (6.4GB) equals 70.4GB, which exceeds the 64GB physical capacity of the NodeManager (`yarn.scheduler.maximum-allocation-mb`), making it unschedulable.

30. **Question**: You enable `spark.dynamicAllocation.enabled=true` but forget to enable the shuffle service or shuffle tracking. What happens?
**Answer**: The application will fail to start.
**Mastery Explanation**: Spark performs a prerequisite check. If dynamic allocation is enabled without a mechanism to preserve shuffle data (external service or tracking), the SparkContext initialization throws an exception.

31. **Question**: You are doing a cross join on two massive datasets. Catalyst chooses a BroadcastNestedLoopJoin. YARN kills the driver. Why the driver, not the executor?
**Answer**: The driver collects the broadcast variable.
**Mastery Explanation**: To broadcast a dataset, the driver must first collect it into its own heap. If the dataset exceeds the driver's memory, the driver JVM crashes (OOM) or gets killed by YARN before the executors even see the data.

32. **Question**: You increase `spark.sql.shuffle.partitions` from 200 to 2000. Your YARN cluster has 50 executors with 4 cores each (200 total vCores). What happens to execution time?
**Answer**: It might increase due to scheduling overhead and small file I/O.
**Mastery Explanation**: While 200 partitions map 1:1 to available vCores, 2000 partitions will require 10 waves of execution. If the data is small, the overhead of task scheduling and generating 2000 tiny shuffle files will degrade performance.

33. **Question**: A stage consists entirely of narrow transformations (map, filter). You increase `spark.locality.wait` from 3s to 30s. What is the impact?
**Answer**: Tasks may be severely delayed.
**Mastery Explanation**: If the local node is busy, the scheduler will wait up to 30 seconds before assigning the task to a non-local node. For quick map tasks, it's often faster to incur the network transfer penalty than to wait 30 seconds.

34. **Question**: You configure `spark.executor.instances=0` with dynamic allocation on. How does the job start?
**Answer**: It starts with `spark.dynamicAllocation.initialExecutors` or `minExecutors`.
**Mastery Explanation**: Spark overrides the 0 instances configuration when dynamic allocation is on, using the initial/min executor configurations to bootstrap the application.

35. **Question**: You have a highly skewed dataset. You configure YARN fair scheduling to allocate max resources. Does this fix the straggler tasks?
**Answer**: No.
**Mastery Explanation**: YARN schedules resources at the container level. Data skew causes a single Spark task to process a massive partition inside one executor thread. Adding more YARN containers won't help a single overloaded thread.

36. **Question**: You use `cache()` on a massive DataFrame. Tungsten is enabled. Will this cause YARN to kill the container for overhead violation?
**Answer**: Unlikely, caching is constrained to the JVM heap.
**Mastery Explanation**: `cache()` (MEMORY_AND_DISK) uses the Storage memory pool within the JVM heap. If it gets full, it spills to disk or evicts old blocks. It does not blindly allocate native memory to the point of a YARN kill.

37. **Question**: You change the YARN queue from `default` to `high_priority_queue` for your Spark job. The job still waits in ACCEPTED. What is the most likely cause?
**Answer**: The queue does not exist, or you lack ACL permissions, or the queue is at its maximum capacity.
**Mastery Explanation**: YARN strictly enforces queue capacities and ACLs. If the queue is fully utilized by other apps, or your user lacks submit access, it remains ACCEPTED.

38. **Question**: In a YARN node with 10 disks, you observe that only 1 disk is heavily utilized during shuffles. How do you fix this?
**Answer**: Configure `spark.local.dir` to use a comma-separated list of all 10 disk mount points.
**Mastery Explanation**: Spark writes shuffle data to the directories defined in `spark.local.dir` (which inherits `YARN_LOCAL_DIRS` in YARN mode). By default, if only one path is set, all I/O goes to one disk, causing a massive bottleneck.

39. **Question**: You change a `groupByKey()` to `reduceByKey()` in your code. How does this affect YARN network utilization?
**Answer**: It massively decreases network traffic.
**Mastery Explanation**: `reduceByKey` triggers map-side combining (partial aggregation on the executor before the shuffle). `groupByKey` sends all raw data across the network, saturating YARN NodeManager network interfaces.

40. **Question**: You set `spark.memory.fraction=0.9` (default 0.6). What happens to standard Java objects in your UDFs?
**Answer**: They are highly likely to cause JVM OOMs.
**Mastery Explanation**: `spark.memory.fraction` dictates the unified memory (execution + storage). Setting it to 0.9 leaves only 10% of the heap for user data structures and internal metadata. Memory-heavy UDFs will crash the JVM.

## Part 4: Coding & Debugging Questions

41. **Question**: Log Analysis: `java.lang.OutOfMemoryError: Java heap space` at `org.apache.spark.sql.execution.joins.BroadcastHashJoinExec`. What is the fix?
**Answer**: Increase `spark.driver.memory` (if on driver) or `spark.executor.memory`, OR lower `spark.sql.autoBroadcastJoinThreshold`.
**Mastery Explanation**: The broadcasted table is too large to fit in the JVM heap. Lowering the threshold forces Catalyst to use a SortMergeJoin instead, which spills to disk and avoids OOM.

42. **Question**: Log Analysis: `ExecutorLostFailure (executor 1 exited caused by one of the running tasks) Reason: Container killed by YARN for exceeding memory limits. 12.4 GB of 12 GB physical memory used.` Fix?
**Answer**: Increase `spark.yarn.executor.memoryOverhead`.
**Mastery Explanation**: The JVM heap is fine, but off-heap memory (Netty buffers, Python workers, native libraries) pushed the total container memory over the 12GB YARN limit.

43. **Question**: You write a Spark Streaming application on YARN. After 3 days, it fails. Logs show a slow, continuous increase in memory usage. What is the architecture flaw?
**Answer**: State buildup without a TTL or checkpointing in stateful operations (e.g., `updateStateByKey` without cleanup).
**Mastery Explanation**: In YARN, long-running streaming apps must carefully manage state. Unbounded state accumulation in the JVM heap eventually exceeds `spark.executor.memory`, causing an OOM.

44. **Question**: Log Analysis: `org.apache.spark.network.client.TransportClient: Failed to connect to /10.0.0.5:7337`. This happens during a shuffle. What is the issue?
**Answer**: The YARN External Shuffle Service on NodeManager 10.0.0.5 is down or unreachable.
**Mastery Explanation**: Port 7337 is the default port for the Spark external shuffle service. If it fails, executors cannot fetch map outputs from that node.

45. **Question**: Your job generates 100,000 tasks. YARN allocates 100 executors. The job spends 90% of its time with only 1 task running. What debugging step is needed?
**Answer**: Check the Spark UI for Data Skew.
**Mastery Explanation**: If 99,999 tasks finish quickly and 1 takes forever, you have data skew. Catalyst and YARN cannot automatically fix this. You must implement salting or skew hints.

46. **Question**: Your code uses `df.repartition(10000)`. YARN NodeManagers start failing with "No space left on device". Why?
**Answer**: Shuffle spill fills up the local disks.
**Mastery Explanation**: `repartition` implies a full network shuffle. Generating 10,000 partitions creates massive amounts of intermediate files in `spark.local.dir`. If disks are small, they fill up and YARN fails the NodeManager.

47. **Question**: You submit a Spark job using `yarn-client` mode from a lightweight edge node. The job processes 10TB of data and attempts to run `.collect()`. What happens?
**Answer**: The edge node crashes.
**Mastery Explanation**: `.collect()` sends all 10TB of data to the Driver. In client mode, the Driver runs on the edge node, which likely has limited memory. It will immediately OOM.

48. **Question**: Log Analysis: `Initial job has not accepted any resources; check your cluster UI to ensure that workers are registered and have sufficient resources`. The YARN UI shows the cluster has 1000 free vCores. Why?
**Answer**: Your requested memory per executor exceeds the maximum allowed by YARN (`yarn.scheduler.maximum-allocation-mb`).
**Mastery Explanation**: Even with free cores, if a single container request asks for more memory than any single node is configured to provide, YARN cannot fulfill the request.

49. **Question**: You are debugging a slow job. You notice in the YARN UI that node locality is 0%, and everything is "ANY". What Catalyst/Spark misconfiguration causes this?
**Answer**: Computing against data not residing in HDFS, or `spark.locality.wait` set to 0.
**Mastery Explanation**: If reading from Amazon S3 or if locality wait is 0, Spark cannot leverage data locality, forcing all reads to go over the network, heavily penalizing performance.

50. **Question**: A developer writes: `val data = spark.read.parquet("..."); data.map(x => new HeavyObject()).count()`. YARN kills executors continuously with GC overhead limit exceeded. Fix?
**Answer**: Avoid creating complex object wrappers. Use Spark SQL/DataFrame built-in functions to keep data in Tungsten format.
**Mastery Explanation**: `map()` with a custom Java object forces Catalyst to deserialize Tungsten's optimized off-heap binary format into full Java objects, overwhelming the JVM Garbage Collector. Use DataFrame operations to keep computation native.
