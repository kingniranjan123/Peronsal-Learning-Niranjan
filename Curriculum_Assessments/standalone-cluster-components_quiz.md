# Spark Standalone Cluster Components - Senior/Staff Assessment

## Part 1: True/False Questions

**1. In Spark Standalone mode, Executor JVMs communicate their execution status back to the Worker daemon, which then relays it to the Driver.**
**Answer:** False
**Mastery Explanation:** The Executor JVM (`CoarseGrainedExecutorBackend`) establishes a direct, bidirectional RPC pipeline directly back to the Driver's `TaskScheduler`. It bypasses both the Worker and the Master during actual data processing to avoid network bottlenecks.

**2. By default, the Standalone Master evaluates resource requests against a FIFO scheduling policy.**
**Answer:** True
**Mastery Explanation:** The Master maintains a stateful ledger of available CPU/memory and schedules resources for competing applications using a simple FIFO queuing model.

**3. The Standalone Master orchestrates shuffle data transfers between executors during wide transformations.**
**Answer:** False
**Mastery Explanation:** The Master is only a resource coordinator. Data processing and shuffle data transfers happen directly between Executor JVMs via the `BlockManager` and `ShuffleClient`, completely bypassing the Master.

**4. If no core constraints are specified, a newly submitted Spark application on a Standalone cluster will allocate a single core per Worker node.**
**Answer:** False
**Mastery Explanation:** By design, an unconstrained application will greedily attempt to grab *all* available CPU cores across *all* alive Worker nodes, monopolizing the cluster.

**5. Implementing Dynamic Resource Allocation in Standalone mode necessitates the deployment of an External Shuffle Service on Worker nodes.**
**Answer:** True
**Mastery Explanation:** Without an External Shuffle Service, dynamically scaling down (killing idle executors) will destroy the shuffle files written by those executors, causing massive stage recalculations.

**6. Deploying in `client` mode means the Standalone Master natively schedules a "Driver wrapper" process on one of the cluster's Worker nodes.**
**Answer:** False
**Mastery Explanation:** This describes `cluster` mode. In `client` mode, the Driver JVM boots on the exact machine where `spark-submit` is invoked, which can introduce severe network latency if outside the cluster's subnet.

**7. Spark's TaskScheduler uses "Delay Scheduling" to achieve Data Locality by waiting for busy executors on preferred nodes to become available.**
**Answer:** True
**Mastery Explanation:** If an executor on the target Worker is busy, Spark will wait up to `spark.locality.wait` (default 3s) for the executor to free up before falling back to `ANY`, eliminating network I/O.

**8. During a ZooKeeper-coordinated Master failover, active Executor tasks are paused or killed until the new Master is elected.**
**Answer:** False
**Mastery Explanation:** During Master failover (which takes ~10-30s), the Driver pauses scheduling new resources, but active Executor tasks continue running completely uninterrupted because they communicate directly with the Driver.

**9. The DAGScheduler is responsible for marking a Worker as "DEAD" when heartbeats are missed.**
**Answer:** False
**Mastery Explanation:** The Master tracks Worker heartbeats and marks them as "DEAD". It then notifies the Driver. The Driver's `DAGScheduler` responds by invalidating cached partitions and rescheduling lost tasks.

**10. Tungsten off-heap memory allocations bypass JVM Garbage Collection and heavily leverage the Unsafe API.**
**Answer:** True
**Mastery Explanation:** Tungsten allocates off-heap memory to store data in a highly optimized binary format, allowing the physical execution engine to operate outside the JVM's GC, significantly reducing GC pauses.

---

## Part 2: Multiple Choice Questions

**11. Which component maintains the global state of the Standalone cluster and tracks available CPU/memory on Workers?**
A) StandaloneAppClient
B) Worker Daemon
C) Standalone Master
D) CoarseGrainedExecutorBackend
**Answer:** C
**Mastery Explanation:** The Standalone Master is the lightweight JVM daemon acting as the cluster's resource coordinator and maintaining the resource state ledger.

**12. When a Worker daemon receives a `LaunchExecutor` command from the Master, what process does it spawn?**
A) StandaloneAppClient
B) CoarseGrainedExecutorBackend
C) BlockManager
D) TaskScheduler
**Answer:** B
**Mastery Explanation:** The Worker daemon forks a new child JVM running the `CoarseGrainedExecutorBackend` class, which is the physical JVM container where actual distributed processing occurs.

**13. In Standalone mode, if `spark.cores.max` is set to 20 and `spark.executor.cores` is set to 5, how many Executor JVMs will be requested?**
A) 100
B) 20
C) 4
D) 5
**Answer:** C
**Mastery Explanation:** The cluster will allocate exactly 4 Executor JVMs (20 total cores / 5 cores per executor). This prevents unconstrained monopolization and dictates precise executor density.

**14. What occurs when an executor exits with "Command exited with code 137"?**
A) The TaskScheduler could not serialize the closure.
B) The Master lost ZooKeeper quorum.
C) The OS OOM killer terminated the JVM due to breaching physical memory limits.
D) The External Shuffle Service failed to fetch blocks.
**Answer:** C
**Mastery Explanation:** Exit code 137 (SIGKILL) is universally associated with the OS Out-Of-Memory (OOM) killer, typically due to excessive off-heap allocation or Python UDF memory overhead breaching the container/node limits.

**15. Which of the following operations has an O(N) performance complexity in Standalone mode (where N is the number of executors)?**
A) Master Election via ZooKeeper
B) Executor Launch
C) Dynamic Allocation scale up/down
D) Task Dispatch
**Answer:** C
**Mastery Explanation:** Dynamic allocation adds or removes executors dynamically, an O(N) operation that requires the External Shuffle Service to avoid data loss on scale-down.

**16. Why is `cluster` deploy mode significantly harder to debug than `client` mode?**
A) The Spark UI is disabled in cluster mode.
B) Driver stdout/stderr logs are isolated on a random Worker node rather than streaming to the user's terminal.
C) Task execution metrics are not sent back to the Master.
D) Heartbeats are suppressed to reduce network overhead.
**Answer:** B
**Mastery Explanation:** In cluster mode, the Driver runs embedded deep within the cluster on a random Worker node, meaning developers cannot simply view the Driver logs in their local terminal, complicating debugging.

**17. What happens if the `StandaloneAppClient` cannot communicate with the active Master?**
A) The application immediately crashes with an OOM error.
B) The Workers terminate all running CoarseGrainedExecutorBackends.
C) In an HA setup, it polls the standby Masters until it finds the newly elected leader.
D) The DAGScheduler routes tasks through ZooKeeper instead.
**Answer:** C
**Mastery Explanation:** When connected to an HA Standalone cluster with multiple Masters configured (e.g., `spark://master1,master2,master3`), the client will automatically poll the standby Masters upon failure of the active Master.

**18. What is the primary role of the `StandaloneAppClient` embedded within the Driver?**
A) Caching RDDs in the local BlockManager.
B) Forking the CoarseGrainedExecutorBackend on Worker nodes.
C) Negotiating directly with the Master for resources and registering the application.
D) Monitoring the ZooKeeper quorum for split-brain scenarios.
**Answer:** C
**Mastery Explanation:** The `StandaloneAppClient` sends a `RegisterApplication` RPC message to the Standalone Master to negotiate resources and begin the execution lifecycle.

**19. How does the Driver's DAGScheduler guarantee data consistency when a Worker is marked "DEAD"?**
A) It queries the Standalone Master for the lost shuffle files.
B) It triggers the OS to restart the Worker daemon.
C) It relies on the External Shuffle Service to replay the data.
D) It invalidates cached partitions on that Worker and aggressively reschedules lost tasks onto surviving Executors.
**Answer:** D
**Mastery Explanation:** The DAGScheduler responds to topology changes; if a Worker dies, it recalculates the RDD lineage for any lost data and reschedules tasks, guaranteeing processing continuity.

**20. Which internal component houses the `BlockManager` for distributed caching?**
A) Standalone Master
B) Worker Daemon
C) CoarseGrainedExecutorBackend
D) StandaloneAppClient
**Answer:** C
**Mastery Explanation:** The `CoarseGrainedExecutorBackend` contains the Task Thread Pool, ShuffleClient, and BlockManager, which handles the physical storage of cached RDD/DataFrame partitions.

**21. What defines the division of the JVM heap inside an Executor?**
A) Standalone Master Scheduling Policy
B) Zookeeper Quorum State
C) Unified Memory Manager
D) OS Page Cache
**Answer:** C
**Mastery Explanation:** The Unified Memory Manager divides the JVM heap into Storage Memory (for caches) and Execution Memory (for shuffles/joins).

**22. How are task closures transported from the Driver to the Executors?**
A) They are persisted to HDFS and read by Executors.
B) They are serialized (Kryo/Java) and sent over the network via Netty RPC.
C) They are broadcasted via ZooKeeper.
D) They are passed as command-line arguments when the Worker forks the JVM.
**Answer:** B
**Mastery Explanation:** The Driver's TaskScheduler serializes the closures and dispatches them directly to the Executors via a high-throughput Netty RPC pipeline.

**23. What is the optimal sweet spot for core density per Executor JVM to balance HDFS throughput and GC efficiency?**
A) 1 core
B) 5 cores
C) 16 cores
D) 64 cores
**Answer:** B
**Mastery Explanation:** 5 cores is widely recognized by senior engineers as the optimal sweet spot to prevent HDFS/Parquet reader thread contention while maximizing the unified memory pool without triggering excessive GC pauses.

**24. In the provided Data Locality code example, what does `sc.makeRDD` leverage to enforce strict IP-to-partition mapping?**
A) `spark.locality.wait`
B) `preferredLocations`
C) `Dynamic Allocation`
D) `NODE_ANY`
**Answer:** B
**Mastery Explanation:** `makeRDD` allows the developer to explicitly set the `preferredLocations` for each partition, allowing the TaskScheduler to attempt `NODE_LOCAL` scheduling on specific Worker nodes.

**25. Which SparkListener event is critical for diagnosing network partitions between Workers and the Master?**
A) `onApplicationStart`
B) `onTaskGettingResult`
C) `onExecutorRemoved`
D) `onBlockUpdated`
**Answer:** C
**Mastery Explanation:** `onExecutorRemoved` intercepts when an executor is lost (e.g., Worker dies, Dynamic Allocation scales down, or OS OOM kill), providing real-time visibility into cluster elasticity and failures.

---

## Part 3: "Small Twist" Questions

**26. Scenario: You submit an app to a Standalone cluster with `spark.executor.cores=5`, but forget to set `spark.cores.max`. The cluster has 10 Workers, each with 20 cores. How many total cores will your app consume?**
A) 5 cores
B) 50 cores
C) 100 cores
D) 200 cores
**Answer:** D
**Mastery Explanation:** Without `spark.cores.max`, the app will greedily grab *all* available cores across the cluster (10 * 20 = 200). It will boot four 5-core executors on every single worker.

**27. Scenario: You enable `spark.dynamicAllocation.enabled=true` but do NOT configure an External Shuffle Service. What occurs during a wide transformation (e.g., `groupByKey`)?**
A) The job proceeds normally but slightly slower.
B) Idle executors scale down, destroying local shuffle data, leading to massive stage recalculations and potential job failure.
C) The Master prevents the executors from scaling down.
D) Shuffle data is automatically routed to HDFS.
**Answer:** B
**Mastery Explanation:** Executors write shuffle data locally. If dynamic allocation kills an idle executor without an external shuffle service to serve those files, the data is lost, forcing the DAGScheduler to recompute the entire lineage.

**28. Scenario: A developer runs `spark-submit --deploy-mode client` from their laptop over a standard corporate VPN to a remote Standalone cluster. They execute `df.collect()`. What is the most likely outcome?**
A) Faster execution due to client-side rendering.
B) The job fails with an OOM or severe network timeout on the Driver.
C) The Master node crashes.
D) Executors switch to `cluster` mode automatically.
**Answer:** B
**Mastery Explanation:** In `client` mode, the Driver JVM runs on the laptop. All Executor data from `collect()` and high-frequency heartbeats must stream over the slow VPN, heavily bottlenecking the TaskScheduler and often crashing the Driver.

**29. Scenario: You configure ZooKeeper HA with `spark.network.timeout=10s`. The active Master crashes, and ZK election takes 25 seconds. What happens to the running Spark app?**
A) It pauses smoothly and resumes after 25 seconds.
B) The Driver prematurely declares executors/workers dead due to the timeout, aborting the application.
C) The StandaloneAppClient takes over Master duties.
D) The ZK quorum kills the Driver.
**Answer:** B
**Mastery Explanation:** The RPC timeout (10s) is much shorter than the ZK election window (25s). The Driver/Executors will time out and drop connections, killing the job before the new Master can take over. `spark.network.timeout` must be increased for HA.

**30. Scenario: You define `preferredLocations` in an RDD, but set `spark.locality.wait=0s`. What happens during task scheduling if the target executor is currently processing a different task?**
A) The TaskScheduler waits indefinitely for the target executor.
B) The TaskScheduler instantly falls back to `ANY` locality, shipping the task to a random node over the network.
C) The task is dropped and marked as FAILED.
D) The OS OOM killer is invoked.
**Answer:** B
**Mastery Explanation:** `spark.locality.wait=0s` removes Delay Scheduling. If the preferred node isn't instantly available, Spark degrades to `ANY` locality, bypassing the data locality optimization and causing cross-rack network shuffling.

**31. Scenario: You set `spark.executor.memory=16g` and `spark.memory.offHeap.size=16g` on a Worker with exactly 20GB of physical RAM. What happens when the app processes a heavy aggregation?**
A) The job completes extremely fast due to Tungsten optimization.
B) The Master restricts the allocation to fit within 20GB.
C) The CoarseGrainedExecutorBackend is killed by the OS (Code 137).
D) The JVM garbage collector prevents the crash.
**Answer:** C
**Mastery Explanation:** You requested 32GB total per executor (16G heap + 16G off-heap). The OS will trigger the OOM killer (Code 137) when the JVM attempts to physically allocate memory beyond the container's 20GB hardware limit.

**32. Scenario: During an HA failover, a Worker successfully connects to the new standby Master. Do the running Executor JVMs on that Worker also need to register with the new Master?**
A) Yes, they must request permission to continue running.
B) No, Executors communicate their status strictly to the Driver, not the Master.
C) Yes, they must send shuffle metadata to the Master.
D) No, the Worker forwards all executor data.
**Answer:** B
**Mastery Explanation:** The Master tracks Workers and Driver resources, but the actual Executor JVMs communicate directly with the Driver's TaskScheduler. The Master failover does not interrupt active Executor-to-Driver RPC pipelines.

**33. Scenario: Two Spark apps are submitted simultaneously to a 100-core Standalone cluster. App A sets `spark.cores.max=100`. App B sets `spark.cores.max=50`. If App A registers 1 millisecond before App B, what does App B receive?**
A) 50 cores (fair share).
B) 0 cores, it queues until App A releases resources.
C) 25 cores.
D) The Master kills App A to prioritize App B.
**Answer:** B
**Mastery Explanation:** The Standalone Master defaults to a FIFO scheduling policy. Since App A registered first and asked for all 100 cores, App B will be starved and queued until App A completes or scales down.

**34. Scenario: A Worker node suffers a hard disk failure and stops sending heartbeats. The Driver's DAGScheduler is notified. What does it do with the tasks that were running on that Worker?**
A) Fails the entire Spark application.
B) Waits for the Master to fix the disk.
C) Invalidates cached partitions on the Worker and aggressively reschedules lost tasks on surviving nodes.
D) Sends a `LaunchExecutor` command to a different cluster.
**Answer:** C
**Mastery Explanation:** The DAGScheduler provides fault tolerance by utilizing the RDD lineage. It invalidates the lost partitions and immediately reschedules the tasks on the remaining alive Executors to ensure absolute data consistency.

**35. Scenario: You configure `spark.executor.cores=1`. How does this impact HDFS reading performance?**
A) It maximizes throughput by isolating tasks.
B) It causes thread contention and severe performance degradation.
C) It prevents HDFS from broadcasting blocks.
D) It optimizes GC pauses perfectly.
**Answer:** B
**Mastery Explanation:** An executor with 1 core cannot run concurrent tasks. Because HDFS thrives on concurrent block reads, 1-core executors are highly inefficient and waste JVM overhead, which is why 5 cores is the architectural sweet spot.

**36. Scenario: The Master is running on `192.168.1.10`. You submit with `--master spark://localhost:7077` from a Worker node. What happens?**
A) It connects successfully.
B) It fails to connect because `localhost` resolves to the Worker's loopback interface, not the actual Master IP.
C) The Master proxies the connection.
D) ZooKeeper intercepts and corrects the IP.
**Answer:** B
**Mastery Explanation:** `localhost` explicitly means the machine executing the command. The `StandaloneAppClient` will attempt to find a Master daemon on the Worker node itself, failing connection instantly.

**37. Scenario: In `cluster` mode, the Driver JVM crashes with an OOM error. Does the Standalone Master automatically restart the Driver?**
A) Yes, always, because it's in cluster mode.
B) Only if configured with `--supervise` during `spark-submit`.
C) No, the Standalone Master never restarts Drivers.
D) Yes, but only if ZooKeeper is enabled.
**Answer:** B
**Mastery Explanation:** By default, if the Driver fails in cluster mode, it remains failed. You must explicitly pass the `--supervise` flag to the `spark-submit` command to instruct the Standalone Master to restart the Driver upon non-zero exit codes.

**38. Scenario: You implement a `SparkListener` and monitor `onExecutorRemoved`. You see a removal due to "Executor heartbeat timed out after 120000 ms". What is the most likely cause?**
A) The OS OOM killer terminated the JVM.
B) A severe, prolonged JVM Garbage Collection pause ("stop-the-world") prevented the Executor from sending its heartbeat to the Driver.
C) The Worker daemon crashed.
D) The Master elected a new leader.
**Answer:** B
**Mastery Explanation:** If the OS killed it, the OS socket drops instantly. A 120-second timeout usually indicates a massive "stop-the-world" GC pause where the JVM is completely frozen and cannot execute the heartbeat thread.

**39. Scenario: A Spark job uses Tungsten code-generated loops heavily. Which memory pool must be monitored to prevent crashes?**
A) Storage Memory
B) External Shuffle Service Memory
C) Off-heap Execution Memory (if enabled)
D) ZooKeeper Quorum Cache
**Answer:** C
**Mastery Explanation:** Tungsten heavily leverages off-heap memory via the Unsafe API. If `spark.memory.offHeap.enabled` is true, the off-heap size must be strictly monitored to prevent exceeding physical container limits.

**40. Scenario: You set `spark.executor.memory=4g` and process a massive broadcast join. The broadcast variable is 6GB. What occurs?**
A) Tungsten seamlessly pages the broadcast to disk.
B) The Executor JVM crashes with a `java.lang.OutOfMemoryError` because the broadcast exceeds available heap space.
C) The Master dynamically allocates more memory.
D) The Driver splits the broadcast into smaller chunks.
**Answer:** B
**Mastery Explanation:** Broadcast variables must fit entirely into the Executor's memory (specifically the unified memory pool). Attempting to load a 6GB object into a 4GB heap will immediately trigger a JVM OOM error.

---

## Part 4: Coding & Debugging Questions

**41. Debug the following snippet. What is wrong with this High Availability configuration?**
```scala
val sparkHA = SparkSession.builder()
  .master("spark://master1.cluster:7077")
  .config("spark.network.timeout", "120s")
  .getOrCreate()
```
**Answer & Mastery Explanation:** The `.master()` string only defines a single Master. For HA to function, it MUST contain a comma-separated list of all standby Masters (e.g., `spark://master1:7077,master2:7077`). If `master1` dies, the client has no other addresses to poll.

**42. Analyze the following resource configuration. What is the subtle logic error?**
```scala
val spark = SparkSession.builder()
  .master("spark://master:7077")
  .config("spark.cores.max", "20")
  .config("spark.executor.cores", "6")
  .getOrCreate()
```
**Answer & Mastery Explanation:** The math doesn't align cleanly. 20 / 6 = 3.33. Spark will allocate three 6-core executors (18 cores). The remaining 2 cores requested by `spark.cores.max` are insufficient to launch a fourth executor, wasting cluster resources and alignment. 

**43. Fix the memory leak risk in this Tungsten configuration:**
```scala
val spark = SparkSession.builder()
  .config("spark.executor.memory", "16g")
  .config("spark.memory.offHeap.size", "8g")
  .getOrCreate()
```
**Answer & Mastery Explanation:** Off-heap memory is disabled by default. Defining the size does nothing unless you explicitly enable it. The fix requires adding `.config("spark.memory.offHeap.enabled", "true")`.

**44. Review the SparkListener implementation:**
```scala
class MyListener extends SparkListener {
  override def onApplicationEnd(appEnd: SparkListenerApplicationEnd): Unit = {
    println("App ended!")
  }
}
```
**What critical method is missing to monitor elastic Standalone topology changes?**
**Answer & Mastery Explanation:** It is missing `onExecutorAdded` and `onExecutorRemoved`. These are the precise events triggered by the DAGScheduler when the Standalone Master dynamically allocates or loses `CoarseGrainedExecutorBackend` instances.

**45. What will cause this Data Locality code to degrade to `ANY` locality?**
```scala
val localityAwareRDD = sc.makeRDD(data, numSlices = 10)
// No preferred locations specified in the data generation
val transformed = localityAwareRDD.map(...)
```
**Answer & Mastery Explanation:** Using standard `makeRDD` or `parallelize` without providing the explicit sequence of preferred hostnames per partition prevents the TaskScheduler from utilizing Delay Scheduling. It will degrade to `ANY` instantly.

**46. Debug the network timeout issue in this production HA cluster setup:**
```scala
val spark = SparkSession.builder()
  .master("spark://m1:7077,m2:7077")
  .config("spark.network.timeout", "5s")
  .getOrCreate()
```
**Answer & Mastery Explanation:** `spark.network.timeout` is set to 5 seconds. ZooKeeper leader election typically takes 15-30 seconds. The Driver and Executors will timeout and drop connections before the standby Master is elected, killing the job.

**47. A junior dev enables dynamic allocation in Standalone mode with this code. Why does it fail during execution?**
```scala
val spark = SparkSession.builder()
  .config("spark.dynamicAllocation.enabled", "true")
  .config("spark.dynamicAllocation.minExecutors", "1")
  .config("spark.dynamicAllocation.maxExecutors", "10")
  .getOrCreate()
```
**Answer & Mastery Explanation:** Dynamic allocation in Standalone mode strictly requires `.config("spark.shuffle.service.enabled", "true")` (and the corresponding service running on Workers). Without it, executors scaling down will lose their shuffle files, crashing subsequent stages.

**48. Why will this deploy mode cause an OOM on an edge node?**
```bash
spark-submit \
  --class com.demo.App \
  --master spark://master:7077 \
  --deploy-mode client \
  --driver-memory 2g \
  my-app.jar
```
*App executes `df.collect()` on a 50GB dataset.*
**Answer & Mastery Explanation:** `--deploy-mode client` runs the Driver JVM on the edge node with only 2GB of heap. `df.collect()` streams the entire 50GB dataset directly into the Driver's heap, instantly causing a `java.lang.OutOfMemoryError`.

**49. An app runs fine on YARN but freezes on Standalone. Look at the config:**
```scala
val spark = SparkSession.builder()
  .master("spark://master:7077")
  // No spark.cores.max provided
  .getOrCreate()
```
**Answer & Mastery Explanation:** Because `spark.cores.max` is omitted, the app greedily grabs every single core on the Standalone cluster. If a second app is submitted, it queues infinitely because the Standalone Master (using FIFO) has no resources left to offer, freezing the second app.

**50. How do you fix this Data Locality implementation to utilize Delay Scheduling properly?**
```scala
val localizedData = Seq(("worker1", "dataA"), ("worker2", "dataB"))
val rdd = sc.parallelize(localizedData)
```
**Answer & Mastery Explanation:** `sc.parallelize` does not accept preferred locations. You must use `sc.makeRDD(Seq((data, Seq(host))))` to explicitly attach the `NODE_LOCAL` preference metadata to the partition, allowing the TaskScheduler to enforce strict Task-to-Worker mapping.
