# Spark Master Class: Running Applications - Assessment

## Section 1: True/False Questions

**1. Tungsten bypasses the standard JVM object model and uses raw memory pointers to manage memory explicitly.**
* **Correct Answer:** True
* **Mastery Explanation:** Project Tungsten stores data in a highly compact binary format and uses raw memory pointers. This eliminates the overhead of standard Java objects and significantly reduces Garbage Collection (GC) pressure, which is critical for bare-metal performance.

**2. `spark.dynamicAllocation.enabled=true` can be used safely without an external shuffle service in all deploy modes to save resources.**
* **Correct Answer:** False
* **Mastery Explanation:** Dynamic allocation requires an external shuffle service (`spark.shuffle.service.enabled=true`). If executors are dynamically downscaled (preempted), any shuffle files stored on them would be lost, causing cascading stage failures. The external shuffle service preserves these files independently of the executor lifecycle.

**3. The G1GC collector is highly recommended for large Spark executor heaps because setting an aggressive `InitiatingHeapOccupancyPercent` prevents expensive full GC pauses.**
* **Correct Answer:** True
* **Mastery Explanation:** Setting an aggressive `InitiatingHeapOccupancyPercent` (e.g., 35%) forces G1GC to trigger concurrent marking cycles earlier. This prevents the heap from filling up completely, avoiding "Stop-The-World" Full GC pauses that cause executor timeouts.

**4. Whole-stage code generation (WSCG) in Catalyst combines multiple physical operators into a single Java function, minimizing virtual function calls.**
* **Correct Answer:** True
* **Mastery Explanation:** WSCG acts like a compiler, taking operations like filter, map, and project, and collapsing them into a single, highly optimized Java function. This leverages CPU registers more efficiently and eliminates the overhead of Volcano-iterator virtual function calls.

**5. Assigning more than 5 cores per executor is considered a best practice in I/O heavy workloads to maximize network interface throughput.**
* **Correct Answer:** False
* **Mastery Explanation:** Assigning too many cores (e.g., > 5) to a single executor leads to degraded HDFS I/O throughput due to concurrent thread contention, lock competition, and overwhelming the network interface card (NIC). 4-5 cores is the sweet spot.

**6. In a Kubernetes deployment, pod templates (`spark.kubernetes.driver.podTemplateFile`) allow injecting Kubernetes-specific configurations like sidecar containers and node selectors without cluttering Spark conf.**
* **Correct Answer:** True
* **Mastery Explanation:** Pod templates are advanced tools that allow engineers to define native K8s manifests for Spark pods. This handles infrastructure requirements (tolerations, IAM roles, sidecars) that the standard Spark config namespace cannot cleanly support.

**7. Speculative execution (`spark.speculation`) is designed to mitigate out-of-memory errors by restarting failed tasks on larger executors.**
* **Correct Answer:** False
* **Mastery Explanation:** Speculative execution does not fix OOMs. It is designed to mitigate *stragglers* (tasks running abnormally slow due to hardware degradation, noisy neighbors, or network hiccups) by proactively launching duplicate tasks on different nodes.

**8. The `SparkListener` API provides a direct hook into the Driver's event bus to capture task completions and low-level JVM metrics in real time.**
* **Correct Answer:** True
* **Mastery Explanation:** The `SparkListener` intercepts events directly from the DAG Scheduler, allowing developers to read `taskMetrics` (like GC time, shuffle bytes, execution time) at runtime for custom alerting and monitoring.

**9. When `spark.memory.fraction` is misconfigured, it can lead to OOM errors, but it has no impact on GC pauses.**
* **Correct Answer:** False
* **Mastery Explanation:** `spark.memory.fraction` divides the heap between Spark's unified memory (execution/storage) and User Memory. Misconfiguring it not only causes OOMs but severely impacts GC. If unified memory is too large, user objects quickly fill the remaining heap, causing constant, aggressive GC cycles.

**10. `spark.kubernetes.allocation.batch.size` optimizes Kubernetes API server interactions by requesting executors in batches, preventing API throttling.**
* **Correct Answer:** True
* **Mastery Explanation:** During large scale-outs, requesting hundreds of pods simultaneously can overwhelm the K8s API server (control plane), leading to throttling or drops. Batching pod creation prevents this.

---

## Section 2: Multiple Choice Questions

**11. Which GC configuration is used in an enterprise-grade `spark-submit` to trigger concurrent GC cycles earlier?**
A) `-XX:+UseParallelGC`
B) `-XX:InitiatingHeapOccupancyPercent=35`
C) `-XX:MaxGCPauseMillis=200`
D) `-XX:+UseZGC`
* **Correct Answer:** B
* **Mastery Explanation:** Option B tells the G1 Garbage Collector to begin its concurrent marking phase when the heap is 35% full, instead of waiting until it is almost completely full. This proactive cleanup prevents catastrophic Full GC pauses.

**12. What is the primary purpose of the Application Master (AM) in cluster mode?**
A) To compile high-level DataFrame code to physical execution plans.
B) To negotiate with the cluster manager to request resources and spawn Executor JVMs.
C) To run the Tungsten Execution Engine on edge nodes.
D) To serve as an external shuffle service.
* **Correct Answer:** B
* **Mastery Explanation:** The AM acts as the application's ambassador to the cluster manager (YARN, K8s). It negotiates container allocations based on the application's resource demands, while the Driver (which runs inside the AM in cluster mode) handles query planning.

**13. How does Tungsten significantly reduce GC pressure?**
A) By partitioning memory into Execution and Storage domains.
B) By storing data in a compact binary format and using raw memory pointers.
C) By implementing a custom Garbage Collector that replaces G1GC.
D) By offloading all shuffle data to disk immediately.
* **Correct Answer:** B
* **Mastery Explanation:** Standard Java objects have massive metadata overhead. Tungsten bypasses the JVM object model, packing data into raw bytes (like C++), rendering it invisible to the JVM Garbage Collector and eliminating GC pauses for cached data.

**14. What occurs if you assign too few cores per executor (e.g., 1 core per executor)?**
A) It leads to degraded HDFS I/O throughput due to thread contention.
B) It triggers massive speculation of tasks.
C) It leads to JVM proliferation, wasting memory on duplicate broadcast variables.
D) It prevents the use of dynamic allocation.
* **Correct Answer:** C
* **Mastery Explanation:** Running 1 core per executor means a 100-core job needs 100 JVMs. This requires 100 copies of broadcast variables, 100 sets of JVM metadata, and massive overhead for the cluster manager to track, wasting memory and resources.

**15. What configuration is specifically required to preserve shuffle files when executors are preempted during dynamic allocation?**
A) `spark.shuffle.file.buffer`
B) `spark.shuffle.service.enabled`
C) `spark.dynamicAllocation.cachedExecutorIdleTimeout`
D) `spark.executor.memoryOverhead`
* **Correct Answer:** B
* **Mastery Explanation:** The external shuffle service runs independently of the Spark executor (often as a NodeManager auxiliary service in YARN). If an executor is killed by dynamic allocation, the external service continues serving its shuffle data to downstream tasks.

**16. In Kubernetes deployments, what is the role of `spark.kubernetes.executor.podTemplateFile`?**
A) To specify the base Docker image for the executor.
B) To define the exact Spark DataFrame logic to execute.
C) To inject K8s-specific configurations like tolerations, node selectors, and sidecars.
D) To replace the Catalyst optimizer with K8s-native query planning.
* **Correct Answer:** C
* **Mastery Explanation:** Pod templates allow injection of K8s primitives that don't have direct 1:1 Spark configuration flags, providing total control over the pod's specification.

**17. If a task executes 1.5 times slower than the median of the 75% completed tasks, what happens if `spark.speculation` is enabled?**
A) The task is killed immediately to save resources.
B) The Driver proactively launches a duplicate copy of the task on another node.
C) The task is allocated more memory dynamically.
D) The job fails and throws a SpeculationException.
* **Correct Answer:** B
* **Mastery Explanation:** Speculation does not kill the slow task immediately. It launches a race. It fires up a duplicate task; whichever task finishes first is committed, and the *loser* is then killed.

**18. What does `spark.executor.memoryOverhead` accommodate?**
A) Only cached DataFrame storage.
B) JVM Heap memory limits.
C) NIO buffers, Python processes, and native Tungsten memory allocations.
D) Broadcast variables exclusively.
* **Correct Answer:** C
* **Mastery Explanation:** `memoryOverhead` is non-heap memory allocated to the container. It is vital for PySpark (where Python worker processes live entirely outside the JVM heap) and Tungsten's unsafe off-heap memory allocations.

**19. Which event within the `SparkListener` API provides access to `jvmGCTime` and `executorRunTime`?**
A) `SparkListenerJobStart`
B) `SparkListenerStageCompleted`
C) `SparkListenerTaskEnd`
D) `SparkListenerApplicationEnd`
* **Correct Answer:** C
* **Mastery Explanation:** Performance metrics are bound to the execution of an individual task. `SparkListenerTaskEnd` provides `TaskMetrics`, exposing precise metrics regarding how that specific task utilized CPU and memory.

**20. A custom `TaskMetricsListener` evaluates that `shuffleWriteMetrics.bytesWritten` is extremely high for a single task compared to others. What does this indicate?**
A) Inefficient Garbage Collection.
B) Network timeout.
C) Data skew.
D) Tungsten WSCG failure.
* **Correct Answer:** C
* **Mastery Explanation:** Data skew occurs when certain partitions are significantly larger than others due to uneven key distribution. A single task writing massive shuffle data proves that one executor is processing a disproportionately massive chunk of data.

**21. What does the DAG Scheduler do in the Spark execution model?**
A) It compiles code into bare-metal Java bytecode.
B) It divides a Job into Stages at shuffle boundaries.
C) It handles the dynamic allocation scaling events.
D) It provisions Kubernetes pod templates.
* **Correct Answer:** B
* **Mastery Explanation:** The DAG (Directed Acyclic Graph) Scheduler translates the logical plan into a physical plan of Stages. A Stage boundary is always drawn where a wide transformation (like a join or group by) forces a data shuffle across the network.

**22. Why intercept a `SIGTERM` signal with a shutdown hook in a streaming/long-running app?**
A) To prevent the cluster manager from ever killing the application.
B) To cleanly finalize writes, commit off-heap memory, and prevent zombie executors.
C) To automatically scale up the executor count.
D) To force a full GC before exiting.
* **Correct Answer:** B
* **Mastery Explanation:** Unceremoniously killing a JVM can leave corrupt part-files in object stores or leave temporary network sockets open. A shutdown hook allows Spark to abort/commit tasks safely and tear down the Catalyst context gracefully.

**23. When adjusting `spark.memory.fraction`, what are the two main regions competing within that fraction?**
A) User Memory and Reserved Memory.
B) Execution Memory and Storage Memory.
C) Off-heap Memory and Heap Memory.
D) Driver Memory and Executor Memory.
* **Correct Answer:** B
* **Mastery Explanation:** `spark.memory.fraction` dictates Spark's unified memory pool. Within this pool, Execution Memory (sorts, shuffles, joins) and Storage Memory (cached DataFrames) dynamically share and evict each other based on workload pressure.

**24. Which of the following is NOT a benefit of Whole-Stage Code Generation (WSCG)?**
A) Minimizing virtual function calls.
B) Leveraging CPU registers efficiently.
C) Expanding the size of Java objects in the JVM heap.
D) Collapsing multiple physical operators into a single Java function.
* **Correct Answer:** C
* **Mastery Explanation:** WSCG aims to eliminate object overhead entirely. Expanding the size of Java objects would drastically worsen GC performance, directly contradicting Tungsten's design philosophy.

**25. Increasing `spark.network.timeout` and `spark.executor.heartbeatInterval` in Kubernetes is useful for:**
A) Speeding up the shuffle process between executors.
B) Accommodating the ephemeral nature of K8s networks and reducing false-positive executor losses.
C) Decreasing the latency of Catalyst plan generation.
D) Forcing the G1GC to run more frequently.
* **Correct Answer:** B
* **Mastery Explanation:** K8s networking (via CNI plugins) can experience micro-outages during pod evictions, node scaling, or IP reassignment. Increasing network timeouts prevents the Driver from falsely assuming an executor is dead during a brief network hiccup.

---

## Section 3: "Small Twist" Questions

**26. Scenario: You enable `spark.dynamicAllocation.enabled=true` but leave `spark.shuffle.service.enabled=false` on YARN. What happens?**
* **Answer:** The application will fail to start or immediately throw an error.
* **Mastery Explanation:** On YARN, dynamic allocation hard-requires the external shuffle service. Without it, scaling down an executor deletes its shuffle data, meaning downstream stages would permanently hang or fail. Spark enforces this validation at startup.

**27. Scenario: You change `-XX:InitiatingHeapOccupancyPercent=35` to `90` in your executor Java options.**
* **Answer:** The application will suffer from catastrophic "Stop-The-World" Full GC pauses.
* **Mastery Explanation:** By setting IHOP to 90%, you instruct G1GC to wait until the heap is 90% full before doing concurrent cleanup. This leaves virtually no headroom. As data spikes, the heap hits 100%, forcing the JVM to freeze all application threads to do a synchronous cleanup, causing executors to time out and drop.

**28. Scenario: You use a K8s deploy with `spark.kubernetes.allocation.batch.size=1000`.**
* **Answer:** The K8s API server will likely throttle or reject the requests.
* **Mastery Explanation:** Requesting 1000 pods concurrently bombards the K8s control plane. This leads to API throttling, meaning pods won't be scheduled, and could destabilize the entire Kubernetes cluster for other tenants.

**29. Scenario: You set `spark.executor.cores=15` on machines with 16 physical cores.**
* **Answer:** Severe performance degradation due to network and disk I/O contention.
* **Mastery Explanation:** While CPU utilization might look high, 15 concurrent threads writing shuffle files or reading from HDFS will overwhelm the disk controller and network interface, causing lock contention and drastically slowing down actual data throughput.

**30. Scenario: You set `spark.speculation.quantile=0.99` and `spark.speculation.multiplier=5.0`.**
* **Answer:** Speculative execution is effectively disabled.
* **Mastery Explanation:** A quantile of 0.99 means 99% of tasks must finish before speculation is even considered. A multiplier of 5.0 means the straggler must be 5 times slower than the median. These thresholds are so extreme they will never be triggered in a real-world scenario.

**31. Scenario: You run a heavy PySpark job. You set `spark.executor.memory=16g` but leave `spark.executor.memoryOverhead` at default (10%).**
* **Answer:** The YARN/K8s container will be killed for exceeding physical memory limits (Off-heap OOM).
* **Mastery Explanation:** PySpark spawns Python worker processes that execute outside the JVM heap. If memoryOverhead is small, the combined RAM of the JVM and the Python processes will quickly exceed the hard container limit, causing the OS or cluster manager to SIGKILL it.

**32. Scenario: `spark.memory.fraction` is reduced from default `0.6` to `0.1`.**
* **Answer:** Heavy disk spilling during shuffles and immediate eviction of cached data.
* **Mastery Explanation:** Only 10% of the JVM heap is given to Spark's engine for execution and caching. 90% is reserved for user objects. The engine will constantly run out of memory during joins/sorts, forcing it to spill intermediate data to slow disk storage.

**33. Scenario: A custom `SparkListener` performs a synchronous 10-second REST API call inside `onTaskEnd`.**
* **Answer:** The Spark Driver event loop freezes, destabilizing the application.
* **Mastery Explanation:** The `SparkListener` runs on a single event-dispatch thread on the Driver. A blocking operation halts the processing of all other events (task completions, heartbeats), causing the DAG Scheduler to stall and executors to potentially time out.

**34. Scenario: You deploy via `spark-submit --deploy-mode client` on K8s but provide a `spark.kubernetes.driver.podTemplateFile`.**
* **Answer:** The pod template file is ignored.
* **Mastery Explanation:** In client mode, the Driver JVM runs locally on the edge node/laptop executing the `spark-submit` command, not inside a K8s pod. Therefore, driver pod templates have no effect.

**35. Scenario: `spark.streaming.stopGracefullyOnShutdown=false` during a rolling K8s cluster update.**
* **Answer:** The streaming job terminates abruptly, leading to potential data loss or corrupted state.
* **Mastery Explanation:** When K8s restarts nodes, it sends a SIGTERM. If graceful shutdown is disabled, the JVM dies instantly. In-flight Kafka offsets aren't committed, and write-ahead logs might be corrupted, requiring manual recovery.

**36. Scenario: You force `spark.sql.codegen.wholeStage=false`.**
* **Answer:** CPU overhead drastically increases, and execution time lengthens.
* **Mastery Explanation:** Without WSCG, Spark falls back to a Volcano-iterator model. Every row processed requires multiple virtual method calls (e.g., `filter()` then `map()`), destroying CPU cache locality and bypassing Tungsten's primary performance advantage.

**37. Scenario: You set `spark.task.cpus=2` on an executor with `spark.executor.cores=4`.**
* **Answer:** The executor will only run 2 concurrent tasks.
* **Mastery Explanation:** By default, 1 task = 1 CPU. If you require 2 CPUs per task, a 4-core executor can only fit 2 tasks (4 / 2). This drastically reduces the parallelism of your cluster.

**38. Scenario: `spark.memory.storageFraction` is set to `0.9`.**
* **Answer:** Caching dominates unified memory, starving Execution Memory.
* **Mastery Explanation:** While execution memory can evict storage memory if needed, a 0.9 fraction makes storage highly immune to eviction up to 90% of the unified pool. Shuffles and joins will lack RAM, forcing them to spill to disk.

**39. Scenario: `sys.addShutdownHook` calls `spark.stop()` while a DataFrame is midway through writing a non-atomic Parquet dataset.**
* **Answer:** The SparkContext closes cleanly, but partial/corrupted files may remain in the object store.
* **Mastery Explanation:** `spark.stop()` stops the AM and executors safely. However, if a multipart upload to S3 or HDFS was occurring and didn't reach the `_SUCCESS` commit phase, the underlying object store is left with dirty data.

**40. Scenario: An executor has 5 cores, but the job has 100,000 tasks that take 2 milliseconds each.**
* **Answer:** Task scheduling overhead becomes the bottleneck.
* **Mastery Explanation:** It takes time for the Driver to serialize the task closure, send it over the network, and acknowledge completion. If tasks take 2ms, the cluster spends 95% of its time communicating and 5% processing. Partitions should be coalesced to create larger tasks.

---

## Section 4: Coding & Debugging Questions

**41. Debugging: A user reports that their job runs fine but randomly fails with `ExecutorLostFailure: GC overhead limit exceeded`.**
* **Issue:** The map/filter transformations are instantiating large, long-lived Java/Scala collections (User Memory), overwhelming the JVM garbage collector.
* **Fix:** Increase `spark.executor.memory`, lower `spark.memory.fraction` (giving more space to User Memory), or optimize the UDF to stream data rather than loading it entirely into lists.

**42. Debugging: During a shuffle stage, one task writes 5 GB of shuffle data while the median task writes 10 MB.**
* **Issue:** Severe Data Skew. One partition contains a massively disproportionate number of identical keys.
* **Fix:** Salt the keys (append random numbers) before the `groupBy`/`join` to distribute the data evenly, or enable Adaptive Query Execution (`spark.sql.adaptive.enabled=true`) to let Spark dynamically handle skewed joins.

**43. Debugging: Application deployed on YARN is killed with: `Container killed by YARN for exceeding memory limits. 17.5 GB of 17.0 GB physical memory used`.**
* **Issue:** The total physical memory used (JVM Heap + Off-Heap + OS buffers) exceeded the YARN container allocation.
* **Fix:** Increase `spark.executor.memoryOverhead`. If PySpark is used, this provides extra breathing room for Python processes running outside the JVM.

**44. Debugging: A K8s Spark app loses executors during heavy shuffles with "Connection reset by peer" or heartbeat timeouts, despite sufficient memory.**
* **Issue:** Network saturation or ephemeral K8s network drops are causing the Driver to miss executor heartbeats, leading it to assume the executor died.
* **Fix:** Increase `spark.network.timeout` (e.g., to `600s`) and `spark.executor.heartbeatInterval` (e.g., to `60s`) to tolerate network turbulence.

**45. Coding: Write the `spark-submit` argument to provide exactly 15% memory overhead on a 20g heap executor.**
* **Answer:** `--conf spark.executor.memoryOverhead=3072` 
* **Mastery Explanation:** 15% of 20 GB (20480 MB) is 3072 MB. Setting this explicitly overrides the default 10% behavior.

**46. Debugging: `spark.speculation=true` is set, but NO tasks are ever speculated, even though some tasks lag by hours.**
* **Issue:** The speculation thresholds are logically unreachable. For example, `spark.speculation.quantile` is set to `1.0`.
* **Fix:** Adjust quantile to `0.75` (start evaluating when 75% of tasks finish) and multiplier to `1.5` (speculate if a task is 1.5x slower than the median).

**47. Debugging: Using dynamic allocation, executors scale up to 100, but immediately scale down, then back up in an endless loop.**
* **Issue:** `spark.dynamicAllocation.executorIdleTimeout` is too aggressive (e.g., 10 seconds), causing executors to be killed between brief job pauses, triggering immediate re-requests.
* **Fix:** Increase `executorIdleTimeout` to 120s or 300s to keep executors alive through minor idle periods.

**48. Coding: How do you configure a `SparkSession` programmatically to use a custom pod template for the driver in Python?**
* **Answer:** 
```python
spark = SparkSession.builder \
    .config("spark.kubernetes.driver.podTemplateFile", "/path/to/driver-template.yaml") \
    .getOrCreate()
```

**49. Debugging: Your custom `TaskMetricsListener` causes the Spark Driver to crash with `java.lang.OutOfMemoryError`.**
* **Issue:** The listener is accumulating task metrics in a local `List` or `Map` on the Driver node but never clearing them. As millions of tasks finish, the list consumes all Driver heap memory.
* **Fix:** Only log/aggregate the metrics and discard the raw objects, or use a size-bound eviction cache.

**50. Debugging: A PySpark job using multiple UDFs is extremely slow and memory-intensive, despite Tungsten being enabled.**
* **Issue:** Standard Python UDFs force Spark to serialize data out of Tungsten's binary format, send it to a Python process via sockets, and deserialize it back, destroying performance.
* **Fix:** Refactor code to use native Spark SQL functions, or use Pandas UDFs (Vectorized UDFs) powered by Apache Arrow, which shares the in-memory columnar format natively.
