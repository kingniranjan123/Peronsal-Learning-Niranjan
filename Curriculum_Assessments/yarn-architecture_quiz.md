# YARN Architecture Assessment

## Part 1: True/False Questions

**1. T/F: In Spark on YARN, the ApplicationMaster's sole responsibility is to launch containers across the cluster.**
* **Answer:** False
* **Mastery Explanation:** The ApplicationMaster negotiates resource containers from the ResourceManager and then contacts the respective NodeManagers to launch them. It does not launch them directly.

**2. T/F: YARN's External Shuffle Service (ESS) is essential for Dynamic Resource Allocation to prevent shuffle data loss when idle executors are spun down.**
* **Answer:** True
* **Mastery Explanation:** When executors are dynamically de-allocated due to idleness, the ESS (a long-running NodeManager service) retains their shuffle files securely for downstream stages.

**3. T/F: The `spark.yarn.maxAppAttempts` configuration defines how many times an individual Spark executor will be restarted upon failure.**
* **Answer:** False
* **Mastery Explanation:** It dictates how many times YARN will retry the entire ApplicationMaster before marking the application as failed.

**4. T/F: Setting `spark.submit.deployMode` to `cluster` means the Spark Driver runs inside a YARN ApplicationMaster container.**
* **Answer:** True
* **Mastery Explanation:** In cluster mode, the driver is hosted inside the ApplicationMaster, allowing the application to persist even if the submitting client disconnects.

**5. T/F: The YARN NodeManager is responsible for deciding which application gets access to the cluster's CPU and Memory resources.**
* **Answer:** False
* **Mastery Explanation:** The ResourceManager is the central authority that orchestrates resource allocation. The NodeManager only monitors and manages resources on its specific local node.

**6. T/F: When an executor is caching RDD partitions, its idle timeout for dynamic allocation is governed by `spark.dynamicAllocation.cachedExecutorIdleTimeout` rather than the standard idle timeout.**
* **Answer:** True
* **Mastery Explanation:** Spark provides a separate timeout configuration to prevent premature eviction of valuable cached data, avoiding expensive re-evaluations.

**7. T/F: Spark executors on YARN can be constrained to specific hardware profiles using YARN Node Labels, provided the administrator has configured them.**
* **Answer:** True
* **Mastery Explanation:** Node Labels allow administrators to partition clusters. Spark can request specific labels using `spark.yarn.executor.nodeLabelExpression`.

**8. T/F: The `spark.yarn.executor.memoryOverhead` configuration is solely used to define the memory allocated to the YARN NodeManager daemon.**
* **Answer:** False
* **Mastery Explanation:** It accounts for JVM overhead, off-heap memory, and non-JVM processes (like Python UDFs) within the executor's specific YARN container.

**9. T/F: Spark's delay scheduling algorithm in YARN will indefinitely wait for a `NODE_LOCAL` container allocation before falling back to `RACK_LOCAL`.**
* **Answer:** False
* **Mastery Explanation:** It waits for a configurable duration defined by `spark.locality.wait` before degrading to a less optimal locality level.

**10. T/F: Python UDF worker processes consume memory that is counted against the executor's total YARN container memory limit.**
* **Answer:** True
* **Mastery Explanation:** Python workers run as separate processes outside the JVM but within the YARN container's boundary. They rely heavily on the `memoryOverhead` allocation.

## Part 2: Multiple Choice Questions

**11. Which component in the YARN architecture is responsible for tracking resource usage on a specific worker node and reporting it?**
A) ResourceManager
B) ApplicationMaster
C) NodeManager
D) Capacity Scheduler
* **Answer:** C
* **Mastery Explanation:** The NodeManager resides on every worker node, launches containers, and reports localized resource telemetry (CPU, RAM) back to the ResourceManager.

**12. Why is configuring `spark.yarn.executor.memoryOverhead` critical when running native or Python UDF-based Spark applications on YARN?**
A) It increases the JVM heap size allocated to the Python worker.
B) It prevents the NodeManager from terminating the container via SIGKILL when off-heap memory exceeds limits.
C) It allocates memory directly to the ApplicationMaster.
D) It moves GC operations off-heap.
* **Answer:** B
* **Mastery Explanation:** Native code and Python UDFs run outside the JVM heap but inside the YARN container. If they exceed `memoryOverhead`, YARN ruthlessly kills the container.

**13. If `spark.dynamicAllocation.enabled` is true, but `spark.shuffle.service.enabled` is false, what is the consequence?**
A) Executors will never scale down.
B) The application risks catastrophic shuffle data loss upon executor scale-down.
C) The External Shuffle Service will automatically start on demand.
D) The ResourceManager takes over shuffle storage.
* **Answer:** B
* **Mastery Explanation:** Without ESS, scaling down an executor destroys its local disk shuffle files, requiring expensive lineage recomputation.

**14. In Spark on YARN cluster mode, what happens if the edge node is abruptly disconnected?**
A) The entire Spark application fails immediately.
B) The ApplicationMaster fails, but executors run.
C) The Spark application continues running unaffected.
D) The ResourceManager pauses the application.
* **Answer:** C
* **Mastery Explanation:** In `cluster` mode, the Driver executes within the ApplicationMaster on a cluster node, making it immune to edge-node disconnects.

**15. What is the primary purpose of the `spark.locality.wait` configuration?**
A) To wait for YARN to restart a failed NodeManager.
B) To pause task scheduling briefly to secure a data-local container before degrading locality.
C) To delay the application start.
D) To wait for ESS file synchronization.
* **Answer:** B
* **Mastery Explanation:** Delay scheduling trades a few seconds of waiting for the immense performance gain of zero-network-transfer `NODE_LOCAL` data reads.

**16. Which JVM garbage collector is recommended to avoid full GC pauses that cause executors to miss YARN heartbeats?**
A) Parallel GC
B) CMS GC
C) G1GC
D) ZGC
* **Answer:** C
* **Mastery Explanation:** G1GC provides predictable pause times and concurrent marking, preventing the long Stop-The-World pauses typical in large heaps.

**17. What role does `spark.yarn.queue` play when submitting a Spark application?**
A) It determines the internal task scheduling queue (FIFO vs FAIR).
B) It routes the application to a specific YARN Scheduler queue for resource isolation.
C) It dictates the queue of HDFS blocks.
D) It configures ESS queue size.
* **Answer:** B
* **Mastery Explanation:** YARN administrators configure Capacity/Fair Schedulers with hierarchical queues to provide resource limits and guarantees for different tenants.

**18. When a YARN container is killed for exceeding memory limits, what is the direct cause?**
A) The Spark JVM heap exceeded `spark.executor.memory`.
B) The sum of JVM heap, off-heap memory, and non-JVM processes exceeded total container size.
C) The ApplicationMaster exceeded memory limits.
D) The node ran out of physical memory.
* **Answer:** B
* **Mastery Explanation:** YARN NodeManagers monitor the entire container's physical memory tree (RSS). If the sum exceeds `executor.memory` + `memoryOverhead`, it SIGKILLs the container.

**19. How does YARN's Capacity Scheduler handle container preemption?**
A) It pauses executors and saves state to HDFS.
B) It reclaims containers from over-limit queues by killing them.
C) It migrates running containers.
D) It prevents new launches but never kills existing containers.
* **Answer:** B
* **Mastery Explanation:** YARN forces preemption by killing containers. This makes the External Shuffle Service vital to prevent cascading data loss.

**20. What is the function of `spark.yarn.maxAppAttempts`?**
A) It dictates task failure limits.
B) It specifies how many times YARN will restart the ApplicationMaster on failure.
C) It controls shuffle retry limits.
D) It determines NodeManager restarts.
* **Answer:** B
* **Mastery Explanation:** This provides high availability for critical/streaming jobs. If the AM fails, YARN launches a new one without killing the entire job history.

**21. Who detects a localized executor container failure and requests a replacement in Spark on YARN?**
A) The YARN ResourceManager
B) The Spark ApplicationMaster
C) The External Shuffle Service
D) The NameNode
* **Answer:** B
* **Mastery Explanation:** The ApplicationMaster acts as the liaison, detecting executor failures via lost connections and negotiating replacements from the ResourceManager.

**22. Which configuration isolates a Spark ApplicationMaster to run only on a specific subset of nodes?**
A) `spark.yarn.am.nodeLabelExpression`
B) `spark.yarn.executor.nodeLabelExpression`
C) `spark.locality.wait.node`
D) `spark.driver.host`
* **Answer:** A
* **Mastery Explanation:** YARN Node Labels allow restricting container allocations to hardware profiles. The `.am.` prefix targets the ApplicationMaster container.

**23. What is the impact of tuning `-XX:InitiatingHeapOccupancyPercent=35`?**
A) Caps maximum heap usage at 35%.
B) Forces disk spillage at 35%.
C) Triggers the G1GC concurrent marking cycle earlier.
D) Allocates 35% for execution memory.
* **Answer:** C
* **Mastery Explanation:** By triggering concurrent marking at 35% heap occupancy, G1GC cleans up garbage proactively, leaving headroom and preventing massive STW full GC pauses.

**24. Which daemon orchestrates resource allocation across the entire YARN cluster?**
A) ApplicationMaster
B) NodeManager
C) ResourceManager
D) JobHistoryServer
* **Answer:** C
* **Mastery Explanation:** The ResourceManager is the global master daemon containing the Scheduler and ApplicationsManager.

**25. How do Python UDFs interact with the YARN container memory limit?**
A) They run inside the executor JVM heap.
B) They run on the Spark Driver.
C) They run in separate worker processes within the same YARN container.
D) They run in dedicated YARN containers.
* **Answer:** C
* **Mastery Explanation:** PySpark launches native Python processes for UDFs. These exist outside the JVM but inside the YARN container, necessitating tuned `memoryOverhead`.

## Part 3: Small Twist Questions

**26. Twist:** You change `spark.submit.deployMode` from `cluster` to `client`. You close your laptop while the job is running. What happens?
* **Answer:** The Spark application fails instantly.
* **Mastery Explanation:** In client mode, the Driver runs locally on your laptop. Closing it terminates the Driver JVM, breaking the application.

**27. Twist:** You set `spark.dynamicAllocation.cachedExecutorIdleTimeout` to `10s` (less than standard `executorIdleTimeout`).
* **Answer:** Executors caching valuable data will be killed *faster* than idle ones, causing massive recomputations.
* **Mastery Explanation:** The cached timeout must be significantly higher to protect in-memory RDD/DataFrames from premature eviction.

**28. Twist:** `spark.yarn.executor.memoryOverhead` is explicitly set to 200MB, but executor memory is 10GB.
* **Answer:** The container is highly likely to be killed by YARN for exceeding memory limits.
* **Mastery Explanation:** The default overhead is max(384MB, 10%). Hardcoding 200MB gives a 10GB JVM virtually zero off-heap breathing room.

**29. Twist:** You set `spark.locality.wait` to `0s`.
* **Answer:** Spark sacrifices data locality immediately, launching tasks on ANY node.
* **Mastery Explanation:** This results in fast scheduling but saturates cluster network bandwidth as tasks pull data from remote HDFS nodes.

**30. Twist:** A job is submitted with `spark.yarn.queue=marketing`. The `marketing` queue is at its hard max capacity limit.
* **Answer:** The application remains in `ACCEPTED` state indefinitely.
* **Mastery Explanation:** The ResourceManager will refuse to allocate the ApplicationMaster container until resources in the marketing queue free up.

**31. Twist:** You set `spark.yarn.executor.nodeLabelExpression="gpu_nodes"`, but no nodes in the cluster have this label.
* **Answer:** The ApplicationMaster will indefinitely request containers, stalling the application.
* **Mastery Explanation:** YARN cannot fulfill the exact label constraint, so no executors are launched.

**32. Twist:** `spark.shuffle.service.enabled` is `true`, but the YARN NodeManagers are not configured to run the shuffle auxiliary service.
* **Answer:** Executors fail to register with the ESS and the application fails.
* **Mastery Explanation:** Spark expects the ESS port to be open. Without it, shuffle reads/writes fail at the networking layer.

**33. Twist:** `spark.yarn.maxAppAttempts` is changed from 2 to 1 for a streaming pipeline.
* **Answer:** A single node failure hosting the ApplicationMaster will permanently kill the pipeline.
* **Mastery Explanation:** 1 attempt disables YARN's AM-level fault tolerance, making long-running applications brittle.

**34. Twist:** You allocate `spark.executor.memory="32g"` and `-XX:+UseParallelGC` instead of G1GC.
* **Answer:** The executor will likely suffer multi-second STW pauses.
* **Mastery Explanation:** ParallelGC scales poorly with massive heaps. The pause will cause the executor to miss NM heartbeats and be declared dead.

**35. Twist:** You set `spark.yarn.submit.waitAppCompletion` to `true` in a CI/CD pipeline script.
* **Answer:** The `spark-submit` process will block indefinitely until the job finishes on YARN.
* **Mastery Explanation:** This holds the CI/CD runner hostage instead of submitting asynchronously.

**36. Twist:** `spark.dynamicAllocation.minExecutors` is set to 100, and `maxExecutors` is set to 10.
* **Answer:** The application throws an exception and fails during initialization.
* **Mastery Explanation:** Spark validates that minExecutors <= maxExecutors.

**37. Twist:** You run a PyArrow Pandas UDF job without increasing `spark.yarn.executor.memoryOverhead`.
* **Answer:** The containers will be rapidly SIGKILLed by YARN.
* **Mastery Explanation:** PyArrow relies heavily on off-heap memory allocation which bypasses the JVM. It requires a massive `memoryOverhead` buffer.

**38. Twist:** `spark.dynamicAllocation.enabled=true`, but your workload consists of a single massive 100GB partition.
* **Answer:** Spark will only allocate 1 executor.
* **Mastery Explanation:** Dynamic allocation is driven by the pending task queue. 1 partition = 1 task = 1 executor.

**39. Twist:** You use cluster mode, set `spark.yarn.am.memory="512m"`, and broadcast a 300MB table.
* **Answer:** The ApplicationMaster container crashes with an OutOfMemoryError.
* **Mastery Explanation:** In cluster mode, the AM hosts the Driver. 512MB is insufficient for the Driver JVM overhead plus a 300MB broadcast variable.

**40. Twist:** A preemption policy is aggressive, and `spark.yarn.shuffle.stopOnFailure` is `true`.
* **Answer:** The Spark job will deliberately fail when an executor is preempted and shuffle data is lost.
* **Mastery Explanation:** Instead of recomputing lost lineage (Spark's default behavior), this config forces a fast-fail.

## Part 4: Coding & Debugging Questions

**41. Debugging:** An application fails with "YARN Container killed... 4.5GB of 4.3GB used." `spark.executor.memory` is 4g. What is the fix for a PySpark job?
* **Answer:** Increase `spark.yarn.executor.memoryOverhead` (e.g., to `1g` or `2g`).
* **Mastery Explanation:** The 300MB default overhead is insufficient for PySpark worker processes. Increasing overhead expands the physical YARN container limit.

**42. Debugging:** A Spark job on YARN is stuck in `ACCEPTED` state. YARN UI shows 0 available vCores. Root cause?
* **Answer:** The cluster or queue is starved of resources.
* **Mastery Explanation:** The ResourceManager cannot even allocate the minimal resources required to launch the ApplicationMaster container.

**43. Debugging:** Dynamic allocation scales up to 100 executors, but 90 are killed after 60 seconds, then requested again 2 minutes later.
* **Answer:** `spark.dynamicAllocation.executorIdleTimeout` is too short.
* **Mastery Explanation:** The aggressive 60s timeout causes thrashing for workloads with intermittent bursty task submissions. Increase the timeout.

**44. Debugging:** RDDs are explicitly cached, but UI shows constant recomputation and executors spinning down.
* **Answer:** `spark.dynamicAllocation.cachedExecutorIdleTimeout` is missing or too low.
* **Mastery Explanation:** Without this, executors holding cached blocks are killed using the standard idle timeout, throwing away the cache.

**45. Debugging:** You specify `spark.locality.wait="0s"`. Network I/O is maxed, CPU is idle, and tasks are slow.
* **Answer:** Setting wait to 0s destroyed data locality.
* **Mastery Explanation:** Tasks are instantly scheduled on non-local nodes, forcing massive HDFS network reads instead of fast local disk reads.

**46. Debugging:** ML PySpark logs show `java.lang.OutOfMemoryError: GC overhead limit exceeded`. `memoryOverhead` is 8g. Fix?
* **Answer:** Increase `spark.executor.memory` (on-heap) or reduce `spark.memory.fraction`.
* **Mastery Explanation:** The OOM is a JVM heap issue, not a YARN container issue. The JVM is spending 98% of its time in GC due to heap exhaustion.

**47. Debugging:** `spark-submit` throws `IllegalArgumentException: Required executor memory... is above the max threshold of this cluster!`
* **Answer:** The requested container size exceeds YARN's `yarn.scheduler.maximum-allocation-mb`.
* **Mastery Explanation:** You cannot request a container larger than what YARN administrators have globally allowed per node. Reduce executor sizes.

**48. Debugging:** A user complains their application fails when they drop off VPN. Their script shows: `spark-submit --master yarn --deploy-mode client ...`
* **Answer:** Change `--deploy-mode client` to `--deploy-mode cluster`.
* **Mastery Explanation:** Moving the Driver from their local VPN-dependent laptop into the YARN cluster's AM ensures network resilience.

**49. Debugging:** `spark.yarn.executor.nodeLabelExpression="gpu"` is set, but tasks fail missing GPU libraries. YARN UI shows allocations on non-GPU nodes.
* **Answer:** The Capacity Scheduler queue's label exclusivity is misconfigured.
* **Mastery Explanation:** YARN might fall back to non-labeled nodes if exclusivity isn't enforced, launching GPU tasks on CPU-only nodes.

**50. Debugging:** Long GC pauses (10-15s) are killing executors. Config: `-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=85`. Flaw?
* **Answer:** `InitiatingHeapOccupancyPercent=85` is too high.
* **Mastery Explanation:** It delays concurrent marking until the heap is 85% full. Under heavy allocation, it runs out of heap during marking, causing a massive fallback Full GC. Reduce to 35-45.
