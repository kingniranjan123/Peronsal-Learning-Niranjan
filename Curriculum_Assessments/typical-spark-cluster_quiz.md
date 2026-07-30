# Typical Spark Cluster - Elite Assessment

## 1. True/False Questions

**1. True/False: In a typical Spark cluster running on YARN in cluster mode, the driver runs within the ApplicationMaster.**
* **Answer:** True. 
* **Mastery Explanation:** In cluster mode, YARN launches the ApplicationMaster on a NodeManager, and the Spark driver runs inside this AM process, keeping it closer to the cluster and untethered from the client submit machine.

**2. True/False: A Spark driver can dynamically scale the number of executors it uses regardless of the Cluster Manager, even if dynamic allocation is disabled.**
* **Answer:** False. 
* **Mastery Explanation:** Dynamic allocation must be explicitly enabled (`spark.dynamicAllocation.enabled=true`) and requires an External Shuffle Service to safely decommission executors without losing shuffle data.

**3. True/False: The Tungsten engine bypasses the JVM object model and stores data directly in off-heap or on-heap memory using a binary format.**
* **Answer:** True. 
* **Mastery Explanation:** Tungsten uses a custom binary format to reduce Garbage Collection (GC) overhead and memory footprint, manipulating raw memory directly via `sun.misc.Unsafe`.

**4. True/False: When an executor throws an OutOfMemoryError, the entire Spark application immediately fails.**
* **Answer:** False. 
* **Mastery Explanation:** Spark is designed for fault tolerance. The Cluster Manager can relaunch a failed executor, and the Driver's DAGScheduler will re-assign the tasks that were running on the lost executor to other available executors.

**5. True/False: Spark's block manager always persists RDD partitions to HDFS if memory is insufficient.**
* **Answer:** False. 
* **Mastery Explanation:** RDD persistence behavior depends strictly on the chosen StorageLevel (e.g., `MEMORY_AND_DISK`). If `MEMORY_ONLY` is used, data is simply recomputed from lineage, not written to HDFS or local disk.

**6. True/False: If a Spark executor dies, the External Shuffle Service on that node also dies, leading to shuffle data loss.**
* **Answer:** False. 
* **Mastery Explanation:** The External Shuffle Service runs as an independent daemon (often inside the YARN NodeManager), so shuffle files remain accessible to reducers even if the specific executor that wrote them has terminated.

**7. True/False: In Spark's physical execution plan, a SortMergeJoin always requires a full shuffle of both datasets.**
* **Answer:** False. 
* **Mastery Explanation:** If the datasets are already co-partitioned (e.g., appropriately bucketed by the join key), the Catalyst optimizer will eliminate the Exchange (shuffle) step, executing the join locally.

**8. True/False: Setting `spark.executor.memory` higher always improves GC performance by reducing the frequency of collections.**
* **Answer:** False. 
* **Mastery Explanation:** Extremely large JVM heap sizes (e.g., >32GB) can lead to massive Garbage Collection pause times (Stop-The-World), degrading overall performance. It's often better to have multiple smaller executors or utilize off-heap memory.

**9. True/False: Broadcast variables are distributed to workers using an HDFS-based distribution mechanism in modern Spark versions.**
* **Answer:** False. 
* **Mastery Explanation:** Modern Spark utilizes a BitTorrent-like peer-to-peer protocol (TorrentBroadcast) to distribute broadcast variables, avoiding a single network bottleneck at the driver.

**10. True/False: A typical Spark cluster avoids network shuffle overhead when applying `reduceByKey` by first performing a map-side combine.**
* **Answer:** True. 
* **Mastery Explanation:** `reduceByKey` triggers a map-side aggregation (HashAggregate) before the shuffle, drastically reducing the volume of data sent across the network, unlike `groupByKey`.

---

## 2. Multiple Choice Questions

**11. Which Spark component is responsible for transforming a logical plan into a physical plan?**
A. DAGScheduler
B. TaskScheduler
C. Catalyst Optimizer
D. BlockManager
* **Correct:** C. 
* **Mastery Explanation:** The Catalyst Optimizer applies rule-based and cost-based optimizations to transform parsed logical plans into optimized physical plans, ultimately generating Java bytecode via Tungsten.

**12. In a typical Spark cluster on Kubernetes, what is the role of the Driver pod?**
A. Runs the Kubelet
B. Coordinates tasks and requests Executor pods from the API server
C. Manages persistent volumes
D. Runs the YARN ResourceManager
* **Correct:** B. 
* **Mastery Explanation:** The Driver acts as a Kubernetes client natively, directly interacting with the Kubernetes API server to request, manage, and monitor Executor pods.

**13. Which of the following best describes Spark's Tungsten Project memory management?**
A. It relies entirely on Java Garbage Collection.
B. It serializes objects into Java standard format.
C. It uses a page-based memory manager acting like an OS to manage binary data.
D. It stores data on disk by default.
* **Correct:** C. 
* **Mastery Explanation:** Tungsten manages memory using a page-based system (similar to an OS) to store data in a highly optimized custom binary format, bypassing JVM GC overhead entirely for managed memory.

**14. What happens when a Spark shuffle block exceeds the `spark.reducer.maxSizeInFlight` configuration?**
A. The job fails with an OOM.
B. The reducer fetches data in multiple smaller chunks.
C. The mapper splits the block.
D. The block is discarded.
* **Correct:** B. 
* **Mastery Explanation:** Spark limits the amount of in-flight shuffle data to avoid reducer OOMs. Reducers fetch larger blocks dynamically in manageable chunks corresponding to this limit.

**15. If a Spark job suffers from severe data skew, which technique is most effective?**
A. Increasing the number of executors
B. Salting the keys before aggregation/join
C. Changing cluster manager from YARN to Mesos
D. Using MEMORY_ONLY storage level
* **Correct:** B. 
* **Mastery Explanation:** Salting adds randomness to the join/aggregation keys, distributing heavily skewed keys across multiple partitions and preventing a single executor task from bottlenecking.

**16. What is the primary function of the DAGScheduler?**
A. Launching containers on worker nodes
B. Splitting the logical plan into stages separated by shuffle boundaries
C. Executing tasks on executors
D. Managing block locations
* **Correct:** B. 
* **Mastery Explanation:** The DAGScheduler builds a Directed Acyclic Graph of stages from the RDD lineage, creating new stage boundaries wherever an Exchange (shuffle) is required.

**17. When configuring `spark.memory.fraction`, what does the remaining fraction (1 - fraction) represent?**
A. Storage memory for caching
B. Execution memory for shuffles
C. User memory for user data structures and internal metadata
D. Off-heap memory
* **Correct:** C. 
* **Mastery Explanation:** The remaining fraction is reserved for user data structures, internal Spark metadata, and safeguarding against OOMs in Spark's Unified Memory Management.

**18. In Spark's Unified Memory Management, what happens when Execution Memory requires space, but Storage Memory is full of cached data?**
A. The application throws an OOM.
B. Execution memory evicts storage blocks until it reaches its minimum threshold.
C. The task fails and retries.
D. Execution memory spills to disk immediately.
* **Correct:** B. 
* **Mastery Explanation:** Execution memory can evict blocks from storage memory if it needs space (up to a certain threshold), but storage memory cannot evict execution memory, prioritizing job completion over caching.

**19. Which broadcast join condition prevents a BroadcastHashJoin from being chosen by Catalyst?**
A. One table is smaller than `spark.sql.autoBroadcastJoinThreshold`.
B. The join type is a Full Outer Join.
C. The tables are stored in Parquet format.
D. The cluster has high memory capacity.
* **Correct:** B. 
* **Mastery Explanation:** Broadcast joins are not supported for Full Outer Joins because the absence of matches on the broadcasted side cannot be accurately determined across all partitions on isolated executors.

**20. What is the role of the `MapOutputTracker`?**
A. Tracks the location of cached RDDs.
B. Informs reducers about the locations of map output files for shuffling.
C. Tracks the standard output of map tasks.
D. Monitors the progress of MapReduce jobs.
* **Correct:** B. 
* **Mastery Explanation:** The MapOutputTracker runs on the driver and executors to keep track of where shuffle map tasks have written their output, serving this location info to reduce tasks.

**21. Why might an application prefer `repartition` over `coalesce`?**
A. `coalesce` always triggers a full shuffle.
B. `repartition` avoids data movement across nodes.
C. To achieve a perfectly balanced distribution of data.
D. `coalesce` cannot decrease partitions.
* **Correct:** C. 
* **Mastery Explanation:** `coalesce` avoids shuffles when decreasing partitions but can lead to heavily skewed partitions. `repartition` forces a full round-robin shuffle, ensuring perfectly balanced partitions.

**22. Which Cluster Manager natively supports fine-grained resource sharing?**
A. YARN
B. Kubernetes
C. Mesos
D. Standalone
* **Correct:** C. 
* **Mastery Explanation:** Apache Mesos supports fine-grained mode (though deprecated in modern Spark) allowing resource allocation at the individual task level, unlike YARN's coarse-grained container allocation.

**23. What configuration is critical to enable Spark to recover gracefully from a Driver failure in Standalone mode?**
A. `spark.deploy.recoveryMode`
B. `spark.driver.memory`
C. `spark.yarn.maxAppAttempts`
D. `spark.kubernetes.restartPolicy`
* **Correct:** A. 
* **Mastery Explanation:** In Standalone mode, configuring `spark.deploy.recoveryMode` with ZooKeeper enables High Availability (HA) for the Spark Master, allowing it to recover driver and cluster state upon failure.

**24. How does Spark prevent Straggler tasks from delaying job completion?**
A. By preempting the task and killing it.
B. By launching Speculative tasks.
C. By increasing the memory of the straggling executor.
D. By skipping the data partition.
* **Correct:** B. 
* **Mastery Explanation:** Spark uses Speculative Execution (`spark.speculation=true`) to launch duplicate copies of slow-running tasks on other nodes, taking the result of the first to finish.

**25. What is a "Shuffle Fetch Failed" exception primarily indicative of?**
A. The reducer's memory is exhausted.
B. The map output file cannot be retrieved, often due to an executor crash or network issue.
C. The Catalyst optimizer failed to generate a physical plan.
D. The Broadcast variable exceeded the limit.
* **Correct:** B. 
* **Mastery Explanation:** A Shuffle Fetch Failed occurs when a reducer attempts to read shuffle data from a remote node and fails, prompting the DAGScheduler to resubmit the missing map stage to regenerate the data.

---

## 3. Small Twist Questions

**26. Twist: You change a `groupByKey()` to `reduceByKey()`. How does this change the physical plan?**
* **Answer:** `reduceByKey` adds a map-side combine phase (HashAggregate) before the Exchange. 
* **Mastery Explanation:** `groupByKey` sends all raw data across the network. `reduceByKey` pre-aggregates locally on the mapper side, significantly reducing shuffle write/read metrics and network congestion.

**27. Twist: You increase `spark.sql.shuffle.partitions` from 200 to 2000 for a 1GB dataset. What is the immediate impact?**
* **Answer:** Task overhead dominates, leading to drastically slower execution. 
* **Mastery Explanation:** Each partition handles ~500KB. The overhead of scheduling 2000 tasks and managing small shuffle files far outweighs the parallelism benefits.

**28. Twist: You switch from YARN client mode to YARN cluster mode. Where does the SparkContext initialization occur?**
* **Answer:** On the ApplicationMaster inside a NodeManager container. 
* **Mastery Explanation:** In cluster mode, the driver runs on the cluster itself, eliminating network latency between the driver and the cluster, and decoupling the job lifecycle from the client session.

**29. Twist: You enable `spark.speculation`, but your tasks are performing database inserts. What is the risk?**
* **Answer:** Duplicate data insertions. 
* **Mastery Explanation:** Speculation runs multiple copies of a task. If operations are not strictly idempotent, multiple tasks might write the same data to the external database before the slower one is killed.

**30. Twist: You cache an RDD, but immediately call a DataFrame action. Why isn't the RDD cache used?**
* **Answer:** DataFrames use logical and physical plans that don't inherently share the underlying RDD BlockManager cache unless explicitly converted. 
* **Mastery Explanation:** If you query via the DataFrame API, Catalyst processes the DataFrame lineage from the source, ignoring the RDD cache. You must use `df.cache()`.

**31. Twist: You use `SortMergeJoin` but one dataset is heavily skewed on the join key. What happens to the Executor?**
* **Answer:** OOM or massive disk spill on the reducer processing the skewed key. 
* **Mastery Explanation:** Even with SortMergeJoin, all records for a specific key go to a single partition. A skewed key forces one task to process disproportionately large amounts of data.

**32. Twist: You set `spark.memory.offHeap.enabled=true` but forget to increase the container memory overhead in YARN. What happens?**
* **Answer:** YARN kills the executor container (Exit Code 137/143). 
* **Mastery Explanation:** Off-heap memory counts against the container's total physical memory limit. If the total memory (heap + off-heap) exceeds the YARN container allocation, YARN's NodeManager will rigidly terminate it.

**33. Twist: You change a DataFrame write format from CSV to Parquet. How does this affect subsequent reads?**
* **Answer:** Subsequent reads can utilize partition pruning and predicate pushdown. 
* **Mastery Explanation:** Parquet is a columnar format containing metadata/stats. Spark's Catalyst optimizer pushes down filters to skip reading irrelevant columns and row groups entirely.

**34. Twist: You replace `count()` with `take(1)` to check if a massive DataFrame is empty. Why is it faster?**
* **Answer:** `take(1)` only computes one partition (or enough to get 1 record). 
* **Mastery Explanation:** `count()` forces a full evaluation and shuffle of all partitions. `take(1)` evaluates lazily, scanning partitions sequentially until it finds a record, skipping the rest.

**35. Twist: You configure `spark.executor.cores=5` instead of 1. How does this impact memory usage per task?**
* **Answer:** Each task effectively gets ~1/5th of the executor's memory pool. 
* **Mastery Explanation:** Multiple cores mean multiple concurrent tasks sharing the same JVM heap. If tasks are memory-intensive, this can lead to OOM errors compared to running 1 task per executor.

**36. Twist: You broadcast a 2GB table, but the driver crashes with an OOM. Why?**
* **Answer:** The driver must collect the entire table into its local memory before broadcasting. 
* **Mastery Explanation:** `broadcast()` brings all partitions to the driver first. A 2GB table easily exhausts a default 1GB driver memory (`spark.driver.memory`).

**37. Twist: You use `checkpoint()` instead of `cache()`. What happens to the RDD lineage?**
* **Answer:** The lineage is completely truncated. 
* **Mastery Explanation:** Checkpointing writes data to reliable storage (HDFS) and cuts the DAG lineage to prevent stack overflows on long lineage, whereas caching retains the lineage so data can be recomputed if a block is lost.

**38. Twist: You change the partition column in `window()` to a column with only 3 unique values in a 100-node cluster. What is the cluster utilization?**
* **Answer:** Only 3 nodes/tasks will process the data. 
* **Mastery Explanation:** Window functions require all data for a partition key to be collocated on a single node. A key with low cardinality drastically limits maximum parallelism.

**39. Twist: You run `df.orderBy("col").limit(10)` vs `df.limit(10).orderBy("col")`. What is the difference?**
* **Answer:** The first sorts the entire dataset globally. The second takes 10 random rows and sorts only them. 
* **Mastery Explanation:** Order of operations matters in Catalyst. Global sort requires a full shuffle, while limit first stops execution early, vastly changing performance.

**40. Twist: You enable `spark.sql.adaptive.enabled=true` (AQE) and join a 10GB table with a 50MB table. What optimization occurs at runtime?**
* **Answer:** AQE dynamically switches the SortMergeJoin to a BroadcastHashJoin. 
* **Mastery Explanation:** Even if static stats estimated the table larger, AQE evaluates the actual size after earlier stages complete and converts the physical plan dynamically at runtime.

---

## 4. Coding & Debugging Questions

**41. Debugging: An application fails with "java.lang.OutOfMemoryError: GC overhead limit exceeded". What is the root cause?**
* **Answer:** The JVM is spending too much time garbage collecting and freeing very little memory. 
* **Mastery Explanation:** This happens when the application creates massive amounts of intermediate objects (e.g., using Python UDFs instead of Spark SQL native functions). Fix: Use native functions or increase heap space.

**42. Debugging: You observe a stage with 200 tasks. 199 tasks finish in 5 seconds, 1 task takes 10 minutes. Diagnosis?**
* **Answer:** Data Skew. 
* **Mastery Explanation:** One partition received significantly more data than others during a shuffle exchange. Fix: Salting, increasing partitions, or using AQE skew join optimization.

**43. Code: `rdd.map(lambda x: db_connection.write(x))` fails with a serialization error. Why?**
* **Answer:** The `db_connection` object is created on the driver and cannot be serialized/sent to executors over the network. 
* **Mastery Explanation:** Network connections, sockets, and file handlers cannot be serialized. Fix: Use `mapPartitions` and initialize the connection inside the partition function on the executor.

**44. Debugging: You see high network I/O and executor OOMs during `df1.join(df2, "id")`. `df2` is 100MB. How do you fix it?**
* **Answer:** Use `broadcast(df2)`. 
* **Mastery Explanation:** The standard join causes a massive shuffle of `df1`. Broadcasting the small `df2` avoids shuffling `df1` entirely, resolving both network bottlenecks and memory issues.

**45. Code: A streaming job processes Kafka messages, but offsets are lost on driver restart. Fix?**
* **Answer:** Enable Checkpointing (`ssc.checkpoint(dir)`) or manually commit offsets back to Kafka. 
* **Mastery Explanation:** Spark Streaming relies on WAL (Write Ahead Logs) or external offset management to achieve exactly-once semantics upon driver failure.

**46. Debugging: A Spark SQL query with multiple `withColumn` operations is extremely slow to compile, before tasks even run.**
* **Answer:** Catalyst optimizer bottleneck (Lineage explosion). 
* **Mastery Explanation:** Iteratively chaining hundreds of `withColumn` calls creates a massive logical plan that Catalyst struggles to parse and optimize. Fix: Use `select()` with multiple columns at once.

**47. Code: `df.repartition(1).write.csv("out")` is taking forever and causing OOM. Alternative?**
* **Answer:** `df.coalesce(1).write.csv("out")` 
* **Mastery Explanation:** `repartition(1)` forces all data through a full shuffle onto a single node, blowing up memory. `coalesce` avoids the shuffle, though it still forces a single node. A better approach is writing to multiple files and merging externally.

**48. Debugging: Executors are being killed by YARN with "Container killed by YARN for exceeding memory limits". Heap is at 4GB, container size is 4.5GB.**
* **Answer:** PySpark/Off-heap overhead exceeding the 0.5GB allowance. 
* **Mastery Explanation:** Python workers (in PySpark) or native memory allocations happen outside the JVM heap. Fix: Increase `spark.executor.memoryOverhead`.

**49. Code: `df.filter(df.id == 5).count()` reads the entire 1TB Parquet file instead of pushing down filters. Why?**
* **Answer:** Type mismatch or missing statistics. 
* **Mastery Explanation:** Predicate pushdown fails if there's a type mismatch (e.g., query uses String vs Parquet schema Int) preventing the reader from safely utilizing metadata blocks.

**50. Debugging: A user complains their application is stuck in the "ACCEPTED" state on a busy YARN cluster.**
* **Answer:** Resource starvation. 
* **Mastery Explanation:** The ApplicationMaster is requesting executor containers, but the YARN queues are full. Fix: Kill stagnant jobs, use dynamic allocation, or submit to a different YARN queue with capacity.
