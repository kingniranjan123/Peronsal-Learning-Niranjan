# Spark Web UI - Elite Technical Assessment

## Part 1: True/False Questions (10 Questions)

**1. True or False:** The "Storage Memory" displayed in the Spark Web UI's Executors tab represents the absolute maximum amount of RAM available for caching RDDs, and once this limit is reached, Spark will inevitably throw an OutOfMemoryError.
**Answer:** False. 
**Mastery Explanation:** Storage memory in unified memory management (default since 1.6) can borrow from Execution memory if execution is not using it, and vice versa. When the storage memory is full, Spark evicts blocks based on LRU (Least Recently Used) to disk or simply drops them (if StorageLevel does not specify disk), rather than throwing an OOM.

**2. True or False:** A high "Scheduler Delay" compared to actual task computation time in the Stages tab is typically a symptom of tasks being too fine-grained (too many small partitions).
**Answer:** True. 
**Mastery Explanation:** Scheduler delay measures the time a task waits to be scheduled plus the time to deserialize the task and serialize results. If tasks are extremely short (e.g., a few milliseconds) but there are millions of them, the driver spends more time scheduling and communicating than actual execution, dominating the total stage time.

**3. True or False:** The Spark Web UI SQL tab displays the Physical Plan, but the Catalyst Optimizer’s Logical Plan is only accessible via the `explain()` method in code and never visible in the UI.
**Answer:** False. 
**Mastery Explanation:** The SQL tab shows a visual representation of the Physical Plan, but clicking on the "Details" section expands a text block that includes the Parsed Logical Plan, Analyzed Logical Plan, Optimized Logical Plan, and Physical Plan.

**4. True or False:** If a task in the Spark Web UI shows a large amount of "Shuffle Read Blocked Time," it indicates that the executors were starved of CPU cycles during the reduce phase.
**Answer:** False. 
**Mastery Explanation:** Shuffle Read Blocked Time (or Fetch Wait Time) indicates that the reducer is blocked waiting to fetch shuffle data over the network from the mappers. This typically implies network congestion, disk I/O bottlenecks on the mapper side, or GC pauses on the mapper side preventing it from serving data, not CPU starvation on the reducer.

**5. True or False:** The Thread Dump available in the Executors tab of the Spark Web UI can be used to diagnose deadlocks within custom UDFs running on executors.
**Answer:** True. 
**Mastery Explanation:** The Thread Dump feature takes a snapshot of the JVM threads on a specific executor. If a custom UDF introduces a deadlock or an infinite loop, analyzing the thread dump will reveal threads stuck in BLOCKED or RUNNABLE state within the user's UDF code.

**6. True or False:** "Shuffle Spill (Memory)" is always strictly greater than or equal to "Shuffle Spill (Disk)" in the Spark Web UI because data must be uncompressed in memory before spilling to disk.
**Answer:** True. 
**Mastery Explanation:** "Shuffle Spill (Memory)" represents the size of the deserialized, uncompressed data in memory that was spilled, whereas "Shuffle Spill (Disk)" is the size of the serialized and compressed data written to disk. Therefore, the memory metric is almost always larger.

**7. True or False:** A "skipped" stage in the Spark Web UI indicates a bug where the DAGScheduler failed to submit the stage.
**Answer:** False. 
**Mastery Explanation:** A skipped stage simply means that the data required for that stage's output partitions is already computed and available (e.g., cached in memory/disk from a previous action, or fetched from a checkpoint). Spark intelligently skips recomputing it.

**8. True or False:** In the SQL tab, a `WholeStageCodegen` block encapsulates multiple physical operators into a single Java function, and the metrics for operators inside it are always aggregated at the `WholeStageCodegen` level, meaning you cannot see individual operator timing.
**Answer:** False. 
**Mastery Explanation:** While `WholeStageCodegen` fuses operators into a single function for execution efficiency (Tungsten), the Spark UI still instruments and displays specific metrics (like rows output, spill size) for individual operators (like Filter or Project) *inside* the codegen block.

**9. True or False:** The "GC Time" metric in the task details includes both Minor GC (Young Generation) and Major GC (Old Generation) pauses experienced by the executor JVM during the task execution.
**Answer:** True. 
**Mastery Explanation:** Spark relies on standard JVM memory management JMX beans to report GC time. It aggregates all stop-the-world GC pauses (both minor and major) that occur on the executor thread running the specific task.

**10. True or False:** If the Environment tab shows `spark.dynamicAllocation.enabled = true`, the Web UI's Executors tab will only ever show the maximum number of executors configured, regardless of current load.
**Answer:** False. 
**Mastery Explanation:** Dynamic allocation allows Spark to add and remove executors based on workload. The Executors tab will dynamically update to show active executors, dead executors, and the fluctuating total, reflecting the real-time scaling of the application.

## Part 2: Multiple Choice Questions (15 Questions)

**11. Which metric in the Spark Web UI Stages tab is the strongest indicator of a data skew problem?**
A) High average GC time across all tasks
B) A large discrepancy between the 75th percentile and Max for Task Duration and Shuffle Read Size
C) High Shuffle Write Time uniformly across all tasks
D) Large number of skipped stages
**Answer:** B
**Mastery Explanation:** Data skew occurs when a few partitions process significantly more data than others. This manifests in the UI as the majority of tasks finishing quickly (low median/75th percentile), while a few tasks (the Max) take an exceptionally long time and read massive amounts of shuffle data.

**12. When examining an execution plan in the SQL tab, you notice a `SortMergeJoin`. What prior physical operator must immediately precede it for both input data streams if the data is not already partitioned correctly?**
A) BroadcastExchange
B) HashAggregate
C) Exchange (hashpartitioning)
D) Coalesce
**Answer:** C
**Mastery Explanation:** A `SortMergeJoin` requires both sides of the join to be co-partitioned (hashed on the join keys) and sorted within those partitions. If they aren't, Spark inserts an `Exchange hashpartitioning` (a shuffle) to repartition the data, followed by a `Sort` operator before the actual join.

**13. In the Storage tab, you see an RDD cached with Storage Level `Memory Deserialized 1x Replicated`. What is the primary performance trade-off of this level compared to `Memory Serialized 1x Replicated`?**
A) Slower read times but lower memory footprint.
B) Faster read times but higher memory footprint.
C) Faster GC times but higher CPU usage.
D) Slower write times but faster network transfers.
**Answer:** B
**Mastery Explanation:** Deserialized storage keeps Java objects in memory, which allows for very fast reads since no deserialization is needed. However, Java object overhead is significant, causing a much larger memory footprint and potentially higher GC pressure compared to serialized storage (which stores data as byte arrays).

**14. What does a red-colored task in the Spark UI Task timeline indicate?**
A) The task experienced a JVM OutOfMemoryError.
B) The task failed and was aborted/retried.
C) The task is currently blocked waiting for shuffle fetches.
D) The task execution time exceeded a predefined threshold.
**Answer:** B
**Mastery Explanation:** In the timeline view, green denotes successful execution, and red specifically highlights tasks that failed and had to be retried (e.g., due to a FetchFailedException, OOM, or user code exception).

**15. You are debugging an OOM issue. In the Executors tab, which two columns provide the best immediate insight into whether the OOM is due to excessive caching versus excessive memory used during transformations (like joins/aggregations)?**
A) Shuffle Read vs. Shuffle Write
B) GC Time vs. Task Time
C) Storage Memory vs. Active Tasks
D) Storage Memory vs. Execution Memory (often inferred via JVM heap usage compared to Storage)
**Answer:** D
**Mastery Explanation:** Storage memory tracks cached RDDs/DataFrames. Execution memory (used for shuffles, joins, sorts, aggregations) shares the same unified pool. If Storage Memory is near its limit, caching is the culprit. If Storage is low but the JVM runs out of heap, Execution memory (or user object creation) is overflowing.

**16. In the SQL tab, a `BroadcastHashJoin` is observed, but the job is failing with OutOfMemory errors on the Executors. What is the most likely cause?**
A) The small table being broadcasted is larger than `spark.sql.autoBroadcastJoinThreshold`.
B) The driver ran out of memory collecting the small table.
C) The large table is severely skewed.
D) The broadcasted table, when deserialized on the executors, exceeds the executor's execution memory limit.
**Answer:** D
**Mastery Explanation:** Even if the table fits within the broadcast threshold and the driver's memory, when it arrives at the executor, it must be deserialized into a hash table in memory for the join. If this uncompressed hash table exceeds the available executor memory, an OOM occurs.

**17. What is the significance of the "Locality Level" column in the Stages tab (e.g., PROCESS_LOCAL, NODE_LOCAL, RACK_LOCAL, ANY)?**
A) It dictates where the resulting output data will be written to HDFS.
B) It indicates how close the executor computing the task was to the data it needed to read.
C) It represents the network topology configured in Mesos/YARN.
D) It shows the replication factor of the underlying block in HDFS.
**Answer:** B
**Mastery Explanation:** Locality level shows Spark's data locality optimization. PROCESS_LOCAL means data was in the same JVM (e.g., cached). NODE_LOCAL means on the same machine but different JVM or on local disk. ANY means data had to be pulled across the network from a different node.

**18. You notice in the Environment tab that `spark.executor.memory` is set to 8g, but in the Executors tab, the "Storage Memory" limit is only around 4.3g. Why?**
A) YARN overhead automatically deducts 3.7g.
B) The OS requires 50% of the memory.
C) Spark reserves memory for JVM overhead, user data structures, and splits the remainder based on `spark.memory.fraction` (default 0.6).
D) The executor failed to allocate the full amount from the resource manager.
**Answer:** C
**Mastery Explanation:** By default, Spark unified memory reserves 300MB. Of the remaining memory, `spark.memory.fraction` (default 0.6) is allocated for Spark execution and storage. Thus, (8192 - 300) * 0.6 = ~4735MB is the maximum unified memory space available, explaining the ~4.3g - 4.7g limit seen.

**19. What does the "Spill (Memory)" metric explicitly represent in the context of a Sort operator?**
A) The amount of data read from the OS page cache.
B) The in-memory size of the collection that was forced to spill to disk because it exceeded execution memory limits.
C) The amount of data stored in the off-heap Tungsten memory allocator.
D) The memory leaked by custom UDFs during the sort.
**Answer:** B
**Mastery Explanation:** When an operator like Sort or HashAggregate runs out of execution memory, it spills its internal data structures (like arrays or hash maps) to disk. "Spill (Memory)" estimates how much space those structures occupied in RAM before they were serialized and written (Spill Disk).

**20. In the SQL tab, you hover over a node and see a metric "peak memory". This metric is primarily enabled and tracked because of which internal Spark architectural feature?**
A) Project Tungsten (Memory Manager)
B) Catalyst Optimizer
C) RDD Lineage Graph
D) Netty RPC layer
**Answer:** A
**Mastery Explanation:** Project Tungsten introduced explicit memory management, operating directly on binary data. Because Tungsten tracks memory allocation precisely (using logical pages), it can report the exact "peak memory" used by specific execution operators (like aggregations and sorts) to the UI.

**21. A job fails with `FetchFailedException`. Where in the Spark UI is the FIRST place you should look to diagnose the root cause?**
A) The SQL tab, to check for Cartesian products.
B) The Executors tab, to look for a specific executor that was marked as "Dead" or lost.
C) The Environment tab, to check `spark.network.timeout`.
D) The Storage tab, to see if cached data was evicted.
**Answer:** B
**Mastery Explanation:** A `FetchFailedException` occurs when a reducer tries to pull shuffle data from a mapper, but the mapper's executor is unreachable. The most common cause is that the mapper executor died (often due to OOM). The Executors tab will show which executor was lost and when.

**22. Which UI tab is most useful for identifying if your Spark Streaming (DStream) application is processing data slower than it is receiving it?**
A) Stages Tab
B) Environment Tab
C) Streaming Tab (Specifically, the Processing Time vs. Scheduling Delay graphs)
D) Executors Tab
**Answer:** C
**Mastery Explanation:** The Streaming tab provides specific metrics for micro-batch processing. If the "Scheduling Delay" (time a batch waits in the queue before processing starts) is consistently increasing, it indicates the processing rate is slower than the ingestion rate, leading to an unstable streaming app.

**23. Under the "Jobs" tab, what does a "Job" correlate to in Spark code?**
A) A single transformation (like `map`).
B) A single Spark application from start to finish.
C) The execution triggered by a single Action (like `count()`, `collect()`, `save()`).
D) A single stage in the DAG.
**Answer:** C
**Mastery Explanation:** Spark employs lazy evaluation. Transformations build a Logical/Physical DAG. A "Job" is only triggered when an Action is invoked, forcing the execution of the DAG to compute a result.

**24. In the SQL tab, what is the visual difference between a `Scan parquet` and `Scan csv` that highlights Parquet's performance advantage?**
A) CSV scans always show a preceding `Exchange` node.
B) Parquet scans often show metrics like "PushedFilters" and "PartitionFilters", indicating data was filtered at the storage layer before reading into Spark.
C) Parquet scans do not require a `WholeStageCodegen` block.
D) CSV scans cannot be distributed across executors.
**Answer:** B
**Mastery Explanation:** Parquet is a columnar format supporting predicate pushdown. The UI details for a Parquet scan will display `PushedFilters` (reading only chunks containing relevant data) and `PartitionFilters` (pruning directories), which drastically reduces I/O. CSV does not support this level of pushdown.

**25. What does the "Blacklisted" status mean for an Executor or Node in the Web UI?**
A) The node was removed due to a security violation.
B) Spark has temporarily stopped assigning tasks to this executor/node because it has experienced too many consecutive task failures.
C) The node is currently undergoing maintenance by the cluster manager.
D) The executor is dedicated entirely to driver communications.
**Answer:** B
**Mastery Explanation:** Spark includes a task blacklisting mechanism (now often called node decommissioning or exclusion). If a specific executor or node fails multiple tasks (e.g., due to a bad disk or network card), Spark blacklists it to prevent it from failing the entire application.

## Part 3: "Small Twist" Questions (15 Questions)

**26. Scenario:** You view the UI for a job running `df.groupBy("id").count()`. It shows two stages separated by an Exchange.
**Twist:** You add `.repartition(200, "id")` before the `groupBy`. You check the UI again.
**Question:** How many exchanges (shuffles) will the UI show for this operation now?
**Answer:** One.
**Mastery Explanation:** Although you explicitly called `repartition(col)` (which causes a shuffle/Exchange), because you repartitioned on the EXACT SAME column used for the `groupBy`, the Catalyst optimizer recognizes the data is already hash-partitioned on the grouping key. It eliminates the second shuffle required for the aggregation, resulting in only one Exchange.

**27. Scenario:** Your Stages tab shows an extremely long "Shuffle Write Time".
**Twist:** You check your code and you are saving to HDFS using `df.write.parquet()`. The cluster is running on slow HDDs.
**Question:** Does the slow HDFS write cause the high "Shuffle Write Time"?
**Answer:** No.
**Mastery Explanation:** "Shuffle Write Time" strictly measures the time taken to write intermediate shuffle files to the executor's *local* disk for the next stage to read. Writing final output data to an external system like HDFS is captured under standard Task Execution time (or I/O metrics), not Shuffle Write.

**28. Scenario:** A job has 1000 tasks. 999 finish in 1 second. 1 task takes 10 minutes. This is classic data skew.
**Twist:** The slow task shows 0 bytes for "Shuffle Read", but 50GB for "Input Size".
**Question:** Is this skew caused by a `groupBy` or `join` operation?
**Answer:** No.
**Mastery Explanation:** "Shuffle Read" relates to data moving between stages (e.g., after a join or groupBy). "Input Size" relates to data read directly from the source system (e.g., HDFS, S3). Therefore, this is a *file size skew* or *unsplittable file* issue (e.g., reading a massive, uncompressed gzip file) at the scan stage, not a shuffle skew.

**29. Scenario:** The Executors tab shows an executor died.
**Twist:** The error message in the UI is "Executor heartbeat timed out after 120000 ms." The GC time for tasks on that executor right before it died was near zero.
**Question:** Was this executor most likely killed by the YARN OOM killer due to exceeding memory limits?
**Answer:** No.
**Mastery Explanation:** A heartbeat timeout without high GC suggests the JVM is completely hung, or there is a severe network partition preventing the executor from communicating with the driver. If it were a YARN OOM kill (Container killed by YARN for exceeding memory limits), the UI usually shows a specific "Container killed on request. Exit code is 137" or similar, rather than a generic heartbeat timeout.

**30. Scenario:** You have a small dimension table (10MB) and a massive fact table (1TB). You join them. The SQL tab shows a `BroadcastHashJoin`.
**Twist:** The dimension table is a DataFrame loaded via `spark.read.jdbc` from an external database, and you do not apply any filters. The UI shows a Stage with only 1 task reading the JDBC source.
**Question:** Will the `BroadcastHashJoin` execute fast, or is there a hidden bottleneck?
**Answer:** There is a hidden bottleneck.
**Mastery Explanation:** Because there is no `.repartition()` or explicit partitioning defined on the JDBC read, Spark reads the entire JDBC table using a single task (single executor). The driver must pull all this data from that single task to broadcast it. While the broadcast join itself is fast, the single-threaded JDBC read creates a severe bottleneck.

**31. Scenario:** You see a `SortMergeJoin` in the SQL tab.
**Twist:** You change one line of code: `spark.conf.set("spark.sql.join.preferSortMergeJoin", "false")`.
**Question:** Will the UI now guarantee a `ShuffleHashJoin` is used instead (assuming broadcast threshold is not met)?
**Answer:** Not guaranteed.
**Mastery Explanation:** While disabling the preference for SortMergeJoin encourages the optimizer to consider ShuffleHashJoin, it will only choose ShuffleHashJoin if it determines that a single partition of the build side will fit comfortably within the executor memory (to build the hash map). If the partitions are too large, it may still fall back to SortMergeJoin to avoid OOM.

**32. Scenario:** The Storage tab shows an RDD is 100% cached in memory.
**Twist:** You run a query on this RDD. The Stages tab shows some tasks have a "Locality Level" of `ANY`.
**Question:** Why wasn't all data processed `PROCESS_LOCAL` if it's 100% cached?
**Answer:** The executors holding the cached partitions were fully occupied.
**Mastery Explanation:** If the executors where the data is cached are busy running other tasks (or the cores are saturated), Spark's task scheduler (after waiting for `spark.locality.wait`) will assign the task to an available executor on a different node. That executor must fetch the cached block over the network, resulting in an `ANY` locality.

**33. Scenario:** You are looking at the DAG visualization for a Stage.
**Twist:** A `coalesce(10)` operation exists in the code.
**Question:** Does the `coalesce` create a new Stage boundary in the DAG visualization?
**Answer:** No.
**Mastery Explanation:** `coalesce` (when reducing partitions) does not trigger a full shuffle; it merely combines existing partitions on the same node. Because there is no all-to-all shuffle network exchange, it does not create a new Stage. (Unlike `.repartition()`, which forces a shuffle and creates a new stage).

**34. Scenario:** The Executors tab shows heavy CPU usage.
**Twist:** The SQL tab shows a lot of `Filter` operations, but you are querying Parquet files.
**Question:** If Parquet has predicate pushdown, why are there `Filter` nodes in the Spark plan using CPU?
**Answer:** Parquet pushdown operates at the row-group level, not the individual row level.
**Mastery Explanation:** Parquet uses min/max stats to skip entire chunks (row groups) of data that don't match the filter. However, for the chunks that *do* match, Spark still must read the chunk into memory and apply the exact `Filter` operator row-by-row to discard the false positives within that chunk.

**35. Scenario:** The Environment tab shows `spark.task.cpus = 2`.
**Twist:** Your executor has 4 cores. You launch 4 tasks that do heavy matrix multiplication using a native BLAS library (like OpenBLAS) that is multi-threaded.
**Question:** How many tasks will the Spark UI show running concurrently on this executor, and is this optimal?
**Answer:** The UI will show 2 tasks running concurrently (4 cores / 2 cores per task). This is likely optimal.
**Mastery Explanation:** Setting `spark.task.cpus=2` tells Spark to reserve 2 cores for each task. Because the native BLAS library uses multiple threads internally (bypassing JVM threading), if you ran 4 tasks, they would spawn too many native threads and cause CPU thrashing. Reserving 2 cores per task aligns Spark's scheduling with the native library's multithreading.

**36. Scenario:** A Stage fails due to OOM.
**Twist:** You look at the SQL tab, and the only complex operation is an `OrderBy` (Sort). You know Spark spills sorts to disk.
**Question:** Why did it OOM instead of spilling?
**Answer:** The objects being sorted were highly complex or deeply nested, causing the PointerArray used by Tungsten to exceed memory limits before a spill was triggered, or the individual rows were simply too large.
**Mastery Explanation:** Spark sorts data by creating an array of pointers and sorting the array. If the records themselves are massive, or if the overhead of tracking the pointers exceeds the memory fraction before the spill threshold is evaluated, the JVM can OOM. Alternatively, the OOM might be in User Memory, outside the execution memory pool that handles spilling.

**37. Scenario:** You see a `BroadcastExchange` in the SQL tab.
**Twist:** The "data size" metric on the `BroadcastExchange` node is 150MB, but `spark.sql.autoBroadcastJoinThreshold` is 10MB.
**Question:** How is this possible?
**Answer:** The size was estimated to be under 10MB during Catalyst optimization, but the actual materialized data was 150MB.
**Mastery Explanation:** Catalyst uses statistics to estimate table sizes. If statistics are missing or stale (e.g., a complex chain of filters and maps obfuscated the size), Catalyst might estimate a small size and plan a Broadcast. When actually executed, the data was much larger.

**38. Scenario:** You are looking at the Jobs tab. Job 1 took 5 minutes. Job 2 took 1 second.
**Twist:** Job 1 executed `df.cache().count()`. Job 2 executed `df.take(1)`.
**Question:** Did Job 2 skip stages because of the cache?
**Answer:** No.
**Mastery Explanation:** `take(1)` only needs to compute the first partition (or until it finds 1 row). Even if `df` was not cached, `take(1)` would be extremely fast because it short-circuits execution. The cache is a red herring for why `take(1)` is fast, though it would read from the cached first partition if available.

**39. Scenario:** The Stages tab shows High GC time.
**Twist:** You switch your code from using UDFs (User Defined Functions) to using native Spark SQL functions (e.g., `pyspark.sql.functions`).
**Question:** Will the GC time in the UI decrease, and why?
**Answer:** Yes, significantly.
**Mastery Explanation:** Native Spark SQL functions operate directly on Tungsten's off-heap binary data format. Custom UDFs (especially in Scala/Java) require Spark to deserialize this binary data into Java objects on the heap, process them, and serialize them back. This massive object creation causes severe GC pressure.

**40. Scenario:** The UI shows tasks with huge "Shuffle Read" size.
**Twist:** You notice `spark.sql.shuffle.partitions` is set to 2. You change it to 200.
**Question:** Will the *total* "Shuffle Read" size across the stage decrease?
**Answer:** No.
**Mastery Explanation:** Changing the number of shuffle partitions distributes the exact same amount of data across more tasks (reducing the size *per task* and preventing OOM/skew). The *total* amount of data shuffled across the network remains identical.

## Part 4: Coding & Debugging Questions (10 Questions)

**41. Debugging Scenario:**
You have a streaming application. The Streaming UI shows "Processing Time" is 5 seconds, but the "Batch Interval" is 2 seconds. "Scheduling Delay" is growing infinitely.
**Code Context:**
```scala
val stream = KafkaUtils.createDirectStream(...)
stream.map(record => processIntensively(record.value()))
      .saveToCassandra(...)
```
**Question:** How can you use the UI and configuration to stabilize this without changing the `processIntensively` logic?
**Mastery Explanation:** The system is inherently unstable because it processes slower than data arrives. You must either scale up horizontally or throttle ingestion. Using the UI, you confirm the issue. The fix is to enable backpressure (`spark.streaming.backpressure.enabled = true`) and set a `spark.streaming.kafka.maxRatePerPartition` to limit the input rate to what the executors can handle (e.g., calculating max rows per second based on the 5-second processing time).

**42. Debugging Scenario:**
A PySpark job performing a simple `df.withColumn("new", custom_python_udf(col("old"))).write...` is failing. The Executors tab shows executor memory is fine, but tasks fail with Python worker crashing.
**Question:** What UI metric or log would you look for, and what is the architectural root cause?
**Mastery Explanation:** You would look for memory usage or OOMs on the *OS level*, not the JVM. PySpark executes Python UDFs by spawning separate Python worker processes outside the JVM and communicating via Py4J/sockets. The JVM memory (tracked in the UI) might be fine, but the Python processes are consuming too much memory and being killed by the OS. Fix: Use Pandas UDFs (Arrow) or rewrite as native SQL.

**43. Debugging Scenario:**
The SQL tab shows a `HashAggregate` node taking an extremely long time. Hovering over it, the "spill (disk)" metric is massive.
**Code Context:**
`df.groupBy("user_id").agg(collect_list("click_event"))`
**Question:** Why is this spilling heavily, and how do you optimize it?
**Mastery Explanation:** `collect_list` creates a massive array for each `user_id`. If a user has millions of clicks, this array cannot fit in the executor's memory for that key, forcing Tungsten to spill the massive object to disk repeatedly. Optimization: Avoid `collect_list` if possible. Use window functions, or if you must aggregate, ensure the downstream sink can handle relational data instead of massive nested arrays.

**44. Debugging Scenario:**
You submit a job to YARN. The Spark UI Environment tab shows `spark.executor.instances = 50`. However, the Executors tab only ever shows 2 active executors. The cluster has plenty of resources.
**Code Context:**
`spark-submit --num-executors 50 --class MyMain app.jar`
**Question:** What setting overrides `--num-executors`, causing this behavior, and how do you verify it in the UI?
**Mastery Explanation:** Dynamic Allocation is likely overriding the static count. Check the Environment tab for `spark.dynamicAllocation.enabled = true`. If the job hasn't demanded more executors (e.g., there are only 2 partitions of data to process), dynamic allocation will only request 2 executors, completely ignoring `--num-executors 50`.

**45. Debugging Scenario:**
The Stages tab shows a stage with 200 tasks. 199 tasks finish in 5 seconds. Task 200 is "stuck" for 30 minutes, but it has processed 0 records and shows no CPU usage in thread dumps.
**Question:** What networking or external system issue does this behavior in the UI point to?
**Mastery Explanation:** This is a classic "hanging connection" issue. Task 200 is likely trying to connect to an external database, API, or acquire a lock, and the connection timed out but didn't throw an exception (lacking a proper socket timeout). The thread dump would show the thread in `WAITING` or `BLOCKED` state on a socket read/connection initialization.

**46. Debugging Scenario:**
You cache a DataFrame: `df.cache().count()`. You check the Storage tab, and it shows 100% cached. You run a second action: `df.filter("age > 20").count()`. The UI Jobs tab shows the second job re-read data from HDFS instead of the cache.
**Code Context:**
```scala
val df = spark.read.parquet("hdfs://...")
df.cache().count()
val df2 = spark.read.parquet("hdfs://...")
df2.filter("age > 20").count()
```
**Question:** Why didn't `df2` use the cache?
**Mastery Explanation:** Spark caches are tied to the lineage/logical plan of the specific DataFrame reference. `df2` is a newly created DataFrame with a new lineage (even though it points to the same path). Spark does not inherently perform cross-DataFrame cache matching based on physical path unless you use the SQL caching mechanism (`spark.catalog.cacheTable`). You must reuse the `df` variable.

**47. Debugging Scenario:**
The SQL tab shows a DAG with: `Scan -> Filter -> Project -> BroadcastExchange`. The job is very slow.
**Code Context:**
```scala
val lookup = spark.read.csv("s3a://massive-csv").filter("id > 0")
val main = spark.read.parquet("s3a://main-data")
main.join(broadcast(lookup), "id")
```
**Question:** Why is the UI showing a bottleneck on the Broadcast, and how do you fix it?
**Mastery Explanation:** Because `lookup` is a CSV, Catalyst cannot push down filters or accurately estimate its size. It reads the entire massive CSV, filters it, and attempts to broadcast it. If the filtered result is still huge, it crashes or hangs. Fix: Ensure statistics are calculated on the CSV, or better, convert the CSV to Parquet so Catalyst knows exactly what it's dealing with before broadcasting.

**48. Debugging Scenario:**
In the Executors tab, you notice "Task Time (GC Time)" is 10s (8s GC). Memory usage is flat, but CPU is 100%.
**Code Context:**
```scala
df.rdd.mapPartitions { iter =>
  val sdf = new java.text.SimpleDateFormat("yyyy-MM-dd")
  iter.map(row => sdf.parse(row.getString(0)))
}
```
**Question:** What is the specific coding error causing this UI profile?
**Mastery Explanation:** While the code looks okay, if `SimpleDateFormat` is instantiated *inside* the `map` loop (instead of `mapPartitions` initialization), it creates millions of short-lived objects per partition, causing massive Young Gen GC thrashing (high GC time, high CPU, flat overall heap because they are immediately collected). The UI perfectly reflects this CPU-bound GC thrashing.

**49. Debugging Scenario:**
A Spark SQL query running a complex join strategy is failing. You use the SQL tab "Details" section to view the physical plan.
**Question:** Which part of the `explain` output in the UI details tells you exactly how many partitions are being used for the shuffle phase of the join?
**Mastery Explanation:** Look for the `Exchange hashpartitioning(key, num_partitions)` node in the Physical Plan. The `num_partitions` value directly reveals the configuration of `spark.sql.shuffle.partitions` applied to that specific shuffle.

**50. Debugging Scenario:**
You observe a stage failing repeatedly with "No space left on device".
**Question:** Using the Spark UI, how do you differentiate if this is HDFS storage space vs. Executor local disk space?
**Mastery Explanation:** Look at the failed task details. If it fails during a "Shuffle Write" or when writing spilled data, the error pertains to the Executor's local disk (defined by `spark.local.dir`). If it fails during an output action (like saving a Parquet file) without shuffle metrics, it is failing to write to the distributed file system (HDFS/S3).
