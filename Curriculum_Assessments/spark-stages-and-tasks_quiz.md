# Spark Stages and Tasks: Senior/Staff Assessment

## Part 1: True/False (10 Questions)

1. **Question:** A Spark Stage boundary is strictly defined by the occurrence of any wide dependency (shuffle) operation, and Catalyst cannot pipeline operations across a shuffle boundary under any circumstances.
   - **Answer:** False.
   - **Mastery Explanation:** While shuffle dependencies generally define stage boundaries, Adaptive Query Execution (AQE) can dynamically coalesce shuffle partitions and optimize post-shuffle stages, though pipelining across the boundary itself (in a single task) isn't strictly done, local exchanges or broadcast joins can eliminate shuffle boundaries entirely, transforming what would be a wide dependency into a narrow one dynamically.

2. **Question:** In Spark 3.x with AQE enabled, a stage's number of tasks is strictly immutable once the stage has been submitted to the DAGScheduler.
   - **Answer:** False.
   - **Mastery Explanation:** AQE dynamically coalesces shuffle partitions *after* the map stage finishes, which changes the number of reduce tasks in the subsequent stage before it begins. Therefore, downstream stage task counts are mutable at runtime.

3. **Question:** The Tungsten memory format guarantees that task serialization overhead between executors during a shuffle is eliminated because data is transferred in its raw binary format.
   - **Answer:** True.
   - **Mastery Explanation:** Tungsten operates directly on off-heap memory using binary formats (UnsafeRow). During a shuffle, Spark can use Tungsten's shuffle write path (UnsafeShuffleWriter) to transfer binary data directly without deserializing and reserializing Java objects, drastically reducing CPU overhead.

4. **Question:** Setting `spark.task.cpus` to a value greater than 1 allows a single Spark task to internally multithread and utilize multiple CPU cores during execution.
   - **Answer:** False.
   - **Mastery Explanation:** Spark tasks are fundamentally single-threaded. Setting `spark.task.cpus > 1` simply reserves more slots from the executor's available core pool for the task, preventing other tasks from running concurrently and reducing memory contention, but the task itself doesn't automatically multithread unless the user's custom code spawns threads.

5. **Question:** Speculative execution (`spark.speculation=true`) is highly recommended for stages writing to external non-idempotent sinks (like some JDBC databases) because it guarantees exactly-once semantics.
   - **Answer:** False.
   - **Mastery Explanation:** Speculative execution launches duplicate tasks for stragglers. If writing to a non-idempotent sink, multiple tasks might write the same data concurrently, leading to data duplication or corruption. Output committers are required to handle this safely (e.g., FileOutputCommitter v2 vs v1).

6. **Question:** A task memory leak caused by unclosed iterators in `mapPartitions` will inevitably trigger an OutOfMemoryError in the Executor, bypassing Spark's memory manager.
   - **Answer:** True.
   - **Mastery Explanation:** User-defined objects created inside `mapPartitions` are managed by the JVM heap (User Memory fraction), not Spark's execution memory manager (which tracks shuffles, joins, etc.). If iterators or large objects are held and not garbage collected, they will exhaust the JVM heap, causing an OOM.

7. **Question:** The size of a task's broadcast variable is limited only by the Executor's heap memory capacity.
   - **Answer:** False.
   - **Mastery Explanation:** While executors need memory to store broadcast blocks, broadcast variables are also limited by the maximum size that can be serialized and transferred via the TorrentBroadcast system, historically 8GB due to ByteBuffer limitations, and also constrained by the Driver's memory since it must collect and chunk the broadcast data.

8. **Question:** FetchFailedException in a task immediately causes the entire Spark application to fail.
   - **Answer:** False.
   - **Mastery Explanation:** A FetchFailedException causes the currently running stage to be aborted, and the DAGScheduler resubmits the upstream stage (the one that produced the missing shuffle files). The application only fails if it exceeds `spark.stage.maxConsecutiveAttempts`.

9. **Question:** In a SortMergeJoin, if the data is already partitioned and sorted on the join keys by a previous stage, Spark will still introduce a new shuffle and sort stage by default.
   - **Answer:** False.
   - **Mastery Explanation:** Catalyst is aware of physical data distribution and ordering. If a dataframe has already been partitioned (HashPartitioning) and sorted (SortOrder) on the exact join keys, Catalyst will insert an `Exchange` only if the number of partitions doesn't match or the distribution differs, otherwise it avoids the shuffle (known as a "shuffle-free join").

10. **Question:** `spark.memory.fraction` directly controls the exact amount of physical RAM allocated to a single task for execution.
    - **Answer:** False.
    - **Mastery Explanation:** `spark.memory.fraction` controls the ratio of heap space dedicated to Spark's unified memory region (Execution + Storage) for the *entire executor*. Memory is dynamically shared among all concurrent tasks running on that executor.

## Part 2: Multiple Choice Questions (15 Questions)

11. **Question:** Which Catalyst physical plan node is responsible for generating Stage boundaries?
    - A) Project
    - B) Exchange
    - C) Filter
    - D) HashAggregate
    - **Answer:** B
    - **Mastery Explanation:** The `Exchange` physical plan node corresponds to a shuffle operation. The DAGScheduler breaks the physical plan into stages at these `Exchange` boundaries.

12. **Question:** During a highly skewed shuffle stage, a single task takes 10x longer than others. Which feature dynamically handles this in Spark 3?
    - A) Speculative Execution
    - B) AQE Skew Join Optimization
    - C) Dynamic Allocation
    - D) Broadcast Hash Join
    - **Answer:** B
    - **Mastery Explanation:** AQE Skew Join Optimization detects skewed partitions at runtime from shuffle map statistics and splits the skewed partition into multiple sub-partitions, launching multiple tasks to process them in parallel during the reduce stage.

13. **Question:** What is the fundamental unit of work in a Spark execution plan?
    - A) Job
    - B) Stage
    - C) Task
    - D) Executor
    - **Answer:** C
    - **Mastery Explanation:** A Task is the smallest unit of work, executed by a single thread on an executor, processing a single partition of data.

14. **Question:** If you have an RDD of 100 partitions and apply `map()`, `filter()`, and `flatMap()`, how many tasks are generated for this sequence?
    - A) 300 tasks (100 per operation)
    - B) 100 tasks (pipelined)
    - C) 1 task
    - D) Cannot be determined
    - **Answer:** B
    - **Mastery Explanation:** Narrow dependencies (map, filter, flatMap) are pipelined by Catalyst into a single stage. Each of the 100 partitions will be processed by a single task executing all three operations sequentially.

15. **Question:** A Spark job fails with `ExecutorLostFailure`. What is the DAGScheduler's immediate reaction?
    - A) Fails the application
    - B) Resubmits the failed tasks to another active executor
    - C) Resubmits the entire stage
    - D) Triggers a GC pause on the driver
    - **Answer:** B
    - **Mastery Explanation:** If an executor is lost (e.g., node failure, OOM), the TaskScheduler attempts to resubmit the failed tasks to other executors. However, if shuffle files stored on the lost executor are needed by downstream stages, a `FetchFailedException` will occur later, prompting the DAGScheduler to resubmit the missing map tasks. (Note: B is the immediate TaskScheduler reaction, DAGScheduler handles the stage-level retry if fetch fails).

16. **Question:** Which metric determines the number of tasks in a shuffle read stage (reduce stage) prior to Spark 3 AQE?
    - A) `spark.sql.shuffle.partitions`
    - B) Number of HDFS blocks
    - C) Number of cores available in the cluster
    - D) `spark.default.parallelism`
    - **Answer:** A
    - **Mastery Explanation:** For DataFrame/SQL API, `spark.sql.shuffle.partitions` (default 200) strictly determines the number of reduce tasks for wide transformations unless AQE dynamically coalesces them.

17. **Question:** What happens to task execution if an Executor runs out of Execution Memory while processing a large sort?
    - A) The task crashes with an OOM.
    - B) Spark spills the excess data to disk.
    - C) The Executor steals memory from the Storage pool, potentially evicting cached blocks.
    - D) Both B and C, in that order.
    - **Answer:** D
    - **Mastery Explanation:** In Spark's Unified Memory Management, Execution memory can borrow from Storage memory. If Storage is empty or can be evicted, it borrows. If it still runs out, operations like sort/aggregate will spill to disk (managed by `TungstenSort` or `ExternalAppendOnlyMap`), preventing OOMs.

18. **Question:** Why might a task show excessive GC time (Garbage Collection)?
    - A) Too many CPU cores assigned.
    - B) High object creation rate in UDFs.
    - C) Tungsten off-heap memory is full.
    - D) Network bandwidth is saturated.
    - **Answer:** B
    - **Mastery Explanation:** High GC time usually indicates that the JVM heap is filling up rapidly with short-lived objects. UDFs (especially in Scala/Python) that instantiate large objects per row bypass Tungsten's optimized memory management, causing GC pressure.

19. **Question:** What does a `TaskKilled (killed intentionally)` status usually indicate?
    - A) The user cancelled the job via the UI.
    - B) Speculative execution succeeded on another node, so this straggler was killed.
    - C) Task memory limits were exceeded.
    - D) A or B.
    - **Answer:** D
    - **Mastery Explanation:** Tasks are intentionally killed when the job/stage is cancelled, or when a speculative task finishes first, rendering the original task redundant.

20. **Question:** Which component sends the Task to the Executor?
    - A) DAGScheduler
    - B) TaskScheduler
    - C) Cluster Manager
    - D) BlockManager
    - **Answer:** B
    - **Mastery Explanation:** The DAGScheduler breaks the logical graph into Stages of Tasks. The TaskScheduler takes these TaskSets and schedules them onto the available Executors via the backend (e.g., YARN/K8s).

21. **Question:** In PySpark, what causes the "Python worker unexpectedly exited (crashed)" error during task execution?
    - A) OOM on the JVM Executor heap.
    - B) OOM on the spawned Python process processing the partition.
    - C) Driver timeout.
    - D) Shuffle fetch failure.
    - **Answer:** B
    - **Mastery Explanation:** PySpark runs a separate Python daemon process per task. If a Python UDF or pandas UDF loads an entire partition into memory (e.g., exceeding container limits), the OS OOM killer terminates the Python process, resulting in this error.

22. **Question:** How does Spark broadcast variables to tasks?
    - A) By sending the variable over RPC to every task individually.
    - B) By using a BitTorrent-like protocol (TorrentBroadcast) to distribute blocks among executors.
    - C) By writing them to HDFS and having tasks read them.
    - D) By storing them in ZooKeeper.
    - **Answer:** B
    - **Mastery Explanation:** TorrentBroadcast chunks the broadcast variable. The driver and executors act as a P2P network, preventing the driver's network from becoming a bottleneck when hundreds of executors need the variable.

23. **Question:** What is the primary cause of task stragglers?
    - A) Evenly distributed data partitions.
    - B) Data skew (one partition is significantly larger than others).
    - C) High memory limits.
    - D) SSD drives on executor nodes.
    - **Answer:** B
    - **Mastery Explanation:** Data skew causes one task to process exponentially more records than others, dominating the stage's total execution time.

24. **Question:** Which of the following operations forces a stage boundary?
    - A) `repartition()`
    - B) `coalesce()`
    - C) `select()`
    - D) `drop()`
    - **Answer:** A
    - **Mastery Explanation:** `repartition()` causes a full shuffle of the data to achieve an even distribution across the specified number of partitions, thus creating an `Exchange` and a new stage boundary. `coalesce()` (usually) avoids a full shuffle by just combining local partitions.

25. **Question:** When an Executor finishes a task, how does the Driver know?
    - A) The Driver constantly polls the Executor.
    - B) The Executor sends a StatusUpdate message to the Driver via RPC.
    - C) The Executor writes a completion flag to HDFS.
    - D) The TaskScheduler reads the Executor's local filesystem.
    - **Answer:** B
    - **Mastery Explanation:** The Executor backend communicates asynchronously with the Driver via RPC endpoints, sending StatusUpdates (Running, Finished, Failed) back to the TaskScheduler.

## Part 3: Small Twist Questions (15 Questions)

26. **Scenario:** You have a dataset of 100 GB. You run `.join(other_df).count()`. It takes 10 minutes.
    **Twist:** You change it to `.join(broadcast(other_df)).count()`. `other_df` is 15 GB. The job now fails with an OutOfMemoryError on the Driver.
    - **Question:** Why did the broadcast join crash the Driver?
    - **Mastery Explanation:** The `broadcast` hint forces Spark to use a BroadcastHashJoin. The Driver must collect the entire 15 GB DataFrame, serialize it, and chunk it. Since the Driver's memory is typically small (e.g., 4GB default), trying to collect a 15GB DataFrame immediately OOMs the Driver. Broadcast joins are only suitable for small tables (default `<10MB`).

27. **Scenario:** You run `df.groupBy("key").count()` and it generates 200 tasks in the reduce stage.
    **Twist:** You enable AQE (`spark.sql.adaptive.enabled=true`) and `spark.sql.adaptive.coalescePartitions.enabled=true`. The reduce stage now only has 15 tasks.
    - **Question:** What mechanism reduced the task count?
    - **Mastery Explanation:** AQE dynamically coalesces contiguous shuffle partitions that are small into a single larger partition during the shuffle read phase. Since the data size after grouping was small, AQE combined the 200 tiny partitions into 15 optimally sized partitions, reducing task scheduling overhead.

28. **Scenario:** You run a PySpark job using UDFs, taking 2 hours.
    **Twist:** You switch the UDF to a Pandas UDF (Vectorized UDF) and it drops to 15 minutes.
    - **Question:** What architectural change caused this massive speedup?
    - **Mastery Explanation:** Standard Python UDFs evaluate row-by-row, incurring massive serialization/deserialization overhead (Java <-> Python via sockets). Pandas UDFs use Apache Arrow for columnar, zero-copy data transfer and operate on batches (Series), significantly reducing CPU serialization overhead and enabling vectorized execution in Python.

29. **Scenario:** You execute `df.coalesce(1).write.csv("out")`. It takes forever.
    **Twist:** You change it to `df.repartition(1).write.csv("out")`. It finishes much faster.
    - **Question:** Why is `repartition` faster than `coalesce` here, even though it causes a shuffle?
    - **Mastery Explanation:** `coalesce(1)` avoids a shuffle but forces all upstream operations in that stage to execute on a single task (1 core). `repartition(1)` performs a shuffle, meaning the upstream transformations run in parallel across the cluster, and only the final write stage operates on a single task.

30. **Scenario:** Your tasks are failing with OOMs. You double `spark.executor.memory`. The tasks still fail.
    **Twist:** You revert memory, but increase `spark.sql.shuffle.partitions` from 200 to 2000. The job succeeds.
    - **Question:** Why did increasing partitions fix the OOM?
    - **Mastery Explanation:** The OOM was caused by individual partitions being too large to fit in task memory during a shuffle or sort. Increasing `shuffle.partitions` divides the same data into smaller, more manageable chunks. Each task now processes 1/2000th of the data instead of 1/200th, requiring less memory per task.

31. **Scenario:** You read a 1TB JSON file using `spark.read.json()`. Spark launches 10,000 tasks.
    **Twist:** You read a 1TB GZIP-compressed JSON file. Spark launches only 1 task and crashes.
    - **Question:** Why did GZIP compression reduce the task count to 1?
    - **Mastery Explanation:** Standard GZIP is not a splittable compression format. Spark cannot seek to arbitrary points in a GZIP file to read it in parallel. Therefore, a single task is forced to read the entire 1TB file sequentially on one executor, causing an OOM or severe bottleneck. BZIP2 or Snappy (in Parquet) should be used.

32. **Scenario:** You have a cluster with 10 executors, 4 cores each. You submit a job and see 40 tasks running concurrently.
    **Twist:** You set `spark.task.cpus=2`. Now only 20 tasks run concurrently.
    - **Question:** Why did concurrency drop?
    - **Mastery Explanation:** `spark.task.cpus` tells Spark how many core slots a single task requires. With 4 cores per executor and `spark.task.cpus=2`, each executor can only fit 2 tasks concurrently (4/2 = 2). 10 executors * 2 tasks = 20 concurrent tasks.

33. **Scenario:** A Spark Streaming job reads from Kafka. Task durations are stable around 1 second.
    **Twist:** During a traffic spike, task durations spike to 30 seconds, causing batch delays. You enable `spark.streaming.kafka.maxRatePerPartition`.
    - **Question:** How does this configuration stabilize task durations?
    - **Mastery Explanation:** Backpressure limits the amount of data ingested per micro-batch. `maxRatePerPartition` caps the number of records read per Kafka partition per second, ensuring tasks receive a predictable data volume and execute within acceptable timeframes, preventing cascading delays.

34. **Scenario:** You execute a heavy Spark SQL query. The UI shows "Spill (Memory)" and "Spill (Disk)" metrics are very high.
    **Twist:** You change the data format from CSV to Parquet and ensure columns are highly compressed. The spill metrics drop to zero.
    - **Question:** How did the file format affect execution spilling?
    - **Mastery Explanation:** Reading CSV requires reading raw text, parsing it into unoptimized Java strings, which explode in memory size. Parquet is columnar and typed; Spark reads it directly into Tungsten's compact binary format. The smaller memory footprint prevents Execution memory from filling up, avoiding the need to spill to disk during sorts/aggregates.

35. **Scenario:** You have a custom `mapPartitions` function doing an API call per record.
    **Twist:** You initialize an HTTP client connection inside the `mapPartitions` iterator rather than outside the `mapPartitions` function.
    - **Question:** Why is this the correct architectural pattern?
    - **Mastery Explanation:** If you initialize the client outside the function (in the driver), Spark tries to serialize the connection object and send it to executors, which fails (NotSerializableException). By initializing it *inside* `mapPartitions`, a single connection is created per task (partition) on the executor JVM, avoiding serialization issues and connection overhead.

36. **Scenario:** You run a job and Task 1 takes 5 minutes, Task 2 takes 5.1 minutes.
    **Twist:** You enable `spark.speculation=true`. The DAGScheduler launches duplicate tasks, but they immediately get cancelled.
    - **Question:** Why did speculative execution trigger but fail to help?
    - **Mastery Explanation:** Speculation triggers when a task runs significantly slower than the median task time. If all tasks are uniformly slow (e.g., heavy compute), speculation might incorrectly trigger if the variance threshold is tight, but if they complete shortly after, the speculative copies are killed. Tuning `spark.speculation.multiplier` is required.

37. **Scenario:** A join produces a massive skew, taking 3 hours.
    **Twist:** You add a random integer column `salt` (1 to 10) to the skewed dataframe, broadcast the small dataframe, and explode the small dataframe 10 times.
    - **Question:** What is this technique called and how does it fix task skew?
    - **Mastery Explanation:** This is "Salting". By appending a random number to the join key, you split the massively skewed key into 10 separate keys. This distributes the records across 10 tasks instead of 1, parallelizing the workload. The small table is replicated so it matches any of the 10 salt variations.

38. **Scenario:** You `.cache()` a DataFrame and trigger an action. The Storage tab shows 100% memory usage.
    **Twist:** You change `.cache()` to `.persist(StorageLevel.MEMORY_AND_DISK_SER)`. The memory usage drops to 30% and the job runs faster on subsequent actions.
    - **Question:** Why did serialized caching reduce memory footprint?
    - **Mastery Explanation:** Default `.cache()` uses `MEMORY_AND_DISK` (deserialized Java objects). Java objects have massive memory overhead (headers, pointers). `_SER` stores data in serialized byte arrays, which are drastically smaller, meaning more data fits in RAM, preventing disk spilling on subsequent reads.

39. **Scenario:** A job writes to S3. It takes 10 minutes to process data, and 20 minutes for the job to "finish" after tasks complete.
    **Twist:** You switch the committer to `FileOutputCommitter` algorithm version 2 (or use the magic S3A committer). The job finishes instantly after tasks complete.
    - **Question:** What was the driver doing for 20 minutes?
    - **Mastery Explanation:** With v1 committers, tasks write to a temporary directory, and the Driver sequentially renames files to the final destination. On object stores like S3, "rename" is actually a slow `COPY` + `DELETE` operation. The v2/magic committer writes directly or uses multipart uploads, eliminating the driver-side rename bottleneck.

40. **Scenario:** Your cluster has `spark.dynamicAllocation.enabled=true`. The job starts with 2 executors, scales to 100 during a map stage, but drops to 0 during the shuffle read, stalling the job.
    **Twist:** You enable `spark.shuffle.service.enabled=true`. The job completes successfully.
    - **Question:** Why did dynamic allocation fail without the external shuffle service?
    - **Mastery Explanation:** When an executor is idle, dynamic allocation removes it to save resources. If it removes an executor that holds map output (shuffle files), downstream tasks cannot fetch that data. The External Shuffle Service is an independent daemon on the node that serves shuffle files even if the executor JVM is killed, allowing safe scale-down.

## Part 4: Coding & Debugging (10 Questions)

41. **Code Snippet:**
    ```python
    global_counter = 0
    def count_records(row):
        global global_counter
        global_counter += 1
        return row

    df.rdd.map(count_records).collect()
    print(global_counter)
    ```
    - **Question:** What will `global_counter` print on the Driver, and why?
    - **Mastery Explanation:** It will print `0`. `global_counter` is serialized and sent to executors. The executors increment their local copies of the variable. These copies are never sent back to the driver. To achieve this, an Accumulator (`sc.accumulator(0)`) must be used.

42. **Code Snippet:**
    ```scala
    val data = spark.read.parquet("s3://bucket/data")
    data.withColumn("id", monotonically_increasing_id())
        .repartition(100)
        .write.parquet("s3://bucket/output")
    ```
    - **Question:** What performance trap exists here regarding Stage boundaries?
    - **Mastery Explanation:** `repartition()` causes a shuffle. `monotonically_increasing_id()` generates IDs based on the partition ID. If placed *before* a shuffle, it's fine, but the order of operations here means Spark will generate IDs, then shuffle. To optimize, `repartition` should happen *before* the projection if the projection is heavy, though here it's lightweight. The actual bug is if you use `monotonically_increasing_id` *after* a repartition, it may yield different IDs if the stage is retried due to node failure, breaking deterministic guarantees.

43. **Debugging Scenario:** You see a `java.lang.OutOfMemoryError: Direct buffer memory` in the executor logs during a heavy `groupBy().agg()` operation.
    - **Question:** Which specific memory pool is exhausted and how do you fix it?
    - **Mastery Explanation:** "Direct buffer memory" refers to off-heap memory used by Tungsten or Arrow, not the JVM heap. This occurs when `spark.memory.offHeap.size` is too small, or overhead memory (`spark.executor.memoryOverhead`) is exhausted by native allocations. Increase `spark.executor.memoryOverhead` to resolve this.

44. **Code Snippet:**
    ```python
    df1 = spark.sql("SELECT * FROM massive_table")
    df2 = spark.sql("SELECT * FROM tiny_table")
    # Developer wants a broadcast join
    df1.join(df2, "id").explain()
    ```
    - **Question:** Catalyst decides to use a SortMergeJoin instead of BroadcastHashJoin. What configuration is likely missing or what statistics are missing?
    - **Mastery Explanation:** Spark relies on table statistics (CBO) to determine table sizes. If `ANALYZE TABLE tiny_table COMPUTE STATISTICS` hasn't been run, Spark doesn't know the table is tiny and defaults to SMJ. Alternatively, `spark.sql.autoBroadcastJoinThreshold` might be set to -1 (disabled) or a value smaller than `tiny_table`. Use `F.broadcast(df2)` to force it.

45. **Debugging Scenario:** A streaming job reads from Kafka. The DAG shows 3 stages per micro-batch. Stage 1 takes 10ms, Stage 2 takes 10ms, Stage 3 takes 5 minutes. Stage 3 consists only of `saveAsTextFile`.
    - **Question:** What is causing the massive bottleneck in Stage 3?
    - **Mastery Explanation:** Output stages run tasks based on the number of partitions of the incoming RDD/DataFrame. If previous operations coalesced the data to 1 partition, or if the Kafka topic only has 1 partition, Stage 3 runs on a single core. The solution is to `.repartition(N)` before writing to increase parallel I/O tasks.

46. **Code Snippet:**
    ```scala
    df.rdd.mapPartitions { iter =>
      val dbConnection = DriverManager.getConnection("jdbc:...")
      iter.map { row =>
        dbConnection.createStatement().execute(s"INSERT INTO table VALUES (${row.id})")
        row
      }
    }.count()
    ```
    - **Question:** Identify the resource leak in this task code.
    - **Mastery Explanation:** The database connection is opened inside `mapPartitions` (which is good), but it is never closed after the iterator is exhausted. This leads to connection exhaustion on the database server. It should be wrapped in a `try-finally` block, or better yet, use `iter.foreach` and close the connection at the end of the partition processing.

47. **Debugging Scenario:** You submit a job via `spark-submit --master yarn --deploy-mode cluster`. The application UI shows the job immediately in state `FAILED`. The executor logs are completely empty. The driver logs show `java.lang.NoClassDefFoundError: org/apache/spark/sql/SparkSession`.
    - **Question:** What dependency packaging error occurred?
    - **Mastery Explanation:** The user bundled Spark libraries (`spark-sql`, `spark-core`) inside their application FAT JAR with scope `compile` instead of `provided`. This causes classloader conflicts between the cluster's Spark version and the bundled version. Spark libraries must be marked as `provided` in Maven/SBT.

48. **Code Snippet:**
    ```python
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    @udf(returnType=StringType())
    def complex_logic(val):
        import time
        time.sleep(1) # simulate complex API call
        return str(val)

    df.withColumn("new_col", complex_logic("col")).show(10)
    ```
    - **Question:** Why does this task execution model result in terrible throughput, and how do you fix it without changing the UDF logic?
    - **Mastery Explanation:** Python UDFs evaluate one row at a time. A 1-second sleep per row means processing 1000 rows takes 1000 seconds per task. To improve throughput without changing logic, you must increase parallelism drastically by repartitioning the dataframe to a very high number (e.g., `.repartition(1000)`) so thousands of tasks execute the sleep concurrently.

49. **Debugging Scenario:** A job has 5 stages. Stage 3 (Shuffle Map) completes successfully. Stage 4 (Shuffle Reduce) fails with `FetchFailedException`. The UI shows Stage 3 is re-executed, but it completes instantly without launching any tasks, and Stage 4 fails again.
    - **Question:** Why did Stage 3 complete instantly upon retry, causing an infinite failure loop?
    - **Mastery Explanation:** The MapOutputTracker still holds cached metadata saying Stage 3's shuffle files are available on a specific executor, even though that executor is dead or network unreachable. This is an edge case bug/issue with stale shuffle metadata. Restarting the cluster or setting stricter executor timeout bounds is required.

50. **Code Snippet:**
    ```scala
    val rdd1 = sc.textFile("large_file.txt").map(x => (x.split(",")(0), x))
    val rdd2 = sc.textFile("small_file.txt").map(x => (x.split(",")(0), x))
    val joined = rdd1.join(rdd2)
    ```
    - **Question:** By default, what type of dependency graph and shuffle does this generate, and how could you optimize it in the RDD API?
    - **Mastery Explanation:** This generates a wide dependency (shuffle) for BOTH `rdd1` and `rdd2` because neither has a known Partitioner. Even if `small_file.txt` fits in memory, a standard `.join()` shuffles both. To optimize in the RDD API, use a Broadcast Hash Join pattern manually: broadcast `rdd2.collectAsMap()` and use `rdd1.mapPartitions` to perform a local map-side join, completely eliminating the shuffle stage.
