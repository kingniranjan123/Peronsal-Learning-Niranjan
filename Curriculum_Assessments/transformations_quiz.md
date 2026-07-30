# Spark Transformations Mastery Assessment

## Part 1: True/False Questions

1. **Question:** In Spark, calling `.map()` followed by `.filter()` generates two separate stages in the physical execution plan.
   **Answer:** False
   **Mastery Explanation:** Both `.map()` and `.filter()` are narrow transformations. Spark pipelines narrow transformations into a single stage (and Tungsten fuses them into a single generated Java method) to avoid unnecessary network I/O and intermediate disk writes.

2. **Question:** Using `countDistinct` on a high-cardinality column allows Spark's Catalyst optimizer to insert a partial aggregation (HashAggregate) before the shuffle, significantly reducing shuffle write volume.
   **Answer:** False
   **Mastery Explanation:** Exact distinct counting (`countDistinct`) requires the full set of unique values, which breaks the two-phase partial aggregation optimization. Spark is forced to shuffle all rows. `approx_count_distinct` re-enables this optimization.

3. **Question:** The DAGScheduler is responsible for pushing down predicates into the Parquet reader to skip row groups.
   **Answer:** False
   **Mastery Explanation:** The Catalyst Optimizer handles logical optimizations like `PushDownPredicate` and `ColumnPruning`. The DAGScheduler's job is to translate the physical plan into stages separated by shuffle boundaries.

4. **Question:** Setting `spark.sql.autoBroadcastJoinThreshold` to a high value guarantees that Spark will use a BroadcastHashJoin for a table within that limit.
   **Answer:** False
   **Mastery Explanation:** Spark relies on catalog statistics to determine table size. If statistics are missing or stale (e.g., `ANALYZE TABLE` was never run), Catalyst may fall back to SortMergeJoin even if the actual data size is below the threshold, unless forced via a `broadcast()` hint.

5. **Question:** Narrow transformations keep intermediate rows in Tungsten's off-heap memory, completely bypassing JVM garbage collection.
   **Answer:** True
   **Mastery Explanation:** Tungsten uses `UnsafeRow` formats to store data natively in raw memory (off-heap) and processes it via `sun.misc.Unsafe`. This eliminates JVM object deserialization and avoids GC pauses.

6. **Question:** In a cluster with 10 TB of data, setting `spark.sql.shuffle.partitions` to 200 (the default) is optimal because it minimizes task scheduling overhead.
   **Answer:** False
   **Mastery Explanation:** For 10 TB, 200 partitions means each task processes 50 GB of data, which will inevitably cause massive spills to disk and OOM errors. The target should be ~100-200 MB per partition.

7. **Question:** An `explode()` operation in Spark requires a shuffle because it creates multiple rows from a single array element.
   **Answer:** False
   **Mastery Explanation:** `explode()` is a narrow transformation (syntactic sugar over `flatMap`). All exploded rows remain in the same partition. This can cause severe partition skew without a shuffle.

8. **Question:** A `repartition()` operation after a highly skewed `explode()` operation is generally considered an anti-pattern because shuffles should always be avoided.
   **Answer:** False
   **Mastery Explanation:** While shuffles are expensive, deliberately using `repartition(n, col)` after `explode()` balances the partitions. The cost of a small shuffle is heavily outweighed by avoiding severe straggler tasks in downstream wide transformations.

9. **Question:** The default `ShuffleManager` in modern Spark (since 1.2) is `SortShuffleManager`, which sorts records by partition ID on the map side.
   **Answer:** True
   **Mastery Explanation:** `SortShuffleManager` sorts output by partition ID (and spills if memory limits are exceeded) using Tungsten's `UnsafeExternalSorter`, keeping shuffle write operations highly efficient.

10. **Question:** A DataFrame persisted with `StorageLevel.MEMORY_ONLY` will gracefully spill to disk if it exceeds available executor memory.
    **Answer:** False
    **Mastery Explanation:** `MEMORY_ONLY` does not spill. If the cache exceeds available memory, partitions are simply dropped (LRU eviction) and must be recomputed upon the next action. `MEMORY_AND_DISK` is required for spilling.

## Part 2: Multiple Choice Questions

11. **Question:** Which component is responsible for translating a physical plan into a DAG of `Stage` objects by inserting stage boundaries?
    A) Catalyst Optimizer
    B) DAGScheduler
    C) TaskScheduler
    D) Tungsten Engine
    **Answer:** B
    **Mastery Explanation:** The DAGScheduler walks the physical plan backwards and creates a stage boundary wherever it encounters a `ShuffleDependency` (wide dependency).

12. **Question:** When reading a Parquet file, which Catalyst rule allows Spark to read only the columns explicitly requested by a `.select()` statement?
    A) PushDownPredicate
    B) CollapseProject
    C) ColumnPruning
    D) CombineFilters
    **Answer:** C
    **Mastery Explanation:** `ColumnPruning` ensures only necessary columns are fetched from the columnar Parquet storage, vastly reducing disk I/O and memory usage.

13. **Question:** Which of the following operations is a NARROW transformation?
    A) `distinct()`
    B) `coalesce()`
    C) `repartition()`
    D) `groupBy()`
    **Answer:** B
    **Mastery Explanation:** `coalesce()` combines partitions without a full shuffle by safely moving data among existing partitions on the same node (when reducing count).

14. **Question:** What is the primary cause of an O(n²) task submission spike in iterative ML Spark workloads?
    A) Setting `spark.sql.shuffle.partitions` too high
    B) Calling actions on un-cached DataFrames within a loop
    C) Using BroadcastHashJoin inside a loop
    D) Over-allocating executor memory
    **Answer:** B
    **Mastery Explanation:** Lazy evaluation forces Spark to re-evaluate the entire lineage from source every time an action is called. In a loop, un-cached data lineage grows exponentially, leading to massive recomputation overhead.

15. **Question:** Which physical join strategy performs a two-phase shuffle where both sides are sorted by the join key?
    A) BroadcastHashJoin
    B) ShuffledHashJoin
    C) SortMergeJoin
    D) NestedLoopJoin
    **Answer:** C
    **Mastery Explanation:** SortMergeJoin shuffles both datasets to ensure identical keys land on the same partition, sorts them, and then merges them. It is the most robust strategy for large tables.

16. **Question:** What is the consequence of one partition processing 90% of the data after a `groupBy` due to a highly skewed key?
    A) The DAGScheduler drops the straggler task.
    B) Spark throws a `SkewJoinException`.
    C) The entire stage must wait for that single task to complete, bottlenecking the job.
    D) AQE automatically skips the skewed partition.
    **Answer:** C
    **Mastery Explanation:** A stage boundary acts as a full barrier. No downstream stage can begin until 100% of the upstream tasks finish. A straggler dictates the stage's duration.

17. **Question:** Why does a `BroadcastHashJoin` consume significant executor JVM heap?
    A) Because the large table is cached in memory.
    B) Because Tungsten cannot process broadcasted data.
    C) Because the broadcasted table is deserialized into a Java `HashMap` for each task core.
    D) Because TorrentBroadcast requires on-heap buffers for network transfer.
    **Answer:** C
    **Mastery Explanation:** While the broadcast blocks sit off-heap, actual task execution deserializes them into an on-heap `HashMap` for fast lookup, causing heavy GC pressure if the table is moderately large (e.g., 300MB+).

18. **Question:** Which feature dynamically coalesces shuffle partitions at runtime to avoid the overhead of too many small tasks?
    A) Speculative Execution
    B) Adaptive Query Execution (AQE)
    C) Dynamic Resource Allocation
    D) Project Tungsten
    **Answer:** B
    **Mastery Explanation:** AQE inspects shuffle statistics mid-job and can dynamically coalesce small shuffle partitions (`spark.sql.adaptive.coalescePartitions.enabled=true`), removing the need for perfect manual tuning.

19. **Question:** What happens when `spark.speculation.multiplier` is triggered?
    A) Spark cancels the slow task.
    B) Spark re-plans the logical query tree.
    C) Spark launches a duplicate task on a different node and takes the first to finish.
    D) Spark allocates more memory to the slow task.
    **Answer:** C
    **Mastery Explanation:** Speculation handles straggler tasks (usually due to bad nodes/disks) by launching a duplicate. It does NOT solve data skew, as the duplicate task will process the same skewed data.

20. **Question:** What is the Tungsten `UnsafeRow` format?
    A) A JSON-based schema-less serialization format.
    B) A JVM object graph representation of a SQL Row.
    C) A raw binary byte-array format that bypasses JVM object overhead.
    D) A Parquet-specific decoding algorithm.
    **Answer:** C
    **Mastery Explanation:** `UnsafeRow` stores rows as raw bytes with a null bitset and variable-length sections, allowing SIMD operations and zero GC overhead.

21. **Question:** How does Spark perform a partial aggregation for a `groupBy().count()`?
    A) It uses a HashAggregate node locally on the map side before shuffling.
    B) It sorts the data first, then aggregates.
    C) It broadcasts the counts to the driver.
    D) It skips the shuffle entirely.
    **Answer:** A
    **Mastery Explanation:** Spark inserts a two-phase aggregation. The first `HashAggregate` computes local sums/counts per partition on the map side, reducing shuffle write data.

22. **Question:** Which action forces an explicit materialization of a `.persist()` operation without retrieving data to the driver?
    A) `show()`
    B) `count()`
    C) `explain()`
    D) `printSchema()`
    **Answer:** B
    **Mastery Explanation:** `count()` triggers the DAG execution across executors and caches the data. `show()` also triggers execution but pulls data to the driver, while `explain()`/`printSchema()` don't trigger execution.

23. **Question:** What is the primary difference between `repartition()` and `coalesce()`?
    A) `repartition` is narrow; `coalesce` is wide.
    B) `coalesce` only works on RDDs; `repartition` works on DataFrames.
    C) `repartition` performs a full shuffle; `coalesce` avoids shuffles when decreasing partitions.
    D) `coalesce` balances partition sizes better than `repartition`.
    **Answer:** C
    **Mastery Explanation:** `coalesce(n)` merges local partitions on the same worker without network I/O. `repartition(n)` always forces a round-robin shuffle.

24. **Question:** In the Catalyst Optimizer, which phase is responsible for generating multiple physical execution candidates?
    A) Analysis
    B) Logical Optimization
    C) Physical Planning
    D) Code Generation
    **Answer:** C
    **Mastery Explanation:** The `SparkPlanner` phase takes the optimized logical plan and generates physical candidates (e.g., SMJ vs BHJ) and selects the cheapest based on a cost model.

25. **Question:** Why is Java Serialization discouraged compared to Kryo in Spark?
    A) Kryo is the default in Spark 3.x.
    B) Java serialization produces payloads 2-5x larger and is significantly slower.
    C) Java serialization cannot handle case classes.
    D) Java serialization crashes on Tungsten rows.
    **Answer:** B
    **Mastery Explanation:** Java serialization carries heavy class metadata overhead, making shuffle writes bloated and slow. Kryo is much more compact.

## Part 3: Small Twist Questions

26. **Scenario:** You have a 10 GB table and a 5 MB table. You run `large.join(small, "id")`. Catalyst chooses SortMergeJoin. 
    **Twist:** Why didn't it choose BroadcastHashJoin, even though 5 MB is under the 10 MB threshold?
    **Answer:** The catalog statistics were missing or stale (e.g. `ANALYZE TABLE` was never run), so Spark didn't know the table was 5 MB.
    **Mastery Explanation:** Catalyst relies strictly on catalog metadata. Without size statistics, it conservatively defaults to SortMergeJoin.

27. **Scenario:** You run `df.filter(col("date") > "2024").count()`. The query takes 5 minutes. 
    **Twist:** You change the storage format from CSV to Parquet and it takes 10 seconds. What specific optimization fired?
    **Answer:** Predicate Pushdown / Parquet Row-Group Skipping.
    **Mastery Explanation:** Catalyst pushed the filter into the Parquet scan. Parquet's metadata allowed skipping entire blocks of data without reading them, impossible in CSV.

28. **Scenario:** You use `df.cache()` and call `.count()`. The UI shows storage as MEMORY_AND_DISK.
    **Twist:** You change it to `df.persist(StorageLevel.MEMORY_AND_DISK_SER)`. The memory footprint drops by 70%. Why?
    **Answer:** MEMORY_AND_DISK stores deserialized Java objects. The _SER variant stores Tungsten binary byte arrays.
    **Mastery Explanation:** Deserialized Java objects carry massive heap overhead (headers, pointers). Serialized storage keeps data in compact byte form, saving heap space at the cost of CPU cycles during read.

29. **Scenario:** A `groupBy` operation on `user_id` takes 2 hours due to a single straggler task.
    **Twist:** You enable `spark.sql.adaptive.skewJoin.enabled=true`, but the job still takes 2 hours. Why?
    **Answer:** AQE's Skew Join optimization only applies to Joins, not `groupBy` Aggregations.
    **Mastery Explanation:** AQE handles skewed joins by splitting partitions, but for `groupBy`, you still need manual salting (adding random prefixes to keys) to distribute the aggregation.

30. **Scenario:** You perform `df.withColumn("exploded", explode(col("array_col")))`. 
    **Twist:** Before the explode, all 200 partitions have exactly 10,000 rows. After explode, one partition has 5 million rows, causing OOM downstream. What happened?
    **Answer:** One or more arrays in that specific partition contained a massive number of elements (e.g. a bot user).
    **Mastery Explanation:** Explode is narrow and does not shuffle. The output cardinality multiplies locally, silently creating data skew in the specific partition holding the large arrays.

31. **Scenario:** You configure `spark.sql.shuffle.partitions = 2000` for a 50 GB dataset.
    **Twist:** The job is extremely slow. You change it to 500, and it speeds up 3x. Why?
    **Answer:** 2000 partitions for 50 GB means 25 MB per partition. This causes excessive task scheduling and overhead.
    **Mastery Explanation:** The ideal partition size is 100-200 MB. 50 GB / 200 MB = 250 partitions. 2000 partitions created too many tiny tasks, overwhelming the TaskScheduler and DAGScheduler.

32. **Scenario:** You have two DataFrames. You `df1.join(broadcast(df2))`.
    **Twist:** The job fails with an OutOfMemoryError on the Executors, even though df2 is only 50 MB on disk. Why?
    **Answer:** The 50 MB compressed Parquet file expands to 500+ MB of deserialized Java HashMaps in the executor heap.
    **Mastery Explanation:** Broadcast tables are kept in-memory as HashMaps per task. A high core count executor (e.g. 16 cores) will deserialize it 16 times, blowing up the JVM heap.

33. **Scenario:** You write `df.distinct().count()`. It triggers a massive shuffle.
    **Twist:** You change it to `df.dropDuplicates(Seq("user_id")).count()`. The shuffle data drops by 90%. Why?
    **Answer:** `distinct()` uses all columns as the grouping key.
    **Mastery Explanation:** `distinct()` shuffles the entire row payload. Limiting the deduplication key drops the shuffle payload size dramatically.

34. **Scenario:** You run `df.groupBy("type").agg(countDistinct("id"))`. The shuffle write is 100 GB.
    **Twist:** You change it to `df.groupBy("type").agg(approx_count_distinct("id"))`. Shuffle write drops to 2 GB. Why?
    **Answer:** `approx_count_distinct` allows Spark to perform a Map-side Partial Aggregation.
    **Mastery Explanation:** Exact distinct counting forces all raw records across the network to be evaluated. HyperLogLog (used by approx) allows local map-side aggregation of state, destroying the data volume before the shuffle.

35. **Scenario:** A job has 5 narrow transformations (filter, map, withColumn). 
    **Twist:** The physical plan shows them bundled into a single step called `WholeStageCodegen`. What is this?
    **Answer:** Tungsten's Code Generation.
    **Mastery Explanation:** Spark compiles the entire chain of narrow operations into a single Java function (a tight loop) at runtime, eliminating virtual function calls and reducing CPU branch mispredictions.

36. **Scenario:** You use `df.repartition(10)` to reduce files before writing to S3.
    **Twist:** The job slows down massively due to a full shuffle. You change it to `df.coalesce(10)` and it finishes instantly. Why?
    **Answer:** `coalesce` shrinks partitions without a shuffle.
    **Mastery Explanation:** `coalesce` simply points downstream tasks to read multiple existing partitions on the same node, avoiding the heavy network I/O of `repartition`.

37. **Scenario:** You have a heavily reused DataFrame `df`. You run `df.cache()`.
    **Twist:** During a long-running job, `df` suddenly starts recomputing from source. Why?
    **Answer:** The BlockManager evicted partitions due to LRU (Least Recently Used) memory pressure.
    **Mastery Explanation:** Caching is not a hard guarantee. If the executor memory fills up with other caches or shuffle blocks, older cached partitions are evicted and must be recomputed on demand.

38. **Scenario:** You run `df.select("a").filter(col("b") > 10)`.
    **Twist:** The query fails during the *Analysis* phase of Catalyst. Why?
    **Answer:** Column "b" does not exist in the schema, or its type cannot be compared to an integer.
    **Mastery Explanation:** The Analyzer is the first step where unresolved logical plans are validated against the Catalog (schema). If a column is missing, it fails here before optimization.

39. **Scenario:** You set `spark.sql.shuffle.partitions = 1` for a 10 GB file.
    **Twist:** The map phase finishes quickly, but the reduce phase crashes with `GC overhead limit exceeded`. Why?
    **Answer:** A single task is trying to process all 10 GB of data in its memory space.
    **Mastery Explanation:** 1 partition means all map outputs are shuffled into a single reducer. This creates a 10 GB task that easily exhausts the execution memory fraction, causing aggressive spilling and GC thrashing.

40. **Scenario:** You execute `df.filter(col("id") === 5).count()`. It takes 10 seconds.
    **Twist:** You run the exact same command again immediately, and it takes 10 seconds again (it wasn't cached). But the third time, it takes 1 second. Why? (Assume OS/Disk cache).
    **Answer:** The OS page cache (Linux filesystem cache) warmed up.
    **Mastery Explanation:** Even without Spark caching, the operating system caches recently read Parquet files in RAM. The first reads pulled from disk; subsequent reads pulled from OS memory.

## Part 4: Coding & Debugging Questions

41. **Code Snippet:**
    ```scala
    val df1 = spark.read.parquet("data1")
    val df2 = df1.filter(col("age") > 30)
    val df3 = df2.withColumn("is_adult", lit(true))
    df3.count()
    df3.show()
    ```
    **Bug/Inefficiency:** `df3` is computed twice from source.
    **Fix:** Add `df3.cache()` before the `count()`.
    **Mastery Explanation:** Because actions (`count`, `show`) trigger execution on un-cached lineages, the entire Parquet scan and narrow transformations execute twice.

42. **Code Snippet:**
    ```scala
    val data = spark.read.parquet("users") // 5 TB table
    val result = data.repartition(200).groupBy("city").count()
    ```
    **Bug/Inefficiency:** `repartition(200)` causes a completely unnecessary full shuffle before the `groupBy` shuffle.
    **Fix:** Remove `.repartition(200)`. Let `groupBy` handle the shuffle. Configure `spark.sql.shuffle.partitions` appropriately for 5 TB.
    **Mastery Explanation:** `groupBy` inherently performs a shuffle based on the grouping key. A blind round-robin `repartition` just adds a massive, useless 5 TB network transfer.

43. **Code Snippet:**
    ```scala
    val df = spark.read.parquet("transactions")
    val stats = df.groupBy("store_id").agg(countDistinct("customer_id"))
    ```
    **Bug/Inefficiency:** `countDistinct` prevents map-side partial aggregation.
    **Fix:** Use `approx_count_distinct("customer_id")` if exact precision is not critical.
    **Mastery Explanation:** This is the classic two-phase HashAggregate blocker. `approx_count_distinct` reduces shuffle write by 90%+ in high-cardinality situations.

44. **Code Snippet:**
    ```scala
    val largeDf = spark.read.parquet("events")
    val smallDf = spark.read.parquet("lookup") // 100 MB
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") // 10MB
    val joined = largeDf.join(smallDf, "id")
    ```
    **Bug/Inefficiency:** 100 MB exceeds the 10 MB threshold, triggering a massive SortMergeJoin for `largeDf`.
    **Fix:** Use `largeDf.join(broadcast(smallDf), "id")` to force the broadcast, provided executors have enough heap.
    **Mastery Explanation:** 100 MB is small enough to broadcast if executor memory is tuned properly. Forcing it avoids shuffling terabytes of `largeDf` data.

45. **Code Snippet:**
    ```scala
    val df = spark.read.parquet("logs")
    val exploded = df.withColumn("error", explode(col("error_stack")))
    val grouped = exploded.groupBy("error").count()
    ```
    **Bug/Inefficiency:** `explode` can cause severe partition skew, breaking the downstream `groupBy`.
    **Fix:** `val exploded = df.withColumn("error", explode(col("error_stack"))).repartition(col("error"))`
    **Mastery Explanation:** Adding a hash repartition on the grouping key after the explode balances the partitions *before* the wide transformation is evaluated.

46. **Code Snippet:**
    ```scala
    // Executor Memory: 4g
    val df = spark.read.parquet("data")
    df.persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
    df.count()
    ```
    **Bug/Inefficiency:** If `df` deserialized is larger than the 2 GB cache fraction, partitions are silently dropped, wasting compute.
    **Fix:** Use `MEMORY_AND_DISK_SER`.
    **Mastery Explanation:** `MEMORY_ONLY` stores fat Java objects. `_SER` stores compact Tungsten bytes, and `_DISK` ensures no recomputation is needed if it spills.

47. **Code Snippet:**
    ```scala
    val logs = spark.read.json("s3://logs/")
    val filtered = logs.filter(col("level") === "ERROR")
    filtered.write.parquet("s3://output/")
    ```
    **Bug/Inefficiency:** Predicate pushdown does not work effectively on JSON files.
    **Fix:** Convert the upstream ingestion to Parquet format before running heavy analytics.
    **Mastery Explanation:** JSON requires a full table scan and parsing of every row. Parquet's columnar footers allow Catalyst to skip I/O entirely.

48. **Code Snippet:**
    ```scala
    val df = spark.table("huge_table")
    df.orderBy("timestamp").write.parquet("s3://sorted/")
    ```
    **Bug/Inefficiency:** `orderBy` forces a global sort, creating a massive bottleneck on a few reducers.
    **Fix:** Use `sortWithinPartitions("timestamp")` if global total ordering is not strictly required.
    **Mastery Explanation:** `orderBy` requires range partitioning and shuffles all data to achieve a global order. `sortWithinPartitions` is a narrow-like local sort, vastly faster for most downstream uses.

49. **Code Snippet:**
    ```scala
    val data = spark.read.parquet("metrics")
    val res = data.groupBy("host").count().collect()
    ```
    **Bug/Inefficiency:** `.collect()` pulls all result data to the Driver JVM heap. If the number of unique hosts is in the millions, the Driver will OOM.
    **Fix:** Write to distributed storage: `res.write.parquet(...)` or use `.take(100)`.
    **Mastery Explanation:** The Driver heap is typically small (1-2GB). `.collect()` bypasses distributed processing and dumps the entire payload onto the single Driver JVM.

50. **Code Snippet:**
    ```scala
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    val df = spark.read.parquet("skewed_data")
    df.groupBy("key").count().show()
    ```
    **Bug/Inefficiency:** AQE is disabled, meaning Spark cannot dynamically handle skew or coalesce partitions.
    **Fix:** Set `spark.sql.adaptive.enabled` to `true` (default in Spark 3+).
    **Mastery Explanation:** Adaptive Query Execution inspects stage statistics at runtime. It is the most powerful tool in modern Spark for mitigating bad default shuffle partition counts and skew (for joins).
