# Optimization Master Class: Assessment

## Section 1: True/False Questions

**1. True/False: AQE can dynamically switch a Sort-Merge Join to a Broadcast Hash Join mid-flight if an intermediate dataset shrinks sufficiently.**
**Answer:** True
**Mastery Explanation:** Correct because AQE observes intermediate dataset sizes after shuffle stages. If a dataset size falls below the broadcast threshold (e.g., due to strong filtering), it downgrades from a costly Sort-Merge Join to a Broadcast Hash Join, avoiding the rest of the shuffle. Other options don't apply.

**2. True/False: Tungsten primarily improves performance by relying entirely on the JVM garbage collector for memory management.**
**Answer:** False
**Mastery Explanation:** False because Tungsten specifically *bypasses* the JVM object model and its GC overhead by utilizing the `Unsafe` API to allocate memory off-heap and storing data in raw binary formats.

**3. True/False: Catalyst evaluates multiple physical plans based solely on Rule-Based Optimization heuristics, ignoring actual table statistics.**
**Answer:** False
**Mastery Explanation:** False because Catalyst generates multiple physical plans and selects the best one using Cost-Based Optimization (CBO), which heavily relies on table statistics (cardinality, row counts, histograms).

**4. True/False: A Broadcast Hash Join entirely avoids the expensive shuffle phase by sending the larger table to all worker nodes.**
**Answer:** False
**Mastery Explanation:** False. A Broadcast Hash Join sends the *smaller* table to all worker nodes, not the larger one. Broadcasting a massive table would cause an Out Of Memory (OOM) error.

**5. True/False: Bucketing pre-shuffles and pre-sorts data, which can completely eliminate the `Exchange` and `Sort` steps in a subsequent Sort-Merge Join.**
**Answer:** True
**Mastery Explanation:** True. Bucketing physically partitions data by a hash function into a fixed number of buckets and sorts it on write. When joined on the bucket column, Spark recognizes this co-location and avoids redundant shuffling (`Exchange`) and sorting.

**6. True/False: AQE's skew join optimization works by splitting the skewed partition into smaller sub-partitions and duplicating the corresponding rows from the other table.**
**Answer:** True
**Mastery Explanation:** True. AQE dynamically detects massive partitions (stragglers) during shuffle, splits the large partition from one side, and broadcasts/duplicates the matching keys from the un-skewed side so they can be processed in parallel.

**7. True/False: Using the `broadcast()` function guarantees that the optimizer will use a Broadcast Hash Join without any risk of OutOfMemory errors, even if the dataframe is 10GB.**
**Answer:** False
**Mastery Explanation:** False. While `broadcast()` forces Catalyst to plan a Broadcast Hash Join overriding size estimations, it poses a massive OOM risk. If a 10GB dataframe is broadcasted, it will crash the driver and executors.

**8. True/False: Tungsten's Whole-Stage Code Generation fuses multiple operators into a single Java function, compiling queries into highly optimized bytecode.**
**Answer:** True
**Mastery Explanation:** True. Instead of using virtual function calls for each operator (Volcano iterator model), Tungsten generates unified Java bytecode that runs as a single function, maximizing CPU efficiency and cache locality.

**9. True/False: Cost-Based Optimization requires analyzing tables to generate exact data distributions (histograms) to accurately estimate output sizes for physical plan selection.**
**Answer:** True
**Mastery Explanation:** True. CBO needs precise statistical data (via `ANALYZE TABLE ... COMPUTE STATISTICS`) to calculate cost metrics correctly. Without histograms, CBO cannot make accurate cardinality estimations for complex filters.

**10. True/False: Tungsten utilizes the `Unsafe` API to allocate memory off-heap, meaning data is serialized into a compact columnar binary format and operated on directly without deserialization.**
**Answer:** True
**Mastery Explanation:** True. Tungsten's memory manager bypasses JVM limits using `Unsafe`, storing data in highly optimized binary formats that CPUs can process directly (via SIMD and cache alignment) without incurring the cost of Java object deserialization.

## Section 2: Multiple Choice Questions

**11. Which Spark component is primarily responsible for generating the Resolved Logical Plan?**
A) Tungsten Execution Engine
B) Cost-Based Optimizer
C) Catalog
D) Adaptive Query Execution
**Answer:** C
**Mastery Explanation:** The Catalyst optimizer consults the Catalog to validate and resolve table and column names from the Unresolved Logical Plan, producing the Resolved Logical Plan. The other components handle physical planning or runtime execution.

**12. What is the main danger of manually enforcing a Broadcast Hash Join using the `broadcast()` hint?**
A) It forces a global sort, taking down the driver.
B) If the table exceeds executor memory, it triggers an instant OOM exception.
C) It disables AQE automatically.
D) It ignores predicate pushdown.
**Answer:** B
**Mastery Explanation:** `broadcast()` forces Spark to collect the dataframe to the driver and send it to all executors. If it's too large, it overwhelms JVM memory causing OOM. It does not force a global sort or disable AQE.

**13. How does Tungsten's memory management differ from standard Java objects?**
A) It uses Java serialization instead of Kryo.
B) It allocates memory directly from the OS (off-heap) to bypass JVM garbage collection.
C) It uses a row-based format to improve JVM GC times.
D) It strictly limits memory to 256MB chunks.
**Answer:** B
**Mastery Explanation:** Standard Java objects have massive metadata overhead and trigger GC pauses. Tungsten uses off-heap allocation to pack data efficiently into binary arrays, saving space and avoiding GC.

**14. Under AQE, what trigger condition determines if a partition is "skewed"?**
A) The partition size exceeds the median size by a specified factor and a minimum byte threshold.
B) The partition count is greater than 200.
C) The data contains NULL values in the join key.
D) The total dataframe size exceeds 10GB.
**Answer:** A
**Mastery Explanation:** AQE uses configs like `skewedPartitionFactor` (relative to median) and `skewedPartitionThresholdInBytes` (absolute size) to identify skew dynamically during runtime.

**15. What specific step does Catalyst's Rule-Based Optimization perform?**
A) Choosing join algorithms based on cardinality.
B) Gathering histograms.
C) Constant folding and predicate pushdown.
D) Dynamic partition coalescing.
**Answer:** C
**Mastery Explanation:** RBO applies logical rules like predicate pushdown (moving filters close to data source) and constant folding (evaluating static expressions at compile time). Join selection is CBO, and coalescing is AQE.

**16. Which operation happens at the shuffle boundaries during AQE?**
A) Whole-stage code generation.
B) Gathering exact statistics about intermediate data to re-optimize execution.
C) Analyzing raw data from S3.
D) Broadcasting data to workers.
**Answer:** B
**Mastery Explanation:** AQE interrupts execution at shuffle stages (materialization points) to measure exactly how much data was produced. It then updates the physical plan with these fresh statistics.

**17. In the context of Spark SQL, what does predicate pushdown accomplish?**
A) Filters rows at the storage layer before bringing data into JVM memory.
B) Pushes join operations into memory instead of disk.
C) Sorts the dataset based on the filter predicate.
D) Pushes aggregates to the driver.
**Answer:** A
**Mastery Explanation:** By pushing predicates (filters) to the data source (like Parquet), Spark avoids reading irrelevant data from disk, massively reducing I/O and network serialization.

**18. What is necessary for two bucketed tables to completely eliminate the `Exchange` step during a join?**
A) They must be joined on the bucket column and have the exact same number of buckets.
B) One table must be broadcasted.
C) AQE must be disabled.
D) The bucket count must be prime.
**Answer:** A
**Mastery Explanation:** For Spark to skip the shuffle (Exchange), both tables must be pre-shuffled into the *same number* of buckets on the *same join keys*. If bucket counts differ, a shuffle is still required.

**19. Which CPU optimization does Tungsten leverage by using its custom binary format?**
A) Multi-threading execution on a single core.
B) Hyper-threading algorithms.
C) L1/L2 cache locality and SIMD instructions.
D) GPU acceleration.
**Answer:** C
**Mastery Explanation:** By aligning dense binary data in memory, Tungsten maximizes CPU cache hits and utilizes Single Instruction Multiple Data (SIMD) for processing vectorized data.

**20. Why might a statically planned Sort-Merge Join be sub-optimal compared to an AQE-optimized join?**
A) Static planning cannot sort data effectively.
B) Intermediate filter results might drastically reduce the data size, making a Broadcast Hash Join much faster, which static planning cannot foresee.
C) Sort-Merge Join is always the worst join type.
D) Static plans ignore rule-based optimization.
**Answer:** B
**Mastery Explanation:** Static plans rely on original table sizes. If a complex filter drops 99% of rows, static CBO doesn't know this in advance. AQE measures the actual output and can dynamically switch to a faster Broadcast Join.

**21. What command is required to provide CBO with precise column distributions?**
A) `CACHE TABLE`
B) `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS`
C) `OPTIMIZE TABLE`
D) `VACUUM TABLE`
**Answer:** B
**Mastery Explanation:** CBO needs histograms and column-level cardinality metrics to calculate costs. The `ANALYZE TABLE` command computes and stores these in the catalog.

**22. How does AQE prevent OOM errors during shuffle reads caused by data skew?**
A) By broadcasting the entire skewed partition.
B) By splitting the massive partition into smaller sub-partitions.
C) By increasing executor memory automatically.
D) By killing the straggler task.
**Answer:** B
**Mastery Explanation:** AQE mitigates skew by taking the single oversized partition and splitting it into multiple smaller tasks, allowing them to run in parallel without exceeding memory limits.

**23. What does Catalyst's Unresolved Logical Plan lack?**
A) SQL syntax tree.
B) Resolved table and column names validated against the Catalog.
C) Functional programming constructs.
D) Dataset schemas from memory.
**Answer:** B
**Mastery Explanation:** The initial Unresolved plan simply parses the code structure. It does not yet know if the tables or columns actually exist or what their data types are; this requires Catalog resolution.

**24. Which execution engine takes over after Catalyst selects a physical plan?**
A) DagScheduler
B) TaskScheduler
C) Tungsten
D) YARN
**Answer:** C
**Mastery Explanation:** Catalyst plans *what* to do, while Tungsten dictates *how* to do it at the hardware level (memory and CPU) via code generation and off-heap execution.

**25. What does the configuration `spark.sql.adaptive.coalescePartitions.enabled` do when AQE is enabled?**
A) Increases the number of shuffle partitions to match the data size.
B) Dynamically combines tiny shuffle partitions to avoid scheduling overhead.
C) Splits skewed partitions.
D) Coalesces all data to a single driver partition.
**Answer:** B
**Mastery Explanation:** Many small partitions cause task scheduling overhead. AQE automatically merges these small partitions post-shuffle into fewer, optimal-sized partitions.

## Section 3: "Small Twist" Questions

**26. Twist: You enable bucketing on `orders` (200 buckets) and `returns` (400 buckets) on `customer_id`. Will the shuffle be eliminated for a join on `customer_id`?**
**Answer:** No.
**Mastery Explanation:** Because the number of buckets differs (200 vs 400), the data distribution is incompatible. Spark will be forced to trigger a shuffle (`Exchange`) on one or both tables to align the partitions before joining.

**27. Twist: You enable CBO, but forget to run `ANALYZE TABLE` before executing a complex multi-join query. What happens to the optimization?**
**Answer:** CBO falls back to basic file-size heuristics.
**Mastery Explanation:** Without table and column statistics in the Catalog, CBO cannot calculate costs accurately. It degrades to using simple byte-size estimates, potentially leading to suboptimal join ordering and execution plans.

**28. Twist: `spark.sql.autoBroadcastJoinThreshold` is 10MB. Table A is 5MB on disk but expands to 50MB in memory due to complex strings. Spark chooses Broadcast Join. Does it succeed or fail?**
**Answer:** It will likely fail with an OOM or exceed limits.
**Mastery Explanation:** Spark relies on compressed disk size or statistical size for the threshold. When loaded into Java objects, data expands massively. Broadcasting 50MB per executor might succeed, but the driver must collect it first, potentially exceeding driver memory or `spark.driver.maxResultSize`.

**29. Twist: You set `spark.sql.adaptive.skewJoin.skewedPartitionFactor` to 5. The largest partition is 500MB, median is 150MB. Will AQE split it?**
**Answer:** No.
**Mastery Explanation:** 500MB is not 5 times larger than the median of 150MB (150 * 5 = 750MB). Therefore, it does not trigger the skew join logic, even if it exceeds the absolute byte threshold.

**30. Twist: A dataframe has 200 shuffle partitions. You apply a `limit(10)` operation without AQE. Will Spark evaluate all 200 partitions?**
**Answer:** No.
**Mastery Explanation:** Spark's Catalyst optimizer pushes down the `LocalLimit` and `GlobalLimit`. It will execute tasks on partitions sequentially or in small batches until it finds 10 rows, then cancel the remaining tasks.

**31. Twist: You bucket Table A by `id` (200 buckets) and Table B by `id` (200 buckets). You join them on `id` AND `date`. Does it eliminate the shuffle?**
**Answer:** Yes.
**Mastery Explanation:** The join includes the bucketed column (`id`). As long as the partitioning guarantees co-location for `id`, rows with the same `id` and `date` will be in the same bucket. The shuffle on `id` is avoided, though the local sort might adjust.

**32. Twist: You broadcast a 10MB table using `broadcast(df)`. The executor has 1GB memory. However, the query uses a `crossJoin` with a 1 Billion row table. What causes the performance issue?**
**Answer:** The Cartesian product output size.
**Mastery Explanation:** The broadcast itself is safe (10MB fits in 1GB). However, a cross join generates a Cartesian product. 1 Billion * (rows in 10MB table) results in trillions of rows, causing immense CPU/memory pressure regardless of broadcast.

**33. Twist: AQE is enabled. A predicate reduces a 100GB intermediate table to 10MB. The other join table is 500GB. `spark.sql.autoBroadcastJoinThreshold` is 20MB. Will a Sort Merge Join happen?**
**Answer:** No.
**Mastery Explanation:** Because AQE measures the intermediate size dynamically, it detects the 100GB table is now 10MB (which is under the 20MB threshold). AQE will downgrade the Sort-Merge Join to a Broadcast Hash Join dynamically at runtime.

**34. Twist: Table A is bucketed by `user_id` into 100 buckets. Table B is bucketed by `user_id` into 100 buckets. You join them on `user_id`, but one table was saved using `sortBy("user_id")` and the other wasn't. Will Spark skip the Sort step?**
**Answer:** It skips shuffle, but performs a Sort on the unsorted table.
**Mastery Explanation:** Bucketing guarantees data distribution (skipping `Exchange`). However, a Sort-Merge Join requires both sides to be locally sorted. Spark will inject a `Sort` step into the physical plan for the unsorted table.

**35. Twist: You have a highly skewed dataset. You configure AQE skew join thresholds properly, but you are performing a LEFT OUTER join, and the skew is on the right table. Does AQE optimize this skew?**
**Answer:** No.
**Mastery Explanation:** AQE skew join optimization cannot split the skewed partition on the right side of a LEFT OUTER join, because doing so would risk generating duplicate rows in the left (preserved) table when matched against the split sub-partitions.

**36. Twist: Tungsten code generation is enabled. You write a UDF (User Defined Function) in Python to extract a substring. Does this operation benefit from Whole-Stage Code Generation?**
**Answer:** No.
**Mastery Explanation:** Python UDFs cannot be compiled into Java bytecode by Tungsten. The data must be serialized out of the JVM, sent to a Python process, evaluated, and serialized back. This breaks Whole-Stage Code Generation and plummets performance.

**37. Twist: You run `ANALYZE TABLE` to generate statistics, but the table underlying files are updated immediately after. Does CBO use the new statistics automatically?**
**Answer:** No.
**Mastery Explanation:** CBO relies on static statistics stored in the Catalog. If the underlying data changes, the statistics become stale. You must manually re-run `ANALYZE TABLE` to update them.

**38. Twist: `spark.sql.sources.bucketing.enabled` is false. You perform a join on two tables that were previously saved with `bucketBy(200, "id")`. What is the physical plan?**
**Answer:** A standard Sort-Merge Join with an `Exchange` (shuffle).
**Mastery Explanation:** Because the bucketing configuration is explicitly disabled at the session level, Catalyst ignores the bucket metadata on the Parquet files and executes a full shuffle as if the data were not bucketed.

**39. Twist: You filter on `date = '2023-01-01'` before a join. The table is partitioned by `date`. Does Spark read the entire table and filter in memory, or use predicate pushdown?**
**Answer:** It uses Partition Pruning.
**Mastery Explanation:** This is better than predicate pushdown. Because `date` is a directory partition, Spark entirely skips reading files in other directories. It doesn't even need to push the predicate to the Parquet reader; the files are completely ignored.

**40. Twist: AQE dynamically coalesces partitions. Initially there are 2000 partitions. AQE coalesces them to 500. Can you rely on `.mapPartitions` to process exactly 2000 files independently?**
**Answer:** No.
**Mastery Explanation:** AQE physically combines the shuffle blocks into 500 tasks. If your logic in `.mapPartitions` assumes a 1:1 mapping with the original 2000 partitions (e.g., writing to distinct files or hitting an API), it will fail or behave incorrectly because there are only 500 execution contexts.

## Section 4: Coding & Debugging Questions

**41. Debugging: Code has `df.repartition(100, "col1").join(df2.repartition(100, "col1"), "col1")`. Why is this an anti-pattern?**
**Answer & Mastery Explanation:** The explicit `repartition()` forces a shuffle. Spark's join will then perform *another* shuffle (`Exchange hashpartitioning`) internally to guarantee co-location, resulting in double shuffling. You should let Catalyst handle the shuffle, or use bucketing if writing to disk.

**42. Debugging: You observe an OOM error on one executor after a long delay, while others finish in seconds. The logical plan shows a SortMergeJoin. What is the root cause?**
**Answer & Mastery Explanation:** Data Skew. One partition contains a massively disproportionate number of keys. While other executors process small partitions quickly, the skewed partition overloads a single executor's memory during the sort phase. Enabling AQE skew join is the fix.

**43. Debugging: A user sets `spark.sql.autoBroadcastJoinThreshold` to 2GB to avoid SortMergeJoin on a 1.5GB dimension table. The driver crashes before tasks start. Why?**
**Answer & Mastery Explanation:** To broadcast a table, Spark must first execute a `collect()` operation, pulling the entire 1.5GB dataset to the Driver node's memory. If the driver is configured with 1GB of memory, it instantly hits an OutOfMemoryError before any executor tasks can begin.

**44. Debugging: A PySpark UDF is applied to a numeric column before filtering (`df.filter(my_udf(col("amount")) > 100)`). Performance plummets compared to native operators. Why?**
**Answer & Mastery Explanation:** PySpark UDFs break Catalyst optimizations. The optimizer cannot inspect the contents of `my_udf`, meaning it cannot perform Predicate Pushdown to the Parquet layer. All data must be loaded into memory, serialized to Python, evaluated, and passed back. Native functions (`pyspark.sql.functions`) should be used instead.

**45. Debugging: You read from a JDBC source with `spark.read.jdbc(url, "table")`. The table is 100GB. The query runs entirely on a single executor task. How to fix?**
**Answer & Mastery Explanation:** By default, JDBC reads use a single partition. You must specify partitioning parameters: `partitionColumn`, `lowerBound`, `upperBound`, and `numPartitions` in the read options. This allows Spark to generate parallel SQL queries to the database and distribute the load.

**46. Debugging: An AQE skew join configuration is set (factor=5, size=256MB). The skewed partition is 300MB, median is 10MB. The join is an INNER join. But the DAG shows a normal SortMergeJoin, no skew split. Why might this happen?**
**Answer & Mastery Explanation:** AQE might not trigger if there's no Shuffle phase. If the datasets were already bucketed or co-partitioned appropriately, Spark skips the exchange. Alternatively, if the data is heavily compressed, the in-memory size might exceed thresholds but AQE operates on map output statistics.

**47. Debugging: You use `df.cache()`, then perform 5 different aggregations on it. However, the Spark UI shows the Parquet file is being read 5 times. What's wrong?**
**Answer & Mastery Explanation:** `df.cache()` is a lazy transformation. If you don't call an Action (like `.count()`) immediately after caching, Spark won't materialize the cache in memory. Subsequent actions trigger the full lineage computation from disk every time.

**48. Debugging: A user buckets a 10MB table into 10,000 buckets to "maximize parallelism". Query performance is terrible. Why?**
**Answer & Mastery Explanation:** Over-bucketing creates massive metadata overhead. 10,000 buckets for 10MB means writing 10,000 tiny files of 1KB each. The NameNode (in HDFS/S3) is choked, and Spark spends more CPU time scheduling tiny tasks and opening/closing files than actually processing data (the small files problem).

**49. Debugging: `df.withColumn("new", expr("cast(rand() * 100 as int)")).join(df2, "id")`. The random column gives unexpected/non-deterministic results upon retries or action calls. Why?**
**Answer & Mastery Explanation:** Catalyst optimizer can re-evaluate non-deterministic functions (like `rand()`) multiple times if tasks fail and retry, or if multiple downstream actions are called without caching. The random values will change on every evaluation, causing inconsistent join results.

**50. Debugging: The physical plan contains an `Exchange hashpartitioning` step before an aggregation, but you already partitioned the data by the grouping key when writing to HDFS. Why is Spark shuffling again?**
**Answer & Mastery Explanation:** Partitioning data into directories on write (e.g., `partitionBy("date")`) optimizes *reads* via partition pruning. It does *not* provide Spark with guarantees about hash-distribution for aggregations. Spark still must hash-shuffle the data across the cluster to group keys together, unlike bucketing.
