# 🔥 Master Class Assessment: Grouping and Sorting

## Section 1: True/False (10 Questions)

**1. True/False:** Spark 3.x Adaptive Query Execution (AQE) automatically handles data skew in arbitrary `groupBy` aggregations just as it does for joins.
- **Answer:** False
- **Mastery Explanation:** AQE's skew handling applies exclusively to joins (shuffle-hash and sort-merge), not arbitrary `groupBy` aggregations. Manual salting remains the standard and required fix for aggregation skew.

**2. True/False:** `HashAggregateExec` utilizes Tungsten's `BytesToBytesMap` for off-heap partial aggregation, effectively bypassing JVM garbage collection for the aggregate map.
- **Answer:** True
- **Mastery Explanation:** Tungsten stores data as compact, aligned bytes off-heap using the `UnsafeRow` binary format. This eliminates JVM object overhead and reduces GC pressure significantly compared to generic `Row` objects.

**3. True/False:** Using `orderBy` will inherently always perform a range-partition shuffle, which requires a pre-shuffle sampling pass to build split points.
- **Answer:** True
- **Mastery Explanation:** `orderBy` imposes a total global order across the dataset. To guarantee this, Spark must first sample the data to create a `RangePartitioner`, then shuffle all data into those non-overlapping ranges before sorting locally.

**4. True/False:** `sortWithinPartitions` performs a map-side sort and requires an O(N log N) shuffle across the cluster to guarantee correct local order.
- **Answer:** False
- **Mastery Explanation:** `sortWithinPartitions` applies a local sort *without* a shuffle. It completes in O((N/P) log(N/P)) time purely on the map side, making it dramatically cheaper than `orderBy` (O(1) network cost).

**5. True/False:** When an aggregate function is not hash-combinable (e.g., `collect_list`), Spark falls back to `SortAggregateExec` which is typically 3-5x slower.
- **Answer:** True
- **Mastery Explanation:** If the function cannot be maintained in a hash map safely, Catalyst falls back to a sort-based approach, which is significantly slower due to the requisite sorting overhead before aggregation.

**6. True/False:** The salting technique for aggregation skew requires configuring `spark.sql.adaptive.skewJoin.enabled=true` to take effect.
- **Answer:** False
- **Mastery Explanation:** Salting is a manual code-level technique (appending a random suffix, aggregating, stripping, and re-aggregating). It does not rely on AQE configurations at all.

**7. True/False:** The secondary sort pattern (repartition by primary key, then sortWithinPartitions by secondary key) forces all rows for a group into the executor's memory simultaneously.
- **Answer:** False
- **Mastery Explanation:** The entire point of the secondary sort pattern is to *avoid* materializing whole groups in memory. By pre-sorting physically, downstream operators like Window functions or Pandas UDFs can stream over the group sequentially.

**8. True/False:** `ColumnPruning` optimization pushes down column selection to happen after the shuffle to minimize memory footprint during the reduce phase.
- **Answer:** False
- **Mastery Explanation:** `ColumnPruning` drops unused columns *before* the shuffle. This critical optimization reduces the volume of data serialized and transmitted across the network, limiting the Netty transfer payload.

**9. True/False:** `SortShuffleManager` bypass-merge mode is activated for small partition counts to avoid sort overhead when no map-side combine is needed.
- **Answer:** True
- **Mastery Explanation:** If partitions are ≤ `spark.shuffle.sort.bypassMergeThreshold` (default 200) and no combine is needed, Spark bypasses the expensive map-side sort entirely and directly writes partition files.

**10. True/False:** Writing globally sorted data to Parquet using `orderBy` ensures each output file contains well-organized row groups for predicate pushdown, while avoiding shuffle costs.
- **Answer:** False
- **Mastery Explanation:** While it does result in well-organized row groups, it fundamentally *does not* avoid shuffle costs. It forces a full range-partition shuffle on the entire dataset. `sortWithinPartitions` achieves the well-organized row groups without the massive shuffle penalty.

---

## Section 2: Multiple Choice (15 Questions)

**11. Which physical operator is preferred by Catalyst for grouped aggregation?**
- A) SortAggregateExec
- B) HashAggregateExec
- C) ObjectHashAggregateExec
- D) WindowGroupAggregate
- **Correct Answer:** B
- **Mastery Explanation:** Catalyst prefers `HashAggregateExec` because it leverages Tungsten's off-heap `BytesToBytesMap` for fast, GC-free partial aggregation. `SortAggregateExec` is used only as a fallback for non-hash-combinable expressions.

**12. What is the primary purpose of the `RangePartitioner` in Spark?**
- A) To co-locate identical keys for hash aggregation
- B) To build split points for globally ordering data in `orderBy`
- C) To partition data for `sortWithinPartitions`
- D) To split skewed partitions during AQE skew optimization
- **Correct Answer:** B
- **Mastery Explanation:** Used exclusively by `orderBy`, the `RangePartitioner` samples the dataset to define boundary split points, ensuring each resulting partition represents a non-overlapping key range for a total global sort.

**13. Why is `collect_list` considered an anti-pattern for large, high-cardinality groups?**
- A) It triggers an immediate collect to the driver
- B) It forces all events for a key into a single executor's memory as a JVM ArrayBuffer, risking OOM
- C) It causes Catalyst to skip Whole-Stage Code Generation
- D) It cannot be serialized by Kryo
- **Correct Answer:** B
- **Mastery Explanation:** `collect_list` materializes the entire group of rows into memory at once. If a single user or key has millions of rows, it will breach executor heap limits and cause a GC overhead limit OOM.

**14. How does `HashAggregateExec` handle partial aggregations when the off-heap map exceeds `spark.memory.fraction`?**
- A) It throws a Java OutOfMemoryError
- B) It triggers AQE to repartition the data
- C) It spills sorted runs to disk and merges them, similar to an external merge sort
- D) It falls back to `SortAggregateExec` in-memory
- **Correct Answer:** C
- **Mastery Explanation:** Spark does not crash. It safely spills the sorted runs of the partial aggregate map to disk and merges them during the read phase, behaving mechanically like an external merge sort to preserve memory bounding.

**15. What format does `HashAggregateExec` use internally for storing partial aggregates?**
- A) Java Objects
- B) Kryo Serialized Bytes
- C) Tungsten's off-heap UnsafeRow binary format
- D) Arrow RecordBatches
- **Correct Answer:** C
- **Mastery Explanation:** Tungsten's `UnsafeRow` represents rows as raw binary data with bitsets for null tracking off-heap, bypassing the JVM completely to drastically cut garbage collection costs.

**16. Which of the following is true about `sortWithinPartitions` vs `orderBy` when writing to Parquet for downstream range scans?**
- A) `orderBy` should always be used to ensure Parquet files are globally ordered.
- B) `sortWithinPartitions` is better because it avoids a global shuffle while still providing local min/max stats for predicate pushdown.
- C) Neither provides benefits for Parquet writes.
- D) `orderBy` uses TimSort while `sortWithinPartitions` uses RadixSort.
- **Correct Answer:** B
- **Mastery Explanation:** `sortWithinPartitions` organizes the data within the output files perfectly for Parquet min/max statistics, enabling massive I/O reduction downstream without paying the heavy network tax of `orderBy`.

**17. In the salting technique for data skew in aggregation, how many shuffles are typically involved?**
- A) 0
- B) 1
- C) 2
- D) 3
- **Correct Answer:** C
- **Mastery Explanation:** Two shuffles occur. The first is on the salted key (spreading the hot key across N tasks). The second is on the original key (merging the tiny partial aggregates). You trade a second small shuffle for the elimination of a massive task bottleneck.

**18. What does Whole-Stage Code Generation (WSCG) do during a `groupBy` with a simple `sum`?**
- A) Compiles the SQL query to a Python UDF
- B) Fuses sort, hash map probing, and aggregation steps into a single tight Java bytecode loop per stage
- C) Generates an execution DAG for the cluster manager
- D) Optimizes the shuffle network transport via Netty zero-copy
- **Correct Answer:** B
- **Mastery Explanation:** WSCG collapses chains of physical operators into a single Java function loop, eliminating virtual method dispatch overhead and object boxing per row. 

**19. A stage containing a skewed `groupBy` operation will most likely exhibit which characteristic in the Spark UI?**
- A) High driver GC time
- B) Max task duration being orders of magnitude larger than median task duration
- C) Many small tasks completing in under 1 millisecond
- D) Uniform task durations but overall slow stage execution
- **Correct Answer:** B
- **Mastery Explanation:** Skew forces the majority of data for a specific key into a single task. The stage cannot finish until this task finishes, leading to a long tail where max duration is e.g. 45 mins while median is 30 seconds.

**20. When Catalyst applies `PushDownPredicate` before a shuffle, what is the primary benefit?**
- A) It pre-sorts the data
- B) It reduces the volume of data serialized and transmitted across the network
- C) It forces bypass-merge mode in the SortShuffleManager
- D) It eliminates the need for map-side combine
- **Correct Answer:** B
- **Mastery Explanation:** Filtering rows prior to the network shuffle ensures you don't pay network I/O, serialization, and deserialization costs for data you are going to discard anyway.

**21. What happens if a `Window` function's `ORDER BY` clause matches the physical sort order already present in a partition?**
- A) Spark resorts the data anyway to guarantee correctness
- B) Spark elides the internal sort step, visible in the query plan as `Window` without a preceding `Sort` operator
- C) Spark throws an `AnalysisException`
- D) Spark skips the window function completely
- **Correct Answer:** B
- **Mastery Explanation:** Catalyst detects the physical layout matches the logical requirement of the Window and skips the redundant sort step, drastically reducing CPU usage.

**22. What determines the value of `SALT_FACTOR` in a manual salting implementation for skew?**
- A) Always use 200 to match `spark.sql.shuffle.partitions`
- B) It should be `ceil(hot_key_row_count / target_partition_size_rows)`
- C) It must be equal to the number of worker nodes
- D) It is automatically determined by AQE
- **Correct Answer:** B
- **Mastery Explanation:** `SALT_FACTOR` dictates how many partitions the skewed key is split into. It should be sized so that the hot key's rows divided by the factor yield standard, manageable partition sizes.

**23. During an `orderBy` operation, when does Spark perform sampling?**
- A) During the reduce phase to verify order
- B) It doesn't sample; it reads the entire dataset twice
- C) On the map side, before the shuffle, to build the RangePartitioner split-point array
- D) On the driver, after the data is collected
- **Correct Answer:** C
- **Mastery Explanation:** Before performing the range-partition shuffle, Spark must determine how to divide the global dataset equally. It runs a sampling job on the map side to establish these partition boundaries.

**24. Which shuffle manager mode avoids sorting overhead when no map-side combine is needed and partition counts are small?**
- A) Bypass-merge mode
- B) Tungsten-sort mode
- C) Hash-shuffle mode
- D) Unsafe-shuffle mode
- **Correct Answer:** A
- **Mastery Explanation:** When `spark.shuffle.sort.bypassMergeThreshold` is respected and no partial aggregation is present, Spark opens one file per reduce partition on the map side to avoid the costly sort step entirely.

**25. What is the time complexity of the `orderBy` operation across the entire dataset?**
- A) O(N)
- B) O(N log N)
- C) O((N/P) log(N/P))
- D) O(K)
- **Correct Answer:** B
- **Mastery Explanation:** `orderBy` implements a total global sort, which theoretically and practically requires O(N log N) complexity, augmented by the network cost of a full shuffle.

---

## Section 3: Small Twist (15 Questions)

**26. Scenario:** You change an aggregation from `F.sum("amount")` to `F.collect_list("amount")`.
**Twist:** How does this small change drastically alter the physical execution plan?
- A) It triggers an immediate OOM error during analysis.
- B) Catalyst falls back from `HashAggregateExec` to `SortAggregateExec`.
- C) Catalyst automatically applies salting.
- D) The shuffle is completely eliminated.
- **Correct Answer:** B
- **Mastery Explanation:** `sum` is a hash-combinable function that works well in a `BytesToBytesMap`. `collect_list` creates an ever-growing sequence that cannot be efficiently updated in an off-heap hash map, forcing Spark to abandon HashAggregate for SortAggregate.

**27. Scenario:** You are writing data to Parquet using `df.sortWithinPartitions("timestamp")`.
**Twist:** You change this to `df.orderBy("timestamp")`. What is the primary negative consequence?
- A) It corrupts the Parquet min/max statistics.
- B) It introduces a full O(N log N) network shuffle, massively increasing runtime, for unnecessary global ordering.
- C) It causes the output to be written to a single partition file.
- D) It disables Tungsten memory management.
- **Correct Answer:** B
- **Mastery Explanation:** `sortWithinPartitions` performs locally with O(1) network cost. Changing it to `orderBy` triggers a range-partition shuffle on the whole dataset without adding any value for individual Parquet file reads.

**28. Scenario:** You implement manual salting to fix a skewed `groupBy("user_id")` by appending `_0` to `_9` randomly.
**Twist:** You decide to use a static salt `_1` for all rows to simplify the code. What happens?
- A) The skew remains exactly the same because all skewed rows still hash to the same single reduce task.
- B) The job runs 10x faster due to reduced shuffle volume.
- C) AQE automatically fixes the static salt.
- D) The data is perfectly balanced.
- **Correct Answer:** A
- **Mastery Explanation:** The purpose of salting is to inject randomness so identical keys map to different hashes. A static suffix simply creates a new static key, mapping all rows to a single partition again and failing to solve the skew.

**29. Scenario:** A developer enables `spark.sql.adaptive.skewJoin.enabled=true` to fix a skewed `groupBy` aggregation.
**Twist:** Why does the skew persist despite this configuration?
- A) AQE requires Whole-Stage Code Generation to be disabled.
- B) The setting only applies to shuffle-hash and sort-merge joins, not to `groupBy` aggregations.
- C) The salt factor was not provided.
- D) The `groupBy` key must be an integer for AQE to work.
- **Correct Answer:** B
- **Mastery Explanation:** AQE dynamic skew mitigation splits partitions on one side of a join and replicates the other side. This mechanism is completely inapplicable to simple group-by aggregations.

**30. Scenario:** You are doing a `repartition(200, "user_id").sortWithinPartitions("user_id", "timestamp")`.
**Twist:** You swap the order to `repartition(200, "timestamp").sortWithinPartitions("user_id", "timestamp")`. What is the result?
- A) The logic remains correct, and performance is identical.
- B) The secondary sort pattern is broken; all events for a user are scattered across partitions based on their timestamp, destroying group co-location.
- C) The query fails with an AnalysisException.
- D) Catalyst optimizes the repartition key automatically.
- **Correct Answer:** B
- **Mastery Explanation:** Repartitioning by the primary key ensures all data for one user lands on the same executor. Repartitioning by timestamp sprays a user's timeline randomly across the cluster, completely nullifying the downstream window/aggregation logic.

**31. Scenario:** You run `df.groupBy("id").agg(F.sum("val"))` and it uses `HashAggregateExec`.
**Twist:** You change `spark.sql.codegen.wholeStage` from `true` to `false`. What happens?
- A) The query falls back to `SortAggregateExec`.
- B) Spark throws an error.
- C) The execution falls back to a slower chain of iterator calls with virtual method dispatch and per-row object allocations.
- D) The job avoids the shuffle phase entirely.
- **Correct Answer:** C
- **Mastery Explanation:** Without WSCG, Spark evaluates rows via Volcano iterator models, calling `next()` up the operator tree and incurring massive CPU overhead for virtual function calls and boxing.

**32. Scenario:** Your `groupBy` task spills to disk.
**Twist:** You increase `spark.memory.fraction`. How does this change the spill behavior?
- A) It forces the data to be spilled sooner.
- B) It provides a larger off-heap allocation for the `BytesToBytesMap`, delaying or preventing the spill of partial aggregates.
- C) It disables Tungsten RadixSort.
- D) It switches the aggregation to use `SortAggregateExec`.
- **Correct Answer:** B
- **Mastery Explanation:** The execution memory pool directly dictates how large the partial hash map can grow before Spark determines memory is exhausted and safely flushes sorted runs to disk.

**33. Scenario:** You are applying a window function `Window.partitionBy("user_id").orderBy("timestamp")`.
**Twist:** Before applying the window, you add `repartition(200, "user_id").sortWithinPartitions("user_id", "timestamp")`. What does Catalyst do?
- A) It throws a warning for redundant sorting.
- B) It executes two Sort operators in the physical plan.
- C) It elides (skips) the internal Sort step of the Window function because the data is already physically ordered.
- D) It disables the window function.
- **Correct Answer:** C
- **Mastery Explanation:** Catalyst is smart enough to detect that the physical characteristics imposed by `sortWithinPartitions` perfectly satisfy the logical requirements of the `Window`, safely stripping the redundant sort from the execution plan.

**34. Scenario:** A query filters data: `df.filter("date = '2023-01-01'").groupBy("user_id").sum("amount")`.
**Twist:** You remove the `.filter(...)` before the `groupBy` and apply it *after* a `.withColumn` that generates the date. How does this affect the shuffle?
- A) Performance improves due to later filtering.
- B) Shuffle payload dramatically increases because all dates must be shuffled before being filtered out.
- C) AQE automatically pushes the filter down anyway.
- D) The shuffle manager bypasses merging.
- **Correct Answer:** B
- **Mastery Explanation:** Moving filters after barriers like `.withColumn` or aggregations disables `PushDownPredicate`. The result is Spark must shuffle 100% of the dataset, only to filter out 90% of it on the reduce side.

**35. Scenario:** You have `df.select("a", "b", "c", "d").groupBy("a").agg(F.max("b"))`.
**Twist:** You remove the initial `.select(...)` completely and just use `df.groupBy("a").agg(F.max("b"))`. What happens to columns "c" and "d"?
- A) They are shuffled across the network and then ignored.
- B) Catalyst's `ColumnPruning` rule drops them automatically before the shuffle, so performance is identical.
- C) The query fails because unaggregated columns are present.
- D) They are included in the final output.
- **Correct Answer:** B
- **Mastery Explanation:** You don't need manual selects before grouping. Catalyst maps the logical dependency graph and automatically trims columns "c" and "d" from the underlying scan and map-side execution, minimizing shuffle size automatically.

**36. Scenario:** You perform `df.orderBy("val")` on a dataset with 1 billion rows.
**Twist:** You change `spark.sql.execution.rangeExchange.sampleSizePerPartition` from 1,000,000 to 10. What is the most likely risk?
- A) The sampling phase takes hours.
- B) The resulting split points for the RangePartitioner are highly inaccurate, leading to massively unbalanced partitions during the shuffle.
- C) Spark falls back to HashPartitioner.
- D) It disables Whole-Stage Code Generation.
- **Correct Answer:** B
- **Mastery Explanation:** The sampling phase defines the boundaries for the shuffle. If the sample size is microscopic, it won't represent the true distribution of data, causing all data to cluster into a few hot partitions during the `orderBy` exchange.

**37. Scenario:** You write output to Parquet after `df.repartition(10)` which results in 10 files.
**Twist:** You use `df.coalesce(10)` instead. What is the difference in execution?
- A) `coalesce` triggers a full shuffle, `repartition` does not.
- B) `coalesce` avoids a full network shuffle by simply combining local partitions on the map side, potentially causing task skew.
- C) `coalesce` sorts the data locally.
- D) There is absolutely no difference.
- **Correct Answer:** B
- **Mastery Explanation:** `coalesce` attempts to minimize network I/O by collapsing partitions locally without shuffling. While faster, it forces map tasks on specific nodes to process massively unequal amounts of data if the upstream data is unbalanced.

**38. Scenario:** You use `df.orderBy("id")` and notice your output is 200 files.
**Twist:** You append `.limit(100)` at the very end of your query. How does Spark execute this?
- A) It sorts the entire dataset globally, then takes 100 rows.
- B) It takes 100 rows randomly, then sorts them.
- C) It pushes down a local limit before the shuffle, shuffles only the top 100 per partition, and then performs a final global limit and sort.
- D) It throws an exception.
- **Correct Answer:** C
- **Mastery Explanation:** Catalyst implements Top-K optimization. It takes 100 locally from each map partition *before* the shuffle, dramatically reducing network traffic, and merges the partial tops into a final 100 on the reduce side.

**39. Scenario:** A job uses `SortShuffleManager` with 10,000 map partitions and 50 reduce partitions.
**Twist:** You reduce the map partitions to 150. What optimization kicks in?
- A) AQE skew join.
- B) Bypass-merge mode activates (if under `spark.shuffle.sort.bypassMergeThreshold`, default 200) avoiding the map-side sort overhead.
- C) Catalyst replaces the shuffle with a broadcast.
- D) The shuffle switches to hash-based.
- **Correct Answer:** B
- **Mastery Explanation:** Because there are fewer than 200 partitions and no aggregation merge required, Spark bypasses sorting the map output entirely, directly writing the files.

**40. Scenario:** You have a job with `groupBy("id").agg(F.count("*"))`.
**Twist:** You change the data source from CSV to Parquet. What specific optimization becomes available?
- A) Predicate Pushdown.
- B) Dictionary-encoded map-side aggregation.
- C) Column pruning at the disk read level, scanning only the "id" column.
- D) Secondary sorting.
- **Correct Answer:** C
- **Mastery Explanation:** Parquet is a columnar format. Because you only request "id", Spark completely ignores all other columns on disk, dropping read I/O by massive orders of magnitude compared to scanning full rows in CSV.

---

## Section 4: Coding & Debugging (10 Questions)

**41. Debugging:** 
```python
result = df.groupBy("device_id").agg(F.collect_list("temperature").alias("temps"))
```
The job fails with OutOfMemoryError. Identify the flaw and the fix.
- **Answer & Explanation:** The flaw is `collect_list`, which materializes millions of temperature records into a single JVM heap array per device. The fix is the Secondary Sort pattern: `df.repartition("device_id").sortWithinPartitions("device_id", "timestamp")` followed by a sequential iteration (e.g., Pandas UDF or Window).

**42. Coding:** Write the PySpark code to properly execute the Secondary Sort Pattern on a dataframe `df` grouping by "customer_id" and ordering by "transaction_date".
- **Answer & Explanation:** 
```python
df.repartition(200, "customer_id").sortWithinPartitions("customer_id", "transaction_date")
```
This guarantees that all rows for a customer are on the same executor (via repartition) and pre-ordered locally, averting a global shuffle.

**43. Debugging:** 
```python
df.orderBy("timestamp").write.parquet("/output/")
```
A developer complains that the job is extremely slow. They just need the files to be efficiently queried by timestamp using Parquet min/max statistics. What is the specific error?
- **Answer & Explanation:** They used `orderBy`, triggering a massive full-cluster range shuffle. For localized Parquet statistics, they should use `df.sortWithinPartitions("timestamp")`, which sorts locally (O(1) network cost) and provides the exact same file-level metadata benefits for downstream reads.

**44. Debugging:** Look at this physical plan segment:
```
SortAggregate(key=[id#1], functions=[collect_set(val#2)])
+- Sort [id#1 ASC NULLS FIRST]
   +- Exchange hashpartitioning(id#1)
```
Why is Spark using `SortAggregate` instead of `HashAggregate`?
- **Answer & Explanation:** Spark falls back to `SortAggregateExec` because `collect_set` is a complex, non-hash-combinable UDAF. It cannot safely maintain the structure of a dynamic set within Tungsten's off-heap `BytesToBytesMap`.

**45. Coding:** Implement a robust manual salting technique for a `groupBy("product_id")` aggregation calculating the sum of "sales", assuming `SALT_FACTOR = 50`. Write only the first-pass partial aggregation step.
- **Answer & Explanation:** 
```python
df.withColumn("salted_id", F.concat(F.col("product_id"), F.lit("_"), (F.rand() * 50).cast("int"))) \
  .groupBy("salted_id").agg(F.sum("sales").alias("partial_sales"), F.count("*").alias("partial_count"))
```
This distributes the hot `product_id` key across 50 random buckets, turning one massive reduce task into 50 manageable ones.

**46. Debugging:** A skewed task runs for 45 minutes on a `groupBy`. The developer enabled `spark.sql.adaptive.skewJoin.enabled = true`, but the task still takes 45 minutes. Why did the fix fail?
- **Answer & Explanation:** AQE dynamically mitigates skew *only* during joins (Sort-Merge, Shuffle-Hash) by breaking up large partitions. It is completely powerless to fix skew in standard GroupBy aggregations. Manual salting is required.

**47. Coding:** You have `df.repartition(200, "user_id").sortWithinPartitions("user_id", "timestamp")`. Construct a Window specification that perfectly matches this physical layout to achieve a no-op sort during execution.
- **Answer & Explanation:** 
```python
Window.partitionBy("user_id").orderBy("timestamp")
```
When this window is applied to the dataframe, Catalyst recognizes the physical layout exactly fulfills the logical requirements and strips the Sort operator from the plan.

**48. Debugging:**
```python
df = spark.read.parquet("/data/")
df.repartition(200, "session_id").orderBy("session_id", "timestamp")
```
Identify the performance killer in this snippet.
- **Answer & Explanation:** The code executes a hash shuffle via `repartition`, immediately followed by a range-partition shuffle via `orderBy`. The second shuffle destroys the partitioning of the first and causes immense network overhead. It must be `sortWithinPartitions`.

**49. Coding:** How do you verify in PySpark that a `groupBy` query successfully compiled down to Whole-Stage Code Generation (WSCG)?
- **Answer & Explanation:** Call `df.explain(mode="codegen")` or `df.explain()`. If WSCG is enabled, the physical plan operators will be prefixed with an asterisk `*` (e.g., `*(2) HashAggregate`).

**50. Debugging:** After applying manual salting to calculate a `count` aggregation, the developer tries to merge the partials in the second pass using:
```python
.groupBy("original_id").agg(F.count("partial_count"))
```
Why is the final result wildly inaccurate?
- **Answer & Explanation:** In the second phase of salting, you are aggregating previously aggregated blocks. Using `count()` merely counts the number of salt bins. You must use `F.sum("partial_count")` to tally the actual row counts from phase one.
