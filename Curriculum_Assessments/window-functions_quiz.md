# 🏆 Master Class Assessment: Window Operations

## Section 1: True/False Questions (10 Questions)

**1. `WindowExec` can operate on un-sorted partitions if `rowsBetween` is used instead of `rangeBetween`.**
**Answer:** False
**Mastery Explanation:** Regardless of whether `rowsBetween` or `rangeBetween` is used, Catalyst strictly enforces a `SortExec` prior to `WindowExec`. `WindowExec` relies on a sequential stream of data to maintain its sliding frame buffer and cannot process un-sorted partitions without materializing the whole partition, which causes OOM errors.

**2. Omitting `partitionBy` in a WindowSpec causes Catalyst to inject a `ShuffleExchangeExec(SinglePartition)` node.**
**Answer:** True
**Mastery Explanation:** Omitting `partitionBy` forces all data to be routed to a single partition on a single executor core for global sorting, nullifying distributed parallelism and often leading to catastrophic disk spills or OOMs.

**3. The default frame boundary when using `orderBy` without explicit boundaries is `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.**
**Answer:** False
**Mastery Explanation:** The implicit default is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. This logical range evaluates all rows with identical ordering values as peers simultaneously, which can cause severe memory spikes on low-cardinality columns.

**4. Catalyst can combine multiple window functions using the same `partitionBy` and `orderBy` specs to share the same `ShuffleExchangeExec` and `SortExec` phases.**
**Answer:** True
**Mastery Explanation:** Catalyst optimizes identical WindowSpecs by evaluating them together over the same physical data stream, bypassing redundant shuffling and sorting overhead.

**5. `monotonically_increasing_id()` guarantees globally contiguous, gap-free IDs across partitions without triggering a shuffle.**
**Answer:** False
**Mastery Explanation:** While it operates without a shuffle (generating 64-bit integers combining partition ID and local offsets), the resulting IDs are NOT gap-free. They are simply globally unique and monotonically increasing.

**6. `rangeBetween` tracks logical values and can dynamically expand the off-heap memory buffer to accommodate rows with identical peer values.**
**Answer:** True
**Mastery Explanation:** Because `rangeBetween` uses logical bounds, Tungsten must buffer all rows matching the logical criteria (including identical peer values) into off-heap memory at the same time.

**7. `rowsBetween` evaluates sliding window frames by buffering logical value boundaries rather than physical iterator offsets.**
**Answer:** False
**Mastery Explanation:** `rowsBetween` strictly uses physical iterator offsets to track frame boundaries in an O(1) memory footprint (for fixed bounds), which is significantly faster and less memory-intensive than `rangeBetween`.

**8. Window operations are evaluated in the logical query plan before `WHERE` and standard `GROUP BY` aggregations.**
**Answer:** False
**Mastery Explanation:** Window functions are evaluated exceptionally late in the logical plan, strictly *after* standard aggregations, filtering, and `HAVING` clauses.

**9. The Whole-Stage Code Generation phase collapses `ShuffleExchangeExec`, `SortExec`, and `WindowExec` into a single highly optimized Java function.**
**Answer:** False
**Mastery Explanation:** A shuffle boundary (`ShuffleExchangeExec`) breaks Whole-Stage Code Generation. The pipeline is split across stages; `SortExec` and `WindowExec` might be grouped together post-shuffle, but the shuffle itself is a stage boundary.

**10. Tungsten operates on binary `UnsafeRow` formats directly in off-heap memory during `WindowExec` frame buffering.**
**Answer:** True
**Mastery Explanation:** Tungsten prevents garbage collection overhead by managing raw bytes (`UnsafeRow`) in off-heap memory rather than allocating thousands of heavy Java objects inside the JVM.

---

## Section 2: Multiple Choice Questions (15 Questions)

**11. Which two physical operators MUST precede `WindowExec` in the physical plan?**
A) FilterExec and ProjectExec
B) BroadcastHashJoinExec and SortExec
C) ShuffleExchangeExec and SortExec
D) HashAggregateExec and SortAggregateExec
**Answer:** C
**Mastery Explanation:** Window functions require data to be collocated by partition key (`ShuffleExchangeExec`) and sequentially ordered within that partition (`SortExec`) so `WindowExec` can stream over the rows without materializing everything at once.

**12. When processing a `WindowSpec` with an `orderBy` clause on a skewed, low-cardinality column without an explicit frame, what is the most likely failure mode?**
A) OOM on the Driver JVM due to massive result collection
B) OOM on the Executor due to Tungsten dynamically expanding off-heap memory to buffer all peer rows
C) Disk spill during `ShuffleExchangeExec`
D) Syntax error during Catalyst logical planning
**Answer:** B
**Mastery Explanation:** The default `RANGE BETWEEN` logic treats identical ordering values as peers. For a skewed column, thousands of rows enter the logical frame simultaneously, blowing up Tungsten's off-heap memory buffer.

**13. A Spark job sits for hours with a single active task in the Spark UI. Which code snippet is most likely responsible?**
A) `sum("amount").over(Window.partitionBy("store_id").orderBy("date"))`
B) `row_number().over(Window.orderBy("revenue").desc())`
C) `avg("price").over(Window.partitionBy("category").rowsBetween(-1, 1))`
D) `monotonically_increasing_id()`
**Answer:** B
**Mastery Explanation:** Omitting `partitionBy` creates a `ShuffleExchangeExec(SinglePartition)` physical plan, routing 100% of the dataset to one executor core and killing distributed parallelism.

**14. What is the fundamental difference between `rowsBetween` and `rangeBetween`?**
A) `rowsBetween` triggers a shuffle, while `rangeBetween` operates locally.
B) `rangeBetween` is evaluated before standard `GROUP BY` aggregations.
C) `rowsBetween` uses physical iterator offsets (O(1) memory), while `rangeBetween` evaluates logical values and buffers all peers.
D) `rowsBetween` only works on numeric data types.
**Answer:** C
**Mastery Explanation:** `rowsBetween` ignores row content and just steps an iterator. `rangeBetween` dynamically sizes a memory buffer by comparing logical column values (e.g., timestamps or scores).

**15. How does Catalyst optimize multiple window functions in the same `.select()` or `.withColumn()` chain?**
A) It executes each window function in a separate spark job.
B) If they have identical `WindowSpec` definitions, it reuses the same `ShuffleExchangeExec` and `SortExec` operators.
C) It caches the dataframe automatically between window function calls.
D) It converts them all into a single UDF.
**Answer:** B
**Mastery Explanation:** Identical window specs yield identical physical data layout requirements. Catalyst optimizes the DAG to shuffle and sort once, then applies multiple `WindowExec` evaluations sequentially.

**16. Why is `monotonically_increasing_id()` preferred over `row_number().over(Window.orderBy("id"))` for creating global IDs?**
A) It returns sequential gap-free IDs.
B) It avoids the single-partition shuffle bottleneck by using RDD partition indices and local offsets.
C) It is evaluated on the driver instead of executors.
D) It supports string IDs natively.
**Answer:** B
**Mastery Explanation:** `row_number` with no partition forces a global shuffle. `monotonically_increasing_id` generates a 64-bit integer combining the partition index (upper bits) and local offset (lower bits), requiring zero shuffling.

**17. What is the primary purpose of adding a secondary tie-breaker column to an `orderBy` clause in a WindowSpec?**
A) To speed up the `ShuffleExchangeExec`.
B) To force a `RowFrame` instead of a `RangeFrame`.
C) To eliminate peer-ties, ensuring deterministic output and preventing off-heap memory spikes under `RANGE BETWEEN`.
D) To reduce the number of partitions.
**Answer:** C
**Mastery Explanation:** A secondary column (like a unique ID) ensures no two rows have identical ordering keys. This restricts the default `RANGE` frame from swallowing multiple peer rows into memory at once.

**18. When computing a 7-day rolling average over sparse time-series data (some days missing), which frame definition is mathematically correct?**
A) `.rowsBetween(-7, 0)`
B) `.rangeBetween(-7, 0)` on a DateType column
C) `.rangeBetween(-604800, 0)` on a UNIX timestamp column (LongType)
D) `.rowsBetween(Window.unboundedPreceding, Window.currentRow)`
**Answer:** C
**Mastery Explanation:** Physical rows (`rowsBetween`) fail because 7 rows might span a month if data is missing. Logical frames (`rangeBetween`) require numeric types, so casting the timestamp to seconds and looking back 604,800 seconds (7 days) perfectly captures the logical temporal window.

**19. Which Catalyst phase converts the window frame evaluation into highly optimized Java byte-code?**
A) Logical Optimization
B) Physical Planning
C) Whole-Stage Code Generation
D) Cost-Based Optimization (CBO)
**Answer:** C
**Mastery Explanation:** While shuffle breaks Whole-Stage Code Gen boundaries, the actual `WindowExec` iteration and computation logic inside the stage is compiled into raw Java byte-code via Whole-Stage Code Gen to maximize CPU cache locality.

**20. What is a "salting" technique used for in the context of window functions?**
A) Encrypting sensitive data before window operations.
B) Distributing the sorting workload for pseudo-global ranking by appending a random integer and partitioning by it.
C) Automatically converting `rangeBetween` to `rowsBetween`.
D) Removing null values before the `SortExec` phase.
**Answer:** B
**Mastery Explanation:** Salting assigns a random bucket ID to rows, allowing `partitionBy("salt")` to execute local sorts in parallel. This circumvents the Single Partition bottleneck for heavy, near-global ranking tasks.

**21. Where is the state buffer for active rows in a window frame stored during physical execution?**
A) On the Driver's heap memory.
B) In Executor Java Heap objects (JVM).
C) In Tungsten off-heap memory as binary `UnsafeRow` formats.
D) Strictly on disk in the Spark local directories.
**Answer:** C
**Mastery Explanation:** Tungsten manages memory natively off-heap, avoiding the massive GC pauses that would occur if Spark instantiated millions of Java objects for sliding window calculations.

**22. If you define a `WindowSpec` with `partitionBy("user_id")` but NO `orderBy` clause, what frame does Catalyst implicitly inject?**
A) `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
B) `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
C) `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
D) It throws an AnalysisException.
**Answer:** B
**Mastery Explanation:** Without an `orderBy` clause, there is no sequential context. Spark defaults to evaluating the entire partition as a single frame (`UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`).

**23. Why is `lag()` an excellent tool for sessionization of clickstream data?**
A) It bypasses the `SortExec` phase.
B) It allows peeking at the preceding timestamp in an ordered partition to evaluate temporal deltas, avoiding O(N^2) self-joins.
C) It computes global unique IDs natively.
D) It pushes the computation down to the storage layer (e.g., Parquet).
**Answer:** B
**Mastery Explanation:** By comparing the current row's timestamp with the `lag(1)` timestamp, you can flag new sessions if the delta exceeds a threshold, transforming a complex relational self-join problem into a linear streaming pass.

**24. In the context of window operations, what does O(1) buffer size mean?**
A) The operation only takes 1 second.
B) `rowsBetween` with fixed boundaries (e.g., -3 to 0) stores exactly 4 pointers in memory, regardless of data skew.
C) The data is reduced to a single row per partition.
D) `WindowExec` requires 1 executor core.
**Answer:** B
**Mastery Explanation:** Physical offset boundaries require a constant amount of memory tracking (just iterators/pointers) regardless of how many rows share the same logical value.

**25. Which configuration or UI tab is most useful for identifying the Single Partition Bottleneck?**
A) Storage Tab -> RDD Blocks
B) Environment Tab -> JVM Properties
C) SQL Tab -> Looking for `ShuffleExchangeExec(SinglePartition)` in the physical DAG.
D) Streaming Tab -> Batch Duration
**Answer:** C
**Mastery Explanation:** The SQL tab explicitly visualizes the physical query plan (DAG). A `ShuffleExchangeExec(SinglePartition)` is the smoking gun for unpartitioned window functions.

---

## Section 3: Small Twist Questions (15 Questions)

**26. Twist:** You have `Window.partitionBy("id").orderBy("date")`. You change it to `Window.partitionBy("id").orderBy("date").rowsBetween(Window.unboundedPreceding, Window.currentRow)`.
**Result:** What happens to memory consumption?
**Answer:** It becomes strictly deterministic and highly memory efficient.
**Mastery Explanation:** The implicit `RANGE` frame is overwritten by a physical `ROWS` frame, meaning Tungsten uses simple pointer offsets rather than buffering massive blocks of rows with identical dates.

**27. Twist:** A developer removes `partitionBy("category")` from a window function computing ranks.
**Result:** How does the physical execution plan change?
**Answer:** Catalyst introduces a `ShuffleExchangeExec(SinglePartition)`.
**Mastery Explanation:** The lack of a partition key means all rows are shuffled across the network to a single executor to perform a global sort and rank, causing severe bottlenecks.

**28. Twist:** You calculate a 30-day moving average using `.rowsBetween(-30, 0)` on a stock ticker dataset. You switch to `.rangeBetween(-2592000, 0)` using a UNIX timestamp (2592000 seconds = 30 days).
**Result:** What changes in the calculation?
**Answer:** The average becomes mathematically accurate for sparse data.
**Mastery Explanation:** Physical rows assume 1 row = 1 day. If weekends are missing, 30 rows span 42 days. `rangeBetween` on timestamps evaluates the actual logical time gap, correctly handling missing data points.

**29. Twist:** You add `transaction_id` (a unique GUID) to `Window.partitionBy("user").orderBy("date")` making it `orderBy("date", "transaction_id")`.
**Result:** What Catalyst default pitfall does this fix?
**Answer:** The implicit `RANGE BETWEEN` memory spike.
**Mastery Explanation:** By introducing a unique secondary key, no two rows evaluate as peers. The `RANGE` frame buffer will now only ever hold 1 row at a time, eliminating OOM risks on skewed dates.

**30. Twist:** A team runs `sum("clicks").over(Window.partitionBy("ip"))`. They add an `orderBy("timestamp")`.
**Result:** How does the output of `sum` change?
**Answer:** It changes from a total partition sum to a running cumulative sum.
**Mastery Explanation:** Without `orderBy`, the frame is `UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING` (total sum). Adding `orderBy` injects the implicit `UNBOUNDED PRECEDING TO CURRENT ROW` (running sum).

**31. Twist:** A dataframe has two window functions: `lag("event").over(Window.partitionBy("id").orderBy("time"))` and `lead("event").over(Window.partitionBy("id").orderBy("time").desc())`.
**Result:** How many SortExecs occur?
**Answer:** Two.
**Mastery Explanation:** Because the ordering directions (`ASC` vs `DESC`) differ, the physical layout requirements differ. Catalyst must execute two separate `SortExec` operations, doubling the sorting overhead.

**32. Twist:** You replace an unpartitioned `row_number().over(Window.orderBy("created_at"))` with `monotonically_increasing_id()`.
**Result:** What happens to the sequential nature of the IDs?
**Answer:** They remain increasing but are no longer gap-free or strictly ordered globally by `created_at`.
**Mastery Explanation:** `monotonically_increasing_id` assigns IDs based on internal RDD partition boundaries and physical layout, completely ignoring business logic ordering, resulting in gaps between partitions.

**33. Twist:** You are using `rowsBetween(-1, 1)`. You change the ordering column from an Integer to a String.
**Result:** Does `rowsBetween` still work?
**Answer:** Yes.
**Mastery Explanation:** `rowsBetween` only cares about physical iterators. As long as the `SortExec` can sort the String, `rowsBetween` functions identically regardless of data type.

**34. Twist:** You are using `rangeBetween(-1, 1)`. You change the ordering column from an Integer to a String.
**Result:** What happens during query execution?
**Answer:** Catalyst throws an AnalysisException.
**Mastery Explanation:** `rangeBetween` requires evaluating logical boundaries using mathematical addition/subtraction (e.g., `current_value - 1`). This is impossible on non-numeric/non-temporal types like Strings.

**35. Twist:** In a sessionization pipeline, you change the `lag(1)` logic to `lag(10)`.
**Result:** What happens to the memory footprint of `WindowExec`?
**Answer:** It increases minimally (O(1) increase to track 10 pointers).
**Mastery Explanation:** `lag` is a physical offset function. Tracking 10 rows back simply requires maintaining an iterator that lags by 10 physical slots in the off-heap buffer, adding negligible overhead.

**36. Twist:** You execute a job with `spark.sql.windowExec.buffer.in.memory.threshold` set exceptionally low.
**Result:** What happens during `rangeBetween` evaluation of heavy peers?
**Answer:** The `WindowExec` will spill the off-heap frame buffer to local executor disk.
**Mastery Explanation:** If Tungsten detects that the buffer required for a logical frame exceeds memory thresholds, it gracefully spills the `UnsafeRow` blocks to disk, preventing JVM OOMs at the cost of massive I/O latency.

**37. Twist:** You have a `WindowSpec` partitioned by `country`. You change it to partition by `country, city`.
**Result:** What is the impact on the `ShuffleExchangeExec`?
**Answer:** The hash distribution logic changes, resulting in smaller, more numerous partitions across the cluster.
**Mastery Explanation:** The shuffle uses a hash of the partition keys. Adding `city` increases cardinality, distributing the data more evenly and mitigating potential data skew from massive `country` partitions.

**38. Twist:** A developer uses `.withColumn("rank", rank().over(Window.partitionBy("A").orderBy("B")))` and then drops the `rank` column immediately.
**Result:** What happens during the physical plan?
**Answer:** Catalyst optimizes the Window operation away entirely.
**Mastery Explanation:** Catalyst's logical optimizer tracks column lineage. If the output of a window function is never referenced or materialized in an action, it prunes the operation from the physical DAG entirely.

**39. Twist:** You swap `Window.partitionBy("A")` to use `.repartition($"A")` prior to the window function.
**Result:** Does Catalyst perform a second shuffle?
**Answer:** No.
**Mastery Explanation:** If Catalyst detects that the physical RDD layout already satisfies the `partitionBy` requirements of the `WindowSpec` via an explicit `.repartition()`, it intelligently skips the `ShuffleExchangeExec`.

**40. Twist:** You apply a `.filter("amount > 100")` AFTER a window function calculating a running total. You swap it to run BEFORE the window function.
**Result:** How does the calculation change?
**Answer:** The running total drops all values <= 100 from its cumulative calculation.
**Mastery Explanation:** Window functions only evaluate the rows passed to them. Filtering before alters the input frame; filtering after retains the cumulative math of the discarded rows in the retained rows.

---

## Section 4: Coding & Debugging Questions (10 Questions)

**41. The Code:** `df.withColumn("max_val", max("price").over(Window.partitionBy("dept").orderBy("date")))`
**The Bug:** The output returns a running maximum, not the absolute maximum for the department.
**The Fix:** Remove `orderBy("date")` or explicitly add `.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)`.
**Mastery Explanation:** `orderBy` implicitly triggers `UNBOUNDED PRECEDING TO CURRENT ROW`. Removing it defaults the frame to the entire partition.

**42. The OOM Trace:** `java.lang.OutOfMemoryError: Java heap space` observed on the Driver node during a Spark SQL window query.
**The Root Cause:** The developer likely called `.collect()` or `.toPandas()` on a massive result set containing window aggregates. Window operations execute on workers; Driver OOMs imply massive payload retrieval, not execution failures.

**43. The Execution Plan:**
```
WindowExec
 +- SortExec (orderBy amount)
    +- ShuffleExchangeExec(SinglePartition)
```
**The Code Defect:** The developer wrote `Window.orderBy("amount")` without a `partitionBy` clause, forcing 100% of data to one executor.

**44. The Skew Problem:** `partitionBy("tenant_id")` causes one executor to hang for hours because "Tenant A" has 500 million rows.
**The Fix:** Implement salted windowing. Append a random number 1-100 to "Tenant A" rows, `partitionBy("tenant_id", "salt")`, compute local ranks/sums, and aggregate in a second stage.

**45. The Code:** 
```scala
val w1 = Window.partitionBy("A").orderBy("B")
val w2 = Window.partitionBy("A").orderBy(col("B").desc)
df.withColumn("c1", lag("val").over(w1)).withColumn("c2", lead("val").over(w2))
```
**The Optimizer Blocker:** `w1` and `w2` have opposing sort directions. Catalyst must perform two heavy `SortExec` operations.
**The Fix:** Use `lag` and `lead` over the same `w1` spec (e.g., use `lead` with different offsets over the same ascending window) to ensure physical plan reuse.

**46. The Logic Error:** Calculating a session ID via:
`when(delta > 30 mins, 1).otherwise(0)` followed by `sum("flag").over(Window.partitionBy("user").orderBy("time"))`.
**The Bug:** The very first event for a user gets `delta = null`, so flag=0, resulting in session_id = 0 for the first session.
**The Fix:** `when(col("prev_time").isNull, 1)` must be added to explicitly flag the first row of a partition as the start of a session.

**47. The Range Exception:** `Window.partitionBy("id").orderBy("string_col").rangeBetween(-1, 1)` throws an AnalysisException.
**The Root Cause:** `rangeBetween` performs mathematical bounds checking (`string_col value - 1`). Strings do not support arithmetic subtraction. Use `rowsBetween` instead.

**48. The Memory Spike Trace:** Tungsten off-heap memory spikes to 30GB on a `WindowExec` node, followed by a GC pause and crash.
**The Root Cause:** An implicit `RANGE BETWEEN` on a highly skewed column (e.g., `orderBy("status")` where 1 million rows have status='ACTIVE'). All 1 million rows enter the peer buffer simultaneously.
**The Fix:** Explicitly set `.rowsBetween(Window.unboundedPreceding, Window.currentRow)`.

**49. The Redundant Shuffle:** 
```scala
val df2 = df.repartition(col("user_id")).sortWithinPartitions("timestamp")
val w = Window.partitionBy("user_id").orderBy("timestamp")
df2.withColumn("running", sum("amt").over(w))
```
**The Debug Analysis:** Does this trigger a new shuffle?
**Answer:** No. Catalyst's physical planner recognizes that the RDD is already perfectly partitioned and sorted by the exact requirements of the `WindowSpec`, completely bypassing `ShuffleExchangeExec` and `SortExec`.

**50. The Missing Data Gap:** A business requirement asks for a rolling 24-hour total revenue per store. The developer writes `.rowsBetween(-24, 0)` assuming hourly data.
**The Bug:** If the store is closed for 12 hours (no rows), `-24` physical rows will look back 36 hours temporally.
**The Fix:** Cast the timestamp to seconds and use `.rangeBetween(-86400, 0)` to enforce strict temporal logic boundaries regardless of physical row presence.
