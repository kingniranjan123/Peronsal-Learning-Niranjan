# 🔥 Master Class Assessment: Window Operations

## Section 1: True/False Questions (1-10)

1. **True/False:** Window operations evaluate exceptionally early in the logical query plan, before standard aggregations and filtering, to ensure the window has access to the full dataset.
   - **Answer:** False
   - **Mastery Explanation:** Window operations are evaluated exceptionally *late* in the logical plan, strictly after standard aggregations, filtering, and `HAVING` clauses. They inject aggregated context into rows that have already survived the primary filters.

2. **True/False:** The physical operator `WindowExec` mandates two prerequisite physical operations: a network shuffle (partitioning) and a local sort on executors.
   - **Answer:** True
   - **Mastery Explanation:** `WindowExec` must iterate sequentially through the data. Therefore, data must first be collocated via `ShuffleExchangeExec` (partitionBy) and then ordered in memory or disk via `SortExec` (orderBy).

3. **True/False:** If an engineer defines an `orderBy` clause within a `WindowSpec` but omits the frame boundaries, Catalyst defaults to `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
   - **Answer:** False
   - **Mastery Explanation:** Catalyst defaults to a logical frame: `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. This is a critical trap, as `RANGE` groups identical values together, potentially leading to massive memory buffers and OOMs.

4. **True/False:** For physical frames defined by `rowsBetween`, Tungsten tracks physical pointer offsets in off-heap memory, making the memory footprint highly efficient (O(1) buffer size for fixed bounds).
   - **Answer:** True
   - **Mastery Explanation:** `rowsBetween` ignores logical values and simply moves an iterator offset, ensuring constant memory overhead regardless of data skew or identical peer values.

5. **True/False:** The `rangeBetween` frame dynamically expands or contracts its off-heap memory buffer to accommodate rows with identical peer values based on the `orderBy` column.
   - **Answer:** True
   - **Mastery Explanation:** Because `rangeBetween` evaluates the absolute value of the column rather than row offsets, any rows sharing the exact same value enter the evaluation frame simultaneously, forcing dynamic buffer expansion.

6. **True/False:** Omitting the `partitionBy` clause in a `WindowSpec` causes Catalyst to generate a `ShuffleExchangeExec(SinglePartition)`, pulling all data to a single executor.
   - **Answer:** True
   - **Mastery Explanation:** This is a fatal anti-pattern. Without a partition key, Spark has no way to distribute the window, reducing cluster compute parallelism to 1 and typically causing catastrophic disk spills or OOMs.

7. **True/False:** Utilizing `monotonically_increasing_id()` for generating unique IDs involves a global `SortExec` across the entire cluster network.
   - **Answer:** False
   - **Mastery Explanation:** `monotonically_increasing_id()` generates 64-bit integers by combining the RDD partition index and the local record offset at the `FileScan` phase. It involves zero shuffle overhead.

8. **True/False:** Catalyst's Whole-Stage Code Generation collapses the `ShuffleExchangeExec`, `SortExec`, and `WindowExec` into a single highly optimized Java function.
   - **Answer:** False
   - **Mastery Explanation:** A shuffle physically moves data across nodes; it acts as a boundary that breaks Whole-Stage Code Generation. However, the iterative frame evaluation within `WindowExec` itself heavily leverages code generation.

9. **True/False:** Multiple window functions (e.g., `lag` and `sum`) defined over the exact same `WindowSpec` in a single DataFrame operation will trigger redundant network shuffles.
   - **Answer:** False
   - **Mastery Explanation:** Catalyst is intelligent enough to execute multiple window functions utilizing the exact same underlying `ShuffleExchangeExec` and `SortExec` if the `WindowSpec` definitions are identical, keeping the plan lightweight.

10. **True/False:** To use `rangeBetween` with temporal logical bounds (e.g., 7 days prior), the ordering column must be cast to a numeric type, such as a UNIX timestamp integer.
   - **Answer:** True
   - **Mastery Explanation:** `rangeBetween` evaluates mathematical offsets (e.g., -604800 seconds). A standard Date or Timestamp type cannot be mathematically offset in the window frame API; it must be converted to a numeric type like a long/integer.

## Section 2: Multiple Choice Questions (11-25)

11. Which Spark SQL physical operator is directly responsible for routing rows with identical `partitionBy` keys over the network to the same executor?
    - A) `SortExec`
    - B) `ShuffleExchangeExec`
    - C) `HashAggregateExec`
    - D) `WindowExec`
    - **Answer:** B
    - **Mastery Explanation:** `ShuffleExchangeExec` performs the hash-based distributed shuffle. `WindowExec` computes the logic, and `SortExec` orders it locally, but the network routing is strictly the domain of `ShuffleExchangeExec`.

12. What is the fundamental difference between `rowsBetween` and `rangeBetween` during physical execution?
    - A) `rowsBetween` operates on network partitions, `rangeBetween` operates on local data.
    - B) `rowsBetween` leverages physical iterator offsets; `rangeBetween` performs logical value comparisons.
    - C) `rangeBetween` does not require a `SortExec` prior to execution.
    - D) `rowsBetween` is only valid for unbounded frames.
    - **Answer:** B
    - **Mastery Explanation:** `rowsBetween` is highly efficient because Tungsten just moves pointers. `rangeBetween` must continuously evaluate the actual values of the ordering column, expanding buffers for peer rows.

13. You observe a single Task in the Spark UI grinding away for hours while 199 other executors sit idle. Which windowing anti-pattern is the likely culprit?
    - A) Using `rangeBetween` instead of `rowsBetween`.
    - B) Forgetting to cast a date column to a unix timestamp.
    - C) Defining an `orderBy` clause but omitting a `partitionBy` clause.
    - D) Defining multiple window functions with different specifications.
    - **Answer:** C
    - **Mastery Explanation:** Omitting `partitionBy` triggers `ShuffleExchangeExec(SinglePartition)`, sending terabytes of data to a single core, entirely neutralizing the distributed architecture.

14. How does a senior engineer eliminate non-deterministic peer-ties when using an implicit `RANGE` frame?
    - A) By removing the `orderBy` clause entirely.
    - B) By appending a secondary tie-breaker column (like a unique ID) to the `orderBy` clause.
    - C) By increasing the executor memory overhead.
    - D) By switching to a hash-based aggregation.
    - **Answer:** B
    - **Mastery Explanation:** If the primary ordering column has duplicates, adding a highly unique secondary column ensures strict determinism and prevents the `RANGE` frame from expanding to swallow thousands of tied rows simultaneously.

15. What memory format does Catalyst heavily leverage during the `WindowExec` iteration phase to prevent GC pauses?
    - A) Java Heap Objects
    - B) Tungsten's binary `UnsafeRow` off-heap memory
    - C) Serialized Parquet buffers
    - D) Kryo Serialized objects
    - **Answer:** B
    - **Mastery Explanation:** Instead of allocating thousands of heavy Java objects for the sliding frame, Tungsten operates directly on binary `UnsafeRow` formats in off-heap memory, sidestepping Garbage Collection almost entirely.

16. What is the Big-O computational complexity of the `orderBy` phase locally within a partition?
    - A) O(N)
    - B) O(1)
    - C) O(N log N)
    - D) O(N^2)
    - **Answer:** C
    - **Mastery Explanation:** Local sorting (via `SortExec`) is an O(N log N) operation. If Tungsten memory is exhausted during this sort, it will spill to disk, heavily impacting performance.

17. Which of the following tasks is functionally IMPOSSIBLE without using Window functions or self-joins?
    - A) Calculating the total revenue per department.
    - B) Filtering out users who have made fewer than 5 purchases.
    - C) Computing a 30-day trailing moving average of stock prices while keeping daily granularity.
    - D) Counting the distinct number of visitors to a website.
    - **Answer:** C
    - **Mastery Explanation:** Moving averages require correlated context (the previous 29 days) injected into *each* individual day's row. Standard aggregations collapse the rows, making moving averages impossible without Windowing or self-joins.

18. If you define a `WindowSpec` with `partitionBy("category")` but NO `orderBy` clause, what is the default frame?
    - A) `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
    - B) `ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING`
    - C) `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
    - D) It throws an AnalysisException.
    - **Answer:** C
    - **Mastery Explanation:** When `orderBy` is omitted, Spark cannot define a "current row" in a sequential sense. The only valid logical frame is the entire partition: unbounded preceding to unbounded following.

19. Why might `rangeBetween` be required over `rowsBetween` for time-series data?
    - A) `rowsBetween` cannot process numeric types.
    - B) The dataset contains physical row gaps (sparse data), meaning 7 rows backwards might not equal 7 days backwards.
    - C) `rangeBetween` avoids the network shuffle entirely.
    - D) `rangeBetween` is computationally cheaper.
    - **Answer:** B
    - **Mastery Explanation:** `rowsBetween` counts physical rows. If days are missing in the data, 7 rows might span a month. `rangeBetween` evaluates the logical time value, dynamically sizing the frame regardless of row count.

20. What is the impact of utilizing `monotonically_increasing_id()` instead of `row_number().over(Window.orderBy("date"))`?
    - A) It guarantees a strict, gapless sequential ranking.
    - B) It generates globally unique IDs with zero network shuffle overhead.
    - C) It requires a SinglePartition shuffle.
    - D) It causes massive disk spills.
    - **Answer:** B
    - **Mastery Explanation:** `monotonically_increasing_id` creates unique 64-bit integers locally on executors, avoiding the catastrophic single-partition bottleneck of a global `row_number()`.

21. What happens if Tungsten's off-heap memory buffer is exhausted during a massive `rangeBetween` frame evaluation due to data skew?
    - A) Catalyst switches to `rowsBetween` automatically.
    - B) The Executor JVM crashes with a hard OutOfMemoryError.
    - C) The data is broadcasted to all nodes.
    - D) Spark silently drops the overflowing rows.
    - **Answer:** B
    - **Mastery Explanation:** Unlike `SortExec` which can gracefully spill to disk, if the dynamic buffer required to hold identical peer rows in a `rangeBetween` frame exceeds available memory, it results in an OOM crash.

22. In the distributed sessionization example, what is the primary purpose of the `lag` function?
    - A) To shuffle the data backward across partitions.
    - B) To compute the total sum of events.
    - C) To peek at the timestamp of the immediately preceding event within the physically sorted partition.
    - D) To filter out null events.
    - **Answer:** C
    - **Mastery Explanation:** `lag` leverages the strict ordering enforced by `SortExec` to look back one row in the physical offset, allowing the calculation of time deltas without joining the dataset to itself.

23. Which technique is used to safely compute pseudo-global rankings without triggering a SinglePartition bottleneck?
    - A) Broadcasting the dataset.
    - B) Distributed Salting (two-stage aggregation pipelines).
    - C) Using `rangeBetween` instead of `rowsBetween`.
    - D) Disabling Tungsten.
    - **Answer:** B
    - **Mastery Explanation:** By assigning rows to random "salt" buckets, engineers force a distributed shuffle. Local ranks can then be computed in parallel before resolving the final global rank, avoiding single-node failure.

24. What is the time complexity of evaluating a `rowsBetween` window frame?
    - A) O(1) per row, O(N) overall.
    - B) O(N) per row, O(N^2) overall.
    - C) O(log N) per row.
    - D) O(N * peers).
    - **Answer:** A
    - **Mastery Explanation:** Because `rowsBetween` operates on fixed physical offsets, the buffer size is constant O(1), leading to a highly efficient O(N) linear scan over the partition.

25. Why is Whole-Stage Code Generation critical for Window operations?
    - A) It partitions the data across the network faster.
    - B) It bypasses virtual method dispatch overhead and maximizes L1/L2 CPU cache locality during iteration.
    - C) It converts Spark code into Python.
    - D) It prevents Disk Spills during `SortExec`.
    - **Answer:** B
    - **Mastery Explanation:** Whole-Stage Code Gen compiles the complex physical operator logic into a single Java function. This keeps the CPU executing tightly within its L1/L2 caches without jumping between virtual object methods.

## Section 3: "Small Twist" Scenario Questions (26-40)

26. **Scenario:** Developer A uses `rowsBetween(-5, 0)` on an `orderBy` column with 1 million identical values. Developer B uses `rangeBetween(-5, 0)` on the exact same column. What happens?
    - **Answer:** Developer A's job completes instantly. Developer B's job crashes with an OOM.
    - **Mastery Explanation:** `rowsBetween` ignores identical values and strictly processes 6 rows physically. `rangeBetween` evaluates logical values, sees 1 million identical peers, and attempts to buffer all 1 million rows into off-heap memory simultaneously.

27. **Scenario:** You define a temporal window: `rangeBetween(-86400, 0)` where the `orderBy` column is a `DateType` (yyyy-MM-dd). Will this execute successfully?
    - **Answer:** No, it will throw an AnalysisException.
    - **Mastery Explanation:** The `orderBy` column must be explicitly cast to a numeric type (like UNIX timestamp long) for `rangeBetween` to mathematically evaluate the sliding boundary logic.

28. **Scenario:** A `WindowSpec` is defined as `partitionBy("user_id").orderBy("timestamp")`. User A has 1 event. User B, a system bot, has 10 billion events. Both use `rowsBetween`. What is the cluster outcome?
    - **Answer:** The executor processing User B will suffer severe data skew, massive disk spills during `SortExec`, and likely an OOM.
    - **Mastery Explanation:** `partitionBy` guarantees all data for a key routes to one executor. Even though `rowsBetween` memory footprint is small, the `SortExec` must still physically sort 10 billion rows locally, creating a massive skew bottleneck.

29. **Scenario:** You apply two window functions: `sum("amt").over(Window.partitionBy("id").orderBy("time"))` and `avg("amt").over(Window.partitionBy("id").orderBy("time", "txn_id"))`. How many local sorts occur?
    - **Answer:** Two.
    - **Mastery Explanation:** Because the `orderBy` specifications differ (one has `txn_id`, the other doesn't), Catalyst cannot collapse the physical plan. It must execute two separate `SortExec` operations.

30. **Scenario:** You remove `orderBy` from your window definition to compute a global sum per department: `sum("revenue").over(Window.partitionBy("dept"))`. What is the implicit frame?
    - **Answer:** `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.
    - **Mastery Explanation:** Without an ordering column, Spark evaluates the entire partition as a single block.

31. **Scenario:** A DataFrame has 500 distributed partitions. You execute `df.withColumn("rank", row_number().over(Window.orderBy("score")))`. How many partitions does the resulting DataFrame have?
    - **Answer:** One.
    - **Mastery Explanation:** Omitting `partitionBy` triggers a `ShuffleExchangeExec(SinglePartition)`. All 500 partitions are shuffled over the network into a single monolithic partition on one node.

32. **Scenario:** You are debugging a moving average calculation. You switch from `rangeBetween` to `rowsBetween(Window.unboundedPreceding, Window.currentRow)`. The OOM is fixed, but the moving average is suddenly higher than expected on weekends. Why?
    - **Answer:** Sparse data gap ignorance.
    - **Mastery Explanation:** Weekends lack trading rows. `rowsBetween` blindly reaches back physical rows (into Thursday/Wednesday), skewing the temporal average. `rangeBetween` would correctly evaluate the logical time gap, dropping older data.

33. **Scenario:** You attempt to use `lag(col, 1).over(Window.partitionBy("user").orderBy("time").rowsBetween(-5, 0))`. Will the frame boundary affect the lag result?
    - **Answer:** No.
    - **Mastery Explanation:** Offset functions like `lag` and `lead` operate purely on the `SortExec` sequence. They explicitly ignore any frame boundaries defined in the `WindowSpec`.

34. **Scenario:** To avoid a single partition bottleneck for a global rank, you salt the data using `partitionBy(rand() * 100)`. Does `row_number()` over this window give a true global rank?
    - **Answer:** No.
    - **Mastery Explanation:** It gives a pseudo-rank. The data is ranked locally within its random salt bucket. Generating a true global contiguous rank from this requires a complex second stage of aggregation and offset math.

35. **Scenario:** You filter a DataFrame: `df.withColumn("rank", rank().over(w)).filter($"rank" < 5)`. Does Catalyst push this filter down to the Parquet source?
    - **Answer:** No.
    - **Mastery Explanation:** Window operations execute exceptionally late in the logical plan. The rank must be fully computed in memory before the filter can be applied. Predicate pushdown does not apply to Window function outputs.

36. **Scenario:** You define a Window with `orderBy(desc("score"))`. Does this alter the data partitioning across the network?
    - **Answer:** No.
    - **Mastery Explanation:** `orderBy` only triggers `SortExec` locally on the executor. `ShuffleExchangeExec` (network routing) is controlled strictly by `partitionBy`.

37. **Scenario:** You execute `rangeBetween(0, 0)` on an `orderBy` column with 5 identical values for a given row. What is the output of `count().over()` for that row?
    - **Answer:** 5.
    - **Mastery Explanation:** `rangeBetween(0, 0)` evaluates the current logical value. Since 5 rows share that identical value, they are all included in the frame simultaneously as peers.

38. **Scenario:** You change `sum("x").over(w)` to `groupBy("id").agg(sum("x"))`. How does the output cardinality change?
    - **Answer:** `groupBy` collapses the rows into one row per ID. `over()` preserves the original dataset row count, duplicating the sum on every row.
    - **Mastery Explanation:** This is the foundational paradigm shift of Window operations—preserving cardinality while injecting aggregated context.

39. **Scenario:** Your `SortExec` phase runs out of Tungsten execution memory while processing a massive `partitionBy` shard. What is the immediate consequence?
    - **Answer:** Disk Spillage.
    - **Mastery Explanation:** Unlike `WindowExec` dynamic buffers which cause OOMs, the `SortExec` operator is designed to gracefully (but slowly) spill its state to local executor disk when memory limits are breached.

40. **Scenario:** A developer uses `rowsBetween` to avoid OOMs on an implicit `RANGE` frame, but leaves the non-unique `orderBy("date")` intact without a tie-breaker. Is the output deterministic?
    - **Answer:** No.
    - **Mastery Explanation:** While memory is safe, the local `SortExec` does not guarantee the order of tied rows. Subsequent runs may yield different running totals because the physical row order fluctuates.

## Section 4: Coding & Debugging Questions (41-50)

41. **Debug:** A Spark application hangs at 99% completion for 3 hours. The UI shows 1 active task.
    ```scala
    val w = Window.orderBy("transaction_amount")
    df.withColumn("rank", rank().over(w))
    ```
    **Identify the issue and fix it.**
    - **Answer & Fix:** Issue is an unpartitioned window causing a SinglePartition bottleneck.
    - **Fix:** If global ranking is required, use `monotonically_increasing_id()` for unique IDs, or apply a distributed salting strategy. If business logic allows, add a `partitionBy("region")` or similar category.

42. **Debug:** This code throws OutOfMemoryError on days with massive trading spikes.
    ```scala
    val w = Window.partitionBy("ticker").orderBy("trade_date")
    df.withColumn("running_total", sum("volume").over(w))
    ```
    **Identify the architectural flaw and fix it.**
    - **Answer & Fix:** Flaw: Implicit default frame is `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`. Thousands of trades on the exact same date enter the off-heap buffer simultaneously as peers.
    - **Fix:** Enforce physical offsets and add a tie-breaker:
      `Window.partitionBy("ticker").orderBy("trade_date", "trade_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)`

43. **Debug:** A user is trying to find the previous login time, but the results seem completely randomized on every run.
    ```scala
    val w = Window.partitionBy("user_id")
    df.withColumn("prev_login", lag("login_timestamp", 1).over(w))
    ```
    **Identify the issue and fix it.**
    - **Answer & Fix:** Issue: `lag` relies on sequential offsets, but there is no `orderBy` clause to instruct `SortExec` how to order the partition.
    - **Fix:** Add `.orderBy("login_timestamp")` to the `WindowSpec`.

44. **Debug:** The following time-series window fails to compile with an AnalysisException regarding data types.
    ```scala
    // trade_date is of type DateType
    val w = Window.partitionBy("ticker")
                  .orderBy("trade_date")
                  .rangeBetween(-7, 0)
    ```
    **Identify the issue and fix it.**
    - **Answer & Fix:** Issue: `rangeBetween` requires a numeric/temporal integer for logical offset computation. It cannot subtract the integer 7 from a DateType directly in the frame API.
    - **Fix:** Cast the date to a unix timestamp.
      `.orderBy(unix_timestamp(col("trade_date")).cast("long"))` and change bounds to `-604800, 0`.

45. **Debug:** This code generates two massive network shuffles instead of one, doubling execution time.
    ```scala
    val w1 = Window.partitionBy("user").orderBy("time")
    val w2 = Window.partitionBy("user").orderBy(desc("time"))
    df.withColumn("first", first("event").over(w1))
      .withColumn("last", first("event").over(w2))
    ```
    **Identify the issue and fix it.**
    - **Answer & Fix:** Issue: The opposing `orderBy` directions force Catalyst to create two entirely separate `SortExec` and `ShuffleExchangeExec` paths.
    - **Fix:** Use the same window `w1` for both, and utilize the `last()` function instead of flipping the sort order:
      `.withColumn("last", last("event").over(w1))` (Note: requires setting frame to unbounded following if checking the absolute last).

46. **Debug:** A developer attempts to retrieve the top 3 highest paid employees per department.
    ```scala
    val w = Window.partitionBy("dept").orderBy(desc("salary"))
    val result = df.withColumn("rank", rank().over(w)).filter(col("rank") == 3)
    ```
    **Identify the logic flaw.**
    - **Answer & Fix:** Flaw: Using `rank() == 3` will completely miss employees if there is a tie for 1st place (ranks would be 1, 1, 3, 4). Also, filtering directly in the same chain might not behave as intended without a CTE, but the logic `== 3` is the main flaw.
    - **Fix:** Change filter to `col("rank") <= 3` or use `dense_rank()` to avoid skipped integers.

47. **Debug:** A developer wants the absolute final event of a user session. It returns the current row's event instead of the session's final event.
    ```scala
    val w = Window.partitionBy("session_id").orderBy("time")
    df.withColumn("final_event", last("event").over(w))
    ```
    **Identify the hidden trap and fix it.**
    - **Answer & Fix:** Trap: The implicit frame is `UNBOUNDED PRECEDING TO CURRENT ROW`. Therefore, `last()` just looks at the end of the frame, which is always the current row!
    - **Fix:** Explicitly define the frame to scan the entire partition:
      `.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)`

48. **Debug:** An engineer writes this to compute a running sum, but it's returning the total sum for every row.
    ```scala
    val w = Window.partitionBy("user_id")
    df.withColumn("running_sum", sum("purchase").over(w))
    ```
    **Identify the issue and fix it.**
    - **Answer & Fix:** Issue: Missing `orderBy`. Without `orderBy`, the implicit frame evaluates the entire partition (`UNBOUNDED PRECEDING TO UNBOUNDED FOLLOWING`), resulting in a grand total per row, not a running total.
    - **Fix:** Add `.orderBy("timestamp")` to trigger the `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` frame, creating the sliding summation.

49. **Debug:** The Spark UI shows catastrophic data skew during `ShuffleExchangeExec`.
    ```scala
    val w = Window.partitionBy("country_code").orderBy("timestamp")
    df.withColumn("running_total", sum("sales").over(w))
    ```
    **Identify the cause and fix it.**
    - **Answer & Fix:** Cause: `country_code` is highly skewed (e.g., US has 100x more data than others). All US data routes to a single executor, causing localized OOM/spill.
    - **Fix:** Salting is required. Add a random salt, compute a local running sum, and then cascade the totals across the salted boundaries in a secondary aggregation step.

50. **Debug:** A junior developer proposes this code to calculate the difference between the current row and the row 5 steps back.
    ```scala
    val w = Window.partitionBy("id").orderBy("time").rowsBetween(-5, 0)
    df.withColumn("diff", col("value") - first("value").over(w))
    ```
    **Identify why this is brittle and propose the elite fix.**
    - **Answer & Fix:** Brittle: If the partition has fewer than 5 rows, `first()` will just grab the earliest available row (e.g., row 2 steps back), leading to silent logical errors in the math.
    - **Fix:** Use the explicit `lag` offset function which strictly enforces the physical offset and returns null if 5 rows don't exist:
      `col("value") - lag("value", 5).over(Window.partitionBy("id").orderBy("time"))`
