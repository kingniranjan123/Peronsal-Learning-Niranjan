# Master Class: DataFrames - Elite Assessment Quiz

Welcome to the Elite Spark Architect Assessment on DataFrames. This quiz evaluates your deep understanding of Catalyst, Tungsten, memory management, physical plans, and advanced transformations.

## Part 1: True/False Questions

**1. Spark's Tungsten engine completely eliminates JVM Garbage Collection for all DataFrame operations.**
* **Answer:** False
* **Mastery Explanation:** While Tungsten stores data off-heap using a highly compact binary format to bypass JVM object overhead for row data, the JVM is still heavily used for driver/executor coordination, task management, and non-Tungsten operations (like standard UDFs). GC pauses are reduced, not eliminated.

**2. Catalyst's predicate pushdown can optimize queries reading from Parquet files by evaluating filters before the data is transferred over the network.**
* **Answer:** True
* **Mastery Explanation:** Catalyst pushes the `filter` operations down to the storage layer. Parquet uses columnar storage with min/max statistics in its footers, allowing Spark to skip reading entire row groups that don't satisfy the predicate, heavily reducing network and disk I/O.

**3. A BroadcastHashJoin is always chosen over a SortMergeJoin if the smaller table fits in the driver's memory.**
* **Answer:** False
* **Mastery Explanation:** The smaller table must fit within the *executor's* memory (and driver memory for the initial broadcast), but Catalyst only chooses it if the table size is below the `spark.sql.autoBroadcastJoinThreshold` (default 10MB). 

**4. Execution Memory and Storage Memory in Spark share a unified region, and Execution Memory can evict cached blocks from Storage Memory if needed.**
* **Answer:** True
* **Mastery Explanation:** Since Spark 1.6, unified memory management allows Execution Memory (used for joins/shuffles) to dynamically evict blocks from Storage Memory (used for caching). However, Storage Memory cannot evict Execution Memory.

**5. Pandas UDFs achieve 100x speedups entirely because they execute in a separate C++ process instead of a Python worker.**
* **Answer:** False
* **Mastery Explanation:** Pandas UDFs still run in a Python worker process. The speedup comes from Apache Arrow (enabling zero-copy columnar memory transfers) and the ability to execute vectorized operations (using C-backed NumPy/Pandas) rather than processing row-by-row.

**6. Whole-Stage Code Generation collapses an entire query's physical plan into a single Java function, completely eliminating the need for Volcano Iterator model calls.**
* **Answer:** False
* **Mastery Explanation:** Whole-Stage Code Generation collapses *fragments* (stages) of a query plan into a single function. Operations that require shuffling (like Exchange) break the code generation pipeline into separate stages.

**7. Using High-Order Functions (HOFs) like `aggregate` on nested arrays avoids the shuffle overhead typically introduced by `explode` and `groupBy` operations.**
* **Answer:** True
* **Mastery Explanation:** HOFs process elements directly inside the nested array within the same row. `explode` flattens arrays into multiple rows, often requiring a subsequent `groupBy` (and shuffle) to re-aggregate, which ruins Tungsten's memory locality.

**8. In a salting strategy for skewed joins, both the large fact table and the smaller dimension table are appended with random salt values.**
* **Answer:** False
* **Mastery Explanation:** The large skewed table is appended with a *random* salt (e.g., 0 to N). The smaller dimension table must be *exploded* (replicated) to contain all possible salt values (0 to N) to ensure matches occur.

**9. `spark.sql.shuffle.partitions` defaults to 200, meaning any shuffle operation will strictly result in 200 output partitions, regardless of other configurations.**
* **Answer:** False
* **Mastery Explanation:** If Adaptive Query Execution (AQE) is enabled (default in Spark 3+), Spark dynamically coalesces shuffle partitions based on actual data size, potentially resulting in fewer than 200 partitions.

**10. Using standard Python UDFs on DataFrames forces Spark to serialize each row into Pickle format, bypassing Tungsten's binary format.**
* **Answer:** True
* **Mastery Explanation:** Standard Python UDFs force Spark to deserialize Tungsten's off-heap binary data into JVM objects, serialize them to Pickle via Py4J, send them to the Python worker, and reverse the process, bottlenecking performance.

---

## Part 2: Multiple Choice Questions

**11. Which Catalyst optimization phase converts an Unresolved Logical Plan into a Resolved Logical Plan?**
A) Rule-Based Optimization
B) Cost-Based Optimization
C) Analysis (Consulting the Catalog)
D) Physical Planning
* **Answer:** C
* **Mastery Explanation:** The Analyzer queries the Spark Catalog to validate column names, table names, and data types, transforming the unresolved plan into a resolved logical plan.

**12. What is the primary reason for using off-heap memory in the Tungsten engine?**
A) To share data across different executor nodes automatically.
B) To bypass the JVM's Garbage Collector overhead and object structure overhead.
C) To allow Python UDFs direct access to memory.
D) To persist DataFrames permanently to disk.
* **Answer:** B
* **Mastery Explanation:** Java objects have a large header overhead (up to 16 bytes per object). Tungsten stores raw binary data off-heap, saving massive amounts of memory and preventing GC pauses from scanning millions of objects.

**13. How does Apache Arrow improve Pandas UDF performance?**
A) It translates Python code into Java bytecode.
B) It executes Python loops inside the JVM.
C) It facilitates zero-copy memory sharing and columnar data transfers.
D) It converts Pandas DataFrames into RDDs.
* **Answer:** C
* **Mastery Explanation:** Arrow is an in-memory columnar format that Spark and Pandas both understand natively. It avoids the costly serialization/deserialization steps previously required for PySpark.

**14. Which join strategy is most resilient to data skew WITHOUT manual salting if the smaller table is 5MB?**
A) Sort Merge Join
B) Broadcast Hash Join
C) Shuffle Hash Join
D) Cartesian Join
* **Answer:** B
* **Mastery Explanation:** Because the 5MB table is broadcast to all executors, no shuffle of the large table is required. Therefore, skewed keys in the large table stay in their original partitions, entirely avoiding skew-induced stragglers.

**15. What happens when Execution memory exceeds its boundaries and Storage memory is empty?**
A) The job fails with an OOM error.
B) Execution memory borrows up to 100% of the unified memory region.
C) The data is immediately spilled to disk.
D) Spark kills the executor.
* **Answer:** B
* **Mastery Explanation:** Under unified memory management, execution and storage share a pool. Execution can borrow storage space if it's unused, preventing premature disk spilling.

**16. What physical plan operator does Catalyst insert to move data across nodes?**
A) ShuffleExchange
B) Filter
C) Project
D) WholeStageCodegen
* **Answer:** A
* **Mastery Explanation:** `ShuffleExchange` represents a shuffle over the network, triggered by wide transformations like `groupBy`, `join`, or `repartition`.

**17. In the context of time-series gap filling, what is the primary benefit of using `Window.unboundedPreceding`?**
A) It allows grouping by time intervals dynamically.
B) It maintains sequential state for cumulative sums across the entire partition.
C) It prevents shuffling entirely.
D) It pushes the computation down to the database level.
* **Answer:** B
* **Mastery Explanation:** By framing the window from the beginning of the partition up to the current row, we can calculate a running total (cumulative sum) of session flags, effectively creating unique session IDs.

**18. Why is `explode` on arrays considered a performance anti-pattern compared to High-Order Functions?**
A) It requires converting the DataFrame back to an RDD.
B) It physically increases the row count and often necessitates a massive network shuffle for re-aggregation.
C) It is deprecated in Spark 3.0.
D) It disables Tungsten execution.
* **Answer:** B
* **Mastery Explanation:** Exploding an array with N elements multiplies the row count by N. Grouping them back together later requires a `ShuffleExchange`, destroying performance. High-Order Functions process the array completely in-place.

**19. Which configuration prevents out-of-memory errors on the driver during a massive `collect()` operation?**
A) `spark.executor.memory`
B) `spark.driver.maxResultSize`
C) `spark.memory.fraction`
D) `spark.sql.shuffle.partitions`
* **Answer:** B
* **Mastery Explanation:** `spark.driver.maxResultSize` limits the total size of serialized results returned to the driver. If the limit is exceeded, Spark aborts the job instead of crashing the driver node with an OOM.

**20. In Catalyst's Cost-Based Optimizer (CBO), which statistic is most crucial for choosing the optimal join algorithm?**
A) Number of partitions
B) Disk space
C) Table size in bytes / row count
D) CPU core count
* **Answer:** C
* **Mastery Explanation:** The CBO uses table sizes and row counts (gathered via `ANALYZE TABLE`) to estimate join costs. It uses this to decide between strategies like Broadcast Hash Join versus Sort Merge Join.

**21. What happens if a Pandas UDF returns a series of a different length than the input?**
A) Spark automatically pads it with nulls.
B) Spark ignores the extra rows.
C) A Py4J exception is thrown at runtime.
D) The Catalyst optimizer drops the column.
* **Answer:** C
* **Mastery Explanation:** A Pandas Scalar UDF must return a `pd.Series` of the exact same length as the input series, otherwise the internal vectorized mapping between input and output rows breaks, throwing an exception.

**22. Which Spark mechanism allows the execution engine to leverage CPU registers for intermediate data?**
A) Vectorized Parquet Readers
B) Adaptive Query Execution
C) Whole-Stage Code Generation
D) JVM Garbage Collection
* **Answer:** C
* **Mastery Explanation:** Whole-Stage Code Gen creates a single Java function for a stage. Because it removes virtual function calls (the Volcano model), the JVM's JIT compiler can optimize intermediate variables directly into CPU registers.

**23. What is a consequence of setting the salt bucket count too high in a skewed join?**
A) It disables the Catalyst optimizer.
B) The dimension table explosion causes severe memory pressure or OOM.
C) It forces a Broadcast join automatically.
D) It reduces the number of output partitions.
* **Answer:** B
* **Mastery Explanation:** Salting requires cross-joining the dimension table by the salt count. If `SALT_BUCKETS` is 10,000, a 1-million row dimension table becomes 10 billion rows, crashing the job.

**24. How does predicate pushdown interact with Parquet file metadata?**
A) It rewrites the Parquet file to remove rows.
B) It uses min/max row group stats to entirely skip reading irrelevant chunks of data.
C) It creates a secondary index in the JVM.
D) It compresses the data faster.
* **Answer:** B
* **Mastery Explanation:** Parquet stores min and max values for columns in each row group. Catalyst passes the SQL filter down, and the reader simply skips row groups where the requested value falls outside the min/max range.

**25. What happens during a "spill to disk" in Spark?**
A) Spark saves the final output of the job.
B) In-memory execution state (like a hash map for grouping) is serialized and written to local executor disks, causing a massive I/O bottleneck.
C) Driver OOMs are prevented by writing to S3.
D) Cached tables are moved to long-term storage.
* **Answer:** B
* **Mastery Explanation:** When execution memory is exhausted, Spark spills intermediate hash tables or sorted buffers to the local disk of the worker node. Disk I/O is vastly slower than RAM, severely degrading performance.

---

## Part 3: Small Twist Questions

**26. Scenario:** You have a 2GB dimension table and a 500GB fact table. You change `spark.sql.autoBroadcastJoinThreshold` to 3GB. What is the twist and outcome?
* **Answer:** The driver must first collect the 2GB table to broadcast it. This will likely cause a Driver OOM if driver memory is not configured large enough, or an Executor OOM when the broadcast variable is materialized in memory.
* **Mastery Explanation:** Increasing the threshold blindly ignores the memory constraints of the driver and executors. Broadcast joins require the entire table to fit in RAM on every node.

**27. Scenario:** You switch your data source from Parquet to standard JSON files. Your code relies heavily on `filter` operations. What happens?
* **Answer:** Predicate pushdown is effectively neutralized.
* **Mastery Explanation:** Unlike Parquet, JSON is a row-based text format without embedded min/max statistics. Spark must read and parse every single row from disk before applying the filter in memory, causing a massive performance hit.

**28. Scenario:** You add a `.cache()` call immediately before a severely skewed join, hoping to fix the skew. Does this solve the skew?
* **Answer:** No.
* **Mastery Explanation:** Caching only persists the data in its current partitioned state. The skew still exists in the partitions, and the subsequent shuffle for the join will still direct billions of rows to a single executor task.

**29. Scenario:** You migrate a standard Python UDF to a Pandas UDF. Inside the Pandas UDF, you write a standard Python `for` loop to iterate over the `pd.Series`. What is the performance impact?
* **Answer:** You lose the primary vectorized CPU benefit of Pandas UDFs.
* **Mastery Explanation:** While you still benefit from Arrow's fast serialization, iterating row-by-row in native Python negates C-backed vectorized array operations. The performance will be severely handicapped compared to using NumPy operations.

**30. Scenario:** You process a tiny 10MB dataset but set `spark.sql.shuffle.partitions` to 10,000. What happens?
* **Answer:** Severe task overhead and the "small files" problem.
* **Mastery Explanation:** Spark will spawn 10,000 tasks for the shuffle phase. The time spent scheduling and coordinating these tasks will drastically outweigh the actual computation time, and writing the output will create 10,000 tiny files.

**31. Scenario:** You define a Window specification with an `orderBy` clause but completely omit the `partitionBy` clause. What is the physical risk?
* **Answer:** A severe data skew leading to an OOM.
* **Mastery Explanation:** Without `partitionBy`, Spark must move the *entire dataset* into a single partition on a single executor to perform a global sort. This will almost certainly crash the executor for large datasets.

**32. Scenario:** In the time-series gap-filling example, you accidentally use `current_timestamp()` instead of `col("timestamp")` inside your lag calculation. What happens?
* **Answer:** The logic completely breaks, calculating the difference in execution time rather than event time.
* **Mastery Explanation:** `current_timestamp()` evaluates to the time the Spark job is running. All rows will effectively have the same or slightly varying timestamps based on job execution, failing to identify historical gaps in the data.

**33. Scenario:** In a salted skewed join, you change the join type from `how="inner"` to `how="left"` (where the large salted table is on the left). What is the semantic risk?
* **Answer:** Duplication or logic breakage.
* **Mastery Explanation:** The dimension table was artificially exploded (multiplied) to contain all salt values. If it's a left join and a match isn't found across all salts, you must be extremely careful to deduplicate or drop the salt properly, otherwise the explosion leaks into the final output.

**34. Scenario:** You attempt to salt a skewed table, but instead of using `rand()`, you generate the salt using `hash(col("customer_id")) % 50`. What happens to the skew?
* **Answer:** The skew remains exactly the same.
* **Mastery Explanation:** A deterministic hash function will always map the same skewed `customer_id` to the exact same salt bucket. Therefore, all records for that customer still land on the same single partition.

**35. Scenario:** You set `spark.sql.codegen.wholeStage=false`. How does the execution engine behave?
* **Answer:** It falls back to the Volcano Iterator model.
* **Mastery Explanation:** Spark will process rows one at a time passing through a chain of physical operators. Each operator requires a virtual function call (`next()`), incurring massive CPU overhead and breaking pipeline optimizations.

**36. Scenario:** You enable AQE (`spark.sql.adaptive.enabled=true`) on a job with a highly skewed join, and you remove your manual salting code. What happens?
* **Answer:** AQE dynamically detects the skew at runtime and handles it automatically.
* **Mastery Explanation:** AQE monitors shuffle file sizes. If it detects a severely skewed partition, it will dynamically split that partition into smaller sub-partitions and replicate the corresponding dimension data, effectively performing automatic salting.

**37. Scenario:** You use `.count()` on a 10-billion-row DataFrame just to get a rough estimate of size, but it takes 15 minutes. You switch to `countApprox(timeout)`. Why is it faster?
* **Answer:** It uses the HyperLogLog algorithm.
* **Mastery Explanation:** `countApprox` provides an approximate distinct count without requiring a full deterministic shuffle, heavily reducing network overhead at the cost of a small margin of error.

**38. Scenario:** You change `spark.memory.fraction` from the default 0.6 down to 0.1. What happens during a large sort operation?
* **Answer:** The job will spill to disk almost immediately.
* **Mastery Explanation:** `spark.memory.fraction` controls the portion of JVM heap dedicated to unified memory (execution + storage). Setting it to 0.1 leaves only 10% of the heap for data processing, forcing aggressive and constant disk spilling.

**39. Scenario:** You replace a `select(explode(col("orders")))` followed by an aggregation with a native High-Order Function `aggregate()`. What specific physical plan change occurs?
* **Answer:** The `Generate` operator and subsequent `HashAggregate` / `Exchange` operators are completely removed.
* **Mastery Explanation:** `explode` translates to a `Generate` physical operator which expands rows. By using HOFs, the operation is done entirely inside a `Project` operator, keeping the data locally packed without shuffling.

**40. Scenario:** You apply a string-to-timestamp cast *inside* your `filter` condition (e.g., `filter(cast(col) > '2023-01-01')`) instead of casting the column beforehand. What is the risk?
* **Answer:** You disable predicate pushdown.
* **Mastery Explanation:** Storage formats like Parquet store the raw string stats. By wrapping the column in a function (`cast`) during the filter evaluation, Catalyst cannot easily map the predicate to the raw Parquet metadata, forcing a full table scan.

---

## Part 4: Coding & Debugging Questions

**41. Debugging a Memory Leak:**
An executor continually dies with an OutOfMemoryError during a `crossJoin` between a 1 million row table and a 10,000 row table. 
* **Diagnosis & Fix:** A cross join creates 10 billion records, overflowing execution memory. To fix this, if the cross join is strictly necessary, ensure the 10,000 row table is explicitly broadcasted using `broadcast(df)`. Otherwise, rewrite the logic to use a proper join condition.

**42. Optimizer Blocker:**
You wrote a Python UDF `is_active(status)` and used it in your query: `df.filter(is_active(col("status")))`. The query reads from a 5TB Parquet table and takes hours.
* **Diagnosis & Fix:** The Python UDF acts as an opaque black box to Catalyst. Predicate pushdown is disabled, and all 5TB of data is shipped to Python workers. Fix: Replace the UDF with native Spark SQL functions, e.g., `df.filter(col("status") == "ACTIVE")`.

**43. Logic Error in Windowing:**
You want to calculate a running total of sales. You use: `Window.partitionBy("store").orderBy("time").rowsBetween(0, 1)`. The numbers are wrong.
* **Diagnosis & Fix:** `rowsBetween(0, 1)` only sums the current row and the *next* row. For a cumulative sum (running total), the frame must start from the beginning. Fix: Change to `Window.unboundedPreceding` and `Window.currentRow`.

**44. Identifying a Shuffle Issue:**
A Spark stage has 200 tasks. 199 tasks finish in 30 seconds. The last task runs for 45 minutes and eventually fails.
* **Diagnosis & Fix:** Classic Data Skew. A single key (e.g., nulls or a highly active user) has routed massive amounts of data to a single partition. Fix: Filter out nulls, use a salting technique (Code Example 2), or enable Spark 3 AQE skew optimization.

**45. Debugging Pandas UDF:**
You implemented a Pandas UDF, but the Python worker process is throwing an OutOfMemoryError, even though the executors have plenty of RAM.
* **Diagnosis & Fix:** Arrow is sending too many rows in a single batch to Pandas, exceeding the Python process's memory. Fix: Lower the configuration `spark.sql.execution.arrow.maxRecordsPerBatch` (default 10,000) to a smaller number like 1,000.

**46. Code Optimization (Logical Plan bloat):**
A developer writes a script with 50 sequential `.withColumn("colX", ...)` statements on a DataFrame. Driver takes 5 minutes just to start the job.
* **Diagnosis & Fix:** Each `withColumn` creates a new projection in the Logical Plan. 50 calls create a massively nested, unoptimized logical tree that Catalyst struggles to parse. Fix: Combine them into a single `select()` statement passing multiple column expressions at once.

**47. Storage Issue:**
You cache a massive 50GB DataFrame using `df.cache()` (default `MEMORY_AND_DISK`). It takes forever, and inspecting the Spark UI shows a massive memory footprint.
* **Diagnosis & Fix:** By default, PySpark caches DataFrames using serialized JVM objects, but sometimes tuning is needed. If using RDDs, Java serialization is huge. Fix: Ensure Tungsten's native execution memory caching is utilized, or for RDD-backed data, switch to `KryoSerializer` for a more compact memory footprint.

**48. Salting Logic Error:**
A junior engineer attempts to fix skew. They append a random salt to the small dimension table and explode the massive 10-billion row fact table.
* **Diagnosis & Fix:** They reversed the logic. Exploding the 10-billion row fact table by a salt factor of 10 creates 100 billion rows, crashing the entire cluster instantly. Fix: Append random salt to the Fact table, Explode the Dimension table.

**49. Gap Filling Logic Error:**
In the sensor gap-filling logic, the engineer uses `lead("timestamp", 1)` instead of `lag("timestamp", 1)` to calculate the time difference. The session IDs are completely scrambled.
* **Diagnosis & Fix:** `lead` looks *forward* in time. Calculating a cumulative sum requires looking *backward* (`lag`) to build upon past state chronologically. Using `lead` breaks the causal arrow of time required for stateful windowing.

**50. High-Order Function Error:**
A developer tries to extract product names from an array of structs: `df.withColumn("names", filter(col("orders"), lambda x: x["product"]))`. Spark throws an AnalysisException.
* **Diagnosis & Fix:** `filter` is used to remove items based on a boolean condition. To extract or manipulate items (mapping), you must use `transform`. Fix: Change `filter` to `transform`.
