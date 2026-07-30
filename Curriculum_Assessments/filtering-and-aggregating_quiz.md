# Filtering and Aggregating Mastery Quiz

## Part 1: True/False Questions (1-10)

**1. True/False:** Catalyst Predicate Pushdown evaluates filters on the JVM heap before Tungsten whole-stage code generation kicks in.
**Correct Answer:** False
**Mastery Explanation:** Predicate pushdown operates at the storage layer (e.g., inside the Parquet vectorized reader). By utilizing file-level metadata (min/max values in row groups), it skips reading non-matching blocks entirely, avoiding deserialization and JVM instantiation completely.

**2. True/False:** `HashAggregateExec` utilizes Tungsten's off-heap hash map to avoid JVM Garbage Collection (GC) pauses during partial aggregation.
**Correct Answer:** True
**Mastery Explanation:** Tungsten stores running aggregation buffers directly as raw binary bytes in off-heap memory. This prevents the JVM from creating heavy Java objects, eliminating GC overhead and taking advantage of L2/L3 CPU caches.

**3. True/False:** Spark falls back to `SortAggregateExec` when aggregating with functions like `collect_set` because they generate variable-length data structures that violate `HashAggregateExec`'s mutable buffer constraints.
**Correct Answer:** True
**Mastery Explanation:** `HashAggregateExec` requires fixed-length byte arrays (like those used for `sum` or `count`). Unbounded or variable-length objects like those produced by `collect_set` cannot be maintained safely in the off-heap hash map, forcing Catalyst to sort the data and use `SortAggregateExec`.

**4. True/False:** Predicate pushdown can skip reading entire Parquet row groups only if the filter condition uses a column that is explicitly excluded from the file's footer statistics.
**Correct Answer:** False
**Mastery Explanation:** The opposite is true. Predicate pushdown specifically relies on the Parquet file footers (Row Group min/max/count statistics) to evaluate if a block contains relevant data.

**5. True/False:** Whole-Stage Code generation fuses multiple physical operators (Scan, Filter, Partial Aggregate) into a single Java function to eliminate virtual function calls.
**Correct Answer:** True
**Mastery Explanation:** Tungsten uses the Janino compiler at runtime to generate tightly scoped `for` loops that fuse these operators. This removes iterator overhead and intermediate object allocations.

**6. True/False:** Salting a highly skewed grouping key will drastically increase the overall network shuffle volume during the final aggregation phase.
**Correct Answer:** False
**Mastery Explanation:** The partial aggregation on the mapper side (using the salted key) massively reduces the data volume before it is shuffled. The final aggregation simply merges these smaller pre-aggregated results.

**7. True/False:** During a `.groupBy().agg(sum("x"))`, Spark will shuffle all raw row data across the cluster before performing any mathematical summation on the Reducer side.
**Correct Answer:** False
**Mastery Explanation:** Spark performs a Partial Aggregation on the mapper side before shuffling. It computes a local sum and count, meaning it only shuffles the compressed intermediate state buffers rather than raw row records.

**8. True/False:** `SortAggregateExec` is generally preferred over `HashAggregateExec` when memory is abundant because distributed sorting is computationally cheaper than hashing.
**Correct Answer:** False
**Mastery Explanation:** Hashing (via `HashAggregateExec`) is O(N) and operates efficiently in off-heap memory. `SortAggregateExec` requires a highly expensive O(N log N) `SortExec` node, which is CPU and memory intensive and heavily strains the JVM heap.

**9. True/False:** Tungsten manages aggregation state buffers for `HashAggregateExec` entirely in off-heap memory using a CPU-cache-aligned binary format.
**Correct Answer:** True
**Mastery Explanation:** This mechanism is what enables Spark to perform aggregations with near C-level performance, avoiding the JVM garbage collector entirely by manipulating raw bytes.

**10. True/False:** Using a Window function with `row_number()` to find the latest record per group is less memory intensive than using `collect_list` within a `.groupBy()` operation.
**Correct Answer:** True
**Mastery Explanation:** `collect_list` creates massive, unbounded arrays on the JVM heap, forcing a fallback to `SortAggregateExec`. A Window function evaluates sort conditions within a partition and filters sequentially, completely circumventing large object allocations.

## Part 2: Multiple Choice Questions (11-25)

**11. Which Catalyst optimization technique directly prevents the instantiation of unnecessary data in the JVM heap?**
A. Whole-Stage Code Generation
B. Predicate Pushdown
C. Hash Partitioning
D. Kryo Serialization
**Correct Answer:** B
**Mastery Explanation:** Predicate Pushdown evaluates filter expressions at the data source level (e.g., Parquet reader). By skipping irrelevant data blocks based on metadata, the data is never read into memory, bypassing JVM object instantiation entirely. The others operate after data is already in memory or during shuffle.

**12. Why does using `collect_set` in an aggregation force Catalyst to use `SortAggregateExec` instead of `HashAggregateExec`?**
A. Sets must be strictly ordered alphabetically before aggregation.
B. Tungsten’s off-heap hash map only supports fixed-length, mutable state buffers.
C. `collect_set` requires an automatic cross join across partitions.
D. `HashAggregateExec` only supports numeric data types.
**Correct Answer:** B
**Mastery Explanation:** `HashAggregateExec` achieves its speed by using CPU-cache-aligned, fixed-length bytes in off-heap memory. Unbounded, variable-length collections like those built by `collect_set` cannot fit into these strict mutable states, forcing Spark to fallback to `SortAggregateExec`.

**13. In a two-phase aggregation execution plan (`HashAggregateExec`), what specifically is transferred over the network during the Shuffle phase?**
A. The complete, raw Parquet row groups matching the filter.
B. The compiled Janino bytecode for the Reducer tasks.
C. The intermediate, pre-aggregated state buffers from the mapper side.
D. The complete, final grouped output arrays.
**Correct Answer:** C
**Mastery Explanation:** The first `HashAggregateExec` (Partial Aggregation) computes local sums/counts on the mapper. The shuffle phase only moves these tiny, intermediate aggregation state buffers to the reducers, significantly reducing network bandwidth compared to moving raw rows.

**14. If a single task in a `groupBy` stage is taking 4 hours while all other 199 tasks finished in 2 minutes, what is the most likely architectural cause?**
A. The Parquet vectorized reader is disabled.
B. The Catalyst optimizer failed to compile the Janino code.
C. A data skew event where a single hashed grouping key contains the vast majority of records.
D. The `spark.sql.shuffle.partitions` value is set too low.
**Correct Answer:** C
**Mastery Explanation:** `groupBy` assigns data to reducers based on the hash of the grouping key. If one key (e.g., `null` or `'ACTIVE'`) dominates the dataset, one executor is burdened with almost all the data. This nullifies distributed processing, causing straggler tasks and OOM exceptions.

**15. What is the primary purpose of Whole-Stage Code Generation in filtering and aggregating?**
A. To convert Spark SQL into executable Python scripts.
B. To collapse multiple physical operators into a single Java `for` loop, eliminating virtual function overhead.
C. To serialize the abstract syntax tree for transmission to executors.
D. To automatically generate Parquet min/max footer statistics.
**Correct Answer:** B
**Mastery Explanation:** Whole-Stage Code Generation utilizes the Janino compiler to fuse operations like Scan, Filter, and Partial Aggregate into optimized bytecode. This removes the overhead of passing objects between iterators and leverages CPU registers for intermediate states.

**16. Which scenario completely negates the benefits of Predicate Pushdown when reading from Parquet?**
A. Filtering on a column that is not partitioned, but is heavily indexed in the Parquet footer.
B. Applying a complex User Defined Function (UDF) to a column in the `where()` clause.
C. Filtering on a date column using `>=` and `<=`.
D. Enabling dictionary filtering in Spark SQL configuration.
**Correct Answer:** B
**Mastery Explanation:** Catalyst cannot push down arbitrary, black-box UDF logic into the Parquet reader because the storage format does not understand the custom Java/Scala code. The data must be fully deserialized into the JVM before the UDF can be evaluated, destroying pushdown benefits.

**17. How does a "Salting" technique mitigate Out-Of-Memory (OOM) errors during skewed aggregations?**
A. By increasing the memory allocated to the driver node.
B. By appending a random integer to the skewed key, distributing the partial aggregation across multiple reducers.
C. By compressing the shuffle data using Snappy.
D. By converting `SortAggregateExec` to `HashAggregateExec` forcefully.
**Correct Answer:** B
**Mastery Explanation:** By concatenating a random number (salt) to the skewed key, the data is artificially split into many sub-partitions. A partial aggregation on this salted key distributes the heavy lifting across the cluster. The final aggregation removes the salt and finishes the math on a drastically reduced data volume.

**18. What is the computational complexity of a `HashAggregateExec` vs a `SortAggregateExec`?**
A. Hash: O(N log N), Sort: O(N)
B. Hash: O(1), Sort: O(N)
C. Hash: O(N), Sort: O(N log N)
D. Hash: O(N^2), Sort: O(N)
**Correct Answer:** C
**Mastery Explanation:** Hashing requires a single pass over the data, making it O(N). Sorting requires a full shuffle and ordering of records, scaling at O(N log N), which is far more CPU and memory intensive.

**19. When writing Spark code, what is the best alternative to using `groupBy("id").agg(collect_list("events"))` to find the most recent event per id?**
A. Using `repartition(1)` before the group by.
B. Using a Window function with `row_number()` ordered by timestamp descending, then filtering for rank 1.
C. Disabling `spark.sql.parquet.enableVectorizedReader`.
D. Converting the DataFrame to an RDD and using `reduceByKey`.
**Correct Answer:** B
**Mastery Explanation:** `collect_list` creates massive memory arrays on the JVM, risking OOM and forcing a `SortAggregateExec`. A Window function sorts data intra-partition and allows immediate row-based filtering, bypassing huge array allocations.

**20. What role does Tungsten play during a `.filter(col("x") > 10)` operation if Whole-Stage Code Gen is enabled?**
A. It compiles the filter check directly into the same loop that reads the data, preventing the creation of temporary objects.
B. It pushes the filter to the remote database using JDBC.
C. It serializes the filter string using Kryo.
D. It writes the filtered data to disk immediately to save memory.
**Correct Answer:** A
**Mastery Explanation:** Whole-Stage Code Generation (part of Tungsten) generates low-level Java code at runtime. It fuses the scan and filter so the condition is evaluated inside the reading loop, keeping intermediate states in CPU registers rather than allocating memory.

**21. Why does Spark perform a Partial Aggregation before the network shuffle?**
A. To backup the data in case of node failure.
B. To validate the data types of the grouping columns.
C. To reduce the sheer volume of byte data transmitted over the network infrastructure.
D. To ensure the final reducer receives strictly sorted data.
**Correct Answer:** C
**Mastery Explanation:** The partial aggregation computes local totals (like sum/count) for the mapper's partition. Instead of sending millions of individual rows across the network, it only sends a single, compressed state buffer per grouping key, preventing network saturation.

**22. Which configuration enables Spark to evaluate filters directly against Parquet dictionaries?**
A. `spark.sql.shuffle.partitions`
B. `spark.sql.parquet.filterPushdown`
C. `spark.sql.codegen.wholeStage`
D. `spark.driver.memory`
**Correct Answer:** B
**Mastery Explanation:** `spark.sql.parquet.filterPushdown` allows Catalyst to embed the filter directly into the file format reader. If dictionary encoding is used in Parquet, it can filter the dictionary itself rather than scanning every row.

**23. What happens if a grouping key is entirely `null` for 90% of the dataset during a `.groupBy().count()`?**
A. Spark automatically filters out `null` keys before shuffling.
B. The data is evenly distributed across all available executors using round-robin.
C. A severe data skew occurs, forcing a single task to process 90% of the data.
D. Catalyst rewrites the query to use an anti-join.
**Correct Answer:** C
**Mastery Explanation:** The hash function for `null` evaluates to a single integer, sending all 90% of those records to one exact partition on one executor. This causes massive skew, leading to extreme execution times or OOM exceptions.

**24. In the physical plan, what node indicates that a filter was successfully pushed down to the storage layer?**
A. `FilterExec`
B. `PushedFilters` inside a `FileScan` node
C. `HashAggregate`
D. `Exchange hashpartitioning`
**Correct Answer:** B
**Mastery Explanation:** A senior engineer looks for `PushedFilters` within the `FileScan` metrics in the `explain` output. This proves that the data reduction occurred at the storage API level, avoiding JVM deserialization.

**25. Tungsten’s off-heap hash map gracefully handles exceeding memory limits by doing what?**
A. Crashing the executor with an `OutOfMemoryError`.
B. Automatically requesting more memory from the YARN ResourceManager.
C. Spilling the intermediate aggregation buffers to local disk.
D. Falling back to an RDD map-reduce job.
**Correct Answer:** C
**Mastery Explanation:** Unlike on-heap Java HashMaps which throw OOM errors when the heap is full, Tungsten's off-heap map tracks its memory usage at a byte level. If it nears the limit, it gracefully sorts and spills the binary data to the local disk, allowing the process to survive.

## Part 3: Small Twist Questions (26-40)

**26. Twist:** You have a fast query: `df.filter(col("id") > 100)`. You change it to `df.filter(my_udf(col("id")) > 100)`. Performance drops by 90%. Why?
**Correct Answer:** The UDF breaks Predicate Pushdown.
**Mastery Explanation:** Parquet readers understand simple SQL predicates (`>`, `<`, `=`). They do not understand custom Python/Scala UDFs. Spark is forced to read every single row into the JVM, deserialize it, and run the UDF against it, completely destroying the storage-layer optimization.

**27. Twist:** You run `df.groupBy("city").agg(sum("sales"))` and it is blazing fast using `HashAggregateExec`. You change it to `df.groupBy("city").agg(collect_list("sales"))`. It takes 10x longer and triggers a `SortAggregateExec`. Why?
**Correct Answer:** `collect_list` lacks a fixed-size mutable buffer.
**Mastery Explanation:** `sum` requires a constant 8 bytes (Long/Double) for state, perfect for Tungsten's off-heap map. `collect_list` creates arrays that grow infinitely, which cannot be stored in the fixed-length off-heap map, forcing a fallback to a costly distributed sort.

**28. Twist:** A `.groupBy("status").count()` job on a 5TB dataset runs fine, but when you filter `df.filter("status = 'ACTIVE'").groupBy("status").count()`, one task hangs for hours. `ACTIVE` makes up 99% of the data. Why did adding the filter cause the job to hang?
**Correct Answer:** The filter removed the natural entropy, exposing the severe data skew.
**Mastery Explanation:** Without the filter, other statuses processed quickly, but the single task handling `ACTIVE` was still slow. By filtering down to *only* `ACTIVE`, you effectively created a dataset where 100% of the data hashes to a single partition, starving all other executors of work.

**29. Twist:** You configure `spark.sql.shuffle.partitions = 2000` instead of the default 200. Your aggregation `df.groupBy("id").sum()` now runs slower, despite the partitions being smaller. Why?
**Correct Answer:** Task scheduling and network overhead outweigh the compute benefits.
**Mastery Explanation:** While more partitions reduce the memory load per task, they increase the number of tasks the driver must schedule and the number of network connections established during the shuffle. If the dataset is small, the overhead of 2000 tasks destroys performance.

**30. Twist:** You read a CSV file instead of a Parquet file: `spark.read.csv().filter("date > '2023'")`. You notice a massive CPU spike on the executors. Why?
**Correct Answer:** CSV does not support Predicate Pushdown via metadata.
**Mastery Explanation:** Unlike Parquet, CSV lacks footers with min/max statistics. Spark must read the entire text file, parse every comma, cast strings to dates, and evaluate the filter in the JVM for every single row.

**31. Twist:** You salt a grouping key using `rand() * 10`, perform a partial aggregation, and then immediately write to disk without performing the final aggregation. What is the state of your data?
**Correct Answer:** The data is partially aggregated but logically incorrect, split across 10 random sub-keys per original key.
**Mastery Explanation:** Salting requires a two-stage aggregation. The first stage aggregates the salted keys. If you stop there, a single original key (e.g., `user_1`) will have up to 10 separate rows (e.g., `user_1_0`, `user_1_5`), yielding incorrect final sums.

**32. Twist:** You run `df.groupBy("id").agg(countDistinct("event_type"))` and performance is terrible. You change it to `approx_count_distinct("event_type")` and it flies. Why?
**Correct Answer:** `approx_count_distinct` uses HyperLogLog, which utilizes a fixed-size buffer.
**Mastery Explanation:** Exact `countDistinct` requires tracking every unique value (variable length, heavy memory, often forcing sorts or large sets). HyperLogLog uses a tiny, fixed-size byte array to estimate cardinality, perfectly aligning with `HashAggregateExec`.

**33. Twist:** You have a query `df.filter("a > 5 or b < 10")`. You change it to `df.filter("a > 5 and b < 10")`. The `and` query reads 1GB of disk, while the `or` query reads 100GB. Both tables are Parquet. Why?
**Correct Answer:** `OR` conditions severely limit Predicate Pushdown effectiveness.
**Mastery Explanation:** With an `AND`, if a block fails the `a > 5` check, it is skipped. With an `OR`, even if `a > 5` is false, Spark must still read the block to check `b < 10`. This forces Spark to read vastly more raw data from disk.

**34. Twist:** You are using Window functions to find the latest event. You change `orderBy(col("timestamp").desc)` to just `orderBy(col("timestamp"))` without adding `.desc()`. What logical error did you just introduce?
**Correct Answer:** You are now keeping the oldest record, not the newest.
**Mastery Explanation:** Ascending order puts the oldest timestamps at rank 1. If your subsequent filter is `rank === 1`, you are extracting the very first event that occurred, completely reversing the analytical intent.

**35. Twist:** You implement salting with `saltConfig = 1000000` (1 million) instead of 50. The job crashes the driver node. Why?
**Correct Answer:** The resulting shuffle map output explosion overwhelmed the driver's metadata tracking.
**Mastery Explanation:** A salt of 1 million creates 1 million sub-partitions *per original key*. This generates a catastrophic number of tiny shuffle blocks. The Spark Driver must track the location of every block, and this metadata explosion causes an OOM on the Driver itself.

**36. Twist:** You write `df.filter(col("timestamp") === lit(null))` to find missing timestamps. The result is always an empty DataFrame, even though nulls exist. Why?
**Correct Answer:** SQL null semantics require `isNull()`, not equality checks.
**Mastery Explanation:** In Spark/SQL, `null === null` evaluates to `null` (unknown), not `true`. The filter discards any row that does not evaluate strictly to `true`. You must use `col("timestamp").isNull()` or `isNotNull()`.

**37. Twist:** Your Parquet files are heavily fragmented (100,000 files of 10KB each). Even with a highly selective filter, the job takes 30 minutes just to start tasks. Why did Predicate Pushdown fail to save you?
**Correct Answer:** Driver-side metadata listing and file opening overhead.
**Mastery Explanation:** Predicate Pushdown happens on the executors. But before that, the Driver must contact the NameNode/S3 to list all 100,000 files. The overhead of opening 100,000 separate TCP connections and reading 100,000 tiny footers swamps the cluster, regardless of the filter logic.

**38. Twist:** You run `df.groupBy("id").agg(sum("amount"))`. You then disable `spark.sql.codegen.wholeStage`. The physical plan still shows `HashAggregate`, but CPU usage doubles. Why?
**Correct Answer:** The loss of Janino code compilation reintroduced iterator virtual function calls.
**Mastery Explanation:** Without Whole-Stage Code Gen, Spark falls back to the Volcano iterator model. For every single row, the Aggregate operator must call `.next()` on the Filter operator, resulting in millions of expensive virtual method dispatches and intermediate object creations.

**39. Twist:** You use `.repartition(200, col("device_id"))` before a `.groupBy("device_id").count()`. The query completes, but the network shuffle volume is exactly the same as if you hadn't repartitioned. Why?
**Correct Answer:** `repartition` triggers a full shuffle *before* any partial aggregation can occur.
**Mastery Explanation:** A native `groupBy` performs a local partial aggregation first, shuffling only tiny state buffers. By calling `repartition` explicitly, you forced Spark to shuffle every raw, un-aggregated row across the network first, entirely defeating the optimization.

**40. Twist:** You have a cluster with 10 executors, 4 cores each (40 total cores). You set `spark.sql.shuffle.partitions = 10`. The shuffle phase is extremely slow, and 30 cores are completely idle. Why?
**Correct Answer:** You artificially capped parallelism below your hardware capacity.
**Mastery Explanation:** Spark creates exactly one reducer task per shuffle partition. If you only have 10 partitions, only 10 tasks can run concurrently. The remaining 30 cores on your cluster have absolutely no data assigned to them and sit idle.

## Part 4: Coding & Debugging (41-50)

**41. Debugging Data Skew Execution**
```scala
val df = spark.read.parquet("hdfs://data")
// The grouping key 'tenant_id' is heavily skewed towards tenant 'A' (95% of data).
val aggDf = df.groupBy("tenant_id").agg(sum("revenue"))
aggDf.write.parquet("hdfs://output")
```
**Identify the bottleneck:** What specific behavior will you see in the Spark UI for this exact code?
**Correct Answer:** In the Stage for the final aggregation (after the exchange), exactly one task will run significantly longer than all others, and it will show a massive "Shuffle Read Size". The other tasks will finish in seconds.

**42. Fixing the Skew via Salting**
Modify the code in Q41 to implement a basic 10-partition salting mechanism.
**Correct Answer:**
```scala
val salted = df.withColumn("salt", floor(rand() * 10))
  .withColumn("salted_tenant", concat(col("tenant_id"), lit("_"), col("salt")))

val partial = salted.groupBy("salted_tenant", "tenant_id").agg(sum("revenue").alias("p_sum"))
val finalAgg = partial.groupBy("tenant_id").agg(sum("p_sum").alias("total_revenue"))
```
**Mastery Explanation:** By splitting the skewed `tenant_id` into 10 distinct sub-keys, the heavy lifting of summation is spread across 10 tasks in the partial phase, preventing single-node memory exhaustion.

**43. Identifying Optimizer Blockers**
```scala
val myFilter = udf((dt: String) => dt.startsWith("2023"))
df.filter(myFilter(col("event_date"))).show()
```
**Identify the error:** Why is this code architecturally flawed for large Parquet datasets?
**Correct Answer:** The use of a UDF completely blocks Catalyst's Predicate Pushdown.
**Mastery Explanation:** Spark must read the entire file from disk, deserialize all strings into the JVM heap, and execute the Scala function. Replacing it with `col("event_date").startsWith("2023")` allows the Parquet reader to use dictionary filtering and skip blocks.

**44. Memory Leaks in Aggregations**
```scala
// Analyzing user session paths
df.groupBy("user_id").agg(collect_list("url_visited").alias("path")).show()
```
**Identify the vulnerability:** What physical operator will this trigger, and what is the risk?
**Correct Answer:** It triggers `SortAggregateExec` and risks a `java.lang.OutOfMemoryError`.
**Mastery Explanation:** `collect_list` creates unbounded JVM arrays. Tungsten cannot use its off-heap memory map. The data must be sorted and buffered on the JVM heap. If a user has millions of clicks, their single array will exceed executor memory limits and crash the node.

**45. Refactoring collect_list to Window Functions**
Refactor the code from Q44 to find ONLY the most recently visited URL per user, without using `collect_list`. Assume a `timestamp` column exists.
**Correct Answer:**
```scala
val w = Window.partitionBy("user_id").orderBy(col("timestamp").desc)
df.withColumn("rank", row_number().over(w))
  .filter(col("rank") === 1)
  .select("user_id", "url_visited")
```
**Mastery Explanation:** This approach utilizes Tungsten's fast sorting mechanics intra-partition and streams the results, filtering out everything but rank 1. It avoids building massive arrays in memory.

**46. Analyzing Physical Plans**
You run `.explain(true)` and see:
`+- SortAggregate(key=[id#1], functions=[collect_set(val#2)])`
`   +- Sort [id#1 ASC NULLS FIRST]`
`      +- Exchange hashpartitioning(id#1, 200)`
**Identify the performance killer:** Why did Catalyst inject a `Sort` node before the aggregation?
**Correct Answer:** `collect_set` requires `SortAggregateExec`, which strictly requires its input partitions to be pre-sorted by the grouping key. The `Exchange` moved the data, but it arrived unordered. The forced `Sort` is highly CPU and memory intensive.

**47. Misconfigured Joins leading to Aggregation Failures**
```scala
val filteredDf = df.filter("amount > 1000")
val joinedDf = filteredDf.join(smallLookupDf, "id")
joinedDf.groupBy("category").count().show()
```
**Identify the optimization:** If `smallLookupDf` is 5MB, how can you radically speed up this aggregation pipeline?
**Correct Answer:** Wrap `smallLookupDf` in a `broadcast()` hint.
**Mastery Explanation:** Without a broadcast, the join triggers a massive shuffle of `filteredDf`. By broadcasting the 5MB table, it acts as a map-side join. This prevents the initial shuffle, allowing the subsequent `.groupBy().count()` to execute its partial aggregation efficiently on the already-local data.

**48. Null Handling in Filters**
```scala
// We want to drop all rows where user_agent is missing
df.filter(col("user_agent") =!= null).groupBy("os").count()
```
**Identify the logic error:** This code runs successfully but yields the wrong answer. Why?
**Correct Answer:** `=!= null` evaluates to null, filtering out EVERYTHING or NOTHING depending on the engine context, but it never acts as a true null check.
**Mastery Explanation:** Standard SQL compliance mandates that null is undefined. Any equality check against null is null. You must use `.isNotNull` or `.filter("user_agent IS NOT NULL")`.

**49. Tungsten Off-Heap Memory Verification**
```scala
spark.conf.set("spark.sql.shuffle.partitions", 50)
df.groupBy("device_id").agg(sum("bytes")).write.parquet("out")
```
**Identify the mechanism:** In the above code, where specifically does the `sum("bytes")` value live during the mapper phase before the network transfer?
**Correct Answer:** It lives in the Tungsten off-heap hash map as raw, CPU-cache-aligned binary bytes.
**Mastery Explanation:** Because `sum` is a fixed-size mutable state (Long/Double), Catalyst uses `HashAggregateExec`. Tungsten allocates this state completely outside the JVM garbage collector's purview, allowing it to aggregate millions of rows per second without GC pauses.

**50. Advanced CodeGen Analysis**
You run a query and check the physical plan:
`*(2) HashAggregate(keys=[id], functions=[sum(val)])`
`+- Exchange hashpartitioning(id, 200)`
`   +- *(1) HashAggregate(keys=[id], functions=[partial_sum(val)])`
`      +- *(1) Filter (val > 10)`
**Identify the asterisks:** What does the `*(1)` and `*(2)` denote in this physical plan?
**Correct Answer:** They denote Whole-Stage Code Generation stages.
**Mastery Explanation:** The `*` indicates that the Catalyst optimizer utilized Janino to fuse those specific operators into a single Java function. Stage `*(1)` fused the Filter and Partial Aggregation into one loop on the mapper. Stage `*(2)` fused the final aggregation on the reducer.
