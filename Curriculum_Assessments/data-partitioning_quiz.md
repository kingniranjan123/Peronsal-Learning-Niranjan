# 🔥 Master Class Assessment: Data Partitioning

This assessment contains 50 elite-level technical questions designed to test Senior/Staff-level knowledge of Apache Spark's data partitioning, Catalyst optimization, Tungsten execution, memory management, and physical planning.

---

## Part 1: True/False Questions

**1. Catalyst's Physical Planning phase leverages Tungsten's vectorized readers to pull data directly into JVM on-heap memory to bypass standard object creation overhead.**
**Answer:** False
**Mastery Explanation:** Tungsten pulls data directly into *off-heap* memory, not on-heap. This is critical for avoiding JVM Garbage Collection (GC) pauses and improving CPU cache locality.

**2. A `HashPartitioner` maps keys to partitions using `Object.hashCode % numPartitions`, which inherently prevents data skew by guaranteeing an even distribution.**
**Answer:** False
**Mastery Explanation:** `HashPartitioner` is highly susceptible to data skew. If a specific key (e.g., `country="USA"`) has a massively disproportionate number of records, all those records will hash to the same partition, causing a "straggler" task and potential OOMs.

**3. During a shuffle, if the incoming partition size exceeds the executor's JVM heap or off-heap allocation, Tungsten will spill the data to disk.**
**Answer:** True
**Mastery Explanation:** Tungsten attempts to process shuffle data in memory. When the partition working set exceeds available memory limits, it safely (but slowly) spills to disk to prevent OOM exceptions, causing massive performance degradation.

**4. Calling `coalesce(1)` immediately after reading a 1TB Parquet file causes the read operation to be executed on a single executor core.**
**Answer:** True
**Mastery Explanation:** `coalesce` does not induce a shuffle boundary (`Exchange`). Thus, the `DAGScheduler` collapses the physical plan into a single stage, forcing the `FileSourceScanExec` to run with a parallelism of 1.

**5. Setting `spark.sql.shuffle.partitions` to a massive number like 8000 is an anti-pattern when Adaptive Query Execution (AQE) is enabled.**
**Answer:** False
**Mastery Explanation:** With AQE enabled, setting a high initial partition count is the recommended pattern. AQE's `coalescePartitions` feature will dynamically shrink the thousands of small map output partitions into optimally sized reducer tasks.

**6. The `DAGScheduler` analyzes the execution plan and inserts a Shuffle boundary (Stage division) whenever it encounters a change in the `Partitioner` trait.**
**Answer:** True
**Mastery Explanation:** A change in partitioning (e.g., from `UnknownPartitioning` to `HashPartitioning` during a join) fundamentally requires data to be moved across the network to satisfy the new distribution, which physically manifests as a shuffle.

**7. `repartition(n)` executes significantly faster than `coalesce(n)` when reducing partition count because it utilizes a highly optimized Round Robin distribution.**
**Answer:** False
**Mastery Explanation:** `repartition(n)` forces a full cluster-wide network shuffle, making it mechanically O(N) in network/disk I/O and much slower than `coalesce(n)`, which is O(1) metadata operation that avoids a shuffle by fusing local partitions.

**8. To completely eliminate the `Exchange` and `SortExec` physical nodes during a Sort-Merge Join, bucketed tables must be saved using both `bucketBy()` and `sortBy()` with the exact same bucket count.**
**Answer:** True
**Mastery Explanation:** Bucketing shifts the shuffle cost to write time. If tables are pre-partitioned (bucketed) and pre-sorted by the join key, Catalyst recognizes this in the Hive Metastore metadata and skips both the network exchange and the local sort phases.

**9. In the context of a skewed join, "salting" prevents OOM errors by replacing a `HashPartitioner` with a `RangePartitioner`.**
**Answer:** False
**Mastery Explanation:** Salting does not change the partitioner type. It modifies the join key (e.g., appending a random integer) so that the `HashPartitioner` distributes the previously skewed key across multiple partitions (e.g., `USA_1`, `USA_2`), splitting the massive load.

**10. The `ShuffleExchangeExec` node is injected by Catalyst during Physical Planning to enforce data distribution requirements across the cluster.**
**Answer:** True
**Mastery Explanation:** When Catalyst determines that an operation (like an aggregation or join) requires a specific distribution (e.g., all rows with the same key on the same node), it injects `ShuffleExchangeExec` to physically move the data.

---

## Part 2: Multiple Choice Questions

**11. Which Spark component manages the local storage of partition data and serves map outputs to reducer tasks over the network?**
A) DAGScheduler
B) TaskScheduler
C) BlockManager
D) Catalyst Optimizer
**Answer:** C
**Mastery Explanation:** The `BlockManager` runs on every executor and handles memory/disk storage. During a shuffle, it writes map outputs locally and serves them to remote `ShuffleClients`. `DAGScheduler` handles stages, and `TaskScheduler` dispatches tasks.

**12. Why is the Java serializer considered detrimental during network shuffles compared to Kryo?**
A) It cannot serialize Catalyst expressions.
B) It produces massive serialized object sizes and is CPU-intensive.
C) It cannot write to off-heap memory.
D) It forces a Round Robin partitioning scheme.
**Answer:** B
**Mastery Explanation:** The native Java serializer includes heavy class metadata with every object, resulting in inflated payload sizes for network I/O and high CPU serialization overhead. Kryo is significantly faster and more compact.

**13. A dataset partitioned by `country` suffers from severe data skew. Which of the following operations will NOT help alleviate the processing bottleneck?**
A) Salting the join keys
B) Increasing `spark.sql.shuffle.partitions` from 200 to 2000
C) Enabling AQE Skew Join Optimization
D) Filtering out null keys before the join
**Answer:** B
**Mastery Explanation:** Increasing shuffle partitions only increases the number of available partitions. The skewed `country` key will still hash to a single partition, sending the massive data block to a single task regardless of how many total partitions exist.

**14. What is the primary physical limit that necessitates data partitioning in distributed systems?**
A) The CPU clock speed of the driver node
B) The memory (RAM) and disk capacity of single commodity servers
C) The maximum number of threads in the JVM
D) The latency of the Hive Metastore
**Answer:** B
**Mastery Explanation:** Petabyte-scale datasets physically cannot fit onto a single node's RAM or disk. Partitioning dictates how data is distributed so that the aggregate memory and storage of the cluster can be utilized.

**15. When using `repartition(col)`, what is the Big-O complexity and does it induce a shuffle?**
A) O(1) metadata, No Shuffle
B) O(N) network I/O, No Shuffle
C) O(1) metadata, Yes Shuffle
D) O(N) network I/O, Yes Shuffle
**Answer:** D
**Mastery Explanation:** `repartition` physically redistributes every row across the network based on the hash of the provided column, making it O(N) in terms of data movement and guaranteeing a shuffle.

**16. How does AQE's `coalescePartitions` feature decide the final number of reducer tasks?**
A) It uses the static value defined in `spark.sql.shuffle.partitions`.
B) It calculates the optimal count based on the physical size of MapStatus statistics.
C) It probes the HDFS block size.
D) It runs a pre-job sampling stage on the entire dataset.
**Answer:** B
**Mastery Explanation:** During the AQE pause between stages, Catalyst inspects the size of the shuffle files written by the map tasks. It dynamically groups small partitions together to reach the `advisoryPartitionSizeInBytes` target.

**17. What does the `partitionBy(col)` method do?**
A) It performs a network shuffle in memory based on the column hash.
B) It writes data into a nested directory structure on disk (e.g., `col=value/`).
C) It creates an index on the column in the Hive Metastore.
D) It pre-sorts the data within Tungsten binary rows.
**Answer:** B
**Mastery Explanation:** `partitionBy` is a write-side operation that dictates the physical directory layout on storage. Overusing it on high-cardinality columns creates the "Small Files Problem".

**18. In the Tungsten execution model, what form does a Spark partition take during physical execution?**
A) A Java `ArrayList` of objects
B) An Iterator of Tungsten binary rows
C) A serialized JSON string
D) A memory-mapped file descriptor
**Answer:** B
**Mastery Explanation:** Tungsten represents partitions as Iterators of highly compact, CPU-cache-friendly binary rows, completely avoiding Java object overhead during transformations.

**19. Why does a cross-join (Cartesian product) become necessary on the dimension table when applying the "salting" technique?**
A) To force Spark to use a Broadcast Hash Join.
B) To trigger a shuffle on the dimension table.
C) Because the large table's keys have been appended with random salts, the dimension table must have matching keys for every possible salt value to ensure a match.
D) To prevent the Catalyst optimizer from pushing down filters.
**Answer:** C
**Mastery Explanation:** If the large table's `USA` key becomes `USA_1` through `USA_100`, the dimension table's single `USA` record must be duplicated 100 times (`USA_1` ... `USA_100`) so the inner join finds corresponding matches.

**20. What is the fundamental difference between `HashPartitioner` and `RangePartitioner`?**
A) Hash uses `hashCode % numPartitions`; Range samples keys to create boundaries for relatively equal-sized partitions.
B) Hash prevents data skew; Range guarantees data skew.
C) Hash is used for Strings; Range is used for Integers.
D) Hash triggers a shuffle; Range avoids a shuffle.
**Answer:** A
**Mastery Explanation:** `RangePartitioner` requires a sampling pass over the data to determine boundaries that will distribute the data evenly based on ordering. `HashPartitioner` blindly applies a hash function, which is faster but risks skew.

**21. When saving a bucketed table, what happens if you omit the `saveAsTable()` method and just use `save()`?**
A) The table is bucketed correctly, but not sorted.
B) The bucketing metadata is not written to the Hive Metastore, rendering the bucketing useless for future query optimizations.
C) Spark throws a compilation error.
D) The data is saved in CSV format instead of Parquet.
**Answer:** B
**Mastery Explanation:** Bucketing relies entirely on the Hive Metastore to store the structural metadata (number of buckets, bucketing column). Without `saveAsTable()`, the metadata is lost, and Catalyst will inject an `Exchange` during future joins anyway.

**22. If a dataset initially has 1000 partitions and you call `coalesce(2000)`, what occurs under the hood?**
A) Spark splits each partition perfectly in half without a shuffle.
B) Spark ignores the command and keeps 1000 partitions.
C) It behaves identically to `repartition(2000)` and induces a full network shuffle.
D) It crashes with an `IllegalArgumentException`.
**Answer:** C
**Mastery Explanation:** `coalesce` can only merge partitions locally. If you request more partitions than currently exist, it must move data across the network, automatically upgrading to a shuffle identical to `repartition`.

**23. Which action best resolves the "Small Files Problem" generated by a heavily partitioned write?**
A) Calling `repartition(10000)` before writing.
B) Using `coalesce()` or `repartition()` to reduce the partition count immediately before the `write` operation.
C) Disabling AQE.
D) Changing the output format to JSON.
**Answer:** B
**Mastery Explanation:** The Small Files Problem occurs when thousands of tasks write tiny files to disk. By reducing the partition count right before the write, you force Spark to write fewer, larger files.

**24. In the `DAGScheduler`, what defines the boundary between two Stages?**
A) A change in the DataFrame column names.
B) The execution of an Action (like `count()`).
C) An operation requiring a network shuffle (change in `Partitioner`).
D) Writing data to disk.
**Answer:** C
**Mastery Explanation:** Stages are sets of tasks that can be executed concurrently without network communication. A shuffle boundary—where data must be redistributed across nodes—mandates the end of one Stage and the beginning of another.

**25. Why might a Catalyst push-down filter fail to improve read parallelism?**
A) The underlying data is stored in an unsplittable format like standard GZIP JSON.
B) The cluster has too many executor cores.
C) AQE is enabled.
D) The filter is applied after a `repartition` operation.
**Answer:** A
**Mastery Explanation:** If the storage format is not splittable (like a monolithic GZIP file) or lacks metadata statistics (like Parquet/ORC min/max footers), Spark cannot parallelize the read or skip data chunks, forcing a single task to read the entire file.

---

## Part 3: "Small Twist" Questions

**26. Scenario:** You bucket table A into 256 buckets and table B into 200 buckets. You then join them on the bucketing key.
**Twist Effect:** Because the bucket counts mismatch, Catalyst cannot guarantee data co-location. It will completely ignore the bucketing on one or both tables and inject `Exchange` (shuffle) nodes, destroying the bucketing optimization.
**Answer/Mastery Explanation:** To eliminate shuffles, bucketed tables MUST have the exact same number of buckets. A mismatch forces a shuffle to align the data distributions.

**27. Scenario:** You enable AQE but set `spark.sql.adaptive.coalescePartitions.enabled` to `false`, leaving `spark.sql.shuffle.partitions` at 8000.
**Twist Effect:** AQE will not merge small map outputs. The Exchange node will emit exactly 8000 tiny partitions, resulting in 8000 micro-tasks. The job will suffer from extreme task scheduling overhead and likely create 8000 small files on write.
**Answer/Mastery Explanation:** Setting high shuffle partitions is only safe if `coalescePartitions` is enabled to dynamically shrink the count post-shuffle.

**28. Scenario:** You change `repartition(10)` to `coalesce(10)` *before* a highly restrictive filter operation on a 1TB dataset.
**Twist Effect:** The DAG collapses. The 1TB read and the filter are forced to execute using only 10 tasks across the cluster. This destroys read parallelism and will likely cause an OOM on the 10 executor cores handling the read.
**Answer/Mastery Explanation:** `coalesce` does not trigger a shuffle, so it pushes the concurrency limit up to the source. `repartition` forces a shuffle, protecting the read parallelism.

**29. Scenario:** You use `repartition("country")` instead of `repartition(100)` on a dataset heavily skewed towards "USA".
**Twist Effect:** Instead of distributing data evenly Round-Robin, you explicitly requested HashPartitioning by `country`. The massive "USA" subset will be sent to a single partition, causing a severe straggler task and an OOM.
**Answer/Mastery Explanation:** `repartition(col)` groups all identical column values into the same physical partition. It concentrates skew rather than diffusing it.

**30. Scenario:** You successfully bucket and sort a DataFrame, but save the format as `format("csv")` instead of Parquet.
**Twist Effect:** CSV does not support the internal file metadata and splittable block structures required for efficient bucket reading in the same way Parquet does. Spark's Catalyst may fall back to scanning and shuffling the CSVs, losing the performance benefit.
**Answer/Mastery Explanation:** Bucketing is deeply integrated with columnar formats like Parquet and ORC, which allow Tungsten to efficiently seek and stream presorted blocks.

**31. Scenario:** In a salted join, you increase `SALT_BINS` from 100 to 1,000,000 to "guarantee no skew" against a 50GB dimension table.
**Twist Effect:** The dimension table is `crossJoin`ed with the salt values. 50GB * 1,000,000 = 50 Petabytes. The driver and executors will crash immediately with an OOM while attempting to explode the dimension table.
**Answer/Mastery Explanation:** Salting requires duplicating the dimension table by the salt factor. The salt multiplier must be large enough to break up the skew, but small enough to fit the exploded dimension table in memory/disk.

**32. Scenario:** The infrastructure team changes the underlying HDFS block size from 128MB to 512MB for your raw data.
**Twist Effect:** Spark's initial `FileSourceScanExec` will generate 4x fewer partitions (each partition reading 512MB). This reduces the number of map tasks and increases the memory pressure on each executor core during the first stage.
**Answer/Mastery Explanation:** Initial partitioning is directly tied to the storage layer's block size. Larger blocks mean fewer, heavier Spark partitions.

**33. Scenario:** You set `spark.sql.adaptive.advisoryPartitionSizeInBytes` to a very small value, like `1048576` (1MB).
**Twist Effect:** AQE evaluates the map outputs and determines they are all larger than 1MB. It will not coalesce anything, leaving you with thousands of tiny tasks and high scheduling overhead.
**Answer/Mastery Explanation:** The advisory size is the target for coalescing. If it's too small, AQE disables itself effectively.

**34. Scenario:** You use a `HashPartitioner` on a sequential, monotonically increasing transaction ID column.
**Twist Effect:** Assuming the number of partitions isn't perfectly aligned with the sequence, `hashCode % numPartitions` on sequential integers generally results in a perfectly uniform distribution, completely avoiding skew.
**Answer/Mastery Explanation:** High cardinality, uniformly distributed keys (like UUIDs or sequential IDs) are the ideal candidates for HashPartitioning.

**35. Scenario:** You join a bucketed table (256 buckets) with an unbucketed table.
**Twist Effect:** Catalyst cannot perform a shuffle-free SortMergeJoin. It will inject an `Exchange` node to shuffle the unbucketed table into 256 hash partitions to match the bucketed table.
**Answer/Mastery Explanation:** While one side of the shuffle is eliminated (the bucketed table), the other side must still be shuffled over the network to align the data geographically.

**36. Scenario:** You apply `coalesce(100)` on a DataFrame that currently has 10 partitions.
**Twist Effect:** Spark will realize you are asking for more partitions than exist. `coalesce` behaves exactly like `repartition(100)` and triggers a full network shuffle.
**Answer/Mastery Explanation:** `coalesce` is only shuffle-free when shrinking the partition count.

**37. Scenario:** You write a perfectly optimized query that pushes down a filter, but you apply a Python UDF in the filter condition.
**Twist Effect:** Catalyst cannot translate opaque Python UDFs into Parquet/storage layer predicates. The push-down fails, Spark reads the entire dataset into memory, serializes it to Python worker processes, applies the UDF, and then filters it—devastating performance.
**Answer/Mastery Explanation:** UDFs are black boxes to the Catalyst optimizer and prevent optimization features like predicate pushdown.

**38. Scenario:** You attempt a shuffle-free join by bucketing `user_id` as an Integer in Table A, and bucketing `user_id` as a Long (BigInt) in Table B.
**Twist Effect:** Catalyst considers `Integer` and `Long` to have different hash distributions. It will ignore the bucketing and inject shuffles for both tables.
**Answer/Mastery Explanation:** Bucketing requires identical data types for the hash function to guarantee that matching keys land in the same bucket file.

**39. Scenario:** You salt the skewed Large Table with a random number, but instead of using a `crossJoin` on the Dimension table, you also assign a single random salt to the Dimension table rows.
**Twist Effect:** You will lose massive amounts of data in the join. A transaction with `USA_45` will only match the dimension table if the dimension table randomly rolled `USA_45`. If it rolled `USA_12`, the join misses.
**Answer/Mastery Explanation:** The dimension table must be exploded to contain ALL possible salt values to guarantee a match against the randomized large table keys.

**40. Scenario:** You configure `spark.sql.shuffle.partitions = 2` on a 10TB dataset, but enable AQE hoping it will fix it.
**Twist Effect:** AQE's `coalescePartitions` can only *reduce* the number of partitions, it cannot increase them beyond the initial `shuffle.partitions` setting. The query will run with 2 massive reducer tasks and immediately OOM.
**Answer/Mastery Explanation:** AQE coalesces downwards. You must provide a high initial ceiling (e.g., 2000+) for it to work effectively.

---

## Part 4: Coding & Debugging Questions

**41. Identify the Anti-Pattern in this snippet:**
```python
df = spark.read.parquet("s3a://massive-data/logs/")
df.coalesce(5).filter(col("error_code") == 500).write.parquet("s3a://output/")
```
**Answer:** The `coalesce(5)` is placed before the filter. Because `coalesce` does not trigger a shuffle, the DAG collapses. Spark will attempt to read the entire `massive-data` directory using only 5 tasks, destroying read parallelism and causing an OOM.
**Fix:** Move `coalesce(5)` after the filter, or change it to `repartition(5)` to induce a shuffle and protect the read stage.

**42. Identify the Optimizer Blocker:**
```python
df1 = spark.table("bucketed_users") # Bucketed by user_id into 100 buckets
df2 = spark.table("bucketed_events") # Bucketed by user_id into 100 buckets
# Joining the bucketed tables
joined = df1.join(df2, upper(df1.user_id) == upper(df2.user_id))
```
**Answer:** The `upper()` function modifies the join key. The data on disk is bucketed based on the hash of the raw `user_id`. By applying a function, Catalyst can no longer guarantee the hash distribution matches the files. It will discard the bucketing metadata and inject an Exchange shuffle.
**Fix:** Join directly on `user_id`. Ensure data is normalized (uppercased) *before* it is written to the bucketed tables.

**43. Fix the Skew Logic Error:**
```scala
val saltedTxn = transactions.withColumn("salt", lit(rand() * 10))
val saltedDim = dimension.withColumn("salt", lit(rand() * 10))
val joined = saltedTxn.join(saltedDim, Seq("id", "salt"))
```
**Answer:** The dimension table is being randomly salted instead of duplicated. This means an `id` in the dimension table will only have one random salt (e.g., `id_4`). If the transaction table has records with `id_1`, `id_2`, etc., they will fail to join.
**Fix:** The dimension table must be exploded using a `crossJoin` against a DataFrame containing all possible salt values (0 through 9).

**44. Debug the Memory Leak / Spilling Issue:**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
df = spark.read.parquet("/data/10TB_logs/")
df.groupBy("user_id").count().write.parquet("/output/")
```
**Answer:** The default 200 shuffle partitions are being used for a 10 Terabyte dataset. 10TB / 200 = 50GB per partition. A 50GB partition will vastly exceed the executor's memory (typically 8GB-32GB), forcing Tungsten to constantly spill to disk, resulting in hours of GC overhead and disk thrashing.
**Fix:** Increase `spark.sql.shuffle.partitions` to a much higher number (e.g., 8000) or enable AQE to let Spark size the partitions dynamically.

**45. Identify the Logical Partitioning Error:**
```python
df.write.partitionBy("timestamp_ms").parquet("/output/data/")
```
**Answer:** `timestamp_ms` (milliseconds) has extreme cardinality. `partitionBy` creates a new folder on the storage layer for every distinct value. This will create millions of folders containing tiny files (The Small Files Problem), crashing the HDFS NameNode or stalling S3 API requests, and making future reads unbearably slow.
**Fix:** Partition by a low-cardinality column like `date` (YYYY-MM-DD) or `year`/`month`.

**46. Explain the resulting Physical Plan:**
```python
df = spark.read.parquet("/data/")
df.repartition(100).coalesce(10).write.parquet("/out/")
```
**Answer:** `repartition(100)` creates a Shuffle Exchange node. The subsequent `coalesce(10)` does not create an Exchange. Therefore, Spark shuffles the data into 100 partitions, and then immediately on the reducer nodes, collapses those 100 partitions into 10 before writing. The write will execute with 10 concurrent tasks.

**47. Fix the AQE Configuration:**
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "10")
```
**Answer:** AQE is enabled, but the initial `shuffle.partitions` ceiling is set to 10. AQE can only group smaller partitions into larger ones; it cannot split massive partitions. A heavy job will process data in 10 massive chunks, causing OOMs.
**Fix:** Set `spark.sql.shuffle.partitions` to a high number (e.g., 2000, 4000) so AQE has room to coalesce downwards.

**48. Identify the Write Bottleneck:**
```python
# df has 10,000 evenly distributed partitions
df.repartition("boolean_flag_column").write.parquet("/out/")
```
**Answer:** The `boolean_flag_column` only has two possible values (True/False). `repartition(col)` uses HashPartitioning. All True records go to Partition A, all False records go to Partition B. The write operation will be severely bottlenecked, utilizing only 2 tasks (2 cores) in the entire cluster, leaving the rest idle.
**Fix:** If the goal is just to reduce file count, use `repartition(num)` without a column to maintain Round-Robin distribution.

**49. Debugging a failed Broadcast Join:**
```python
# dim_df is 5MB, fact_df is 1TB
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") # 10MB
joined = fact_df.join(dim_df, "id")
```
**Answer:** While `dim_df` is 5MB on disk, Parquet is highly compressed. When read into memory as Java objects / Tungsten rows, the 5MB file might inflate to 30MB, exceeding the 10MB broadcast threshold. Catalyst will fallback to a SortMergeJoin, shuffling the 1TB fact table.
**Fix:** Use a broadcast hint `fact_df.join(broadcast(dim_df), "id")` to force the broadcast, or increase the threshold `spark.sql.autoBroadcastJoinThreshold`.

**50. Why does this code cause a shuffle despite bucketing?**
```scala
val dfA = spark.table("bucketed_256_A")
val dfB = spark.table("bucketed_256_B")
val joined = dfA.join(dfB, "id").groupBy("id").count()
```
**Answer:** The join itself is shuffle-free because both tables are bucketed by `id` into 256 buckets. However, the subsequent `.groupBy("id")` forces an aggregation. Depending on Spark version and configs, Catalyst might inject a new Exchange to satisfy the aggregation's required distribution, or it might leverage the existing partition layout. If a shuffle occurs, it's because the aggregation requires a shuffle boundary.
**Fix:** In modern Spark, Catalyst is smart enough to realize the data is already partitioned by `id` from the join and will skip the shuffle for the `groupBy`. If it doesn't, ensure `spark.sql.optimizer.plannedWrite.enabled` or related AQE features aren't interfering. (Strictly speaking, `groupBy` on the bucketing key after a bucketed join should NOT shuffle, but operations on *different* keys will).
