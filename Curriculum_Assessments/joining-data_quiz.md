# Master Class Assessment: Joining Data

## Part 1: True/False Questions (10)

**1. True or False:** The Tungsten execution engine relies heavily on the JVM Garbage Collector to efficiently manage memory during the Sort phase of a Sort Merge Join (SMJ).
* **Answer:** False
* **Mastery Explanation:** Tungsten intentionally operates directly on serialized binary data in off-heap memory specifically to *bypass* the JVM Garbage Collector, avoiding massive GC pauses during intensive sort operations.

**2. True or False:** A Sort Merge Join (SMJ) guarantees that both datasets will be hash-partitioned across the cluster using the join keys before the merge phase begins.
* **Answer:** True
* **Mastery Explanation:** The first phase of an SMJ is the Exchange (Shuffle) phase, which hash-partitions the data. This guarantees that records with the same join keys from both datasets physically reside on the same node.

**3. True or False:** Broadcast Hash Joins (BHJ) are highly resilient to Driver Out-Of-Memory (OOM) exceptions because the broadcasted dataset is sent directly from executor to executor.
* **Answer:** False
* **Mastery Explanation:** In a BHJ, the smaller dataset is first collected by the Driver node before it is serialized and broadcasted to all executors. If this dataset exceeds the Driver's memory, it will crash the Driver.

**4. True or False:** Pre-bucketing two tables on their join keys will completely eliminate the Exchange (Shuffle) phase during an SMJ, even if one table has 100 buckets and the other has 200 buckets.
* **Answer:** False
* **Mastery Explanation:** To eliminate the shuffle phase, both tables must have the exact same number of buckets and utilize the same bucketing scheme.

**5. True or False:** Catalyst generally avoids using the Shuffle Hash Join (SHJ) strategy by default unless explicitly hinted, due to the risk of executor OOM errors.
* **Answer:** True
* **Mastery Explanation:** SHJ requires building an in-memory hash table for a single partition. If a partition is heavily skewed, this hash table can easily exceed executor memory, whereas SMJ can safely spill sorted data to disk.

**6. True or False:** When employing a salted join to combat severe data skew, the smaller dimension table must be multiplied via a cross join with every possible salt value.
* **Answer:** True
* **Mastery Explanation:** Because the skewed fact keys are randomized with a salt, the dimension table must be replicated for every salt permutation to guarantee that the randomized fact keys find a corresponding match.

**7. True or False:** A Theta Join (Range Join) lacking any equality conditions forces Catalyst to perform an O(N*M) Broadcast Nested Loop Join (BNLJ) or a Cartesian Product.
* **Answer:** True
* **Mastery Explanation:** Without an equality condition, Catalyst cannot use hash-partitioning to align records. It must compare every row in dataset A with every row in dataset B.

**8. True or False:** Whole-Stage CodeGen is primarily used in Spark joins to generate Java bytecode that parallelizes network shuffles across executors.
* **Answer:** False
* **Mastery Explanation:** Whole-Stage CodeGen collapses multiple physical operators into a single Java function to evaluate data at CPU speeds. It optimizes execution within a task, not the network shuffle mechanism.

**9. True or False:** Supplying the `broadcast()` hint instructs the Catalyst Optimizer to bypass its default size-estimation checks when formulating the physical plan.
* **Answer:** True
* **Mastery Explanation:** The hint forces a BHJ regardless of the underlying table statistics, which is highly effective when Catalyst's statistics are stale or inaccurate.

**10. True or False:** In an SMJ, sorting is performed globally across the entire cluster to ensure one monolithic sorted dataset before the merge phase.
* **Answer:** False
* **Mastery Explanation:** Sorting in an SMJ is performed *locally* within each partition after the hash-shuffle phase. The merge phase then steps through these locally sorted partitions.

## Part 2: Multiple Choice Questions (15)

**11. Which of the following is NOT a distinct phase in a standard Sort Merge Join (SMJ) execution?**
A) Exchange
B) Sort
C) Hash Table Probing
D) Merge
* **Answer:** C
* **Mastery Explanation:** Hash Table Probing is characteristic of Broadcast Hash Joins (BHJ) and Shuffle Hash Joins (SHJ). SMJ relies on iterators stepping through sorted partitions, not hash tables.

**12. What is the primary reason the Catalyst Optimizer typically avoids the Shuffle Hash Join (SHJ) strategy?**
A) It results in significantly higher network I/O than SMJ.
B) It risks Executor OOM errors if a single partition's hash table grows too large.
C) It cannot process equi-joins efficiently.
D) It requires data to be pre-bucketed.
* **Answer:** B
* **Mastery Explanation:** While SHJ avoids the sort phase, it must fit the smaller of the two partitions into memory as a hash table. Skewed partitions will cause this to breach memory limits, whereas SMJ gracefully spills to disk.

**13. In Code Example 3, what critical operation must be performed on the dimension table to facilitate a salted join?**
A) It must be hash-partitioned on the salted key.
B) It must be broadcasted to all nodes.
C) It must be cross-joined with a DataFrame of all possible salt values.
D) It must be globally sorted.
* **Answer:** C
* **Mastery Explanation:** Because the heavy key in the fact table is artificially fragmented with a random salt, the dimension table must contain every possible salt value for that key to ensure successful matches.

**14. What occurs if a dataset subjected to a Broadcast Hash Join exceeds `spark.driver.maxResultSize`?**
A) The Driver crashes with an Out Of Memory exception.
B) The Executors crash with an Out Of Memory exception.
C) Catalyst dynamically falls back to an SMJ.
D) The query defaults to a Broadcast Nested Loop Join.
* **Answer:** A
* **Mastery Explanation:** Before broadcasting, the dataset is collected at the Driver. If it exceeds the maximum result size, the Driver immediately halts the job with an error.

**15. A Theta Join composed purely of conditions like `events.date > promotions.date` defaults to which execution strategy?**
A) Sort Merge Join
B) Broadcast Hash Join
C) Broadcast Nested Loop Join
D) Shuffle Hash Join
* **Answer:** C
* **Mastery Explanation:** Range conditions prevent hash-partitioning. Catalyst must resort to a BNLJ, evaluating the condition against every possible pair of rows in an O(N*M) operation.

**16. What is the paramount advantage of Tungsten operating on off-heap memory during a join?**
A) It bypasses network serialization entirely.
B) It eliminates the overhead and unpredictable pauses of the JVM Garbage Collector.
C) It enables zero-copy networking between executors.
D) It prevents the need to spill data to disk.
* **Answer:** B
* **Mastery Explanation:** Off-heap memory avoids the JVM heap. By managing memory manually, Tungsten eliminates GC overhead, allowing for highly predictable and performant data processing at CPU speeds.

**17. When attempting to eliminate the Exchange (Shuffle) phase using bucketing, which condition is an absolute requirement?**
A) Both tables must be stored in Parquet format.
B) Both tables must have the exact same number of buckets and partition keys.
C) The data must be small enough to fit entirely in memory.
D) Adaptive Query Execution must be disabled.
* **Answer:** B
* **Mastery Explanation:** Catalyst can only skip the shuffle if it can mathematically guarantee that matching keys already reside on the same physical partitions, which requires identical bucketing schemes and bucket counts.

**18. What is the root cause of a "straggler task" during a Sort Merge Join?**
A) Outdated table statistics in the Catalyst Optimizer.
B) Data skew mapping a highly disproportionate number of keys to a single partition.
C) Utilizing a BNLJ on a massive dataset.
D) Failing to supply a `broadcast()` hint.
* **Answer:** B
* **Mastery Explanation:** Because SMJ hash-partitions data by key, millions of identical keys (like a "null" or default value) are forced into a single partition, overloading one core while the rest of the cluster idles.

**19. Which mechanism explicitly instructs Catalyst to ignore its default size-estimation heuristics?**
A) Adaptive Query Execution (AQE)
B) The `broadcast()` hint function
C) Bucketing
D) Salting
* **Answer:** B
* **Mastery Explanation:** The `broadcast()` hint forces Catalyst to plan a BHJ, overriding its internal size checks. This is useful when table statistics are missing or inaccurate.

**20. How does introducing an equality condition (e.g., `A.id == B.id`) optimize a Theta Join?**
A) It allows Catalyst to leverage SMJ or BHJ for the equi-join, applying the range condition as a post-filter.
B) It completely eliminates the shuffle phase.
C) It forces Tungsten to utilize off-heap memory for the range evaluation.
D) It triggers Adaptive Query Execution to split skewed dates.
* **Answer:** A
* **Mastery Explanation:** By providing an equality condition, Catalyst can use efficient hash-based routing and sorting (SMJ/BHJ) to find potential matches, and only evaluate the expensive non-equi condition on those localized subsets.

**21. Why might an executor crash due to GC overhead during an SMJ on a skewed dataset?**
A) The Driver ran out of memory broadcasting the dimension table.
B) The hash table for an SHJ exceeded available memory limits.
C) A single executor is forced to process and instantiate objects for a massive number of identically keyed records.
D) Catalyst failed to compile the Whole-Stage CodeGen.
* **Answer:** C
* **Mastery Explanation:** Extreme skew routes too much data to one task. As the iterator processes this massive partition, object churn can overwhelm the Garbage Collector, leading to GC overhead limits being exceeded.

**22. If a Broadcast Hash Join fails with an Executor OOM, what is the most probable cause?**
A) The Driver memory allocation was too low.
B) The broadcasted table, once deserialized into a hash table, exceeded the executor's memory capacity.
C) The network partition between nodes failed.
D) The fact table was too large to fit in memory.
* **Answer:** B
* **Mastery Explanation:** While the fact table streams through memory, the broadcasted dimension table must reside entirely in memory as a hash table on every executor. If it's too large, the executor runs out of memory.

**23. Which join strategy involves hash-partitioning both datasets over the network, and then building an in-memory hash table from the smaller partition?**
A) Broadcast Hash Join
B) Sort Merge Join
C) Shuffle Hash Join
D) Broadcast Nested Loop Join
* **Answer:** C
* **Mastery Explanation:** This is the exact mechanism of a Shuffle Hash Join. It shuffles data like an SMJ but probes a hash table like a BHJ, trading sort time for memory risk.

**24. What is the functional purpose of the "Sort" phase in a Sort Merge Join?**
A) To prepare the data for off-heap caching in Tungsten.
B) To align matching records sequentially so an iterator can step through them linearly without requiring an in-memory hash table.
C) To compress the data prior to the network shuffle.
D) To distribute the data evenly across the cluster to prevent skew.
* **Answer:** B
* **Mastery Explanation:** Sorting places the keys in order. During the merge phase, a pointer simply steps forward through both sorted partitions simultaneously, finding matches highly efficiently with minimal memory overhead.

**25. Based on Code Example 4, what is a highly effective technique for optimizing massive range joins?**
A) Increasing the driver's memory overhead.
B) Binning dates into discrete integers (e.g., year-month) to introduce an artificial equality condition.
C) Forcing a Shuffle Hash Join.
D) Disabling AQE.
* **Answer:** B
* **Mastery Explanation:** Binning creates an artificial key that allows Catalyst to perform an equi-join on the bin. This changes the physical plan from an O(N*M) BNLJ to an efficient SMJ.

## Part 3: Small Twist Scenarios (15)

**26. Scenario:** You are executing an SMJ that suffers from severe data skew on a "null" key, causing the job to hang on a single straggler task. You increase the cluster size from 10 to 100 executors. Does this resolve the issue?
* **Answer:** No.
* **Mastery Explanation:** Hash-partitioning is deterministic. All "null" keys will always hash to the exact same partition, meaning a single task on a single executor will still process the entire skewed workload, leaving 99 executors idle.

**27. Scenario:** You configure a bucketed join. Table A has 100 buckets. Table B has 200 buckets. Both are sorted by `user_id`. What happens to the Exchange phase during an SMJ?
* **Answer:** The Exchange phase is NOT eliminated; a shuffle occurs.
* **Mastery Explanation:** For Catalyst to safely skip the shuffle, the physical layout of the buckets must align perfectly. A mismatch in bucket counts (100 vs 200) forces Spark to reshuffle the data to align the partitions.

**28. Scenario:** You apply a `broadcast()` hint to a 50MB DataFrame. Your executor memory is 4GB, but `spark.driver.maxResultSize` is capped at 20MB. What happens?
* **Answer:** The job fails with a Driver OOM or maxResultSize exception.
* **Mastery Explanation:** Even though the executors have plenty of memory, the Driver must first collect the 50MB DataFrame to broadcast it. It breaches the 20MB limit and crashes before the join even begins.

**29. Scenario:** Your query features an equi-join condition (`A.id == B.id`) combined with a non-equi condition (`AND A.price > B.price`). Does Catalyst degrade the execution to a BNLJ?
* **Answer:** No.
* **Mastery Explanation:** Because at least one equality condition exists, Catalyst can successfully hash-partition and sort the data (SMJ) based on `id`. The non-equi price condition is simply applied as a filter during the merge phase.

**30. Scenario:** Catalyst executes a Shuffle Hash Join (SHJ). The overall dataset is 500GB, partitioned into 2000 tasks. Due to skew, one partition on the build side receives 10GB of data. The executor has 4GB of memory. What occurs?
* **Answer:** The executor crashes with an Out Of Memory error.
* **Mastery Explanation:** Unlike an SMJ which can gracefully spill sorted data to disk, an SHJ must materialize the entire build-side partition into an in-memory hash table. 10GB exceeds the 4GB limit.

**31. Scenario:** You manually salt a skewed fact table using `rand() * 10` (yielding 0-9). For the dimension table, you cross join it with a DataFrame containing IDs 1 through 10. Will the join produce accurate results?
* **Answer:** No, data will be lost.
* **Mastery Explanation:** The fact table contains salt value 0, but the dimension table does not. Fact records salted with 0 will find no match, and dimension records with salt 10 are useless. The salt ranges must match exactly.

**32. Scenario:** You are running an SMJ. You explicitly disable Tungsten's off-heap memory allocation via Spark configurations. Does the SMJ fail?
* **Answer:** No, it continues but with degraded performance.
* **Mastery Explanation:** Spark will fall back to using standard JVM on-heap memory for sorting. The job will still complete, but it will be subjected to severe Garbage Collection pauses and higher memory pressure.

**33. Scenario:** You perform a `LEFT OUTER JOIN`. You apply a `broadcast()` hint to the left table (the driving table). Does Catalyst perform a Broadcast Hash Join?
* **Answer:** No, it ignores the hint and performs an SMJ.
* **Mastery Explanation:** In a left outer join, the right table can be broadcasted. However, broadcasting the left table is unsupported because the hash table probe mechanism cannot guarantee the output of unmatched left rows across distributed partitions.

**34. Scenario:** Your data has a single key that represents 40% of the entire 10TB dataset. You enable Adaptive Query Execution (AQE). Will AQE perfectly balance this workload and eliminate the straggler task?
* **Answer:** No. AQE cannot magically distribute a single massive key across multiple executors for an SMJ.
* **Mastery Explanation:** While AQE can split large partitions, all records for a specific key must still be joined against the corresponding key on the other side. Salting remains the only definitive engineering solution for extreme single-key skew.

**35. Scenario:** You save a bucketed table as Parquet. Later, to optimize a join, you read it using `spark.read.parquet("s3a://path")` instead of `spark.table("table_name")`. Will Catalyst eliminate the shuffle?
* **Answer:** No, a shuffle will occur.
* **Mastery Explanation:** The physical Parquet files do not contain the bucketing metadata. Bucketing metadata is stored in the Hive Metastore. By bypassing `spark.table()`, Catalyst is unaware the data is bucketed.

**36. Scenario:** During an SMJ, the Sort phase is a massive bottleneck. The query joins on `id` but carries a 5MB nested JSON string column in the `SELECT` clause that is not part of the join logic. How does this affect the sort?
* **Answer:** It heavily degrades sort performance and causes excessive disk spillage.
* **Mastery Explanation:** Tungsten must carry the entire payload of the row through the shuffle and sort phases. Massive unneeded columns consume memory, forcing early disk spills and exponentially slowing down the I/O.

**37. Scenario:** A daily ETL job uses a BHJ flawlessly for months. The dimension table grows steadily, eventually reaching 15MB. The `autoBroadcastJoinThreshold` is 10MB. What happens tomorrow?
* **Answer:** Catalyst silently degrades the plan from a BHJ to an SMJ.
* **Mastery Explanation:** Because the table now exceeds the 10MB threshold, Catalyst will no longer auto-broadcast it. This introduces a full network shuffle (Exchange) into the physical plan, causing a sudden performance cliff.

**38. Scenario:** You attempt to join two DataFrames with absolutely no join conditions. The configuration `spark.sql.crossJoin.enabled` is set to `false`. What is the outcome?
* **Answer:** The query immediately fails with an AnalysisException.
* **Mastery Explanation:** Spark restricts Cartesian products by default because an O(N*M) operation on distributed data is typically catastrophic. You must explicitly enable the configuration or use the `crossJoin()` API.

**39. Scenario:** You apply salting to fix skew. You use a salt range of 0 to 999. The dimension table contains 10 million rows. What is the side effect on the dimension table?
* **Answer:** The dimension table inflates to 10 billion rows.
* **Mastery Explanation:** The required cross join replicates the dimension table for every salt value (10M * 1000). This massive data explosion can easily cause OOM errors or network bottlenecks, nullifying the benefits of the salt.

**40. Scenario:** You attempt to optimize a Theta join by binning `event_date` into a `year_month` string. However, promotions span multiple months (e.g., Jan-Mar). If you only bin by the start month, what happens?
* **Answer:** You will lose matches for events that occur in February or March.
* **Mastery Explanation:** Simple equality binning fails for overlapping ranges. If a promotion spans multiple bins, the promotion record must be exploded/replicated into every bin it touches; otherwise, the equi-join condition will exclude valid matches.

## Part 4: Coding & Debugging (10)

**41. Debug the Logic Error:**
```python
df_fact_salted = df_fact.withColumn("salt", (rand() * 10).cast("int"))
df_dim_salted = df_dim.withColumn("salt", (rand() * 10).cast("int"))
joined = df_fact_salted.join(df_dim_salted, ["key", "salt"])
```
* **Answer:** The dimension table uses random salting instead of a deterministic cross join.
* **Mastery Explanation:** A fact record with salt 4 will only match a dimension record if that specific dimension record randomly received salt 4. Massive amounts of valid matches will be lost. The dimension table MUST be replicated via cross join for all salt values.

**42. Debug the Performance Issue:**
```python
df1.write.bucketBy(50, "id").saveAsTable("t1")
df2.write.bucketBy(50, "id").saveAsTable("t2")
spark.table("t1").join(spark.table("t2"), "id").explain()
```
* **Issue:** The physical plan still shows a "Sort" node before the merge phase.
* **Answer:** The tables were bucketed but not pre-sorted.
* **Mastery Explanation:** `bucketBy` only dictates the physical distribution of files. To eliminate the sort phase during an SMJ, the data must also be sorted within the buckets using `.sortBy("id")` prior to calling `saveAsTable`.

**43. Debug the Bottleneck:**
```python
df1.join(df2, df1.timestamp > df2.start_time)
```
* **Issue:** The query takes hours to execute and physical plan shows `BroadcastNestedLoopJoin`.
* **Answer:** This is a Theta join lacking an equality condition.
* **Mastery Explanation:** Without an equi-key, Spark cannot use hash-partitioning. You must introduce an artificial equality condition (like binning timestamps by day) to allow Catalyst to use an SMJ for the coarse join, applying the timestamp range as a filter.

**44. Debug the Crash:**
```python
# df1 is 100GB, df2 is 8GB
joined = df1.join(broadcast(df2), "id")
```
* **Issue:** The Spark application crashes with `java.lang.OutOfMemoryError` on the Driver node.
* **Answer:** The broadcasted table (8GB) exceeds the Driver's memory allocation.
* **Mastery Explanation:** The `broadcast()` hint forces the Driver to collect the 8GB dataset. If the Driver is only allocated 4GB of memory, it crashes instantly. You must increase driver memory or remove the hint and rely on SMJ.

**45. Debug the Executor OOM:**
```python
# Using a hint to force SHJ
joined = df1.join(df2.hint("SHUFFLE_HASH"), "id")
```
* **Issue:** An executor crashes with an OOM error during the join, despite plenty of disk space.
* **Answer:** A skewed partition built an in-memory hash table that exceeded the executor's heap memory.
* **Mastery Explanation:** SHJ does not spill to disk. You must remove the `SHUFFLE_HASH` hint and allow Catalyst to default to a Sort Merge Join (SMJ), which gracefully spills sorted partitions to disk.

**46. Debug the Straggler Task:**
```python
joined = users.join(transactions, "user_id")
```
* **Issue:** 199 tasks finish in 10 seconds. 1 task runs for 30 minutes and eventually crashes.
* **Answer:** Extreme data skew on `user_id`, highly likely due to a massive number of `null` or default values.
* **Mastery Explanation:** If millions of transactions have a null `user_id`, they all hash to the same partition. To fix this, filter out nulls before the join (if they aren't needed), or apply salting techniques.

**47. Debug the Ignored Hint:**
```python
df1.join(broadcast(df2), df1.id == df2.id, "full_outer")
```
* **Issue:** Catalyst completely ignores the `broadcast()` hint and executes an SMJ.
* **Answer:** Full Outer Joins do not support Broadcast Hash Joins.
* **Mastery Explanation:** A Full Outer Join must output unmatched records from both tables. A BHJ only builds a hash table for one side, making it impossible to ascertain which records on the broadcasted side were *not* matched.

**48. Debug the Data Explosion:**
```python
# Dimension table is 500MB
salts = spark.range(0, 10000).withColumnRenamed("id", "salt")
replicated_dim = dim_df.crossJoin(salts)
```
* **Issue:** The job crashes with OOMs and excessive GC overhead while preparing the dimension table.
* **Answer:** The salt range (10,000) is absurdly large for a 500MB dimension table.
* **Mastery Explanation:** The cross join replicates the 500MB table 10,000 times, creating a 5 Terabyte dataset. Salt ranges should only be as large as necessary to distribute the skew (e.g., 10 to 50).

**49. Debug the Shuffle:**
```python
# Tables were saved with saveAsTable("bucketed_users")
users = spark.read.parquet("s3a://warehouse/bucketed_users")
transactions = spark.read.parquet("s3a://warehouse/bucketed_trans")
users.join(transactions, "user_id").explain()
```
* **Issue:** The physical plan shows an `Exchange` (Shuffle) node, even though the tables are bucketed.
* **Answer:** Reading via `spark.read.parquet` bypasses the Hive Metastore.
* **Mastery Explanation:** Catalyst needs the table metadata to know the data is bucketed. You must read the tables using `spark.table("bucketed_users")` so the optimizer recognizes the bucketing and eliminates the shuffle.

**50. Debug the Slow Sort:**
```python
# transactions has a 10MB JSON column 'raw_payload'
joined = users.join(transactions, "user_id")
joined.select("user_id", "transaction_amt").write.parquet("out/")
```
* **Issue:** The Sort phase of the SMJ is extremely slow and spills to disk repeatedly.
* **Answer:** The massive `raw_payload` column is being carried through the shuffle and sort phases unnecessarily.
* **Mastery Explanation:** Even though `raw_payload` is dropped in the final `select`, it is present during the join. You must drop or explicitly select only the required columns *before* the join to reduce memory footprint and disk I/O during sorting.
