# 🔥 Master Class Assessment: Shuffling

## Part 1: True/False (10 Questions)

**1. T/F: In modern Spark versions, map tasks during the Shuffle Write phase produce one file per reduce partition to ensure efficient network fetching.**
* **Answer:** False
* **Mastery Explanation:** Hash Shuffle produced one file per reduce partition (M * R files), causing massive file descriptor exhaustion. Modern Sort-Based Shuffle produces just one single data file per map task along with one index file that tracks byte offsets for the reduce partitions.

**2. T/F: `coalesce()` always avoids shuffling entirely, regardless of whether you are increasing or decreasing the number of partitions.**
* **Answer:** False
* **Mastery Explanation:** `coalesce()` only avoids shuffling when decreasing the number of partitions by merging local partitions. If you attempt to increase partitions with `coalesce()`, it will do nothing or require a shuffle (if `shuffle=True` is passed).

**3. T/F: During Shuffle Read, reduce tasks query the `MapOutputTracker` on the Driver to find the physical locations of the data blocks they need.**
* **Answer:** True
* **Mastery Explanation:** The MapOutputTrackerMaster on the Driver maintains the directory of block locations. Reduce tasks query it to know which remote Executor's BlockManager to connect to via the BlockTransferService.

**4. T/F: A massive discrepancy between "Shuffle Read Size" and "Spill (Disk)" in the Spark UI typically indicates that `spark.executor.memory` is too high and should be reduced.**
* **Answer:** False
* **Mastery Explanation:** A large Spill (Disk) indicates that the allocated execution memory is too *low* for the size of the partition being processed, forcing the ExternalSorter or ExternalAppendOnlyMap to aggressively flush to disk to avoid an OOM.

**5. T/F: `reduceByKey` reduces network I/O compared to `groupByKey` by utilizing an `ExternalAppendOnlyMap` on the map side to perform partial aggregation before the Shuffle Write phase finalizes.**
* **Answer:** True
* **Mastery Explanation:** `reduceByKey` leverages map-side combiners. It aggregates local data in memory before writing to the shuffle spill files, drastically shrinking the payload sent across the network.

**6. T/F: Salting a heavily skewed DataFrame requires the smaller dimension DataFrame to be exploded by the exact same salt space factor to guarantee accurate join results.**
* **Answer:** True
* **Mastery Explanation:** If the large table's keys are randomized across 20 buckets, the small table must have every row duplicated 20 times (one for each salt) so that the hash partitioner routes matching keys to the same executor.

**7. T/F: Tungsten's off-heap memory entirely eliminates the physical disk I/O penalty when shuffle data spills, because the data remains in RAM outside the JVM.**
* **Answer:** False
* **Mastery Explanation:** Tungsten off-heap memory bypasses JVM Garbage Collection overhead. However, when the off-heap execution memory is exhausted, Spark must still serialize and flush the data to the physical local disk OS filesystem.

**8. T/F: Setting `spark.sql.shuffle.partitions` to an extremely high number like 1,000,000 is universally beneficial as it guarantees tiny partition sizes and zero disk spills.**
* **Answer:** False
* **Mastery Explanation:** Creating millions of partitions generates massive scheduling overhead for the DAGScheduler, overloads the MapOutputTracker with metadata, and causes the creation of millions of tiny files, destroying performance.

**9. T/F: `BroadcastHashJoin` operates by the Driver pulling the entire smaller DataFrame into its memory, then sending a copy to every Executor's JVM heap via the `BlockManager`.**
* **Answer:** True
* **Mastery Explanation:** This mechanism bypasses the network-heavy SortMergeJoin by materializing the small table on the Driver and distributing it globally, preventing any all-to-all shuffle phase.

**10. T/F: The `SortShuffleManager` uses a `RoundRobin` partitioner by default for `repartition(n)` operations to evenly distribute records across the cluster.**
* **Answer:** True
* **Mastery Explanation:** When explicitly repartitioning without a key, Spark forces a full shuffle using a RoundRobin algorithm to ensure all resulting partitions are exactly equal in size.

---

## Part 2: Multiple Choice Questions (15 Questions)

**11. Which component is responsible for orchestrating the network fetch of shuffle blocks during the Shuffle Read phase?**
A) MapOutputTracker
B) BlockTransferService
C) DAGScheduler
D) ExternalSorter
* **Answer:** B
* **Mastery Explanation:** The BlockTransferService (running on Netty) actually moves the bytes over the network. The MapOutputTracker only acts as the directory to find the IPs.

**12. When dealing with a 10TB dataset, if `spark.sql.shuffle.partitions` is left at the default of 200, what is the most likely failure mode during a wide transformation?**
A) Driver OOM due to MapOutputTracker overload
B) Executor OOM or agonizing GC pauses due to massive disk spills
C) Network timeout from Netty
D) File descriptor exhaustion during Shuffle Write
* **Answer:** B
* **Mastery Explanation:** 10TB / 200 = 50GB per reduce task. No standard Executor heap can hold this. The `ExternalAppendOnlyMap` will thrash the GC and constantly spill to disk, eventually crashing the Executor.

**13. What is the primary purpose of the index file generated during the Shuffle Write phase?**
A) To sort the keys within the data file
B) To track the byte offsets for each target reduce partition within the single data file
C) To map Executor IPs to BlockManagers
D) To store the partially aggregated results for reduceByKey
* **Answer:** B
* **Mastery Explanation:** Sort-Based Shuffle consolidates map outputs into a single data file. The index file allows remote reduce tasks to randomly seek and stream only their specific block of data.

**14. How does Catalyst optimize a join if one of the tables is smaller than `spark.sql.autoBroadcastJoinThreshold`?**
A) It uses an ExternalSorter to perform a map-side combine
B) It triggers a SortMergeJoin with salting
C) It substitutes the SortMergeJoin with a BroadcastHashJoin, bypassing the shuffle entirely
D) It increases spark.sql.shuffle.partitions automatically
* **Answer:** C
* **Mastery Explanation:** By copying the small table to all nodes, Catalyst eliminates the need to shuffle the massive fact table, saving immense network and disk I/O.

**15. What data structure is utilized during the Shuffle Read side for final aggregations or sorting if the data exceeds memory capacity?**
A) ExternalSorter
B) ExternalAppendOnlyMap
C) RoundRobinPartitioner
D) BlockManagerMaster
* **Answer:** B
* **Mastery Explanation:** Reduce tasks use the `ExternalAppendOnlyMap` to aggregate data as it streams in from the network. If it fills up, it spills to the local disk.

**16. Why does `groupByKey` often cause OutOfMemoryErrors on skewed data?**
A) It performs an inefficient map-side combine
B) It blindly streams all raw records across the network without map-side reduction
C) It forces the use of a BroadcastHashJoin
D) It requires the Driver to collect all data before shuffling
* **Answer:** B
* **Mastery Explanation:** Without map-side aggregation, every single record is serialized and sent to a single reduce task, easily blowing up the JVM heap if millions of identical keys exist.

**17. Which of the following operations has O(1) per-partition complexity and strictly avoids shuffling?**
A) repartition()
B) reduceByKey()
C) coalesce()
D) SortMergeJoin
* **Answer:** C
* **Mastery Explanation:** `coalesce()` shrinks partition counts by mapping multiple parent partitions to a single child partition on the same node, bypassing the network.

**18. If you increase `spark.memory.fraction` from 0.6 to 0.8, what is the expected impact on the shuffle process?**
A) More memory is allocated to user storage, increasing disk spills
B) More memory is allocated to the execution pool, giving ExternalSorter more room before spilling
C) The MapOutputTracker can track more blocks
D) Broadcast joins can handle larger tables
* **Answer:** B
* **Mastery Explanation:** The execution memory pool is used by shuffle data structures. Increasing the fraction steals memory from User/Storage, delaying the point at which shuffle tasks must spill to disk.

**19. In a SortMergeJoin, what condition MUST be met before the actual join can occur on the reduce side?**
A) One DataFrame must be broadcasted
B) Both DataFrames must be hash-partitioned and sorted by the join key
C) Keys must be salted
D) The DAGScheduler must merge the DataFrames into a single Stage
* **Answer:** B
* **Mastery Explanation:** SortMergeJoin physically requires both sides of the join to be aligned via HashPartitioning and sorted in memory/disk so the algorithm can stream through them efficiently.

**20. What is the fundamental mechanism Catalyst uses to route identical keys to the same reduce task?**
A) Map-side combiner
B) RoundRobin Partitioner
C) Hash Partitioner
D) MapOutputTracker
* **Answer:** C
* **Mastery Explanation:** Spark runs a deterministic hash function on the join/group key and applies modulo arithmetic (`hash(key) % numPartitions`) to guarantee identical keys land on the same node.

**21. What happens when the execution memory pool limit is breached during the Shuffle Write phase?**
A) Spark throws an OutOfMemoryError and crashes the job
B) Spark triggers the BlockTransferService to send data early
C) Spark aggressively spills sorted data from the ExternalSorter to the local disk
D) Spark requests more memory from the Driver
* **Answer:** C
* **Mastery Explanation:** To prevent the JVM from crashing, Spark flushes the in-memory buffer to disk as intermediate spill files, which are later merged.

**22. Which Spark UI metric serves as a critical red flag indicating that partitions are too large for allocated Executor memory?**
A) High Shuffle Write Time
B) Massive discrepancy between Shuffle Read Size and Spill (Disk)
C) High GC Time on the Driver
D) BlockManager connection timeouts
* **Answer:** B
* **Mastery Explanation:** If Shuffle Read is 5GB but Spill (Disk) is 50GB, it means the task uncompressed the data, ran out of RAM, and repeatedly thrashed the local disk to finish the sort/aggregation.

**23. When employing key salting to fix a straggler problem, what trade-off is introduced?**
A) The small dimension table must be exploded, increasing its memory footprint
B) The Catalyst optimizer disables predicate pushdown
C) The large fact table must be broadcasted
D) The job is forced to use RDDs instead of DataFrames
* **Answer:** A
* **Mastery Explanation:** To match the randomized salted keys in the large table, the small table must be Cartesian-exploded by the salt array, consuming more RAM.

**24. Under modern Spark architecture, what protocol/framework does the BlockTransferService use for network fetches?**
A) gRPC
B) REST HTTP
C) Netty
D) Akka
* **Answer:** C
* **Mastery Explanation:** Spark replaced Akka with Netty for all data-plane block transfers due to Netty's superior performance with massive, asynchronous binary streams and zero-copy capabilities.

**25. What is the primary reason legacy Hash Shuffle (one file per reduce task) was replaced by Sort-Based Shuffle?**
A) Hash Shuffle could not perform map-side combines
B) Sort-Based Shuffle prevents data skew
C) Hash Shuffle caused massive file descriptor exhaustion and I/O thrashing on the local disk
D) Hash Shuffle required more network bandwidth
* **Answer:** C
* **Mastery Explanation:** With 10,000 map tasks and 10,000 reduce tasks, Hash Shuffle created 100,000,000 files simultaneously. Sort-Based Shuffle creates only 20,000 files total in the same scenario.

---

## Part 3: Small Twist Questions (15 Questions)

**26. Scenario: You are tuning a pipeline that joins a 1TB table and a 10MB table. You add a `broadcast()` hint, but it STILL shuffles 1TB of data. What minor config change is overriding your hint?**
A) `spark.sql.shuffle.partitions` is too high
B) `spark.sql.autoBroadcastJoinThreshold` was explicitly set to -1
C) The tables are not cached
D) `spark.memory.fraction` is too low
* **Answer:** B
* **Mastery Explanation:** Setting the threshold to -1 globally disables BroadcastHashJoins, and in many Spark versions, this strictly overrides user hints, forcing a SortMergeJoin.

**27. Scenario: A map task is processing a massive dataset and its `ExternalSorter` fills up. Twist: Instead of getting an OOM, the task succeeds but takes 10x longer. Why?**
A) It switched to a Broadcast Join
B) It spilled to local disk multiple times and incurred heavy disk I/O and serialization overhead
C) It sent the data to the Driver
D) It repartitioned the data automatically
* **Answer:** B
* **Mastery Explanation:** Spark's safety net is spilling. It prevents crashes but introduces brutal disk latency.

**28. Scenario: You join two DataFrames on a skewed key. You add salt to the large DataFrame (random 0-19). Twist: You forget to explode the small DataFrame. What happens?**
A) The job OOMs
B) The join succeeds but drops approximately 95% of the matching records
C) Catalyst automatically explodes the small DataFrame
D) The shuffle falls back to HashShuffle
* **Answer:** B
* **Mastery Explanation:** The large table keys now have "_0" to "_19" appended. If the small table keys are unaltered, almost nothing will match, resulting in massive silent data loss.

**29. Scenario: You set `spark.sql.shuffle.partitions = 200000` for a 50GB dataset. Twist: The job slows to a crawl despite having no disk spills. Why?**
A) The Driver's MapOutputTracker is overwhelmed by metadata, and task scheduling overhead dominates
B) The Netty buffer overflows
C) Catalyst switches to a Cartesian product
D) The Executors run out of execution memory
* **Answer:** A
* **Mastery Explanation:** 200,000 tasks for 50GB means each task processes ~250KB. The Driver spends more time serializing tasks and tracking metadata than the Executors spend processing data.

**30. Scenario: You use `coalesce(10)` to reduce partitions from 1000 to 10. Twist: You change it to `repartition(10)`. What is the architectural impact?**
A) Both do the same thing, no impact
B) `coalesce` triggers a full shuffle, `repartition` does not
C) `repartition` forces a full cross-cluster shuffle using a RoundRobin partitioner, radically increasing network I/O
D) `repartition` uses map-side combines
* **Answer:** C
* **Mastery Explanation:** `repartition()` strictly invokes a shuffle boundary. `coalesce()` shrinks partitions on the same node without network transfer.

**31. Scenario: You use `rand()` as a salt without bounding it (e.g., floor or integer cast). What happens during the join?**
A) The join completes perfectly but takes longer
B) The join produces near-zero matches because random floats on the large table will never exactly match the exploded floats on the small table
C) Spark throws an exception
D) Catalyst defaults to a broadcast join
* **Answer:** B
* **Mastery Explanation:** Floating-point exact matching is highly improbable. Salting requires discrete integers.

**32. Scenario: Your executors have 8GB of memory. Twist: Tungsten off-heap memory is enabled and set to 12GB. Why does the task still exhibit high latency during the Shuffle Write phase when processing a 30GB block?**
A) Off-heap memory is strictly for the Driver
B) The `ExternalSorter` must still serialize and write the sorted data to the local OS filesystem as spill files, incurring physical disk I/O
C) Netty cannot read off-heap memory
D) The hash partitioner ignores off-heap memory
* **Answer:** B
* **Mastery Explanation:** Off-heap memory prevents JVM Garbage Collection, but it does not magically prevent disk writes when the off-heap limit is exceeded.

**33. Scenario: You are querying a 10TB table and run `.count()`. Twist: The Spark UI shows zero shuffle data. Why?**
A) `count()` always triggers a shuffle, the UI is bugged
B) `count()` aggregates counts locally on each map partition, and the Driver simply sums the final integers
C) The data was already sorted on disk
D) BlockManager cached the results
* **Answer:** B
* **Mastery Explanation:** Not all aggregations require a full shuffle. A simple count maps to a local sum, and only the scalars are returned to the Driver.

**34. Scenario: Two tasks finish in 5 seconds. One task takes 4 hours. Twist: You check the "Shuffle Read Size" in the UI for the 4-hour task, and it's exactly the same as the 5-second tasks. What is happening?**
A) Data skew on the join key
B) Computational skew (e.g., an expensive UDF or complex regex) is occurring on specific records, not data volume skew
C) The MapOutputTracker is hung
D) The network connection dropped
* **Answer:** B
* **Mastery Explanation:** If the data volume (Shuffle Read Size) is identical, the hash partitioner did its job perfectly. The delay is caused by CPU-bound operations on specific edge-case rows.

**35. Scenario: You run a BroadcastHashJoin. The small table is 8MB on disk. Twist: The job OOMs on the Executors. How is this possible if the threshold is 10MB?**
A) Parquet compression means the 8MB file uncompresses into Gigabytes of Java objects in memory, blowing up the Executor heap
B) Broadcast joins still shuffle data
C) The Driver memory was too small
D) The threshold only applies to ORC files
* **Answer:** A
* **Mastery Explanation:** Spark evaluates the size of the uncompressed data in memory, not the highly compressed Parquet size on disk. An 8MB Parquet file can easily exceed heap limits when deserialized into Java objects.

**36. Scenario: You use `coalesce(100)` on a 10,000 partition DataFrame right after reading it from S3. Twist: The job takes 10x longer to read the data. Why?**
A) Coalesce forces a full shuffle
B) Coalesce merges partitions logically, which forces the upstream read stage to only use 100 tasks to read the massive dataset, bottlenecking parallelism
C) S3 doesn't support coalesce
D) It triggers a Broadcast Join
* **Answer:** B
* **Mastery Explanation:** Because `coalesce()` does not shuffle, Catalyst pushes the partition reduction up the DAG. Only 100 tasks are spun up to pull data from S3 instead of 10,000.

**37. Scenario: A Shuffle Read phase fails repeatedly with `FetchFailedException`. Twist: The network is perfectly healthy. What happened on the map side?**
A) The MapOutputTracker crashed
B) The Executor that performed the map task experienced an OOM and died, taking the BlockManager and the shuffle spill files down with it
C) The reduce task requested the wrong partition ID
D) The Hash Partitioner changed its algorithm
* **Answer:** B
* **Mastery Explanation:** `FetchFailedException` almost always means the target Node/Executor is dead, usually because the preceding map task pushed it into an OOM state.

**38. Scenario: You set `spark.sql.shuffle.partitions` to 200. You are joining two 100MB DataFrames. Twist: The job is incredibly slow. Why?**
A) 200 partitions is too few for 100MB
B) 200 partitions means each task processes just 0.5MB, causing excessive scheduling overhead and small file creation for no benefit
C) SortMergeJoin requires at least 1000 partitions
D) BroadcastHashJoin was disabled
* **Answer:** B
* **Mastery Explanation:** Over-partitioning small datasets destroys performance. The ideal partition size is 100MB-200MB.

**39. Scenario: You execute `df.repartition("colA").groupBy("colA").count()`. What happens to the physical plan?**
A) It performs two full shuffles
B) It bypasses the shuffle
C) Catalyst detects the partitioning, so the groupBy does not trigger a second shuffle, resulting in only one shuffle
D) It performs a map-side combine without shuffling
* **Answer:** C
* **Mastery Explanation:** Catalyst's `EnsureRequirements` rule recognizes that the data is already partitioned by `colA` and removes the secondary Exchange (shuffle) operator.

**40. Scenario: You increase `spark.executor.memory` from 4GB to 32GB to avoid spills. Twist: You leave `spark.memory.fraction` at 0.1. Does it help?**
A) Yes, 32GB is always enough
B) No, because only 10% of the heap (3.2GB) is allocated for execution and storage, meaning the `ExternalSorter` will still spill constantly
C) Yes, because memory fraction only affects Broadcast joins
D) No, because spills are based on disk size
* **Answer:** B
* **Mastery Explanation:** Throwing RAM at Spark does nothing if the internal Memory Manager config restricts the execution pool to a tiny fraction of the heap.

---

## Part 4: Coding & Debugging (10 Questions)

**41. Debugging: What is the logic error blocking the broadcast optimization?**
```python
df_large = spark.read.parquet("1tb_data")
df_small = spark.read.parquet("5mb_data")
df_joined = df_large.join(broadcast(df_small), "id", "full_outer")
```
A) `broadcast()` hint cannot be used with Parquet
B) Full Outer Join does not support BroadcastHashJoin. It will fallback to SortMergeJoin and shuffle 1TB.
C) The hint syntax is wrong
D) `df_small` is too large to broadcast
* **Answer:** B
* **Mastery Explanation:** Broadcast joins rely on hash lookups. In a Full Outer Join, Spark must track matches from both sides simultaneously, which is impossible in a distributed broadcast architecture. It falls back to SortMergeJoin.

**42. Debugging: Why will this code likely crash on a cluster?**
```python
rdd = sc.parallelize(range(10000000), 100)
rdd_kv = rdd.map(lambda x: (x % 10, x))
result = rdd_kv.groupByKey().mapValues(sum).collect()
```
A) `sum` is not a valid Python function
B) `groupByKey` forces all values for each of the 10 keys into a massive list in memory during the shuffle read before applying `sum`, causing OOM
C) `mapValues` triggers a second shuffle
D) 100 partitions is too many
* **Answer:** B
* **Mastery Explanation:** `groupByKey` provides no map-side combiner. It shuffles all raw integers across the network and attempts to build an iterator holding millions of elements in RAM on a single Executor.

**43. Debugging: Identify the physical execution flaw.**
```python
df = spark.read.parquet("events")
df = df.repartition(1000)
df = df.coalesce(10)
df.write.parquet("out")
```
A) `coalesce` fails on 1000 partitions
B) The `repartition` causes a massive network shuffle to 1000 nodes, and `coalesce` immediately bottlenecks the write to 10 nodes, combining high network I/O with low write parallelism
C) Parquet cannot be written with 10 partitions
D) Catalyst will optimize both away
* **Answer:** B
* **Mastery Explanation:** This is an anti-pattern. You pay the maximum penalty for a cross-cluster shuffle, only to immediately funnel the data into 10 single-threaded writers.

**44. Debugging: What is the inevitable result of this code?**
```python
spark.conf.set("spark.sql.shuffle.partitions", "200")
df = spark.read.parquet("100_terabytes_of_data")
df_agg = df.groupBy("category").count()
```
A) The job completes quickly due to Catalyst
B) The job fails with an Executor OOM or disk space exhaustion because each of the 200 reduce tasks must process 500GB of shuffle data
C) The job uses BroadcastHashJoin
D) The Driver runs out of memory
* **Answer:** B
* **Mastery Explanation:** The default 200 partitions is lethal for large datasets. Tungsten cannot process 500GB blocks per task without catastrophic spilling and disk exhaustion.

**45. Debugging: What is the logic error in this salting implementation?**
```python
df_skewed = spark.read.parquet("skewed")
df_dim = spark.read.parquet("dim")
df_skewed = df_skewed.withColumn("salt", rand())
df_dim = df_dim.withColumn("salt", rand())
df_joined = df_skewed.join(df_dim, ["key", "salt"])
```
A) `rand()` returns an integer
B) Random floats on both sides will never match; the dimension table must be exploded with the finite array of all possible salt values
C) Salting only works for RDDs
D) The join type must be outer
* **Answer:** B
* **Mastery Explanation:** Appending a random float to both sides breaks the join condition. Salting requires assigning discrete integer buckets to the large table and exploding the small table by that exact discrete array.

**46. Debugging: Assuming "id" is heavily skewed, why might the `cache()` command exacerbate memory issues?**
```python
df1 = spark.table("huge_table_A")
df2 = spark.table("huge_table_B")
df_joined = df1.join(df2, "id")
df_joined.cache()
df_joined.count()
df_joined.groupBy("category").count().show()
```
A) `cache()` clears the disk spills
B) `cache()` forces the massive, skewed partition to be materialized entirely in the Executor's storage memory, starving the execution memory pool and causing the subsequent `groupBy` shuffle to spill immediately
C) `cache()` disables Catalyst
D) `cache()` triggers an extra shuffle
* **Answer:** B
* **Mastery Explanation:** Spark's Unified Memory Manager dynamically shares boundaries between execution and storage. Filling storage RAM with skewed cached data guarantees the `ExternalSorter` has no room to breathe during the shuffle.

**47. Debugging: Why does `repartition` here ruin the pipeline?**
```python
def complex_logic(rows):
    import time
    time.sleep(0.1) # Simulate heavy NLP model
    return [r for r in rows]

rdd.repartition(2000).mapPartitions(complex_logic).count()
```
A) `repartition` changes the schema
B) It forces an all-to-all network shuffle just to redistribute data before a map operation, whereas `coalesce` or doing it on existing partitions would avoid the network completely
C) `mapPartitions` requires 1 partition
D) `count()` does not work with `mapPartitions`
* **Answer:** B
* **Mastery Explanation:** Never shuffle data solely to increase parallelism for a map task unless absolutely necessary. The network I/O penalty usually outweighs the compute gains.

**48. Debugging: Assuming the tables were pre-bucketed and sorted in Hive, what happens to the physical plan here?**
```python
df1 = spark.read.parquet("bucketed_data1") 
df2 = spark.read.parquet("bucketed_data2") 
df1.join(df2, "user_id").write.parquet("out")
```
A) It performs a bucketed map-join
B) Reading directly from Parquet bypasses the metastore's bucketing metadata, forcing Spark to perform a full Sort-Merge Join with a massive shuffle, completely wasting the bucketing optimization
C) It crashes because bucketed tables cannot be read via `read.parquet`
D) It performs a Broadcast join
* **Answer:** B
* **Mastery Explanation:** Spark can only leverage bucketing to avoid shuffles if it reads through the Hive Metastore (e.g., `spark.table("bucketed_table")`). Reading raw files strips all metadata.

**49. Debugging: Why does this commonly crash on massive datasets?**
```python
df = spark.read.csv("100gb_data.csv")
df_sorted = df.orderBy("timestamp")
df_sorted.write.csv("out")
```
A) `orderBy` uses RangePartitioner, which samples the data. If the sample is skewed, it routes massive amounts of data to a few reduce tasks, causing OOMs and huge disk spills during the shuffle.
B) `orderBy` forces all data to the Driver.
C) `orderBy` requires a BroadcastHashJoin.
D) The data must be salted.
* **Answer:** A
* **Mastery Explanation:** Global sorts require a full shuffle using the RangePartitioner. If the timestamp data is skewed (e.g., a massive spike in events on a single day), the partition holding that range will blow up.

**50. Debugging: While setting partitions to 1 works for this tiny data, why is it fatal for a 1TB dataset in production?**
```python
df = spark.createDataFrame([("A", 1), ("B", 2)], ["key", "val"])
spark.conf.set("spark.sql.shuffle.partitions", "1")
df.groupBy("key").count().show()
```
A) It limits the map tasks to 1
B) It forces the entire 1TB to be shuffled to a single Executor JVM, guaranteeing an immediate OutOfMemoryError and completely nullifying distributed processing
C) It forces a Broadcast join
D) It disables Tungsten
* **Answer:** B
* **Mastery Explanation:** Setting shuffle partitions to 1 forces all output from all map tasks to be funneled into exactly one reduce task on one Executor, effectively making your 100-node cluster act like a single laptop.
