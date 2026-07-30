# Elite Assessment: Resilient Distributed Datasets (RDDs)

## Part 1: True/False Questions

**1. True/False:** The Spark `DAGScheduler` splits a single physical stage into multiple stages whenever it encounters a `NarrowDependency` to ensure parallel execution across executors.
**Correct Answer:** False
**Mastery Explanation:** The `DAGScheduler` collapses continuous chains of `NarrowDependency` transformations (like `map`, `filter`) into a single stage (task fusion). It only splits stages when it encounters a `ShuffleDependency` (wide dependency), which requires data to be partitioned and shuffled across the network.

**2. True/False:** Applying the `map` transformation on a key-value RDD automatically strips any custom partitioner (e.g., `HashPartitioner`) attached to the parent RDD, whereas `mapValues` preserves it.
**Correct Answer:** True
**Mastery Explanation:** The `map` transformation allows the user to modify both the key and the value. Because the key might change, the previous partitioning scheme is no longer guaranteed to be valid, so Spark sets the partitioner to `None`. `mapValues` only modifies values, guaranteeing keys stay in their original partitions, thus preserving the partitioner.

**3. True/False:** RDDs utilize Tungsten's off-heap columnar memory format to reduce JVM Garbage Collection (GC) pressure.
**Correct Answer:** False
**Mastery Explanation:** Tungsten's off-heap columnar memory management and Catalyst optimizations are exclusive to the DataFrame and Dataset APIs. Raw RDDs store data as standard JVM objects, making them heavily susceptible to GC overhead.

**4. True/False:** If `MEMORY_AND_DISK_SER` is used, Spark will write evicted memory blocks to local disk in their serialized format, avoiding the CPU cost of re-serialization during eviction.
**Correct Answer:** True
**Mastery Explanation:** When using `_SER` storage levels, the RDD is kept serialized in memory. When blocks are evicted to disk due to memory pressure, they are already serialized, so Spark simply flushes the byte buffers directly to disk without CPU overhead.

**5. True/False:** `groupByKey` performs a map-side combine before shuffling data across the network, making it just as efficient as `reduceByKey` for large datasets.
**Correct Answer:** False
**Mastery Explanation:** `groupByKey` does *not* perform map-side combine. It shuffles all raw values across the network for each key, leading to massive network I/O and potential OutOfMemoryErrors on reducers. `reduceByKey` and `aggregateByKey` perform map-side aggregation, significantly reducing shuffle volume.

**6. True/False:** Two RDDs partitioned by `new HashPartitioner(100)` and `new HashPartitioner(200)` respectively will execute a shuffle-free join because they both use the HashPartitioner class.
**Correct Answer:** False
**Mastery Explanation:** For the DAGScheduler to classify a join as a `NarrowDependency` (shuffle-free), both the partitioner class AND the number of partitions must be identical. `Partitioner.equals()` requires both to match. 

**7. True/False:** Calling `coalesce(10)` on an RDD with 100 partitions always triggers a full network shuffle to redistribute the data evenly.
**Correct Answer:** False
**Mastery Explanation:** `coalesce` defaults to `shuffle = false` and simply merges local partitions on the same executor without moving data across the network. It causes uneven partition sizes but avoids shuffle cost. `repartition(10)` is the equivalent of `coalesce(10, shuffle = true)`.

**8. True/False:** By default, calling `rdd.unpersist()` is a non-blocking fire-and-forget operation, meaning blocks might still reside in the BlockManager momentarily while downstream jobs start.
**Correct Answer:** True
**Mastery Explanation:** Without `blocking = true`, `unpersist` does not wait for the `BlockManagerMaster` to confirm that all executor `BlockManager` instances have dropped the blocks. This can cause unexpected memory pressure for immediately subsequent jobs.

**9. True/False:** RDD lineage provides fault tolerance by replicating data across multiple HDFS nodes after every transformation.
**Correct Answer:** False
**Mastery Explanation:** RDDs achieve fault tolerance through *lineage*, logging the graph of transformations used to build a dataset. If a partition is lost, Spark recomputes it from the parent RDDs, rather than relying on storage replication (unless explicitly checkpointed or cached with replication like `MEMORY_ONLY_2`).

**10. True/False:** If you configure `spark.serializer` to `KryoSerializer`, Spark will automatically register all custom user classes for optimal serialization performance.
**Correct Answer:** False
**Mastery Explanation:** While Spark will *use* Kryo, it does not automatically register your custom classes unless you explicitly register them via `spark.kryo.classesToRegister`. Unregistered classes fall back to writing full class names for every object, bloating the serialized data.

## Part 2: Multiple Choice Questions

**11. Which of the following operations is virtually guaranteed to trigger a `ShuffleDependency` (Wide Dependency)?**
A) `mapPartitions`
B) `filter`
C) `reduceByKey`
D) `union`
**Correct Answer:** C
**Mastery Explanation:** `reduceByKey` requires all values for a specific key to be co-located on the same executor to apply the reduce function, requiring a network shuffle of data. `mapPartitions`, `filter`, and `union` (typically) are narrow dependencies.

**12. When choosing between `MEMORY_ONLY_SER` and `MEMORY_ONLY`, what is the primary tradeoff?**
A) `MEMORY_ONLY_SER` uses more memory but reduces CPU usage.
B) `MEMORY_ONLY_SER` uses less memory (reduces GC pressure) but increases CPU usage during reads/writes.
C) `MEMORY_ONLY_SER` writes to disk faster than `MEMORY_ONLY`.
D) There is no difference; `_SER` is a deprecated flag.
**Correct Answer:** B
**Mastery Explanation:** Serialized caching stores partitions as large byte arrays, significantly reducing the object count and thereby minimizing JVM garbage collection overhead. However, it costs CPU cycles to serialize and deserialize the data upon access.

**13. A Spark job fails with an OutOfMemoryError during a `join` operation between a massive event RDD and a small reference RDD (10 MB). Both are RDDs (not DataFrames). What is the optimal RDD-native solution?**
A) Increase `spark.executor.memory`.
B) Use a Broadcast Variable for the small RDD and use `mapPartitions` to do a manual map-side join.
C) Repartition both RDDs to a higher number of partitions.
D) Switch the join to a `leftOuterJoin`.
**Correct Answer:** B
**Mastery Explanation:** RDDs do not have a built-in Catalyst optimizer to automatically convert a standard `join` into a Broadcast Hash Join. To achieve this, you must explicitly `sc.broadcast` the small dataset and use `map` or `mapPartitions` on the large RDD to perform the lookup locally, entirely avoiding the shuffle that caused the OOM.

**14. What determines if two RDDs can be joined without a shuffle?**
A) If both RDDs have the exact same number of partitions.
B) If both RDDs reside in `MEMORY_ONLY`.
C) If both RDDs share the exact same `Partitioner` instance (or logically equal partitioners) and partition count.
D) It is impossible to join two RDDs without a shuffle.
**Correct Answer:** C
**Mastery Explanation:** The `DAGScheduler` checks `rdd1.partitioner == rdd2.partitioner`. If they match (e.g., same `HashPartitioner` with the same number of partitions), it infers that keys are co-located and processes the join as a `NarrowDependency` (specifically a `CoGroupedRDD` with `ZippedPartitionsRDD`).

**15. In iterative graph algorithms (like PageRank) implemented with RDDs, what is the most critical optimization to apply to the static edge/node structure RDD?**
A) Persisting it with `MEMORY_AND_DISK`.
B) Calling `partitionBy()` with a custom partitioner and then `persist()`.
C) Calling `checkpoint()` after every iteration.
D) Using `groupByKey()` to group edges.
**Correct Answer:** B
**Mastery Explanation:** By applying a `HashPartitioner` via `partitionBy()` and then caching the RDD, you physically align the data. In subsequent loops, joins against this RDD will be shuffle-free because the DAGScheduler recognizes the stable partitioner, reducing network I/O from $O(N)$ iterations to $O(1)$.

**16. How does RDD `checkpointing` differ from `persist()`/`cache()`?**
A) Checkpointing truncates the RDD lineage graph and writes data reliably to a distributed file system (e.g., HDFS).
B) Checkpointing keeps data strictly in executor memory.
C) Checkpointing is faster than caching because it doesn't serialize data.
D) Checkpointing happens synchronously during the transformation phase without an action.
**Correct Answer:** A
**Mastery Explanation:** `persist()` caches data but keeps the lineage intact (if nodes die, lineage is used to recompute). `checkpoint()` saves the RDD to a reliable filesystem (HDFS/S3) and *removes* the lineage graph completely. It is heavily used in very long iterative algorithms to prevent StackOverflow errors from massive DAGs.

**17. You execute `rdd.map(x => (x.id, x.value)).partitionBy(new HashPartitioner(100)).map(x => (x._1, x._2 * 2)).join(otherRdd)`. Why will this join trigger a shuffle?**
A) `HashPartitioner` cannot be used with `join`.
B) The second `map` operation clears the `HashPartitioner`.
C) `otherRdd` was not explicitly cached.
D) Joins always trigger a shuffle regardless of partitioners.
**Correct Answer:** B
**Mastery Explanation:** Because `map` can alter keys, Spark automatically strips the `partitioner` metadata from the resulting RDD. To maintain the partitioner, `mapValues(v => v * 2)` must be used. Because the partitioner is lost, the `join` triggers a full shuffle.

**18. What is the consequence of setting `spark.rdd.compress` to `true` (default)?**
A) All RDD data in memory is compressed using Snappy.
B) Serialized RDD partitions (like `MEMORY_ONLY_SER`) are compressed, trading slight CPU overhead for significant space savings.
C) It compresses the shuffle map output files.
D) It compresses broadcast variables.
**Correct Answer:** B
**Mastery Explanation:** `spark.rdd.compress` specifically applies compression to serialized RDD caches. `spark.shuffle.compress` handles shuffle output, and `spark.broadcast.compress` handles broadcasts.

**19. Why might `aggregateByKey` perform better than `reduceByKey` in certain scenarios?**
A) It avoids the map-side combine phase.
B) It allows the accumulator type (U) to be different from the input value type (V), avoiding object allocation overheads like creating single-element lists for every input record.
C) It automatically applies a HashPartitioner.
D) It runs off-heap via Tungsten.
**Correct Answer:** B
**Mastery Explanation:** While both perform map-side combines, `aggregateByKey` requires a zero value and allows a different return type. For example, if you want to output a `Set`, `reduceByKey` forces you to map every input value into a 1-element `Set` first (massive GC overhead), whereas `aggregateByKey` lets you fold raw values directly into an accumulating `Set`.

**20. When an RDD partition computation fails due to a transient network error, who is responsible for retrying it?**
A) `DAGScheduler`
B) `TaskScheduler`
C) `BlockManager`
D) `ShuffleManager`
**Correct Answer:** B
**Mastery Explanation:** The `TaskScheduler` is responsible for sending tasks to executors and handles low-level task failures (e.g., transient network issues, executor death) by retrying the task up to `spark.task.maxFailures`. If a stage fails completely (e.g., missing shuffle map output), it gets pushed back up to the `DAGScheduler`.

**21. What happens if you call an action on an RDD that has been `unpersist()`ed?**
A) Spark throws an `IllegalStateException`.
B) The action returns empty results.
C) Spark silently ignores the action.
D) Spark recomputes the RDD from its lineage.
**Correct Answer:** D
**Mastery Explanation:** RDDs are immutable and lazily evaluated based on lineage. `unpersist()` just removes the cached blocks from the `BlockManager`. If an action is called again, Spark simply traverses the DAG and recalculates the data from the original source.

**22. Which RDD operation is most likely to cause a `StackOverflowError` on the driver if called inside a loop of 10,000 iterations?**
A) `rdd.count()`
B) `rdd.map(f)`
C) `rdd.checkpoint()`
D) `rdd.persist()`
**Correct Answer:** B
**Mastery Explanation:** `map` is a transformation. Doing it in a loop without forcing materialization (or checkpointing) builds a massive unexecuted DAG inside the Driver's JVM. When an action is finally called, the `DAGScheduler` recursively traverses the lineage using depth-first search, blowing the JVM call stack.

**23. You have an RDD with 10 partitions. You perform `rdd.repartition(100)`. How many shuffle files (map outputs) are generated assuming 10 executors?**
A) 10
B) 100
C) 1000
D) 10000
**Correct Answer:** A
**Mastery Explanation:** During a shuffle, each map task (10 partitions = 10 map tasks) writes one data file and one index file (in `SortShuffleManager`). The data file contains regions for all 100 reduce partitions. Therefore, exactly 10 map output data files are generated.

**24. In the context of RDDs, what is a "Shuffle Fetch Failed" exception usually indicative of?**
A) The Driver ran out of memory.
B) An Executor hosting shuffle map output died (e.g., OOM), so the reducing Executor cannot fetch its required partition data.
C) The `mapPartitions` code threw a NullPointerException.
D) The RDD lineage is too long.
**Correct Answer:** B
**Mastery Explanation:** A fetch failure means a Reducer task tried to pull its partition data from a remote `BlockManager`, but the network call failed. This almost always means the remote Executor crashed (usually OOM) and the `DAGScheduler` must resubmit the previous stage to regenerate the lost shuffle files.

**25. Which serializer does Spark use by default for RDD shuffling if no configuration is provided?**
A) KryoSerializer
B) JavaSerializer
C) TungstenSerializer
D) AvroSerializer
**Correct Answer:** B
**Mastery Explanation:** For raw RDDs, the default is `JavaSerializer`. Kryo must be explicitly enabled via `spark.serializer = org.apache.spark.serializer.KryoSerializer`. (Note: Spark uses Kryo by default internally for *some* things like shuffling DataFrames, but for custom RDD objects, it defaults to Java unless specified).

## Part 3: "Small Twist" Questions

**26. Scenario:** You have `rdd1.join(rdd2)`. Both are partitioned with `new HashPartitioner(100)`. The join is shuffle-free. 
**Twist:** You change `rdd1` to use `new RangePartitioner(100, rdd1)`. What happens to the join?
**Answer:** It becomes a Wide Dependency and triggers a full shuffle.
**Mastery Explanation:** `HashPartitioner` and `RangePartitioner` are different classes. `Partitioner.equals()` evaluates to false. Spark cannot guarantee that a key 'X' will live on partition 'Y' for both datasets, so it must perform a shuffle to co-locate them.

**27. Scenario:** You cache an RDD: `rdd.persist(StorageLevel.MEMORY_ONLY)`. It fits entirely in memory. Subsequent actions are fast.
**Twist:** You change it to `rdd.persist(StorageLevel.DISK_ONLY)`. What happens to the lineage graph?
**Answer:** The lineage graph is preserved.
**Mastery Explanation:** Caching (even to disk) does not truncate lineage. If the disk fails or the node goes down, Spark still knows how to rebuild the RDD from source. Only `checkpoint()` truncates lineage. The reads will just be much slower due to disk I/O.

**28. Scenario:** You are aggregating user clicks with `rdd.reduceByKey(_ + _)`.
**Twist:** You notice severe data skew where user "bot1" has 99% of the clicks, causing one executor to OOM. What is the immediate RDD-based fix?
**Answer:** Salt the keys.
**Mastery Explanation:** Map the keys to append a random integer: `map(k => (k + "_" + Random.nextInt(10), v))`, apply `reduceByKey`, then strip the salt and apply a second `reduceByKey`. This forces the map-side combines to distribute across multiple reducers for the skewed key.

**29. Scenario:** You write `rdd.mapValues(v => v * 2).join(otherRdd)`. It executes efficiently without a shuffle.
**Twist:** You change it to `rdd.map(kv => (kv._1, kv._2 * 2)).join(otherRdd)`. What happens?
**Answer:** A full shuffle occurs.
**Mastery Explanation:** `map` drops the partitioner metadata (sets it to `None`) because the compiler cannot guarantee you didn't modify the key `kv._1`. The `DAGScheduler` sees `None` and schedules a `ShuffleDependency`.

**30. Scenario:** You call `rdd.coalesce(10)` on an RDD with 1000 partitions. It runs instantly without network overhead.
**Twist:** You call `rdd.coalesce(2000)` on the same RDD. What happens?
**Answer:** The partition count remains exactly 1000.
**Mastery Explanation:** `coalesce` with `shuffle = false` (default) can only merge partitions. It cannot split them. If the target number is greater than the current number, it does nothing. To increase partitions, you must use `repartition(2000)` (which forces a shuffle).

**31. Scenario:** You implement a Broadcast variable: `val b = sc.broadcast(map)`. Inside your RDD map function, you use `b.value.get(key)`.
**Twist:** Inside the loop, you re-assign `b = sc.broadcast(newMap)`. Do executors immediately see the `newMap` for the currently running RDD transformation?
**Answer:** No.
**Mastery Explanation:** Broadcast variables are shipped to executors alongside the task closure. Once tasks are running on the executor, they use the broadcast instance serialized with their closure. Updating the reference on the driver has no effect on currently running tasks.

**32. Scenario:** You run `rdd.count()` and it takes 10 minutes. You call `rdd.cache()` and run `rdd.count()` again.
**Twist:** The second `rdd.count()` still takes 10 minutes. Why?
**Answer:** `cache()` is lazy.
**Mastery Explanation:** Calling `cache()` on an RDD does not immediately materialize it. It merely marks it for caching. The *first* action after `cache()` (the second `count()`) executes the computation and caches it. A *third* `count()` would take milliseconds.

**33. Scenario:** You configure `spark.serializer = KryoSerializer`.
**Twist:** You forget to configure `spark.kryo.classesToRegister`. What happens at runtime?
**Answer:** The job succeeds, but uses more memory/network than necessary.
**Mastery Explanation:** Spark will still use Kryo, but because the classes aren't registered, Kryo writes the fully qualified class name string alongside every single serialized object, defeating much of the performance and space benefits.

**34. Scenario:** An RDD pipeline is: `rdd.filter(...).map(...).reduceByKey(...)`. The DAG shows 2 stages.
**Twist:** You add a `.sortByKey()` at the end. How many stages exist now?
**Answer:** 3 stages.
**Mastery Explanation:** `reduceByKey` requires a shuffle (Wide Dependency) -> Stage 1 & 2. `sortByKey` requires *another* shuffle ( specifically a RangePartitioner shuffle) -> Stage 3. Each shuffle boundary increments the stage count.

**35. Scenario:** You read an HDFS file into an RDD. `rdd.partitions.size` is 100 (based on HDFS block size).
**Twist:** The file is a GZIP compressed text file (`.gz`). What is `rdd.partitions.size`?
**Answer:** 1 partition.
**Mastery Explanation:** Standard GZIP is not splittable. Hadoop/Spark cannot calculate byte boundaries for chunks within the gzip stream, so it is forced to read the entire file using a single task (partition), leading to severe parallelism bottlenecks.

**36. Scenario:** You use `rdd.unpersist()` to free memory.
**Twist:** You change it to `rdd.unpersist(blocking = true)`. What changes?
**Answer:** The driver pauses execution until all executors confirm the memory blocks are deleted.
**Mastery Explanation:** By default, `unpersist` sends async RPC messages. If a subsequent heavy job starts instantly, the blocks might still occupy RAM, causing OOM. `blocking = true` makes the RPC synchronous, ensuring the JVM heap is clean before the next line of Driver code runs.

**37. Scenario:** You are joining a large `eventRdd` with a small `userRdd` using RDD joins.
**Twist:** You convert them to DataFrames and do `eventDf.join(userDf)`. Why is the DF version fundamentally faster?
**Answer:** Catalyst optimizer and Tungsten execution.
**Mastery Explanation:** Catalyst will detect the size of `userDf` and automatically rewrite the physical plan to a `BroadcastHashJoin`, eliminating the shuffle entirely. Tungsten will process the rows in columnar format off-heap. RDDs execute naive SortMerge/Hash shuffles and create heavy JVM objects.

**38. Scenario:** Your `reduceByKey(_ + _)` job works perfectly.
**Twist:** You change it to `groupByKey().mapValues(_.sum)`. What resource metric spikes in the Spark UI?
**Answer:** Shuffle Write Bytes (and Shuffle Read).
**Mastery Explanation:** `reduceByKey` combines values on the map-side, writing only one aggregated record per key per partition to the shuffle files. `groupByKey` sends *every single raw record* across the network, exponentially increasing shuffle I/O and disk spillage.

**39. Scenario:** You call `rdd.checkpoint()`. The checkpoint is saved to HDFS.
**Twist:** The driver program crashes and you restart the application using `spark-submit`. Can you recover the RDD from the HDFS checkpoint?
**Answer:** No, not automatically via RDD API.
**Mastery Explanation:** RDD checkpointing is tied to the `SparkContext` of the running application. If the application terminates, the context is lost. While the files remain in HDFS, you cannot magically re-attach them to an RDD in a *new* application using the old checkpoint metadata. You would have to manually read them via `sc.objectFile()`.

**40. Scenario:** You partition an RDD: `val pRdd = rdd.partitionBy(new HashPartitioner(100))`
**Twist:** You do not call `.persist()` on `pRdd`. You use it in a loop: `(1 to 10).foreach(i => pRdd.join(otherRdd(i)).count())`. What happens?
**Answer:** `partitionBy` executes 10 times, causing 10 redundant shuffles.
**Mastery Explanation:** `partitionBy` is a transformation that triggers a shuffle. Because RDDs are lazy and recomputed from lineage on every action, the `partitionBy` shuffle is re-executed on every loop iteration. You MUST `persist()` an RDD after custom partitioning for iterative algorithms.

## Part 4: Coding & Debugging Questions

**41. Debug this snippet:**
```scala
class EventProcessor {
  val threshold = 100
  def process(rdd: RDD[Int]): RDD[Int] = {
    rdd.filter(x => x > threshold)
  }
}
```
**Error:** `TaskNotSerializableException`.
**Mastery Explanation:** The closure `x => x > threshold` captures the `threshold` variable. Because `threshold` is a member of `EventProcessor`, Spark must serialize the entire `EventProcessor` instance and send it to executors. If `EventProcessor` is not serializable, it crashes. Fix: Assign `val localThreshold = threshold` inside the method, then use `localThreshold` in the filter.

**42. Analyze Performance:**
```scala
val rdd = sc.parallelize(1 to 10000000)
val result = rdd.map(x => (x % 10, x)).groupByKey().collect()
```
**Error:** Severe Network Shuffle & OOM risk on Reducers.
**Mastery Explanation:** 10 million records are being sent over the network to only 10 distinct keys. The reducer for a key will have to construct an iterable of 1 million integers in JVM heap, likely causing OutOfMemory. Fix: Use `aggregateByKey` or `reduceByKey` depending on the desired final aggregation.

**43. Fix the Missing Optimizer:**
```scala
val smallRdd = sc.parallelize(Seq((1, "A"), (2, "B")))
val massiveRdd = // RDD with 100 TB of data
val joined = massiveRdd.join(smallRdd)
```
**Error:** Full network shuffle for 100 TB of data just to join 2 records.
**Mastery Explanation:** RDDs do not auto-broadcast. Fix:
```scala
val smallMap = sc.broadcast(smallRdd.collectAsMap())
val joined = massiveRdd.flatMap { case (k, v) => 
  smallMap.value.get(k).map(smallV => (k, (v, smallV))) 
}
```

**44. Debug the Lineage:**
```scala
var rdd = sc.parallelize(1 to 100)
for (i <- 1 to 10000) {
  rdd = rdd.map(_ + 1)
}
rdd.count()
```
**Error:** Driver JVM `StackOverflowError`.
**Mastery Explanation:** The loop builds an RDD lineage DAG 10,000 layers deep. When `count()` is called, the `DAGScheduler` evaluates the lineage recursively using DFS, blowing the JVM stack. Fix: Periodically call `rdd.checkpoint()` (e.g., every 500 iterations) to truncate the lineage.

**45. Diagnose the Partitioner:**
```scala
val p1 = new HashPartitioner(10)
val p2 = new HashPartitioner(20)
val rdd1 = sc.parallelize(Seq((1, 1))).partitionBy(p1)
val rdd2 = sc.parallelize(Seq((1, 1))).partitionBy(p2)
rdd1.join(rdd2).count()
```
**Error:** Unnecessary Shuffle.
**Mastery Explanation:** Because the partition counts differ (10 vs 20), `p1.equals(p2)` is false. Spark falls back to a Wide Dependency and shuffles both datasets. Fix: Ensure both RDDs use the exact same `HashPartitioner` count.

**46. Fix the Cache Leak:**
```scala
val df = sc.textFile("hdfs://...")
val cachedDf = df.map(parse).persist(StorageLevel.MEMORY_ONLY)
cachedDf.count()
// ... 2 hours of other Spark jobs ...
```
**Error:** The memory is held indefinitely, starving the next 2 hours of jobs.
**Mastery Explanation:** The blocks remain in `BlockManager` until forced out by LRU eviction. This causes heavy GC and memory pressure for subsequent operations. Fix: Explicitly call `cachedDf.unpersist(blocking = true)` immediately after it is no longer needed.

**47. Analyze the Skew:**
```scala
// rdd: (city_name, person_data) -> 10 billion records, but 90% are from "New York"
val counts = rdd.mapValues(_ => 1).reduceByKey(_ + _)
```
**Error:** Executor handling "New York" partition takes 5 hours; others take 2 minutes (Straggler problem).
**Mastery Explanation:** Hash partitioning routes all "New York" keys to a single task. Fix: Salt the key before reduce, then reduce again.
```scala
rdd.map(kv => (kv._1 + "_" + scala.util.Random.nextInt(100), 1))
   .reduceByKey(_ + _)
   .map(kv => (kv._1.split("_")(0), kv._2))
   .reduceByKey(_ + _)
```

**48. Identify the Bad Serialization:**
```scala
case class Data(id: Int, payload: String)
val conf = new SparkConf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
val sc = new SparkContext(conf)
val rdd = sc.parallelize(1 to 1000).map(i => Data(i, "val"))
rdd.persist(StorageLevel.MEMORY_ONLY_SER).count()
```
**Error:** Bloated serialized footprint.
**Mastery Explanation:** The user enabled Kryo but forgot to register `Data`. Kryo will store the fully qualified class name `com.package.Data` for every single object in the serialized byte array. Fix: Add `.registerKryoClasses(Array(classOf[Data]))` to `SparkConf`.

**49. Debug the File Output:**
```scala
val rdd = sc.parallelize(1 to 1000, 1000)
rdd.saveAsTextFile("hdfs://output")
```
**Error:** Small files problem (1000 tiny files generated on HDFS).
**Mastery Explanation:** `parallelize` created 1000 partitions. `saveAsTextFile` maps 1:1 with partitions, creating 1000 output files. Writing massive amounts of small files destroys HDFS NameNode performance. Fix: `rdd.coalesce(10).saveAsTextFile(...)` to merge data into 10 partitions locally before writing.

**50. Unmask the Narrow Dependency:**
```scala
val rdd = sc.textFile("data.txt")
val mapped = rdd.map(_.split(","))
val keyed = mapped.map(arr => (arr(0), arr(1)))
val filtered = keyed.filter(_._1 == "ERROR")
filtered.count()
```
**Question:** How many stages does this DAG have?
**Answer:** Exactly 1 stage.
**Mastery Explanation:** `textFile`, `map`, and `filter` are all `NarrowDependency` operations. The `DAGScheduler` fuses all of them into a single Stage (task fusion). No shuffle data is written to disk. The data streams directly from HDFS through the map/filter logic in a single pass per partition.
