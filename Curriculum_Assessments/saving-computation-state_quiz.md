# Apache Spark: Saving Computation State Assessment

## Section 1: True/False Questions

1. **Question:** Caching a DataFrame in Spark using the default storage level stores the data as raw JVM objects on the heap, similarly to RDDs.
**Answer:** False
**Mastery Explanation:** DataFrames utilize the Tungsten execution engine which stores cached data in a highly optimized, flat binary format within off-heap memory, bypassing the JVM GC overhead. Raw RDDs default to JVM objects on the heap.

2. **Question:** Checkpointing a DataFrame completely truncates its logical and physical lineage, permanently discarding the previous operations from the DAG.
**Answer:** True
**Mastery Explanation:** Unlike caching which retains the lineage in the DAGScheduler for resilience, checkpointing physically writes data to storage and creates a new `ReliableCheckpointRDD`, permanently severing the previous lineage to prevent StackOverflowErrors.

3. **Question:** When memory is exhausted and a cached block is evicted under the LRU policy, the DAGScheduler immediately reschedules tasks to recompute it.
**Answer:** False
**Mastery Explanation:** The DAGScheduler only schedules recomputation if and when a subsequent job stage actually requires that evicted block, not immediately upon eviction.

4. **Question:** Using `localCheckpoint(eager=false)` ensures that if an executor dies, the locally checkpointed data can be seamlessly recovered from another executor's local disk.
**Answer:** False
**Mastery Explanation:** `localCheckpoint` writes to the local disk of the executing worker. If that node dies, the data is irretrievably lost, and because the lineage was severed, the entire job will fail.

5. **Question:** The BlockManagerMaster resides solely on the Driver JVM and coordinates all cached blocks across the cluster.
**Answer:** True
**Mastery Explanation:** The BlockManagerMaster is the central registry on the Driver that keeps track of block locations to ensure data locality for downstream tasks.

6. **Question:** Applying `persist(StorageLevel.MEMORY_AND_DISK)` immediately triggers the physical execution of the preceding DAG to materialize the cache.
**Answer:** False
**Mastery Explanation:** `persist()` is a lazy operation. It only intercepts the plan; actual materialization is deferred until a terminal action is invoked.

7. **Question:** `MEMORY_ONLY_SER` is mathematically guaranteed to use less CPU during reads compared to `MEMORY_ONLY` because the data is compressed.
**Answer:** False
**Mastery Explanation:** `MEMORY_ONLY_SER` uses more CPU during reads because it must deserialize the byte arrays back into objects. Its advantage is reduced GC overhead and smaller memory footprint, not lower CPU usage on read.

8. **Question:** By default, calling `checkpoint()` on an RDD will result in a double computation of the lineage if an action hasn't materialized it beforehand.
**Answer:** True
**Mastery Explanation:** Checkpointing itself triggers a job to write the data, but if you don't cache/persist the RDD before checkpointing, the lineage is computed once for the action you call, and again for the checkpoint write.

9. **Question:** Tungsten's binary format allows for vectorized reads directly on cached blocks utilizing CPU SIMD instructions.
**Answer:** True
**Mastery Explanation:** Because Tungsten stores data off-heap in a flat columnar format, it avoids JVM object overhead and allows modern CPUs to perform SIMD operations directly on the memory.

10. **Question:** Calling `unpersist()` is merely a polite gesture in Spark and does not actively affect application performance since the LRU cache automatically handles eviction.
**Answer:** False
**Mastery Explanation:** While LRU evicts data eventually, actively calling `unpersist()` prevents silent memory leaks, immediately freeing up highly contended storage memory for subsequent application phases, thereby avoiding unnecessary disk spills.

## Section 2: Multiple Choice Questions

11. **Question:** Which Catalyst optimizer issue is `localCheckpoint()` most directly intended to solve?
A) Insufficient network bandwidth for shuffle read
B) Slow logical plan generation due to deeply nested iterative lineages
C) Off-heap memory fragmentation
D) Kryo serializer buffer overflow
**Answer:** B
**Mastery Explanation:** The Catalyst optimizer can take minutes to generate a physical plan when the logical lineage is massive. `localCheckpoint()` forcefully truncates this logical plan, vastly accelerating downstream query planning without the HDFS write penalty.

12. **Question:** When an executor crashes, what happens to data cached with `MEMORY_ONLY_2`?
A) The Driver crashes because it lost communication.
B) The DAGScheduler automatically recomputes the blocks.
C) The downstream tasks read from the replicated blocks on another executor's memory.
D) The blocks are recovered from HDFS.
**Answer:** C
**Mastery Explanation:** `_2` storage levels synchronously replicate blocks to a peer executor. If one executor dies, the BlockManager Master immediately directs tasks to the replica, bypassing DAG recomputation entirely.

13. **Question:** Why does caching large raw RDDs (not DataFrames) with `MEMORY_ONLY` often cause cluster failure?
A) It uses Java Serialization by default, causing massive CPU spikes.
B) Raw JVM objects have massive object headers, leading to GC thrashing and OOM.
C) Tungsten automatically spills it to disk too quickly.
D) The DAG linege grows infinitely.
**Answer:** B
**Mastery Explanation:** Raw JVM objects have a large footprint (headers, alignment). Millions of them overwhelm the JVM Garbage Collector, causing massive GC pauses that lead to heartbeat timeouts and node failures.

14. **Question:** What happens if `spark.memory.fraction` is fully consumed by Execution memory and Storage memory tries to cache a new block?
A) Execution memory is evicted.
B) The new block is dropped or spilled to disk depending on StorageLevel.
C) The JVM throws an OutOfMemoryError.
D) The Spark application hangs until Execution memory clears.
**Answer:** B
**Mastery Explanation:** Execution memory has priority. If it occupies the space, Storage cannot evict Execution blocks. The attempted cache block will either not be stored or will spill to disk (if `_AND_DISK` is set).

15. **Question:** How does checkpointing differ from caching in terms of fault tolerance?
A) Caching survives application restarts; checkpointing does not.
B) Checkpointing relies on DAG recomputation; caching does not.
C) Checkpointing survives application restarts and Driver failures by storing data reliably; caching relies on cluster memory and DAG lineage.
D) There is no difference.
**Answer:** C
**Mastery Explanation:** Checkpoints are written to HDFS/S3 and survive the application lifecycle. Caching is tied to the live executors and the Driver's BlockManager Master.

16. **Question:** In an iterative PageRank algorithm, why is standard `checkpoint()` preferred over `persist()`?
A) It is faster to write to HDFS.
B) It prevents Driver JVM StackOverflowErrors by truncating the ever-growing DAG lineage.
C) It uses Kryo serialization by default.
D) It prevents executors from running out of disk space.
**Answer:** B
**Mastery Explanation:** Iterative loops create massive DAGs. During planning, the Driver recursively traverses this DAG. Too many iterations cause a StackOverflow. Checkpointing severs the DAG.

17. **Question:** Which component physically writes data to disk when `MEMORY_AND_DISK` triggers a spill?
A) DAGScheduler
B) TaskRunner
C) DiskStore (via BlockManager)
D) ShuffleManager
**Answer:** C
**Mastery Explanation:** The BlockManager contains a MemoryStore and a DiskStore. When memory is full, the DiskStore handles writing the evicted blocks to local executor disks.

18. **Question:** What is the primary advantage of Tungsten's off-heap memory for caching?
A) It is replicated automatically.
B) It circumvents the JVM Garbage Collector completely.
C) It avoids network shuffles.
D) It uses standard Java serialization.
**Answer:** B
**Mastery Explanation:** Off-heap memory is managed directly by Spark via Unsafe APIs, not the JVM. This prevents the GC from scanning millions of objects, eliminating GC thrashing.

19. **Question:** If you apply `df.cache()` and `df` is a Dataset/DataFrame, what is the default underlying StorageLevel?
A) MEMORY_ONLY
B) MEMORY_AND_DISK
C) MEMORY_ONLY_SER
D) DISK_ONLY
**Answer:** B
**Mastery Explanation:** DataFrames use `MEMORY_AND_DISK` by default (unlike RDDs which use `MEMORY_ONLY`), storing data in Tungsten format and gracefully spilling if needed.

20. **Question:** What is a symptom of "Cache Thrashing"?
A) High network I/O from HDFS.
B) The cluster spending more CPU cycles calculating, evicting, and recalculating the same blocks.
C) The Driver JVM running out of heap space.
D) The Spark UI showing 0% storage utilization.
**Answer:** B
**Mastery Explanation:** When the cache is too small for the active working set, blocks are constantly evicted. Subsequent stages require them, forcing recomputation. This cycle destroys performance.

21. **Question:** When configuring Kryo serialization for RDD caching, why is `spark.kryo.registrationRequired` critical?
A) It prevents Spark from starting.
B) It forces developers to register classes, avoiding the massive overhead of writing full class names for every object serialized.
C) It automatically registers Scala core classes.
D) It converts RDDs to DataFrames.
**Answer:** B
**Mastery Explanation:** If a class isn't registered, Kryo writes the fully qualified class name for *every single object*, completely negating the memory benefits of serialization.

22. **Question:** What happens if you call `checkpoint()` on a DataFrame without setting a checkpoint directory?
A) It defaults to the local file system.
B) It defaults to the Spark temp directory.
C) The application throws a SparkException.
D) It silently falls back to `cache()`.
**Answer:** C
**Mastery Explanation:** Spark requires an explicitly defined reliable storage directory (like HDFS or S3) via `SparkContext.setCheckpointDir()` before `checkpoint()` can be invoked.

23. **Question:** Why must an action (like `.count()`) be called immediately after `.checkpoint()`?
A) To clear the Catalyst optimizer cache.
B) Because `checkpoint()` is lazy and the truncation won't occur until a physical job writes the data.
C) To unpersist previous caches.
D) To trigger garbage collection.
**Answer:** B
**Mastery Explanation:** Like transformations, checkpointing is lazy. Without an action, the data is never materialized to storage, and the lineage graph continues to grow indefinitely.

24. **Question:** In the context of Spark memory management, what does the eviction policy target first?
A) RDD lineage graphs.
B) Checkpoint files on HDFS.
C) Least Recently Used (LRU) data blocks in the BlockManager.
D) Broadcast variables.
**Answer:** C
**Mastery Explanation:** When storage memory is full, the BlockManager uses a strict LRU policy to evict the oldest, least accessed blocks to make room for new ones.

25. **Question:** Which serialization format does Tungsten use to cache DataFrames off-heap?
A) Java Native Serialization
B) Kryo
C) Parquet
D) Its own highly optimized flat binary format
**Answer:** D
**Mastery Explanation:** Tungsten doesn't use Kryo or Java serialization; it uses a custom, Unsafe-based columnar binary format optimized for CPU cache locality and direct SIMD processing.

## Section 3: Small Twist Questions

26. **Scenario:** You have an RDD cached with `MEMORY_ONLY`. You change it to `MEMORY_ONLY_SER`.
**Twist:** What happens to the CPU utilization during the *first* action that materializes the cache?
**Answer:** CPU utilization INCREASES.
**Mastery Explanation:** Materializing the cache with `_SER` requires Spark to actively serialize the raw objects into byte arrays before storing them, consuming more CPU cycles during the initial write.

27. **Scenario:** You have a DataFrame cached with `df.cache()`. You change it to `df.persist(StorageLevel.MEMORY_ONLY)`.
**Twist:** How does the disk spill behavior change if memory is exceeded?
**Answer:** It will no longer spill to disk; evicted blocks are permanently dropped and must be recomputed.
**Mastery Explanation:** `df.cache()` uses `MEMORY_AND_DISK`. Changing to `MEMORY_ONLY` explicitly removes the DiskStore fallback, meaning memory exhaustion leads to pure eviction and recomputation.

28. **Scenario:** An iterative algorithm uses `rdd.checkpoint()` every 10 iterations.
**Twist:** You change `rdd.checkpoint()` to `rdd.localCheckpoint()`. What happens when a spot-instance worker node terminates randomly?
**Answer:** The entire job fails and crashes.
**Mastery Explanation:** `localCheckpoint()` writes to local executor disks and truncates the DAG. If the node dies, the local disk is gone. Because the DAG was severed, Spark cannot recompute the lost partitions, causing a fatal job failure.

29. **Scenario:** You cache a DataFrame using `df.persist()`.
**Twist:** You accidentally call `df.persist()` a second time on the same DataFrame reference later in the code.
**Answer:** Spark throws an exception (or ignores it depending on version), as you cannot change or re-persist an already persisted Dataset without unpersisting first.
**Mastery Explanation:** Spark strictly prevents changing the StorageLevel of an RDD/DataFrame once it has been assigned, to prevent cluster-state inconsistencies.

30. **Scenario:** You call `df.cache()` and immediately write to Parquet.
**Twist:** You then apply a `df.filter().count()`. Does the filter read from the cache?
**Answer:** Yes.
**Mastery Explanation:** The Parquet write acts as the terminal action that materializes the cache. Subsequent actions, like the filter count, will hit the BlockManager and read the cached Tungsten blocks.

31. **Scenario:** You use `df.persist(StorageLevel.DISK_ONLY)`.
**Twist:** You replace your SSD cluster nodes with cheap HDD nodes. What is the impact on cache read speed?
**Answer:** Performance degrades massively due to terrible random read IOPS.
**Mastery Explanation:** `DISK_ONLY` forces all cached blocks to the DiskStore. Reading from HDDs incurs massive seek time penalties compared to SSDs or memory, effectively making the cache slower than just recomputing from a fast source.

32. **Scenario:** You register Kryo classes for RDD serialization.
**Twist:** You forget to set `spark.kryo.registrationRequired = true` and miss one heavily nested custom class.
**Answer:** Kryo silently falls back to writing the fully qualified class name for every instance of that missing class, silently bloating memory usage.
**Mastery Explanation:** Without enforcing registration, Kryo's fallback negates its primary benefit. The memory footprint will balloon unexpectedly, often leading to hidden GC issues.

33. **Scenario:** You have an execution-heavy stage that requires 80% of cluster memory for shuffles.
**Twist:** You aggressively cache a massive dataset right before it. What happens to the cache?
**Answer:** It is immediately evicted.
**Mastery Explanation:** Execution memory has immunity and can evict Storage memory if Storage exceeds its unevictable boundary (usually 50% of the total memory fraction). The cache will be instantly flushed to make room for the shuffle.

34. **Scenario:** You write a pipeline: `val df2 = df.map(...); df2.cache(); df2.count(); df2.unpersist(); df2.show()`
**Twist:** What does `df2.show()` compute from?
**Answer:** It computes entirely from the original source.
**Mastery Explanation:** `unpersist()` immediately drops the blocks from the BlockManager. The subsequent `show()` action will find no cached blocks and will force the DAGScheduler to recompute the entire lineage.

35. **Scenario:** You are checkpointing an RDD.
**Twist:** You add `.persist(StorageLevel.MEMORY_AND_DISK)` immediately *before* calling `.checkpoint()`.
**Answer:** The checkpoint write will be much faster, and the lineage is only computed once.
**Mastery Explanation:** By persisting before checkpointing, the action that triggers the checkpoint reads from the cached memory rather than recomputing the lineage twice (once for the action, once for the checkpoint file write).

36. **Scenario:** You use `localCheckpoint()` on a DataFrame.
**Twist:** You change `eager=true` to `eager=false`.
**Answer:** The logical plan is truncated, but the physical write to local disk is deferred until the next action.
**Mastery Explanation:** `eager=true` acts like an embedded action. `eager=false` makes it lazy. If no action is called, the local checkpoint is never actually materialized.

37. **Scenario:** You have a heavily partitioned DataFrame (10,000 partitions).
**Twist:** You call `.cache()` and then `.count()`.
**Answer:** The BlockManager creates 10,000 separate cached blocks, potentially overwhelming the BlockManagerMaster's RPC endpoints with block status updates.
**Mastery Explanation:** Every partition becomes a distinct block. Massive partition counts cause a metadata storm on the Driver's BlockManagerMaster, leading to RPC timeouts.

38. **Scenario:** You are using `MEMORY_ONLY_SER` with Kryo.
**Twist:** You switch from an RDD to a DataFrame.
**Answer:** The Kryo serialization configuration becomes largely irrelevant for the caching layer.
**Mastery Explanation:** DataFrames completely bypass JVM object caching and Kryo serialization, utilizing Tungsten's native Unsafe columnar format instead.

39. **Scenario:** You configure `spark.memory.storageFraction = 0.9`.
**Twist:** You run a massive broadcast join.
**Answer:** The job might fail with OOM.
**Mastery Explanation:** By forcing storage to reserve 90% of memory, Execution memory (needed for unrolling the broadcast variable) is starved, leading to execution OOMs because execution cannot evict the reserved storage space.

40. **Scenario:** You call `df.cache()`.
**Twist:** The Catalyst optimizer determines that pushing down a filter to the Parquet source is cheaper than reading the massive un-filtered cache.
**Answer:** False. Catalyst does NOT optimize around user caches.
**Mastery Explanation:** Once cached, Spark blindly reads from the BlockManager. If you cache a raw table and filter later, Spark will scan the massive cache in memory rather than pushing the predicate down to the efficient Parquet source.

## Section 4: Coding & Debugging Questions

41. **Code Snippet:**
```scala
val data = spark.read.csv("data.csv")
data.checkpoint()
val res1 = data.filter($"age" > 30).count()
```
**Bug:** The application throws an error immediately at `checkpoint()`.
**Fix:** Add `spark.sparkContext.setCheckpointDir("hdfs://path/")` before checkpointing. Checkpointing requires a configured distributed storage path.

42. **Code Snippet:**
```scala
var graph = initialGraph
for(i <- 1 to 100) {
  graph = performComplexJoin(graph)
  if(i % 10 == 0) graph.checkpoint()
}
graph.write.parquet("out")
```
**Bug:** Driver crashes with `StackOverflowError` during physical planning of the parquet write.
**Fix:** `checkpoint()` is lazy. Add an action like `graph.count()` right after `graph.checkpoint()` inside the loop to force materialization and actual truncation.

43. **Code Snippet:**
```scala
val df = massiveData.repartition(2000)
df.persist(StorageLevel.MEMORY_ONLY)
df.count()
```
**Bug:** Executors are dying with `java.lang.OutOfMemoryError: Java heap space`.
**Fix:** Change to `StorageLevel.MEMORY_AND_DISK`. The dataset exceeds available memory, and `MEMORY_ONLY` has no disk fallback, leading to OOM or catastrophic eviction depending on execution memory pressure.

44. **Code Snippet:**
```scala
val rdd = sc.textFile("logs.txt").map(parseCustomObject)
rdd.persist(StorageLevel.MEMORY_ONLY_SER)
rdd.count()
```
**Bug:** Memory usage is still extremely high, and GC pauses are occurring.
**Fix:** The default serializer is Java Serialization. You must configure `spark.serializer` to use `org.apache.spark.serializer.KryoSerializer` and register `CustomObject` to actually get the benefits of `_SER`.

45. **Code Snippet:**
```scala
val df = spark.table("huge_table").cache()
df.filter($"region" === "US").write.parquet("us_out")
df.filter($"region" === "EU").write.parquet("eu_out")
```
**Bug:** The first parquet write takes 2 hours. The second takes 2 hours. The cache isn't helping.
**Fix:** The cache is lazy and is materialized *during* the first write. But if `huge_table` is larger than memory, the first write fills memory, spills, or evicts. You should filter *before* caching if you only need specific regions, or use `MEMORY_AND_DISK`. Also, Catalyst predicate pushdown is blocked by the cache.

46. **Code Snippet:**
```scala
val df1 = process1()
df1.cache().count()
val df2 = process2(df1)
df2.cache().count()
```
**Bug:** The application runs out of storage memory quickly over time.
**Fix:** `df1` is never explicitly removed from the BlockManager. Add `df1.unpersist()` after `df2` is fully materialized to free up the storage memory and prevent eviction cascades.

47. **Code Snippet:**
```scala
val localDF = complexQuery.localCheckpoint()
val result = localDF.join(otherTable).count()
```
**Bug:** Random task failures cause the entire job to crash with "BlockNotFoundException".
**Fix:** `localCheckpoint` stores data on the ephemeral executor disk. If an executor dies, the data is permanently lost. Use standard `.checkpoint()` if high fault-tolerance is required in a volatile cluster environment.

48. **Code Snippet:**
```scala
val df = spark.read.parquet("data").cache()
df.createOrReplaceTempView("cached_table")
spark.sql("SELECT * FROM cached_table").show()
```
**Bug:** The SQL query does not appear to use the cached data in the Spark UI.
**Fix:** `df.cache()` caches the specific DataFrame object. To cache a table for SQL, use `spark.catalog.cacheTable("cached_table")` or `CACHE TABLE cached_table` in SQL.

49. **Code Snippet:**
```scala
val df = spark.range(1, 1000000000)
df.persist(StorageLevel.DISK_ONLY)
df.count()
```
**Bug:** The operation is incredibly slow, slower than just reading the range.
**Fix:** `DISK_ONLY` forces the entire dataframe to be serialized and written to local disk. For trivial generation like `range()`, recomputing is orders of magnitude faster than Disk I/O. Remove the persist.

50. **Code Snippet:**
```scala
val baseDF = readData()
baseDF.checkpoint(eager=false)
baseDF.count()
baseDF.count()
```
**Bug:** The data is recomputed twice from the source.
**Fix:** Checkpointing does not automatically cache the data in memory. It writes to HDFS. The first count computes and writes the checkpoint. The second count reads from HDFS. To avoid recomputing the lineage for the checkpoint write itself, call `baseDF.cache()` *before* `baseDF.checkpoint()`.
