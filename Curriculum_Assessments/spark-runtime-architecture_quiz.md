# Spark Runtime Architecture - Elite Assessment

## Section 1: True/False Questions (10)

**1. Tungsten's off-heap memory management makes Spark entirely immune to OutOfMemory (OOM) errors during execution.**
* **Answer**: False
* **Mastery Explanation**: While Tungsten significantly reduces garbage collection overhead by managing memory explicitly (often off-heap using Unsafe APIs), OOM can still occur if the off-heap memory limit (`spark.memory.offHeap.size`) is breached, or if on-heap memory is exhausted by object instantiations outside Tungsten's control (e.g., Python UDFs, third-party libraries, or Driver memory collection).

**2. Adaptive Query Execution (AQE) can dynamically switch a SortMergeJoin to a BroadcastHashJoin at runtime, completely eliminating the need for a shuffle.**
* **Answer**: False
* **Mastery Explanation**: AQE can change a SortMergeJoin to a BroadcastHashJoin if the intermediate shuffle file size is smaller than the broadcast threshold. However, this happens *after* the map stage of the shuffle has already materialized the data to disk. It eliminates the *reduce* phase network shuffle, but the map-side shuffle write still occurs.

**3. In Spark's Unified Memory Manager, the boundary defined by `spark.memory.storageFraction` is a hard limit that execution memory cannot cross.**
* **Answer**: False
* **Mastery Explanation**: The Unified Memory Manager allows execution and storage to share a unified region. Execution memory can evict storage memory if storage exceeds `spark.memory.storageFraction`, but storage cannot evict execution memory. The fraction represents an immunity boundary for storage, not a hard limit.

**4. The External Shuffle Service is primarily designed to accelerate shuffle read speeds by caching shuffle blocks in memory.**
* **Answer**: False
* **Mastery Explanation**: The External Shuffle Service does not cache shuffle blocks in memory to accelerate reads. Its primary purpose is to decouple shuffle data availability from the executor's lifecycle, allowing executors to be dynamically downscaled (Dynamic Allocation) without losing the shuffle files they generated.

**5. Whole-Stage Code Generation collapses the entire Spark physical plan into a single Java function to eliminate virtual function calls.**
* **Answer**: False
* **Mastery Explanation**: It collapses *fragments* of the physical plan, not the entire plan. Boundaries like exchanges (shuffles), sorts, and certain unsupported operations break the pipeline into multiple generated Java functions.

**6. Setting `spark.sql.shuffle.partitions` to a very high number (e.g., 100,000) is the best way to resolve data skew issues in all scenarios.**
* **Answer**: False
* **Mastery Explanation**: While more partitions reduce the data per task, excessive partitions lead to massive task scheduling overhead, small file problems, and network connection exhaustion during the shuffle fetch phase. Data skew requires specific skew-handling techniques (AQE skew optimization, salting), not just raw partition increases.

**7. RDDs inherently cache data in Tungsten's highly optimized binary format.**
* **Answer**: False
* **Mastery Explanation**: RDDs operate on native Java/Scala objects and serialize them using Java or Kryo serialization. Tungsten's UnsafeRow binary format is exclusively used by the Catalyst optimizer for DataFrames and Datasets.

**8. Speculative execution (`spark.speculation=true`) is an effective architectural remedy for handling skewed tasks.**
* **Answer**: False
* **Mastery Explanation**: Speculative execution is designed to mitigate straggler tasks caused by hardware degradation or node-specific issues. If a task is slow due to data skew (heavy payload), the speculative duplicate will process the exact same heavy payload and be equally slow.

**9. A Spark Executor's `MemoryOverhead` is used for storing Broadcast variables.**
* **Answer**: False
* **Mastery Explanation**: Broadcast variables are stored in the execution/storage memory region (specifically storage). `MemoryOverhead` is meant for off-heap allocations, JVM metaspace, thread stacks, and native C++ libraries (like netty networking or Python worker processes).

**10. When a Spark job uses `MEMORY_AND_DISK` caching, data evicted from memory is automatically sent to the executor's local disk.**
* **Answer**: True
* **Mastery Explanation**: The BlockManager handles this transition. When storage memory is full, the LRU (Least Recently Used) blocks are serialized and written to the local disk of the executor that originally held the memory block, rather than failing the cache request.

## Section 2: Multiple Choice Questions (15)

**11. Which phase of the Catalyst Optimizer is responsible for converting unresolved attributes into typed objects?**
A) Logical Optimization
B) Physical Planning
C) Analysis
D) Cost-Based Optimization
* **Answer**: C
* **Mastery Explanation**: The Analysis phase uses the Catalog (schema metadata) to resolve column names and table names into typed, validated Logical Plans. Physical Planning handles execution strategies, and Optimization handles rule-based simplifications.

**12. When does Spark bypass the Sort Shuffle Manager and use the BypassMergeSortShuffleWriter?**
A) When map-side aggregation is enabled and partitions < 200.
B) When map-side aggregation is disabled and partitions < `spark.shuffle.sort.bypassMergeThreshold`.
C) When AQE determines the output is broadcastable.
D) When Tungsten memory is exhausted.
* **Answer**: B
* **Mastery Explanation**: BypassMergeSortShuffleWriter avoids sorting data before writing it out. It is only used if there is no map-side aggregation (like in groupByKey, but NOT reduceByKey) and the number of partitions is below the threshold (default 200), saving CPU cycles on unnecessary sorting.

**13. In Spark's memory architecture, what happens if an Executor needs more Execution Memory but Storage Memory is fully occupied by cached blocks that are outside the `storageFraction` immunity zone?**
A) The task fails with OOM.
B) Execution memory evicts the storage blocks to disk (if configured) or drops them.
C) The Executor pauses until Storage blocks are manually unpersisted.
D) Spark spins up a new Executor to handle the memory request.
* **Answer**: B
* **Mastery Explanation**: The Unified Memory Manager prioritizes Execution over Storage. It will forcefully evict Storage blocks that exceed the `storageFraction` limit to make room for Execution memory to prevent task failure.

**14. What is the primary architectural bottleneck that prevents Python UDFs from matching Scala UDF performance in Spark?**
A) Python's Global Interpreter Lock (GIL).
B) Serialization overhead between the JVM and Python worker processes.
C) Lack of Tungsten code generation in Python.
D) PyArrow memory limits.
* **Answer**: B
* **Mastery Explanation**: Traditional Python UDFs require Spark to serialize UnsafeRow data from the JVM (using Pickle), send it over a socket to a Python worker, process it, and serialize it back. This massive serialization and inter-process communication overhead is the main bottleneck. (Pandas UDFs mitigate this using Arrow).

**15. Which physical join strategy is Spark most likely to choose for two massive, unsorted tables (10TB each) with an equi-join condition?**
A) Broadcast Hash Join
B) Shuffle Hash Join
C) Sort Merge Join
D) Cartesian Product Join
* **Answer**: C
* **Mastery Explanation**: Sort Merge Join is the default and most robust strategy for large datasets. It sorts both datasets by the join key and merges them, requiring only a small memory footprint during the merge phase, unlike Shuffle Hash Join which risks OOM if a partition's hash table exceeds memory.

**16. How does Tungsten's Cache-Aware Computation improve performance?**
A) By caching all RDDs in the L1 CPU cache.
B) By using highly compressed pointer structures and fixed-width data to fit more records into L1/L2/L3 CPU caches.
C) By disabling JVM garbage collection completely.
D) By writing directly to off-heap memory bypassing the OS page cache.
* **Answer**: B
* **Mastery Explanation**: Tungsten uses `UnsafeRow` which packs data contiguously without Java object headers (saving 16+ bytes per object). This dense packing allows more data to fit into CPU hardware caches, drastically improving cache hit rates and CPU throughput.

**17. What role does the Driver's BlockManagerMaster play during a shuffle?**
A) It transmits the actual shuffle data between executors.
B) It tracks the locations of all shuffle map outputs and provides this metadata to reducers.
C) It compresses the shuffle files on disk.
D) It caches the shuffle outputs in the Driver's memory.
* **Answer**: B
* **Mastery Explanation**: Executors register their map output locations with the Driver's `MapOutputTrackerMaster` (closely tied to BlockManagerMaster). Reducers query the Driver to find which executors hold the blocks they need to fetch. The Driver never touches the actual shuffle payload data.

**18. Why does Delay Scheduling exist in Spark's TaskScheduler?**
A) To pause execution until data skew is resolved.
B) To wait for a localized executor (NODE_LOCAL or PROCESS_LOCAL) to become available before falling back to ANY locality.
C) To delay garbage collection until the task completes.
D) To prevent the Driver from being overwhelmed by task updates.
* **Answer**: B
* **Mastery Explanation**: Spark prefers data locality. If a core is available on a node without the data, Spark waits briefly (`spark.locality.wait`) hoping a core frees up on the node *with* the data, before giving up and shipping the data over the network to the available core.

**19. What is the fundamental risk of setting `--executor-cores 16` or higher?**
A) Disk I/O throughput becomes infinite.
B) HDFS NameNode throttling.
C) Severe HDFS I/O contention and JVM Garbage Collection degradation due to too many concurrent threads allocating objects.
D) Tungsten disables Code Generation automatically.
* **Answer**: C
* **Mastery Explanation**: Highly concurrent JVMs (16+ cores) suffer from severe GC pauses and thread contention. Usually, 5 cores per executor is the architectural sweet spot for maximizing HDFS throughput while keeping JVM GC overhead manageable.

**20. Which statement correctly differentiates Checkpointing from Caching in Spark?**
A) Caching truncates the lineage graph, while checkpointing preserves it.
B) Checkpointing writes to reliable distributed storage (HDFS/S3) and truncates the lineage, whereas caching keeps lineage in case of node failure.
C) Checkpointing uses Tungsten format, caching uses Java serialization.
D) Caching is synchronous, checkpointing is entirely asynchronous by default.
* **Answer**: B
* **Mastery Explanation**: Caching stores data locally (memory/disk) and retains the RDD lineage so it can be recomputed if a node dies. Checkpointing saves to durable storage and severs the lineage, fundamentally terminating the DAG graph at that point.

**21. Under AQE, what does Dynamic Partition Pruning (DPP) achieve?**
A) It deletes empty files on disk.
B) It pushes a filter from a small dimension table to a large fact table's scan phase during a join, avoiding scanning unnecessary partitions.
C) It automatically reduces `spark.sql.shuffle.partitions`.
D) It prunes Spark's logical plan of unused columns.
* **Answer**: B
* **Mastery Explanation**: In a star schema join, DPP executes the small dimension table first, collects the filter keys, and injects them as a dynamic filter into the physical scan of the large fact table, drastically reducing the data read from storage.

**22. How is a `BroadcastVariable` distributed across a cluster?**
A) Sent individually from the Driver to every single Task.
B) Using a BitTorrent-like peer-to-peer protocol where executors share blocks with each other to reduce Driver network bottleneck.
C) Saved to HDFS and read by executors.
D) Piggybacked on the task serialization payload.
* **Answer**: B
* **Mastery Explanation**: Spark uses `TorrentBroadcast`. The Driver divides the variable into chunks. Executors fetch chunks from the Driver and then immediately act as servers to share those chunks with other executors, preventing the Driver's NIC from bottlenecking.

**23. What triggers a "FetchFailedException" in Spark?**
A) The Driver failing to read from HDFS.
B) A reducer task failing to pull shuffle blocks from an executor because that executor crashed or timed out.
C) A syntax error in a Catalyst expression.
D) An off-heap memory leak.
* **Answer**: B
* **Mastery Explanation**: A FetchFailedException occurs during the shuffle read phase. If the executor hosting the map-side shuffle files dies (often due to OOM), the reducer cannot fetch the data. Spark handles this by resubmitting the missing map tasks.

**24. What is the effect of `spark.sql.inMemoryColumnarStorage.compressed=true`?**
A) It compresses Parquet files on disk.
B) It uses dictionary and run-length encoding for cached DataFrames, drastically reducing memory footprint.
C) It compresses shuffle network traffic.
D) It zips the RDD lineage graph.
* **Answer**: B
* **Mastery Explanation**: When using `df.cache()`, Catalyst applies highly efficient columnar compression schemes (RLE, Dictionary, Bit-packing) to the cached partitions in memory, maximizing the capacity of Storage Memory.

**25. Which GC algorithm does Spark 3.x recommend for executors with large heaps (>32GB)?**
A) Parallel GC
B) Serial GC
C) G1GC (Garbage-First GC)
D) ZGC
* **Answer**: C
* **Mastery Explanation**: G1GC provides predictable pause times by dividing the heap into regions and collecting the most garbage-heavy regions first. It prevents the massive "stop-the-world" pauses that Parallel GC exhibits on large heaps.

## Section 3: Small Twist Questions (15)

**26. Scenario:** You join a 10MB table with a 10TB table. The query runs in 5 minutes via BroadcastHashJoin.
**Twist:** You change the 10MB table to use a Left Outer Join against the 10TB table (10MB table on the left).
* **Question:** What happens to the physical execution plan and performance?
* **Answer & Mastery Explanation**: The plan reverts to a SortMergeJoin, and the query takes hours. BroadcastHashJoin does NOT support broadcasting the left side of a Left Outer Join (or right side of Right Outer Join). The small table cannot be broadcast if it is the preserved side of the outer join.

**27. Scenario:** Your job is writing 10,000 files to S3 successfully using `df.repartition(10000).write...`.
**Twist:** You change `repartition(10000)` to `coalesce(10000)`. The upstream DataFrame has 500 partitions.
* **Question:** How many files are written to S3?
* **Answer & Mastery Explanation**: 500 files. `coalesce()` can only *reduce* the number of partitions to avoid a shuffle. If you specify a number higher than the current partition count, it does nothing and outputs the existing number of partitions.

**28. Scenario:** A query uses `reduceByKey` and completes without OOM.
**Twist:** You switch `reduceByKey` to `groupByKey`, keeping all other logic the same.
* **Question:** Why does the job suddenly crash with an OOM error?
* **Answer & Mastery Explanation**: `reduceByKey` performs map-side combining (partial aggregation before the shuffle network transfer). `groupByKey` transfers *all* raw records across the network and forces the reducer to hold all values for a key in memory, leading to an OOM if a key is skewed.

**29. Scenario:** You cache a DataFrame using `df.cache()` and run `df.count()`. It takes 10 seconds.
**Twist:** You run `df.select("col1").count()` immediately after.
* **Question:** Does the second query read from the cache? Why or why not?
* **Answer & Mastery Explanation**: No, it likely will not read from the cache (or it will read the full cached row). Spark's columnar cache is tied to the exact logical plan. However, Catalyst's Columnar storage allows reading specific columns from the cached payload. If the exact logical plan isn't matched or optimized away, Spark might recalculate. Actually, in Spark SQL, caching evaluates the underlying plan. `select("col1").count()` will hit the cache, but since it only needs the count, it's trivial. The real twist: if you added a filter *before* caching vs *after*.

*(Refining Question 29 for strict accuracy)*
**29 (Revised). Scenario:** You execute `val cached = df.filter($"a" > 10).cache()`, then run `cached.count()`.
**Twist:** You then run `df.filter($"a" > 10).filter($"b" > 5).count()`.
* **Question:** Does Catalyst use the `cached` dataset for the second query?
* **Answer & Mastery Explanation**: Yes. Catalyst's CacheManager is smart enough to detect that the logical plan of the new query contains an exact subtree of a cached logical plan. It will swap the subtree with an `InMemoryTableScan` and only apply the `$"b" > 5` filter on the cached data.

**30. Scenario:** Your AQE (Adaptive Query Execution) successfully optimizes a Skewed Join at runtime.
**Twist:** You introduce a `df.cache()` right before the join on the skewed table.
* **Question:** Does AQE still optimize the skewed join?
* **Answer & Mastery Explanation**: No. Caching materialized data introduces a barrier. AQE relies on shuffle boundaries to collect statistics and dynamically alter plans. Caching circumvents the standard shuffle metrics AQE needs, often disabling AQE optimizations for that specific branch.

**31. Scenario:** You set `spark.sql.shuffle.partitions=2000`. Your shuffle writes 2000 files.
**Twist:** You enable AQE with `spark.sql.adaptive.coalescePartitions.enabled=true`.
* **Question:** How many tasks execute the *reduce* phase?
* **Answer & Mastery Explanation**: Fewer than 2000. AQE will inspect the map-side output statistics. If many of the 2000 partitions are tiny, AQE will coalesce contiguous small partitions into larger ones, resulting in fewer, well-sized reduce tasks.

**32. Scenario:** You are running PySpark and your `map` operation uses a simple lambda: `rdd.map(lambda x: x + 1)`.
**Twist:** You switch to `df.select(col("x") + 1)`.
* **Question:** What architectural component shifts to cause the massive speedup?
* **Answer & Mastery Explanation**: The lambda requires Pickling data, spinning up a Python worker daemon, and IPC overhead. The DataFrame `select` uses Catalyst to translate the expression directly into Tungsten-optimized Java bytecode via Whole-Stage CodeGen, keeping execution entirely inside the JVM.

**33. Scenario:** You have `spark.memory.fraction=0.6` and `spark.memory.storageFraction=0.5`.
**Twist:** You change `spark.memory.storageFraction=1.0`.
* **Question:** Does this prevent Execution memory from being used?
* **Answer & Mastery Explanation**: No. Storage memory can only claim memory up to 100% of the fraction *if execution isn't using it*. If Execution needs memory, it will evict Storage blocks down to 0% if necessary. `storageFraction` only provides immunity *if* the space is acquired first, but Execution always takes priority for new allocations.

**34. Scenario:** A job with `--executor-memory 10g` completes fine.
**Twist:** You add a Python UDF `df.withColumn("res", my_python_udf(col("data")))`. The job immediately dies with YARN killing the container for exceeding memory limits.
* **Question:** Why didn't Spark's Unified Memory Manager prevent this OOM?
* **Answer & Mastery Explanation**: The Memory Manager only tracks on-heap JVM memory. The Python UDF spawns a separate Python process outside the JVM. This process consumes native OS memory. The total container memory (JVM + Python process) exceeded the YARN container allocation, triggering a YARN SIGKILL, which Spark cannot intercept.

**35. Scenario:** You join two datasets using an inner join and it performs a SortMergeJoin.
**Twist:** You add an inequality condition to the join: `df1.join(df2, df1("id") === df2("id") && df1("date") < df2("date"))`.
* **Question:** Does Spark still use a SortMergeJoin?
* **Answer & Mastery Explanation**: Yes. Spark will use a SortMergeJoin on the equi-join key (`id`), and then apply the inequality condition (`date < date`) as a post-join filter on the matched records.

**36. Scenario:** You do a cross join (Cartesian product) on two 1000-row tables.
**Twist:** You do the cross join on two 1,000,000-row tables.
* **Question:** What specific Catalyst safeguard is triggered?
* **Answer & Mastery Explanation**: Spark will throw an `AnalysisException` stating that a Cartesian product could result in a massive data explosion. You must explicitly set `spark.sql.crossJoin.enabled=true` to bypass this architectural safety valve.

**37. Scenario:** Your cluster has nodes with 64 cores each. You deploy executors with `--executor-cores 64`.
**Twist:** You change it to deploy 12 executors per node, each with `--executor-cores 5`.
* **Question:** Why does HDFS read throughput drastically improve?
* **Answer & Mastery Explanation**: HDFS client has a fundamental concurrency limitation per JVM. 64 threads in a single JVM will severely contend for HDFS client locks and GC pauses. Spreading the cores across 12 isolated JVMs eliminates the lock contention and keeps GC cycles fast.

**38. Scenario:** You read a 1GB CSV file and process it in 10 tasks.
**Twist:** You compress the CSV file using GZIP (100MB file) and read it.
* **Question:** How many tasks does Spark generate, and why does performance degrade?
* **Answer & Mastery Explanation**: 1 task. GZIP is a non-splittable compression format. Spark cannot divide the file across multiple partitions, forcing a single core on a single executor to read and decompress the entire file serially, destroying parallelism.

**39. Scenario:** You use `.orderBy("timestamp")` on a 1TB dataset, writing to 200 partitions.
**Twist:** You change it to `.sortWithinPartitions("timestamp")`.
* **Question:** What expensive architectural phase is completely bypassed?
* **Answer & Mastery Explanation**: The network Shuffle. `orderBy` enforces a global sort, requiring a massive all-to-all network shuffle to range-partition the data globally. `sortWithinPartitions` only sorts the data locally on the executor without moving records across the network.

**40. Scenario:** You configure `spark.serializer=org.apache.spark.serializer.KryoSerializer`.
**Twist:** You use DataFrames exclusively instead of RDDs.
* **Question:** How much performance improvement does Kryo provide here?
* **Answer & Mastery Explanation**: Zero. DataFrames and Datasets use Tungsten's internal `UnsafeRow` binary format and a highly specialized code-generated serialization mechanism for shuffles. The `spark.serializer` config only applies to raw RDD operations and broadasting Java objects.

## Section 4: Coding & Debugging (10)

**41. Debug the Leak:**
```scala
var counter = 0
rdd.foreach(x => counter += x)
println(counter)
```
* **Error**: `counter` prints 0.
* **Mastery Explanation**: The integer `counter` is serialized and sent to executors via closures. The executors update their local copies. The driver's `counter` is never updated. Use a Spark `Accumulator` to aggregate values back to the driver.

**42. Debug the Catalyst Blocker:**
```scala
def myComplexLogic(s: String): String = { /* complex parsing */ }
val myUdf = udf(myComplexLogic _)
df.select(myUdf($"text_col")).filter($"text_col" !== "")
```
* **Error**: Performance is terrible because Catalyst cannot inspect inside the UDF.
* **Mastery Explanation**: UDFs are black boxes to Catalyst. Catalyst cannot push down the filter `text_col !== ""` into the data source if the UDF was evaluated first (though here filter is on the raw col, so it's fine). But if the filter was on the UDF result, it prevents predicate pushdown and Whole-Stage CodeGen for that expression. Use native Spark SQL functions (like `regexp_extract`) whenever possible.

**43. Debug the Memory Blowout:**
```scala
val lookupMap = Map("A" -> 1, "B" -> 2 /* ... 1GB of data ... */)
val res = rdd.map(row => lookupMap.getOrElse(row.key, 0))
```
* **Error**: The job crashes with OOM or massive GC overhead on Executors.
* **Mastery Explanation**: `lookupMap` is caught in the closure of the `map` function. Spark will serialize and send a distinct copy of this 1GB map with *every single task*. If an executor runs 5 concurrent tasks, it uses 5GB of memory just for closures. Use `sparkContext.broadcast(lookupMap)` to send it once per executor.

**44. Debug the Lineage Recomputation:**
```scala
val df2 = df.filter($"age" > 20)
df2.count()
df2.write.parquet("s3://...")
```
* **Error**: The HDFS read and filter logic is executed twice.
* **Mastery Explanation**: Spark DataFrames are lazily evaluated. Both `count()` and `write` are terminal actions. Because `df2` is not cached, the entire lineage (reading the source, applying the filter) is recomputed from scratch for the `write` action. Add `df2.cache()` before the actions.

**45. Debug the Executor Skew:**
```scala
df.groupBy("country").agg(sum("revenue"))
```
* **Error**: 199 tasks finish in 5 seconds, 1 task takes 2 hours.
* **Mastery Explanation**: This is classic data skew (e.g., "country" = "USA" has 99% of the data). All records with the same key are shuffled to the same reducer partition. Fix: Enable AQE Skew Join/Optimization (`spark.sql.adaptive.skewJoin.enabled`), or salt the key by appending a random integer `0-9` to distribute the heavy key across multiple reducers, then perform a second aggregation.

**46. Debug the Cartesian Explosion:**
```scala
dfA.join(dfB, $"dfA.id" === $"dfB.id" || $"dfA.alt_id" === $"dfB.alt_id")
```
* **Error**: Spark triggers a BroadcastNestedLoopJoin or complains about Cross Joins.
* **Mastery Explanation**: Spark's SortMergeJoin and HashJoin require equi-joins using `AND`. Using an `OR` condition breaks the hash/sort guarantee. Spark is forced to evaluate every row in A against every row in B (nested loop), which is essentially a Cartesian product. Rewrite as a union of two separate joins.

**47. Debug the Window Function OOM:**
```scala
df.withColumn("rank", rank().over(Window.partitionBy("department").orderBy("salary")))
```
* **Error**: OOM on specific executors.
* **Mastery Explanation**: Window functions require all records for a specific partition (`department`) to fit into an executor's memory simultaneously to compute the order/rank. If one department has millions of records, it overflows the memory limit. 

**48. Debug the Useless Coalesce:**
```scala
val df = spark.read.parquet("s3://massive_data/") // 10,000 files
df.coalesce(10).filter($"status" === "active").show()
```
* **Error**: The read phase is incredibly slow, bottlenecked on 10 cores.
* **Mastery Explanation**: `coalesce()` is pushed down effectively into the reader. It forces Spark to read the 10,000 files using only 10 partitions (10 tasks) right from the start, destroying read parallelism. Do the filter first, *then* coalesce, or use `repartition()` which forces a shuffle but allows parallel reads.

**49. Debug the Checkpoint Deadlock:**
```scala
rdd.checkpoint()
rdd.count()
```
* **Error**: The lineage is recomputed twice during the checkpoint process.
* **Mastery Explanation**: When `checkpoint()` is followed by an action, Spark computes the RDD to satisfy the action, and then initiates a *completely separate job* to compute the RDD again to save it to the checkpoint directory. You must call `rdd.cache()` *before* `rdd.checkpoint()` to ensure the checkpointing job just reads from memory instead of recalculating.

**50. Debug the Driver Memory Crash (Collect):**
```scala
val results = df.groupBy("category").count().collect()
```
* **Error**: Driver crashes with `java.lang.OutOfMemoryError`.
* **Mastery Explanation**: `.collect()` forces all distributed data from the executors to be serialized and sent over the network directly into the Driver's JVM heap. If the resulting grouped dataset is larger than `--driver-memory`, the driver crashes. Always use `limit().collect()` or write to distributed storage if the result size is unknown.
