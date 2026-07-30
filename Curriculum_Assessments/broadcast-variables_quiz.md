# Master Class Assessment: Broadcast Variables

## Part 1: True/False Questions (10 Questions)

**1. TorrentBroadcast distributes data sequentially from the Driver to each Executor in a linear O(N) fashion.**
- **Answer:** False
- **Mastery Explanation:** TorrentBroadcast uses a logarithmic peer-to-peer (P2P) protocol. Once an executor fetches a chunk from the Driver's BlockManager, it becomes a seeder for other executors, preventing Driver network saturation.

**2. A Broadcast Hash Join entirely bypasses the Shuffle phase.**
- **Answer:** True
- **Mastery Explanation:** A Broadcast Hash Join is a map-side join. The small table is broadcasted to all executors, and the join is evaluated locally without requiring the costly Sort-Merge Shuffle phase involving local disk spills and network fetches.

**3. Catalyst physical planning relies exclusively on in-memory size to determine `spark.sql.autoBroadcastJoinThreshold`.**
- **Answer:** False
- **Mastery Explanation:** Catalyst relies on *on-disk* statistics or metadata estimations (e.g., Parquet footers), which represent compressed sizes. This leads to the "Parquet Compression Trap" where highly compressed data expands significantly in memory.

**4. When a broadcast variable is created, it immediately pushes data to all executors.**
- **Answer:** False
- **Mastery Explanation:** TorrentBroadcast fetches data lazily. Deserialization and P2P fetching only occur when the first task on the executor explicitly calls `.value` on the broadcast variable.

**5. Tungsten Whole-Stage Code Generation integrates with broadcast variables by reading from an off-heap `HashedRelation`.**
- **Answer:** True
- **Mastery Explanation:** Instead of raw rows, Spark transforms the dataset into a `HashedRelation` on the Driver, broadcasts it, and Tungsten generates bytecode to probe this off-heap memory, operating at CPU cache speeds.

**6. Failing to call `unpersist()` on broadcast variables in long-running Structured Streaming jobs has no impact due to JVM GC.**
- **Answer:** False
- **Mastery Explanation:** Broadcast chunks are cached in the executor's BlockManager. Without explicit `unpersist()`, these accumulate and cause severe memory leaks and eventual executor OOMs.

**7. Broadcast variables are deserialized once per task.**
- **Answer:** False
- **Mastery Explanation:** Broadcast variables are deserialized once per *executor*, not per task. This shifts the mechanism from task-level redundancy to executor-level singleton caching.

**8. The Driver JVM must hold the entire uncompressed dataset in its heap to serialize and chunk it for a TorrentBroadcast.**
- **Answer:** True
- **Mastery Explanation:** During an automatic Broadcast Hash Join, Catalyst gathers all partitions to the Driver, requiring the Driver heap to accommodate the entire uncompressed dataset before chunking.

**9. `HttpBroadcast` is the default broadcast mechanism in modern Spark versions.**
- **Answer:** False
- **Mastery Explanation:** `TorrentBroadcastFactory` entirely replaced the legacy `HttpBroadcast` because the HTTP approach suffered from massive Driver bottlenecks at scale.

**10. Kryo serialization is preferred for broadcast variables due to its reduced footprint and faster serialization cycles.**
- **Answer:** True
- **Mastery Explanation:** Kryo significantly outperforms standard Java serialization in both payload size and speed, drastically reducing network I/O for broadcast chunks.

---

## Part 2: Multiple Choice Questions (15 Questions)

**11. Which Catalyst physical operator coordinates the asynchronous collection and broadcasting of a DataFrame partition to all worker nodes?**
A) `BroadcastHashJoinExec`
B) `BroadcastExchangeExec`
C) `ShuffleExchangeExec`
D) `TorrentBroadcastFactory`
- **Answer:** B
- **Mastery Explanation:** `BroadcastExchangeExec` is the physical operator that collects the data to the driver and initiates the broadcast. `BroadcastHashJoinExec` performs the actual join using the broadcasted data.

**12. When an engineer blindly raises `spark.sql.autoBroadcastJoinThreshold` to 2GB to force a Broadcast Hash Join, what is the most likely failure mode?**
A) Executor OOM during task closure serialization
B) Driver OOM during `BroadcastExchange`
C) Network timeout fetching chunks from peers
D) Disk spill exception in BlockManager
- **Answer:** B
- **Mastery Explanation:** Bumping the threshold forces the driver to collect up to 2GB of uncompressed data. If `spark.driver.memory` isn't increased accordingly, it triggers a catastrophic Driver OOM.

**13. What is the default chunk size used by TorrentBroadcast?**
A) 1MB
B) 4MB
C) 10MB
D) 64MB
- **Answer:** B
- **Mastery Explanation:** The serialized payload is chunked into discrete 4MB blocks to optimize network transfer and avoid memory fragmentation.

**14. What causes the "Parquet Compression Trap" in auto-broadcast joins?**
A) Parquet metadata reporting uncompressed sizes instead of compressed
B) Snappy compression failing during Kryo serialization
C) Catalyst using compressed on-disk statistics to evaluate against the auto-broadcast threshold
D) Spark attempting to decompress Parquet directly on the Executor without Driver intervention
- **Answer:** C
- **Mastery Explanation:** Catalyst compares compressed on-disk sizes to `spark.sql.autoBroadcastJoinThreshold`. When decompressed in the Driver heap, the data expands massively (10x-20x), causing unexpected OOMs.

**15. Why does broadcasting a deeply nested machine learning model cause execution delays?**
A) The Torrent protocol struggles with nested objects
B) The initial `.value` call triggers lazy deserialization on the executor, causing massive CPU spikes and GC pauses
C) Tungsten cannot generate bytecode for ML models
D) Task closures become too large to serialize
- **Answer:** B
- **Mastery Explanation:** Deserialization happens on the first `.value` call. Complex object graphs require heavy CPU to deserialize and live permanently in the executor heap, competing with Tungsten execution memory.

**16. Which of the following is a recommended mitigation for broadcasting complex ML objects in PySpark?**
A) Broadcast the instantiated model object using Python's Pickle
B) Increase `spark.task.cpus` to speed up deserialization
C) Broadcast the raw binary bytes and instantiate the model dynamically on the executor
D) Use `HttpBroadcast` instead of `TorrentBroadcast`
- **Answer:** C
- **Mastery Explanation:** Broadcasting raw bytes bypasses serialization framework failures (like Kryo/Pickle with C++ bindings). The model is then reconstructed lazily on the executor using a temporary file.

**17. What Spark internal component physically caches the broadcast blocks in memory or on disk?**
A) TaskMemoryManager
B) SparkEnv
C) TorrentBroadcastFactory
D) BlockManager
- **Answer:** D
- **Mastery Explanation:** The BlockManager is Spark's distributed key-value store that caches blocks (like broadcast chunks) on both the Driver and Executor JVMs and manages eviction.

**18. How does `TorrentBroadcast` scale compared to standard task-level shipping?**
A) Linearly O(N)
B) Logarithmically O(log N)
C) Constant time O(1)
D) Exponentially O(2^N)
- **Answer:** B
- **Mastery Explanation:** By using a P2P protocol, `TorrentBroadcast` scales logarithmically. Executors act as seeders, preventing linear scaling bottlenecks on the Driver's network interface.

**19. What is the time complexity of the Sort-Merge Shuffle phase bypassed by a Broadcast Hash Join?**
A) O(1)
B) O(N log N)
C) O(N * M)
D) O(N + M)
- **Answer:** B
- **Mastery Explanation:** Sort-Merge joins require a full shuffle and sorting of both datasets, which is generally an O(N log N) operation. Broadcast Hash Join is O(N).

**20. In Catalyst's `explain` plan, what indicates that Tungsten has converted a DataFrame into an optimized off-heap map?**
A) `SortMergeJoin`
B) `HashedRelationBroadcastMode`
C) `WholeStageCodegen`
D) `ShuffleHashJoin`
- **Answer:** B
- **Mastery Explanation:** `BroadcastExchange HashedRelationBroadcastMode` proves Tungsten is transforming the DataFrame into a highly optimized binary `HashedRelation` inside the Driver before broadcasting.

**21. When a UDF captures a `Broadcast` wrapper variable, what is sent within the task closure?**
A) The entire deserialized object
B) The Kryo-serialized chunks
C) Only the lightweight `Broadcast` object reference
D) The BlockManager memory address
- **Answer:** C
- **Mastery Explanation:** The task closure only captures the lightweight reference to the broadcast wrapper, preventing the massive redundancy of serializing the underlying object for every task.

**22. Which memory area does Tungsten typically use to store the `HashedRelation` for a Broadcast Hash Join?**
A) JVM Young Generation
B) JVM Old Generation
C) Off-heap memory
D) On-disk swap
- **Answer:** C
- **Mastery Explanation:** The `HashedRelation` is managed off-heap by Tungsten to circumvent JVM garbage collection overhead, allowing for rapid bare-metal memory access speeds.

**23. What happens if a broadcasted variable's size slightly exceeds the executor's available memory?**
A) The executor immediately crashes with OOM
B) The application fails over to a SortMergeJoin
C) The BlockManager spills the broadcast blocks to local disk
D) Spark splits the broadcast variable across multiple executors
- **Answer:** C
- **Mastery Explanation:** The BlockManager can spill blocks to disk if memory is constrained, though this incurs a performance penalty during deserialization. However, if the deserialized object itself exceeds heap, an OOM occurs.

**24. Which action must an engineer take to safely manage a continuous Structured Streaming job utilizing broadcasted lookups?**
A) Call `broadcast.destroy()` after every micro-batch
B) Call `broadcast.unpersist()` to clear BlockManager caches
C) Set `spark.sql.streaming.broadcastTimeout`
D) Disable auto-broadcasting
- **Answer:** B
- **Mastery Explanation:** `unpersist()` asynchronously removes the cached blocks from the executors' BlockManagers. Failing to do this causes a memory leak that eventually crashes the cluster.

**25. If table statistics are completely missing (e.g., `ANALYZE TABLE` has never been run), what join strategy does Catalyst default to?**
A) Broadcast Hash Join
B) Shuffle Hash Join
C) Sort Merge Join
D) Cartesian Product Join
- **Answer:** C
- **Mastery Explanation:** Without statistics, Catalyst assumes the tables are large and defaults to the safest (but most expensive) distributed algorithm: the Sort Merge Join.

---

## Part 3: Small Twist Questions (15 Questions)

**26. Twist:** You have a 5MB dictionary. You configure `spark.sql.autoBroadcastJoinThreshold = 10MB`. However, the Driver OOMs during the join. You notice the dictionary consists of complex nested HashMaps. What happened?
- **Answer:** The 5MB size was likely the serialized/on-disk size. The JVM object graph overhead of nested HashMaps inflated the in-memory footprint to exceed the Driver's available heap.
- **Mastery Explanation:** Java objects carry massive header and pointer overhead. A 5MB serialized payload can easily expand to hundreds of megabytes in the JVM heap, causing the Driver OOM spiral.

**27. Twist:** You explicitly broadcast a 50MB array and pass it to a UDF. The cluster network saturates, and tasks take minutes to start. The code looks like this: `val myVar = array; val myBroadcast = sc.broadcast(myVar); val udf = (x) => { myVar.contains(x) }`. What is the error?
- **Answer:** The UDF references `myVar` instead of `myBroadcast.value`.
- **Mastery Explanation:** Because the closure cleaner detects a reference to the raw `myVar`, it serializes and ships the 50MB array with every single task closure, completely bypassing the broadcast mechanism.

**28. Twist:** A Broadcast Hash Join plan is selected. The data is 8MB compressed. The Driver has 4GB memory, so no OOM occurs. However, tasks on the executors take 45 seconds to process their first row, then run instantly. Why?
- **Answer:** Late-Stage Deserialization Overhead.
- **Mastery Explanation:** The 8MB compressed data expanded significantly. When the first task called `.value`, the executor had to deserialize the massive chunk payload into JVM objects, causing a massive CPU and GC spike.

**29. Twist:** You use `df.hint("broadcast")` on a 50GB table. `spark.sql.autoBroadcastJoinThreshold` is 10MB. The Driver has 64GB of memory. The join executes, but it performs a Sort Merge Join. Why?
- **Answer:** The hint was ignored because the table size strictly exceeds the driver memory safety limits or hardcoded maximums in Spark (8GB max for broadcast).
- **Mastery Explanation:** Spark limits broadcast variables to 8GB. A 50GB table cannot be broadcasted regardless of hints, so Catalyst silently falls back to Sort Merge Join.

**30. Twist:** Two identical Spark jobs run. Job A uses `.join(broadcast(df))`. Job B uses `spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "100MB")` and `.join(df)`. Job B crashes with a Driver OOM, but Job A succeeds. Why?
- **Answer:** Job B applied the threshold globally, accidentally auto-broadcasting multiple other tables in the DAG simultaneously.
- **Mastery Explanation:** Changing the global threshold affects every join in the application. Job B likely attempted to collect and broadcast several tables concurrently at the Driver, exhausting heap space, whereas Job A isolated the behavior via hint.

**31. Twist:** A PySpark engineer broadcasts an XGBoost model object. The application fails immediately with a `PickleException` / `KryoException`.
- **Answer:** C++ bound objects (like XGBoost) cannot be natively serialized by PySpark's default serializers.
- **Mastery Explanation:** The memory pointers in the native bindings don't translate across the JVM/Python serialization boundary. The fix is to broadcast raw binary bytes and instantiate lazily.

**32. Twist:** You broadcast a variable in a streaming job and call `broadcast.unpersist()`. On the next micro-batch, tasks referencing the broadcast variable fail with a `BlockNotFoundException`.
- **Answer:** You unpersisted a broadcast variable that was still needed in subsequent computations, or you did not re-broadcast the updated state for the new micro-batch.
- **Mastery Explanation:** `unpersist()` deletes the chunks from the BlockManager. If a delayed task or new batch attempts to fetch it, the data is gone.

**33. Twist:** `TorrentBroadcast` chunks are 4MB. You broadcast a 2MB object. You observe network traffic is highly centralized on the Driver and peer-to-peer fetching seems inactive. Why?
- **Answer:** The object fits entirely within a single chunk.
- **Mastery Explanation:** P2P sharing thrives on multiple chunks. If there is only one 4MB chunk, the executor must fetch it directly from the Driver, limiting the logarithmic P2P distribution benefits.

**34. Twist:** You execute a Broadcast Hash Join. Tungsten is enabled. The physical plan shows `BroadcastHashJoinExec` but DOES NOT show `HashedRelationBroadcastMode`. Instead it shows a generic object broadcast. Why?
- **Answer:** The join condition likely involves an inequality (`>`, `<`) or complex non-equi join condition.
- **Mastery Explanation:** `HashedRelation` is specifically optimized for equi-joins (hash lookups). Non-equi joins require different internal representations (like `BroadcastNestedLoopJoin`), losing the bare-metal hash map speed.

**35. Twist:** An executor crashes with an OOM. You analyze the heap dump and find it is 95% full of `java.lang.String` objects from a broadcast variable, even though the raw data was just 10MB of CSV text.
- **Answer:** Deserializing CSV into JVM Strings creates massive memory overhead due to String headers and UTF-16 encoding.
- **Mastery Explanation:** 10MB of raw text can easily become 150MB of JVM Strings. Using dictionary encoding, RoaringBitmaps, or off-heap Arrow memory prevents this heap bloat.

**36. Twist:** You use `pandas_udf` and broadcast a model. You instantiate the model inside the UDF: `def predict(s): model = xgb.Booster(); model.load(broadcasted.value)`. Latency is terrible.
- **Answer:** You are deserializing and loading the model once per *batch/partition* rather than once per *worker process*.
- **Mastery Explanation:** The model should be cached in Python's `globals()` so it is only loaded once per worker JVM lifecycle, not re-instantiated for every Arrow batch.

**37. Twist:** You join a 1TB fact table with a 5MB dimension table. No broadcast hint is used. The plan shows a Shuffle Hash Join. `autoBroadcastJoinThreshold` is 10MB. Why did it not broadcast?
- **Answer:** The dimension table statistics are missing, and Catalyst's fallback configuration prioritizes Shuffle Hash Join over Broadcast when statistics are unknown but the cluster has high shuffle partitions.
- **Mastery Explanation:** Without `ANALYZE TABLE`, Catalyst has no size estimate. It refuses to auto-broadcast an unknown table to protect the Driver.

**38. Twist:** A broadcast variable is created. No actions (like `.collect()` or `.count()`) are called on the main DataFrame yet. However, you see the Driver's memory spiking immediately. Why?
- **Answer:** You created a broadcast variable from a local Driver collection (e.g., `sc.broadcast(myLargeLocalList)`).
- **Mastery Explanation:** Explicitly broadcasting a local object immediately serializes and chunks it in the Driver's BlockManager, consuming memory before any distributed Spark action is triggered.

**39. Twist:** You set `spark.sql.broadcastTimeout = 10` (seconds). The query fails with a timeout exception. The Driver has 128GB of RAM and the broadcasted table is only 50MB. What is the bottleneck?
- **Answer:** The 50MB table is the result of a highly complex upstream computation (e.g., joining 10 large tables).
- **Mastery Explanation:** `BroadcastExchange` forces the execution of all upstream lineage to materialize the 50MB table on the Driver. If that lineage takes 15 seconds to compute, the 10-second broadcast timeout triggers.

**40. Twist:** An engineer tries to broadcast an RDD directly: `sc.broadcast(myRdd)`. The code fails to compile or throws an exception.
- **Answer:** You cannot broadcast a distributed Spark collection (RDD/DataFrame) directly.
- **Mastery Explanation:** Broadcast variables wrap local Driver-side objects. You must call `.collect()` to bring the RDD data to the Driver before passing it to `sc.broadcast()`.

---

## Part 4: Coding & Debugging (10 Questions)

**41. Memory Leak in Streaming**
```scala
df.writeStream.foreachBatch { (batchDF, batchId) =>
  val lookupMap = generateLookupMap(batchId)
  val bVar = spark.sparkContext.broadcast(lookupMap)
  batchDF.map(row => bVar.value.get(row.id)).write.save()
}.start()
```
- **Bug:** `bVar.unpersist()` is missing.
- **Fix/Mastery:** The `bVar` is created every micro-batch. Without `unpersist()`, BlockManagers accumulate chunks until executors OOM. Add `bVar.unpersist()` after the batch write.

**42. Closure Serialization Disaster**
```scala
val taxRates = loadTaxRates() // Returns Map[String, Double] of size 200MB
val bTax = sc.broadcast(taxRates)
val calculateTax = udf((state: String) => taxRates.getOrElse(state, 0.0))
```
- **Bug:** UDF references `taxRates` directly, not `bTax.value`.
- **Fix/Mastery:** Change to `bTax.value.getOrElse(state, 0.0)`. Currently, Spark will serialize the 200MB `taxRates` map into every task closure, saturating the network.

**43. The Silent Fallback**
```scala
val dfLarge = spark.table("trillion_rows")
val dfSmall = spark.table("un-analyzed_table") // 5MB physically
val joined = dfLarge.join(dfSmall, "id")
```
- **Bug:** `dfSmall` lacks table statistics, causing a SortMergeJoin.
- **Fix/Mastery:** Run `ANALYZE TABLE un-analyzed_table COMPUTE STATISTICS` or use the `broadcast(dfSmall)` hint to force the BHJ.

**44. Driver OOM Death Spiral**
```scala
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "5GB")
val giantDf = spark.read.parquet("...") // 4.5 GB compressed
val result = factDf.join(giantDf, "key")
```
- **Bug:** 4.5GB compressed expands to 30GB+ in memory, crashing the Driver during `BroadcastExchange`.
- **Fix/Mastery:** Revert the threshold. 4.5GB compressed is far too large to broadcast. Rely on SortMergeJoin or optimize the data skew.

**45. Python Model Serialization Failure**
```python
import spacy
nlp = spacy.load("en_core_web_lg")
b_nlp = spark.sparkContext.broadcast(nlp)
```
- **Bug:** `spacy` models contain C-level pointers that Pickle cannot serialize.
- **Fix/Mastery:** Broadcast the raw model path or binary bytes, and instantiate `spacy.load()` lazily inside the executor via `globals()` caching.

**46. Unnecessary Broadcasting**
```scala
val bString = sc.broadcast("STATIC_CONFIG_VALUE")
val df2 = df.withColumn("config", lit(bString.value))
```
- **Bug:** Broadcasting a tiny primitive string is overkill and adds BlockManager overhead.
- **Fix/Mastery:** Just capture the string in the closure: `val str = "STATIC_CONFIG_VALUE"; df.withColumn("config", lit(str))`. Closure serialization is perfectly efficient for tiny primitives.

**47. Misunderstanding Lazy Evaluation**
```scala
val expensiveObject = buildMassiveGraph()
val bGraph = sc.broadcast(expensiveObject)
// Job crashes here before any dataframe actions!
```
- **Bug:** The OOM happens on the Driver immediately upon calling `sc.broadcast()`.
- **Fix/Mastery:** Broadcast does not delay Driver-side serialization. The Driver must have enough heap to hold `expensiveObject` and its serialized chunk arrays immediately.

**48. PySpark Memory Thrashing**
```python
def my_udf(x):
    model = pickle.loads(b_model.value)
    return model.predict(x)
```
- **Bug:** The model is deserialized `pickle.loads` for every single row!
- **Fix/Mastery:** Deserialization must be cached at the worker level. Use `globals()` or a singleton pattern to deserialize only once per Python worker process.

**49. Premature Unpersist**
```scala
val bVar = sc.broadcast(map)
val res = df.map(x => bVar.value(x))
bVar.unpersist()
res.write.parquet("...")
```
- **Bug:** `unpersist()` is called before the action (`write`) evaluates the lazy `map` transformation.
- **Fix/Mastery:** The broadcast variable is destroyed before the tasks actually run. Move `bVar.unpersist()` to execute *after* `res.write.parquet(...)`.

**50. Cross-Join Cartesian Explosion**
```scala
val bTable = broadcast(spark.table("small_table"))
val joined = largeTable.crossJoin(bTable)
```
- **Bug:** Using `broadcast` hint on a crossJoin creates a `BroadcastNestedLoopJoin`.
- **Fix/Mastery:** While valid, it does not use `HashedRelation` and relies on nested loops. If the small table has 10,000 rows, every task evaluates 10,000 times per input row. Ensure this is analytically necessary, as it is computationally devastating despite the broadcast.
