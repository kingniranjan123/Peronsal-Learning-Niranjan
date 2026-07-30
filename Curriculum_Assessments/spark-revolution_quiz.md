# Spark Revolution Quiz: Master Class Assessment

## Part 1: True/False Questions (10 Questions)

**1. True/False: MapReduce writes to HDFS at the end of every Map and Reduce phase to ensure fault tolerance, whereas Spark achieves fault tolerance purely through RDD lineage tracking and never writes to disk unless explicitly told to or during memory spill.**
*Answer:* True.
*Mastery Explanation:* Spark's foundational insight is replacing materialization with lineage. The DAGScheduler tracks transformations (lineage) and recomputes lost partitions on failure, writing to disk only on explicit actions (e.g., save, checkpoint) or when memory pressure forces a spill.

**2. True/False: Tungsten's Whole-Stage Code Generation (WSCG) fuses all transformations across an entire job into a single compiled JVM bytecode function.**
*Answer:* False.
*Mastery Explanation:* WSCG fuses operators *within a single stage*, not the entire job. A stage boundary is defined by a shuffle.

**3. True/False: Python UDFs seamlessly integrate with Tungsten's WSCG, allowing them to perform on par with native Spark SQL functions.**
*Answer:* False.
*Mastery Explanation:* Python UDFs break WSCG because they require a JVM-to-Python serialization boundary for every row, disabling WSCG for that operation and drastically reducing throughput.

**4. True/False: The Unified Memory Manager divides Spark Memory exclusively into Execution Memory and Storage Memory, which cannot borrow from each other.**
*Answer:* False.
*Mastery Explanation:* Execution and Storage memory dynamically borrow from each other. If execution needs more space, it can evict cached blocks from Storage memory (LRU policy) rather than failing the task with OOM.

**5. True/False: Catalyst Optimizer's Physical Planning phase uses cost-based statistics to choose between execution strategies, such as BroadcastHashJoin versus SortMergeJoin.**
*Answer:* True.
*Mastery Explanation:* While the Logical Optimization phase relies on rule-based rewrites, Physical Planning relies on cost-based optimization (CBO) and statistics (like table sizes) to select physical operators.

**6. True/False: Calling `rdd.cache()` immediately materializes the RDD into memory.**
*Answer:* False.
*Mastery Explanation:* `cache()` is a lazy transformation. It only marks the RDD to be cached. The actual materialization happens upon the first subsequent action (e.g., `count()`).

**7. True/False: `groupByKey` uses map-side partial aggregation (combiners) automatically to reduce shuffle data volume.**
*Answer:* False.
*Mastery Explanation:* `groupByKey` ships all raw values across the network without any map-side aggregation, which can cause severe shuffle bottlenecks. `reduceByKey` is the one that utilizes map-side combine.

**8. True/False: Checkpointing truncates the RDD lineage graph.**
*Answer:* True.
*Mastery Explanation:* Checkpointing writes the data to reliable storage (like HDFS) and replaces the RDD's parent lineage pointer with a `CheckpointRDD`, preventing `StackOverflowError` in long iterative jobs.

**9. True/False: Spark defaults to Kryo serialization for shuffle data.**
*Answer:* False.
*Mastery Explanation:* Spark defaults to Java serialization for RDD operations, though it strongly recommends Kryo. (For DataFrames, it uses Tungsten's `Encoder` binary format).

**10. True/False: A single straggler map task delays the start of all reduce tasks in a shuffle.**
*Answer:* True.
*Mastery Explanation:* A shuffle introduces a hard stage barrier. Reducers cannot begin fetching partitions until all map tasks in the preceding stage have successfully written their outputs.


## Part 2: Multiple Choice Questions (15 Questions)

**11. Which Spark component translates the logical lineage graph into a directed acyclic graph (DAG) of stages?**
A) Catalyst Optimizer
B) DAGScheduler
C) TaskScheduler
D) BlockManager
*Answer:* B
*Mastery Explanation:* The DAGScheduler splits the logical plan into physical stages at shuffle boundaries and submits TaskSets to the TaskScheduler. Catalyst only handles query optimization for SQL/DataFrames.

**12. In Spark 1.6+, what is the default behavior if Execution Memory is full and requires more space, but Storage Memory is occupied by cached RDDs?**
A) The task fails with OutOfMemoryError
B) Execution memory spills to disk immediately
C) It evicts cached blocks from Storage Memory
D) It pauses execution until memory is freed
*Answer:* C
*Mastery Explanation:* The Unified Memory Manager allows Execution memory to evict Storage memory blocks (LRU policy) if it needs room, prioritizing job completion over caching.

**13. What is the primary advantage of Tungsten's `UnsafeRow` format?**
A) It uses Java object serialization for better compatibility.
B) It stores data as raw bytes matching CPU cache lines, avoiding object deserialization overhead.
C) It automatically encrypts data in memory.
D) It enables dynamic type casting at runtime.
*Answer:* B
*Mastery Explanation:* `UnsafeRow` stores rows as contiguous byte arrays outside normal JVM GC reach, enabling direct CPU-register operations without the overhead of Java object serialization/deserialization.

**14. Which Catalyst optimization phase handles resolving column names against the catalog?**
A) Analysis
B) Logical Optimization
C) Physical Planning
D) Code Generation
*Answer:* A
*Mastery Explanation:* The Analysis phase uses the Catalog to resolve unresolved attributes (column names) and types into a resolved logical plan.

**15. When using `StorageLevel.MEMORY_AND_DISK_SER`, how is the data stored in memory?**
A) As deserialized Java objects
B) As serialized byte arrays
C) As Tungsten UnsafeRows on disk
D) As raw text
*Answer:* B
*Mastery Explanation:* The `_SER` suffix means the data is serialized (typically Kryo-compressed) into byte arrays, trading CPU time (for deserialization on read) for a much smaller memory footprint and lower GC pressure.

**16. Why does `reduceByKey` outperform `groupByKey` for aggregations?**
A) It uses a faster sorting algorithm.
B) It avoids the shuffle phase entirely.
C) It performs map-side partial aggregation, significantly reducing network transfer.
D) It bypasses Catalyst optimization.
*Answer:* C
*Mastery Explanation:* `reduceByKey` combines values locally on each mapper before shuffling, which can reduce shuffle data volume by up to 90%, whereas `groupByKey` shuffles all raw data.

**17. Which of the following operations does NOT trigger a shuffle?**
A) `repartition(n)`
B) `distinct()`
C) `map()`
D) `sortMergeJoin()`
*Answer:* C
*Mastery Explanation:* `map()` processes data row-by-row within existing partitions and is pipelined. The others require redistributing data across partitions.

**18. What physical operator does Catalyst use when `broadcast(smallRight)` is explicitly called in a join?**
A) SortMergeJoin
B) BroadcastHashJoin
C) ShuffleHashJoin
D) CartesianProduct
*Answer:* B
*Mastery Explanation:* The `broadcast()` hint forces Catalyst to use a `BroadcastHashJoin`, bypassing the `autoBroadcastJoinThreshold` and avoiding a shuffle of the large table.

**19. How are broadcast variables distributed to executors?**
A) Direct fan-out from the Driver to each Executor
B) BitTorrent-like peer-to-peer protocol
C) Written to HDFS and read by executors
D) Pushed through the TaskScheduler
*Answer:* B
*Mastery Explanation:* Spark uses a P2P protocol to distribute broadcast blocks (chunked to 4MB) to prevent the Driver's network bandwidth from becoming a bottleneck at scale.

**20. What is the result of applying a `cache()` transformation inside a loop without an accompanying action?**
A) Data is immediately stored in memory.
B) The cache instruction is ignored completely.
C) A new node is appended to the logical plan, but nothing is materialized until an action occurs.
D) The DAGScheduler throws an exception.
*Answer:* C
*Mastery Explanation:* `cache()` is lazy. It simply adds a caching instruction to the lineage graph. Without an action to trigger execution, no data is actually computed or stored.

**21. Why is calling `.count()` inside a loop for a Spark job considered an anti-pattern?**
A) It disables Catalyst optimization.
B) It triggers a full job submission and execution from source for every iteration.
C) It causes memory leaks on the driver.
D) It forces data to be written to disk.
*Answer:* B
*Mastery Explanation:* `count()` is an action. Calling it in a loop submits a distinct Spark job each time, resulting in massive recomputation overhead.

**22. What happens if a broadcast table exceeds the executor's available Storage Memory?**
A) The Driver crashes with OOM.
B) The task fails with `java.lang.OutOfMemoryError` on the executor building the hash map.
C) Catalyst falls back to SortMergeJoin dynamically.
D) Spark ignores the broadcast hint.
*Answer:* B
*Mastery Explanation:* Broadcast data is kept in memory. If it exceeds executor memory capacity, the executor throws an OOM error when trying to build the hash map.

**23. What role does `Janino` play in Spark?**
A) It is the default shuffle manager.
B) It compiles the Whole-Stage Code Generation plan into JVM bytecode.
C) It manages off-heap memory allocation.
D) It parses SQL strings into abstract syntax trees.
*Answer:* B
*Mastery Explanation:* Janino is a lightweight, fast Java compiler used by Tungsten to compile generated code into executable JVM bytecode at runtime.

**24. In the context of Spark UI, a high "Shuffle Read Size" combined with a high "Shuffle Write Size" usually indicates:**
A) Efficient map-side aggregation.
B) A SortMergeJoin or `groupByKey` operation transferring large amounts of data.
C) A BroadcastHashJoin.
D) Tungsten WSCG failure.
*Answer:* B
*Mastery Explanation:* Heavy shuffle I/O indicates operations that move massive data across the network, common in naive joins or `groupByKey` usage without map-side combining.

**25. Without checkpointing, what is the primary risk of running iterative ML algorithms (like PageRank) for hundreds of iterations?**
A) HDFS fills up with intermediate data.
B) The Spark UI crashes.
C) A `StackOverflowError` in the Driver JVM during task planning due to deeply nested lineage.
D) Executors run out of off-heap memory.
*Answer:* C
*Mastery Explanation:* The lineage DAG grows with every iteration. Eventually, recursive operations on this massive graph (like serialization for task scheduling) cause a Driver stack overflow.


## Part 3: "Small Twist" Scenario Questions (15 Questions)

**26. Scenario:** You change `repartition(10)` to `coalesce(10)` to reduce partitions from 1000 to 10.
**Twist:** How does the physical execution change?
*Answer:* `coalesce` merges partitions on the same executor without a full shuffle, changing an O(n) network operation into a localized operation.
*Mastery Explanation:* `repartition` strictly forces a shuffle (using round-robin partitioning). `coalesce` minimizes data movement by merging local partitions, removing the expensive shuffle stage barrier.

**27. Scenario:** You register a custom Python UDF to clean a string column in a DataFrame that was otherwise executing entirely in Tungsten WSCG.
**Twist:** What happens to the stage execution?
*Answer:* WSCG is broken for that operation; Spark must serialize rows to Python and back, plummeting throughput.
*Mastery Explanation:* Python UDFs cannot be compiled into Java bytecode by Janino. Spark must spin up a Python daemon, serialize the data out of the JVM, process it, and deserialize it back, bypassing WSCG benefits.

**28. Scenario:** You run `df.join(small_df, "id")` where `small_df` is 5MB, and it runs fast via BroadcastHashJoin. You create the exact same tables using a different pipeline without `ANALYZE TABLE`.
**Twist:** The new job takes 10x longer. Why?
*Answer:* Missing table statistics cause Catalyst to fall back to a SortMergeJoin.
*Mastery Explanation:* Without statistics (like Parquet footers or Hive metastore stats), Catalyst assumes the table is large and defaults to the conservative SortMergeJoin, triggering a massive two-sided shuffle.

**29. Scenario:** You run an iterative algorithm calling `df.checkpoint()` every 10 iterations. You notice checkpointing takes twice as long as expected.
**Twist:** You forgot to call `df.persist()` before `df.checkpoint()`. Why does this double the time?
*Answer:* Spark computes the lineage twice: once to write the checkpoint, and again to continue the next action.
*Mastery Explanation:* Checkpointing does not implicitly cache data in memory. If not persisted, the data is computed and written to HDFS, and then the next transformation recomputes it from source.

**30. Scenario:** A DataFrame query aggregates using `.groupBy("id").agg(sum("val"))`. You replace it with an identical-looking custom Python UDAF.
**Twist:** Network traffic spikes by 10x. Why?
*Answer:* Custom Python UDAFs bypass Catalyst's automatic map-side partial aggregation.
*Mastery Explanation:* Built-in declarative aggregations use `HashAggregate` with map-side combine. A custom Python UDAF forces a full shuffle (like `groupByKey`) before applying the reduction logic on the reducer.

**31. Scenario:** You have a cluster with 4GB executor memory. You cache a 3GB dataset using `MEMORY_ONLY`.
**Twist:** You get an OOM error, even though 3GB < 4GB. Why?
*Answer:* The Unified Memory Manager reserves 300MB, and defaults Spark Memory fraction to 60%, leaving only ~2.2GB for execution/storage.
*Mastery Explanation:* Executor heap is not entirely available for storage. (4GB - 300MB) * 0.6 = ~2.2GB. A 3GB dataset will either evict execution blocks or OOM if execution demands memory simultaneously.

**32. Scenario:** You write `rdd.filter(f1).map(m1).reduceByKey(r1)`.
**Twist:** How many stages does the DAGScheduler create?
*Answer:* Two stages.
*Mastery Explanation:* The `filter` and `map` are pipelined together with the map-side combine of `reduceByKey` in Stage 1. The network transfer forms the boundary. The reduce-side merge of `reduceByKey` is Stage 2.

**33. Scenario:** You set `spark.sql.autoBroadcastJoinThreshold` to 20MB. Your right-side table is 15MB on disk as Parquet.
**Twist:** Catalyst still chooses SortMergeJoin. Why?
*Answer:* The *in-memory* deserialized size of the table exceeds 20MB, or Catalyst lacks precise row-level statistics.
*Mastery Explanation:* Parquet is highly compressed. 15MB on disk can easily expand to 50MB+ in memory. If Catalyst relies on file-size estimation that exceeds the threshold when inflated, it refuses to broadcast.

**34. Scenario:** You replace `StorageLevel.MEMORY_ONLY` with `MEMORY_AND_DISK_SER` for a cached DataFrame.
**Twist:** CPU utilization goes up during reads, but GC pause times drop dramatically. Why?
*Answer:* Data is stored as byte arrays, requiring CPU time to deserialize, but shielding millions of objects from the Garbage Collector.
*Mastery Explanation:* Deserialized objects put immense pressure on the JVM GC. Kryo serialization converts them into compact byte arrays (one object to the GC), lowering memory footprint and GC pauses at the cost of CPU cycles during read.

**35. Scenario:** You perform a `join` between two DataFrames partitioned by the same key into the exact same number of partitions (e.g., 100).
**Twist:** Does this trigger a full network shuffle?
*Answer:* No, it triggers a co-located (or node-local) join if the partitioners match.
*Mastery Explanation:* If both DataFrames share the same partitioner and partition count, Spark knows that matching keys already reside in corresponding partitions, avoiding cross-network shuffle entirely.

**36. Scenario:** A Stage fails due to a temporary network blip in one executor.
**Twist:** Does Spark re-run the entire job from the beginning?
*Answer:* No, the DAGScheduler only re-submits the missing tasks for the lost partitions.
*Mastery Explanation:* Thanks to lineage, Spark knows exactly which partitions were lost and only recomputes that specific slice of data by tracing the DAG backward.

**37. Scenario:** You call `df.count()` followed by `df.write.parquet(...)` on a complex pipeline without any caching.
**Twist:** You notice the source HDFS files are read twice. Why?
*Answer:* Both `count()` and `write` are actions; without caching, each triggers an independent job execution from source.
*Mastery Explanation:* Spark's lazy evaluation means no state is kept between actions unless explicitly requested via `.persist()`.

**38. Scenario:** You observe `java.lang.OutOfMemoryError: GC overhead limit exceeded` specifically on the *Driver* node, not executors.
**Twist:** What single API call likely caused this?
*Answer:* `.collect()` on a massive DataFrame.
*Mastery Explanation:* `collect()` pulls all partitions from all executors into the Driver's single JVM heap. If the dataset exceeds the Driver's memory capacity, it crashes.

**39. Scenario:** You have a heavily skewed dataset where 90% of values map to the key "UNKNOWN". You run `reduceByKey`.
**Twist:** The job still OOMs on one executor despite map-side combining. Why?
*Answer:* Map-side combining reduces data volume, but a single reducer must still process all "UNKNOWN" records in memory during the final aggregation.
*Mastery Explanation:* Skew forces one task to handle a disproportionate amount of data. Even with combiners, if the final state (or intermediate hash map) for a single key exceeds memory, it fails. Salting is required.

**40. Scenario:** You enable Tungsten's off-heap memory (`spark.memory.offHeap.enabled=true`).
**Twist:** Executor JVM GC logs show almost zero pause times, but the OS kills the container. Why?
*Answer:* You didn't allocate enough YARN/Kubernetes container memory to account for off-heap allocations, triggering the OOM Killer.
*Mastery Explanation:* Off-heap memory avoids JVM GC, but it still consumes physical RAM. If JVM Heap + Off-Heap > Container Limit, the OS terminates the process.


## Part 4: Coding & Debugging Questions (10 Questions)

**41. Debugging a Query Plan:**
You run `df.explain()` and see:
`*(3) SortMergeJoin ...`
`:- *(1) Sort ...`
`+- Exchange hashpartitioning ...`
What does the `*(1)` and `*(3)` indicate?
*Answer:* It denotes Whole-Stage Code Generation (WSCG) domains. `*(1)` is one compiled stage, and `*(3)` is another. The `Exchange` (shuffle) breaks WSCG, dividing the execution into separate compiled blocks.

**42. Identifying a Memory Leak:**
A streaming Spark application slowly consumes more memory over 5 days until it crashes. You notice cached RDDs from Day 1 are still in the BlockManager. What API call is missing?
*Answer:* `rdd.unpersist()`
*Mastery Explanation:* Caching is persistent until explicitly unpersisted or evicted. In long-running or streaming jobs, failing to release cached blocks from old batches exhausts Storage Memory.

**43. Fixing Catalyst Strategy:**
Code:
```python
large_df.join(small_df, "id").write.parquet("out")
```
It takes 2 hours due to a massive shuffle. You know `small_df` is 5MB. Write the code to fix this.
*Answer:*
```python
from pyspark.sql.functions import broadcast
large_df.join(broadcast(small_df), "id").write.parquet("out")
```
*Mastery Explanation:* Explicitly injecting a `BroadcastHint` forces Catalyst to serialize `small_df` to the driver and distribute it via P2P, eliminating the shuffle for `large_df`.

**44. Correcting Checkpoint Logic:**
Code:
```python
df.checkpoint()
for i in range(10): df = df.withColumn(...)
```
You observe the lineage is not actually truncated before the loop. What is missing?
*Answer:* An action.
```python
df.checkpoint()
df.count() # Or any action
```
*Mastery Explanation:* `checkpoint()` is lazy. It requires an action to actually execute the job that writes the checkpoint files to HDFS and truncates the lineage.

**45. Tuning Executor Memory:**
An executor has 10GB heap. `spark.memory.fraction=0.6`, `spark.memory.storageFraction=0.5`. How much memory is guaranteed for execution before it starts evicting cached data?
*Answer:* ~2.91 GB.
*Mastery Explanation:* (10GB - 300MB reserved) = 9.7GB. Spark Memory pool = 9.7 * 0.6 = 5.82GB. Execution fraction = (1 - 0.5) = 0.5. 5.82 * 0.5 = 2.91GB.

**46. Debugging a DAG Skew:**
In the Spark UI Stages tab, a stage has 199 tasks finish in 5 seconds, and 1 task taking 2 hours. What is this phenomenon called and how do you resolve it for an aggregation?
*Answer:* Data Skew. Resolve it using "salting" (appending a random number to the skewed key before the first aggregation, then aggregating again without the salt).
*Mastery Explanation:* One partition receives vastly more data than others (e.g., a default null value). Salting distributes the skewed key across multiple reducers.

**47. Optimizing Aggregations:**
Code:
```python
rdd.groupByKey().mapValues(sum).collect()
```
Rewrite this for 10x better performance at scale.
*Answer:*
```python
rdd.reduceByKey(lambda a, b: a + b).collect()
```
*Mastery Explanation:* `groupByKey` buffers all values in memory and causes massive shuffle data transfer. `reduceByKey` performs map-side combine, radically reducing network I/O.

**48. UDF vs Native Functions:**
Code:
```python
import pyspark.sql.functions as F
df.withColumn("len", F.udf(lambda x: len(x))("name"))
```
Rewrite this to utilize Tungsten WSCG.
*Answer:*
```python
df.withColumn("len", F.length("name"))
```
*Mastery Explanation:* Using `F.length` utilizes native Catalyst expressions which are compiled directly into JVM bytecode, whereas the Python UDF breaks WSCG and forces serialization.

**49. Diagnosing Lineage Stack Overflow:**
A graph algorithm throws `java.lang.StackOverflowError` on the Driver. You see no memory errors on executors. What is the fix?
*Answer:* Implement `checkpoint()` every N iterations.
*Mastery Explanation:* The Logical Plan / Lineage Graph is stored in the Driver JVM. A loop creating hundreds of transformations creates a massive nested object tree, causing stack overflow during DAG serialization. Checkpointing truncates this tree.

**50. Analyzing Shuffle Partitions:**
You process a 50MB file using `df.groupBy("id").count()`. The Spark UI shows 200 tasks in the final stage, with 199 doing almost no work. Why?
*Answer:* `spark.sql.shuffle.partitions` defaults to 200.
*Mastery Explanation:* By default, any shuffle operation creates 200 partitions, regardless of input size. For small datasets, this causes unnecessary task scheduling overhead. Fix by setting `.config("spark.sql.shuffle.partitions", "4")` (or an appropriate small number).
