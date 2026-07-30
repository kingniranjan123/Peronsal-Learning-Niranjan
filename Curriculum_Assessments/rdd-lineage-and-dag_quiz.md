# RDD Lineage and DAG - Elite Assessment

## Part 1: True/False Questions

**Q1: Calling `checkpoint()` on an RDD immediately truncates its lineage graph and writes data to disk.**
**Answer:** False
**Mastery Explanation:** `checkpoint()` is a lazy operation. It merely marks the RDD for checkpointing. The lineage is only truncated and the data written to disk after an action is called and the job finishes executing.

**Q2: Narrow dependencies allow for pipelined execution (task fusion) within a single stage, avoiding disk I/O and network shuffles.**
**Answer:** True
**Mastery Explanation:** Spark's DAGScheduler collapses chains of RDDs with narrow dependencies into a single stage, allowing multiple operations (e.g., map, filter) to be executed in a single pass over the data in memory.

**Q3: A `ShuffleDependency` always results in a new Stage boundary being created by the DAGScheduler.**
**Answer:** True
**Mastery Explanation:** Wide dependencies (ShuffleDependencies) require data to be reorganized across partitions, which necessitates a barrier. The DAGScheduler breaks the DAG into distinct stages at these shuffle boundaries.

**Q4: The Catalyst optimizer can automatically rewrite RDD lineage to push down filters before expensive map operations.**
**Answer:** False
**Mastery Explanation:** Catalyst is the query optimizer for Spark SQL (DataFrames/Datasets). RDDs are completely opaque to Catalyst; their lineage is executed exactly as defined by the user code without logical optimization.

**Q5: Using `coalesce(numPartitions)` to decrease the number of partitions always creates a wide dependency (shuffle).**
**Answer:** False
**Mastery Explanation:** `coalesce` (when shrinking) by default uses a `NarrowDependency` by collapsing multiple partitions from the parent RDD into a single partition in the child RDD, avoiding a shuffle. (`repartition`, which is `coalesce(numPartitions, shuffle=true)`, forces a shuffle).

**Q6: If an executor is lost, Spark achieves fault tolerance by recomputing only the missing partitions by tracing back the RDD lineage.**
**Answer:** True
**Mastery Explanation:** Because RDDs are immutable and keep track of their lineage, Spark can recompute exactly the lost partitions without needing to recompute the entire RDD or restart the whole job.

**Q7: RDD lineage graphs are stored locally on the worker nodes to ensure decentralized fault tolerance.**
**Answer:** False
**Mastery Explanation:** The entire DAG and RDD lineage is maintained centrally by the Driver program (specifically the DAGScheduler). Workers only receive individual Tasks to execute on specific partitions.

**Q8: A `cogroup` operation between two RDDs will always create a `ShuffleDependency` for all input RDDs.**
**Answer:** False
**Mastery Explanation:** If one or both of the input RDDs are already partitioned using the same Partitioner (e.g., HashPartitioner) as the resulting `cogroup` RDD, it will be a `OneToOneDependency` (narrow) for those RDDs, avoiding a shuffle.

**Q9: Extremely long RDD lineages have no practical impact on the Driver's memory footprint or stability.**
**Answer:** False
**Mastery Explanation:** Lineage graphs are represented as objects in the Driver's JVM memory. Iterative algorithms without checkpointing can produce lineages so long that they cause a `StackOverflowError` during DAG serialization or task scheduling.

**Q10: Tungsten's optimized binary memory format is applied automatically to all cached RDDs to reduce GC overhead.**
**Answer:** False
**Mastery Explanation:** Tungsten targets the Dataset/DataFrame API. Standard RDDs cache data as raw Java objects (unless explicitly serialized), meaning they do not benefit from Tungsten's off-heap binary format and are subject to JVM Garbage Collection overhead.

---

## Part 2: Multiple Choice Questions

**Q11: Which component is responsible for transforming the logical RDD lineage graph into a physical execution plan of Stages?**
A) TaskScheduler
B) DAGScheduler
C) BlockManager
D) Catalyst Optimizer
**Answer:** B
**Mastery Explanation:** The DAGScheduler takes the logical RDD DAG and computes a DAG of stages, splitting the graph at shuffle boundaries. The TaskScheduler only handles running the individual tasks of those stages on the cluster.

**Q12: When does an RDD lineage graph actually get evaluated?**
A) Upon transformation creation
B) When `cache()` is called
C) When an Action is called
D) When `checkpoint()` is called
**Answer:** C
**Mastery Explanation:** RDD transformations are lazy. The lineage is purely theoretical until an action (like `count`, `collect`, `saveAsTextFile`) is invoked, triggering the DAGScheduler to create jobs.

**Q13: What happens to the lineage of an RDD after it is successfully checkpointed?**
A) It is appended with a `CheckpointDependency`.
B) It is completely severed and replaced by a `ReliableCheckpointRDD`.
C) It is compressed into a single `StageBoundaryRDD`.
D) It remains unchanged, but a flag is set.
**Answer:** B
**Mastery Explanation:** Checkpointing cuts the lineage graph entirely. The RDD's parent becomes a `ReliableCheckpointRDD` that reads directly from HDFS/S3, meaning previous failures won't trigger recomputation of the entire chain.

**Q14: Which of the following operations is GUARANTEED to introduce a wide dependency?**
A) `map`
B) `filter`
C) `reduceByKey`
D) `union`
**Answer:** C
**Mastery Explanation:** `reduceByKey` requires data with the same key to be grouped together across partitions, which fundamentally requires a shuffle (wide dependency). `union` is a narrow dependency.

**Q15: In a Stage containing 5 narrow transformations (e.g., map, filter, map, filter, map), how many times is data written to disk?**
A) 5 times
B) 1 time
C) 0 times (assuming memory allows)
D) 2 times
**Answer:** C
**Mastery Explanation:** Due to Task Fusion, all narrow transformations within a stage are pipelined. The record flows through all 5 transformations in memory before moving to the next record, resulting in 0 intermediate disk writes.

**Q16: Why might a developer choose to `cache()` an RDD in the middle of a DAG?**
A) To cut the lineage for fault tolerance.
B) To push down filters to the data source.
C) To prevent recomputation when multiple actions are called on the same RDD.
D) To force a shuffle boundary.
**Answer:** C
**Mastery Explanation:** `cache()` (or `persist()`) stores the evaluated partitions in memory. If multiple branches of a DAG depend on this RDD, or multiple actions are called, caching avoids re-executing the entire lineage leading up to that point.

**Q17: What is the primary difference between `cache()` and `checkpoint()` regarding lineage?**
A) `cache()` cuts the lineage; `checkpoint()` does not.
B) `checkpoint()` cuts the lineage; `cache()` preserves it.
C) Both cut the lineage, but `checkpoint()` saves to disk.
D) Neither cuts the lineage.
**Answer:** B
**Mastery Explanation:** `cache()` preserves the lineage because cached partitions can be evicted from memory (LRU). If evicted, Spark uses the lineage to recompute them. `checkpoint()` writes to reliable storage (HDFS) and severs the lineage completely.

**Q18: A `ShuffleMapStage` produces what kind of output?**
A) Final results to the Driver.
B) Output files on HDFS.
C) Intermediate shuffle files on local disk to be read by the next stage.
D) In-memory DataFrames.
**Answer:** C
**Mastery Explanation:** A `ShuffleMapStage` executes tasks that map data and write out intermediate shuffle files (partitioned by the target stage's reducer IDs) to the local disk of the worker nodes.

**Q19: Which dependency type represents a 1-to-1 mapping between parent and child RDD partitions?**
A) `ShuffleDependency`
B) `OneToOneDependency`
C) `RangeDependency`
D) `PruneDependency`
**Answer:** B
**Mastery Explanation:** `OneToOneDependency` is a subclass of `NarrowDependency` where each partition of the child RDD depends on exactly one partition of the parent RDD (e.g., `map`, `filter`).

**Q20: When joining two RDDs, how can you avoid a shuffle?**
A) Use an outer join instead of an inner join.
B) Broadcast both RDDs.
C) Ensure both RDDs are pre-partitioned using the same Partitioner.
D) Call `coalesce(1)` on both before joining.
**Answer:** C
**Mastery Explanation:** If both RDDs are partitioned with the identical `Partitioner` (and same number of partitions), Spark knows that matching keys are on the same node, converting the join into a narrow dependency (co-partitioned join).

**Q21: What is a `ResultStage` in Spark?**
A) The first stage in any DAG.
B) Any stage that involves writing intermediate shuffle data.
C) The final stage of a job that computes the action's result.
D) A stage that only contains narrow transformations.
**Answer:** C
**Mastery Explanation:** The DAGScheduler divides a job into stages. All stages before the final one are `ShuffleMapStages`. The final stage, which executes the action and returns results to the driver (or writes to a sink), is the `ResultStage`.

**Q22: Why does `groupByKey` often lead to OutOfMemory (OOM) errors compared to `reduceByKey`?**
A) `groupByKey` creates a longer lineage.
B) `reduceByKey` performs map-side combiners (partial aggregation), while `groupByKey` shuffles all raw data.
C) `groupByKey` forces the use of Kryo serialization.
D) `reduceByKey` uses Tungsten memory, `groupByKey` uses Java Objects.
**Answer:** B
**Mastery Explanation:** `reduceByKey` aggregates data locally on the map side before shuffling, vastly reducing network I/O and memory pressure. `groupByKey` shuffles all values for a key to a single executor, often causing OOM if a key is highly skewed.

**Q23: How does Spark track dependencies between RDDs?**
A) Through the Catalyst optimizer.
B) Via the `dependencies` method in the RDD class, returning a sequence of `Dependency` objects.
C) Through Zookeeper state.
D) By parsing the abstract syntax tree of the Python/Scala code.
**Answer:** B
**Mastery Explanation:** Every RDD class implements a `getDependencies` (or `dependencies`) method that returns a list of `Dependency` objects (`NarrowDependency` or `ShuffleDependency`) pointing to parent RDDs, forming the lineage DAG.

**Q24: What is the significance of the `spark.rdd.compress` configuration?**
A) It compresses the logical DAG string sent to executors.
B) It compresses serialized RDD partitions when `MEMORY_ONLY_SER` is used.
C) It compresses the shuffle map output files.
D) It compresses DataFrames using Parquet.
**Answer:** B
**Mastery Explanation:** `spark.rdd.compress` applies compression (like LZ4 or Snappy) to serialized RDD partitions cached in memory (or disk), saving space at the cost of CPU cycles during reads/writes.

**Q25: In a lineage graph, what role does a `Partitioner` play?**
A) It determines how data is serialized.
B) It defines the placement of data across nodes for wide dependencies.
C) It optimizes logical plans into physical plans.
D) It tracks which blocks are cached in the BlockManager.
**Answer:** B
**Mastery Explanation:** A `Partitioner` (like `HashPartitioner` or `RangePartitioner`) dictates which key goes to which partition during a shuffle, directly influencing the physical layout of data across the cluster.

---

## Part 3: "Small Twist" Questions

**Q26: Scenario: You have RDD A -> `map` -> RDD B -> `reduceByKey` -> RDD C. You change `reduceByKey` to `groupByKey().mapValues(...)`. How does the DAG change?**
**Answer:** The number of stages remains the same (2 stages), but the amount of data shuffled increases massively.
**Mastery Explanation:** Both `reduceByKey` and `groupByKey` create a `ShuffleDependency`, resulting in a 2-stage DAG. The critical twist is that `reduceByKey` uses map-side combiners. Changing it to `groupByKey` removes the map-side reduction, causing a massive increase in shuffle write/read volumes, potentially leading to network bottlenecks or OOM.

**Q27: Scenario: You call `rdd.persist(StorageLevel.MEMORY_ONLY)` before a complex shuffle, and then call `rdd.checkpoint()`. You run an action. Where is the data read from on a subsequent action?**
**Answer:** The data is read from memory (if still cached). If evicted, it is read from the checkpoint on disk.
**Mastery Explanation:** Checkpointing truncates lineage, but persisting keeps data in memory. Spark is smart enough to use the cached memory partitions if available. If memory is lost, it falls back to the reliable HDFS checkpoint rather than recomputing from the beginning.

**Q28: Scenario: You use `rdd.repartition(100)` instead of `rdd.coalesce(100)` on an RDD with 1000 partitions. What happens to the DAG?**
**Answer:** A new Stage boundary (shuffle) is introduced.
**Mastery Explanation:** `coalesce` (when reducing partitions) avoids a shuffle by creating a narrow dependency, grouping existing partitions on the same node. `repartition` is explicitly `coalesce(numPartitions, shuffle=true)`, which forces a wide dependency and a new stage, ensuring even data distribution at the cost of a shuffle.

**Q29: Scenario: You join two RDDs, both having a `HashPartitioner` with 50 partitions. You then change one RDD to have 51 partitions. What happens to the Stage count?**
**Answer:** The stage count increases (a shuffle is added).
**Mastery Explanation:** To have a co-partitioned join (Narrow Dependency, no shuffle), both RDDs MUST have the same Partitioner AND the same number of partitions. Changing one to 51 breaks this alignment, forcing a `ShuffleDependency` and creating extra stages.

**Q30: Scenario: RDD X is computed and used in two different actions. You notice it's being evaluated twice. You add `.cache()` to RDD X. However, looking at the UI, the lineage is STILL re-evaluated from scratch for the second action. What configuration twist caused this?**
**Answer:** The cluster lacks memory, causing instant cache eviction, or the RDD was so large it didn't fit.
**Mastery Explanation:** `cache()` is lazy and relies on the BlockManager. If the data exceeds memory capacity and uses `MEMORY_ONLY`, partitions are not cached (or are immediately evicted). Thus, Spark uses the lineage to recompute them on the second action.

**Q31: Scenario: You implement an iterative PageRank algorithm using RDDs running for 100 iterations. It works fine for 20 iterations but throws `StackOverflowError` on the Driver around iteration 30. What changed?**
**Answer:** The lineage DAG grew too deeply nested, exceeding the Driver's JVM stack size during DAG resolution.
**Mastery Explanation:** In iterative algorithms, if you don't periodically `checkpoint()`, the lineage graph appends new transformations infinitely. When the DAGScheduler tries to serialize or traverse this massive nested object graph, it overflows the JVM stack.

**Q32: Scenario: You have `rdd.map(f1).filter(f2).map(f3)`. You configure Spark to run with a single executor with 1 core. How many intermediate passes over the data occur?**
**Answer:** 0 intermediate passes (1 single pass total).
**Mastery Explanation:** This is a purely narrow dependency chain. Task fusion pipelines these operations. Even with 1 core, a single record is read, mapped, filtered, and mapped again before moving to the next record. There are no intermediate passes.

**Q33: Scenario: You join RDD A and RDD B. RDD A is 10GB, RDD B is 10MB. It triggers a massive shuffle. You switch to `Broadcast` for RDD B. How does the DAG change?**
**Answer:** The `ShuffleDependency` is eliminated, merging the two stages into a single stage.
**Mastery Explanation:** By broadcasting the smaller RDD, the join becomes a map-side join. Every partition of RDD A can look up values in the broadcasted RDD B locally, transforming a wide dependency into a narrow one and dropping a stage.

**Q34: Scenario: You perform `rdd.sortByKey()`. You then change the code to `rdd.sortByKey(numPartitions=1)`. What happens to cluster utilization?**
**Answer:** Cluster utilization drops to 1 core for the final Stage.
**Mastery Explanation:** `sortByKey` triggers a shuffle. The resulting RDD has partitions equal to the `numPartitions` argument. Setting it to 1 means all data is shuffled to a single partition on a single executor, completely killing parallelism and likely causing an OOM.

**Q35: Scenario: You set `spark.cleaner.referenceTracking=false` and run a long-lived streaming application using RDD lineages. What is the eventual result?**
**Answer:** The Driver runs out of memory (OOM).
**Mastery Explanation:** Spark's ContextCleaner relies on reference tracking to garbage collect old RDD lineages and broadcast variables. Disabling it means old DAGs and metadata remain in the Driver's memory indefinitely, causing a memory leak.

**Q36: Scenario: You have an RDD of custom Java Objects. You cache it using `MEMORY_ONLY`. It takes 10GB. You change the class to implement `java.io.Externalizable` and optimize the serialization, but it still takes 10GB. Why?**
**Answer:** `MEMORY_ONLY` stores deserialized Java objects.
**Mastery Explanation:** `MEMORY_ONLY` completely ignores serialization optimizations because it stores raw JVM object references. To see the benefit of optimized serialization, you must twist the storage level to `MEMORY_ONLY_SER`, forcing Spark to store byte arrays.

**Q37: Scenario: You perform `rdd.reduceByKey(func)` which takes 5 minutes. You change `func` so it is no longer commutative (e.g., string concatenation order matters). Does the DAG change?**
**Answer:** The DAG does not change, but the result will be non-deterministic/corrupt.
**Mastery Explanation:** The DAGScheduler doesn't understand the logic inside `func`. It still creates a `ShuffleMapStage` with map-side combiners. Because combiners group data in arbitrary orders based on network/iterator speed, a non-commutative function will silently produce garbage results without altering the physical plan.

**Q38: Scenario: You use `rdd.sample(withReplacement=false, fraction=0.1)`. Does this create a new stage?**
**Answer:** No.
**Mastery Explanation:** Sampling (without replacement) is a narrow dependency transformation. It simply drops records locally within existing partitions. It does not require a shuffle or a new stage boundary.

**Q39: Scenario: Two identical RDDs, A and B, are read from the same HDFS path. You do `A.join(B)`. Spark executes a massive shuffle. You expect a narrow dependency since they are identical. Why did the shuffle happen?**
**Answer:** HDFS files do not inherently have a `Partitioner`.
**Mastery Explanation:** Even though the data is identical, RDDs loaded from text files default to `None` for their partitioner. Spark cannot guarantee that keys are co-located, so it safely assumes a shuffle is necessary unless an explicit `partitionBy()` is applied beforehand.

**Q40: Scenario: You change an RDD workflow to an identical DataFrame workflow. The RDD workflow threw OOM errors during a shuffle, but the DataFrame workflow succeeds. What architectural difference caused this?**
**Answer:** Tungsten external sorting and binary memory formats.
**Mastery Explanation:** DataFrames use Catalyst and Tungsten. Tungsten operates on binary row formats (avoiding JVM object overhead) and uses advanced external sorting during shuffles, spilling to disk efficiently. Raw RDDs shuffle JVM objects, causing massive GC overhead and heap bloat leading to OOM.

---

## Part 4: Coding & Debugging Questions

**Q41: The following code fails with a `NotSerializableException`. What in the DAG/closure is causing this?**
```scala
class MyHelper {
  val suffix = "_processed"
  def process(rdd: RDD[String]): RDD[String] = {
    rdd.map(x => x + suffix)
  }
}
```
**Answer:** The `MyHelper` class is not serializable.
**Mastery Explanation:** Inside the `map` closure, `suffix` is referenced, which implicitly pulls in the entire `this` instance of `MyHelper`. Since `MyHelper` does not extend `Serializable`, Spark's closure serializer fails when trying to send the DAG tasks to executors. Fix: move `suffix` to a local variable inside the method.

**Q42: You have the following iterative loop:**
```scala
var currentRdd = initialRdd
for (i <- 1 to 500) {
  currentRdd = currentRdd.map(x => x + 1)
}
currentRdd.count()
```
**What error will this likely throw and why?**
**Answer:** `StackOverflowError` on the Driver.
**Mastery Explanation:** The lineage graph is 500 layers deep. When `count()` is called, the DAGScheduler recursively traverses this massive lineage object to build stages and tasks, overflowing the JVM's call stack. Fix: use `checkpoint()` periodically.

**Q43: Review this code:**
```scala
val rdd2 = rdd1.groupByKey().mapValues(iter => iter.sum)
```
**Identify the performance blocker and rewrite it for an optimized physical plan.**
**Answer:** `groupByKey` causes excessive shuffling and potential OOM.
**Mastery Explanation:** `groupByKey` shuffles all raw values across the network. Rewrite to: `val rdd2 = rdd1.reduceByKey(_ + _)`. This allows map-side combiners to sum values locally before shuffling, drastically reducing network I/O and memory usage.

**Q44: Why does the following code execute `loadData()` twice?**
```scala
val base = loadData().filter(...)
val agg1 = base.reduceByKey(...)
val agg2 = base.groupByKey(...)
agg1.count()
agg2.count()
```
**Answer:** RDD `base` is not cached.
**Mastery Explanation:** Because RDDs are lazy and transient by default, calling `count()` on `agg1` evaluates the lineage back to `loadData`. Calling `count()` on `agg2` triggers an entirely separate job, evaluating `loadData` again. Fix: `base.cache()`.

**Q45: You are debugging a DAG in the Spark UI. You see 1 Stage that takes 5 hours, containing 10,000 tasks. 9,999 tasks finish in 10 seconds, but 1 task takes 4.9 hours. What is the specific issue in the lineage execution?**
**Answer:** Data Skew in a specific partition.
**Mastery Explanation:** The physical execution plan distributed the data, but one key (or a few keys) mapped to a single partition contains 99% of the data. That single task processes everything, bottlenecking the entire stage. Fix: Salting the keys before shuffling.

**Q46: A developer writes:**
```scala
val joined = rddA.join(rddB)
joined.checkpoint()
joined.count()
```
**They notice the join computation still takes a long time on the first action. They expected `checkpoint()` to speed it up. Why didn't it?**
**Answer:** Checkpointing requires evaluating the lineage first.
**Mastery Explanation:** `checkpoint()` itself triggers a job to materialize the RDD and write it to disk. Therefore, the first time you call an action, the heavy join is still computed. The benefit of the checkpoint is only realized on *subsequent* actions that use `joined`.

**Q47: Identify why this PySpark DAG suffers from massive serialization overhead compared to Scala.**
```python
def complex_math(x):
    import numpy as np
    return np.sqrt(x)

rdd.map(complex_math).count()
```
**Answer:** Python/JVM boundary serialization.
**Mastery Explanation:** Standard RDDs in PySpark require serializing the JVM data into Python objects (via Py4J/Pickle), piping it to Python worker processes, executing the function, and serializing it back to the JVM. This breaks execution pipelining efficiency. Fix: Use DataFrames/Vectorized UDFs where Catalyst handles JVM memory.

**Q48: Consider this code:**
```scala
rdd.repartition(100).filter(x => x > 10).count()
```
**How many stages are created, and how could you optimize the lineage?**
**Answer:** 2 Stages are created. Optimization: Swap the order.
**Mastery Explanation:** `repartition` forces a shuffle (Stage 1), pushing all data across the network, then filtering it (Stage 2). By reversing the lineage to `rdd.filter(x => x > 10).repartition(100)`, you dramatically reduce the amount of data shuffled over the network.

**Q49: What is the risk of doing this in a DAG?**
```scala
val lookupMap = sparkContext.parallelize(Seq((1, "A"), (2, "B"))).collectAsMap()
val result = largeRdd.map(row => lookupMap.get(row.id))
```
**Answer:** Driver OOM or massive task serialization size.
**Mastery Explanation:** `collectAsMap` brings the data to the Driver. When referenced inside the `map` closure, the entire `lookupMap` object is serialized and sent to every executor with every task. If `lookupMap` grows, it kills the Driver or network. Fix: Use a Broadcast variable (`sc.broadcast(lookupMap)`).

**Q50: A DAG has a stage that writes output to Amazon S3. The job fails randomly, and when it retries, it produces duplicate data in S3. What property of RDD lineage execution causes this, and how do you fix it?**
**Answer:** Task idempotency and speculative execution/retries.
**Mastery Explanation:** When a task fails, Spark's fault tolerance re-runs the lineage for that partition. If the `map` or `foreachPartition` function writes directly to S3 non-idempotently (e.g., appending files without atomic renames), retries cause duplicates. Fix: Use atomic file output committers or idempotent write logic (e.g. upserts).
