# Master Class: Spark Components - Elite Technical Assessment

This assessment evaluates Senior/Staff-level knowledge of Apache Spark's architecture, including Catalyst optimizations, Tungsten memory management, JVM tuning, and physical execution plans.

## 1. True/False Questions (10)

**Q1:** Tungsten's memory manager stores data as standard JVM objects to leverage Java's native Garbage Collection for cache management.
**Answer:** False
**Mastery Explanation:** Tungsten implements a custom memory manager that operates off-heap and stores data in a dense, binary format (`UnsafeRow`). This eliminates Garbage Collection overhead, whereas standard JVM objects would exacerbate it.

**Q2:** The Catalyst Optimizer's Physical Planning phase utilizes a Cost-Based Optimizer (CBO) to generate multiple physical plans and select the most efficient one.
**Answer:** True
**Mastery Explanation:** Catalyst's Physical Planning phase creates multiple potential plans and uses CBO estimations (like data size and row counts) to select the most cost-effective execution strategy.

**Q3:** Decreasing `spark.memory.storageFraction` allocates a larger portion of the unified memory region to execution memory, potentially reducing disk spills during heavy shuffles.
**Answer:** True
**Mastery Explanation:** Spark uses unified memory where storage and execution share space. Lowering the storage fraction prioritizes execution memory, which is critical for operations like sorting and hashing during shuffles, thus preventing disk spills.

**Q4:** Whole-Stage Code Generation uses the Volcano iterator model to evaluate queries, compiling multiple virtual function calls into a single step.
**Answer:** False
**Mastery Explanation:** Whole-Stage Code Generation *replaces* the Volcano iterator model. It collapses an entire query stage into a single Java function, eliminating the massive virtual function call overhead inherent in the Volcano model.

**Q5:** `spark.driver.maxResultSize` protects the Driver from Out-Of-Memory (OOM) errors by limiting the total size of serialized results sent from executors to the driver when `collect()` is called.
**Answer:** True
**Mastery Explanation:** If executors send back data exceeding this threshold, the job is aborted safely, preventing the Driver JVM from crashing abruptly due to OOM.

**Q6:** Broadcast variables in Spark use a master-slave HTTP pull protocol to send the read-only variable to every task closure independently.
**Answer:** False
**Mastery Explanation:** Broadcast variables use a BitTorrent-like peer-to-peer protocol to distribute the variable efficiently. Furthermore, they are distributed once per *Executor*, not per task closure, saving immense network bandwidth.

**Q7:** An `explain(mode="cost")` command will show row counts and data size estimations which are essential for debugging skewed joins.
**Answer:** True
**Mastery Explanation:** Cost-mode explain plans reveal the Catalyst CBO's underlying statistical estimations, helping engineers identify poor query paths and data skew.

**Q8:** In Tungsten's UnsafeRow format, data is always stored in an on-heap binary format to avoid Java serialization during shuffles.
**Answer:** False
**Mastery Explanation:** Tungsten's UnsafeRow operates *off-heap*, allowing Spark to bypass JVM object overhead and Garbage Collection entirely.

**Q9:** Using the `MEMORY_AND_DISK_SER` storage level on an RDD stores it as deserialized Java objects to prioritize fast read access over memory footprint.
**Answer:** False
**Mastery Explanation:** `_SER` denotes that the data is stored as *serialized byte arrays*. This significantly reduces the memory footprint compared to deserialized Java objects, at the cost of CPU cycles required for deserialization upon read.

**Q10:** The Cluster Manager translates declarative DataFrame code into an optimized physical execution plan.
**Answer:** False
**Mastery Explanation:** The Cluster Manager (YARN, K8s) only handles resource allocation (containers). The translation of declarative code into physical plans is strictly the job of the Driver's Catalyst Optimizer.

---

## 2. Multiple Choice Questions (15)

**Q11:** Which phase of the Catalyst Optimizer is responsible for resolving column names against the catalog?
A) Logical Optimization
B) Analysis
C) Physical Planning
D) Whole-Stage Code Generation
**Answer:** B
**Mastery Explanation:** The Analysis phase resolves unresolved attributes and relations against the Spark Catalog (metadata), converting an Unresolved Logical Plan into a Resolved Logical Plan.

**Q12:** What is the primary reason Tungsten aligns data in memory?
A) To comply with JVM object specification
B) To leverage CPU L1/L2 caches and cache-aware algorithms
C) To force data to disk more sequentially
D) To enable compatibility with Python UDFs
**Answer:** B
**Mastery Explanation:** By aligning data densely and avoiding object headers, Tungsten ensures data fits neatly into CPU cache lines, significantly speeding up sorting and hashing operations.

**Q13:** Which of the following best describes the function of `spark.memory.fraction`?
A) Dictates the ratio between heap and off-heap memory
B) Determines how much of the Executor's total JVM heap is dedicated to Spark's execution and storage combined
C) Defines the memory allocated for YARN overhead
D) Sets the threshold for Garbage Collection kicks
**Answer:** B
**Mastery Explanation:** It reserves a portion (default 0.6 or 60%) of the JVM heap strictly for Spark's internal memory management (storage and execution), leaving the rest for user data structures and internal metadata.

**Q14:** Why is `-XX:+UseG1GC` recommended for Executors in heavy ETL jobs?
A) It increases the total heap size dynamically
B) It optimizes garbage collection pauses, preventing pipeline stalls
C) It automatically serializes data to disk
D) It converts RDDs to DataFrames
**Answer:** B
**Mastery Explanation:** The G1 Garbage Collector is designed for large heaps and minimizes "stop-the-world" pause times, which is a common bottleneck in long-running Spark jobs.

**Q15:** What does the `*(1)` notation indicate when inspecting a Spark physical execution plan?
A) A broadcast variable is being utilized
B) Tungsten's Whole-Stage Code Generation is being applied to that segment
C) A disk spill occurred in stage 1
D) The Catalyst CBO has rejected the plan
**Answer:** B
**Mastery Explanation:** Asterisks with numbers (e.g., `*(1)`, `*(2)`) in the `explain()` output denote that the operations within that block have been collapsed into a single generated Java function via Whole-Stage CodeGen.

**Q16:** If a custom UDF references a large local dictionary without broadcasting, what is the architectural consequence?
A) The Executor will throw a ClassNotFoundException
B) The Driver serializes and sends the dictionary over the network for every single task, crushing bandwidth
C) Catalyst automatically optimizes it into a BroadcastHashJoin
D) The dictionary is stored off-heap in Tungsten
**Answer:** B
**Mastery Explanation:** Without `broadcast()`, the Python/Java closure encompasses the variable, forcing the Driver to serialize and transmit it with every task, leading to massive network and deserialization overhead.

**Q17:** What is the role of `spark.sql.adaptive.enabled = true` (Adaptive Query Execution)?
A) It changes the Cluster Manager dynamically based on load
B) It dynamically adapts query plans during runtime based on shuffle statistics
C) It adaptive sizes the Driver memory
D) It switches from RDDs to DataFrames mid-flight
**Answer:** B
**Mastery Explanation:** AQE uses runtime statistics collected during shuffles to re-optimize the logical plan, dynamically coalescing partitions, changing join strategies (e.g., to BroadcastHashJoin), and handling skew.

**Q18:** When using a `broadcast()` hint on a join, what specific operation is Catalyst forced to prioritize?
A) SortMergeJoin
B) ShuffleHashJoin
C) BroadcastHashJoin
D) CartesianProduct
**Answer:** C
**Mastery Explanation:** The hint instructs Catalyst to distribute the smaller DataFrame to all Executors, prioritizing a BroadcastHashJoin which completely avoids the expensive network shuffle required by a SortMergeJoin.

**Q19:** Which partitioning strategy should be used to prevent a single Executor from OOMing due to massive data skews in RDDs?
A) Default Hash Partitioner
B) Range Partitioner on a monotonically increasing ID
C) A custom partitioner using hash mod on a specific key prefix to enforce even distribution
D) Coalesce to 1 partition
**Answer:** C
**Mastery Explanation:** Skew occurs when a few keys have massive data. A custom partitioner breaks or distributes these keys evenly, ensuring no single Executor receives a disproportionate memory load.

**Q20:** In the master-worker topology, what is the primary cause of a Driver OOM before tasks even launch?
A) A complex UDF failing on an Executor
B) The Driver is overwhelmed with metadata from tracking an excessive number of partitions or files
C) Executors writing too much data to HDFS
D) Tungsten failing to allocate off-heap memory
**Answer:** B
**Mastery Explanation:** The Driver holds the metadata for the entire job. Reading millions of tiny files or creating millions of partitions exhausts the Driver's heap during the Catalyst Analysis/Planning phase before any data is processed.

**Q21:** What execution model does Tungsten replace to eliminate massive virtual function call overhead?
A) MapReduce Iterator
B) The Volcano iterator model
C) Catalyst Pipeline
D) DAG Scheduler
**Answer:** B
**Mastery Explanation:** The Volcano model processes one row at a time via virtual function calls across an operator tree. Tungsten's CodeGen compiles the tree into a single function, looping over rows tightly.

**Q22:** How does Tungsten's binary format impact network shuffles?
A) It forces data to be converted to JSON
B) It avoids the heavy CPU cost of Java serialization before transfer
C) It encrypts the payload
D) It requires double the bandwidth
**Answer:** B
**Mastery Explanation:** Because UnsafeRow is already a dense binary byte array, it can be streamed directly over the network or to disk without undergoing expensive JVM object serialization/deserialization.

**Q23:** When you write a DataFrame transformation, what is the immediate output generated before Catalyst optimization begins?
A) An Optimized Physical Plan
B) Java Bytecode
C) An Unresolved Logical Plan
D) A DAG of RDDs
**Answer:** C
**Mastery Explanation:** Your declarative code first builds an Unresolved Logical Plan. Catalyst then resolves it, optimizes it, and finally plans it physically.

**Q24:** Where are broadcast variables cached after distribution?
A) Driver Heap
B) HDFS
C) In the Executor's Block Manager
D) Zookeeper
**Answer:** C
**Mastery Explanation:** Once transferred via the peer-to-peer protocol, broadcast variables reside in the Executor's Block Manager, allowing all tasks on that Executor to share a single read-only copy.

**Q25:** Which Catalyst optimization technique reduces the number of columns passed through the plan?
A) Predicate Pushdown
B) Constant Folding
C) Column Pruning
D) Whole-Stage CodeGen
**Answer:** C
**Mastery Explanation:** Column pruning analyzes the query and drops any columns that are not required for the final output as early as possible, saving memory and I/O.

---

## 3. "Small Twist" Questions (15)

**Q26:** *Scenario:* You are caching an RDD. You change `StorageLevel.MEMORY_AND_DISK` to `StorageLevel.MEMORY_AND_DISK_SER`. What drastically changes?
**Answer:** The memory footprint decreases significantly, but CPU usage for deserialization upon access increases.
**Mastery Explanation:** Standard caching stores full Java objects. `_SER` stores byte arrays. The twist is the tradeoff: you save heap space (preventing OOM/spills) but pay a CPU penalty when reading.

**Q27:** *Scenario:* You join a 10TB table and a 10MB table. By default, it uses SortMergeJoin. You add a `broadcast()` hint to the 10MB table. What changes on the network?
**Answer:** Shuffle network traffic drops to near zero.
**Mastery Explanation:** Instead of hashing and shuffling both the 10TB and 10MB tables across the network, the 10MB table is broadcasted to all nodes. The 10TB table is processed locally, eliminating the massive shuffle phase.

**Q28:** *Scenario:* You increase `spark.memory.storageFraction` from 0.5 to 0.8 to cache more DataFrames. What negative impact might occur during a large `groupBy`?
**Answer:** Execution memory is squeezed, leading to painful disk spills during shuffles/aggregations.
**Mastery Explanation:** Unified memory is shared. If 80% is protected for storage (caching), only 20% remains for execution. Aggregations require execution memory; lacking it forces Spark to spill intermediate data to disk.

**Q29:** *Scenario:* A UDF accesses a 50MB Python dictionary. You wrap it in `spark.sparkContext.broadcast()`. How many times is the dictionary serialized per Executor running 100 tasks?
**Answer:** Once.
**Mastery Explanation:** Without broadcast, it would be serialized 100 times (once per task closure). Broadcasting ensures the Executor pulls it exactly once and caches it for all tasks.

**Q30:** *Scenario:* You change GC algorithm to `-XX:+UseG1GC` and set `InitiatingHeapOccupancyPercent=35`. What changes in task execution?
**Answer:** GC cycles start earlier, preventing long "stop-the-world" pauses.
**Mastery Explanation:** By triggering GC when the heap is only 35% full (instead of the default ~45%), Spark cleans up dead objects more frequently in the background, preventing massive GC spikes that stall executors.

**Q31:** *Scenario:* You call `df.explain(mode="cost")` instead of `df.explain()`. What extra information alters your debugging?
**Answer:** CBO estimations of row counts and data sizes.
**Mastery Explanation:** The twist is that standard explain shows the physical plan, but cost mode shows *why* Catalyst chose it by revealing the statistics it used, which is vital for diagnosing skewed joins or missing table stats.

**Q32:** *Scenario:* Tungsten switches from standard JVM object creation to UnsafeRow. What happens to JVM Garbage Collection overhead?
**Answer:** It is virtually eliminated for cached data.
**Mastery Explanation:** UnsafeRow operates off-heap in a dense binary format. Because these aren't JVM objects, the Garbage Collector doesn't track them, drastically reducing GC pressure.

**Q33:** *Scenario:* You run `df.collect()` and the Driver crashes with OOM. You set `spark.driver.maxResultSize` to "2g". What happens next?
**Answer:** The job aborts gracefully with an exception if the result exceeds 2GB.
**Mastery Explanation:** Instead of the JVM crashing abruptly (which can leave zombie processes or unclear logs), Spark proactively kills the job, protecting the Driver node's stability.

**Q34:** *Scenario:* You define a DataFrame operation. Catalyst applies Predicate Pushdown. How does this alter the source read?
**Answer:** Filters are applied at the storage layer (e.g., Parquet) before data enters Spark memory.
**Mastery Explanation:** Predicate pushdown pushes the `WHERE` clause down to the file reader. Only data matching the filter is read into memory, drastically reducing network I/O and memory pressure.

**Q35:** *Scenario:* A custom RDD partitioner changes `hash(key) % 10` to `hash(key.split("_")[0]) % 500`. What happens to cluster utilization?
**Answer:** Data is distributed across 500 partitions based on the prefix, alleviating executor memory pressure from skewed default hashing.
**Mastery Explanation:** The twist is moving from 10 partitions (low parallelism, high skew probability) to 500 prefix-based partitions, enforcing even data spread and preventing single-executor OOMs.

**Q36:** *Scenario:* You view a physical plan and see no `*(1)` markers. What performance feature is missing?
**Answer:** Tungsten's Whole-Stage Code Generation.
**Mastery Explanation:** Without this, Spark falls back to the Volcano iterator model, incurring massive virtual function call overhead for every row processed.

**Q37:** *Scenario:* You use `spark.sql.shuffle.partitions = 2000` instead of the default 200 for a 10MB dataset. What is the immediate consequence?
**Answer:** Too many small tasks are created, and task scheduling/metadata overhead overwhelms the actual processing time.
**Mastery Explanation:** Over-partitioning tiny data results in thousands of millisecond-long tasks. The Driver spends more time scheduling and serializing tasks than the Executors spend executing them.

**Q38:** *Scenario:* You switch from YARN to Kubernetes as the Cluster Manager. What changes in the Driver's Catalyst Optimizer?
**Answer:** Nothing.
**Mastery Explanation:** Catalyst operates at the logical and physical query plan level. It is completely independent of the Cluster Manager, which only handles physical container allocation.

**Q39:** *Scenario:* You change `spark.executor.memory` from 4g to 16g but keep cores at 1. What resource bottleneck occurs?
**Answer:** CPU starvation / Underutilization.
**Mastery Explanation:** You gave the Executor a massive heap but only 1 thread (core) to process data. The memory is wasted because parallel execution within the JVM is crippled.

**Q40:** *Scenario:* You bypass Catalyst DataFrames and write raw RDD `map` transformations. What Tungsten optimization do you lose?
**Answer:** Whole-Stage Code Generation and UnsafeRow binary format optimizations.
**Mastery Explanation:** RDDs contain opaque Java/Python objects. Catalyst cannot see inside them, meaning it cannot optimize the execution plan or apply Tungsten's off-heap memory formats.

---

## 4. Coding & Debugging Questions (10)

**Q41:** *Debugging:* A streaming job stalls periodically for 10-15 seconds. Logs show no data skew. What JVM configuration is likely missing?
**Answer:** Executor Garbage Collection tuning.
**Mastery Explanation:** Long stalls without data skew almost always point to "Stop-The-World" GC pauses. Adding `-XX:+UseG1GC` to `spark.executor.extraJavaOptions` usually resolves this.

**Q42:** *Code:* `res = massive_df.collect()`. The Driver dies entirely. How do you prevent the crash without changing the application logic?
**Answer:** Set `.config("spark.driver.maxResultSize", "4g")` (or a safe limit).
**Mastery Explanation:** This fails the job with a clear Spark exception rather than a catastrophic JVM OOM crash.

**Q43:** *Code:* Identify the architectural error in this snippet:
```python
lookup = {"A": 1, "B": 2} # 50MB in reality
df.withColumn("val", udf(lambda x: lookup.get(x))(col("id")))
```
**Answer:** `lookup` is not broadcasted.
**Mastery Explanation:** Because it's captured in the closure, the Driver serializes the 50MB dict for *every single task*. Fix: `bc = spark.sparkContext.broadcast(lookup)` and use `bc.value.get(x)`.

**Q44:** *Debugging:* A `SortMergeJoin` is taking hours on a 500GB table and a 50MB table. How do you fix it in code?
**Answer:** Wrap the 50MB table in a `broadcast()` hint.
```python
large_df.join(broadcast(small_df), "id")
```
**Mastery Explanation:** This forces Catalyst to abandon the expensive shuffle required for SortMergeJoin and use a highly efficient BroadcastHashJoin.

**Q45:** *Debugging:* You set `spark.memory.fraction` to 0.99 and `spark.memory.storageFraction` to 0.99. Shuffles are extremely slow and spilling to disk constantly. Why?
**Answer:** Storage is monopolizing the unified memory.
**Mastery Explanation:** You reserved 99% of the Spark memory strictly for caching. This leaves almost 0 bytes for execution memory, forcing shuffles to spill to disk immediately.

**Q46:** *Code:* How do you explicitly verify if the Cost-Based Optimizer (CBO) is generating accurate row estimations in PySpark?
**Answer:** Run `df.explain(mode="cost")`.
**Mastery Explanation:** This outputs the physical plan along with Catalyst's internal statistics (sizeInBytes, rowCount). If these numbers are vastly different from reality, you know table statistics are missing or stale.

**Q47:** *Debugging:* An RDD operation has one task taking 30 minutes while 199 tasks finish in 10 seconds. Memory usage on one executor spikes heavily. What is the fix?
**Answer:** Fix the data skew using a custom partitioner.
**Mastery Explanation:** The default hash partitioner placed massive amounts of data on a single key. Use `.partitionBy(num, custom_hash_func)` to salt the keys and distribute the load evenly.

**Q48:** *Debugging:* Executors are reporting `OutOfMemoryError: Java heap space` when running `rdd.persist()`. How do you alter the code to fix this?
**Answer:** Change the default storage level to `pyspark.StorageLevel.MEMORY_AND_DISK_SER`.
**Mastery Explanation:** Storing as `_SER` (serialized bytes) drastically cuts the memory footprint. Adding `AND_DISK` allows it to safely spill rather than crashing the JVM.

**Q49:** *Code:* In an explain plan, you see a `SortMergeJoin`. You want to verify Whole-Stage Code Generation is active for the preceding stages. What character string do you search for in the output?
**Answer:** `*(1)`, `*(2)`, etc.
**Mastery Explanation:** The asterisk indicates that the node and its children have been compiled into a single Java function by Tungsten.

**Q50:** *Debugging:* The Driver OOMs instantly on application submission. The code reads 10 million tiny JSON files into a DataFrame. Why?
**Answer:** The Driver is overwhelmed with metadata during the Catalyst Analysis phase.
**Mastery Explanation:** The Driver must track the block locations and schema for all 10 million files in heap memory before execution begins. Fix: Compact the files externally or heavily increase `spark.driver.memory`.
