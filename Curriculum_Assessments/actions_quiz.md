# 🏆 Master Class Assessment: Actions

## Part 1: True/False Questions

**Q1:** A Spark action is the only trigger that commands the Catalyst Optimizer to begin analyzing the lineage graph and executing instructions on the cluster.
**Answer:** True
**Mastery Explanation:** Spark relies on lazy evaluation. Without an action, Spark simply builds a logical plan. The action forces Catalyst to analyze the DAG backward, applying optimizations like predicate pushdown before execution.

**Q2:** When `collect()` is invoked, Tungsten's off-heap memory is directly transferred to the Driver JVM without serialization overhead.
**Answer:** False
**Mastery Explanation:** Executors must serialize the results using the Kryo or default Java serializer, transmitting them over the network via Netty. Tungsten's off-heap format is strictly for execution, not network transfer to the driver.

**Q3:** The DAGScheduler is responsible for dispatching tasks to active executors and handling localized data placement.
**Answer:** False
**Mastery Explanation:** The DAGScheduler computes the graph of stages. It is the `TaskScheduler` that receives these stages and dispatches individual tasks to executors, handling data locality and retries.

**Q4:** If `spark.driver.maxResultSize` is exceeded during a `collect()` operation, Spark gracefully aborts the job before a driver OOM occurs.
**Answer:** True
**Mastery Explanation:** `maxResultSize` acts as a fail-safe. If the serialized results from executors exceed this limit, Spark aborts the job and throws a `SparkException`, preventing a fatal `java.lang.OutOfMemoryError` on the driver.

**Q5:** `cache()` or `persist()` immediately materializes the dataset into the BlockManager.
**Answer:** False
**Mastery Explanation:** `cache()` is a transformation, not an action. It only takes effect lazily after the first action (like `count()`) forces the data to be materialized into the BlockManager.

**Q6:** Calling `df.count()`, `df.show()`, and `df.write.parquet()` sequentially on an uncached DataFrame results in Catalyst executing the entire lineage graph three times.
**Answer:** True
**Mastery Explanation:** Because evaluation is lazy, every distinct action triggers a full recomputation from the source data unless an intermediate state is explicitly cached and materialized.

**Q7:** The `take(n)` action evaluates all partitions concurrently and halts executors once `n` rows are received at the driver.
**Answer:** False
**Mastery Explanation:** `take(n)` scans partitions iteratively. It starts with partition 0, then exponentially increases the number of partitions scanned (1, 4, 16...) until `n` rows are found, which can cause scheduling bottlenecks on heavily filtered data.

**Q8:** Using Whole-Stage Code Generation (WSCG), Tungsten collapses the chain of operations within a stage into a single Java function.
**Answer:** True
**Mastery Explanation:** WSCG eliminates virtual function calls and leverages CPU registers by compiling the entire stage into compact Java bytecode, significantly reducing CPU overhead and GC pauses.

**Q9:** A `ShuffleMapTask` is responsible for computing the final result and sending it back to the driver.
**Answer:** False
**Mastery Explanation:** The `ResultTask` sends data to the driver. The `ShuffleMapTask` computes intermediate data and writes it to local disk for a subsequent stage to consume across a shuffle boundary.

**Q10:** Submitting multiple asynchronous actions from different driver threads allows a single `SparkSession` to process multiple jobs in parallel.
**Answer:** True
**Mastery Explanation:** Spark supports concurrent job submissions from multiple threads. The `TaskScheduler` multiplexes these tasks across executors (especially if FAIR scheduling is enabled), maximizing cluster utilization.

## Part 2: Multiple Choice Questions

**Q11:** Which of the following best describes the network behavior of the `saveAsTable()` action?
- A) Executors send data to the driver, which then writes to distributed storage.
- B) Executors write directly to distributed storage, bypassing the driver.
- C) The DAGScheduler writes data to storage using the BlockManager.
- D) Data is shuffled to a single executor before writing to storage.
**Answer:** B
**Mastery Explanation:** `saveAsTable()` and similar write actions allow executor task threads to write directly to distributed storage (like S3 or HDFS). Bypassing the driver is essential to prevent network bottlenecks and memory limits, ensuring infinite horizontal scalability.

**Q12:** What happens when `df.limit(50000).collect()` is executed?
- A) The driver pulls all data, then limits it to 50000 rows.
- B) Catalyst pushes a `GlobalLimit` to the executors, avoiding driver network traffic.
- C) Catalyst pushes a `LocalLimit` to each executor and a `GlobalLimit` at the driver node.
- D) `take(50000)` is implicitly called, scanning partitions iteratively.
**Answer:** C
**Mastery Explanation:** Catalyst optimizes `limit` by pushing a `LocalLimit` to each executor (restricting rows per partition) and applying a `GlobalLimit` at the driver, drastically reducing network I/O and executor compute time.

**Q13:** Which internal Spark component groups RDD operations into pipelined stages separated by shuffle boundaries?
- A) TaskScheduler
- B) BlockManager
- C) DAGScheduler
- D) Tungsten Engine
**Answer:** C
**Mastery Explanation:** The DAGScheduler analyzes the lineage graph and splits the job into distinct stages based on shuffle dependencies. Operations that don't require a shuffle are pipelined together.

**Q14:** Why is invoking `count()` immediately after `df.persist()` considered a best practice in some pipelines?
- A) `count()` validates the data integrity before caching.
- B) `count()` forces the lazy `persist()` to materialize the data into the BlockManager.
- C) `count()` avoids shuffle boundaries compared to `collect()`.
- D) `count()` is required to initialize Tungsten off-heap memory.
**Answer:** B
**Mastery Explanation:** `persist()` is lazy. A cheap action like `count()` forces data through the physical plan, materializing the `InMemoryRelation`. Subsequent actions will hit the BlockManager instead of recomputing from source.

**Q15:** Which serializer is used by executors to transmit results to the driver during a `collect()` action by default if not explicitly configured?
- A) Tungsten Binary Format
- B) Parquet Vectorized Reader
- C) Java Serializer
- D) Kryo Serializer
**Answer:** C
**Mastery Explanation:** By default, Spark uses the Java serializer to transmit data back to the driver over Netty. Kryo must be explicitly configured, and Tungsten format is only used for internal execution memory.

**Q16:** When a user filters 99.9% of a dataset and invokes `take(10)`, what is the primary performance risk?
- A) Driver OutOfMemoryError
- B) Massive scheduling overhead due to exponential partition scanning
- C) Tungsten off-heap memory exhaustion
- D) Heavy network shuffle
**Answer:** B
**Mastery Explanation:** `take(n)` scans iteratively (1, 4, 16...). If a filter is highly restrictive, Spark will repeatedly coordinate tiny jobs across partitions until it finds 10 rows, causing massive DAGScheduler and TaskScheduler overhead.

**Q17:** What does Tungsten's Whole-Stage Code Generation (WSCG) aim to eliminate?
- A) Network shuffling
- B) Virtual function calls and GC overhead
- C) Lazy evaluation
- D) Driver memory limits
**Answer:** B
**Mastery Explanation:** WSCG collapses operations into a single Java function, leveraging CPU registers and avoiding the overhead of virtual function calls and intermediate Java object creation, thereby minimizing GC pauses.

**Q18:** If a job requires returning data to the driver, which type of task is executed by the worker JVMs?
- A) ShuffleMapTask
- B) DriverTask
- C) ResultTask
- D) TungstenTask
**Answer:** C
**Mastery Explanation:** A `ResultTask` computes the final result and transmits it back to the driver, whereas a `ShuffleMapTask` writes intermediate data to local disk for the next stage.

**Q19:** In a non-blocking architecture, what is the best way to trigger parallel actions on the same DataFrame?
- A) Use `df.foreachPartitionAsync()`
- B) Wrap actions in Scala `Future`s or Python's `ThreadPoolExecutor`
- C) Set `spark.sql.async.execution=true`
- D) Trigger actions inside a UDF
**Answer:** B
**Mastery Explanation:** Submitting jobs from multiple threads (using Futures or ThreadPoolExecutors) allows the DAGScheduler to receive multiple DAGs concurrently, multiplexing them across executors via FAIR scheduling.

**Q20:** Which of the following actions has an O(N) complexity and fundamentally requires a shuffle if not optimized by metadata?
- A) `reduce()`
- B) `collect()`
- C) `take(n)`
- D) `first()`
**Answer:** A
**Mastery Explanation:** `reduce()` requires a commutative and associative function, executing local reductions on executors and then shuffling the results to the driver for the final reduction.

**Q21:** How does `count()` optimize execution when reading directly from un-filtered Parquet or Delta tables?
- A) It skips reading data and returns `0`
- B) It leverages file metadata (e.g., Parquet footers) instead of scanning rows
- C) It pushes a `LocalLimit` to each executor
- D) It caches the data in the BlockManager automatically
**Answer:** B
**Mastery Explanation:** For un-filtered columnar formats like Parquet, Catalyst can optimize `count()` by reading row counts directly from file footers/metadata rather than loading and counting individual rows.

**Q22:** What is the fundamental difference between `collect()` and `take(n)`?
- A) `collect()` uses Kryo, `take()` uses Java Serializer.
- B) `collect()` processes all partitions concurrently; `take()` scans partitions iteratively.
- C) `collect()` executes on the driver, `take()` executes on executors.
- D) `collect()` requires a shuffle, `take()` does not.
**Answer:** B
**Mastery Explanation:** `collect()` triggers execution across all partitions and pulls everything. `take(n)` starts with partition 0 and iterates, preventing unnecessary execution if the `n` rows are found early.

**Q23:** A job crashes with `java.lang.OutOfMemoryError: Java heap space` strictly on the driver node. What is the most likely culprit?
- A) High data skew during a join
- B) `saveAsTable()` on a massive dataset
- C) `collect()` on a dataset larger than `spark.driver.memory`
- D) Tungsten WSCG failure
**Answer:** C
**Mastery Explanation:** `collect()` pulls all serialized partition data to the driver JVM. If the dataset exceeds the driver's heap memory, it throws a fatal OOM error.

**Q24:** When does an `InMemoryRelation` node appear in Catalyst's physical plan?
- A) Whenever `collect()` is called.
- B) When WSCG is enabled.
- C) When `persist()` is intercepted after materialization.
- D) When `take(n)` is executed.
**Answer:** C
**Mastery Explanation:** Calling `persist()` instructs Catalyst to insert an `InMemoryRelation` node. Once materialized by an initial action, subsequent actions read from this node (the BlockManager) rather than the source.

**Q25:** If `spark.driver.maxResultSize` is set to 2GB, and a `collect()` operation attempts to return 3GB of data, what is the exact outcome?
- A) Driver OOM crash.
- B) Executors spill the remaining 1GB to disk.
- C) Spark gracefully aborts the job with a `SparkException`.
- D) Only 2GB of data is returned to the driver, truncating the rest.
**Answer:** C
**Mastery Explanation:** Spark tracks the accumulated serialized size. When it breaches `maxResultSize`, it aborts the job proactively with a `SparkException`, saving the driver from crashing.

## Part 3: "Small Twist" Questions

**Q26:** Scenario: You call `df.count()` on a cached DataFrame. Twist: The cache was created using `StorageLevel.DISK_ONLY`. How does Catalyst execute this action?
**Answer:** It bypasses source recomputation but must read deserialized data from the local executor disks rather than memory.
**Mastery Explanation:** While memory is bypassed, reading from `DISK_ONLY` BlockManager is still much faster than re-executing a complex DAG (like parsing JSON or heavy joins), though slower than `MEMORY_AND_DISK`.

**Q27:** Scenario: You use `limit(50000).collect()`. Twist: The DataFrame is highly skewed, and 49000 rows are in partition 0. What happens?
**Answer:** Partition 0's LocalLimit captures 49000 rows. Other partitions capture their respective limits. The GlobalLimit at the driver easily aggregates and slices the exact 50000 rows safely.
**Mastery Explanation:** Catalyst's `LocalLimit` handles skew gracefully. It doesn't matter if one partition has most of the data; the `GlobalLimit` on the driver will truncate the final aggregated dataset perfectly without OOMing.

**Q28:** Scenario: You run `df.take(10)`. Twist: You add an `.orderBy("timestamp")` before `take(10)`. How does this change the partition scanning?
**Answer:** It completely disables iterative partition scanning, forcing a full cluster-wide execution and shuffle.
**Mastery Explanation:** A global sort (`orderBy`) requires comparing data across all partitions. Catalyst cannot iteratively scan; it must execute the entire DAG, shuffle all data to sort it, and then take the top 10.

**Q29:** Scenario: You execute `df.write.parquet()`. Twist: You include `.partitionBy("date")`. Does this action trigger a shuffle?
**Answer:** Yes, it can trigger a shuffle.
**Mastery Explanation:** Standard writes do not require shuffles, but `partitionBy` forces executors to reorganize data so that rows with the same "date" are written to the appropriate directory, which often induces a shuffle (specifically, a hash partition or sort).

**Q30:** Scenario: You call `df.show()`. Twist: You call `df.show(truncate=False)`. Does this impact driver memory significantly?
**Answer:** Marginally, but it does not pull the whole dataset.
**Mastery Explanation:** `show()` implicitly calls `take(20)`. `truncate=False` just forces the driver to render full strings in the console. It won't cause an OOM unless a single row's string payload is multiple gigabytes.

**Q31:** Scenario: You configure FAIR scheduling and use `ThreadPoolExecutor`. Twist: You submit 50 `count()` actions on the same uncached DataFrame. What is the cluster impact?
**Answer:** The cluster will independently recompute the DAG 50 times simultaneously, causing massive I/O throttling and compute starvation.
**Mastery Explanation:** Async actions do not share intermediate state unless explicitly cached. Submitting 50 async actions on uncached data multiplies the source reads by 50, devastating performance.

**Q32:** Scenario: You want to avoid `collect()` OOMs, so you use `df.toPandas()`. Twist: PyArrow is NOT installed. What is the danger?
**Answer:** Massive memory overhead and slow serialization.
**Mastery Explanation:** Without PyArrow, `toPandas()` falls back to collecting standard Python objects to the driver, which is highly inefficient and memory-intensive, vastly increasing the risk of a driver OOM compared to vectorized Arrow transfers.

**Q33:** Scenario: You call `df.cache()`, then `df.write.parquet()`. Twist: You never call `count()` or `show()` in between. Is the cache utilized?
**Answer:** Yes, the cache is materialized during the `write` action.
**Mastery Explanation:** The `write` action forces execution. Catalyst will cache the data in the BlockManager *while* writing it out. Subsequent actions will hit the cache, but the write itself absorbs the materialization cost.

**Q34:** Scenario: You have `spark.driver.maxResultSize` set to "0" (unlimited). Twist: You call `collect()` on a 5GB dataset with a 4GB driver heap. What happens?
**Answer:** Fatal `java.lang.OutOfMemoryError` on the driver.
**Mastery Explanation:** Setting `maxResultSize` to 0 disables the fail-safe. The driver will attempt to deserialize 5GB of data into a 4GB heap, causing an immediate crash.

**Q35:** Scenario: `df.count()` is extremely fast. Twist: The data is stored in deeply nested raw JSON files (not Parquet). Why is it suddenly slow?
**Answer:** Spark must read, parse, and infer schema for every single JSON string across all partitions.
**Mastery Explanation:** Unlike Parquet which has metadata footers, JSON lacks metadata. Catalyst cannot optimize a `count()` on JSON; it forces Tungsten to scan and parse the entire dataset fully.

**Q36:** Scenario: You run `df.take(10)` on a filtered dataset. Twist: You add `.repartition(100)` before the filter. What is the performance impact on `take()`?
**Answer:** It completely ruins the iterative optimization of `take()`.
**Mastery Explanation:** `repartition()` forces a full cluster-wide shuffle. Even if `take(10)` only needs a few rows, Catalyst must execute the shuffle across the entire dataset *before* the `take` can scan partition 0.

**Q37:** Scenario: You execute a `reduce()` action. Twist: The reduction function is NOT commutative. What happens to the result?
**Answer:** The result becomes non-deterministic and mathematically incorrect.
**Mastery Explanation:** Spark executes `reduce()` locally on partitions and then globally at the driver. If the function isn't commutative/associative, the arbitrary order of partition completion alters the final result unpredictably.

**Q38:** Scenario: You call `df.unpersist()` immediately after an action. Twist: The unpersist is called with `blocking=True`. What is the impact?
**Answer:** The driver thread pauses until all executors have confirmed eviction of the blocks from memory/disk.
**Mastery Explanation:** By default, `unpersist()` is asynchronous. Blocking forces the driver to wait, guaranteeing memory is freed before the next line of code, preventing memory pressure in tight loops at the cost of execution delay.

**Q39:** Scenario: You use `collect()` on a tiny aggregated dataset (100 rows). Twist: You did this inside a `foreachPartition` loop. What happens?
**Answer:** `NullPointerException` or `SparkException`.
**Mastery Explanation:** You cannot invoke an action (which requires communicating with the Driver/SparkContext) from within an executor task (like `foreachPartition`). Actions can only be invoked on the driver.

**Q40:** Scenario: `df.limit(10).collect()` works perfectly. Twist: You change it to `df.limit(10).write.parquet()`. What is the file output structure?
**Answer:** Multiple small files (or one file), but total rows across all files equal 10.
**Mastery Explanation:** `limit()` pushes the `GlobalLimit` to the single partition handling the final stage. The write action writes exactly 10 rows, likely into a single small Parquet file, without routing back to the driver.

## Part 4: Coding & Debugging Questions

**Q41:** 
```python
df = spark.read.csv("data.csv")
df_clean = df.dropna()
df_clean.cache()
df_clean.write.parquet("out1/")
df_clean.write.parquet("out2/")
```
**Issue:** Is there a redundant compute penalty here?
**Answer:** No. 
**Mastery Explanation:** `cache()` is materialized during the first action (`out1/`). The second action (`out2/`) safely reads from the BlockManager. No dummy `count()` is strictly required if the first action isn't critically latency-sensitive.

**Q42:**
```python
results = spark.table("huge_table").filter("val > 100").collect()
```
**Issue:** What is the critical production vulnerability?
**Answer:** High risk of Driver OOM.
**Mastery Explanation:** The filter's selectivity is unknown. If it returns 500 million rows, `collect()` will crash the driver. A `limit()` or aggregation must precede it.

**Q43:**
```scala
val df = spark.read.parquet("data")
Future { df.count() }
Future { df.show() }
```
**Issue:** If `df` is complex, what is the cluster impact?
**Answer:** Both futures trigger simultaneous execution of the full DAG.
**Mastery Explanation:** Since the DAG isn't cached, asynchronous execution causes double the I/O and compute load on the cluster simultaneously, potentially throttling storage APIs.

**Q44:**
```python
safe_df = df.limit(5000000).collect()
```
**Issue:** The developer added a limit to be "safe", but the driver still crashed. Why?
**Answer:** 5 million rows of wide data easily exceeds `spark.driver.memory`.
**Mastery Explanation:** `limit()` only bounds the row count. If each row is 1KB, 5M rows is 5GB. If the driver heap is the default 1GB, it will still OOM. Limit must align with physical memory bounds.

**Q45:**
```python
def process(partition):
    df_lookup = spark.table("lookup").collect() # Action inside worker
    # process logic...

df.rdd.foreachPartition(process)
```
**Issue:** Why does this fail immediately?
**Answer:** Nested actions / SparkContext not available on executors.
**Mastery Explanation:** You cannot trigger an action (which requires `SparkContext`) from inside an executor thread (`foreachPartition`). Data must be broadcasted or joined beforehand.

**Q46:**
```python
df = spark.read.parquet("data")
df.withColumn("new", col("id") * 2).cache()
df.count()
```
**Issue:** Why is the cache totally useless here?
**Answer:** The `.cache()` was applied to a transformed DataFrame that wasn't assigned to a variable.
**Mastery Explanation:** `df.withColumn(...).cache()` returns a new DataFrame. Since it's not assigned, the subsequent `df.count()` executes against the original uncached `df`. The cached branch is garbage collected and never materialized.

**Q47:**
```python
df.orderBy("id").take(5)
```
**Issue:** Developer expects this to be fast like `take(5)`. Why does it take 10 minutes?
**Answer:** `orderBy` forces a global shuffle.
**Mastery Explanation:** To find the top 5 absolute lowest IDs, Catalyst must sort the entire dataset across all partitions. The iterative optimization of `take()` is nullified by the blocking shuffle stage.

**Q48:**
```python
total = df.rdd.map(lambda x: x.value).reduce(lambda a, b: a - b)
```
**Issue:** Running this multiple times yields different results. Why?
**Answer:** Subtraction is not commutative/associative.
**Mastery Explanation:** `reduce()` requires a commutative and associative function because partitions are aggregated in non-deterministic order. `a - b` violates this, leading to unpredictable results.

**Q49:**
```python
for i in range(100):
    df.filter(col("id") == i).write.parquet(f"out/{i}")
```
**Issue:** What is the architectural flaw in this loop?
**Answer:** It triggers 100 separate sequential jobs, re-evaluating the full DAG 100 times.
**Mastery Explanation:** A loop triggering actions sequentially causes the DAGScheduler to run 100 full cluster jobs. The correct approach is to use `partitionBy("id")` in a single write action, letting Tungsten handle it in one pass.

**Q50:**
```python
spark.conf.set("spark.driver.maxResultSize", "0")
data = df.collect()
```
**Issue:** What safety mechanism did the developer just disable?
**Answer:** The serialization threshold abort mechanism.
**Mastery Explanation:** Setting `maxResultSize` to 0 means "unlimited." If `df` is 50GB, the executors will blindly blast 50GB to the driver, guaranteeing a catastrophic `OutOfMemoryError` that kills the application.
