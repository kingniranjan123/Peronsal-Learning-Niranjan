# Spark Ecosystem Mastery Quiz

## Part 1: True/False Questions (10)

**1. True or False:** The Tungsten Execution Engine primarily relies on the standard Java JVM object model to store complex objects like strings and integers in a dense format.
* **Answer:** False
* **Mastery Explanation:** Tungsten actively *bypasses* the standard JVM object model to avoid metadata overhead and GC pauses, utilizing `sun.misc.Unsafe` for off-heap memory allocation in a custom binary format.

**2. True or False:** During the Physical Planning phase, the Catalyst Optimizer generates a single physical execution strategy and directly compiles it into bytecode.
* **Answer:** False
* **Mastery Explanation:** Catalyst generates *multiple* physical execution strategies and uses a cost-based model to select the most optimal one before moving to the Code Generation phase.

**3. True or False:** Standard Python UDFs rely on Py4J for communication, which breaks Whole-Stage Code Generation and forces row-by-row serialization.
* **Answer:** True
* **Mastery Explanation:** Standard UDFs serialize Tungsten's off-heap binary data into Python objects row-by-row via Py4J, destroying vectorized execution. Vectorized Pandas UDFs (using Apache Arrow) are the native solution to prevent this.

**4. True or False:** A Broadcast Hash Join completely eliminates network shuffles by sending the smaller dataset to the BlockManager of every executor JVM.
* **Answer:** True
* **Mastery Explanation:** By building a local hash table in memory on all executors, the larger table can be streamed and joined locally without triggering a cluster-wide shuffle.

**5. True or False:** Sort-Merge Joins are inherently immune to data skew because the sorting phase evenly distributes keys across partitions.
* **Answer:** False
* **Mastery Explanation:** Sort-Merge Joins are highly susceptible to data skew. If a join key is skewed (e.g., millions of 'null' values), a single executor task receives an overwhelming volume of records, often leading to an `OutOfMemoryError`.

**6. True or False:** Structured Streaming leverages a completely separate optimization engine from Spark SQL to handle unbounded data streams.
* **Answer:** False
* **Mastery Explanation:** The entire ecosystem routes all high-level workloads (batch, streaming, ML) through the exact same Catalyst optimizer and Tungsten execution engine.

**7. True or False:** Z-Ordering relies on min/max statistics stored in Parquet file footers to allow Tungsten's vectorized reader to aggressively skip reading irrelevant data blocks.
* **Answer:** True
* **Mastery Explanation:** This synergy allows Tungsten to evaluate the min/max range before reading file data, massively reducing network bandwidth and CPU cycles when querying petabytes of data.

**8. True or False:** Whole-Stage Code Generation compiles entire fragments of a physical plan into multiple virtual function calls to maximize polymorphism.
* **Answer:** False
* **Mastery Explanation:** Whole-Stage Code Generation collapses the physical plan into a *single* massive Java function specifically to *eliminate* virtual function calls and keep data in CPU registers.

**9. True or False:** Kryo serialization is schema-less and heavily optimized, making it fundamentally faster for network shuffles than standard Java serialization.
* **Answer:** True
* **Mastery Explanation:** Java serialization carries heavy reflection and class metadata. Kryo, combined with Tungsten's binary format, allows data to move across the network in the exact layout it holds in memory.

**10. True or False:** In Structured Streaming, omitting a watermark on windowed aggregations ensures that all historical data is perfectly retained in the executor's State Store without risking an OutOfMemoryError.
* **Answer:** False
* **Mastery Explanation:** Omitting a watermark means the executor accumulates aggregation state indefinitely. Without an eviction mechanism, the State Store will eventually exhaust memory and crash the JVM.

---

## Part 2: Multiple Choice Questions (15)

**11. Which of the following phases of the Catalyst Optimizer is responsible for resolving column names against the Catalog?**
A) Logical Optimization
B) Physical Planning
C) Analysis
D) Code Generation
* **Answer:** C
* **Mastery Explanation:** The Analysis phase takes the Unresolved Logical Plan and validates column names, table names, and data types against the Catalog.

**12. Why does Tungsten allocate memory off-heap using `sun.misc.Unsafe`?**
A) To utilize Python's memory manager.
B) To avoid JVM garbage collection (GC) pauses and metadata overhead.
C) To enable Java Object reflection during network shuffles.
D) To bypass the CPU's L1/L2 caches.
* **Answer:** B
* **Mastery Explanation:** JVM objects have immense metadata overhead that triggers devastating GC pauses. Off-heap allocation bypasses this, storing data in dense binary formats.

**13. What cross-language platform do Vectorized Pandas UDFs use to transfer memory between the JVM and Python without serialization overhead?**
A) Py4J
B) Kryo
C) Apache Arrow
D) Protocol Buffers
* **Answer:** C
* **Mastery Explanation:** Apache Arrow provides a standard columnar memory format that both the JVM and Python can read, allowing for zero-copy memory transfer and avoiding row-by-row serialization.

**14. What is the default value of `spark.sql.autoBroadcastJoinThreshold`?**
A) 10 MB
B) 50 MB
C) 1 GB
D) 100 MB
* **Answer:** A
* **Mastery Explanation:** By default, Catalyst will plan a Broadcast Hash Join if one side of the join is under 10MB; otherwise, it defaults to a Sort-Merge Join.

**15. What happens to a Sort-Merge Join when a highly skewed key (like a default 'Unknown' category) is encountered?**
A) Catalyst automatically splits the key across multiple executors.
B) Tungsten recompiles the execution plan dynamically.
C) A single executor task receives a massive volume of records, causing an OutOfMemoryError on the JVM heap.
D) The DAGScheduler drops the skewed records to prevent failure.
* **Answer:** C
* **Mastery Explanation:** Standard Sort-Merge Join hashes all identical keys to the same partition. A heavily skewed key funnels disproportionate data to one task, overwhelming the ShuffleManager's buffer.

**16. Which component is responsible for translating Catalyst's Physical Plan into a Directed Acyclic Graph of stages based on shuffle boundaries?**
A) Tungsten Engine
B) BlockManager
C) TaskScheduler
D) DAGScheduler
* **Answer:** D
* **Mastery Explanation:** The DAGScheduler coordinates execution by breaking the physical plan at shuffle boundaries (e.g., joins, groupBys) to create distinct stages.

**17. What mechanism allows Tungsten to keep data in CPU registers as long as possible?**
A) Off-heap garbage collection
B) Whole-Stage Code Generation
C) Py4J Serialization
D) The Analysis Phase
* **Answer:** B
* **Mastery Explanation:** Whole-Stage Codegen compiles multiple operations into a single Java function, eliminating virtual function calls and allowing the CPU to efficiently pipeline data.

**18. In Structured Streaming, what is the primary purpose of a watermark?**
A) To speed up network shuffles during stateful aggregations.
B) To instruct the State Store when it is safe to evict old window aggregates to prevent OOM errors.
C) To synchronize clocks across all executor JVMs.
D) To convert Python UDFs into Pandas UDFs.
* **Answer:** B
* **Mastery Explanation:** Watermarks bound the late-arriving data allowance, allowing Spark to safely drop state for older windows and keep the memory profile predictable.

**19. How does predicate pushdown reduce I/O overhead?**
A) By caching all data in off-heap memory before filtering.
B) By pushing filters as close to the storage layer as possible, scanning only relevant row groups.
C) By compressing the output of the ShuffleManager.
D) By broadcasting the entire dataset to the driver JVM.
* **Answer:** B
* **Mastery Explanation:** Predicate pushdown ensures that filters are applied at the Parquet/Delta reader level, bypassing the read of entire files or blocks that don't match the predicate.

**20. Which algorithm is used by MLlib's VectorAssembler?**
A) Map-side operations leveraging Tungsten for fast row-to-vector conversion.
B) A Sort-Merge network shuffle to aggregate features.
C) A Broadcast Hash Join across all feature columns.
D) Python row-by-row serialization.
* **Answer:** A
* **Mastery Explanation:** VectorAssembler is an O(N) operation that does not require a shuffle. It operates entirely map-side, using Tungsten's memory format for rapid conversion.

**21. Why is standard Java serialization considered a "death knell" for Spark performance?**
A) It uses Apache Arrow under the hood.
B) It requires heavily optimized, schema-less memory layouts.
C) It involves heavy reflection and class metadata, crushing the CPU during deserialization.
D) It forces Catalyst to bypass the DAGScheduler.
* **Answer:** C
* **Mastery Explanation:** Java serialization is slow and bulky due to metadata. Kryo combined with Tungsten's binary format is the native, high-performance alternative.

**22. How does Z-Ordering interact with Tungsten's vectorized reader?**
A) It forces the reader to shuffle data randomly to avoid skew.
B) It stores min/max statistics in Parquet footers, allowing Tungsten to skip irrelevant data blocks completely.
C) It automatically broadcasts tables larger than 10MB.
D) It converts the data into a standard Java string representation.
* **Answer:** B
* **Mastery Explanation:** Z-Ordering spatially clusters data. The vectorized reader checks the file footer's min/max stats and skips the file entirely if the queried predicate falls outside that range.

**23. What happens to an Unresolved Logical Plan?**
A) It is directly executed by the Tungsten Engine.
B) It is serialized via Py4J and sent to a Python worker.
C) It is analyzed against the Catalog by Catalyst.
D) It is broadcast to all executor JVMs by the BlockManager.
* **Answer:** C
* **Mastery Explanation:** The very first step for an Unresolved Logical Plan (AST) is the Analysis phase, where Catalyst validates it against the Catalog.

**24. The state store used in Structured Streaming on executors is typically backed by:**
A) Redis
B) Apache Kafka
C) RocksDB
D) HDFS
* **Answer:** C
* **Mastery Explanation:** The local state store on executor JVMs, used to track watermarks and aggregations, is frequently backed by RocksDB for high-performance localized storage.

**25. Which statement best describes the difference between Catalyst and Tungsten?**
A) Catalyst handles physical memory; Tungsten handles query parsing.
B) Catalyst is the optimizer for the AST; Tungsten is the physical execution engine generating bytecode.
C) Catalyst is only for Streaming; Tungsten is only for Batch.
D) Catalyst serializes data; Tungsten deserializes data.
* **Answer:** B
* **Mastery Explanation:** Catalyst plans and optimizes the logical/physical execution. Tungsten executes that plan utilizing off-heap memory and Whole-Stage Codegen.

---

## Part 3: "Small Twist" Questions (15)

**26. Scenario:** You join a 3 GB fact table with a 5 MB dimension table. 
*Twist:* You manually change `spark.sql.autoBroadcastJoinThreshold` to 2 MB.
**What happens?**
* **Answer:** Catalyst evaluates the 5 MB table, sees it exceeds the 2 MB threshold, and chooses a Sort-Merge Join. This triggers a cluster-wide shuffle of both tables instead of a Broadcast Hash Join, massively degrading performance.
* **Mastery Explanation:** The physical planner strictly obeys the threshold configuration. Lowering it forces expensive shuffles for small tables that could easily fit in executor memory.

**27. Scenario:** You are using a Vectorized Pandas UDF to run a machine learning prediction.
*Twist:* You configure `spark.sql.execution.arrow.pyspark.enabled = false`.
**What happens?**
* **Answer:** The pipeline falls back to standard Py4J row-by-row serialization. The zero-copy memory transfer is lost, destroying Tungsten's Whole-Stage Codegen benefits and dropping throughput by orders of magnitude.
* **Mastery Explanation:** Pandas UDFs require Arrow to be enabled. Without it, you incur the catastrophic CPU cost of translating off-heap binary data into Python objects sequentially.

**28. Scenario:** You perform a windowed aggregation in Structured Streaming on a 1-minute window.
*Twist:* You apply `.withWatermark("event_time", "1 year")`.
**What happens?**
* **Answer:** The State Store will hold onto all window aggregates for an entire year before evicting them. The executor JVM will likely crash with an `OutOfMemoryError` within days or weeks as state accumulates indefinitely.
* **Mastery Explanation:** A watermark bounds the state size. Setting it to an absurdly long duration effectively removes the eviction mechanism, defeating the purpose of watermarking.

**29. Scenario:** You filter a Delta table partitioned by `date` for a specific day.
*Twist:* You apply a complex regex function (UDF) to the `date` column *before* the filter (e.g., `filter(my_udf(date) == '2026-07-30')`).
**What happens?**
* **Answer:** Predicate pushdown is broken. The Parquet reader must read all partitions and file blocks into memory, apply the UDF, and then filter, resulting in a massive I/O spike.
* **Mastery Explanation:** Catalyst cannot push opaque UDFs down to the storage layer. To leverage partition pruning and min/max stats, filters must be applied to the raw columns directly.

**30. Scenario:** You force a Broadcast Hash Join using the `broadcast()` hint on a dimension table.
*Twist:* The dimension table is unexpectedly 5 GB in size.
**What happens?**
* **Answer:** The driver JVM attempts to collect the 5 GB table to broadcast it, likely resulting in a Driver `OutOfMemoryError`. If the driver survives, the executors may OOM when building the 5 GB hash table in memory.
* **Mastery Explanation:** Broadcasting circumvents the network shuffle but requires the entire table to fit in both the Driver's memory (for distribution) and the Executors' memory.

**31. Scenario:** You join two large tables using a Sort-Merge Join.
*Twist:* 40% of the `join_key` column consists of the value `null`.
**What happens?**
* **Answer:** Extreme data skew occurs. All records with a `null` key are routed to a single executor task during the shuffle, causing an OOM error or a massive straggler task that stalls the pipeline.
* **Mastery Explanation:** The DAGScheduler hashes keys to determine shuffle partitions. Identical keys go to the same partition, overwhelming a single core.

**32. Scenario:** You write a PySpark application processing 100 million rows.
*Twist:* You use a Python `map()` lambda function over the DataFrame RDD instead of DataFrame APIs.
**What happens?**
* **Answer:** You bypass the Catalyst Optimizer entirely. The data is serialized to Python via Py4J, processed slowly, and you lose all benefits of Tungsten, predicate pushdown, and Code Generation.
* **Mastery Explanation:** RDD APIs in Python force serialization across the JVM boundary. The DataFrame API keeps operations inside the JVM, orchestrated by Catalyst.

**33. Scenario:** You query a Parquet file for a specific `customer_id`.
*Twist:* The data was ingested completely randomly, with no Z-Ordering or sorting on `customer_id`.
**What happens?**
* **Answer:** The min/max statistics in the Parquet footers for `customer_id` will likely span the entire range (e.g., Min: A, Max: Z) for every file. Tungsten cannot skip any blocks and must perform a full table scan.
* **Mastery Explanation:** File-skipping relies on tightly clustered data. Without Z-ordering, min/max ranges are useless, resulting in maximum I/O.

**34. Scenario:** A structured streaming job reads from Kafka.
*Twist:* You use an ordinary standard batch `groupBy()` without a time window or watermark.
**What happens?**
* **Answer:** Global aggregation on a streaming DataFrame forces the state store to maintain the state of every key forever. It will inevitably OOM as unbounded data flows in.
* **Mastery Explanation:** Streaming requires bounded state. Standard global groupBys are only safe in bounded batch processing.

**35. Scenario:** You are tuning a Spark job and disable Whole-Stage Codegen (`spark.sql.codegen.wholeStage=false`).
*Twist:* You run a complex mathematical pipeline.
**What happens?**
* **Answer:** The CPU will execute the physical plan via Volcan-style iteration (next() calls) rather than a single compiled function. The pipeline will suffer heavily from virtual function call overhead and poor cache locality.
* **Mastery Explanation:** Whole-Stage Codegen fuses operations to keep data in CPU registers. Disabling it reverts to an inefficient, traditional JVM execution path.

**36. Scenario:** You write data to disk heavily utilizing JVM objects in a MapReduce-like custom Scala job.
*Twist:* You migrate the code identically to Spark without utilizing DataFrames.
**What happens?**
* **Answer:** You will likely suffer massive Garbage Collection (GC) pauses as millions of standard Java objects fill the JVM heap, entirely missing out on Tungsten's off-heap binary format.
* **Mastery Explanation:** Spark's core performance comes from its SQL/DataFrame abstractions bypassing the JVM object model. Native RDD programming with standard objects invites GC hell.

**37. Scenario:** You attempt to use Kryo serialization.
*Twist:* You mistakenly leave a large configuration object un-registered with Kryo and pass it to a map function.
**What happens?**
* **Answer:** Spark falls back to standard Java serialization for that object (if enabled) or throws a serialization exception, severely degrading task startup time and network shuffle speed.
* **Mastery Explanation:** Kryo is schema-less and requires explicit registration of classes for maximum performance. Missing registrations break the optimization.

**38. Scenario:** A Sort-Merge join executes perfectly.
*Twist:* You change the join to an outer join on a non-equi condition (e.g., `df1.value > df2.value`).
**What happens?**
* **Answer:** Catalyst cannot perform a Sort-Merge Join on a non-equi condition. It will degrade into a Broadcast Nested Loop Join, which is essentially a Cartesian product with an O(N^2) complexity, likely causing an OOM or taking hours.
* **Mastery Explanation:** Sort-Merge requires equality (`===`) to align sorted partitions. Inequalities force Spark to compare every row to every other row.

**39. Scenario:** You are running an MLlib VectorAssembler.
*Twist:* The cluster has very slow network bandwidth between nodes.
**What happens?**
* **Answer:** The VectorAssembler executes perfectly without network degradation, because it operates entirely map-side and does not require a shuffle.
* **Mastery Explanation:** VectorAssembler is an O(N) local transformation leveraging Tungsten in-memory; it does not trigger the DAGScheduler to create a shuffle boundary.

**40. Scenario:** Catalyst runs the Logical Optimization phase.
*Twist:* You provided a mathematical query: `select cost * (100 / 100) from table`.
**What happens?**
* **Answer:** Catalyst's rule-based optimizer performs "Constant Folding" and "Expression Simplification," reducing the query to `select cost from table` before generating physical plans.
* **Mastery Explanation:** The logical optimizer cleans up human-generated inefficiencies using algebraic rules before attempting cost-based physical planning.

---

## Part 4: Coding & Debugging Questions (10)

**41. Debug this PySpark pipeline:**
```python
def classify(text):
    import json
    return json.loads(text).get('class')

from pyspark.sql.functions import udf
classify_udf = udf(classify)
df = spark.read.parquet("data")
df.withColumn("class", classify_udf(df["raw_text"])).show()
```
* **Bug/Issue:** Uses a standard Python UDF for row-by-row processing, triggering Py4J serialization and destroying Tungsten optimization.
* **Mastery Fix:** Rewrite using Spark SQL native functions (e.g., `from_json`, `get_json_object`) or a Vectorized Pandas UDF. Native SQL functions stay inside the Tungsten engine.

**42. Debug this Join execution:**
```scala
val transactions = spark.read.parquet("s3a://data/tx") // 500 GB
val rates = spark.read.parquet("s3a://data/rates") // 15 MB
val enriched = transactions.join(rates, "currency_id")
```
* **Bug/Issue:** Because `rates` is 15 MB, it exceeds the default 10 MB `autoBroadcastJoinThreshold`. Catalyst will trigger a 500 GB Sort-Merge shuffle.
* **Mastery Fix:** Wrap `rates` with `broadcast(rates)`. This bypasses the shuffle, streams the 500 GB table in-place, and performs a Broadcast Hash Join.

**43. Debug this Streaming pipeline:**
```scala
val stream = spark.readStream.format("kafka").load()
val agg = stream
  .groupBy(window($"timestamp", "5 minutes"))
  .count()
agg.writeStream.format("console").start()
```
* **Bug/Issue:** Missing a `.withWatermark()` definition.
* **Mastery Fix:** The executor's State Store will accumulate window state indefinitely. Add `.withWatermark("timestamp", "10 minutes")` before the `groupBy` to allow the engine to safely evict old state.

**44. Debug this Data Skew issue:**
```python
# 'status' is 99% 'ACTIVE' and 1% 'INACTIVE'
df1.join(df2, "status").write.parquet("output")
```
* **Bug/Issue:** The highly skewed join key ('ACTIVE') will all hash to the same shuffle partition. One executor task will process 99% of the data, likely throwing an `OutOfMemoryError`.
* **Mastery Fix:** Implement a "Salting" technique. Append a random integer (0 to 10) to the 'ACTIVE' keys in both tables to distribute the skew across 10 partitions, then join on the salted key.

**45. Debug this I/O performance issue:**
```python
df = spark.read.parquet("s3a://data")
filtered = df.withColumn("year", year(df["date"])).filter("year = 2026")
```
* **Bug/Issue:** Applying a function (`year()`) before filtering breaks Predicate Pushdown. The Parquet reader must scan the entire dataset instead of using partition pruning or min/max footers.
* **Mastery Fix:** Filter directly on the raw column: `filter((col("date") >= "2026-01-01") & (col("date") <= "2026-12-31"))`.

**46. Debug this memory allocation assumption:**
```scala
// Submitting a job to YARN
spark-submit --executor-memory 32G --class Main app.jar
```
* **Bug/Issue:** The developer assumes giving 32G to the JVM heap will prevent OOMs. However, Tungsten utilizes *off-heap* memory. 
* **Mastery Fix:** Must also tune `spark.memory.offHeap.enabled=true` and `spark.memory.offHeap.size` if explicitly relying on massive Tungsten allocations, or understand that giving massive heaps only increases GC pause times.

**47. Identify the Catalyst behavior in this code:**
```python
df.filter("age > 21").filter("status = 'ACTIVE'").select("name", "age")
```
* **Observation:** The developer wrote multiple chained filters and selects.
* **Mastery Explanation:** Catalyst's Logical Optimization phase will combine the filters (`age > 21 AND status = 'ACTIVE'`) and push the `select` projection down to the Parquet reader so only `name`, `age`, and `status` are read from disk.

**48. Debug this aggregation pipeline:**
```scala
val rdd = spark.sparkContext.textFile("data.txt")
val counts = rdd.map(word => (word, 1)).groupByKey().mapValues(_.sum)
```
* **Bug/Issue:** Uses legacy RDD `groupByKey()`, which shuffles all values across the network without map-side combining, causing massive network I/O and potential executor OOM.
* **Mastery Fix:** Use the DataFrame API `df.groupBy("word").count()`, which utilizes Catalyst and Tungsten's HashAggregate with map-side partial aggregation.

**49. Debug this serialization crash:**
```scala
class MyConfig(val threshold: Int) 
val config = new MyConfig(10)
df.map(row => row.getInt(0) * config.threshold).count()
```
* **Bug/Issue:** `MyConfig` is not `Serializable`. When the DAGScheduler attempts to send the task closure to the executor JVMs, it will throw a `NotSerializableException`.
* **Mastery Fix:** Make `MyConfig` implement `Serializable`, or better, extract the primitive `threshold` value into a local variable *before* the map function to avoid capturing the entire object.

**50. Debug this PySpark Delta update:**
```python
deltaTable = DeltaTable.forPath(spark, "s3a://table")
deltaTable.update(
  condition = "customer_id = 'A'",
  set = { "status": "'INACTIVE'" }
)
```
* **Observation:** The update takes 45 minutes on a 1 TB table.
* **Bug/Issue:** The Delta table is not partitioned or Z-Ordered by `customer_id`. The engine must rewrite massive numbers of Parquet files to update a few rows.
* **Mastery Fix:** Run `OPTIMIZE s3a://table ZORDER BY (customer_id)` prior to updates. This clusters the data so the update operation only touches the specific Parquet files containing 'A'.
