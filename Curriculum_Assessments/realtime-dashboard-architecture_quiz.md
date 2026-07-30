# Realtime Dashboard Architecture Assessment

## Part 1: True/False Questions (1-10)

**1. Using `HDFSBackedStateStore` for large stateful streaming aggregations tracking millions of users will likely lead to severe JVM Garbage Collection pauses.**
* **Correct Answer:** True
* **Mastery Explanation:** `HDFSBackedStateStore` stores state objects in JVM memory. With millions of keys, the heap fills up, causing long GC pauses that spike micro-batch latency. Off-heap storage like RocksDB avoids this.

**2. Tungsten's `UnsafeRow` format increases serialization overhead because it requires converting binary data into complex Java objects before Catalyst can process it.**
* **Correct Answer:** False
* **Mastery Explanation:** Tungsten eliminates this overhead by keeping data in a dense binary format (`UnsafeRow`). Catalyst generates bytecode that manipulates these bytes directly, avoiding Java object instantiation entirely.

**3. Spark's Continuous Processing Mode supports all stateful operations (like sliding window aggregations) but guarantees sub-millisecond latency.**
* **Correct Answer:** False
* **Mastery Explanation:** Continuous Processing Mode currently only supports map-like operations (projections, filters). It does not support stateful aggregations or sorting.

**4. When writing a custom sink for a realtime dashboard, initializing a database connection pool inside the driver and passing it to `foreachPartition` is an anti-pattern that causes serialization errors.**
* **Correct Answer:** True
* **Mastery Explanation:** Connection pools are not serializable. If created on the driver, Spark cannot send them to executors. They must be instantiated on the executor, typically inside `foreachPartition`.

**5. Watermarking is strictly used to delay the output of data to the sink in `append` mode; it has no impact on the internal state store memory usage.**
* **Correct Answer:** False
* **Mastery Explanation:** Watermarking is crucial for bounding state memory. It tells Spark when it is safe to discard old intermediate aggregates, preventing OutOfMemory (OOM) errors over time.

**6. Catalyst treats streaming queries as incremental execution plans and can apply optimizations like predicate pushdown to reduce the data entering Tungsten's memory.**
* **Correct Answer:** True
* **Mastery Explanation:** Catalyst optimizes streaming plans much like batch plans. Pushing filters down to the source (e.g., Kafka) reduces serialization and memory overhead.

**7. While RocksDB avoids JVM GC pressure, it introduces a massive CPU bottleneck due to JNI serialization that makes it slower than `HDFSBackedStateStore` for all workloads.**
* **Correct Answer:** False
* **Mastery Explanation:** While there is a small JNI serialization overhead, eliminating unpredictable, massive GC pauses makes RocksDB the preferred choice for large state. Predictable latency is more critical than minor JNI overhead.

**8. Continuous Processing Mode uses the Chandy-Lamport algorithm to achieve asynchronous checkpointing without micro-batch scheduling overhead.**
* **Correct Answer:** True
* **Mastery Explanation:** To provide at-least-once guarantees without blocking the continuous stream, Continuous Mode uses this distributed snapshot algorithm for checkpointing.

**9. In `foreachBatch`, using `repartition(N)` prior to `foreachPartition` is a mechanism to strictly control the number of concurrent database connections opened against the sink.**
* **Correct Answer:** True
* **Mastery Explanation:** `repartition(N)` sets the exact number of partitions (and thus tasks). Since a connection pool is initialized per partition, this caps the concurrent connections, preventing connection exhaustion at the sink.

**10. Continuous Processing Mode currently provides exact-once processing guarantees.**
* **Correct Answer:** False
* **Mastery Explanation:** It currently only provides at-least-once guarantees, unlike the default micro-batch mode which provides exactly-once guarantees.

## Part 2: Multiple Choice Questions (11-25)

**11. Which state store provider is most appropriate for a Spark Structured Streaming dashboard aggregating 50 million distinct users per hour?**
A) HDFSBackedStateStore
B) MemoryStateStore
C) RocksDBStateStoreProvider
D) RedisStateStoreProvider
* **Correct Answer:** C
* **Mastery Explanation:** RocksDB manages its own memory off-heap via JNI, completely bypassing JVM GC limits, making it the only viable native option for massive state.

**12. What is the primary purpose of `pipeline.sync()` when writing data to Redis in a `foreachPartition` block?**
A) To serialize the dataframe schema
B) To batch network requests at the TCP level and reduce round-trips
C) To force Spark to checkpoint the micro-batch
D) To clear the Redis cache before writing
* **Correct Answer:** B
* **Mastery Explanation:** Redis pipelining sends multiple commands to the server without waiting for individual replies, drastically reducing network latency. `sync()` executes the batch.

**13. In a streaming aggregation without a watermark, what happens to the state store over 30 days of continuous operation?**
A) Spark automatically compacts it every hour.
B) The state store grows indefinitely until an OutOfMemory (OOM) error occurs.
C) Spark writes older state to Kafka.
D) Catalyst optimizer drops inactive keys after 24 hours.
* **Correct Answer:** B
* **Mastery Explanation:** Without a watermark, Spark does not know when late data will stop arriving, so it must keep all window aggregates in state forever, leading to OOM.

**14. Why does Continuous Processing Mode achieve lower latency than micro-batching?**
A) It bypasses Catalyst and Tungsten entirely.
B) It stores all data in Redis instead of Spark memory.
C) It eliminates the overhead of planning and scheduling tasks on executors for every batch.
D) It uses UDP instead of TCP for Kafka consumption.
* **Correct Answer:** C
* **Mastery Explanation:** In micro-batching, Spark schedules tasks for every interval. Continuous mode launches long-running tasks once that eagerly poll partitions, removing scheduling latency.

**15. If a dashboard requires updating the UI with intermediate aggregation results as the window progresses, which output mode must be used?**
A) Append Mode
B) Complete Mode
C) Update Mode
D) Continuous Mode
* **Correct Answer:** C
* **Mastery Explanation:** Update mode outputs only the rows that were updated in the current micro-batch. Append mode waits until the watermark passes to output anything.

**16. What does the configuration `spark.sql.streaming.stateStore.rocksdb.compactOnCommit` do?**
A) It compresses Kafka messages.
B) It triggers RocksDB compactions during the micro-batch commit phase to control state store growth.
C) It merges small files in HDFS checkpoints.
D) It compacts the Tungsten bytecode.
* **Correct Answer:** B
* **Mastery Explanation:** This setting forces RocksDB to compact its SST files when committing state, preventing disk usage bloat and maintaining read/write performance at the cost of slightly higher commit latency.

**17. When implementing a custom sink via `foreachBatch`, what do the parameters of the provided function represent?**
A) `(Dataset<Row> batchData, Long batchId)`
B) `(Iterator<Row> rows, String partitionId)`
C) `(DataFrame df, Boolean isFinal)`
D) `(String topic, Long offset)`
* **Correct Answer:** A
* **Mastery Explanation:** `foreachBatch` takes a function that receives the micro-batch as a standard batch DataFrame/Dataset and a unique `batchId`.

**18. How does Tungsten's `UnsafeRow` handle strings?**
A) As `java.lang.String` objects on the heap.
B) As pointers to Kafka offsets.
C) Using UTF-8 encoded bytes stored natively in a memory array.
D) Using a distributed Redis cache.
* **Correct Answer:** C
* **Mastery Explanation:** Tungsten avoids object overhead by encoding strings directly as UTF-8 byte arrays inside its off-heap or contiguous on-heap memory regions.

**19. You are building an alerting dashboard looking for 5 failed logins within 10 minutes. Which execution model must you use?**
A) Continuous Processing Mode
B) Micro-batching with Stateful Aggregation
C) MapReduce
D) Stateless `foreachBatch`
* **Correct Answer:** B
* **Mastery Explanation:** Continuous mode does not support aggregations. Tracking counts over a time window requires stateful aggregations, which is only supported in the micro-batch model.

**20. Which operation is fully supported in Structured Streaming Continuous Processing Mode?**
A) `.groupBy("user_id").count()`
B) `.orderBy("timestamp")`
C) `.selectExpr("CAST(value AS STRING)").filter("status = 'ERROR'")`
D) `.join(static_df, "id")`
* **Correct Answer:** C
* **Mastery Explanation:** Only map-like operations (projections, selections, filters) are currently supported in Continuous mode.

**21. What happens if a Spark executor running a RocksDB state store crashes?**
A) The state is lost forever.
B) Spark reads the state from the driver's memory.
C) The new executor recovers the state from the distributed checkpoint location (e.g., S3/HDFS).
D) Kafka automatically replays the state.
* **Correct Answer:** C
* **Mastery Explanation:** RocksDB asynchronously backs up its data to the checkpoint location. On failure, Spark tasks recreate the RocksDB instance and load the state from the checkpoint.

**22. Which Spark mechanism allows for the combination of streaming data with static reference data (e.g., enriching user IDs with names)?**
A) Watermarking
B) Stream-Static Joins
C) Continuous Aggregation
D) Whole-stage Code Generation
* **Correct Answer:** B
* **Mastery Explanation:** Spark supports joining a streaming DataFrame with a static DataFrame, executing the join continuously as new stream data arrives.

**23. Why is `coalesce()` generally dangerous when used on the streaming DataFrame before writing to a sink in `foreachBatch`?**
A) It corrupts Tungsten memory.
B) It triggers a full shuffle and reduces parallelism to a single executor, causing a bottleneck.
C) It deletes the RocksDB state store.
D) It drops late data automatically.
* **Correct Answer:** B
* **Mastery Explanation:** `coalesce` minimizes partitions on the current stage, forcing all data to a smaller number of executors, destroying write throughput. `repartition` is preferred if even distribution is needed.

**24. In the context of the Catalyst optimizer, how does `select(from_json(...))` behave?**
A) It falls back to Python execution, bypassing Catalyst.
B) It generates a JVM object for every JSON record.
C) It parses JSON natively using optimized internal expressions and generates Tungsten rows.
D) It sends the JSON to the driver for parsing.
* **Correct Answer:** C
* **Mastery Explanation:** `from_json` is a built-in Catalyst expression. It parses JSON efficiently and directly populates Tungsten's `UnsafeRow` structure without creating generic Java objects.

**25. A streaming query with `outputMode("append")` and a 10-minute watermark receives an event that is 15 minutes late. What does Spark do?**
A) Throws an exception and halts the stream.
B) Updates the previous output in the sink.
C) Drops the event silently.
D) Writes the event to a dead-letter queue automatically.
* **Correct Answer:** C
* **Mastery Explanation:** In append mode with a watermark, data older than the watermark is dropped because the state for that window has already been cleared and finalized.

## Part 3: Small Twist Questions (26-40)

**26. SCENARIO:** You have a streaming pipeline outputting to Redis via `foreachPartition`.
**TWIST:** You move the `new JedisPool()` initialization from *inside* `foreachPartition` to the line *immediately before* `foreachPartition` (still inside `foreachBatch`).
* **Correct Answer:** The job crashes with a `NotSerializableException`.
* **Mastery Explanation:** Code inside `foreachBatch` but outside `foreachPartition` executes on the driver. The `JedisPool` is created on the driver, but it cannot be serialized and sent over the network to the executors.

**27. SCENARIO:** You are calculating a 1-hour windowed aggregation with a 10-minute watermark. You use `outputMode("update")`.
**TWIST:** You change the output mode to `outputMode("append")`.
* **Correct Answer:** The dashboard stops updating in real-time; results are only emitted once, 10 minutes *after* the 1-hour window closes.
* **Mastery Explanation:** Append mode waits until the watermark completely passes the window end time to ensure no more late data can arrive before emitting the final, immutable result.

**28. SCENARIO:** You configure `RocksDBStateStoreProvider` for your streaming job.
**TWIST:** You deploy the application to a cluster where the executor instances have large heaps but severely limited disk space (e.g., 5GB local disk).
* **Correct Answer:** The executors crash with `No space left on device` (Disk Full) errors.
* **Mastery Explanation:** RocksDB writes state to local disk (SST files). If local disk is highly constrained, RocksDB will fail, bringing down the executor, regardless of JVM heap size.

**29. SCENARIO:** You have a filter `.filter("status = 'CRITICAL'")` in Continuous Processing Mode.
**TWIST:** You change it to `.groupBy("status").count()`.
* **Correct Answer:** The query throws an `AnalysisException` during initialization.
* **Mastery Explanation:** Continuous processing mode does not support aggregations. Catalyst detects this during planning and fails immediately.

**30. SCENARIO:** A developer uses `df.withWatermark("timestamp", "10 minutes")` on a streaming dataset.
**TWIST:** The source Kafka topic is currently experiencing a network partition and no new data has arrived for 2 hours.
* **Correct Answer:** The watermark does not advance, and no pending windows are finalized.
* **Mastery Explanation:** Watermarks are event-time driven. If no new events arrive to push the maximum observed event time forward, the watermark stalls.

**31. SCENARIO:** You are tracking active users with `countDistinct("user_id")` using `HDFSBackedStateStore`.
**TWIST:** The application runs fine on Monday but starts experiencing 30-second micro-batch latencies by Friday.
* **Correct Answer:** The JVM heap is suffering from severe GC overhead due to accumulating state.
* **Mastery Explanation:** As unique users accumulate throughout the week, the JVM heap fills up with state objects, causing major garbage collection pauses. RocksDB is needed.

**32. SCENARIO:** You write to a SQL database in `foreachBatch` using `batchDF.write.jdbc()`.
**TWIST:** You add `.cache()` to `batchDF` before writing it.
* **Correct Answer:** You consume executor memory unnecessarily for a write-once operation, potentially causing eviction or OOM.
* **Mastery Explanation:** `batchDF` is only used once in this block. Caching it provides no reuse benefit and wastes memory. (Note: Caching inside `foreachBatch` is only useful if writing the *same* batch to multiple sinks).

**33. SCENARIO:** You are executing a stream-stream join. Both streams have watermarks.
**TWIST:** You remove the time constraint in the join condition (e.g., `stream1.time BETWEEN stream2.time AND stream2.time + 1 hour`).
* **Correct Answer:** Spark state grows infinitely, eventually causing an OOM.
* **Mastery Explanation:** For stream-stream joins, Spark requires watermarks on both sides AND a time interval condition to know when it is safe to discard old state. Without the interval, state cannot be cleaned up.

**34. SCENARIO:** Your Kafka source processes 1M events/sec. You use `.select(from_json(...))`.
**TWIST:** You replace `from_json` with a Python UDF `parse_json_udf(col("value"))`.
* **Correct Answer:** Throughput drops drastically and CPU usage spikes.
* **Mastery Explanation:** Python UDFs force data out of Tungsten's memory, serialize it to a Python worker process via Py4J, execute the function, and serialize it back. Built-in `from_json` is natively executed in Tungsten.

**35. SCENARIO:** You are writing metrics to Redis via pipelines in `foreachPartition`.
**TWIST:** The developer forgets to call `pipeline.sync()` before closing the connection.
* **Correct Answer:** No data is written to Redis.
* **Mastery Explanation:** `pipeline` queues commands in memory. `sync()` (or `execute()`) is required to actually send the byte buffer over the network to the Redis server.

**36. SCENARIO:** You are running Continuous Processing mode with a 1-second checkpoint interval.
**TWIST:** The backend database experiences a 5-second network timeout.
* **Correct Answer:** The continuous tasks block on the write, and Kafka consumer lag increases.
* **Mastery Explanation:** Continuous mode pushes data as fast as the sink can take it. If the sink blocks, backpressure naturally occurs, halting consumption.

**37. SCENARIO:** You use `outputMode("complete")` to output a total count of events.
**TWIST:** The data volume is 10 billion rows over 5 hours.
* **Correct Answer:** The query executes fine, but writing the output to the sink becomes a massive bottleneck in every batch.
* **Mastery Explanation:** Complete mode forces Spark to output the *entire* state table to the sink in every micro-batch. With huge state, this creates an enormous network and I/O bottleneck.

**38. SCENARIO:** You set `spark.sql.streaming.stateStore.rocksdb.compactOnCommit = true`.
**TWIST:** You change it to `false` to reduce micro-batch latency.
* **Correct Answer:** Micro-batch commit latency drops, but local disk usage grows rapidly.
* **Mastery Explanation:** Without compaction on commit, RocksDB accumulates many overlapping SST files. Disk space balloons, and eventually, read performance degrades due to scanning multiple files.

**39. SCENARIO:** A dashboard UI reads directly from Spark's memory via JDBC/Thrift.
**TWIST:** You change the architecture to write Spark's output to Redis, and the UI reads from Redis.
* **Correct Answer:** Dashboard query latency drops from seconds to milliseconds, and concurrency limits scale massively.
* **Mastery Explanation:** Spark is an analytics engine, not a low-latency serving database. Direct JDBC queries to Spark compete for resources and have high overhead. Redis is built for sub-millisecond key-value serving.

**40. SCENARIO:** You configure `spark.sql.streaming.stateStore.providerClass` to RocksDB.
**TWIST:** You run the code on Spark 2.4 without installing the third-party Databricks RocksDB package.
* **Correct Answer:** The application throws a `ClassNotFoundException`.
* **Mastery Explanation:** Native RocksDB state store support was added in Apache Spark 3.2. On older open-source versions, the class does not exist unless a custom plugin is provided.

## Part 4: Coding & Debugging Questions (41-50)

**41. DEBUGGING:**
```python
def process_batch(df, epoch_id):
    df.write.format("jdbc").save()
    df.write.format("kafka").save()

streaming_df.writeStream.foreachBatch(process_batch).start()
```
* **Error:** The batch is recomputed twice from the source.
* **Correction/Explanation:** Because `df` is an un-cached Spark dataframe, executing two write actions triggers two separate Spark jobs. `df.cache()` must be called at the start of `process_batch` (and `df.unpersist()` at the end) to avoid re-reading from Kafka.

**42. DEBUGGING:**
```scala
val pool = new JedisPool("host", 6379)
df.writeStream.foreachBatch { (batchDF, id) =>
  batchDF.foreach { row =>
    val jedis = pool.getResource()
    jedis.set(row.getString(0), row.getString(1))
  }
}.start()
```
* **Error:** `NotSerializableException`.
* **Correction/Explanation:** `JedisPool` is initialized on the driver. It cannot be serialized to the executors where `batchDF.foreach` runs. Move `new JedisPool` inside `foreachPartition`.

**43. DEBUGGING:**
```python
df = spark.readStream.format("kafka").load()
agg_df = df.groupBy("user_id").count()
agg_df.writeStream.outputMode("append").format("console").start()
```
* **Error:** `AnalysisException: Append output mode not supported when there are streaming aggregations on streaming DataFrames/DataSets without watermark`.
* **Correction/Explanation:** You cannot use `append` mode with aggregations unless a watermark is defined, because Spark must know when a window is closed to append it safely.

**44. DEBUGGING:**
```scala
batchDF.repartition(1000).foreachPartition { partition =>
  val dbConnection = DriverManager.getConnection("jdbc:...")
  // write data
}
```
* **Error:** Database connection exhaustion (Too many connections).
* **Correction/Explanation:** `repartition(1000)` forces 1000 partitions. This creates 1000 concurrent database connections in the micro-batch, likely overwhelming the sink database. Use a smaller number of partitions (e.g., 10-50).

**45. DEBUGGING:**
```python
parsed = df.selectExpr("CAST(value AS STRING)")
alerts = parsed.filter(lambda row: "ERROR" in row.value)
```
* **Error:** In a streaming context, using RDD-like lambdas (`filter(lambda...)`) breaks Catalyst optimization.
* **Correction/Explanation:** Using Python lambdas forces data out of Tungsten into Python objects. Use Catalyst expressions instead: `parsed.filter("value LIKE '%ERROR%'")`.

**46. DEBUGGING:**
```scala
df.writeStream
  .trigger(Trigger.Continuous("1 second"))
  .foreachBatch { (df, id) => df.write.jdbc(...) }
  .start()
```
* **Error:** `foreachBatch` is not supported in Continuous Processing Mode.
* **Correction/Explanation:** Continuous mode only supports writing directly to streaming sinks (like Kafka or console). `foreachBatch` requires micro-batch semantics.

**47. DEBUGGING:**
```python
stream = spark.readStream.load()
grouped = stream.groupBy(window("time", "1 hour"))
grouped.orderBy("count")
```
* **Error:** Global sorting is not supported in streaming.
* **Correction/Explanation:** Sorting an unbounded stream requires knowing all data, which is impossible. You can only sort within a `foreachBatch` on the micro-batch dataframe, not on the unbounded streaming dataframe.

**48. DEBUGGING:**
```scala
// Running with HDFSBackedStateStore
val stream = df.withWatermark("timestamp", "5 minutes")
  .groupBy("user_id") // High cardinality, 100M users
  .count()
```
* **Error:** Severe GC Pauses leading to OOM.
* **Correction/Explanation:** Grouping by a high-cardinality key like `user_id` creates massive state. `HDFSBackedStateStore` stores this on-heap. Switch to `RocksDBStateStoreProvider`.

**49. DEBUGGING:**
```scala
partition.foreach { row =>
  val jedis = pool.getResource
  jedis.set(row.getAs[String]("id"), "1")
  jedis.close()
}
```
* **Error:** High network latency / Bottleneck.
* **Correction/Explanation:** Doing a network round-trip for every single row (`jedis.set`) defeats the purpose of high-throughput streaming. Use Redis pipelines (`jedis.pipelined()`) to batch commands.

**50. DEBUGGING:**
```python
spark.conf.set("spark.sql.streaming.stateStore.providerClass", "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
# No checkpoint location defined
df.writeStream.start()
```
* **Error:** `AnalysisException: checkpointLocation must be specified`.
* **Correction/Explanation:** Stateful streaming (especially with RocksDB) strictly requires a distributed checkpoint location to persist the state files. Add `.option("checkpointLocation", "s3://...")` to the `writeStream`.
