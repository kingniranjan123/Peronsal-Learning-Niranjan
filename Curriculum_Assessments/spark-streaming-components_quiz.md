# 🔥 Master Class Assessment: Spark Streaming Components

## 1. True/False Questions (10 Questions)

Q1. True or False: Checkpoint metadata in Spark Streaming is serialized using the Kryo serializer by default to minimize memory footprint and latency.
Answer: False
Mastery Explanation: Checkpoint metadata uses Java serialization, not Kryo. This is because DStream subclasses and user closures must be fully reconstructible across JVM restarts, and Kryo's lack of class registration enforcement makes it unsuitable for cross-restart deserialization.

Q2. True or False: Enabling Write-Ahead Logs (WAL) in Spark Streaming is sufficient to guarantee end-to-end exactly-once semantics across a pipeline.
Answer: False
Mastery Explanation: The WAL only provides exactly-once *processing* (input recovery) guarantees. If an output action (e.g., writing to a DB) is not idempotent and a failure occurs mid-batch, data will be duplicated. End-to-end exactly-once requires transactional or idempotent sinks.

Q3. True or False: The DStreamGraph is eagerly evaluated at transformation-definition time to allow Catalyst optimization across multiple micro-batches.
Answer: False
Mastery Explanation: The DStreamGraph is a DAG of DStream objects evaluated lazily. `generateJobs()` triggers a depth-first traversal that builds the RDD DAG on-demand for every batch interval.

Q4. True or False: In a Receiver-based ingestion model, reducing `spark.streaming.blockInterval` increases the number of tasks processing the stream.
Answer: True
Mastery Explanation: The block interval determines how often a receiver seals a new block. Since 1 Block = 1 BlockRDD partition = 1 Task, a smaller block interval yields more partitions per batch, increasing task-level parallelism.

Q5. True or False: A `DirectKafkaInputDStream` requires setting `spark.streaming.blockInterval` carefully to ensure sufficient parallelism during Kafka ingestion.
Answer: False
Mastery Explanation: Direct streams do not use Receivers or blocks. The parallelism (number of RDD partitions) maps exactly 1:1 to the number of Kafka topic-partitions.

Q6. True or False: When `reduceByKeyAndWindow` is provided with an inverse reduce function, its computational complexity scales with the size of the window (number of batches in the window).
Answer: False
Mastery Explanation: With an inverse function, Spark uses an incremental algorithm (O(new_batch + old_batch)) that only processes the new batches entering the window and the old batches leaving, making it independent of the total window size.

Q7. True or False: The `ReceiverTracker` assigns records to micro-batches by atomically snapshotting the current set of pending blocks when the `JobGenerator` calls `allocateBlocksToBatch(time)`.
Answer: True
Mastery Explanation: This atomic snapshot is the linearization point that dictates precisely which records belong to which micro-batch, ensuring no block is assigned to two batches.

Q8. True or False: Without `spark.streaming.backpressure.enabled = true`, a slow processing pipeline will cause executors to OOM before the Driver OOMs.
Answer: False
Mastery Explanation: The Driver usually OOMs first due to the unbounded accumulation of metadata in the `ReceiverTracker`'s `HashMap[Time, Seq[BlockId]]`, causing a `java.lang.OutOfMemoryError: Java heap space`.

Q9. True or False: Calling `dstream.checkpoint()` periodically on a windowed DStream truncates the RDD lineage to prevent `StackOverflowError` during task serialization.
Answer: True
Mastery Explanation: Without checkpointing, stateful operations like windowing or `updateStateByKey` cause the RDD lineage chain to grow by 1 RDD per batch indefinitely, eventually exceeding the JVM stack depth.

Q10. True or False: If a `Receiver` fails and throws an exception but does not call `restart()`, the executor task automatically completes and the DAGScheduler resubmits it.
Answer: False
Mastery Explanation: If `restart()` is not called, the thread dies but the executor task slot remains occupied, producing zero data. It leads to a silent data loss failure until the Driver's block report times out.

## 2. Multiple Choice Questions (15 Questions)

Q11. Which Spark Streaming component is responsible for triggering the `GenerateJobs` event at the end of every batch interval?
A) StreamingContext
B) ReceiverTracker
C) JobGenerator
D) DAGScheduler
Answer: C
Mastery Explanation: The JobGenerator runs a timer thread that fires at each batch interval, calling the ReceiverTracker to get block IDs and the DStreamGraph to generate the Spark job.

Q12. What is the primary reason Kryo serialization is not used for Spark Streaming checkpoints?
A) Kryo cannot serialize Catalyst logical plans.
B) Kryo lacks strict class registration enforcement, making it unsafe for cross-JVM/restart deserialization of arbitrary closures.
C) Java serialization is faster for small metadata payloads.
D) Kryo is incompatible with off-heap Tungsten execution.
Answer: B
Mastery Explanation: Checkpoints store DStream subclasses and user closures that must be robustly restored if the Driver crashes. Java serialization safely encodes the full class schema, whereas Kryo can break upon code or topology changes if classes aren't meticulously registered in order.

Q13. In a receiver-based streaming application, the Driver JVM crashes with an OutOfMemoryError after running smoothly for 6 hours. Which configuration is most likely missing?
A) `spark.streaming.receiver.writeAheadLog.enable = true`
B) `spark.streaming.backpressure.enabled = true`
C) `spark.serializer = org.apache.spark.serializer.KryoSerializer`
D) `spark.memory.offHeap.enabled = true`
Answer: B
Mastery Explanation: Without backpressure, receivers ingest data faster than it can be processed. The Driver's `ReceiverTracker` accumulates an unbounded queue of blocks, leading to heap exhaustion.

Q14. How does `DirectKafkaInputDStream` achieve exactly-once input semantics without a Write-Ahead Log (WAL)?
A) By utilizing Kafka's transactional producer.
B) By storing the block IDs in Zookeeper.
C) By mapping RDD partitions 1:1 to Kafka partitions and deterministically tracking `[fromOffset, untilOffset)` ranges.
D) By relying on Tungsten's MEMORY_AND_DISK_SER storage level.
Answer: C
Mastery Explanation: The direct approach eliminates the block multiplexing of receivers. Each KafkaRDD partition represents a deterministic offset range, allowing exactly-once replay on failure without WAL overhead.

Q15. You have a `batchInterval` of 2 seconds and 20 executor cores. Using a receiver, you want to maximize parallel execution across all cores. What should `spark.streaming.blockInterval` be?
A) 200ms
B) 100ms
C) 500ms
D) 2000ms
Answer: B
Mastery Explanation: 1 block = 1 partition = 1 task. To get 20 partitions per 2000ms batch, block interval = 2000ms / 20 = 100ms.

Q16. When does the cast to `HasOffsetRanges` fail in a direct Kafka streaming pipeline?
A) If WAL is enabled.
B) If it is performed after a shuffle operation (e.g., `groupByKey`).
C) If auto.offset.reset is set to "latest".
D) If the streaming job uses Java serialization.
Answer: B
Mastery Explanation: The `HasOffsetRanges` interface is only present on the original `KafkaRDD`. Any transformation that induces a shuffle creates a `ShuffledRDD`, which loses this Kafka-specific metadata.

Q17. What determines the StorageLevel of data ingested by a custom reliable receiver?
A) The global `spark.storage.level` setting.
B) The constructor parameter passed to `Receiver(StorageLevel)`.
C) The `spark.streaming.receiver.writeAheadLog.enable` flag.
D) The receiver supervisor's default configuration.
Answer: B
Mastery Explanation: The developer explicitly specifies the storage level when extending the `Receiver` abstract class (e.g., `StorageLevel.MEMORY_AND_DISK_SER`), which dictates how blocks are kept in the BlockManager.

Q18. What happens if `enable.auto.commit` is set to `true` when using Spark Streaming with direct Kafka?
A) Spark ignores it and overrides it to false.
B) Exactly-once processing is maintained because Spark manages the transaction boundary.
C) Spark may crash due to offset mismatch exceptions.
D) Offsets may be committed before the micro-batch finishes processing, leading to silent data loss on crash recovery.
Answer: D
Mastery Explanation: Kafka's background thread commits offsets asynchronously. If a crash occurs after the commit but before Spark finishes the batch, the data is skipped on recovery.

Q19. Which sub-component executes the PID controller logic to mitigate processing lags?
A) JobGenerator
B) RateController / RateEstimator
C) ReceiverTracker
D) BlockManagerMaster
Answer: B
Mastery Explanation: The `RateEstimator` calculates a new maximum ingestion rate using a PID controller (to prevent oscillation) and pushes this rate via RPC to the `ReceiverSupervisor`.

Q20. When a task attempts to read a `BlockRDDPartition`, what is the hierarchy of its read path?
A) WAL -> Remote BlockManager -> Local BlockManager
B) Local BlockManager -> Remote BlockManager -> WAL
C) Driver memory -> Local BlockManager -> WAL
D) HDFS -> Driver memory -> Executor memory
Answer: B
Mastery Explanation: The read path is highly optimized for locality: first it tries `SparkEnv.get.blockManager.get(blockId)` (local off-heap/disk), then fetches from remote executors, and only falls back to WAL if the block was evicted.

Q21. What is the complexity of an invertible `reduceByKeyAndWindow`?
A) O(window_size)
B) O(number_of_keys_in_window)
C) O(new_batch + old_batch)
D) O(1)
Answer: C
Mastery Explanation: The incremental windowing algorithm adds the newest batch and subtracts the oldest expired batch, scaling by `O(new_batch + old_batch)` instead of iterating over every batch within the sliding window.

Q22. Why must `StreamingContext.getOrCreate()` be used instead of simply using `new StreamingContext()` in production?
A) It prevents the creation of multiple DAGSchedulers.
B) It allows Spark to reconstruct the `DStreamGraph` from a checkpoint upon driver recovery, maintaining in-flight batch state.
C) It automatically tunes the batch interval based on historical processing times.
D) It prevents Kryo serialization exceptions on executor startup.
Answer: B
Mastery Explanation: If a driver crashes, `new StreamingContext()` executes from scratch, losing the topology's state. `getOrCreate()` loads the serialized graph, preventing data skipping or duplicate processing.

Q23. What triggers the submission of the generated RDD DAG to the DAGScheduler?
A) DStream transformation definition (e.g. `map`, `filter`).
B) The end of the block interval.
C) Output DStream operations (e.g. `foreachRDD`, `print`).
D) The PID RateEstimator signal.
Answer: C
Mastery Explanation: The DStreamGraph is lazily evaluated. Only output DStreams ("sink" nodes) trigger job submission. A graph with no output operations evaluates to dead code.

Q24. In the context of Spark Streaming, why is `MEMORY_AND_DISK_SER` preferred over `MEMORY_ONLY` for receiver storage?
A) It avoids the need for a Write-Ahead Log.
B) It stores data off-heap in Tungsten binary format, reducing GC pressure and preventing fatal `BlockNotFoundException` on memory eviction.
C) It replicates data across multiple executors automatically.
D) It bypasses the BlockManager entirely.
Answer: B
Mastery Explanation: Serialized data bypasses the Java object overhead. More importantly, if memory pressure forces an eviction, `_SER` spills to disk, whereas `MEMORY_ONLY` drops it permanently, causing the batch to fail unless a WAL exists.

Q25. Which interface must a receiver implement to guarantee exactly-once ingestion when WAL is enabled?
A) It must call `store(bytes)` and only acknowledge the source after the store call returns.
B) It must override `commitAsync`.
C) It must implement the `HasOffsetRanges` trait.
D) It must use `LocationStrategies.PreferConsistent`.
Answer: A
Mastery Explanation: Reliable receivers acknowledge the source system (e.g., RabbitMQ, Flume) *only after* `store()` returns. With WAL enabled, `store()` blocks until data is safely written to HDFS/S3, ensuring durability before acknowledgment.

## 3. "Small Twist" Questions (15 Questions)

Q26. Twist: You change a stateful aggregation from `reduceByKeyAndWindow(sum, subtract)` to `reduceByKeyAndWindow(max)`. What drastically changes about your performance?
A) Network shuffle volume decreases to zero.
B) Complexity changes from O(new+old) to O(all_batches_in_window), multiplying execution time.
C) Tungsten codegen is disabled.
D) The checkpoint interval is automatically bypassed.
Answer: B
Mastery Explanation: `max` is not algebraically invertible. You cannot "subtract" the max of the oldest batch to find the new max. Spark reverts to the naive algorithm, recomputing the entire window every slide.

Q27. Twist: You implement `foreachRDD` and write the data using an external connection pool. A database deadlock causes the write to fail midway. Next batch, the task is retried. If you used `DirectKafkaInputDStream`, what is the consequence?
A) Data loss.
B) Duplicates, unless the DB write is idempotent (e.g. UPSERT).
C) The JobGenerator crashes.
D) Kafka offsets are permanently corrupted.
Answer: B
Mastery Explanation: The Direct Kafka DStream guarantees exactly-once replay. When the task retries, the exact same records are delivered. If the DB write isn't idempotent, the partially written records will be duplicated.

Q28. Twist: You set `spark.streaming.blockInterval = 10ms` on a 2-second batch. Throughput plummets to near zero. Why?
A) Receivers cannot serialize blocks that fast.
B) The DAGScheduler is overwhelmed by 200 tasks per batch, causing massive scheduling overhead.
C) WAL writes throttle the filesystem.
D) The `RateEstimator` throttles ingestion to 0.
Answer: B
Mastery Explanation: 10ms block interval over 2000ms batch yields 200 partitions. For a simple graph, scheduling 200 tasks every 2 seconds introduces overhead that dominates processing time, leading to backpressure throttling.

Q29. Twist: You enable `spark.streaming.backpressure.enabled = true` but forget to configure `spark.streaming.kafka.maxRatePerPartition`. During a huge Kafka spike, what happens?
A) The Driver OOMs.
B) Backpressure PID controller takes a few batches to adapt, leading to 1-2 massively oversized initial batches.
C) Spark refuses to start the application.
D) DirectKafkaInputDStream automatically limits fetches to 1MB per partition.
Answer: B
Mastery Explanation: The PID controller needs historical metrics to estimate the rate. On startup, without a defined max rate, the first batch attempts to ingest the entire backlog, which can crash executors or delay processing drastically before backpressure kicks in.

Q30. Twist: A custom receiver encounters a `java.net.ConnectException`. It catches the exception and simply prints an error. What happens to the Spark job?
A) The Driver shuts down immediately.
B) The task fails and Spark resubmits it.
C) The executor task remains running but ingests nothing; Spark UI shows no errors until a timeout.
D) Backpressure scales the rate to 0.
Answer: C
Mastery Explanation: Receivers run as long-lived tasks. If the loop terminates without throwing an uncaught exception or calling `restart()`, the thread exits silently. The supervisor doesn't restart it, leaving a zombie task slot.

Q31. Twist: You use Direct Kafka. Inside `foreachRDD`, you apply `rdd.repartition(100)` before calling `asInstanceOf[HasOffsetRanges]`. What is the result?
A) ClassCastException.
B) The offsets are corrupted.
C) It successfully retrieves offsets from the newly partitioned RDD.
D) The checkpoint size blows up.
Answer: A
Mastery Explanation: `repartition` creates a `CoalescedRDD` or `ShuffledRDD`. Only the original `KafkaRDD` implements `HasOffsetRanges`. You must extract offsets *before* any shuffle or repartitioning.

Q32. Twist: You deploy a Spark Streaming job with `StreamingContext.getOrCreate()`. After a week, you change the code to add a new `map` transformation. You restart the Driver, but the new transformation doesn't execute. Why?
A) Checkpoint data from HDFS deserializes the *old* DStreamGraph, ignoring the new code.
B) Catalyst optimizes away the map transformation.
C) The `map` transformation lacks a sink.
D) The JAR was not updated on the executors.
Answer: A
Mastery Explanation: `getOrCreate()` loads the serialized graph from the checkpoint. Any changes in the `createStreamingContext` function are completely ignored if a valid checkpoint exists.

Q33. Twist: You enable WAL for a `DirectKafkaInputDStream`. What impact does this have on the application?
A) Exactly-once semantics are guaranteed for outputs.
B) Network I/O doubles, but durability increases.
C) It has no effect, as Direct streams bypass the Receiver and WAL entirely.
D) Kafka offsets are written to WAL instead of Zookeeper.
Answer: C
Mastery Explanation: The Direct Kafka stream pulls data directly from Kafka brokers via `KafkaRDD` partitions. There are no `Receiver` or `ReceiverSupervisor` components, so the WAL configuration is strictly irrelevant.

Q34. Twist: You perform a `reduceByKey` operation on an ingested stream, followed by an output to MySQL. The batch processing time is 4s, interval is 2s. Backpressure is disabled. What happens to the `BlockManager`?
A) It spills to WAL.
B) It drops blocks, causing data loss.
C) It accumulates blocks off-heap (or on disk), eventually causing an executor disk full or Driver OOM.
D) It automatically adjusts the batch interval.
Answer: C
Mastery Explanation: Since processing is slower than the interval and backpressure is disabled, the `Receiver` continues writing blocks to the `BlockManager`. Unprocessed blocks accumulate, eventually exhausting executor storage and driver metadata.

Q35. Twist: In a custom receiver, you switch `StorageLevel` from `MEMORY_AND_DISK_SER` to `MEMORY_AND_DISK_2`. What is the immediate architectural trade-off?
A) 2x network write cost during ingestion, but WAL can be safely disabled.
B) Memory usage halves.
C) Job scheduling latency doubles.
D) Off-heap execution is enabled.
Answer: A
Mastery Explanation: The `_2` suffix means the block is synchronously replicated to a peer executor's `BlockManager`. This protects against a single node failure without HDFS WAL, but doubles the ingestion network cost.

Q36. Twist: You implement manual offset commits (`commitAsync`) for Direct Kafka, but you place the commit call *before* the external DB write in `foreachRDD`. The DB write fails. What happens on recovery?
A) Data is safely retried.
B) The batch is skipped on restart; permanent data loss occurs.
C) Kafka automatically rolls back the commit.
D) Spark catches the exception and un-commits.
Answer: B
Mastery Explanation: By committing before the DB write, you told Kafka "this data is safe." When the DB write fails and the job crashes, the restarted job will fetch the *new* committed offsets, skipping the failed batch forever.

Q37. Twist: You use `checkpoint(10)` on a windowed stream, but your `spark.serializer` is set to Kryo. The job crashes. On restart, what is the most likely error?
A) NotSerializableException or ClassNotFoundException during Driver graph deserialization.
B) BlockNotFoundException.
C) KafkaOffsetMismatchException.
D) OutOfMemoryError.
Answer: A
Mastery Explanation: Checkpoints always use Java serialization. However, if closures or DStream types rely heavily on Kryo-specific tricks, or if the classpath changes slightly, Java deserialization of the checkpoint will violently fail on restart.

Q38. Twist: You observe high GC pauses on executors during receiver ingestion. You change the receiver's storage level from `MEMORY_ONLY` to `MEMORY_ONLY_SER`. What happens to GC?
A) GC pauses increase due to serialization overhead.
B) GC pauses drop dramatically because millions of objects are compacted into flat byte arrays (Tungsten).
C) GC is unaffected; it's handled by the Driver.
D) OOM occurs.
Answer: B
Mastery Explanation: Storing raw Java objects (`MEMORY_ONLY`) clutters the heap, overwhelming the GC. `_SER` stores data as serialized byte buffers, which are essentially opaque to the GC, drastically reducing pause times.

Q39. Twist: In your `StreamingContext` factory, you call `sys.addShutdownHook` to call `ssc.stop(true, true)`. During a scale-down event, a SIGTERM is sent. What prevents mid-batch data corruption?
A) `stopGracefully=true` ensures the current micro-batch fully completes before shutting down the SparkContext.
B) The WAL intercepts the SIGTERM.
C) Kafka locks the partitions.
D) The PID controller zeroes out the rate.
Answer: A
Mastery Explanation: `stopGracefully = true` forces the `StreamingContext` to wait until the `JobGenerator` finishes processing the active micro-batch, ensuring no partial writes occur before JVM exit.

Q40. Twist: Your Spark Streaming job has no output actions (no `foreachRDD`, no `print`). You submit it to the cluster. What happens?
A) It processes data infinitely but writes nowhere.
B) It throws an `IllegalArgumentException` stating that the DStreamGraph has no output operations.
C) It caches all data in memory until an action is attached.
D) The JobGenerator runs, but generates 0 Spark jobs per interval.
Answer: B
Mastery Explanation: The `DStreamGraph` requires at least one output stream. If none are registered, calling `ssc.start()` throws an exception because the streaming graph is conceptually empty and dead code.

## 4. Coding & Debugging Questions (10 Questions)

Q41. Debug this snippet. It OOMs after 48 hours.
```scala
val counts = pairs.updateStateByKey[Int] { (newCounts, runningCount) =>
  Some(runningCount.getOrElse(0) + newCounts.sum)
}
counts.foreachRDD { rdd => println(rdd.count()) }
```
A) The state is never pruned. Keys accumulating over 48 hours exhaust heap space.
B) `checkpoint()` is not called on the DStream, leading to unbounded RDD lineage.
C) `foreachRDD` must be called inside `transform`.
D) Both A and B are likely structural flaws that cause memory exhaustion over time.
Answer: D
Mastery Explanation: `updateStateByKey` requires a checkpoint directory to break RDD lineage. Without it, the DAG chain grows indefinitely causing a `StackOverflowError` or lineage OOM. Furthermore, if the state is never pruned (returning `None`), state size grows infinitely, blowing up the heap.

Q42. Identify the logical bug that breaks exactly-once semantics:
```scala
stream.foreachRDD { rdd =>
  val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
  stream.asInstanceOf[CanCommitOffsets].commitAsync(offsets)
  rdd.foreachPartition { iter => db.write(iter) }
}
```
A) `commitAsync` is called before `db.write`. If `db.write` fails, data is lost.
B) `HasOffsetRanges` is invalid here.
C) `db.write` cannot be called inside `foreachPartition`.
D) `commitAsync` is a blocking call.
Answer: A
Mastery Explanation: Offsets must be committed *after* the successful side-effect (DB write). By committing before the write, a failure in the write step results in a recovered application starting from the new offsets, skipping the failed data.

Q43. Optimize this code to prevent a massive bottleneck:
```scala
stream.foreachRDD { rdd =>
  rdd.foreach { record =>
    val conn = DriverManager.getConnection("jdbc:mysql://...")
    conn.createStatement().executeUpdate(s"INSERT INTO t VALUES (${record})")
    conn.close()
  }
}
```
A) Use `rdd.map` instead of `foreach`.
B) Use `foreachPartition` and create one connection per partition, rather than one per record.
C) Use `rdd.repartition(1)`.
D) Enable WAL.
Answer: B
Mastery Explanation: Opening and closing a JDBC connection per record is incredibly slow (thousands of ms per record). `foreachPartition` allows opening one connection per task, amortizing the connection overhead across all records in the block.

Q44. What is the issue with this `getOrCreate` implementation?
```scala
val ssc = StreamingContext.getOrCreate(checkpointDir, () => {
  val context = new StreamingContext(conf, Seconds(2))
  context.checkpoint(checkpointDir)
  context
})
val lines = ssc.socketTextStream("host", 9999)
lines.print()
ssc.start()
```
A) `socketTextStream` doesn't support checkpointing.
B) The DStream definitions are *outside* the factory function. On recovery, they won't be recreated.
C) `print()` cannot be used with `getOrCreate`.
D) The batch interval is too short.
Answer: B
Mastery Explanation: When recovering from a checkpoint, the factory function is *not* called. But since the DStream logic is outside, a fresh run executes them, but a recovered run has a deserialized context *without* these transformations attached properly to the recovery graph. They must be inside the factory.

Q45. This receiver code silently drops data on network failure. How do you fix it?
```scala
try {
  while(running) { store(reader.readLine()) }
} catch {
  case e: Exception => println("Error: " + e)
}
```
A) Throw the exception.
B) Call `restart("Network error", e)` inside the catch block.
C) Call `System.exit(1)`.
D) Change `running` to true.
Answer: B
Mastery Explanation: Without calling `restart()`, the thread simply dies but the Spark task remains active. The `ReceiverSupervisor` assumes the receiver is still fine. `restart()` triggers the supervisor's exponential backoff recovery.

Q46. Why does this Kafka Direct Stream code throw a ClassCastException?
```scala
val stream = KafkaUtils.createDirectStream(...)
val processed = stream.map(_.value).repartition(10)
processed.foreachRDD { rdd =>
  val offsets = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
}
```
A) Direct streams don't support offsets.
B) `repartition(10)` creates a `CoalescedRDD` or `ShuffledRDD` which does not implement `HasOffsetRanges`.
C) `map` changes the type to String.
D) The RDD must be cached first.
Answer: B
Mastery Explanation: The metadata is tied strictly to the `KafkaRDD`. Any transformation that changes the RDD type strips away the Kafka-specific interfaces. You must extract the offsets from `stream` directly.

Q47. You see a huge scheduling delay in the Spark UI. Your batch interval is 1 second, and your Kafka topic has 2000 partitions. What is the root cause?
A) Lack of WAL.
B) Backpressure is disabled.
C) Too many partitions (2000 tasks/sec) causing DAGScheduler overhead to exceed the batch interval.
D) Tungsten is disabled.
Answer: C
Mastery Explanation: In Direct Kafka, 1 Kafka partition = 1 RDD partition. 2000 partitions mean 2000 tasks per batch. Launching 2000 tasks every second causes massive Driver-side scheduling overhead, pushing processing time > 1s and causing backlog.

Q48. A developer sets `spark.streaming.blockInterval = 1000ms` on a 2-second batch. The cluster has 10 cores. The UI shows max 2 active tasks at a time. Why?
A) Data skew.
B) 2-second batch / 1000ms block = 2 partitions per batch. Max parallelism is bounded by partition count.
C) Backpressure limited the rate to 2.
D) WAL is writing to a single HDFS block.
Answer: B
Mastery Explanation: The number of partitions for a receiver stream is exactly `batchInterval / blockInterval`. Here, 2000 / 1000 = 2 partitions. Thus, only 2 tasks can run concurrently, leaving 8 cores completely idle.

Q49. Code review this checkpointing strategy:
```scala
stream.map(x => x.toUpperCase).checkpoint(Seconds(2))
```
Batch interval is 2 seconds. What is wrong?
A) Checkpointing interval must be a multiple of the batch interval and typically larger (e.g., 5-10x) to avoid HDFS I/O overhead on every single micro-batch.
B) `toUpperCase` is not a valid transformation.
C) It causes memory leaks.
D) Checkpoints only work on pairs.
Answer: A
Mastery Explanation: Checkpointing writes the serialized RDD to HDFS. Doing this every single batch (2s) introduces massive network/disk I/O overhead. Best practice is to checkpoint every 5-10 batches to amortize the cost while breaking lineage.

Q50. A stream processes 10,000 records/sec. You enable WAL, and throughput drops to 1,000 records/sec. What is the likely bottleneck and fix?
A) The BlockManager is out of memory; enable off-heap memory.
B) HDFS write latency is blocking receiver acknowledgment; use `MEMORY_AND_DISK_SER` and ensure HDFS is optimized, or switch to Direct Kafka to bypass WAL.
C) The JobGenerator timer is skewed.
D) Java serialization is too slow; use Kryo for checkpoints.
Answer: B
Mastery Explanation: The `WriteAheadLogBasedBlockHandler` writes synchronously to durable storage (HDFS) before ACKing the receiver. If HDFS latency is high, ingestion plummets. Direct Kafka bypasses this entirely by relying on Kafka's own durability.
