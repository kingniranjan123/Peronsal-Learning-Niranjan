# Streaming Performance Tuning - Elite Assessment

This assessment is designed to test Senior/Staff-level knowledge of Apache Spark Structured Streaming, focusing on internal execution mechanics, state management, and performance tuning. 

## Part 1: True/False Questions (10 Questions)

**1. True/False:** Structured Streaming relies on `spark.streaming.backpressure.enabled` as its primary backpressure mechanism to prevent trigger stacking.
**Answer:** False. 
**Mastery Explanation:** Structured Streaming has no built-in backpressure mechanism analogous to DStreams. The primary backpressure lever is explicitly configuring `maxOffsetsPerTrigger` on the streaming source, which hard-caps the records fetched per trigger.

**2. True/False:** Setting `maxOffsetsPerTrigger = 100000` means every single Kafka partition will read exactly 100,000 records per micro-batch.
**Answer:** False.
**Mastery Explanation:** The `KafkaMicroBatchReader` distributes the cap proportionally across all partitions globally. If you have 100 partitions, each partition reads at most 1,000 records, totaling 100,000 records. 

**3. True/False:** Using `RocksDBStateStoreProvider` eliminates JVM memory usage for the state store entirely, as everything is moved to the native OS page cache.
**Answer:** False.
**Mastery Explanation:** While MemTables and SST files are off-heap or on-disk, the RocksDB Block Cache (`spark.sql.streaming.stateStore.rocksdb.blockCacheSize`) consumes JVM off-heap memory, requiring careful tuning of `spark.memory.offHeap.size`.

**4. True/False:** In a stream-stream join, the global watermark that triggers state eviction is calculated as the maximum of the two stream watermarks.
**Answer:** False.
**Mastery Explanation:** State eviction is gated by the **minimum** of the two watermarks. If one stream lags significantly, it holds back the global watermark, causing the state buffer for the other stream to grow rapidly.

**5. True/False:** RocksDB changelog checkpointing reduces checkpoint I/O overhead from O(state_size) to O(batch_writes).
**Answer:** True.
**Mastery Explanation:** Instead of uploading the entire state snapshot every trigger, changelog checkpointing only uploads the WAL delta entries to the checkpoint directory, making it mandatory for large state workloads.

**6. True/False:** When using `GroupStateTimeout.ProcessingTimeTimeout` in `flatMapGroupsWithState`, timed-out state is automatically evicted from RocksDB after the function returns.
**Answer:** False.
**Mastery Explanation:** You must explicitly call `state.remove()` inside the timeout branch. If you forget, the state entry remains in RocksDB indefinitely, causing unbounded state accumulation and compaction stalls.

**7. True/False:** Asynchronous progress tracking decouples the WAL offset commit from the next batch's construction, executing the commit in a separate background thread on the driver.
**Answer:** True.
**Mastery Explanation:** This prevents the driver from waiting on high-latency operations (like S3 checkpoint writes) before starting the next micro-batch, reducing trigger-to-trigger latency by 15-40%.

**8. True/False:** Task parallelism in a Kafka Structured Streaming query is determined dynamically by the Catalyst optimizer based on `spark.default.parallelism`.
**Answer:** False.
**Mastery Explanation:** `KafkaMicroBatchReader` creates a `KafkaRDD` where partitions map strictly 1-to-1 with Kafka topic partitions. Under-partitioned topics are the #1 cause of under-utilization in streaming jobs.

**9. True/False:** Stream-stream joins maintain a single `StateStore` instance per shuffle partition that combines both the left and right side buffers.
**Answer:** False.
**Mastery Explanation:** The physical plan generates two separate `StateStore` instances per shuffle partition—one for the left side and one for the right side. This effectively doubles the number of RocksDB instances and compaction threads compared to a standard aggregation.

**10. True/False:** Event-time watermarking can natively express wall-clock inactivity timeouts, such as expiring a session after 30 minutes of real-time silence.
**Answer:** False.
**Mastery Explanation:** Event-time windows are driven strictly by the data's watermarks. To implement wall-clock inactivity timeouts, you must use `flatMapGroupsWithState` combined with `ProcessingTimeTimeout`.

---

## Part 2: Multiple Choice Questions (15 Questions)

**11. What is the root cause of an executor task appearing stuck at near-100% duration with no progress in a high-throughput stateful query using RocksDB?**
a) Stop-the-world JVM GC pauses
b) RocksDB LSM compaction stalls
c) Global watermark mismatch
d) Asynchronous progress tracking thread blocking
**Answer:** b
**Mastery Explanation:** When `writeBufferSizeMB` and `maxWriteBufferNumber` are too small, high write throughput fills the MemTables faster than the background compaction threads can flush to SST files, causing write stalls that block the `StateStoreSaveExec` node entirely.

**12. Which parameter mitigates excessive disk reads for non-existent keys in a RocksDB state store?**
a) `blockCacheSize`
b) `maxBackgroundJobs`
c) `bloomFilterBitsPerKey`
d) `writeBufferSizeMB`
**Answer:** c
**Mastery Explanation:** Bloom filters allow point lookups to bypass disk reads if a key does not exist. Increasing `bloomFilterBitsPerKey` (e.g., to 10) reduces the false-positive rate, which is critical for operations like `dropDuplicates` or high-cardinality aggregations.

**13. In an event-time windowed aggregation, what is the impact of using `outputMode("update")` instead of `"append"`?**
a) Only fully completed windows are written to the sink.
b) Intermediate window states are re-emitted on every trigger, causing massive downstream write amplification.
c) The watermark is ignored, and all state is kept indefinitely.
d) It reduces RocksDB compaction overhead.
**Answer:** b
**Mastery Explanation:** `"update"` mode emits the current state of a window every time it is updated, whereas `"append"` waits until the watermark passes the window end, emitting it only once.

**14. A stream-stream join has impressions watermarked at 2 hours and clicks watermarked at 30 minutes. The join condition is `click_time BETWEEN impression_time AND impression_time + 1 HOUR`. What is the maximum retention time for impression state?**
a) 1 hour
b) 2 hours
c) 3 hours
d) 3.5 hours
**Answer:** c
**Mastery Explanation:** Retention time = Minimum Watermark (2 hours) + Time Range Constraint (1 hour) = 3 hours.

**15. What is the primary benefit of enabling `spark.sql.streaming.asyncProgressTrackingEnabled`?**
a) Allows state variables to be updated concurrently by multiple executor threads.
b) Moves the heavy lifting of RocksDB compaction to the driver.
c) Decouples WAL offset commits from the critical path of constructing the next micro-batch.
d) Asynchronously writes to Delta Lake without blocking the micro-batch execution.
**Answer:** c
**Mastery Explanation:** By committing offsets asynchronously, the `StreamExecution` thread can immediately start planning the next batch without waiting for the WAL write to complete in the checkpoint directory.

**16. Which metric from `StreamingQueryProgress` is the ultimate indicator of trigger stacking and compounding lag?**
a) `numInputRows` > 1,000,000
b) `durationMs.triggerExecution` > `triggerIntervalMs` for consecutive batches
c) `stateOperators[].memoryUsedBytes` growing exponentially
d) `inputRowsPerSecond` < `processedRowsPerSecond`
**Answer:** b
**Mastery Explanation:** If `triggerExecution` duration exceeds the scheduled trigger interval, the job enters a perpetual backlog where each batch begins immediately after the last, constantly falling further behind the source ingestion rate. Alternatively, `inputRowsPerSecond` consistently exceeding `processedRowsPerSecond` indicates the same issue.

**17. Why is the default `spark.sql.shuffle.partitions = 200` often detrimental for streaming queries, particularly stream-stream joins?**
a) Streaming queries do not shuffle data, making the partitions useless.
b) It creates 400 RocksDB state store instances (2 per partition) per executor, starving background threads and causing memory pressure.
c) It exceeds the max offsets per trigger.
d) Catalyst cannot optimize plans with 200 partitions.
**Answer:** b
**Mastery Explanation:** Each shuffle partition creates its own RocksDB instance. In a stream-stream join, there are two instances per partition. 200 partitions = 400 RocksDB instances. Reducing this (e.g., to 50) halves the overhead and prevents task threads from competing with hundreds of compaction threads.

**18. What happens if you execute a stream-stream left-outer join without an event-time range constraint in append mode?**
a) The join falls back to nested loop join.
b) Spark throws an AnalysisException at query startup.
c) State size grows unboundedly until an OOM occurs.
d) The join executes, but matches are only made within the same micro-batch.
**Answer:** b
**Mastery Explanation:** Spark refuses to run the query because without a time-range constraint, it cannot determine an upper bound for when an un-matched left row can be safely emitted as a null-click, making the state unbounded.

**19. How does Tungsten's Whole-Stage CodeGen benefit stateless transformations in Structured Streaming?**
a) It moves state from JVM heap to off-heap memory.
b) It fuses the generated bytecode of multiple physical plan nodes into a single `processNext()` loop, eliminating virtual dispatch overhead.
c) It automatically determines the optimal `maxOffsetsPerTrigger`.
d) It serializes closures via Kryo rather than Java natively.
**Answer:** b
**Mastery Explanation:** Whole-Stage CodeGen collapses a query's operator tree into a single optimized Java function, processing records sequentially in CPU registers without function call overhead.

**20. When implementing `flatMapGroupsWithState`, which configuration must be set to ensure state expires when the user has been inactive for 30 wall-clock minutes?**
a) `.withWatermark("event_time", "30 minutes")`
b) `GroupStateTimeout.EventTimeTimeout`
c) `GroupStateTimeout.ProcessingTimeTimeout`
d) `spark.sql.streaming.stateStore.rocksdb.ttl`
**Answer:** c
**Mastery Explanation:** `ProcessingTimeTimeout` ties the timeout to the wall-clock time of the Spark executors, making it ideal for inactivity tracking, whereas `EventTimeTimeout` relies on data timestamps (watermarks).

**21. Why should RocksDB SST files strictly be placed on NVMe local instance storage rather than network-attached storage (like EBS)?**
a) EBS does not support JNI bindings.
b) Network-attached storage lacks Hadoop FileSystem compatibility.
c) Random read amplification on high-latency block storage causes catastrophic point lookup degradation.
d) RocksDB requires a specific sector size only available on NVMe.
**Answer:** c
**Mastery Explanation:** RocksDB's LSM tree structure requires fast random reads during point lookups and compactions. High-latency network drives amplify this latency, causing task executions to slow to a crawl.

**22. How is task parallelism determined when reading from Kafka in Structured Streaming?**
a) It equals the number of executor cores available.
b) It is determined by `spark.sql.shuffle.partitions`.
c) It is determined by `maxOffsetsPerTrigger` divided by a heuristic chunk size.
d) It is strictly a 1-to-1 mapping to the number of Kafka topic partitions.
**Answer:** d
**Mastery Explanation:** The `KafkaMicroBatchReader` maps each Kafka topic partition directly to one Spark task. If you have 5 Kafka partitions, you have a maximum of 5 parallel reading tasks, regardless of cluster size.

**23. Which RocksDB parameter should be sized specifically against the executor's off-heap JVM memory?**
a) `writeBufferSizeMb`
b) `blockCacheSize`
c) `maxWriteBufferNumber`
d) `maxBackgroundJobs`
**Answer:** b
**Mastery Explanation:** The RocksDB block cache sits in the JVM's off-heap native memory. Increasing it requires explicitly allocating more memory via `spark.memory.offHeap.size`.

**24. In the `MicroBatchExecution` cycle, when are offsets written to the WAL (checkpoint/offsets/)?**
a) After all executor tasks finish successfully.
b) Before the logical plan is fully resolved, as soon as `constructNextBatch` negotiates the offsets.
c) Simultaneously with the RocksDB state snapshot upload.
d) Only when changelog checkpointing is enabled.
**Answer:** b
**Mastery Explanation:** The driver writes the negotiated offset range to the WAL *before* executing the batch. This guarantees exactly-once semantics because if the driver crashes during execution, it recovers the exact offset range from the WAL to replay the exact same batch.

**25. What is the impact of setting `Trigger.ProcessingTime("30 seconds")` for a batch that finishes processing in 10 seconds?**
a) The executors idle and sleep for 20 seconds, saving cloud compute cycles.
b) The driver immediately pulls the next offsets and queues them for execution.
c) The driver shuts down the executors to save costs and restarts them.
d) RocksDB pauses compaction for 20 seconds.
**Answer:** a
**Mastery Explanation:** The `StreamExecution` thread sleeps until the interval boundary. This prevents hot-looping and dramatically reduces costs by allowing CPU scale-down or preventing unnecessary empty-batch cycles.

---

## Part 3: "Small Twist" Scenario Questions (15 Questions)

**26. Scenario:** You change `outputMode` from `"update"` to `"append"` on a query utilizing `flatMapGroupsWithState` because you only want to write out completed sessions. 
**Twist:** What happens to the query?
**Answer:** It fails during query planning. `"append"` mode is not supported with `flatMapGroupsWithState` because Catalyst has no visibility into the user-defined state to guarantee when a row is "final". You must use `"update"` mode.

**27. Scenario:** Your Kafka streaming query has 1 partition. You increase `maxOffsetsPerTrigger` from 10,000 to 1,000,000, expecting a massive throughput increase, but the processing rate stays exactly the same.
**Twist:** Why didn't throughput increase?
**Answer:** With only 1 Kafka partition, task parallelism is capped at 1. A single executor core is already maxed out processing the data sequentially, so raising the ceiling does nothing.

**28. Scenario:** You set `spark.sql.streaming.stateStore.rocksdb.blockCacheSize` to 4GB. You observe random executor OOMKills by the Linux OOM Killer, despite having 16GB of JVM heap.
**Twist:** What configuration is missing?
**Answer:** You must configure `spark.memory.offHeap.enabled = true` and `spark.memory.offHeap.size` to account for the 4GB block cache, otherwise the container exceeds its YARN/Kubernetes memory limit and gets hard-killed by the OS.

**29. Scenario:** In a stream-stream join, Stream A has a 10-minute watermark and Stream B has a 5-minute watermark. The time constraint is `B.time BETWEEN A.time AND A.time + 1 HOUR`. You realize data for B is arriving late, so you relax B's watermark to 2 hours.
**Twist:** How does this affect Stream A's state retention?
**Answer:** Stream A's retention time increases massively. The global watermark drops from 5 minutes to 10 minutes (min of 10m and 2h is 10m). However, wait—if B is 2 hours, the minimum is now 10 minutes. Wait! Min(10m, 2h) = 10m. Min(10m, 5m) = 5m. Actually, the global watermark becomes MORE lenient (slower). The twist is that relaxing B's watermark slows down the global watermark, keeping Stream A's state in memory much longer.

**30. Scenario:** You enable changelog checkpointing, but you notice your S3 checkpoint write times are still terribly slow, taking minutes per trigger. You check your configs and realize you are using `HDFSBackedStateStore`.
**Twist:** Why didn't changelog checkpointing help?
**Answer:** Changelog checkpointing is an exclusive feature of `RocksDBStateStoreProvider`. `HDFSBackedStateStore` always serializes and uploads the entire JVM heap snapshot, ignoring the changelog config entirely.

**31. Scenario:** A query takes 45 seconds to process a batch. You add `Trigger.ProcessingTime("30 seconds")` to try and force it to run faster. 
**Twist:** How long does the job sleep between batches?
**Answer:** 0 seconds. If batch duration exceeds the trigger interval, `MicroBatchExecution` skips the sleep and instantly starts the next batch. This is classic trigger stacking.

**32. Scenario:** In `flatMapGroupsWithState` with `ProcessingTimeTimeout`, an event arrives for a session. You correctly call `state.update(new_state)`. 
**Twist:** The session never times out, even after hours of inactivity. Why?
**Answer:** You forgot to call `state.setTimeoutDuration()`. The timeout is cleared every time `state.update()` is called. It must be explicitly reset on every single update.

**33. Scenario:** To save memory, you set `maxWriteBufferNumber = 1` for RocksDB.
**Twist:** What happens during peak traffic?
**Answer:** RocksDB immediately stalls when the single MemTable fills up. It has no secondary buffer to accept incoming writes while the background thread flushes the full MemTable to an SST file on disk.

**34. Scenario:** You use `withWatermark("event_time", "1 hour")` immediately *after* an aggregation: `groupBy("id", window("event_time", "10 min"))`.
**Twist:** What happens to the state?
**Answer:** The state grows unboundedly. The watermark MUST be defined *before* the stateful operator in the logical plan. Defining it after makes it a no-op for the preceding groupBy.

**35. Scenario:** You execute a stream-stream inner join. Both streams have a 1-hour watermark. You do not provide an event-time range constraint in the join condition.
**Twist:** Does Catalyst throw an AnalysisException?
**Answer:** No, for *inner* joins, Catalyst will allow it, but state size will grow unboundedly forever because it has no constraint to evict un-matched rows. (AnalysisExceptions for missing constraints primarily target Outer joins in append mode).

**36. Scenario:** You have a cluster with 500 cores. You decide to set `spark.sql.shuffle.partitions = 2000` to maximize parallelism in your streaming aggregation.
**Twist:** What happens to RocksDB?
**Answer:** You spawn 2,000 independent RocksDB instances across the cluster. The overhead of 2,000 Block Caches, MemTables, and thousands of compaction threads will crash the executors with off-heap OOMs or severe CPU starvation.

**37. Scenario:** You have `asyncProgressTrackingCheckpointIntervalMs` set to 1 hour to heavily amortize S3 write costs. The driver crashes after 45 minutes of processing.
**Twist:** What happens on restart?
**Answer:** The job will replay the last 45 minutes of data from Kafka. Because the WAL was only held in driver memory and not flushed to S3 for 45 minutes, exactly-once semantics forces a replay of all uncommitted offsets.

**38. Scenario:** You start a query with `outputMode("append")` and a 15-minute watermark. Data is flowing rapidly, but no output appears in the Delta table for the first 15 minutes.
**Twist:** Why is the output delayed?
**Answer:** This is expected. Append mode cannot emit a window until the global watermark advances past the end of the window. It takes at least 15 minutes of event-time progression to emit the first result.

**39. Scenario:** You notice `inputRowsPerSecond = 10,000` and `processedRowsPerSecond = 5,000`. You double the cluster size from 10 executors to 20 executors, but the metrics remain completely unchanged.
**Twist:** Why didn't scaling compute help?
**Answer:** Your bottleneck isn't compute, it's parallelism. If your Kafka topic has only 10 partitions, Spark will never use more than 10 executor cores concurrently.

**40. Scenario:** An executor has 4 CPU cores. You set `spark.sql.streaming.stateStore.rocksdb.maxBackgroundJobs` to 50 to "speed up compactions."
**Twist:** What happens to the streaming tasks?
**Answer:** The 50 RocksDB background threads context-switch aggressively, starving the 4 Spark task threads of CPU time, resulting in the executor grinding to a halt and timing out.

---

## Part 4: Coding & Debugging Questions (10 Questions)

**41. Identify the bug:**
```python
def update_session(user_id, events, state):
    if state.hasTimedOut:
        sess = state.get
        yield (user_id, sess[0], True)
        # BUG HERE
        return
    
    # ... logic to update state ...
    state.update(new_state)
    state.setTimeoutDuration(30 * 60 * 1000)
```
**Answer & Fix:** The code is missing `state.remove()` inside the `if state.hasTimedOut` block. Without it, the state row remains in RocksDB indefinitely, leading to a massive state store explosion.

**42. Identify the bug:**
```scala
val counts = df
  .groupBy(window($"event_time", "10 minutes"), $"user_id")
  .count()
  .withWatermark("event_time", "1 hour")
```
**Answer & Fix:** The `withWatermark` is applied *after* the `groupBy`. Watermarks must be defined before the stateful operator to function. State will grow unboundedly. Fix: Swap the order of `.withWatermark()` and `.groupBy()`.

**43. Identify the bug:**
```scala
val joined = streamA.join(
  streamB,
  streamA("id") === streamB("id"),
  "leftOuter"
)
```
**Answer & Fix:** This is a stream-stream left outer join missing an event-time range constraint (e.g., `streamB.time BETWEEN streamA.time AND streamA.time + 1 HOUR`). In append mode, this fails query planning because Spark cannot bound state retention.

**44. Identify the bug:**
```python
spark = SparkSession.builder \
    .config("spark.sql.streaming.stateStore.providerClass", "RocksDBStateStoreProvider") \
    .config("spark.sql.streaming.stateStore.rocksdb.blockCacheSize", "2147483648") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()
```
**Answer & Fix:** The `blockCacheSize` is set to 2GB, but off-heap memory is neither enabled nor sized. Fix: Add `.config("spark.memory.offHeap.enabled", "true")` and `.config("spark.memory.offHeap.size", "3g")`.

**45. Identify the bottleneck:**
```scala
val df = spark.readStream
  .format("kafka")
  .option("subscribe", "events")
  .load()
```
**Answer & Fix:** The code is missing `maxOffsetsPerTrigger`. Under a traffic spike, this will fetch all available Kafka data into a single massive micro-batch, causing severe trigger stacking and GC failure. Add `.option("maxOffsetsPerTrigger", <safe_limit>)`.

**46. Identify the bug:**
```python
query = windowed_agg.writeStream \
    .format("delta") \
    .outputMode("update") \
    .start("/path")
```
**Answer & Fix:** Using `outputMode("update")` with event-time windowing to a Delta table will result in multiple append writes for the same window as it updates, duplicating data or causing massive write amplification. Use `outputMode("append")`.

**47. Identify the logical error:**
```python
events.groupBy("user_id") \
    .flatMapGroupsWithState(
        output_schema,
        GroupStateTimeout.EventTimeTimeout,
        update_state_func 
    )
# The update function implements a 30-minute inactivity wall-clock timeout.
```
**Answer & Fix:** Using `EventTimeTimeout` relies on data watermarks to progress time. If you want a wall-clock inactivity timeout (e.g., expire if no events arrive in real-time), you MUST use `GroupStateTimeout.ProcessingTimeTimeout`.

**48. Identify the bug:**
```scala
val impressions = spark.readStream... // no watermark
val clicks = spark.readStream.withWatermark("click_time", "1 hour")
val joined = impressions.join(clicks, expr("... time constraints ..."))
```
**Answer & Fix:** Both sides of a stream-stream join MUST have a watermark defined. If `impressions` lacks a watermark, the global watermark cannot be calculated properly, and state will not be evicted. 

**49. Identify the performance issue:**
```scala
spark.conf.set("spark.sql.shuffle.partitions", "2000")
// ... streaming aggregation code ...
```
**Answer & Fix:** Setting shuffle partitions to 2000 for a streaming stateful job creates 2000 RocksDB instances. This causes massive off-heap fragmentation and compaction thread starvation. Fix: Set it to a small multiple of the total executor cores (e.g., 50-200).

**50. Identify the missing feature for a 50GB state store:**
```python
spark = SparkSession.builder \
    .config("spark.sql.streaming.stateStore.providerClass", "RocksDBStateStoreProvider") \
    .getOrCreate()
```
**Answer & Fix:** For a large state store, uploading the full RocksDB snapshot to the checkpoint directory every trigger will cause massive S3 costs and latency. Fix: Add `.config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")` to only upload WAL deltas.
