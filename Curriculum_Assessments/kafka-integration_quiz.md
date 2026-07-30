# Elite Technical Assessment: Kafka Integration in Apache Spark

## Section 1: True/False Questions (10)

**1. Spark executors communicate with the driver to fetch Kafka records in micro-batches.**
**Answer:** False
**Mastery Explanation:** The driver handles offset negotiation and planning, but executors bypass the driver completely to fetch records directly from the Kafka partition leaders via TCP.

**2. Spark uses Kafka's internal `__consumer_offsets` topic to track micro-batch progress.**
**Answer:** False
**Mastery Explanation:** Spark tracks offsets in its own HDFS/S3-backed checkpoint directory to ensure offsets are advanced in lockstep with the successful commit of state updates in Spark.

**3. Setting `failOnDataLoss=false` forces Spark to skip missing offsets, breaking exactly-once semantics.**
**Answer:** True
**Mastery Explanation:** It tells Spark to jump to the earliest available offset, skipping any data that aged out. This intentionally breaks exactly-once guarantees.

**4. The `KafkaDataConsumer` caches and pools `KafkaConsumer` instances on the executors to avoid connection overhead.**
**Answer:** True
**Mastery Explanation:** Spark pools `KafkaConsumer` instances carefully, managing thread affinity since underlying consumers are not thread-safe, avoiding TCP handshake overhead per micro-batch.

**5. Increasing `minPartitions` triggers a Catalyst network shuffle to increase parallelism.**
**Answer:** False
**Mastery Explanation:** `minPartitions` instructs the driver to splinter single Kafka partitions into multiple Spark tasks. This increases read parallelism *without* a costly network shuffle.

**6. To achieve exactly-once semantics when writing to Kafka, `checkpointLocation` is optional if `kafka.transactional.id` is set.**
**Answer:** False
**Mastery Explanation:** `checkpointLocation` is non-negotiable. Without it, Spark cannot coordinate offset commits with its own WAL, breaking exactly-once across failures.

**7. Instantiating a `KafkaConsumer` within `mapPartitions` is the recommended way to enrich streaming data from another Kafka topic.**
**Answer:** False
**Mastery Explanation:** Doing so without proper pooling can DDoS the Kafka cluster or cause catastrophic memory leaks since instances aren't easily shared or closed across tasks safely.

**8. The offset fetch operations executed by `KafkaOffsetReader` are O(N) complexity proportional to data volume.**
**Answer:** False
**Mastery Explanation:** Offset fetch is a metadata operation with O(P) complexity, where P is the number of partitions. It is extremely fast and handled entirely by the driver.

**9. `kafka_arrival_time` and `event_timestamp` are inherently identical in structured streaming.**
**Answer:** False
**Mastery Explanation:** `kafka_arrival_time` is when the broker received the message. `event_timestamp` is within the payload (when the event occurred) and is essential for correct watermarking.

**10. Capping `maxOffsetsPerTrigger` protects the JVM heap during massive backfills.**
**Answer:** True
**Mastery Explanation:** It limits the amount of data processed per micro-batch, preventing out-of-memory errors when processing a huge backlog.

## Section 2: Multiple Choice Questions (15)

**11. Which component is responsible for querying Kafka for the earliest and latest offsets during a micro-batch?**
A) KafkaDataConsumer
B) KafkaOffsetReader
C) HDFSBackedStateStore
D) KafkaSourceProvider
**Answer:** B
**Mastery Explanation:** `KafkaOffsetReader` resides on the Driver and queries Kafka for offsets to calculate the delta for the logical plan.

**12. By default, how does Catalyst map Spark tasks to Kafka partitions during ingestion?**
A) Many tasks to one Kafka partition
B) One task to many Kafka partitions
C) Strict one-to-one mapping
D) It relies on random distribution
**Answer:** C
**Mastery Explanation:** One Spark task is responsible for exactly one Kafka partition by default for optimal locality and performance.

**13. What is the complexity and shuffle requirement for reading from Kafka (per partition)?**
A) O(1), Yes
B) O(N log N), Yes
C) O(N), No
D) O(P), No
**Answer:** C
**Mastery Explanation:** Reading is O(N) where N is records, and it requires no shuffle as tasks read directly from leaders.

**14. What occurs if `failOnDataLoss=true` (default) and Spark's checkpointed offset is older than the earliest available offset?**
A) Spark skips the missing records automatically
B) Spark polls older logs from cold storage
C) The query crashes immediately with `OffsetOutOfRangeException`
D) The driver resets the checkpoint to 0
**Answer:** C
**Mastery Explanation:** Spark crashes explicitly to prevent silent data loss, requiring manual intervention or reconfiguration.

**15. Which of the following is an elite tuning technique to optimize network fetches on the executor side?**
A) `spark.sql.shuffle.partitions`
B) `kafka.fetch.min.bytes` and `kafka.fetch.max.wait.ms`
C) `maxOffsetsPerTrigger`
D) `minPartitions`
**Answer:** B
**Mastery Explanation:** Tuning `fetch.min.bytes` and `max.wait.ms` via the `kafka.` prefix pushes optimization to the C/Java client, batching transfers into substantial chunks.

**16. Why must you cast Kafka's `key` and `value` to STRING before parsing?**
A) Kafka sends data as JSON natively
B) Spark Structured Streaming requires String inputs
C) Kafka values arrive as raw binary (byte arrays)
D) Tungsten only supports string operations
**Answer:** C
**Mastery Explanation:** Kafka's payload is binary. Casting to string is required before Catalyst's `from_json` can apply schema parsing.

**17. What is the primary purpose of applying `.withWatermark()` when dealing with late-arriving Kafka data?**
A) To enforce exactly-once writing
B) To trigger early micro-batches
C) To prevent unbounded state growth in the `HDFSBackedStateStore`
D) To filter out corrupted JSON payloads
**Answer:** C
**Mastery Explanation:** Watermarks allow the state store to safely evict old state data after the threshold, preventing memory exhaustion.

**18. What output mode should be used when writing stateful aggregated results back to Kafka to minimize network I/O?**
A) Append
B) Complete
C) Update
D) Upsert
**Answer:** C
**Mastery Explanation:** Update mode ensures only changed rows are serialized and transmitted to Kafka, minimizing I/O compared to Complete mode.

**19. How does `minPartitions` improve performance?**
A) It increases the number of Spark tasks reading a single Kafka partition without a shuffle.
B) It forces Kafka to repartition its topics dynamically.
C) It limits the amount of memory each executor uses.
D) It re-routes data through the driver to balance the load.
**Answer:** A
**Mastery Explanation:** `minPartitions` splits a single Kafka partition's offset range across multiple Spark tasks, utilizing cluster cores efficiently without triggering a shuffle.

**20. Which feature guarantees that Spark's Kafka sink deduplicates retried messages?**
A) HDFSBackedStateStore
B) `failOnDataLoss`
C) `kafka.enable.idempotence` and `kafka.transactional.id`
D) OutputMode Append
**Answer:** C
**Mastery Explanation:** Enabling producer idempotence and transactions allows the Kafka broker to deduplicate retried micro-batch outputs using sequence numbers.

**21. Where is the `KafkaDataConsumer` component located in the Spark cluster?**
A) On the Driver JVM only
B) Distributed across the Executor JVMs
C) Within the Kafka Brokers
D) In the Zookeeper ensemble
**Answer:** B
**Mastery Explanation:** `KafkaDataConsumer` manages the `KafkaConsumer` pools on the executor side to handle direct data fetching efficiently.

**22. If you do not provide an explicit schema to `from_json` when parsing Kafka data...**
A) Catalyst will infer it with zero performance overhead.
B) Spark will throw an AnalysisException.
C) You incur catastrophic schema inference cost on streams.
D) The data remains binary.
**Answer:** C
**Mastery Explanation:** Schema inference on streaming data forces scanning, which is exceptionally costly. Explicit schemas leverage Tungsten vectorization.

**23. What happens if a Spark executor crashes mid-batch during Kafka ingestion?**
A) The data is lost.
B) The driver relaunches the task with the exact same offset range.
C) Kafka automatically rolls back the partition leader.
D) Spark increments the offset and skips the batch.
**Answer:** B
**Mastery Explanation:** Because offsets are tracked in Spark's checkpoint dir, the driver replays the exact physical plan, requesting the same deterministic offset range.

**24. The fundamental architectural benefit of Kafka + Spark integration is:**
A) Coupled storage and compute for lower latency.
B) Decoupled storage and compute with resilient micro-batch state management.
C) Eliminating the need for checkpoints entirely.
D) Replacing HDFS with Kafka as the primary data lake.
**Answer:** B
**Mastery Explanation:** The architecture aligns Kafka's partitioned commit log with Spark's distributed processing and state stores for fault-tolerant decoupling.

**25. Which prefix must be used to pass native properties directly to the underlying Kafka consumer/producer?**
A) `spark.kafka.`
B) `kafka.`
C) `consumer.`
D) `streaming.`
**Answer:** B
**Mastery Explanation:** Options prefixed with `kafka.` (e.g., `kafka.fetch.min.bytes`) are passed directly to the Kafka client API by the Spark integration.

## Section 3: "Small Twist" Questions (15)

**26. Scenario: Your pipeline has `checkpointLocation` set. You change the cluster from 10 nodes to 20 nodes and restart. What happens to the offsets?**
A) They are reset to earliest.
B) They resume from the exact last checkpointed offset.
C) Spark crashes due to topology mismatch.
D) Data is duplicated across the new nodes.
**Answer:** B
**Mastery Explanation:** The checkpoint directory is independent of cluster size. The driver reads the state and resumes perfectly.

**27. Scenario: You set `minPartitions=50` on a topic with 10 partitions. You then run a `groupBy` and aggregation. Did you avoid a shuffle?**
A) Yes, `minPartitions` prevents all shuffles.
B) No, the `groupBy` operation still requires a shuffle by the grouping key.
C) Yes, because the data is already pre-partitioned in Kafka.
D) No, `minPartitions` forces a shuffle immediately.
**Answer:** B
**Mastery Explanation:** While `minPartitions` avoids a shuffle *during ingestion*, an aggregation (`groupBy`) fundamentally requires a shuffle to co-locate keys (O(N log N)).

**28. Scenario: You configure `kafka.transactional.id` but forget to define `checkpointLocation`. What happens?**
A) Exact-once is still achieved via Kafka.
B) Spark fails to start the streaming query.
C) Spark writes exactly-once per micro-batch, but duplicates across app restarts.
D) Spark ignores the transaction ID.
**Answer:** B
**Mastery Explanation:** A checkpoint location is mandatory for exactly-once streaming sinks. Without it, Spark will throw an exception during query initialization.

**29. Scenario: You set `startingOffsets=latest`. The app runs, checkpoints, and is stopped for 2 days. When restarted, where does it read from?**
A) The newest messages arriving right now (latest).
B) The offset recorded in the checkpoint from 2 days ago.
C) It crashes with `OffsetOutOfRangeException`.
D) It falls back to `earliest`.
**Answer:** B
**Mastery Explanation:** `startingOffsets` only applies when *no checkpoint exists*. Once checkpointed, it always resumes from the checkpoint directory.

**30. Scenario: You set `maxOffsetsPerTrigger=100000` to limit batch size. You have 10 partitions. What does this configuration do?**
A) Limits each partition to 100,000 records.
B) Limits the total micro-batch to 100,000 records across all partitions.
C) Drops records exceeding 100,000.
D) Forces a shuffle.
**Answer:** B
**Mastery Explanation:** `maxOffsetsPerTrigger` is a global limit for the entire micro-batch, distributed proportionally across partitions to protect the overall JVM heap.

**31. Scenario: You use `from_json` on the `value` column without casting it from binary to string first. What is the result?**
A) Catalyst implicitly casts it and proceeds.
B) The query throws an Analysis/Type mismatch exception.
C) The output is corrupted JSON.
D) The query runs but drops all records.
**Answer:** B
**Mastery Explanation:** Catalyst's `from_json` strictly expects a string column. Passing a binary column directly results in a type mismatch error during logical planning.

**32. Scenario: Your `OffsetOutOfRangeException` is caused by 7 days of downtime, but Kafka retention is 3 days. You set `failOnDataLoss=false`. What is the immediate consequence?**
A) The driver recovers the 4 days of lost data fromWAL.
B) Spark begins reading from the earliest available offset in Kafka, permanently skipping 4 days of data.
C) Spark reads from the latest offset.
D) The query waits until the data is manually backfilled.
**Answer:** B
**Mastery Explanation:** `failOnDataLoss=false` forces the application to swallow the data loss and resume at the earliest offset still physically retained by brokers.

**33. Scenario: You increase `kafka.fetch.min.bytes` to 10MB. The incoming data rate is very slow (1MB/minute). What happens to the micro-batch latency?**
A) Latency decreases due to less network IO.
B) Latency increases because the consumer waits for 10MB to accumulate or `fetch.max.wait.ms` to expire.
C) Latency is unaffected.
D) The executors crash with TimeoutException.
**Answer:** B
**Mastery Explanation:** `fetch.min.bytes` forces the broker to wait until that much data accumulates before returning the fetch request, bound by `fetch.max.wait.ms`. This increases latency for slow streams.

**34. Scenario: A developer puts `new KafkaConsumer(...)` inside `foreachPartition` to write data. The cluster runs 10,000 micro-batches. What happens?**
A) High performance writing.
B) The Kafka cluster is DDoS'd and executors leak massive amounts of memory due to unclosed consumers.
C) Spark automatically pools them.
D) It achieves exactly-once semantics.
**Answer:** B
**Mastery Explanation:** Instantiating raw consumers without connection pooling or cleanup per partition creates millions of lingering TCP connections, rapidly exhausting JVM and broker resources.

**35. Scenario: `kafka_arrival_time` is used for `.withWatermark()`. Data arrives immediately to Kafka but is processed 2 hours later. Is late data handled correctly?**
A) Yes, arrival time is the safest metric.
B) No, the watermark will be based on when the broker received it, not when the event actually occurred, breaking business logic for delayed events.
C) Yes, Spark corrects the timestamp automatically.
D) No, watermarks only support integer columns.
**Answer:** B
**Mastery Explanation:** Watermarks must be applied to the *actual* `event_timestamp` extracted from the payload to accurately manage state eviction based on reality, not broker latency.

**36. Scenario: You write to a Kafka sink using `.outputMode('complete')` on an aggregation. The data volume is huge. What happens?**
A) Only updated rows are sent to Kafka.
B) The entire accumulated state is serialized and sent to Kafka every micro-batch, causing massive network I/O and potential crashes.
C) Kafka rejects the complete output.
D) Spark automatically converts it to 'update' mode.
**Answer:** B
**Mastery Explanation:** Complete mode forces Spark to output the entire result table every trigger. For stateful streaming, this causes devastating network overhead. Update mode should be used.

**37. Scenario: `minPartitions` is set to 100, but `maxOffsetsPerTrigger` is set to 10. What is the execution behavior?**
A) Spark processes 1000 records.
B) Spark creates 100 tasks, but 90+ of them will read 0 records, creating severe scheduling overhead for no benefit.
C) Spark ignores `minPartitions`.
D) Spark throws an exception.
**Answer:** B
**Mastery Explanation:** Splintering offsets across 100 partitions when the total batch size is only 10 means almost all tasks will be completely empty, wasting CPU on task scheduling.

**38. Scenario: You want exactly-once writes to Kafka, so you set `kafka.enable.idempotence=true`. The Kafka cluster is version 0.10. What happens?**
A) Exact-once is achieved.
B) The Kafka producer throws an error because idempotence requires Kafka 0.11 or higher.
C) Spark polyfills the transaction manager.
D) The configuration is silently ignored.
**Answer:** B
**Mastery Explanation:** Idempotent producers and transactional APIs were introduced in Kafka 0.11. Older brokers cannot support exactly-once semantics via this mechanism.

**39. Scenario: Spark executor crashes. The new executor comes up and requests offset 1000 from the Kafka partition leader. The partition leader moved to another broker during the crash. What happens?**
A) Spark fails the job.
B) The driver's `KafkaOffsetReader` forces a shuffle.
C) The executor receives a NotLeaderForPartition error, fetches the new metadata, and connects to the new leader seamlessly.
D) Data duplication occurs.
**Answer:** C
**Mastery Explanation:** The Kafka client embedded in the executor automatically handles cluster metadata refreshes and leader elections, ensuring seamless recovery.

**40. Scenario: The driver calculates the offset range, but before executors fetch the data, a user manually deletes the Kafka topic. What happens?**
A) Executors fetch nulls.
B) Spark creates the topic automatically.
C) Executors fail with UnknownTopicOrPartitionException, and the query crashes.
D) The state store retains the data.
**Answer:** C
**Mastery Explanation:** The offsets were planned, but the physical data is gone. The executor's KafkaConsumer will fail to fetch, crashing the task and subsequently the query.

## Section 4: Coding & Debugging Questions (10)

**41. Identify the performance blocker in this read query:**
```python
df = spark.readStream.format("kafka").option("subscribe", "topic").load()
df.repartition(100).writeStream...
```
**Answer & Mastery Explanation:** The `repartition(100)` forces a global network shuffle. An elite engineer would remove the `repartition` and use `.option("minPartitions", 100)` in the `readStream` to parallelize the reads at the source without a shuffle.

**42. A pipeline is silently losing data after a weekend outage. `failOnDataLoss` is not specified in the code. Why?**
**Answer & Mastery Explanation:** If it's silently losing data, `failOnDataLoss` must have been explicitly set to `false` somewhere (like external configs), OR the data loss is occurring upstream before Kafka. By default, Spark's `failOnDataLoss` is `true` and would crash if it was purely a Kafka retention issue.

**43. A developer parses JSON using UDFs:**
```python
from pyspark.sql.functions import udf
@udf(StringType())
def extract_id(val): return json.loads(val)['id']
df = df.withColumn("id", extract_id(col("value").cast("string")))
```
**Why is this a critical mistake?**
**Answer & Mastery Explanation:** Using a Python UDF forces serialization of every row between the JVM and Python processes, destroying Tungsten's vectorization. The correct approach is using the native Catalyst `from_json` with an explicit schema.

**44. Review the watermark implementation:**
```python
df.selectExpr("CAST(value AS STRING)", "timestamp") \
  .withWatermark("timestamp", "1 hour") \
  .groupBy(window("timestamp", "10 minutes"))
```
**What is fundamentally wrong if the topic contains delayed mobile telemetry?**
**Answer & Mastery Explanation:** The `timestamp` column from Kafka is the *broker arrival time*. Mobile telemetry can be delayed by days if the phone is offline. The watermark must be applied to the *event timestamp* extracted from the JSON payload, otherwise valid delayed data is immediately dropped as "late".

**45. Debug the missing configuration for exactly-once output:**
```python
df.writeStream.format("kafka") \
  .option("kafka.bootstrap.servers", "b1:9092") \
  .option("topic", "out") \
  .option("checkpointLocation", "/path/") \
  .start()
```
**Answer & Mastery Explanation:** While `checkpointLocation` is present, it lacks the producer-side transaction configurations. It must include `.option("kafka.transactional.id", "txn-id")` and `.option("kafka.enable.idempotence", "true")` to prevent duplicates on retries.

**46. You observe high CPU utilization and terrible network throughput on Kafka brokers during Spark ingestion. The payloads are tiny (100 bytes). How do you fix this in Spark?**
**Answer & Mastery Explanation:** The executors are making thousands of tiny fetch requests. You must inject Kafka consumer options to batch the network I/O: `.option("kafka.fetch.min.bytes", "1048576")` and `.option("kafka.fetch.max.wait.ms", "500")`.

**47. A developer writes the following code to write to Kafka:**
```python
df.writeStream.foreachBatch(lambda batch_df, batch_id: 
    batch_df.write.format("kafka").option("topic", "t1").save()
).start()
```
**Why is this an anti-pattern?**
**Answer & Mastery Explanation:** `foreachBatch` is completely unnecessary here and bypasses the native streaming sink's exactly-once transactional guarantees. It should use `df.writeStream.format("kafka")...start()` directly.

**48. Why does this query fail at runtime?**
```python
df = spark.readStream.format("kafka")...load()
df.select("value").writeStream.format("kafka").option("topic", "out").start()
```
**Answer & Mastery Explanation:** The native Kafka sink requires the output DataFrame to contain specific columns, primarily `value` (and optionally `key`). However, the `value` must be of type `String` or `Binary`. If it wasn't cast before writing, or if it was modified to a struct during processing without being encoded back (e.g., via `to_json`), it fails.

**49. Identify the memory leak in this custom writer:**
```python
def write_to_kafka(partition):
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    for row in partition:
        producer.send('topic', row.value)
df.writeStream.foreachPartition(write_to_kafka)
```
**Answer & Mastery Explanation:** The `KafkaProducer` is instantiated but never explicitly closed (`producer.close()`). Because `foreachPartition` runs per micro-batch partition, this will create thousands of lingering TCP connections and buffer pools, causing a catastrophic memory leak.

**50. How do you configure a job to process a 5TB backfill of Kafka data without crashing the executors with OutOfMemoryError, while ensuring it catches up as fast as possible?**
**Answer & Mastery Explanation:** Set a reasonable `maxOffsetsPerTrigger` (e.g., 500,000) to cap the heap usage per micro-batch. Combine this with `minPartitions` (e.g., 200) to utilize the entire cluster CPU, and tune `kafka.fetch.min.bytes` to optimize the network throughput.
