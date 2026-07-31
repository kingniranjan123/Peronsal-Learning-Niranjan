<Master Class: WebSockets>
Welcome to the Master Class on WebSockets in Apache Spark. As data engineering evolves towards sub-second latency requirements, traditional batch processing and even micro-batching fall short for certain real-time interaction paradigms. WebSockets provide a full-duplex, persistent communication channel over a single TCP connection, making them ideal for high-frequency trading dashboards, live IoT telemetry, and real-time gaming analytics. However, integrating WebSockets directly with Apache Spark's distributed architecture presents profound architectural challenges. Spark is fundamentally designed around distributed data partitions and task-based execution, whereas a WebSocket is a long-lived, stateful, point-to-point TCP connection. 

To bridge this gap, data engineers must understand how Spark handles memory and network serialization. When a Spark executor maintains an open WebSocket connection, the incoming byte streams must be aggressively deserialized and loaded into memory managed by the Tungsten execution engine. If not handled correctly, JVM Garbage Collection (GC) pauses will obliterate your latency SLAs and lead to missed frames. Furthermore, the Catalyst optimizer cannot push down filters to a raw continuous TCP stream; all filtering and projection must occur post-ingestion. Therefore, a robust WebSocket integration requires custom Receiver implementations or Structured Streaming custom Continuous Processing DataSources. These sources must buffer incoming frames directly into Tungsten's `UnsafeRow` binary format, ensuring that backpressure mechanisms gracefully signal the WebSocket sender via standard TCP sliding windows to prevent Executor OutOfMemory (OOM) errors. We will explore how to build and tune these pipelines for maximum throughput and minimal latency.

## 💻 Code Example 1: Building a Custom WebSocket Receiver in Scala
```scala
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.java_websocket.client.WebSocketClient
import org.java_websocket.handshake.ServerHandshake
import java.net.URI

class WebSocketReceiver(uri: String) extends Receiver[String](StorageLevel.MEMORY_AND_DISK_2) {
  @transient private var client: WebSocketClient = _

  override def onStart(): Unit = {
    client = new WebSocketClient(new URI(uri)) {
      override def onOpen(handshakedata: ServerHandshake): Unit = {}
      
      override def onMessage(message: String): Unit = {
        // Store incoming message directly into Spark's memory block
        store(message) 
      }
      
      override def onClose(code: Int, reason: String, remote: Boolean): Unit = {
        restart("WebSocket connection closed")
      }
      
      override def onError(ex: Exception): Unit = {
        restart("Error receiving data", ex)
      }
    }
    client.connect()
  }

  override def onStop(): Unit = {
    if (client != null) client.close()
  }
}
```
In this example, we define a custom DStream `Receiver` in Scala to ingest WebSocket messages. The critical component here is the `StorageLevel.MEMORY_AND_DISK_2` parameter. Because WebSocket streams can exhibit sudden bursts (e.g., a market volatility spike), we replicate the blocks across two nodes. When `onMessage` is invoked, `store(message)` buffers the payload into Spark's BlockManager. To optimize this, the JVM heap must be carefully tuned. We bypass complex object creation inside `onMessage` to minimize GC pressure. While this example uses DStreams for simplicity, the underlying principle of managing the asynchronous network thread separate from Spark's task thread pool remains critical. The receiver thread must remain non-blocking, delegating the persistence to Spark's internal storage mechanisms.

## Optimizing JVM and Tungsten for High-Frequency Streams
When ingesting WebSocket data at scale, the default Spark configurations are often inadequate. WebSockets generate continuous, unbounded data streams characterized by highly unpredictable burst patterns. When millions of small JSON payloads hit your executors, the standard Java string deserialization becomes a massive bottleneck. The Tungsten execution engine is designed to operate on off-heap memory using a highly optimized binary format (`UnsafeRow`), but getting the data from the WebSocket network buffer into Tungsten requires careful serialization.

To mitigate object churn, modern pipelines often employ a two-stage ingestion strategy. First, the WebSocket client reads the raw byte array without immediately converting it to a Java String. Second, a custom memory allocator directly copies this byte array into an off-heap buffer. By utilizing Spark's Catalyst optimizer, we can define a schema upfront. Catalyst will then generate JVM bytecode (Whole-Stage CodeGen) that reads directly from the byte array, applying schema validations and filters in CPU registers rather than traversing object graphs. 

Furthermore, tuning the network parameters is essential. You must configure `spark.streaming.receiver.maxRate` to enforce backpressure. Without backpressure, a high-throughput WebSocket will overwhelm the BlockManager, leading to fatal OOMs. Additionally, ensuring that `spark.executor.extraJavaOptions` includes G1GC (or ZGC on modern JVMs) with a carefully calculated `-XX:MaxGCPauseMillis` ensures that garbage collection does not interrupt the TCP heartbeat mechanisms of the WebSocket protocol, preventing abrupt connection terminations.

## 💻 Code Example 2: Structured Streaming with Kafka as a WebSocket Buffer
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# It is generally an anti-pattern to connect Spark directly to WebSockets in production.
# The enterprise pattern uses a lightweight service (e.g., Node.js/Go) to bridge WebSocket to Kafka.
spark = SparkSession.builder \
    .appName("WebSocket_Kafka_Ingestion") \
    .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false") \
    .getOrCreate()

schema = StructType([
    StructField("symbol", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Read the buffered WebSocket stream from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:29092") \
    .option("subscribe", "websocket_raw_events") \
    .option("startingOffsets", "latest") \
    .load()

# Catalyst Optimizer uses the schema to generate highly optimized deserialization bytecode
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Filter pushed down directly after Tungsten memory allocation
high_value_df = parsed_df.filter(col("price") > 1000.0)
```
In an enterprise architecture, directly terminating WebSockets on Spark Executors is an anti-pattern due to elasticity and fault-tolerance issues. This code demonstrates the production-grade approach: a lightweight proxy bridging the WebSocket into an Apache Kafka topic, which Spark then consumes via Structured Streaming. By casting the Kafka value to a string and applying `from_json` with a predefined `StructType`, we empower the Catalyst Optimizer. Catalyst leverages Whole-Stage Code Generation to compile this entire physical plan—deserialization, casting, and filtering—into a single, highly optimized Java function. This bypasses the creation of intermediate `Row` objects, processing the stream directly within Tungsten's off-heap memory, dramatically reducing GC overhead and CPU cycles per event.

## 💻 Code Example 3: Stateful Processing and Watermarking over WebSocket Data
```python
from pyspark.sql.functions import window, avg

# Handling late data from flaky mobile WebSocket connections
watermarked_df = high_value_df \
    .withWatermark("timestamp", "5 seconds") \
    .groupBy(
        window(col("timestamp"), "10 seconds", "5 seconds"),
        col("symbol")
    ) \
    .agg(avg("price").alias("moving_avg_price"))

# Start the continuous query
query = watermarked_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .trigger(processingTime="5 seconds") \
    .start()
```
WebSocket clients, especially on mobile networks, frequently experience latency spikes and temporary disconnects, resulting in out-of-order events. This example demonstrates how to implement stateful processing with event-time watermarking to handle this unreliability. The `withWatermark` function instructs Spark's state store (typically backed by HDFS-compatible storage via RocksDB) to maintain intermediate aggregations for up to 5 seconds of event-time delay. If a WebSocket packet containing an older timestamp arrives within this threshold, Spark updates the running aggregation. Once the watermark passes, the state is purged to prevent memory leaks in the Executor. This guarantees that your Tungsten memory footprint remains bounded, even if thousands of WebSocket connections are actively streaming chaotic, out-of-order data.

## 💻 Code Example 4: Implementing a Custom WebSocket Sink using `foreachBatch`
```python
import websocket
import json

def write_to_websocket(df, epoch_id):
    # Collect data to driver or use mapPartitions to send directly from executors
    # For high throughput, always send from executors using foreachPartition
    
    def send_partition(partition):
        # Establish one WebSocket connection per partition to avoid connection overhead
        ws = websocket.create_connection("ws://dashboard-service:8080/ingest")
        for row in partition:
            payload = json.dumps({
                "window_start": row.window.start.isoformat(),
                "symbol": row.symbol,
                "moving_avg": row.moving_avg_price
            })
            ws.send(payload)
        ws.close()

    # Distribute the WebSocket network I/O across the cluster
    df.rdd.foreachPartition(send_partition)

# Write the aggregated results back to a WebSocket dashboard
sink_query = watermarked_df.writeStream \
    .outputMode("update") \
    .foreachBatch(write_to_websocket) \
    .trigger(processingTime="5 seconds") \
    .start()
```
Outputting data from Spark to a WebSocket requires extreme care to avoid paralyzing the Driver node. This final example illustrates a scalable WebSocket Sink utilizing `foreachBatch` and `foreachPartition`. Rather than collecting the massive result set to the Driver and bottlenecking a single network interface, we distribute the network egress. Inside `send_partition`, each Spark Executor task establishes its own WebSocket connection to the downstream dashboard or API gateway. This parallelizes the TCP connection overhead and the network I/O. Furthermore, since `foreachPartition` operates on Tungsten's `UnsafeRow` objects, we perform a lightweight JSON serialization immediately before transmission. This architecture scales linearly: adding more Executors automatically increases the number of concurrent WebSocket connections, allowing Spark to push millions of updates per second to real-time client dashboards.
</Master Class: WebSockets>
