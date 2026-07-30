# Amazon EC2 Deployment for Apache Spark: Elite Assessment

## Part 1: True/False Questions (10 Questions)

**1. Setting `spark.executor.memoryOverhead` to 5% of `spark.executor.memory` is always sufficient for Spark workloads on EC2.**
* **Answer:** False.
* **Mastery Explanation:** YARN container memory enforcement is strict. If the overhead is too low (default is typically 10% or minimum 384MB), YARN will hard-kill executors with a `Container killed by YARN for exceeding memory limits` error. Off-heap memory, Tungsten operations, and native library allocations require sufficient overhead.

**2. The `ExternalShuffleService` is mandatory for safely using dynamic allocation on EC2 Spot Instances.**
* **Answer:** True.
* **Mastery Explanation:** Without `ExternalShuffleService`, executor deallocation destroys shuffle files stored on that executor's local disk, forcing Spark to recompute the entire upstream stage. The service serves shuffle blocks independently of executor JVM lifetimes.

**3. EBS `gp3` volumes are always superior to Instance NVMe for Spark shuffle spill because they offer dedicated network paths.**
* **Answer:** False.
* **Mastery Explanation:** EBS `gp3` volumes can silently throttle (capped at 1,000 MB/s and 16,000 IOPS) and compete with S3A network traffic. Instance NVMe bypasses the network entirely, offering much lower latency and higher throughput (e.g., 2.5 GB/s sequential write) for heavy shuffle spill.

**4. The S3A Magic Committer reduces job commit latency from O(files) to O(tasks) by using S3 multipart uploads.**
* **Answer:** True.
* **Mastery Explanation:** The Magic Committer writes task output directly to the final S3 path using multipart upload and commits via `CompleteMultipartUpload` calls, eliminating the slow O(n) LIST + COPY + DELETE operations associated with standard file committers.

**5. `CAPACITY_OPTIMIZED` allocation strategy for EMR Instance Fleets prioritizes the absolute lowest spot price.**
* **Answer:** False.
* **Mastery Explanation:** `CAPACITY_OPTIMIZED` selects the Spot pool with the deepest available capacity to minimize interruption probability. `LOWEST_PRICE` prioritizes cost but leads to higher interruption rates, which is disastrous for long-running Spark jobs.

**6. Graviton3 instances generally deliver higher memory bandwidth than equivalent x86 instances, directly benefiting Tungsten hash aggregations.**
* **Answer:** True.
* **Mastery Explanation:** Graviton3's DDR5 memory subsystem provides ~60 GB/s aggregate memory bandwidth vs ~45 GB/s on equivalent m5 instances, directly accelerating memory-bound UnsafeRow hash aggregations.

**7. In an EMR Instance Fleet, the Master node should typically be provisioned using Spot Instances to maximize cost savings.**
* **Answer:** False.
* **Mastery Explanation:** The Master node hosts the YARN ResourceManager and Spark Driver. If it is interrupted, the entire cluster and job fail. It should always be On-Demand.

**8. High-cardinality partitioning (e.g., partitioning by `user_id` with millions of unique users) is recommended when writing Parquet files to S3 to improve read performance.**
* **Answer:** False.
* **Mastery Explanation:** High-cardinality partitioning creates millions of S3 prefixes. S3 throttles at 3,500 PUT/s per prefix, leading to massive write throttling. It is better to partition by low-cardinality fields like date.

**9. When a Spot interruption wave kills all executors holding shuffle map output, Spark's default downstream task retries (`spark.task.maxFailures`) are sufficient to recover.**
* **Answer:** False.
* **Mastery Explanation:** This causes a fetch failure cascade. The DAGScheduler receives `FetchFailed` exceptions, aborts the current stage, and marks the *upstream* shuffle map stage as failed, forcing full-stage recomputation.

**10. Speculative execution (`spark.speculation=true`) is particularly useful on mixed Spot fleets to mitigate task duration variance.**
* **Answer:** True.
* **Mastery Explanation:** Mixed fleets use heterogeneous instance types (e.g., `r6g.2xlarge` and `r5.2xlarge`). The same task might take different amounts of time on different CPUs, creating stragglers. Speculation relaunches slow tasks to prevent stage blockages.

## Part 2: Multiple Choice Questions (15 Questions)

**11. Which configuration setting is critical to prevent YARN from killing executor containers due to Tungsten off-heap allocations?**
A) `spark.memory.fraction`
B) `spark.executor.memoryOverhead`
C) `spark.memory.storageFraction`
D) `spark.yarn.am.memory`
* **Answer:** B
* **Mastery Explanation:** `spark.executor.memoryOverhead` accounts for non-JVM heap memory, including off-heap Tungsten memory. If this boundary is crossed, YARN enforces the hard limit and kills the container.

**12. When configuring `fs.s3a.readahead.range`, what is the optimal value to align with?**
A) The maximum RAM of the executor
B) The HDFS block size (128 MB)
C) The Parquet row group size
D) The default TCP window size
* **Answer:** C
* **Mastery Explanation:** Aligning the readahead range with the Parquet row group size (typically 128MB) ensures exactly one row group is fetched per GET request, minimizing wasted bandwidth on bytes the reader would otherwise discard.

**13. A Spark job UI shows tasks in the "RUNNING" state with zero I/O rates during a shuffle write phase on `gp3` EBS volumes. What is the most likely cause?**
A) Network partition between executors
B) Driver JVM Garbage Collection pause
C) S3 throttling (503 Slow Down)
D) EBS volume silent I/O throttling
* **Answer:** D
* **Mastery Explanation:** When shuffle write exceeds provisioned IOPS, EBS queues I/O. The task thread blocks silently while waiting for disk. Correlating this with CloudWatch `VolumeQueueLength` > 1 confirms the EBS saturation.

**14. What does the `fs.s3a.committer.name=magic` configuration achieve?**
A) Compresses shuffle data using Zstandard magically
B) Eliminates the two-phase rename-on-commit pattern for S3 writes
C) Automatically infers schemas for CSV files without full scans
D) Enables Catalyst predicate pushdown for S3 objects
* **Answer:** B
* **Mastery Explanation:** The Magic Committer directly writes task output via multipart uploads and only requires a `CompleteMultipartUpload` API call at commit time, avoiding the costly O(n) LIST+RENAME operations.

**15. Why is G1GC preferred over ZGC for Spark on EMR 7.x during heavy shuffle workloads?**
A) ZGC does not support Tungsten off-heap memory.
B) G1GC has better concurrent marking keeping pauses < 200ms on large heaps with high allocation rates.
C) ZGC requires AVX-512 instructions not available on Graviton.
D) G1GC disables Spark's BlockManager.
* **Answer:** B
* **Mastery Explanation:** Shuffle-heavy ETL causes massive object allocation rates. G1GC effectively manages these large executor heaps (e.g., 48GB) with concurrent marking, preventing long stop-the-world pauses that trigger heartbeat timeouts.

**16. How does Instance Fleet reduce Spot interruption probability for a Spark cluster?**
A) It prepays for instances up to 3 years in advance.
B) It runs tasks redundantly on multiple instances simultaneously.
C) It diversifies capacity requests across multiple instance families and AZs.
D) It forces AWS to send a 10-minute warning instead of a 2-minute warning.
* **Answer:** C
* **Mastery Explanation:** By specifying multiple instance types (e.g., `r6g`, `r6gd`, `r5`) and using `CAPACITY_OPTIMIZED`, the fleet controller targets Spot pools with the lowest interruption risk, avoiding correlated failures.

**17. Which Catalyst optimization pushes filters directly to Parquet column statistics?**
A) AQE Coalesce Partitions
B) PushDownPredicate
C) BroadcastHashJoin
D) WholeStageCodegen
* **Answer:** B
* **Mastery Explanation:** The logical optimization phase applies `PushDownPredicate`, moving filters below the scan. The Parquet reader uses this to evaluate min/max stats and dictionaries, skipping entire row groups.

**18. What is the primary benefit of enabling Kryo serializer with `unsafe=true` on shuffle-heavy stages?**
A) It encrypts shuffle data with AES-NI.
B) It writes directly from off-heap memory, reducing GC allocation pressure by 15-25%.
C) It compresses shuffle files by 90%.
D) It allows Python UDFs to run without serialization.
* **Answer:** B
* **Mastery Explanation:** Zero-copy Kryo buffers bypass intermediate byte array allocations in the JVM heap, significantly reducing garbage collection overhead during intensive shuffle writes.

**19. When writing checkpoints to S3 to protect against Spot interruptions, what ensures the write completed atomically?**
A) The `_temporary` prefix
B) The Spark UI History Server
C) The `_SUCCESS` sentinel file using S3 strong consistency
D) YARN NodeManager logs
* **Answer:** C
* **Mastery Explanation:** S3 provides strong read-after-write consistency. Checking for the `_SUCCESS` file guarantees the checkpoint is complete and prevents partial reads upon restart.

**20. What is the bottleneck when performing a Broadcast Join with an 11 GB dimension table?**
A) Shuffle network throughput
B) Driver JVM memory and network push capacity
C) Tungsten UnsafeRow size limits
D) Parquet vectorized reader batch limits
* **Answer:** B
* **Mastery Explanation:** Broadcast joins require the Driver to collect the entire table, serialize it, and push it to all executors. Tables larger than 8-10 GB typically crash the Driver or cause severe network saturation.

**21. Why should you avoid `LOWEST_PRICE` Allocation Strategy for long-running Spark jobs?**
A) It only provisions older generation hardware.
B) It ignores the `WeightedCapacity` configuration.
C) It increases the probability of spot interruptions by targeting shallow capacity pools.
D) It requires manual IAM role adjustments.
* **Answer:** C
* **Mastery Explanation:** `LOWEST_PRICE` targets the absolute cheapest pool, which is often heavily utilized and prone to immediate interruptions. A single interruption causes full-stage recomputation, wiping out any hourly cost savings.

**22. Which JVM flag activates ARM-optimized hardware acceleration for encrypted shuffles on Graviton3?**
A) `-XX:+UseZGC`
B) `-XX:+UseStringDeduplication`
C) `-XX:+UseAES -XX:+UseAESIntrinsics`
D) `-XX:+EnableARM64`
* **Answer:** C
* **Mastery Explanation:** These flags enable ARM's hardware AES-NI instructions, speeding up encrypted shuffles by 3-5x compared to software-based AES.

**23. What happens if an executor's heap is 48GB, `memoryOverhead` is 6GB, and Tungsten `offHeap.size` is set to 8GB?**
A) Spark runs faster due to more memory.
B) The job fails because off-heap size exceeds memory overhead.
C) YARN allocates a 62GB container automatically.
D) Tungsten falls back to JVM heap allocation.
* **Answer:** B
* **Mastery Explanation:** Off-heap memory is strictly bounded by the `memoryOverhead` configuration. If `offHeap.size` (8GB) > `memoryOverhead` (6GB), the container will breach its YARN memory limit and be killed.

**24. What is the impact of a high `spark.task.maxFailures` value (e.g., 8) on a Spot cluster?**
A) It disables speculative execution.
B) It allows Spark to tolerate more transient task losses without aborting the entire job.
C) It increases shuffle spill latency.
D) It prevents the Driver from receiving heartbeat timeouts.
* **Answer:** B
* **Mastery Explanation:** Spot interruptions cause artificial task failures. Increasing `maxFailures` prevents the DAGScheduler from prematurely failing the job when executors are frequently preempted.

**25. Why is AQE (Adaptive Query Execution) critical on a heterogeneous Spot fleet?**
A) It dynamically bids on lower spot prices.
B) It merges small post-shuffle partitions automatically to account for varying partition sizes from different instance types.
C) It automatically tunes JVM garbage collection based on CPU architecture.
D) It switches from Parquet to ORC format dynamically.
* **Answer:** B
* **Mastery Explanation:** Heterogeneous nodes process data at different speeds, leading to unpredictable partition sizes post-shuffle. AQE's `coalescePartitions` dynamically groups these to the target `advisoryPartitionSizeInBytes`, avoiding tiny task overheads.

## Part 3: Small Twist Questions (15 Questions)

**26. Scenario:** You enable dynamic allocation on EMR. You forget to set `spark.shuffle.service.enabled=true`.
* **Twist:** What happens during a scale-down event?
* **Answer:** Executor scale-down deletes local shuffle data. Downstream tasks fail to fetch blocks, resulting in `FetchFailedException` and full-stage recomputations.
* **Mastery Explanation:** Dynamic allocation removes idle executors. Without the External Shuffle Service serving their blocks, any shuffle data they held is lost forever, crippling job stability.

**27. Scenario:** You set `fs.s3a.committer.name=magic`. However, the S3 bucket is configured with object lock and versioning strictly enforced.
* **Twist:** Does the job complete faster?
* **Answer:** The job will likely fail or experience massive cost bloat.
* **Mastery Explanation:** While the committer uses `CompleteMultipartUpload`, strict object lock or complex versioning lifecycle rules can interfere with multipart upload part management or incur massive costs for aborted parts if not cleaned up properly, though standard S3 buckets see immense speedups.

**28. Scenario:** You deploy an Instance Fleet with `m6g.xlarge` (Graviton) and `m5.xlarge` (x86) in the same core pool. You set `-XX:+UseAESIntrinsics`.
* **Twist:** What happens to tasks running on the `m5.xlarge`?
* **Answer:** The JVM on x86 will recognize the flag (as it's a standard HotSpot flag) and use x86 AES-NI, but if you used ARM-specific JNI libraries, it would crash. Here, the standard flag works safely on both.
* **Mastery Explanation:** Standard JVM flags like `-XX:+UseAES` adapt to the underlying hardware (x86 AES-NI vs ARM SVE/AES).

**29. Scenario:** You partition your S3 output by `date`. You then change the code to partition by `timestamp` (down to the second).
* **Twist:** How does this affect S3 write performance?
* **Answer:** It triggers severe S3 throttling (HTTP 503 Slow Down).
* **Mastery Explanation:** `timestamp` is extremely high cardinality. This creates millions of S3 prefixes. S3 enforces a hard limit of 3,500 PUTs per second per prefix.

**30. Scenario:** Your EBS `gp3` volumes have 16,000 IOPS. You upgrade to `io2` Block Express with 64,000 IOPS to solve a shuffle bottleneck.
* **Twist:** The UI still shows zero I/O and "RUNNING" stalls. Why?
* **Answer:** The EC2 instance network bandwidth is saturated by S3 reads/writes.
* **Mastery Explanation:** EBS runs over the network. If the instance type (e.g., `m5.4xlarge`) only has 10 Gbps total network bandwidth, maximizing S3 throughput starves the EBS network link, rendering the IOPS upgrade useless.

**31. Scenario:** You configure a Spark Structured Streaming job to use Spot Instances for the executor fleet.
* **Twist:** You set `spark.task.maxFailures=8`. What is the fundamental risk?
* **Answer:** Micro-batch latency spikes unpredictably.
* **Mastery Explanation:** While high `maxFailures` prevents job crashes, the time taken to recompute lost shuffle map data during a Spot interruption causes severe SLA violations in low-latency streaming workloads. Spot is for ETL, not sub-minute streaming.

**32. Scenario:** You set `spark.memory.offHeap.size=4g`. You forget to set `spark.memory.offHeap.enabled=true`.
* **Twist:** How much off-heap memory does Tungsten use?
* **Answer:** 0 GB.
* **Mastery Explanation:** The `.size` configuration is completely ignored if `.enabled` is not explicitly set to `true`. Tungsten will fall back to using JVM heap for `UnsafeRow` allocations.

**33. Scenario:** You run a query with `.filter(col("amount") > 100.0)` on a Parquet file. You change it to `.filter(udf_check(col("amount")))`.
* **Twist:** How does S3 read throughput change?
* **Answer:** S3 read volume spikes massively.
* **Mastery Explanation:** Python UDFs cannot be pushed down to the Parquet reader. The Catalyst optimizer abandons predicate pushdown, forcing Spark to read every single row group from S3 into memory before applying the UDF.

**34. Scenario:** You configure Instance Fleet with 3 AZ subnets. The `TargetSpotCapacity` is 100.
* **Twist:** AZ 'us-east-1a' has an outage. Does the job fail?
* **Answer:** Not necessarily, but EMR will attempt to provision the entire 100 capacity from the remaining two AZs.
* **Mastery Explanation:** Instance Fleet automatically falls back and shifts capacity requests to healthy/available subnets defined in the configuration, maintaining cluster size if capacity exists.

**35. Scenario:** You use `df.coalesce(1).write.parquet()`.
* **Twist:** Does the Magic Committer speed up this write?
* **Answer:** Minimally.
* **Mastery Explanation:** `coalesce(1)` forces all data through a single task on a single executor. The Magic Committer optimizes O(n) task commits, but with only 1 task, the bottleneck is purely single-thread compute and network upload speed, not commit metadata overhead.

**36. Scenario:** You configure `fs.s3a.fast.upload.buffer=array`. The executor runs low on JVM heap.
* **Twist:** What happens to the multipart uploads?
* **Answer:** The executor throws an OutOfMemoryError (OOM).
* **Mastery Explanation:** `array` buffers the multipart chunks in the JVM heap. Under memory pressure, this causes catastrophic OOMs. Changing it to `disk` stages the chunks to local storage (NVMe/EBS) safely.

**37. Scenario:** You set `spark.speculation.multiplier=1.1` on a mixed Spot fleet.
* **Twist:** What happens to cluster utilization?
* **Answer:** It plummets due to severe task thrashing.
* **Mastery Explanation:** A multiplier of 1.1 means any task 10% slower than the median is speculated. On a mixed fleet (e.g., fast Graviton3 + slow older x86), normal hardware variance exceeds 10%, causing Spark to constantly kill and relaunch perfectly healthy tasks.

**38. Scenario:** You change `spark.sql.parquet.enableVectorizedReader` from `true` to `false`.
* **Twist:** How does this affect CPU utilization?
* **Answer:** CPU utilization skyrockets for deserialization.
* **Mastery Explanation:** Disabling the vectorized reader forces Spark to deserialize Parquet data row-by-row into Java objects, abandoning SIMD Arrow-style batches and causing massive GC pressure and CPU burn.

**39. Scenario:** You implement the `read_or_checkpoint` idempotent pattern. A spot interruption occurs exactly as the `_SUCCESS` file is being written.
* **Twist:** Does the restarted job read corrupted data?
* **Answer:** No.
* **Mastery Explanation:** S3's strong consistency ensures that either the `_SUCCESS` file is fully visible (and thus the Parquet parts are complete), or it doesn't exist. If it doesn't exist, the job correctly falls back to recomputing the stage.

**40. Scenario:** You use an `r6g.4xlarge` (Graviton) instance but leave `spark.shuffle.sort.bypassMergeThreshold` at its default (200).
* **Twist:** Are you fully utilizing the memory bandwidth?
* **Answer:** No.
* **Mastery Explanation:** Graviton's high memory bandwidth allows for much larger in-memory sort buffers before spilling. Leaving it at default means Spark writes smaller spill files prematurely, increasing disk I/O instead of utilizing RAM.

## Part 4: Coding & Debugging Questions (10 Questions)

**41. Debugging S3 Throttling**
* **Symptom:** Your log shows `java.nio.file.AccessDeniedException: 503 Slow Down`. The code is: `.partitionBy("user_id", "timestamp").parquet("s3a://out/")`
* **Fix & Explanation:** Change partitioning to `.partitionBy("year", "month")`. High cardinality partitioning creates too many S3 PUT requests per second per prefix, hitting S3's hard rate limit.

**42. Identifying Memory Leaks from YARN**
* **Symptom:** Executor dies with `Container killed by YARN for exceeding memory limits. 54.1 GB of 54 GB physical memory used.`
* **Fix & Explanation:** The off-heap memory or native libraries (like Python/Pandas UDFs) have exceeded the YARN container budget. Increase `spark.executor.memoryOverhead` from its default to a higher value (e.g., `6g` or `8g`).

**43. Fixing FetchFailed Cascades**
* **Symptom:** A 3-hour job fails with `Job aborted due to stage failure: Task X in stage Y failed 4 times`. You are on 100% Spot instances.
* **Fix & Explanation:** Spot nodes were terminated. Increase `spark.task.maxFailures` (e.g., 8) and `spark.stage.maxConsecutiveAttempts` (e.g., 8) to give Spark the runway to retry tasks on surviving nodes rather than failing the whole job.

**44. Correcting EBS Queue Saturation**
* **Symptom:** `VolumeQueueLength` > 15 in CloudWatch. Tasks are "RUNNING" but `Shuffle Write` is stalled at 0 B/s.
* **Fix & Explanation:** The EBS `gp3` volume IOPS/throughput limits are saturated by shuffle spills. Either switch to an instance with NVMe instance store (like `r6id`) or provision higher IOPS/throughput for the EBS volumes.

**45. Optimizing Parquet Reads**
* **Symptom:** S3 network traffic is triple the size of the Parquet dataset being read, despite filtering.
* **Fix & Explanation:** The `fs.s3a.readahead.range` is likely misaligned (e.g., set to default TCP sizes). Align it to the Parquet row group size (e.g., 128 MiB `134217728` bytes) to prevent fetching partial chunks that the vectorized reader discards.

**46. Enabling Magic Committer Safely**
* **Symptom:** Job takes 20 minutes to compute and 45 minutes to finish writing 50,000 output files.
* **Fix & Explanation:** Add `.config("spark.hadoop.fs.s3a.committer.name", "magic")`. The current setup is doing 150,000 S3 API calls (LIST/COPY/DELETE). Magic Committer uses `CompleteMultipartUpload`, reducing commit time to seconds.

**47. Resolving Driver OOM on Broadcast**
* **Symptom:** `java.lang.OutOfMemoryError: Java heap space` on the Driver node during a Join operation.
* **Fix & Explanation:** A table over the broadcast threshold (e.g., 10GB) is being forced into a BroadcastHashJoin. Disable auto broadcast `spark.sql.autoBroadcastJoinThreshold=-1` or increase Driver memory if the broadcast is explicitly required.

**48. Fixing OOM during S3 Uploads**
* **Symptom:** Executors crash with JVM Heap OOM during the final write stage to S3.
* **Fix & Explanation:** `spark.hadoop.fs.s3a.fast.upload.buffer` is set to `array` (in-memory). Change it to `disk` so multipart chunks are staged to the local EBS/NVMe disk instead of bloating the JVM heap.

**49. Correcting Spot Allocation Strategy**
* **Symptom:** The EMR Instance Fleet provisions quickly but nodes are terminated every 15 minutes, causing perpetual recomputation.
* **Fix & Explanation:** The LaunchSpecification `AllocationStrategy` is set to `LOWEST_PRICE`. Change it to `CAPACITY_OPTIMIZED` to instruct EC2 to pick pools with the lowest interruption risk.

**50. Checkpoint Lineage Truncation**
* **Symptom:** After an hour-long stage completes, a node dies in the next stage. The Spark UI shows it is re-reading the raw CSV files from step 1.
* **Fix & Explanation:** Lineage is too long. Implement S3-backed checkpoints using `df.write.parquet(ckpt_path)` and reloading it via `spark.read.parquet(ckpt_path)` between major shuffles. This truncates the DAG and prevents cascading recomputation.
