# Spark VM Setup: Master Class Assessment

This elite-level assessment tests deep architectural knowledge of Apache Spark's JVM internals, Catalyst optimizations, memory management, and execution engines (Tungsten), specifically within constrained Virtual Machine (VM) or local environments.

## Part 1: True/False Questions (10 Questions)

1. **Question:** In `local[*]` mode, Spark bypasses network serialization entirely because data is shared between Executor threads directly via heap memory references without being serialized.
   * **Correct Answer:** False
   * **Mastery Explanation:** Even in local mode, Spark strictly simulates distributed processing. Data is still serialized to byte streams to move between threads or spill to disk during shuffles. It does not simply pass JVM object references.

2. **Question:** Setting `spark.sql.shuffle.partitions` to match the number of allocated logical cores (e.g., 4) in a local VM dramatically improves local shuffle performance compared to the default value of 200.
   * **Correct Answer:** True
   * **Mastery Explanation:** The default of 200 partitions creates significant task overhead and tiny files. Matching partitions to logical cores in a constrained VM avoids this overhead and aligns task concurrency with CPU capacity.

3. **Question:** Spark’s storage memory is primarily utilized for execution operations such as shuffles, joins, and sorts.
   * **Correct Answer:** False
   * **Mastery Explanation:** Storage memory is used for caching RDDs and DataFrames. Execution memory is the region used for operations like shuffles, joins, and sorts.

4. **Question:** Tungsten off-heap memory allocations are subject to JVM Garbage Collection but benefit greatly from G1GC's incremental region-based collection.
   * **Correct Answer:** False
   * **Mastery Explanation:** Off-heap memory (managed via `sun.misc.Unsafe`) is allocated directly from the OS, bypassing the JVM heap entirely. Therefore, it is completely immune to JVM Garbage Collection pauses.

5. **Question:** If the total memory footprint of Spark (JVM Heap + Off-Heap Memory) exceeds the physical RAM of the Linux VM, the Linux Out-Of-Memory Killer (OOMK) may terminate the Spark process.
   * **Correct Answer:** True
   * **Mastery Explanation:** The Linux kernel monitors overall OS memory. Since off-heap memory directly consumes physical OS memory outside the JVM, over-allocating it will trigger the kernel's OOMK to kill the process to save the OS.

6. **Question:** Changing the shuffle compression codec to `zstd` guarantees reduced CPU usage at the cost of a larger disk footprint.
   * **Correct Answer:** False
   * **Mastery Explanation:** It is the opposite. Zstandard (`zstd`) trades slightly increased CPU cycles for a significantly reduced memory and disk footprint due to its superior compression ratio compared to the default `lz4`.

7. **Question:** Increasing `spark.shuffle.file.buffer` from 32k to 1m optimizes disk writes during spills by decreasing the number of I/O system calls.
   * **Correct Answer:** True
   * **Mastery Explanation:** A larger buffer means Spark accumulates more data in RAM before flushing to disk, resulting in larger, less frequent writes, which mitigates disk I/O bottlenecks common in VMs.

8. **Question:** Enabling the KryoSerializer is achieved by setting `spark.serializer` to `org.apache.spark.serializer.KryoSerializer`.
   * **Correct Answer:** True
   * **Mastery Explanation:** This is the exact configuration required. Kryo is significantly faster and more compact than the default Java serialization, reducing the footprint of shuffled data.

9. **Question:** The G1GC parameter `-XX:InitiatingHeapOccupancyPercent=35` configures the garbage collector to wait until the heap is 65% full before starting concurrent marking.
   * **Correct Answer:** False
   * **Mastery Explanation:** It configures G1GC to start concurrent marking when the heap reaches exactly 35% occupancy, which is more proactive than the default 45% and prevents full GC pauses in Spark.

10. **Question:** The `spark.reducer.maxSizeInFlight` setting determines the maximum amount of memory consumed by a single reduce task when fetching shuffle data from map tasks.
    * **Correct Answer:** True
    * **Mastery Explanation:** Limiting this buffer prevents reduce tasks from exhausting JVM execution memory when pulling large amounts of remote (or local) shuffle blocks simultaneously.

## Part 2: Multiple Choice Questions (15 Questions)

11. **Question:** Which JVM memory region does Spark promote surviving DataFrame objects to if they outlive minor GC cycles?
    * A. Eden Space
    * B. Survivor Space
    * C. Old Generation
    * D. Metaspace
    * **Correct Answer:** C
    * **Mastery Explanation:** Objects are created in the Young Gen (Eden/Survivor). If they survive minor GC cycles (like cached DataFrames), they are promoted to the Old Generation.

12. **Question:** What is the default number of `spark.sql.shuffle.partitions` in Apache Spark?
    * A. 4
    * B. 200
    * C. 1000
    * D. Equal to logical cores
    * **Correct Answer:** B
    * **Mastery Explanation:** The default is 200. In a local VM, this often leads to excessive task scheduling overhead for small datasets, making tuning this parameter critical.

13. **Question:** Which Garbage Collector is highly recommended for Spark workloads involving large in-memory caches on a VM to avoid long "Stop-The-World" pauses?
    * A. Parallel GC
    * B. CMS GC
    * C. G1GC
    * D. ZGC
    * **Correct Answer:** C
    * **Mastery Explanation:** G1GC (Garbage-First) divides the heap into regions and performs GC incrementally, resulting in highly predictable and shorter pause times compared to Parallel GC.

14. **Question:** Which Tungsten feature allows Spark to operate on serialized data directly without deserializing it back into Java objects?
    * A. Project Hydrogen
    * B. Off-heap memory & Binary Processing
    * C. Kryo Fallback
    * D. Catalyst Cost-Based Optimizer
    * **Correct Answer:** B
    * **Mastery Explanation:** Tungsten operates directly on serialized binary data (often in off-heap memory), avoiding the CPU overhead and GC pressure of instantiating Java objects for simple operations like sorting.

15. **Question:** What does the `-XX:InitiatingHeapOccupancyPercent` parameter control in G1GC?
    * A. The maximum size of the Old Generation.
    * B. The heap occupancy threshold to trigger the start of concurrent marking.
    * C. The percentage of total RAM dedicated to off-heap memory.
    * D. The ratio of Execution memory to Storage memory.
    * **Correct Answer:** B
    * **Mastery Explanation:** It defines the percentage of heap occupancy at which G1GC begins its concurrent marking phase. Setting it lower (e.g., 35%) makes GC more proactive.

16. **Question:** How does Spark strictly simulate a distributed environment in `local[*]` mode?
    * A. By launching multiple JVM processes on the host OS.
    * B. By managing concurrent tasks via threads within a single JVM process.
    * C. By disabling the Catalyst optimizer to run linearly.
    * D. By utilizing a localized Docker daemon to orchestrate containers.
    * **Correct Answer:** B
    * **Mastery Explanation:** Local mode runs the Driver and Executors in a single, unified JVM process, utilizing thread pools to simulate distributed concurrent tasks.

17. **Question:** What is the primary purpose of execution memory in the Spark memory model?
    * A. Caching DataFrames for iterative algorithms.
    * B. Storing broadcast variables on Executors.
    * C. Buffering data for operations like shuffles, joins, and sorts.
    * D. Storing the Catalyst physical execution plan.
    * **Correct Answer:** C
    * **Mastery Explanation:** Execution memory is specifically allocated for short-lived, heavy computations like sorts, hash aggregations, and shuffle buffers.

18. **Question:** Which parameter injects G1GC configurations directly into the JVM for the local Spark driver?
    * A. `spark.executor.extraJavaOptions`
    * B. `spark.driver.memory`
    * C. `spark.driver.extraJavaOptions`
    * D. `spark.sql.gc.collector`
    * **Correct Answer:** C
    * **Mastery Explanation:** `spark.driver.extraJavaOptions` passes raw JVM arguments (like `-XX:+UseG1GC`) directly to the JVM hosting the local Driver (which also acts as the Executor in local mode).

19. **Question:** What is a key consequence of increasing `spark.shuffle.file.buffer` to 1m on a VM?
    * A. It increases network timeouts during shuffles.
    * B. It reduces the number of I/O system calls when spilling data to disk.
    * C. It limits the size of broadcast variables to 1MB.
    * D. It automatically disables off-heap memory usage.
    * **Correct Answer:** B
    * **Mastery Explanation:** A larger buffer means disk writes happen in larger, less frequent chunks (1MB instead of 32KB), drastically improving disk throughput on constrained VM storage.

20. **Question:** Why is explicitly configuring `spark.local.dir` to a high-speed SSD crucial for local VM setups?
    * A. To store the Spark installation binaries permanently.
    * B. To mitigate massive performance degradation caused by disk spilling during heavy shuffles.
    * C. To cache DataFrames permanently across SparkSession restarts.
    * D. To persist the Catalyst logical plans as JSON.
    * **Correct Answer:** B
    * **Mastery Explanation:** When execution memory fills up, Spark spills data to `spark.local.dir`. If this points to a slow virtualized drive, the job stalls. Pointing it to an SSD mitigates this bottleneck.

21. **Question:** What happens if Tungsten off-heap memory usage combined with the JVM heap exceeds the VM's OS limits?
    * A. Spark automatically shrinks the JVM heap dynamically.
    * B. The JVM throws a standard `java.lang.OutOfMemoryError`.
    * C. The Linux OOM Killer abruptly terminates the Spark process.
    * D. Spark gracefully spills the off-heap memory to HDFS.
    * **Correct Answer:** C
    * **Mastery Explanation:** Because off-heap memory is invisible to the JVM's internal memory manager, overallocating it causes the entire OS to run out of RAM, triggering the Linux kernel to kill the process.

22. **Question:** Which compression codec does the curriculum recommend for trading slightly increased CPU cycles for a significantly reduced memory/disk footprint?
    * A. lz4
    * B. snappy
    * C. gzip
    * D. zstd
    * **Correct Answer:** D
    * **Mastery Explanation:** Zstandard (`zstd`) provides a superior compression ratio compared to the default `lz4`, reducing disk I/O at the cost of a slight CPU overhead.

23. **Question:** Which method extracts the exact physical operators and cost statistics chosen by Catalyst for local testing?
    * A. `df.show()`
    * B. `df.printSchema()`
    * C. `df.explain("cost")`
    * D. `df.queryExecution.logical`
    * **Correct Answer:** C
    * **Mastery Explanation:** `explain("cost")` (or inspecting `queryExecution.executedPlan`) outputs the physical plan along with the cost estimates Catalyst used to make decisions like join strategies.

24. **Question:** How can you programmatically force a `BroadcastHashJoin` for local optimization testing?
    * A. By setting `spark.sql.join.preferSortMergeJoin` to false.
    * B. By using the `broadcast()` hint on the DataFrame and adjusting `spark.sql.autoBroadcastJoinThreshold`.
    * C. By disabling all shuffle partitions.
    * D. By allocating at least 10GB of off-heap memory.
    * **Correct Answer:** B
    * **Mastery Explanation:** Wrapping the smaller DataFrame in `broadcast()` and ensuring the threshold is high enough forces Catalyst to select a BroadcastHashJoin instead of a SortMergeJoin.

25. **Question:** By default, what heap occupancy threshold does G1GC use to start concurrent marking if not explicitly configured?
    * A. 25%
    * B. 35%
    * C. 45%
    * D. 60%
    * **Correct Answer:** C
    * **Mastery Explanation:** The default `InitiatingHeapOccupancyPercent` in Java is 45%. For Spark's rapid object generation, this is often too late, which is why tuning it down to 35% is recommended.

## Part 3: Small Twist Questions (15 Questions)

26. **Scenario:** You set `spark.driver.memory` to 4g and `spark.memory.offHeap.size` to 4g on a VM with exactly 8GB of RAM. The OS requires 1GB to run.
    * **Twist:** You run a heavy memory-intensive DataFrame caching job. What happens?
    * **Correct Answer:** The Linux OOM Killer abruptly terminates the Spark JVM.
    * **Mastery Explanation:** 4GB (Heap) + 4GB (Off-Heap) = 8GB. The OS needs 1GB. Total demand is 9GB on an 8GB machine. The Linux kernel will kill the JVM to prevent an OS crash.

27. **Scenario:** You optimize a local job by changing `spark.sql.shuffle.partitions` from 200 to 4 in a `local[4]` environment.
    * **Twist:** You process 10GB of data, but one specific key contains 9.9GB of the data. What happens?
    * **Correct Answer:** A severe OOM error or massive disk spill occurs on a single thread.
    * **Mastery Explanation:** Data skew invalidates uniform partitioning. One thread receives 9.9GB of data to sort/reduce, instantly exhausting its execution memory fraction, despite matching core counts.

28. **Scenario:** You configure `-XX:InitiatingHeapOccupancyPercent=85`.
    * **Twist:** Your Spark job generates millions of short-lived objects rapidly during a complex `groupBy`. What happens?
    * **Correct Answer:** The job suffers from massive "Stop-The-World" full GC pauses.
    * **Mastery Explanation:** Waiting until the heap is 85% full to start concurrent marking is too late. The Old Gen fills up completely before marking finishes, forcing a synchronous, application-freezing Full GC.

29. **Scenario:** You want to use Tungsten off-heap memory, so you set `spark.memory.offHeap.size` to "2g".
    * **Twist:** You forget to set `spark.memory.offHeap.enabled` to "true". What happens?
    * **Correct Answer:** Spark ignores the size configuration and allocates all execution data on the standard JVM heap.
    * **Mastery Explanation:** The `enabled` flag is strictly required. Without it, Tungsten defaults to on-heap allocation, resulting in unexpected JVM GC pressure.

30. **Scenario:** You configure `spark.local.dir` to `/mnt/shared-nas` (a Network Attached Storage drive).
    * **Twist:** You trigger a `SortMergeJoin` that exceeds your VM's execution memory. What happens?
    * **Correct Answer:** Job performance degrades drastically due to extreme network latency and disk I/O bottlenecks.
    * **Mastery Explanation:** Spill files are heavily read/written during shuffles. Pointing the local dir to a NAS routes all this temporary swap traffic over the network instead of a local SSD, crippling performance.

31. **Scenario:** You set `spark.reducer.maxSizeInFlight` to 2g on a local VM with a total JVM heap of 4g.
    * **Twist:** 4 reduce tasks run concurrently in `local[4]`. What happens?
    * **Correct Answer:** The job instantly crashes with a JVM `OutOfMemoryError`.
    * **Mastery Explanation:** 4 concurrent tasks attempting to buffer up to 2GB each requires 8GB of execution memory. This vastly exceeds the 4GB heap, causing an OOM.

32. **Scenario:** You use the `broadcast(df)` hint on a 50MB DataFrame. The `spark.sql.autoBroadcastJoinThreshold` is left at the 10MB default.
    * **Twist:** You are running Spark 3.x. What happens?
    * **Correct Answer:** Catalyst performs a `BroadcastHashJoin` anyway.
    * **Mastery Explanation:** In modern Spark, explicit developer hints (`broadcast()`) override the size-based threshold configuration.

33. **Scenario:** You are debugging Catalyst locally. You run `df.explain("cost")` and see a `SortMergeJoin`.
    * **Twist:** You aggressively decrease `spark.sql.autoBroadcastJoinThreshold` to 1KB. What happens?
    * **Correct Answer:** Catalyst still chooses `SortMergeJoin`.
    * **Mastery Explanation:** Decreasing the threshold makes Spark *less* likely to broadcast. Since the tables are larger than 1KB, it defaults to the robust `SortMergeJoin`.

34. **Scenario:** You switch `spark.serializer` to `KryoSerializer` but leave `spark.kryoserializer.buffer.max` at its default (64m).
    * **Twist:** Your DataFrame contains a massive nested JSON string that serializes to 100MB. What happens?
    * **Correct Answer:** The job fails with a Kryo buffer overflow exception.
    * **Mastery Explanation:** Kryo requires the max buffer to be strictly larger than the largest single serialized object. It will not automatically chunk single objects.

35. **Scenario:** You allocate a massive 16-core VM and start Spark with `local[*]`.
    * **Twist:** You manually set `spark.sql.shuffle.partitions=2` and trigger a global aggregation. What happens?
    * **Correct Answer:** The aggregation executes using only 2 cores, leaving 14 cores completely idle.
    * **Mastery Explanation:** The number of reduce tasks is exactly equal to `shuffle.partitions`. Even with 16 cores available, only 2 tasks exist to run, resulting in massive underutilization.

36. **Scenario:** You set `spark.io.compression.codec` to `zstd` to minimize spill sizes.
    * **Twist:** Your VM has severely throttled vCPUs but an ultra-fast local NVMe PCI-e drive. What happens?
    * **Correct Answer:** CPU bottlenecking makes the job slower than if you used `lz4` or uncompressed shuffles.
    * **Mastery Explanation:** `zstd` trades CPU for disk I/O. If disk I/O is already infinitely fast (NVMe) but CPUs are weak, the overhead of Zstandard compression becomes the primary bottleneck.

37. **Scenario:** You use Spark's unified memory model and allocate 90% of the heap to storage (`spark.memory.storageFraction=0.9`).
    * **Twist:** Your job performs zero caching, but involves massive `SortMergeJoin` shuffles. What happens?
    * **Correct Answer:** The job executes normally without OOMs, as execution memory dynamically borrows the unused storage space.
    * **Mastery Explanation:** Under the Unified Memory Management model, if storage memory is empty, execution memory can freely borrow that space for shuffle buffers.

38. **Scenario:** You set `spark.shuffle.file.buffer=1m` on a tiny VM with only 500MB of RAM.
    * **Twist:** You have 200 shuffle partitions and run 4 map tasks concurrently. What happens?
    * **Correct Answer:** The map phase crashes with an OOM error.
    * **Mastery Explanation:** 4 tasks writing to 200 partitions = 800 open file buffers. 800 * 1MB = 800MB of memory required just for buffers, which exceeds the 500MB VM RAM.

39. **Scenario:** You start Spark on an 8-core VM but configure the master URL as `local`.
    * **Twist:** You set `spark.sql.shuffle.partitions=8` and run a join. What happens?
    * **Correct Answer:** The join runs entirely sequentially on a single thread.
    * **Mastery Explanation:** The string `local` (without `[*]` or `[N]`) dictates exactly ONE thread. Regardless of partitions or physical cores, tasks will execute one by one.

40. **Scenario:** You configure `-XX:+UseParallelGC` instead of G1GC for a job with a 10GB heap.
    * **Twist:** The job caches 8GB of DataFrames, leaving minimal free space. What happens?
    * **Correct Answer:** The VM experiences massive, multi-second "Stop-The-World" freezes.
    * **Mastery Explanation:** Parallel GC pauses application threads to sweep the entire heap at once. With 8GB of long-lived cached objects, the GC pauses will be catastrophic compared to G1GC's incremental approach.

## Part 4: Coding & Debugging Questions (10 Questions)

41. **Debugging:** A local job crashes with `java.lang.OutOfMemoryError: GC overhead limit exceeded`. The VM has a 4GB heap.
    * **Code:** `df.groupBy("id").agg(collect_list("data")).show()` (where one "id" maps to 5GB of data).
    * **Identify the error:** `collect_list` gathers all values for a single key into a single array entirely in memory on the Executor thread. A 5GB array cannot fit in a 4GB heap.
    * **Mastery Fix:** Avoid `collect_list` for massive skewed keys. Use window functions, or increase heap size if strictly necessary.

42. **Debugging:** You configure Tungsten off-heap memory but the JVM crashes via Linux OOM Killer.
    * **Code:** 
      `spark.conf.set("spark.memory.offHeap.enabled", "true")`
      `spark.conf.set("spark.driver.memory", "4g")`
    * **Identify the error:** You enabled off-heap memory but failed to define `spark.memory.offHeap.size`. Alternatively, if defaulted, the combined requested size exceeds physical OS RAM.
    * **Mastery Fix:** Explicitly declare `spark.memory.offHeap.size` (e.g., `2g`) and ensure Heap + Off-Heap + OS Overhead < Physical VM RAM.

43. **Debugging:** Catalyst is failing to push down filters to a Parquet file.
    * **Code:**
      `val rdd = df.rdd.map(row => (row.getInt(0), row.getString(1)))`
      `val df2 = rdd.toDF("id", "value").filter($"id" > 100)`
    * **Identify the error:** Converting a DataFrame to an RDD and back breaks Catalyst's logical lineage and drops data source statistics.
    * **Mastery Fix:** Keep the transformations entirely within the DataFrame/SQL API (`df.select(...)`) so Catalyst can push the filter down to the Parquet reader.

44. **Debugging:** A shuffle processes 50MB of data on an 8-core VM but takes 5 minutes.
    * **Code:** `spark = SparkSession.builder.master("local[*]").getOrCreate()` (No other configs).
    * **Identify the error:** `spark.sql.shuffle.partitions` defaults to 200. Shuffling 50MB across 200 partitions creates 200 tiny tasks with massive scheduling overhead and micro-files.
    * **Mastery Fix:** Set `spark.sql.shuffle.partitions` to 8 (matching `local[*]`) for small local datasets.

45. **Debugging:** Kryo serialization fails at runtime.
    * **Code:** 
      `.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")`
      `.config("spark.kryo.registrationRequired", "true")`
    * **Identify the error:** `registrationRequired=true` forces Kryo to reject any custom class that hasn't been explicitly registered, throwing an `IllegalArgumentException`.
    * **Mastery Fix:** Provide a KryoRegistrator class or use `spark.kryo.classesToRegister` to explicitly register all custom domain objects.

46. **Debugging:** Local disk throws "No space left on device" during a shuffle.
    * **Code:** `.config("spark.local.dir", "/tmp/spark-spill")`
    * **Context:** `/tmp` is a 2GB `tmpfs` (ramdisk). The dataset is 10GB.
    * **Identify the error:** The shuffle spill directory is pointed to a tiny RAM disk which fills up instantly when execution memory spills to disk.
    * **Mastery Fix:** Point `spark.local.dir` to a persistent, high-capacity SSD volume (e.g., `/mnt/fast-ssd/spark-temp`).

47. **Debugging:** VM runs out of memory after 50 iterations of a loop.
    * **Code:** `for (i <- 1 to 1000) { spark.read.csv(s"file_$i").cache().count() }`
    * **Identify the error:** The loop caches a new DataFrame on every iteration without releasing previous ones. This fills up Storage Memory until it eventually OOMs.
    * **Mastery Fix:** Call `unpersist()` on the DataFrame at the end of each iteration.

48. **Debugging:** The JVM fails to start locally due to a configuration syntax error.
    * **Code:** `.config("spark.driver.extraJavaOptions", "-XX:+UseG1GC; -XX:InitiatingHeapOccupancyPercent=35")`
    * **Identify the error:** JVM arguments must be separated by spaces, not semicolons. The semicolon causes the JVM parser to fail.
    * **Mastery Fix:** Remove the semicolon: `"-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35"`.

49. **Debugging:** CPU monitoring shows only 2 cores are active on a 16-core VM.
    * **Code:** 
      `.master("local[2]")`
      `.config("spark.sql.shuffle.partitions", "200")`
    * **Identify the error:** The master URL is hardcoded to `local[2]`, which artificially limits the Spark JVM thread pool to exactly 2 execution threads, ignoring the other 14 physical cores.
    * **Mastery Fix:** Change the master string to `local[*]` or `local[16]`.

50. **Debugging:** Job crashes with OOM during the reduce phase.
    * **Code:** `.config("spark.reducer.maxSizeInFlight", "1024m")`
    * **Context:** VM has 4GB RAM. `local[4]` is active.
    * **Identify the error:** 4 concurrent tasks fetching 1024MB of shuffle blocks each requires 4GB of JVM heap just for network buffers, starving the system of all other memory.
    * **Mastery Fix:** Reduce `spark.reducer.maxSizeInFlight` to a much smaller value (e.g., `48m` or `96m`) to limit the memory footprint of shuffle block fetching.
