# Assessment: Understand Cluster Components

This assessment tests elite, senior-level knowledge of Apache Spark cluster components, runtime architecture, memory management, and advanced troubleshooting.

## 1. True/False Questions

1. **Question:** In cluster mode, the Spark driver runs on one of the worker nodes and is managed by the cluster manager (e.g., YARN ApplicationMaster), whereas in client mode, the driver runs on the machine where the application was submitted.
   * **Answer:** True
   * **Mastery Explanation:** Client mode retains the driver process on the submitting machine, which can lead to a bottleneck or network disconnect if the client machine goes down. Cluster mode delegates the driver to a worker node inside the cluster manager's boundary, ensuring fault tolerance and offloading network traffic from the client.

2. **Question:** Tungsten memory management uses JVM objects exclusively for row representation to leverage the JVM's garbage collector.
   * **Answer:** False
   * **Mastery Explanation:** Tungsten explicitly avoids standard JVM objects to reduce memory overhead and garbage collection pauses. It stores rows in a compact, raw binary format (off-heap memory) and operates on them directly using pointer arithmetic (similar to C/C++), significantly improving cache locality and performance.

3. **Question:** If a Spark Executor fails, the Driver will automatically resubmit the entire job from the very beginning.
   * **Answer:** False
   * **Mastery Explanation:** Spark achieves fault tolerance through RDD lineage. If an executor fails, the driver only recomputes the lost partitions (tasks) by tracing back the lineage DAG. It does not restart the entire job unless the driver itself fails.

4. **Question:** In YARN cluster mode, setting `spark.executor.instances` is strictly required for dynamic allocation to function properly.
   * **Answer:** False
   * **Mastery Explanation:** Dynamic allocation (`spark.dynamicAllocation.enabled = true`) automatically scales the number of executors up and down based on workload. Setting a static `spark.executor.instances` conflicts with dynamic allocation, though you can configure `spark.dynamicAllocation.minExecutors` and `maxExecutors`.

5. **Question:** Broadcast variables are sent to every task individually by the driver during the execution phase.
   * **Answer:** False
   * **Mastery Explanation:** Broadcast variables are sent to each *executor* exactly once via a peer-to-peer BitTorrent-like protocol, not to each task. This drastically reduces the network bottleneck at the driver.

6. **Question:** An accumulator’s value can be reliably read inside a transformation (like `map` or `filter`) to make business logic decisions.
   * **Answer:** False
   * **Mastery Explanation:** Accumulators are write-only for tasks and should only be read by the driver. Reading them in transformations is unreliable because Spark's task retries and speculative execution can cause them to be incremented multiple times.

7. **Question:** The BlockManager is a component present only on the driver node to coordinate data blocks.
   * **Answer:** False
   * **Mastery Explanation:** The BlockManager exists on the driver AND every executor. The executors' BlockManagers store actual RDD blocks, cached data, and shuffle outputs, while the driver's BlockManager (BlockManagerMaster) keeps track of block locations across the cluster.

8. **Question:** A single Spark executor can run multiple tasks concurrently if configured with more than one core.
   * **Answer:** True
   * **Mastery Explanation:** An executor uses a thread pool to execute tasks. If `spark.executor.cores` is set to > 1, the executor can process multiple tasks simultaneously, sharing memory and broadcast variables among them.

9. **Question:** Setting `spark.memory.fraction` to 0.9 will allocate 90% of the entire JVM heap to Spark execution and storage.
   * **Answer:** False
   * **Mastery Explanation:** `spark.memory.fraction` specifies the fraction of the JVM heap *minus* the reserved system memory (default 300MB). So it is 90% of (Heap - 300MB), not 90% of the absolute total heap.

10. **Question:** In Spark 3.x with Adaptive Query Execution (AQE), the physical execution plan is finalized before any tasks begin execution.
    * **Answer:** False
    * **Mastery Explanation:** AQE dynamically optimizes the query plan *during* runtime. It collects runtime statistics from completed stages (like shuffle map stages) and uses them to re-optimize subsequent stages (e.g., converting sort-merge joins to broadcast joins).

## 2. Multiple Choice Questions

11. **Question:** Which cluster manager component is responsible for negotiating resources across the entire cluster?
    * A) Application Master
    * B) Node Manager
    * C) Resource Manager (YARN) / Master (Standalone)
    * D) Spark Context
    * **Answer:** C
    * **Mastery Explanation:** The Resource Manager acts as the global scheduler, allocating resources (containers) across the cluster to competing applications. The Application Master negotiates with the Resource Manager on behalf of a specific application.

12. **Question:** When an action is called, which Spark component is responsible for converting the logical DAG into physical execution stages?
    * A) BlockManager
    * B) DAGScheduler
    * C) TaskScheduler
    * D) Catalyst Optimizer
    * **Answer:** B
    * **Mastery Explanation:** The DAGScheduler converts the logical execution graph (DAG of RDDs) into physical stages by breaking the graph at shuffle boundaries. It then submits these stages as TaskSets to the TaskScheduler.

13. **Question:** In a shuffle operation, where do executors write their intermediate shuffle files?
    * A) HDFS
    * B) The Driver's memory
    * C) Local disk of the worker node
    * D) Amazon S3 / Cloud Storage
    * **Answer:** C
    * **Mastery Explanation:** Shuffle map tasks write intermediate shuffle data to local disk on the worker nodes. Reducer tasks then fetch this data over the network. This avoids overwhelming the driver and avoids the latency of writing to a distributed file system like HDFS.

14. **Question:** What happens to executor memory when a large cached DataFrame is no longer actively used, but `unpersist()` hasn't been called, and a memory-intensive task starts?
    * A) The task fails with OutOfMemoryError.
    * B) Execution memory aggressively evicts storage memory blocks (LRU cache).
    * C) The Executor crashes and restarts.
    * D) The cached DataFrame is serialized and sent to the Driver.
    * **Answer:** B
    * **Mastery Explanation:** Under Spark's unified memory model, execution memory can borrow space from storage memory and evict cached blocks if necessary. Storage memory can only borrow execution memory if it is completely idle.

15. **Question:** Which Spark process is responsible for keeping track of the physical location of shuffle map output files to serve reducer tasks?
    * A) Spark Driver (MapOutputTracker)
    * B) YARN Resource Manager
    * C) HDFS NameNode
    * D) Spark Executor (TaskScheduler)
    * **Answer:** A
    * **Mastery Explanation:** The Driver contains the MapOutputTrackerMaster. Executors report their shuffle block locations to the driver upon completing map tasks. Reducer tasks query the driver's MapOutputTracker to find where to fetch their required partitions.

16. **Question:** If you have 100 executors with 4 cores each, and a stage has 200 partitions, how many tasks will run concurrently?
    * A) 400
    * B) 200
    * C) 100
    * D) 50
    * **Answer:** B
    * **Mastery Explanation:** The cluster has a capacity of 400 concurrent slots (100 executors * 4 cores). However, the stage only has 200 partitions (tasks). Therefore, only 200 tasks will run concurrently; the remaining 200 slots will be idle.

17. **Question:** Which Catalyst Optimizer phase translates a Logical Plan into a Physical Plan?
    * A) Analysis
    * B) Logical Optimization
    * C) Physical Planning
    * D) Code Generation
    * **Answer:** C
    * **Mastery Explanation:** The Physical Planning phase takes the optimized logical plan and generates one or more physical plans using available execution strategies (e.g., choosing between SortMergeJoin and BroadcastHashJoin).

18. **Question:** What is the primary purpose of Tungsten's Whole-Stage Code Generation?
    * A) To compile Python UDFs into Scala code.
    * B) To collapse a query plan into a single optimized Java function, eliminating virtual function calls.
    * C) To generate HTML reports for the Spark UI.
    * D) To serialize data for network transport.
    * **Answer:** B
    * **Mastery Explanation:** Whole-Stage CodeGen compiles an entire query pipeline (stage) into a single Java function at runtime. This avoids the overhead of Volcano iterator models (virtual function dispatches per row), vastly improving CPU efficiency.

19. **Question:** When running Spark on YARN, if an executor is lost due to an OOM error, what component detects this failure and requests a replacement?
    * A) Spark Driver (ApplicationMaster)
    * B) The NodeManager on the failed node
    * C) The failed Executor itself
    * D) TaskScheduler
    * **Answer:** A
    * **Mastery Explanation:** The ApplicationMaster (which hosts the Driver in cluster mode) monitors the health of its containers (executors). If one fails, the ApplicationMaster negotiates with the YARN Resource Manager for a replacement container.

20. **Question:** Which of the following is NOT a phase in a Spark Stage execution?
    * A) Fetching shuffle data
    * B) Computing RDD transformations
    * C) Writing shuffle data
    * D) Negotiating YARN containers
    * **Answer:** D
    * **Mastery Explanation:** Negotiating containers is an application/job-level resource management task handled by the cluster manager and driver before tasks are scheduled. Stage execution focuses entirely on data processing (fetching, computing, writing).

21. **Question:** How does Spark handle Straggler tasks (tasks that run significantly slower than others)?
    * A) It kills the task immediately and fails the job.
    * B) It ignores them and waits indefinitely.
    * C) It uses Speculative Execution to launch duplicate tasks on other nodes.
    * D) It moves the data to a faster node.
    * **Answer:** C
    * **Mastery Explanation:** Speculative execution (`spark.speculation=true`) detects slow-running tasks and launches duplicate copies on other nodes. Whichever finishes first is kept, and the other is killed.

22. **Question:** What is the role of the External Shuffle Service?
    * A) It allows executors to be dynamically scaled down without losing their shuffle files.
    * B) It moves shuffle data to external storage like S3.
    * C) It compresses shuffle data before network transfer.
    * D) It allows Spark to shuffle data between different Spark applications.
    * **Answer:** A
    * **Mastery Explanation:** Without the external shuffle service, if an executor is scaled down, its local shuffle files are lost, breaking reducer tasks. The shuffle service runs independently on the worker node and serves shuffle blocks even if the executor dies.

23. **Question:** Which memory region in Spark is primarily used for storing intermediate data during Sorts, Joins, and Aggregations?
    * A) Storage Memory
    * B) Execution Memory
    * C) User Memory
    * D) Reserved Memory
    * **Answer:** B
    * **Mastery Explanation:** Execution memory is dedicated to computation. Storage memory is for caching (`persist()`), User memory is for custom data structures, and Reserved memory is for Spark's internal engine overhead.

24. **Question:** In a Broadcast Hash Join, where is the hash table built?
    * A) Exclusively on the Driver
    * B) Exclusively on the Master node
    * C) On every Executor
    * D) On the Driver, then shipped to the Executors
    * **Answer:** D
    * **Mastery Explanation:** The small table is collected to the Driver, which builds the hash table (or raw relation), broadcasts it to all Executors, and then the Executors use it to probe the large table locally.

25. **Question:** What does a `java.lang.OutOfMemoryError: GC overhead limit exceeded` on an Executor typically indicate?
    * A) The Executor ran out of disk space.
    * B) The Driver is sending too many tasks.
    * C) The JVM is spending too much time garbage collecting and very little time executing, usually due to massive object creation (like complex UDFs).
    * D) The network bandwidth is saturated.
    * **Answer:** C
    * **Mastery Explanation:** This specific OOM error means the JVM is spending >98% of its time doing GC and recovering <2% of heap. It happens when you create millions of small Java objects per partition, bypassing Tungsten's off-heap efficiency.

## 3. "Small Twist" Questions

26. **Scenario:** You run `df.groupBy("id").count()`. It takes 10 minutes.
    **Twist:** You change it to `df.repartition(1000).groupBy("id").count()`. It now takes 15 minutes. Why?
    * **Answer:** The explicit repartitioning introduces an unnecessary full shuffle before the aggregation.
    * **Mastery Explanation:** `groupBy` inherently performs a shuffle based on hash partitioning of the group key. By adding `repartition()`, you force a random round-robin shuffle, followed by the hash-based shuffle required by `groupBy`. Two shuffles instead of one.

27. **Scenario:** You have a cluster with 10 executors, 4 cores each. You run a job with 40 partitions. It finishes in 5 minutes.
    **Twist:** You change `spark.task.cpus` from 1 to 4. The job now takes 20 minutes. Why?
    * **Answer:** Concurrency dropped from 40 tasks to 10 tasks.
    * **Mastery Explanation:** `spark.task.cpus` tells Spark how many cores *each task* requires. With 4 cores per executor and `spark.task.cpus=4`, each executor can only run 1 task at a time. The total cluster concurrency drops from 40 to 10, processing fewer partitions in parallel.

28. **Scenario:** You configure `spark.executor.memory=8g` and your application runs fine.
    **Twist:** You add a complex Python UDF, and suddenly the executor nodes crash with OS-level OOM kills (YARN container killed). Why?
    * **Answer:** Python worker processes consume memory outside the JVM heap.
    * **Mastery Explanation:** In PySpark, UDFs execute in separate Python worker processes, not in the JVM. `spark.executor.memory` only sizes the JVM. The OS killed the container because the combined memory of the JVM + Python processes exceeded the YARN container limit (`spark.executor.memoryOverhead`).

29. **Scenario:** You cache a large DataFrame using `df.cache()`. The Spark UI Storage tab shows it is 100% cached.
    **Twist:** You change to `df.persist(StorageLevel.MEMORY_AND_DISK_SER)`. The memory footprint drops significantly, but CPU usage spikes on subsequent reads. Why?
    * **Answer:** Serialization overhead.
    * **Mastery Explanation:** `MEMORY_AND_DISK_SER` stores data as serialized byte arrays instead of Java objects (or raw Tungsten rows). This saves massive amounts of memory but requires the CPU to deserialize the data every time it is read.

30. **Scenario:** You are broadcasting a 50MB lookup table. The job runs in 2 minutes.
    **Twist:** The lookup table grows to 5GB. The job fails with an OOM error on the Driver. Why?
    * **Answer:** The Driver must collect the entire table into its own heap before broadcasting.
    * **Mastery Explanation:** When you call `broadcast(df)`, the driver executes a `collect()` to bring all data to the driver node to create the broadcast variable. A 5GB collection easily blows out standard driver memory limits.

31. **Scenario:** You configure `spark.speculation=true` to handle stragglers. It works perfectly for your data processing pipeline.
    **Twist:** You add a step that writes the final output to a non-transactional database (e.g., plain JDBC without upsert logic). Data corruption/duplication occurs. Why?
    * **Answer:** Speculative tasks run concurrently and write duplicate data.
    * **Mastery Explanation:** Speculation launches multiple copies of the same task. If the sink is not idempotent or doesn't support transactional commits (like HDFS OutputCommitters do), both tasks will write to the database, causing duplicates.

32. **Scenario:** You have a long lineage DAG of 50 transformations.
    **Twist:** You add `.checkpoint()` in the middle. The DAG is truncated, but the job takes twice as long. Why?
    * **Answer:** Checkpointing forces an action and writes data to HDFS.
    * **Mastery Explanation:** Unlike `cache()`, checkpointing cuts the lineage and writes the RDD reliably to distributed storage. This requires an immediate evaluation of the DAG up to that point and involves heavy disk/network I/O.

33. **Scenario:** You have 100 small Parquet files. You read them and do a `map`. 100 tasks are created.
    **Twist:** You read 1 huge 10GB Parquet file. 100 tasks are still created. Why?
    * **Answer:** Spark splits large files based on HDFS/S3 block sizes (typically 128MB).
    * **Mastery Explanation:** Input partitions are determined by the underlying file system blocks, not just file count. A 10GB file split into 128MB chunks naturally results in ~80 partitions/tasks.

34. **Scenario:** You execute a `join` between a 1TB table and a 1GB table. The optimizer chooses SortMergeJoin.
    **Twist:** You increase `spark.sql.autoBroadcastJoinThreshold` to 2GB. The job now fails with an executor OOM during the join. Why?
    * **Answer:** The 1GB table expands when deserialized in memory on the executors.
    * **Mastery Explanation:** The threshold applies to the compressed, serialized size of the table. When the 1GB table is broadcast and decompressed/deserialized into memory on every executor, it may expand to 5-10GB, blowing out the executor's execution memory.

35. **Scenario:** A shuffle operation writes 500GB of shuffle data to local disk.
    **Twist:** You enable Adaptive Query Execution (`spark.sql.adaptive.enabled=true`). The shuffle data size drops, and the job runs faster. Why?
    * **Answer:** AQE dynamically coalesces post-shuffle partitions.
    * **Mastery Explanation:** AQE looks at the shuffle map statistics. If it sees many small partitions, it coalesces them into larger partitions before the reducer phase, minimizing overhead and network connections.

36. **Scenario:** You monitor your cluster and see high network traffic during a groupBy.
    **Twist:** You change the aggregation from `groupByKey()` to `reduceByKey()`. Network traffic plummets. Why?
    * **Answer:** Map-side combine.
    * **Mastery Explanation:** `groupByKey` sends all raw key-value pairs across the network. `reduceByKey` aggregates values locally on the map side *before* shuffling, drastically reducing the amount of data sent over the network.

37. **Scenario:** You launch a PySpark job on YARN. It takes 10 seconds to process a small dataframe.
    **Twist:** You convert a native PySpark `withColumn` to a pandas UDF (`@pandas_udf`). It now takes 2 seconds. Why?
    * **Answer:** Apache Arrow vectorization.
    * **Mastery Explanation:** Standard Python UDFs serialize data row-by-row between the JVM and Python via Py4J (very slow). Pandas UDFs use Apache Arrow to transfer data in columnar batches, yielding massive performance gains.

38. **Scenario:** An executor crashes due to hardware failure. The driver recovers and resubmits the lost tasks.
    **Twist:** The driver also crashes. The YARN ApplicationMaster restarts the driver, but the entire job starts from the very beginning. Why?
    * **Answer:** RDD Lineage is kept in the Driver's memory.
    * **Mastery Explanation:** The Driver maintains the DAG and lineage. If the Driver dies, the cluster manager can restart it (if configured), but the Driver loses all memory of previous stages. The job must run from scratch unless checkpointing was used.

39. **Scenario:** You use `df.repartition(10, "customer_id")` to write out 10 files.
    **Twist:** You notice that one file is 5GB, while the other 9 are ~10MB each. Why?
    * **Answer:** Data Skew on the partition key.
    * **Mastery Explanation:** Hash partitioning groups identical keys into the same partition. If one `customer_id` dominates the dataset (e.g., a default or null value), all its records go to a single partition/file, causing extreme skew.

40. **Scenario:** You submit a job in `client` mode. The driver logs show it's stuck waiting for executors.
    **Twist:** You look at the YARN UI and see the executors are running, but doing nothing. Why?
    * **Answer:** Network firewall or routing issue from Executors back to the Client Driver.
    * **Mastery Explanation:** In client mode, executors must communicate back to the client machine's IP address. If the client is on a laptop behind a NAT or firewall, executors cannot reach it, and the job hangs.

## 4. Coding & Debugging Questions

41. **Debugging: The Silent Memory Leak**
    **Code:**
    ```python
    for i in range(100):
        df = df.withColumn(f"col_{i}", expr("some_complex_logic"))
    df.write.parquet(...)
    ```
    **Problem:** The Driver crashes with OOM before any executors start working. Why?
    * **Answer:** Lineage graph expansion blows out the Driver's heap.
    * **Mastery Explanation:** DataFrames are evaluated lazily. The loop creates a massive logical plan DAG on the Driver. When `write` triggers the action, the Catalyst Optimizer attempts to analyze and optimize this gigantic plan, causing an OOM on the Driver due to object explosion.

42. **Debugging: The Optimizer Blocker**
    **Code:**
    ```python
    df = spark.read.parquet("huge_data")
    df_filtered = df.rdd.filter(lambda row: row['age'] > 18).toDF()
    df_filtered.write.parquet("output")
    ```
    **Problem:** The job is extremely slow and reads the entire `huge_data` file.
    * **Answer:** Dropping to RDD API breaks Catalyst optimizations (Predicate Pushdown).
    * **Mastery Explanation:** When you convert to RDD, Catalyst cannot inspect the lambda function. It cannot push the `age > 18` filter down to the Parquet reader. Spark must read the entire massive file into memory, convert to Java objects, and then apply the Python filter.

43. **Debugging: Unintended Cartesian Product**
    **Code:**
    ```python
    df1 = spark.table("customers")
    df2 = spark.table("transactions")
    result = df1.join(df2, df1.id == df2.cust_id, "left")
    ```
    **Problem:** (Twist on Code) Developer typed: `df1.join(df2, df1.id == df1.id, "left")`. The cluster grinds to a halt.
    * **Answer:** The join condition evaluates to `True` for all rows, resulting in a BroadcastNestedLoopJoin (Cartesian product).
    * **Mastery Explanation:** A typo in the join condition (`df1.id == df1.id`) creates a cross join. Spark will attempt to multiply every row in df1 with every row in df2, generating trillions of records and exhausting cluster resources.

44. **Debugging: The Shared Variable Anti-Pattern**
    **Code:**
    ```python
    counter = 0
    def increment(row):
        global counter
        counter += 1
    df.rdd.foreach(increment)
    print(counter)
    ```
    **Problem:** `counter` always prints `0` on the Driver.
    * **Answer:** Standard variables are serialized and copied to executors. Updates are local to the executor.
    * **Mastery Explanation:** The `counter` variable is sent to each executor as a copy. They increment their local copies. The Driver's original `counter` remains untouched. To fix this, an Accumulator must be used.

45. **Debugging: Executor Lost during Shuffle**
    **Log snippet:**
    `FetchFailedException: Failed to connect to <executor-ip>:7337`
    **Problem:** Reducer tasks are failing constantly.
    * **Answer:** The executor serving shuffle blocks died (likely OOM), or the External Shuffle Service is misconfigured.
    * **Mastery Explanation:** When a Reducer tries to fetch map output data and fails, it throws a `FetchFailedException`. The Driver marks the previous Map stage as failed and attempts to rerun it. This is a classic symptom of an executor running out of memory during a heavy sort/shuffle.

46. **Debugging: Time Skew**
    **Code:**
    ```python
    df = df.withColumn("current_time", current_timestamp())
    ```
    **Problem:** When inspecting the output files, the `current_time` is identical across millions of rows, even though writing took hours.
    * **Answer:** `current_timestamp()` is evaluated once at the start of the query on the Driver.
    * **Mastery Explanation:** Catalyst optimizes deterministic-looking functions. `current_timestamp` is evaluated at the start of the query execution and passed as a constant literal to all tasks to ensure consistency across the query.

47. **Debugging: The UDF Bottleneck**
    **Code:**
    ```python
    import json
    def parse_json(json_str):
        return json.loads(json_str)["field"]
    
    spark.udf.register("parse_json", parse_json)
    df.selectExpr("parse_json(raw_data)").show()
    ```
    **Problem:** CPU utilization is extremely high, but throughput is tiny.
    * **Answer:** Python UDF serialization overhead and Python interpreter spin-up.
    * **Mastery Explanation:** For every row, Spark serializes the data, sends it via a local socket to a Python process, parses the JSON in Python, and sends it back to the JVM. Native Spark SQL function `get_json_object` should be used instead, running natively in Tungsten.

48. **Debugging: Skewed Joins**
    **Code:**
    ```python
    df_sales.join(df_users, "user_id").write...
    ```
    **Problem:** 199 tasks finish in 1 minute. 1 task runs for 4 hours.
    * **Answer:** Data Skew on `user_id`. (e.g., Guest user ID `0` has millions of transactions).
    * **Mastery Explanation:** Hash partitioning sends all records for a specific key to a single executor. That executor is overwhelmed. Fix: Use Salting (appending random numbers to the skewed key) or enable AQE Skew Join Optimization.

49. **Debugging: Driver OOM on Action**
    **Code:**
    ```python
    results = df.collect()
    for row in results:
        print(row)
    ```
    **Problem:** Application crashes with Driver OOM.
    * **Answer:** `collect()` pulls the entire distributed dataset into the Driver's memory space.
    * **Mastery Explanation:** The driver has limited heap space (e.g., 1-2GB). If the DataFrame is large, `collect()` will easily exceed this limit. Alternative: use `take(100)`, or save to distributed storage using `write`.

50. **Debugging: Accidental Cross Join via Catalyst**
    **Code:**
    ```python
    df_filtered1 = df.filter(df.id > 100)
    df_filtered2 = df.filter(df.id < 50)
    result = df_filtered1.crossJoin(df_filtered2)
    ```
    **Problem:** Job fails instantly before execution with an `AnalysisException`.
    * **Answer:** Cross joins are disabled by default to prevent catastrophic cluster meltdowns.
    * **Mastery Explanation:** Catalyst protects against accidental cartesian products. You must explicitly set `spark.sql.crossJoin.enabled = true` in the SparkSession configuration to allow this execution, acting as a safeguard for developers.
