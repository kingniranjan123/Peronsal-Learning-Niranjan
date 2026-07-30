# <Master Class: YARN Architecture>

Apache Hadoop YARN (Yet Another Resource Negotiator) represents the foundational cluster resource management technology upon which enterprise-grade Apache Spark applications are typically deployed. Unlike Spark's standalone cluster manager, which is purpose-built for Spark workloads, YARN is a multi-tenant, general-purpose resource allocator designed to multiplex the compute capabilities of a vast cluster among various distributed processing frameworks, including MapReduce, Tez, and Spark.

At its core, YARN decouples the responsibilities of resource management and job scheduling/monitoring into distinct daemon processes. The global ResourceManager (RM) operates as the master, orchestrating resource allocation across the entire cluster. It consists of a Pluggable Scheduler (like Capacity or Fair Scheduler) and an ApplicationsManager. Each node in the cluster hosts a NodeManager (NM), which is responsible for launching containers, monitoring their resource usage (CPU, memory, disk, network), and reporting this telemetry back to the ResourceManager.

When an Apache Spark application is submitted to a YARN cluster, a per-application ApplicationMaster (AM) is instantiated. In Spark on YARN, the ApplicationMaster acts as the crucial liaison between the SparkContext and the YARN ResourceManager. The AM negotiates resource containers from the RM and, upon allocation, contacts the respective NodeManagers to launch Spark Executors within those containers. This architecture effectively distributes the overhead of application lifecycle management away from the central ResourceManager, mitigating single points of failure and bottlenecks, while providing robust fault tolerance. If an executor container fails, the ApplicationMaster detects the localized failure and simply requests a replacement container from the ResourceManager, allowing Spark's resilient RDD/DataFrame lineage to recompute any lost partitions seamlessly.

## 💻 Code Example 1: Programmatic YARN Configuration via SparkSession

```python
from pyspark.sql import SparkSession

# Building a SparkSession tuned for YARN Cluster mode with aggressive resource requests
spark = SparkSession.builder \
    .appName("YARN_Mastery_App") \
    .master("yarn") \
    .config("spark.submit.deployMode", "cluster") \
    .config("spark.yarn.queue", "high_priority_etl") \
    .config("spark.yarn.maxAppAttempts", "2") \
    .config("spark.yarn.am.memory", "2g") \
    .config("spark.yarn.am.cores", "2") \
    .config("spark.yarn.submit.waitAppCompletion", "false") \
    .getOrCreate()

# Example dataframe operation
df = spark.range(1, 1000000).repartition(200)
df.write.format("parquet").mode("overwrite").save("hdfs:///tmp/yarn_mastery_output")
spark.stop()
```

In this example, we configure the SparkSession specifically for a YARN cluster deployment. The `spark.submit.deployMode` is set to `cluster`, meaning the Spark Driver runs inside the YARN ApplicationMaster container rather than on the edge node, ensuring the application continues running even if the client disconnects. We specify a dedicated YARN queue (`high_priority_etl`) to leverage YARN's Capacity Scheduler routing. Crucially, we configure the ApplicationMaster's memory and cores independently of the executors. The `maxAppAttempts` parameter dictates how many times YARN will retry the entire ApplicationMaster before marking the application as failed, which is vital for long-running streaming jobs or critical ETL pipelines where transient network partitions or node failures might otherwise cause job termination.

## Resource Allocation and Dynamic Executor Allocation

In a multi-tenant YARN cluster, static allocation of Spark executors often leads to severe resource underutilization. If a Spark job reserves 100 executors but encounters a long, sequential, single-partition operation, 99 executors sit idle, hoarding valuable cluster memory and vCores from other tenants. To combat this, Spark introduced Dynamic Resource Allocation, which works in tandem with YARN's External Shuffle Service (ESS).

When Dynamic Allocation is enabled, Spark scales the number of executors up and down based on the pending task queue and idle timeouts. However, removing an executor traditionally means losing all shuffle files stored on that executor's local disk, which would necessitate expensive recomputation. YARN solves this via the External Shuffle Service—a long-running NodeManager auxiliary service. The ESS takes ownership of the shuffle files written by Spark executors. Consequently, when an executor is dynamically de-allocated due to idleness, its shuffle files remain securely accessible to downstream stages via the ESS.

Furthermore, YARN's Capacity Scheduler allows administrators to define hierarchical queues with minimum capacity guarantees and maximum resource limits. Spark on YARN must gracefully handle preemption, where YARN reclaims containers from an over-limit queue to satisfy the minimum guarantees of another. Spark handles this by killing executors, which emphasizes the necessity of the ESS to prevent cascading shuffle data loss during aggressive container preemption by the ResourceManager.

## 💻 Code Example 2: Configuring Dynamic Allocation and External Shuffle Service

```python
from pyspark.sql import SparkSession

# Configuring Spark for Dynamic Allocation on YARN
spark = SparkSession.builder \
    .appName("Dynamic_Allocation_YARN") \
    .master("yarn") \
    .config("spark.dynamicAllocation.enabled", "true") \
    .config("spark.shuffle.service.enabled", "true") \
    .config("spark.dynamicAllocation.initialExecutors", "10") \
    .config("spark.dynamicAllocation.minExecutors", "5") \
    .config("spark.dynamicAllocation.maxExecutors", "100") \
    .config("spark.dynamicAllocation.executorIdleTimeout", "60s") \
    .config("spark.dynamicAllocation.cachedExecutorIdleTimeout", "600s") \
    .config("spark.yarn.shuffle.stopOnFailure", "false") \
    .getOrCreate()

# A skewed workload that benefits from dynamic scaling
df1 = spark.range(1, 10000000).withColumn("key", (spark._sc._jvm.org.apache.spark.sql.functions.rand() * 10).cast("int"))
df2 = spark.range(1, 100000).withColumn("key", (spark._sc._jvm.org.apache.spark.sql.functions.rand() * 10).cast("int"))
df1.join(df2, "key").count()
spark.stop()
```

Here, we explicitly enable `spark.dynamicAllocation.enabled` alongside `spark.shuffle.service.enabled`. The configuration defines a baseline of 5 executors and scales up to a maximum of 100 during bursty workloads. Notice the distinction between `executorIdleTimeout` and `cachedExecutorIdleTimeout`. Regular executors are released after 60 seconds of inactivity to free up YARN resources. However, if an executor is actively caching RDD or DataFrame partitions in memory, its timeout is extended to 600 seconds, preventing the premature eviction of valuable cached data which would otherwise force expensive re-evaluations.

## Managing Data Locality and Node Labels

Data locality is a cornerstone of big data performance. When Spark on YARN requests containers from the ResourceManager, it passes along preferred node locations based on the HDFS block placement of the underlying input splits. The YARN RM attempts to honor these preferences, allocating containers on the exact nodes where the data resides (NODE_LOCAL), on the same rack (RACK_LOCAL), or anywhere else (ANY). Spark employs a delay scheduling algorithm, configurable via `spark.locality.wait`, where it pauses briefly to secure a NODE_LOCAL container before degrading to RACK_LOCAL.

Beyond strict HDFS locality, modern YARN clusters utilize Node Labels. Node Labels allow administrators to partition the cluster into logical sub-clusters based on hardware profiles (e.g., GPU-enabled nodes, high-memory nodes). Spark applications can explicitly request their ApplicationMaster and Executors to be scheduled exclusively on nodes bearing specific labels, ensuring memory-intensive joins or machine learning tasks land on appropriately provisioned hardware.

## 💻 Code Example 3: Leveraging YARN Node Labels and Locality Waits

```python
from pyspark.sql import SparkSession

# Submitting a job targeting specific hardware using YARN Node Labels
spark = SparkSession.builder \
    .appName("Hardware_Aware_YARN_Job") \
    .master("yarn") \
    .config("spark.yarn.executor.nodeLabelExpression", "high_memory") \
    .config("spark.yarn.am.nodeLabelExpression", "core_nodes") \
    .config("spark.locality.wait", "3s") \
    .config("spark.locality.wait.node", "5s") \
    .config("spark.locality.wait.rack", "1s") \
    .config("spark.yarn.containerLauncherMaxThreads", "25") \
    .getOrCreate()

# Intensive aggregation requiring high-memory nodes
large_df = spark.read.parquet("hdfs:///data/massive_clickstream_dataset")
large_df.groupBy("user_id", "session_id").agg({"duration": "sum"}).show()
spark.stop()
```

This snippet demonstrates advanced YARN scheduling capabilities. We force YARN to launch Spark Executors only on nodes labeled `high_memory`, isolating our heavy aggregation workload to beefy machines. Conversely, the ApplicationMaster is restricted to `core_nodes`. The locality wait configurations instruct Spark's internal scheduler to wait up to 5 seconds for a `NODE_LOCAL` allocation before falling back, optimizing for zero-network-transfer reads from HDFS. Increasing `containerLauncherMaxThreads` accelerates the simultaneous launch of many executors across the NodeManagers.

## YARN Memory Overhead and JVM Tuning

A pervasive source of failure in Spark on YARN is the dreaded `YARN Container killed by YARN for exceeding memory limits`. When you request a 4GB executor (`spark.executor.memory`), YARN actually allocates a larger container to account for the JVM overhead (off-heap memory, thread stacks, NIO buffers). This padding is controlled by `spark.yarn.executor.memoryOverhead`, which defaults to 10% of the executor memory or 384MB, whichever is larger. If native libraries (like RocksDB or Python UDFs) consume memory beyond this total container size, YARN's NodeManager will ruthlessly terminate the container via a SIGKILL, leading to application instability. Tuning this overhead, alongside rigorous JVM garbage collection parameters, is mandatory for large-scale production stability on YARN.

## 💻 Code Example 4: Advanced Memory Tuning and GC Configuration

```python
from pyspark.sql import SparkSession

# Configuring robust memory overhead and G1GC for YARN containers
spark = SparkSession.builder \
    .appName("YARN_Memory_Mastery") \
    .master("yarn") \
    .config("spark.executor.memory", "16g") \
    .config("spark.yarn.executor.memoryOverhead", "4g") \
    .config("spark.executor.cores", "4") \
    .config("spark.executor.extraJavaOptions", 
            "-XX:+UseG1GC -XX:InitiatingHeapOccupancyPercent=35 -XX:MaxGCPauseMillis=200 -XX:+PrintGCDetails -XX:+PrintGCTimeStamps") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.5") \
    .config("spark.python.worker.memory", "1g") \
    .getOrCreate()

# Triggering complex python UDFs which utilize off-heap memory
import pyspark.sql.functions as F
from pyspark.sql.types import StringType

@F.udf(returnType=StringType())
def intensive_string_manipulation(data):
    return data.upper() * 100

df = spark.range(1, 10000000).withColumn("raw_text", F.lit("sample_text"))
df.withColumn("processed", intensive_string_manipulation("raw_text")).count()
spark.stop()
```

In this final example, we allocate 16GB of on-heap memory but explicitly request an enormous 4GB of `memoryOverhead`. This is critical because Python UDFs run in separate worker processes outside the executor JVM but within the same YARN container limit. We also explicitly allocate `spark.python.worker.memory`. Furthermore, we inject JVM flags to utilize the G1 Garbage Collector (`-XX:+UseG1GC`), tuning the `InitiatingHeapOccupancyPercent` to 35% to trigger concurrent marking cycles earlier, avoiding catastrophic full GC pauses that could cause the executor to miss YARN NodeManager heartbeats and be falsely declared dead.
