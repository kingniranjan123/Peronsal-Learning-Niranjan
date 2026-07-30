# Master Class: Spark Submit - Elite Technical Assessment

## 1. True/False Questions

**1. In YARN Cluster mode, the spark-submit bash script's JVM process remains alive for the entire duration of the application as the active Spark Driver.**
**Answer:** False
**Mastery Explanation:** In Cluster mode, the local `spark-submit` process acts merely as a short-lived REST/RPC client that stages files to HDFS and requests an ApplicationMaster. The ApplicationMaster JVM running on a worker node becomes the actual Spark Driver.

**2. Setting `spark.executor.memoryOverhead` increases the internal JVM heap space (`-Xmx`) allocated for object instantiation.**
**Answer:** False
**Mastery Explanation:** The memory overhead dictates off-heap memory at the container level (used for Tungsten, NIO buffers, and Python processes), while `--executor-memory` dictates the JVM heap (`-Xmx`). 

**3. In Client Mode, the Catalyst Optimizer executes locally on the edge node where `spark-submit` was invoked.**
**Answer:** True
**Mastery Explanation:** In Client mode, the Spark Driver is instantiated directly on the edge node. Because Catalyst operates entirely within the Driver JVM, query parsing, logical optimization, and physical planning happen locally before tasks are dispatched via RPC.

**4. `spark.driver.userClassPathFirst=true` is the definitively safest, foolproof way to resolve all library conflicts with Spark's internal Catalyst dependencies.**
**Answer:** False
**Mastery Explanation:** While it resolves user-level `NoSuchMethodError`, it is dangerous. If user JARs override critical internal Spark dependencies, it can crash Tungsten. The safest method is shading dependencies via the Maven Shade Plugin.

**5. When submitting to Kubernetes, if you do not explicitly mount a PVC for `spark-local-dir`, Spark will crash immediately upon initialization.**
**Answer:** False
**Mastery Explanation:** Spark won't crash immediately; instead, it will silently spill shuffle data to the K8s Pod's ephemeral root overlay filesystem. This drastically saturates network and disk I/O, slowing execution to a crawl without immediately failing.

**6. The `spark.dynamicAllocation.enabled` flag is functionally useless in Local Mode launch.**
**Answer:** True
**Mastery Explanation:** Local mode runs the Driver and Executor within a single JVM process, scaling only up to local CPU cores. Dynamic allocation requires negotiating with a cluster manager (YARN/K8s) for independent container scaling.

**7. `Exit code: 137` on YARN indicates that the DAGScheduler failed to plan the job due to a syntax error.**
**Answer:** False
**Mastery Explanation:** Exit 137 is a Unix signal (SIGKILL) triggered by the YARN NodeManager when a container exceeds its total physical memory allocation limit (Heap + Overhead).

**8. The `SparkLauncher` Java API relies on JNI (Java Native Interface) to interact with YARN without spawning a separate process.**
**Answer:** False
**Mastery Explanation:** `SparkLauncher` does not use JNI; it physically forks a child JVM process to execute the `spark-submit` command internally, communicating state back to the parent process.

**9. YARN Client launch mode has a performance complexity of O(N) because the Driver starts immediately and directly negotiates N Executors.**
**Answer:** True
**Mastery Explanation:** The Driver launches locally instantly (O(1) startup for driver), but must negotiate N executors over RPC. It avoids the O(M) HDFS upload cost associated with Cluster mode's AM staging.

**10. When executing PySpark in YARN cluster mode, increasing Metaspace is completely unnecessary since Python does not use the JVM.**
**Answer:** False
**Mastery Explanation:** PySpark relies on Py4J to bridge Python and the JVM. If you modify classloaders (e.g., `userClassPathFirst`), duplicate classes will fill the JVM Metaspace rapidly, causing an OOM before tasks even start.

## 2. Multiple Choice Questions

**11. Why does an edge node network partition permanently kill a Spark application in YARN Client mode?**
A) The ApplicationMaster stops sending heartbeats to HDFS.
B) The Catalyst optimizer loses its connection to the Hive Metastore.
C) The Driver JVM, hosted on the edge node, acts as the DAGScheduler and RPC endpoint for all Executors.
D) YARN dynamically reallocates the edge node's CPU cores.
**Answer:** C
**Mastery Explanation:** In Client mode, the edge node hosts the Driver. If it disconnects, the Executors lose their TaskScheduler and RPC coordinator, permanently orphaning the application.

**12. By default, what is the size of `spark.executor.memoryOverhead`?**
A) 10% of executor memory, minimum 384MB
B) 25% of executor memory, minimum 512MB
C) 1GB fixed
D) Equal to Tungsten off-heap size
**Answer:** A
**Mastery Explanation:** The default overhead is 10% of heap or 384MB (whichever is larger). This is often insufficient for heavy PySpark or Tungsten workloads.

**13. Which scenario most necessitates explicitly configuring K8s Persistent Volume Claims (PVC) for `spark-local-dir`?**
A) Reading a 10GB CSV file using `.count()`
B) A highly selective `.filter()` followed by `.write.parquet()`
C) A massive `.groupBy().agg()` triggering a wide shuffle
D) Executing a UDF on a broadcast variable
**Answer:** C
**Mastery Explanation:** Wide transformations write shuffle files locally to `spark-local-dir`. On K8s, without a fast NVMe PVC, this spills to the slow pod overlay filesystem.

**14. When using `spark.executor.userClassPathFirst=true`, what secondary configuration becomes critical to prevent early JVM crashes?**
A) `spark.executor.memoryOverhead`
B) `-XX:MaxMetaspaceSize` via `extraJavaOptions`
C) `spark.dynamicAllocation.minExecutors`
D) `spark.rpc.message.maxSize`
**Answer:** B
**Mastery Explanation:** Forcing child-first classloading means many classes are loaded redundantly across classloaders, rapidly exhausting the default JVM Metaspace limit.

**15. What is the primary role of the `SparkSubmit` class during the bootstrap phase?**
A) JIT compilation of Catalyst expressions
B) Negotiating with the YARN NodeManager directly
C) Parsing arguments, resolving Ivy dependencies, and constructing the execution classpath
D) Managing Tungsten's off-heap memory pointers
**Answer:** C
**Mastery Explanation:** `SparkSubmit` sets up the JVM environment, downloads `--packages` via Ivy, and delegates to the cluster-specific client. It does no physical execution.

**16. In YARN Cluster mode, which component is responsible for dynamically scaling Executor JVMs?**
A) The local `spark-submit` bash process
B) The ApplicationMaster running the Spark Driver
C) The YARN ResourceManager
D) The Edge Node TaskScheduler
**Answer:** B
**Mastery Explanation:** The ApplicationMaster (which is the Driver in Cluster mode) communicates with YARN to request and release Executor containers based on dynamic allocation rules.

**17. What causes a `NoSuchMethodError` during Spark job initialization?**
A) Missing Python libraries in a PySpark environment
B) Catalyst failing to generate Whole-Stage Code Generation Java code
C) The JVM's parent-first classloader loading an older internal Spark library instead of the user's updated library
D) YARN killing the container before the method is invoked
**Answer:** C
**Mastery Explanation:** Default classloader precedence loads Spark's bundled dependencies first. If a user provides a newer JAR, its updated methods won't be found because the older class was already loaded.

**18. Why is `spark.network.timeout` commonly increased in YARN Client mode interactive sessions?**
A) To allow users more time to type queries in Jupyter
B) To prevent the Driver from marking Executors as dead during heavy GC pauses or RPC congestion
C) To increase the time Spark takes to read from HDFS
D) To delay the YARN ResourceManager from reclaiming containers
**Answer:** B
**Mastery Explanation:** Client mode involves high network latency between the edge node and cluster. Network blips or GC pauses can trigger premature timeouts, severing Executor connections.

**19. Which configuration prevents the edge node Driver from crashing with an OutOfMemoryError when running `.collect()`?**
A) `spark.executor.memory`
B) `spark.driver.maxResultSize`
C) `spark.rdd.compress`
D) `spark.sql.shuffle.partitions`
**Answer:** B
**Mastery Explanation:** `maxResultSize` acts as a circuit breaker, explicitly failing the job if the serialized results returning to the Driver exceed the limit, protecting the Driver's heap.

**20. What is the fundamental disadvantage of using the Maven Shade Plugin compared to `userClassPathFirst`?**
A) It increases JVM Metaspace usage
B) It causes Tungsten to fail
C) It requires modifying the build process and repackaging JARs, rather than a simple runtime flag
D) It cannot handle Scala libraries
**Answer:** C
**Mastery Explanation:** Shading requires build-time configuration (pom.xml/build.sbt) to physically relocate namespaces, whereas `userClassPathFirst` is a quick, albeit risky, runtime configuration.

**21. In a PySpark application, where is the Python process executed relative to the Executor JVM?**
A) Inside the JVM heap using Jython
B) In a separate worker node
C) As a separate OS process inside the same YARN/K8s container, communicating via Py4J local sockets
D) In the ApplicationMaster container exclusively
**Answer:** C
**Mastery Explanation:** PySpark spawns separate Python worker processes inside the executor container. They consume off-heap memory, heavily contributing to `memoryOverhead` limits.

**22. What does an O(N+M) complexity imply in YARN Cluster Launch?**
A) N nodes, M CPU cores
B) N seconds of GC, M seconds of Catalyst optimization
C) N Executor RPC negotiations, M size/time complexity for uploading JARs to HDFS
D) N memory limits, M disk limits
**Answer:** C
**Mastery Explanation:** Cluster mode pays an O(M) penalty to stage files (uploading application dependencies to distributed cache) plus the O(N) cost of allocating Executors.

**23. How does `SparkLauncher` differ from running `spark-submit` via `Runtime.getRuntime().exec()`?**
A) It uses REST APIs instead of the JVM
B) It natively attaches state listeners and securely manages process streams without deadlocking
C) It runs entirely in-memory without creating a new process
D) It bypasses YARN completely
**Answer:** B
**Mastery Explanation:** `SparkLauncher` manages the child process's I/O streams automatically and provides native callback handlers for state changes, preventing common `exec()` pipe deadlocks.

**24. When using `--packages`, which component resolves the transitive dependencies?**
A) Catalyst Optimizer
B) YARN ResourceManager
C) Ivy Dependency Resolver inside SparkSubmit
D) Tungsten Engine
**Answer:** C
**Mastery Explanation:** Before the Driver starts, `SparkSubmit` uses Apache Ivy to resolve, download, and construct the classpath from the provided Maven coordinates.

**25. In Kubernetes deploy mode, what entity does the `spark-submit` client communicate with first?**
A) YARN ApplicationMaster
B) Kubernetes API Server
C) Kubelet on the worker node
D) HDFS NameNode
**Answer:** B
**Mastery Explanation:** The client directly requests the K8s API server to spin up the Driver Pod. The Driver Pod then subsequently contacts the API server to request Executor Pods.

## 3. Small Twist Questions

**26. Scenario:** You have `--executor-memory 8G`. Your application processes massive Parquet files with complex Tungsten aggregations. The container is killed with Exit 137.
**Twist:** You change to `--executor-memory 12G`. What happens?
**Answer:** The container is likely STILL killed.
**Mastery Explanation:** Increasing heap space does not increase the off-heap buffer space proportionally to satisfy Tungsten's native memory allocations. You must explicitly increase `spark.executor.memoryOverhead` or `spark.memory.offHeap.size`.

**27. Scenario:** You run `spark-submit --deploy-mode client`. Your SSH session drops, and the application terminates.
**Twist:** You run it inside `tmux` or `screen`. What happens when your SSH session drops?
**Answer:** The application continues running perfectly.
**Mastery Explanation:** `tmux` keeps the edge node's bash session (and thus the Driver JVM) alive even if the client's network connection to the edge node drops.

**28. Scenario:** You set `spark.driver.userClassPathFirst=true` and your PySpark job initializes properly.
**Twist:** You change the master to `k8s://...` and use a minimal base image. The job crashes on startup. Why?
**Answer:** Metaspace OOM.
**Mastery Explanation:** Modifying classloaders spikes Metaspace. If the K8s pod lacks explicit `-XX:MaxMetaspaceSize` configurations (which may have been implicitly set on your old YARN edge node), the JVM dies instantly.

**29. Scenario:** You use `SparkLauncher` and set `spark.yarn.am.memory` to `2G`.
**Twist:** You change deploy-mode from `cluster` to `client`. What does `spark.yarn.am.memory` control now?
**Answer:** It strictly controls the ApplicationMaster (which merely manages executor requests), NOT the Spark Driver.
**Mastery Explanation:** In client mode, the AM is just a lightweight proxy. The Driver memory is controlled by the local JVM heap where `SparkLauncher` is executed.

**30. Scenario:** Your `spark-submit` script includes `--packages org.apache.hudi:hudi-spark...` and runs in YARN Cluster mode.
**Twist:** The cluster lacks outbound internet access. What happens?
**Answer:** Immediate submission failure.
**Mastery Explanation:** `SparkSubmit` runs on the edge node and uses Ivy to hit Maven Central. If the edge node (or the AM in cluster mode) cannot reach the internet, dependency resolution fails before any execution begins.

**31. Scenario:** You submit a K8s job with dynamic PVCs for `spark-local-dir`. Execution is extremely fast.
**Twist:** You remove the PVC mount configs, relying on default behavior. The cluster has high network bandwidth. What happens to performance?
**Answer:** It degrades severely due to Disk I/O bottlenecks.
**Mastery Explanation:** Default K8s pod storage uses the overlay filesystem (e.g., OverlayFS/ext4) which is incredibly slow for the rapid random I/O required by Spark shuffles, regardless of network speed.

**32. Scenario:** You configure `spark.executor.memory=4G` and `spark.executor.memoryOverhead=1G`. Total container limit is 5G.
**Twist:** You invoke a Python UDF that loads a 2GB machine learning model into memory. What happens?
**Answer:** YARN kills the container with Exit 137.
**Mastery Explanation:** PySpark worker processes run outside the JVM heap. The 2GB model consumes memory from the 1G overhead pool, immediately violating the 5G container limit.

**33. Scenario:** You run a heavy `.collect()` on a 5GB DataFrame in Cluster Mode.
**Twist:** You run the exact same job in Client Mode from your laptop over a VPN. What happens?
**Answer:** Network timeout or Laptop JVM OOM.
**Mastery Explanation:** In Client mode, `.collect()` attempts to stream 5GB of serialized data over the VPN to your laptop's Driver JVM, severely bottlenecking RPC and likely crashing your local machine.

**34. Scenario:** You configure `spark.rpc.message.maxSize=1024` for a job.
**Twist:** You increase the number of partitions from 2,000 to 200,000. The job crashes. Why?
**Answer:** Driver OOM or Timeout.
**Mastery Explanation:** Massive partition counts create massive DAG Task metadata. Even with a large RPC size limit, the Driver must track and broadcast task metadata for 200k partitions, overwhelming its heap or GC cycles.

**35. Scenario:** You use the Maven Shade Plugin to relocate `com.google.common` to `shaded.com.google.common`.
**Twist:** You pass `spark.executor.userClassPathFirst=true` as well. What happens?
**Answer:** The job succeeds, but the configuration is redundant and wastes Metaspace.
**Mastery Explanation:** Since the classes are now physically in a different namespace, there is no conflict. Forcing child-first classloading provides no benefit and only duplicates other non-shaded classes in memory.

**36. Scenario:** A PySpark job uses `--py-files utils.zip`. In Cluster mode, the zip is uploaded to HDFS and localized.
**Twist:** You use a Python virtual environment (`.tar.gz`) instead and set `spark.pyspark.python=./env/bin/python`. What phase handles the extraction?
**Answer:** The YARN NodeManager (via distributed cache).
**Mastery Explanation:** YARN's distributed cache automatically extracts `.tar.gz` and `.zip` archives into the container's working directory before the Spark Executor starts.

**37. Scenario:** You run `spark-submit` and specify `--master yarn`.
**Twist:** You forgot to specify `--deploy-mode`. What is the default?
**Answer:** Client Mode.
**Mastery Explanation:** Spark defaults to Client mode if unspecified. This is a common pitfall that accidentally runs heavy production drivers on edge nodes instead of securely within the cluster.

**38. Scenario:** A K8s Spark job dynamically requests 10 executors.
**Twist:** The K8s cluster only has resources for 5. What happens?
**Answer:** The job runs with 5 executors and queues the rest.
**Mastery Explanation:** Unlike static MPI workloads, Spark is fault-tolerant and elastic. The Driver will schedule tasks on the available 5 executors while waiting for the others.

**39. Scenario:** You set `spark.driver.memory=8G` in `spark-defaults.conf`.
**Twist:** You pass `--driver-memory 4G` in the `spark-submit` CLI. Which one wins?
**Answer:** `4G` (Command Line).
**Mastery Explanation:** `SparkSubmitArguments` parses command-line flags with higher precedence, overriding statically defined configurations in `spark-defaults.conf`.

**40. Scenario:** Your `SparkLauncher` app listens for `handle.getState().isFinal()`.
**Twist:** The YARN cluster restarts forcefully. What state is returned?
**Answer:** FAILED or KILLED.
**Mastery Explanation:** The AM loses its heartbeat with YARN. `SparkLauncher` receives the terminal state transition from the child JVM and successfully breaks the while-loop.

## 4. Coding & Debugging Questions

**41. Debug the Error:** You see `java.lang.NoSuchMethodError: com.fasterxml.jackson.core.JsonGenerator.writeStartObject()`.
**Resolution:** This is a classic dependency hell issue. Spark internal Catalyst uses an older version of Jackson. You must use the Maven Shade Plugin to relocate your application's modern Jackson dependency, or (with caution) set `spark.executor.userClassPathFirst=true`.

**42. Debug the Error:** `ExecutorLostFailure (executor 12 exited caused by one of the running tasks) Reason: Container killed by YARN for exceeding memory limits. 4.5 GB of 4.5 GB physical memory used.`
**Resolution:** The Executor exceeded its total container limit. The application is likely doing heavy off-heap processing (Tungsten/PySpark). Increase `spark.executor.memoryOverhead` from its default (10%) to a larger absolute value, e.g., `--conf spark.executor.memoryOverhead=2g`.

**43. Debug the Error:** `java.lang.OutOfMemoryError: Metaspace` occurs immediately after job submission in Cluster mode.
**Resolution:** The user enabled `spark.driver.userClassPathFirst=true`. This causes redundant class loading. The fix is to add `--conf spark.driver.extraJavaOptions="-XX:MaxMetaspaceSize=512m"` to expand the Metaspace buffer.

**44. Debug the Error:** In Client mode, the job runs fine for 30 minutes, then fails with `TimeoutException: Futures timed out after [120 seconds]`.
**Resolution:** The Driver on the edge node experienced a network blip or a heavy GC pause, severing the RPC heartbeat with executors. Increase `--conf spark.network.timeout=600s` and `spark.executor.heartbeatInterval=60s`.

**45. Identify the Logic Error:**
```bash
spark-submit --master yarn --deploy-mode cluster \
  --executor-memory 4G \
  --conf spark.executor.memoryOverhead=4G \
  --driver-memory 16G \
  local:///opt/app.jar
```
**Error:** Providing `local:///opt/app.jar` in **Cluster mode** is highly problematic unless that JAR physically exists at `/opt/app.jar` on *every single worker node* in the cluster. In Cluster mode, the AM runs on a random worker node. It should be a distributed URI (e.g., `hdfs://`) or a local file *without* `local://` so `spark-submit` stages it.

**46. Debug the Error:** K8s submission: `Pod 'spark-driver' is in Pending state`.
**Resolution:** The Driver pod cannot be scheduled. This is usually due to insufficient cluster resources, missing NodeSelectors, or an invalid ServiceAccountName (`spark.kubernetes.authenticate.driver.serviceAccountName`) lacking RBAC permissions to create pods.

**47. Identify the Bottleneck:** A Spark job on K8s is taking 10x longer than on YARN. CPU usage is low, but Disk I/O Wait is 99%.
**Resolution:** The user forgot to map a Persistent Volume Claim (PVC) to `spark-local-dir`. Shuffle files are writing to the ephemeral pod overlay filesystem. Add PVC mount configs in `spark-submit`.

**48. Identify the Logic Error in PySpark Submit:**
```python
# Inside python code
spark = SparkSession.builder.master("yarn").config("spark.submit.deployMode", "cluster").getOrCreate()
```
**Error:** `deployMode` cannot be dynamically changed from *inside* the Python application code. By the time `SparkSession.builder` executes, the JVM and ApplicationMaster architecture have already been initialized by the `spark-submit` bash wrapper.

**49. Debug the Error:** `java.lang.IllegalArgumentException: Size exceeds Integer.MAX_VALUE` during a `.collect()` in Client Mode.
**Resolution:** The serialized result set exceeds 2GB, breaching the JVM's byte array limit for a single object. You cannot safely collect this amount of data to the driver. Write the output to distributed storage (`.write.parquet()`) instead.

**50. Debug the Error:** Using `SparkLauncher`, the application stays in `UNKNOWN` state indefinitely.
**Resolution:** The edge node running the Java process lacks the `HADOOP_CONF_DIR` or `YARN_CONF_DIR` environment variables. The internal `spark-submit` child process cannot locate the YARN cluster endpoints to submit the application or track its status.
