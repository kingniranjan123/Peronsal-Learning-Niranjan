# Spark Cluster Types - Senior Assessment

## Section 1: True/False (10 Questions)

1. **Question**: In YARN `client` mode, the Spark Driver is encapsulated within a container managed by the NodeManager to mitigate network latency between the Driver and remote Executors.
**Answer**: False
**Mastery Explanation**: In `client` mode, the Driver runs on the submitting machine (the client), not in the cluster. It is in `cluster` mode that the Driver is encapsulated within the ApplicationMaster container on a worker node.

2. **Question**: When deploying on Kubernetes, the K8s API server spawns an ApplicationMaster pod which then negotiates for Executor pods.
**Answer**: False
**Mastery Explanation**: Kubernetes does not have a dedicated Spark ApplicationMaster. The `spark-submit` script communicates directly with the K8s API server to create a Driver pod, which acts as its own cluster manager client.

3. **Question**: Setting `spark.yarn.executor.memoryOverhead` is essential because Tungsten allocates memory off-heap, which can cause YARN to kill the container with an OOM error if not accounted for.
**Answer**: True
**Mastery Explanation**: YARN tracks total container memory. Tungsten leverages off-heap native memory. If the combined JVM on-heap and off-heap memory exceeds the container size, YARN's NodeManager will kill the container.

4. **Question**: The Catalyst optimizer dynamically coalesces shuffle partitions (AQE) specifically to reduce network I/O strain on Kubernetes overlay networks like Calico.
**Answer**: True
**Mastery Explanation**: AQE adapts query plans at runtime, dynamically coalescing shuffle partitions. This is highly beneficial on K8s where overlay network performance can bottleneck heavy shuffle operations.

5. **Question**: In a Standalone cluster, memory management is primarily handled by Linux cgroups ensuring strict isolation between concurrent Spark applications on the same worker.
**Answer**: False
**Mastery Explanation**: Standalone Master is a lightweight actor system and does not natively use Linux cgroups for strict isolation unlike YARN or Mesos. It relies on OS-level JVM process limits and garbage collection behavior.

6. **Question**: Standalone cluster security is robust by default and encrypts all communication between the Driver and Executors automatically.
**Answer**: False
**Mastery Explanation**: Standalone mode security is inherently weak by default. You must explicitly enforce `spark.authenticate` and `spark.network.crypto.enabled` to secure raw TCP socket communications.

7. **Question**: In Mesos coarse-grained mode, Spark acquires a fixed number of resources for the entire duration of the application.
**Answer**: True
**Mastery Explanation**: Coarse-grained allocation means Spark holds onto resources even when idle during the application lifecycle, unlike fine-grained mode which negotiated per task (now deprecated).

8. **Question**: Kryo serialization is the default serialization framework in Spark 3.x and is universally used for all RDD and DataFrame shuffles.
**Answer**: False
**Mastery Explanation**: Java serialization is still the default for RDDs (though DataFrames use Tungsten encoders). To use Kryo for RDDs and certain internal operations, you must explicitly configure `spark.serializer=org.apache.spark.serializer.KryoSerializer`.

9. **Question**: When using Dynamic Resource Allocation on Kubernetes, an External Shuffle Service is strictly required to preserve intermediate shuffle files.
**Answer**: False
**Mastery Explanation**: Spark 3.2+ introduced shuffle tracking on K8s using Persistent Volume Claims (PVCs), eliminating the strict need for a Node-level External Shuffle Service.

10. **Question**: In a Standalone cluster configured with ZooKeeper for HA, setting `spark.deploy.recoveryMode` to `ZOOKEEPER` allows a standby master to seamlessly take over if the active master crashes.
**Answer**: True
**Mastery Explanation**: ZooKeeper maintains the cluster state (worker and application info). Upon active master failure, a standby reads the state from ZK and seamlessly resumes orchestration.

## Section 2: Multiple Choice (15 Questions)

11. **Question**: You are encountering aggressive container preemption by the YARN NodeManager with OOM errors. Your executor memory is 16GB. Which configuration is the MOST appropriate fix?
A) Increase `spark.executor.memory` to 32GB
B) Set `spark.yarn.executor.memoryOverhead=4096`
C) Reduce `spark.executor.cores` to 1
D) Enable `spark.dynamicAllocation.enabled=true`
**Answer**: B
**Mastery Explanation**: YARN OOM kills happen when the physical memory of the container is exceeded. Increasing executor memory (on-heap) leaves less room for off-heap overhead. Increasing `memoryOverhead` accommodates Tungsten's off-heap allocations, preventing the NM from killing the container.

12. **Question**: When K8s executor pods are aggressively evicted due to disk pressure, what is the most robust architectural solution?
A) Increase `spark.executor.memory`
B) Rely on default ephemeral K8s storage
C) Mount an NVMe hostPath to `spark.local.dir` using a Pod Template
D) Disable Spark dynamic allocation
**Answer**: C
**Mastery Explanation**: Spark spills shuffle data to `spark.local.dir`. Ephemeral container storage quickly exhausts K8s node disk, causing eviction. Mounting a dedicated high-I/O volume (like SSD/NVMe) via Pod Templates resolves this.

13. **Question**: Which component is responsible for strict resource isolation via Linux cgroups in a Hadoop YARN deployment?
A) ApplicationMaster
B) ResourceManager
C) NodeManager
D) Spark Driver
**Answer**: C
**Mastery Explanation**: The NodeManager resides on worker nodes and manages the physical execution environment, including enforcing container memory/CPU limits via cgroups.

14. **Question**: In a Spark Mesos deployment, what does `spark.mesos.coarse=true` signify?
A) Spark negotiates resources on a per-task basis.
B) Spark acquires a fixed block of resources for the application's duration.
C) Spark uses the Universal Containerizer instead of Docker.
D) Spark disables the Catalyst Optimizer.
**Answer**: B
**Mastery Explanation**: Coarse-grained mode locks resources for the entire lifespan of the SparkContext, avoiding the latency of negotiating resources per task, which is ideal for long-running batch jobs.

15. **Question**: Why is tuning `spark.executor.cores` crucial in YARN?
A) Too many cores per executor overwhelms the JVM garbage collector and reduces HDFS throughput.
B) YARN charges money per core.
C) Cores dictate how much memory the ApplicationMaster receives.
D) Tungsten requires exactly 1 core per executor.
**Answer**: A
**Mastery Explanation**: While more cores mean more parallelism, >5 cores often leads to HDFS I/O bottlenecks and excessive GC pauses due to massive concurrent thread memory allocations in a single JVM.

16. **Question**: In K8s Native Execution, how does the Driver request Executors?
A) Through the YARN ResourceManager
B) By launching a Mesos framework
C) Communicating directly with the Kubernetes API server
D) Using the Standalone Master actor system
**Answer**: C
**Mastery Explanation**: The K8s Driver acts as the cluster manager client, making direct API calls to the K8s master to create/delete Executor pods dynamically.

17. **Question**: What is the primary purpose of `spark.dynamicAllocation.enabled` when used with YARN?
A) To dynamically change the JVM heap size at runtime.
B) To gracefully scale executors up/down based on workload, preserving shuffles via the External Shuffle Service.
C) To dynamically switch between YARN and Kubernetes.
D) To alter the Catalyst physical plan.
**Answer**: B
**Mastery Explanation**: Dynamic allocation adds/removes executors based on task backlog. The External Shuffle Service ensures intermediate shuffle files aren't lost when an executor scales down.

18. **Question**: Which serialization framework provides the highest performance for network I/O during heavy YARN shuffles?
A) Java Serialization
B) Kryo Serialization
C) JSON Serialization
D) XML Serialization
**Answer**: B
**Mastery Explanation**: Kryo is significantly faster and more compact than Java serialization, severely reducing the memory footprint and network I/O during massive shuffle operations.

19. **Question**: In a Standalone cluster, what scheduler does the Master primarily support at the application level?
A) Fair Scheduler
B) Dominant Resource Fairness (DRF)
C) FIFO (First-In, First-Out)
D) Capacity Scheduler
**Answer**: C
**Mastery Explanation**: Standalone mode natively uses a simple FIFO scheduler across applications unless constrained by cores.max, unlike YARN which has advanced Capacity/Fair queues.

20. **Question**: How does Tungsten interact with YARN container limits?
A) Tungsten only uses on-heap memory.
B) Tungsten allocations are invisible to YARN.
C) Tungsten native off-heap memory counts against the total YARN container size, risking OOM kills.
D) YARN automatically adjusts container sizes for Tungsten.
**Answer**: C
**Mastery Explanation**: YARN monitors the total resident set size (RSS) of the container. Tungsten’s off-heap allocations increase RSS. If `memoryOverhead` isn't large enough, YARN kills the container.

21. **Question**: To ensure an expansive join on Mesos doesn't cause a massive network shuffle, which strategy is best?
A) Coarse-grained allocation
B) Fine-grained allocation
C) Using a Broadcast Hash Join for the smaller dataset
D) Disabling Tungsten
**Answer**: C
**Mastery Explanation**: Broadcasting the small DataFrame copies it to all executors, allowing Spark to perform a map-side join, entirely avoiding the costly shuffle phase across the Mesos network fabric.

22. **Question**: What happens in a Standalone cluster if `spark.memory.fraction` is tuned too high for execution, and caching is heavy?
A) Spark switches to YARN.
B) The JVM throws OOM or experiences massive GC pauses, tricking the Master into thinking the Executor failed.
C) The Master provisions more memory dynamically.
D) The Catalyst optimizer drops cached data automatically.
**Answer**: B
**Mastery Explanation**: Poor memory fraction tuning leads to on-heap pressure. Severe GC pauses block heartbeats, causing the Standalone Master to mark the executor as dead and fail the tasks.

23. **Question**: What is the purpose of `spark.kubernetes.authenticate.driver.serviceAccountName`?
A) To authenticate the driver with YARN.
B) To provide the Driver pod with RBAC permissions to request Executor pods from the K8s API server.
C) To encrypt data on disk.
D) To log into HDFS.
**Answer**: B
**Mastery Explanation**: The Driver pod needs authorization to create, read, and delete Executor pods in the K8s namespace. A properly configured ServiceAccount with RBAC roles grants this permission.

24. **Question**: Which K8s feature eliminates the need for a Node-level External Shuffle Service for dynamic allocation in modern Spark?
A) StatefulSets
B) DaemonSets
C) Persistent Volume Claims (PVCs) for shuffle tracking
D) Ingress Controllers
**Answer**: C
**Mastery Explanation**: Spark 3.2+ tracks shuffle files on PVCs so that if an executor pod scales down or dies, the shuffle data persists on the volume and remains accessible to other executors.

25. **Question**: In Mesos, why might you configure `spark.mesos.executor.docker.forcePullImage=false`?
A) To force a fresh download every time.
B) To avoid dependency hell by reusing locally cached images, saving startup time and bandwidth.
C) To bypass Docker entirely.
D) To use the Universal Containerizer instead.
**Answer**: B
**Mastery Explanation**: Preventing unnecessary image pulls speeds up executor launch times and reduces network load on the container registry, relying on the image already cached on the Mesos agent.

## Section 3: Small Twist Questions (15 Questions)

26. **Scenario**: You run a Spark job on YARN with `--deploy-mode client`. It runs fine. 
**Twist**: You change to `--deploy-mode cluster`. The job now fails unable to read a local config file on the submitting machine.
**Answer**: In cluster mode, the Driver runs on a random worker node.
**Mastery Explanation**: Client mode keeps the Driver on the edge node where it can read local files. In cluster mode, the Driver moves to the AM on a YARN worker, which doesn't have the edge node's local filesystem.

27. **Scenario**: You configure K8s dynamic allocation using PVCs for shuffle tracking. It works perfectly.
**Twist**: You switch to ephemeral `emptyDir` volumes. The job fails during a heavy shuffle scale-down.
**Answer**: Ephemeral volumes are deleted when the pod dies, destroying shuffle data.
**Mastery Explanation**: PVCs persist beyond pod lifecycle. `emptyDir` is tied to the pod. When dynamic allocation scales down an executor, `emptyDir` is wiped, causing `FetchFailedException` for shuffle blocks.

28. **Scenario**: A Standalone cluster uses `spark.cores.max=16`. The job shares the cluster well.
**Twist**: You remove `spark.cores.max`. Suddenly no other jobs can run.
**Answer**: The job greedily consumed all available cores in the cluster.
**Mastery Explanation**: Without `spark.cores.max`, a Spark application on Standalone mode will claim all available cores on all workers, starving concurrent applications (FIFO scheduling).

29. **Scenario**: On YARN, `spark.executor.memory=16G` and `memoryOverhead=2G`. Containers are stable.
**Twist**: You implement heavy PySpark UDFs using Apache Arrow. Containers start getting OOM killed by YARN.
**Answer**: PySpark UDFs spawn Python worker processes off-heap.
**Mastery Explanation**: Arrow/Python UDFs allocate massive amounts of off-heap memory. The 2G overhead is no longer sufficient for the JVM off-heap + Python processes, breaching the YARN container limit.

30. **Scenario**: In K8s, your Pod Template sets a `nodeSelector` for `disktype: ssd`. Executors launch successfully.
**Twist**: You misspell it as `disktype: sdd`. The job hangs indefinitely in `Accepted` state.
**Answer**: The K8s scheduler cannot find nodes matching the selector.
**Mastery Explanation**: The Driver requests pods with a non-existent label. K8s leaves them in `Pending`. Spark Driver waits indefinitely for executors to register, causing an application hang.

31. **Scenario**: You run a job on Mesos coarse-grained mode, allocating 100 executors. It takes 1 hour.
**Twist**: You enable fine-grained mode (on an older Spark version). The job now takes 4 hours.
**Answer**: Fine-grained mode incurs massive scheduling latency per task.
**Mastery Explanation**: Fine-grained mode negotiates Mesos offers for *every single task*. For a job with 100,000 tasks, the RPC overhead to the Mesos Master destroys performance compared to holding coarse resources.

32. **Scenario**: A Standalone HA cluster has `master1` and `master2` in Zookeeper. `master1` dies, `master2` takes over smoothly.
**Twist**: You deploy the exact same setup but forget `spark.deploy.recoveryMode=ZOOKEEPER`.
**Answer**: The standby master does not inherit the state, and the cluster must be fully restarted.
**Mastery Explanation**: Without the ZK recovery mode flag, the masters operate independently and do not sync state. Failing over means losing all knowledge of workers and active applications.

33. **Scenario**: YARN job runs with `spark.executor.cores=5`. HDFS read throughput is optimal.
**Twist**: You change `spark.executor.cores=32`. The job becomes incredibly slow and times out.
**Answer**: Severe JVM Garbage Collection contention and thread context switching.
**Mastery Explanation**: 32 cores mean 32 concurrent tasks allocating objects in a single JVM heap. This overwhelms the GC, leading to "Stop the World" pauses that cause heartbeat timeouts and executor loss.

34. **Scenario**: You broadcast a 10MB table on Mesos to avoid a shuffle. Performance is excellent.
**Twist**: The table grows to 10GB. You increase `spark.sql.autoBroadcastJoinThreshold` to 10GB. The Driver crashes with OOM.
**Answer**: The Driver must collect the entire broadcast variable in its heap before sending it to executors.
**Mastery Explanation**: A Broadcast join pulls the data to the Driver first. A 10GB dataset will easily blow up a standard Driver heap, causing `java.lang.OutOfMemoryError: Java heap space`.

35. **Scenario**: On Kubernetes, you use the default container image and jobs run fine.
**Twist**: You switch to a custom Alpine-based Docker image. Jobs fail complaining about missing Native libraries (Hadoop/Snappy).
**Answer**: Alpine uses `musl` libc instead of `glibc`.
**Mastery Explanation**: Spark and Hadoop native libraries (for compression/I/O) are compiled against `glibc`. Using Alpine breaks these bindings, requiring a Debian/Ubuntu-based image or recompilation.

36. **Scenario**: You enable dynamic allocation on YARN and the External Shuffle Service. Scaling works perfectly.
**Twist**: You forget to enable the External Shuffle Service on the NodeManagers. Executors scale down, and the job fails with `FetchFailedException`.
**Answer**: Intermediate shuffle files were deleted when the executors scaled down.
**Mastery Explanation**: Without the External service holding the shuffle files, when an idle executor is killed by dynamic allocation, its local shuffle files vanish, breaking the DAG lineage for downstream stages.

37. **Scenario**: You run a Standalone cluster with `spark.network.crypto.enabled=true`.
**Twist**: You set `spark.authenticate=false`. The job fails to start.
**Answer**: Encryption requires authentication to exchange symmetric keys securely.
**Mastery Explanation**: Spark's AES encryption uses a shared secret to negotiate session keys. If authentication is disabled, key exchange cannot occur, and the RPC layer fails to initialize.

38. **Scenario**: A PySpark job writes to HDFS perfectly on YARN.
**Twist**: You change `spark.serializer` to Kryo. The job runs the same. Why didn't it crash if PySpark doesn't use Kryo?
**Answer**: PySpark uses Pickle for Python objects, Kryo setting only affects internal Java/Scala JVM data structures.
**Mastery Explanation**: Python data is serialized via Pickle. The `spark.serializer` flag dictates JVM-level RDD shuffling and broadcasting. The setting is ignored for Python-native object movement.

39. **Scenario**: On K8s, your Driver requests executors with 4GB memory. They schedule perfectly.
**Twist**: You add a Pod Template that injects an Istio sidecar proxy to every pod. Executors never reach the `Running` state.
**Answer**: The sidecar consumes node resources, pushing the total pod requirement beyond available node capacity.
**Mastery Explanation**: Sidecars add memory/CPU requests. If the K8s node only had exactly 4GB free, injecting a 500MB sidecar makes the pod unschedulable (Pending).

40. **Scenario**: YARN job runs with 400 shuffle partitions. Performance is good.
**Twist**: You filter the data severely before the shuffle, resulting in 400 empty/tiny partitions. Performance degrades due to task overhead.
**Answer**: Adaptive Query Execution (AQE) is disabled.
**Mastery Explanation**: Without AQE coalescing the tiny partitions, Spark launches 400 tasks to process KB of data. The scheduler overhead vastly exceeds the execution time.

## Section 4: Coding & Debugging (10 Questions)

41. **Question**: You observe this error in YARN NodeManager logs: `Container killed by YARN for exceeding memory limits. 17.5 GB of 17 GB physical memory used. Consider boosting spark.yarn.executor.memoryOverhead`. 
You check your code and see heavy use of `Window` functions without a `PARTITION BY` clause. What is the architectural root cause?
**Answer**: All data is forced into a single executor, blowing up the JVM heap and off-heap memory.
**Mastery Explanation**: A Window function without a partition clause forces a global sort on a single node. Tungsten attempts to buffer this off-heap, exceeding the YARN container RSS limit.

42. **Question**: Debug this K8s Spark Submit:
```bash
spark-submit --master k8s://... \
  --deploy-mode cluster \
  --conf spark.kubernetes.container.image=spark:3.4 \
  local:///opt/spark/work/main.py
```
The Driver pod starts, but zero Executors are ever requested. The logs show `HTTP 403 Forbidden` when contacting the API server.
**Answer**: Missing `spark.kubernetes.authenticate.driver.serviceAccountName`.
**Mastery Explanation**: The default ServiceAccount lacks RBAC permissions to create pods. The Driver is denied access to spawn executors. You must configure a role-bound service account.

43. **Question**: Identify the memory leak in this PySpark code running on a Standalone cluster:
```python
for i in range(100):
    df = spark.read.parquet(f"/data/part_{i}")
    df.cache()
    df.count()
```
The cluster slowly grinds to a halt and executors die.
**Answer**: The loop caches 100 DataFrames into memory without unpersisting them, saturating the Storage Memory fraction.
**Mastery Explanation**: `cache()` evaluates lazily, but `count()` materializes it. Doing this 100 times fills the Standalone worker's memory, pushing out Execution memory and causing massive GC pauses.

44. **Question**: Debug this physical plan optimizer blocker on Mesos.
```python
def custom_logic(x):
    import requests
    return requests.get(f"http://api/{x}").text

df.rdd.map(custom_logic).toDF().join(other_df, "id")
```
Why does this cause catastrophic network I/O and task failures on the Mesos cluster?
**Answer**: The UDF breaks Catalyst optimization (Whole-stage CodeGen) and opens a synchronous HTTP connection per row.
**Mastery Explanation**: Black-box Python UDFs prevent Catalyst from optimizing the join. Furthermore, an HTTP call per row causes task thread starvation and socket exhaustion on the Mesos agents.

45. **Question**: You submit to YARN with `--conf spark.executor.instances=50`, but the job only ever gets 2 executors. The YARN UI shows plenty of cluster resources available. What is blocking the allocation?
**Answer**: YARN queue capacity limits or dynamic allocation overriding the static instance count.
**Mastery Explanation**: If submitted to a YARN queue with a `max-capacity` of 2 containers, YARN strictly enforces it regardless of cluster size. Alternatively, if dynamic allocation is on with `minExecutors=2`, it may start at 2 and not scale up if the initial DAG is small.

46. **Question**: Analyze this K8s Pod Template snippet:
```yaml
volumes:
  - name: spark-local-dir
    emptyDir: {}
```
Why is this dangerous for a heavy shuffle workload spanning terabytes of data?
**Answer**: `emptyDir` writes to the K8s node's ephemeral disk (usually root filesystem).
**Mastery Explanation**: Heavy shuffles will fill the node's `/var/lib/kubelet` partition. The Kubelet will detect `DiskPressure` and ruthlessly evict the executor pods, failing the Spark job.

47. **Question**: A Standalone cluster has 4 workers, 16 cores each. You submit with `spark.cores.max=32`. The job only runs on 2 workers, leaving the other 2 completely idle. Why?
**Answer**: The scheduler greedily assigns cores per worker until the `cores.max` is hit.
**Mastery Explanation**: By default, Standalone schedules executors greedily. It takes 16 cores from Worker 1, 16 cores from Worker 2 (total 32). To spread evenly, you must set `spark.deploy.spreadOut=true` or limit `spark.executor.cores`.

48. **Question**: Debug this PySpark YARN submission:
```bash
spark-submit --master yarn --deploy-mode cluster \
  --conf spark.executor.memory=2G \
  --conf spark.yarn.executor.memoryOverhead=500 \
  app.py
```
The job fails during a `df.toPandas()` action. Why?
**Answer**: `toPandas()` collects all data to the Driver's memory.
**Mastery Explanation**: The default Driver memory in YARN cluster mode is typically 1G-2G. Collecting a distributed dataset to a single Pandas DataFrame easily blows up the Driver's JVM heap (OOM).

49. **Question**: On a Mesos cluster, you use `spark.mesos.executor.docker.volumes=/host/data:/container/data:ro`. Your Spark job attempts to write output to `/container/data/output.csv`. What happens and why?
**Answer**: The job fails with an `IOException: Read-only file system`.
**Mastery Explanation**: The Docker volume is mounted with the `ro` (read-only) flag. Spark executors inside the container cannot write to that path. It must be changed to `rw` (read-write).

50. **Question**: You observe massive CPU spikes on YARN NodeManagers, but task execution is extremely slow. The application uses `df.repartition(10000)`. What is the architectural flaw?
**Answer**: Creating 10,000 partitions for a small dataset causes task scheduling overhead to dominate CPU time.
**Mastery Explanation**: The Driver (in the AM) must schedule 10,000 tasks. The executors spend all their CPU time deserializing task binaries and spinning up threads, rather than doing actual data processing.
