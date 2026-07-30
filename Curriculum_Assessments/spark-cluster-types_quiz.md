# Spark Cluster Types - Elite Technical Assessment

## Part 1: True/False Questions (10)

**Q1:** In YARN `cluster` mode, the Spark Driver runs within the ApplicationMaster container on a NodeManager, while in `client` mode, it runs on the submitting machine.
**Answer:** True
**Mastery Explanation:** The ApplicationMaster (AM) negotiates resources from the ResourceManager. In cluster mode, the AM hosts the Driver, encapsulating it within the cluster and isolating it from client-side network latency or disconnections.

**Q2:** Setting `spark.executor.cores` to an extremely high number (e.g., 32) on YARN always improves HDFS read throughput due to maximum parallelism.
**Answer:** False
**Mastery Explanation:** Assigning too many cores per executor can overwhelm the JVM garbage collector (GC), leading to excessive GC pauses. Typically, 5 cores per executor is considered optimal to balance throughput and GC overhead.

**Q3:** Kubernetes requires an external shuffle service to achieve Dynamic Resource Allocation in Spark 3.2+.
**Answer:** False
**Mastery Explanation:** Spark 3.2+ introduced shuffle tracking with Persistent Volume Claims (PVCs) for K8s, which allows Dynamic Resource Allocation without needing a dedicated external shuffle service.

**Q4:** In a Standalone cluster, the Master daemon uses Linux cgroups by default to enforce strict multi-tenant memory isolation.
**Answer:** False
**Mastery Explanation:** Standalone mode is a lightweight Scala actor system that primarily uses FIFO scheduling at the app level. It lacks the deep multi-tenant resource isolation of YARN (which uses cgroups).

**Q5:** Spark's Catalyst optimizer and Tungsten execution engine are decoupled from the cluster management layer, meaning query physical plans do not change purely based on using YARN vs Kubernetes.
**Answer:** True
**Mastery Explanation:** Spark's core execution engine is decoupled. While execution contexts (containers) change, the Catalyst optimizations and Tungsten's memory management run uniformly within the allocated JVMs across all cluster types.

**Q6:** Setting `spark.kubernetes.executor.podTemplateFile` allows Data Engineers to pin specific executor pods to SSD-backed nodes using K8s `nodeSelector`.
**Answer:** True
**Mastery Explanation:** Pod templates natively expose K8s constructs like `nodeSelector`, `tolerations`, and `volumeMounts`, allowing precise hardware targeting, which is critical for I/O heavy shuffle operations.

**Q7:** If Tungsten experiences heavy off-heap memory fragmentation in a Standalone cluster, it directly causes the Standalone Master to restart the worker node.
**Answer:** False
**Mastery Explanation:** Memory fragmentation or excessive GC pauses make the worker/executor unresponsive. The Master might think the Executor failed due to timeouts, but it doesn't natively detect Tungsten fragmentation to explicitly restart the worker.

**Q8:** By default, Spark on Apache Mesos runs in fine-grained allocation mode to maximize resource sharing across heterogeneous workloads.
**Answer:** False
**Mastery Explanation:** Coarse-grained is the default and fine-grained is deprecated. In coarse-grained mode, Spark holds onto the resources for the duration of the application.

**Q9:** In YARN, if `spark.executor.memory` + `spark.yarn.executor.memoryOverhead` exceeds `yarn.nodemanager.resource.memory-mb`, the ResourceManager will reject the container allocation.
**Answer:** True
**Mastery Explanation:** YARN strictly enforces maximum container sizes based on NodeManager capacities. If the total requested memory exceeds this limit, YARN cannot provision the container.

**Q10:** Using `spark.deploy.recoveryMode=ZOOKEEPER` in a Standalone cluster encrypts the network shuffle data between executors.
**Answer:** False
**Mastery Explanation:** ZooKeeper recovery mode provides High Availability for the Standalone Master. To encrypt data, you must configure `spark.network.crypto.enabled=true` and use authentication.

## Part 2: Multiple Choice Questions (15)

**Q11:** When tuning a heavy shuffle workload on YARN, what is the primary purpose of increasing `spark.yarn.executor.memoryOverhead`?
A) To increase the on-heap memory for RDD caching.
B) To accommodate Tungsten’s off-heap allocations and prevent NodeManager OOM kills.
C) To allow the Driver to broadcast larger datasets.
D) To speed up Kryo serialization.
**Answer:** B
**Mastery Explanation:** YARN monitors total container memory. Tungsten aggressively uses off-heap memory for whole-stage code generation. If off-heap allocations exceed the overhead, YARN kills the container with an OOM error.

**Q12:** In a Kubernetes-native Spark deployment, why is it critical to mount a `hostPath` volume for `spark.local.dir` instead of using default K8s ephemeral storage?
A) Ephemeral storage cannot be accessed by the K8s API.
B) It allows the Driver to directly read executor memory.
C) Default ephemeral storage can cause disastrous I/O bottlenecks and pod evictions during heavy shuffles.
D) `hostPath` is required for K8s Cluster Autoscaler to function.
**Answer:** C
**Mastery Explanation:** Spark spills shuffle data to disk. Ephemeral K8s storage is often mapped to the root disk, which can fill up or bottleneck. A `hostPath` mapping to an NVMe drive ensures high IOPS and prevents disk pressure evictions.

**Q13:** Which K8s feature eliminates the need for an External Shuffle Service when using Dynamic Allocation in Spark 3.2+?
A) Kubelet garbage collection
B) StatefulSets
C) Persistent Volume Claims (PVCs) with shuffle tracking
D) Calico overlay networking
**Answer:** C
**Mastery Explanation:** Spark tracks shuffle files on PVCs so that even if an executor scales down, the intermediate shuffle data remains accessible, mimicking the External Shuffle Service.

**Q14:** In a Standalone cluster, what happens if prolonged JVM GC pauses occur on an executor?
A) The Master optimizes the DAG to reduce memory load.
B) The Master might interpret the pause as an executor failure due to missed heartbeats.
C) ZooKeeper automatically allocates more RAM.
D) Tungsten switches to on-heap execution.
**Answer:** B
**Mastery Explanation:** Standalone Master relies on heartbeats. Long GC pauses (often from poor memory fraction tuning) block heartbeats, tricking the Master into declaring the executor dead.

**Q15:** Which serialization framework is recommended in YARN to reduce network I/O and footprint during shuffles?
A) Java Native Serialization
B) KryoSerializer
C) Avro
D) Protobuf
**Answer:** B
**Mastery Explanation:** Kryo is significantly faster and more compact than default Java serialization, reducing network overhead during shuffles and complementing Tungsten's performance.

**Q16:** How does the Driver pod in a K8s native deployment provision Executors?
A) By requesting them from a YARN ResourceManager running in K8s.
B) By using a dedicated Spark ApplicationMaster pod.
C) By directly communicating with the Kubernetes API server.
D) By spinning up local threads using Docker-in-Docker.
**Answer:** C
**Mastery Explanation:** K8s has no dedicated AM. The `spark-submit` creates the Driver pod, which then acts as its own cluster manager client directly against the K8s API.

**Q17:** What does `spark.mesos.coarse=true` signify in an Apache Mesos cluster?
A) Spark acquires resources per task and releases them immediately.
B) Spark acquires a fixed number of resources for the entire duration of the application.
C) Mesos ignores cgroups and uses coarse memory limits.
D) Spark forces Mesos to use the Docker containerizer.
**Answer:** B
**Mastery Explanation:** Coarse-grained mode claims resources for the app's lifetime, reducing scheduling overhead compared to fine-grained mode (which negotiates per task).

**Q18:** In a Standalone cluster, what is the role of `spark.cores.max`?
A) It sets the maximum cores a single executor can use.
B) It sets the global limit of cores the application can acquire across the entire cluster.
C) It limits the cores the Driver can use.
D) It limits the number of partitions in a shuffle.
**Answer:** B
**Mastery Explanation:** Without `spark.cores.max`, a Standalone app will greedily consume all available cores on the cluster, blocking concurrent multi-tenant applications.

**Q19:** When configuring HA for a Standalone cluster, what service stores the cluster state for recovery?
A) HDFS
B) Redis
C) ZooKeeper
D) K8s etcd
**Answer:** C
**Mastery Explanation:** `spark.deploy.recoveryMode=ZOOKEEPER` allows Standalone masters to persist worker and app state to ZooKeeper for seamless failover.

**Q20:** Why is Client mode generally unsuitable for production ETL jobs on a remote cluster?
A) Client mode doesn't support Kryo serialization.
B) Network latency and potential disconnections between the submitting machine (Driver) and cluster (Executors) can fail the job.
C) Client mode forces execution to bypass Tungsten.
D) Client mode only supports 1 executor.
**Answer:** B
**Mastery Explanation:** In client mode, the Driver runs locally. Any network blip between the client machine and the cluster severs the connection, terminating the distributed execution.

**Q21:** On YARN, if an executor is dynamically allocated but remains idle, what allows Spark to scale it down without losing its shuffle data?
A) Tungsten off-heap storage
B) External Shuffle Service
C) NodeManager Cache
D) HDFS replication
**Answer:** B
**Mastery Explanation:** The External Shuffle Service runs as a standalone daemon on the NodeManager. It serves shuffle files even after the executor JVM that generated them is terminated.

**Q22:** What is the primary benefit of using a Pod Template in Spark on Kubernetes?
A) It allows Spark to bypass the Catalyst optimizer.
B) It enables fine-grained control over K8s constructs like Node Selectors and Tolerations.
C) It replaces the need for a Docker image.
D) It automatically encrypts data at rest.
**Answer:** B
**Mastery Explanation:** Pod templates merge with Spark's base pod definitions, letting you inject advanced K8s scheduling configurations like tolerations (for dedicated nodes) or node selectors (for SSDs).

**Q23:** What component in YARN is responsible for allocating physical memory and CPU resources using Linux cgroups?
A) ResourceManager
B) NodeManager
C) ApplicationMaster
D) JobHistoryServer
**Answer:** B
**Mastery Explanation:** While the ResourceManager orchestrates at a high level, the NodeManager daemon on each worker physically allocates and enforces resource constraints via cgroups.

**Q24:** In Mesos, what configuration explicitly ensures strict environment consistency and avoids dependency hell across agent nodes?
A) `spark.mesos.coarse`
B) `spark.mesos.executor.docker.image`
C) `spark.mesos.task.labels`
D) `spark.dynamicAllocation.enabled`
**Answer:** B
**Mastery Explanation:** Utilizing the Docker containerizer in Mesos (`spark.mesos.executor.docker.image`) ensures that dependencies are packaged into an immutable image rather than relying on host-level libraries.

**Q25:** How does Adaptive Query Execution (AQE) specifically assist in K8s environments?
A) By bypassing the K8s API server during scaling.
B) By dynamically coalescing shuffle partitions, reducing network I/O strain on K8s overlay networks.
C) By converting all K8s ephemeral storage to Persistent Volumes.
D) By automatically enabling ZooKeeper.
**Answer:** B
**Mastery Explanation:** K8s overlay networks (Flannel, Calico) can bottleneck under heavy shuffle traffic. AQE coalesces small partitions dynamically, minimizing the number of network connections and data transfer overhead.

## Part 3: "Small Twist" Scenario Questions (15)

**Q26:** You have a YARN cluster. You run `spark-submit --deploy-mode client`. The job runs fine. You change it to `spark-submit --deploy-mode cluster` and it immediately fails with "File not found" for a local configuration file. Why?
**Answer:** In client mode, the Driver runs on your local machine and accesses local files. In cluster mode, the Driver is on a remote NodeManager, which lacks your local filesystem file. You must use `--files` to ship it.
**Mastery Explanation:** The twist is the physical location of the Driver. Cluster mode ships the driver execution remotely, breaking hardcoded local paths.

**Q27:** You configure `spark.executor.memory=16G` and `spark.yarn.executor.memoryOverhead=2G`. `yarn.nodemanager.resource.memory-mb` is 16384 (16GB). The job is rejected by YARN. Why?
**Answer:** The total container size requested is 18GB (16G + 2G), which strictly exceeds the NodeManager's 16GB limit.
**Mastery Explanation:** YARN math is absolute. Executor Memory + Overhead cannot exceed the NM container maximum.

**Q28:** You enable Dynamic Allocation on YARN but forget to enable `spark.shuffle.service.enabled`. What happens when an idle executor scales down?
**Answer:** The shuffle data on that executor is lost. Spark will have to recompute the lost RDD/partitions, leading to massive performance degradation or job failure.
**Mastery Explanation:** Without the External Shuffle Service, executor death means local shuffle file death.

**Q29:** In K8s, you use a pod template to mount an `emptyDir` for `spark.local.dir`. You switch to a `hostPath` pointing to an NVMe drive. What is the immediate behavioral change during a massive `groupBy`?
**Answer:** Shuffle spill latency drops significantly, preventing K8s pod eviction due to root disk pressure, as `hostPath` bypasses the ephemeral layer directly to the fast host disk.
**Mastery Explanation:** `emptyDir` usually shares the node's root filesystem IO. `hostPath` to a dedicated drive isolates and accelerates shuffle IO.

**Q30:** You run a Standalone cluster with `spark.deploy.recoveryMode=ZOOKEEPER`. Master1 dies. Will the currently running Executors crash?
**Answer:** No. Executors communicate with the Driver. The Master just handles new resource allocations. The job will continue, and Master2 will take over scheduling for future apps.
**Mastery Explanation:** The Master is out of the data path. Its failure only impacts new container scheduling, not running task execution.

**Q31:** You set `spark.executor.cores=15` on YARN to maximize parallelism. The job runs slower than when it was 5 cores. Why?
**Answer:** 15 concurrent tasks in a single JVM cause excessive garbage collection overhead, overwhelming the GC threads and causing thrashing.
**Mastery Explanation:** CPU cores != linear performance in the JVM. High core counts per executor lead to GC hell. 5 is the sweet spot.

**Q32:** You submit a K8s job using Spot Instances. You use Spark 3.1 (no PVC shuffle tracking). A spot instance is preempted. What happens?
**Answer:** All shuffle data on that executor is destroyed. The Driver must recompute the DAG lineage for all lost partitions, severely delaying the job.
**Mastery Explanation:** K8s on Spot is highly volatile. Prior to 3.2 PVC tracking, preemptions meant total data loss for that executor.

**Q33:** On a Mesos cluster, you change `spark.mesos.coarse` from `true` to `false`. Your job takes 3x longer. Why?
**Answer:** Fine-grained mode (false) negotiates resources for *every single task* with the Mesos master, creating immense scheduling latency overhead.
**Mastery Explanation:** Coarse-grained grabs the resources once. Fine-grained constantly negotiates, which is an anti-pattern for short Spark tasks.

**Q34:** In Standalone mode, you do not set `spark.cores.max`. You submit App A, then App B. What happens to App B?
**Answer:** App B starves and stays in the WAITING state because App A greedily claimed all available cores in the cluster.
**Mastery Explanation:** Standalone uses FIFO. Without `cores.max`, the first app consumes the entire cluster's CPU.

**Q35:** You switch from `org.apache.spark.serializer.JavaSerializer` to `KryoSerializer` in YARN. A custom class `MyData` starts throwing `NotSerializableException`. Why?
**Answer:** You must explicitly register custom classes with Kryo (using `spark.kryo.classesToRegister`) or ensure they implement standard serialization interfaces Kryo expects.
**Mastery Explanation:** Kryo is fast but strict. Unregistered classes lose optimization or fail if they lack a no-arg constructor/registration.

**Q36:** On K8s, your Driver pod requests Executors, but they stay in `Pending` state forever. Your `nodeSelector` requires `disktype: nvme`. What is wrong?
**Answer:** The cluster Autoscaler cannot provision nodes, or no existing nodes have the label `disktype=nvme`.
**Mastery Explanation:** The K8s scheduler strictly obeys `nodeSelector`. If the label is absent, the pod cannot be scheduled.

**Q37:** You configure `spark.memory.fraction=0.9` in Standalone mode. During a complex join, the worker node stops responding to heartbeats. Why?
**Answer:** Reserving 90% for Spark leaves only 10% for user data structures and internal metadata. This likely caused immense GC pressure, freezing the JVM and missing heartbeats.
**Mastery Explanation:** The Master assumes missing heartbeats mean death. Poor memory tuning causes GC pauses that look like death.

**Q38:** In Mesos, you implement a Broadcast Hash Join instead of a Sort Merge Join. Why does this drastically improve performance on a distributed Mesos fabric?
**Answer:** Broadcast Hash Join avoids a full all-to-all network shuffle by sending the small table to all executors once, bypassing the heavy network serialization over Mesos.
**Mastery Explanation:** Network I/O is the bottleneck. Broadcasting eliminates the cross-node shuffle phase entirely.

**Q39:** You deploy to YARN. You notice Tungsten's whole-stage code generation is crashing with OOM despite having 16GB of executor memory. What did you forget to tune?
**Answer:** `spark.yarn.executor.memoryOverhead`. Tungsten uses off-heap memory. If the overhead limit is exceeded, YARN kills the container, regardless of on-heap availability.
**Mastery Explanation:** The twist is on-heap vs off-heap. Tungsten is off-heap. YARN monitors total physical memory.

**Q40:** You run Spark on K8s. You delete the Driver pod manually via `kubectl delete pod`. What happens to the Executors?
**Answer:** K8s Garbage Collection automatically terminates the Executor pods because they are owned by the Driver pod via OwnerReferences.
**Mastery Explanation:** Unlike YARN where the RM cleans up, K8s relies on native object ownership. When the parent (Driver) dies, children (Executors) are reaped.

## Part 4: Coding & Debugging Questions (10)

**Q41:** Analyze the following YARN submission:
```bash
spark-submit --master yarn --deploy-mode cluster \
--conf spark.executor.instances=10 \
--conf spark.executor.memory=32G \
--conf spark.executor.cores=10 \
app.py
```
**Bug:** What is the critical performance anti-pattern here?
**Answer:** `spark.executor.cores=10` is too high.
**Mastery Explanation:** 10 cores per executor will cause massive GC thrashing. The recommended max is 5 cores per executor.

**Q42:** Review this K8s Pod Template snippet for Spark:
```yaml
volumes:
  - name: spark-local-dir
    emptyDir: {}
```
**Bug:** What is the risk during a heavy sort operation?
**Answer:** `emptyDir` uses the node's root filesystem. A heavy sort spills to disk and will quickly cause Node Disk Pressure, evicting the pod.
**Mastery Explanation:** `hostPath` to a dedicated large volume is required for heavy K8s shuffles.

**Q43:** You are using Standalone mode:
```python
spark = SparkSession.builder.master("spark://master:7077").getOrCreate()
df = spark.range(100000).cache()
df.count()
```
**Bug:** Another app is submitted but hangs. Why?
**Answer:** The code does not set `spark.cores.max`. It consumes all cores, starving other apps.
**Mastery Explanation:** Standalone mode requires explicit core limits for multi-tenancy.

**Q44:** Debug this Mesos configuration:
```python
spark = SparkSession.builder.master("mesos://zk://zk1:2181/mesos") \
.config("spark.mesos.coarse", "false").getOrCreate()
```
**Bug:** The app takes 5 minutes just to start tasks.
**Answer:** Fine-grained mode (`coarse=false`) is deprecated and creates immense scheduling latency by negotiating per-task resources.
**Mastery Explanation:** Always use `coarse=true` for modern Spark on Mesos.

**Q45:** A YARN job fails with: `Container killed by YARN for exceeding memory limits. 17.1 GB of 17 GB physical memory used.`
**Code:** `spark.executor.memory=16G`, `spark.yarn.executor.memoryOverhead=1G`
**Fix:** How do you resolve this specifically for a Tungsten-heavy workload?
**Answer:** Increase `spark.yarn.executor.memoryOverhead` to `2G` or `4G`.
**Mastery Explanation:** Tungsten allocates heavily off-heap. The default 10% overhead is often insufficient for complex Catalyst generated code.

**Q46:** A K8s Spark job has Dynamic Allocation enabled but fails during scale-down.
**Code:** `spark.dynamicAllocation.enabled=true`
**Fix:** What Spark 3.2+ property must be added for K8s?
**Answer:** `spark.dynamicAllocation.shuffleTracking.enabled=true`
**Mastery Explanation:** K8s lacks a native External Shuffle Service. PVC shuffle tracking is required to safely scale down executors.

**Q47:** Standalone HA fails to recover.
**Code:** `spark.deploy.recoveryMode=FILESYSTEM`
**Fix:** How do you fix this for a multi-node master setup?
**Answer:** Change it to `spark.deploy.recoveryMode=ZOOKEEPER` and provide the ZK URL.
**Mastery Explanation:** FILESYSTEM only works for a single machine restart. Multi-node HA requires ZooKeeper.

**Q48:** YARN job network I/O is maxed out during a shuffle.
**Code:** Uses default serialization.
**Fix:** Implement which two lines of code?
**Answer:** 
`.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")`
`.config("spark.kryo.registrationRequired", "true")` (optional but recommended)
**Mastery Explanation:** Java serialization is bloated. Kryo compresses data, saving massive network bandwidth during YARN shuffles.

**Q49:** K8s executors are being scheduled on slow HDD nodes.
**Code:** `spark.kubernetes.executor.podTemplateFile=template.yaml`
**Fix:** What block must be added to `template.yaml`?
**Answer:**
```yaml
nodeSelector:
  disktype: ssd
```
**Mastery Explanation:** Pod templates are the only way to natively inject K8s node selectors to force executors onto optimized hardware.

**Q50:** You are running PySpark on YARN. The Python workers are consuming too much memory and getting killed.
**Code:** `spark.executor.memory=8G`
**Fix:** What configuration controls the memory allocated to PySpark workers?
**Answer:** `spark.executor.pyspark.memory`
**Mastery Explanation:** PySpark runs Python processes outside the JVM. You must explicitly allocate memory for them or they compete with YARN's off-heap container limits and get killed.
