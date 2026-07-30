# 📝 Elite Assessment: Running Spark with Docker

## Part 1: True/False Questions (10)

**1. Exit code 137 on a Spark executor container implies the JVM threw an `OutOfMemoryError` and shut down.**
*Answer: False*
*Mastery Explanation: Exit code 137 (128 + 9 for SIGKILL) is the canonical signature of a Linux cgroup OOM kill. The kernel terminates the container instantly, completely bypassing the JVM. A JVM `OutOfMemoryError` typically results in exit code 1 or a custom exit code, but rarely 137.*

**2. The `spark.executor.memoryOverhead` configuration acts as a hard limit enforced by the Linux cgroup memory controller.**
*Answer: False*
*Mastery Explanation: `memoryOverhead` is strictly a Spark-level accounting value used to tell the cluster manager how much memory to request. The Linux kernel knows nothing about it; the kernel only enforces the cgroup `memory.max` limit.*

**3. Setting `SPARK_LOCAL_IP=127.0.0.1` in a Docker bridge network prevents Executors and Drivers from communicating across containers.**
*Answer: True*
*Mastery Explanation: If an Executor binds to `127.0.0.1`, it advertises its localhost address to the Driver. When the Driver attempts to send tasks via RPC to `127.0.0.1`, it routes to the Driver's own container loopback interface, resulting in a `CoarseGrainedSchedulerBackend` connection timeout.*

**4. Java 8u191+ and Java 10+ automatically read cgroup memory limits instead of host memory when running inside a container.**
*Answer: True*
*Mastery Explanation: These versions introduced `UseContainerSupport` (enabled by default), which directs the JVM to read limits from `/sys/fs/cgroup` rather than `/proc/meminfo`, ensuring GC thread pools and max heap ergonomics are scaled to the container, not the host.*

**5. Without an init system like `tini` as PID 1, `docker stop` causes the Spark JVM to immediately receive a SIGKILL.**
*Answer: False*
*Mastery Explanation: `docker stop` sends a SIGTERM. Without `tini` to properly forward the signal, the JVM often ignores it or fails to deregister properly. Docker waits 10 seconds (default timeout) and *then* sends the SIGKILL, causing task resubmission storms.*

**6. To properly size a Spark Docker container, the cgroup memory limit must equal `spark.executor.memory` + `spark.executor.memoryOverhead`.**
*Answer: False*
*Mastery Explanation: The formula must also account for Tungsten off-heap allocations (`spark.memory.offHeap.size`) and JVM internals like the code cache (e.g., 256MB). Ignoring these leads to guaranteed OOM kills when off-heap memory is utilized.*

**7. In a multi-stage Dockerfile, placing the application JAR in an earlier layer than the Spark distribution improves build caching.**
*Answer: False*
*Mastery Explanation: Docker caches layers sequentially. Because the application JAR changes frequently (with every deployment), placing it early invalidates all subsequent layers, forcing workers to re-download the massive Spark distribution on every code change.*

**8. Writing Spark shuffle data to the container's default writable layer incurs a performance penalty due to OverlayFS copy-on-write semantics.**
*Answer: True*
*Mastery Explanation: OverlayFS introduces latency on the first write to a file as it copies the page from the read-only layer. For high-throughput shuffle operations, this causes a 30-45% I/O regression compared to direct disk writes via host-mounted volumes.*

**9. In Kubernetes, setting memory `requests` equal to `limits` on a developer machine is the recommended way to maximize cluster efficiency.**
*Answer: False*
*Mastery Explanation: Setting `requests` equal to `limits` creates a Guaranteed QoS class, preventing resource overcommit. On resource-constrained developer machines (like Docker Desktop), this wastes resources. `requests` should be lower than `limits` to allow bin-packing.*

**10. Using `-XX:MaxRAMPercentage=75.0` allows the JVM to dynamically size its heap based on the container's cgroup limit without requiring hardcoded `-Xmx` values.**
*Answer: True*
*Mastery Explanation: `MaxRAMPercentage` calculates the maximum heap size as a percentage of the limit read via `UseContainerSupport`. If the Kubernetes limit is changed, the JVM heap adjusts automatically without updating JVM args.*

---

## Part 2: Multiple Choice Questions (15)

**11. Which exact memory regions must be summed to calculate the minimum safe cgroup memory limit for a Spark executor container?**
A) JVM Heap + Metaspace
B) `spark.executor.memory` + `spark.executor.memoryOverhead`
C) `executor.memory` + `memoryOverhead` + `offHeap.size` + JVM code cache buffer
D) Total Host RAM divided by Executor Cores
*Answer: C*
*Mastery Explanation: A cgroup limit restricts the total RSS of the container. The JVM Heap, Spark overhead (Metaspace/Direct NIO), Tungsten Off-Heap, and internal JVM thread/code caches all consume native memory. Omitting any of these from the cgroup limit triggers an OOM kill.*

**12. When examining a failed Spark container, you see exit code 143. What does this mean?**
A) Cgroup OOM Kill
B) Graceful termination via SIGTERM
C) Application code threw an Exception
D) Segmentation fault in native Hadoop libraries
*Answer: B*
*Mastery Explanation: Exit code 143 is 128 + 15 (SIGTERM). This indicates the container was gracefully stopped, likely by a scheduler scale-down or a `docker stop` command handled correctly by `tini`.*

**13. Why does running a Spark container with an older Java version (e.g., 8u181) often result in immediate OOM crashes?**
A) Old versions lack a Garbage Collector.
B) The JVM reads `/proc/meminfo` (host RAM) and sizes its heap/GC threads too large for the cgroup limit.
C) Older Java versions have a memory leak in the Netty RPC layer.
D) Spark intentionally crashes on unsupported JVMs.
*Answer: B*
*Mastery Explanation: Before `UseContainerSupport` (8u191), the JVM was container-blind. On a 64GB host with a 4GB cgroup limit, the JVM calculates default heap and GC threads for 64GB, rapidly allocating past the 4GB cgroup limit and triggering the kernel OOM killer.*

**14. What is the primary operational symptom of mapping Spark's shuffle directory to the container's OverlayFS writable layer?**
A) The Driver disconnects from Executors.
B) Shuffle write times in the Spark UI increase significantly (30-45% I/O throughput regression).
C) Tasks fail with `NullPointerException`.
D) The Master UI fails to load.
*Answer: B*
*Mastery Explanation: OverlayFS copy-on-write semantics introduce high latency on first page writes. The Spark UI will show elevated "Shuffle Write Time", but without inspecting `/proc/mounts`, the root cause is invisible.*

**15. In a Kubernetes deployment using `emptyDir` for shuffle storage, why is `sizeLimit: 50Gi` critical?**
A) It increases disk write speed.
B) It prevents a runaway shuffle spill from exhausting the host node's entire disk space, causing node failure.
C) It automatically compresses shuffle data.
D) It tells Spark how many partitions to create.
*Answer: B*
*Mastery Explanation: If a massive data skew causes an executor to spill endlessly, an unbounded `emptyDir` will fill the physical host disk, crashing `kubelet` and other pods on the node. `sizeLimit` ensures Kubernetes cleanly evicts the offending pod instead.*

**16. Which JVM flag is essential for verifying that the JVM is correctly interpreting cgroup limits at startup?**
A) `-XX:+PrintGCDetails`
B) `-XX:+PrintFlagsFinal`
C) `-Xlog:gc*`
D) `-XX:+HeapDumpOnOutOfMemoryError`
*Answer: B*
*Mastery Explanation: `-XX:+PrintFlagsFinal` combined with `UseContainerSupport` prints the calculated max heap and logs "container memory limit: Xg", proving the JVM sees the cgroup limit rather than the host's `/proc/meminfo`.*

**17. What is the purpose of the `procps` package in a Spark Docker image?**
A) To process Python UDFs.
B) To provide the `ps` command, which Spark's Worker process uses to monitor executor process health.
C) To compress shuffle files.
D) To enable cgroups v2.
*Answer: B*
*Mastery Explanation: The Spark Worker daemon monitors the lifecycle of Executor JVMs using system utilities like `ps`. If `procps` is missing, the Worker fails to track executor health accurately.*

**18. If a cgroup v2 memory controller hits `memory.high` but not `memory.max`, what happens?**
A) The container is SIGKILL'd.
B) The kernel triggers `kswapd` to forcibly reclaim page cache, causing high latency spikes.
C) The JVM throws an OutOfMemoryError.
D) Spark spills to disk automatically.
*Answer: B*
*Mastery Explanation: `memory.high` is a soft limit. Exceeding it does not kill the process, but forces the kernel into aggressive synchronous memory reclaim (page cache eviction), which stalls execution and creates severe latency spikes during Tungsten sort phases.*

**19. How does Docker BuildKit layer caching optimize Spark application deployments?**
A) By caching the application JAR in memory.
B) By skipping the download and extraction of the heavy JRE and Spark distribution layers if they haven't changed.
C) By compressing the network traffic between nodes.
D) By compiling Python code to C.
*Answer: B*
*Mastery Explanation: By ordering instructions from least-frequently changed (OS/JRE, Spark) to most-frequently changed (App JAR), BuildKit permanently caches the massive base layers on workers. Only the small JAR layer is transmitted on deployment.*

**20. What configuration ensures Executor containers in a docker-compose bridge network advertise reachable addresses to the Driver?**
A) `SPARK_LOCAL_IP={{.Task.Name}}` (Network alias)
B) `SPARK_LOCAL_IP=127.0.0.1`
C) `spark.network.timeout=300s`
D) `-XX:+UseG1GC`
*Answer: A*
*Mastery Explanation: User-defined bridge networks resolve DNS via container names/aliases. The Executor must advertise its container alias so the Driver can route RPC traffic to it. `127.0.0.1` routes to the Driver's own isolated loopback interface.*

**21. In PySpark, reading `/sys/fs/cgroup/memory.max` returning the string "max" indicates:**
A) The container has exhausted its memory limit.
B) The container is unconstrained and has no cgroup memory limit applied.
C) The file system is corrupted.
D) The JVM is using the maximum available host RAM.
*Answer: B*
*Mastery Explanation: In cgroup v2, a limit value of "max" means the controller is not enforcing a memory cap on the container.*

**22. Which command correctly runs an Executor as a non-root user in a Dockerfile?**
A) `USER root`
B) `RUN chmod 777 /`
C) `RUN groupadd -r spark && useradd -r -g spark spark && chown -R spark:spark /opt/spark; USER spark`
D) `ENV USER=spark`
*Answer: C*
*Mastery Explanation: Best practice dictates explicitly creating a group and user, assigning ownership of the Spark directory, and switching context using the `USER` directive. This satisfies Kubernetes PodSecurityPolicies.*

**23. Which component manages the allocation of off-heap memory outside GC control in Spark?**
A) G1GC
B) OverlayFS
C) Tungsten (`sun.misc.Unsafe`)
D) The DAGScheduler
*Answer: C*
*Mastery Explanation: Spark's Tungsten execution engine heavily utilizes off-heap memory for fast sorting and aggregations, completely bypassing the JVM Garbage Collector. This memory directly consumes the cgroup allowance.*

**24. What is the danger of setting `SPARK_WORKER_MEMORY=4g` but setting the container's `deploy.resources.limits.memory` to 4096M?**
A) The Master will reject the Worker registration.
B) The Worker will offer 4g to Executors. Once an Executor claims it, the JVM + Overhead will exceed the 4096M cgroup limit, resulting in an instant OOM kill.
C) Spark will run 10% slower.
D) The Worker will override the cgroup limit.
*Answer: B*
*Mastery Explanation: `SPARK_WORKER_MEMORY` is logical accounting. If an Executor uses the full 4g for heap, the native overhead pushes the total container RSS past 4096M, triggering the kernel OOM killer.*

**25. If a Spark cluster lacks `tini` and an executor is stopped via `docker stop`, what occurs after 10 seconds?**
A) The container reboots.
B) Docker sends a SIGKILL, abruptly terminating the JVM, causing the Driver to mark all tasks on that executor as failed and resubmit them.
C) Spark spills all in-memory RDDs to disk.
D) The Master gracefully unregisters the executor.
*Answer: B*
*Mastery Explanation: Without `tini` to intercept and forward SIGTERM, the JVM does not execute shutdown hooks. Docker times out and sends SIGKILL. The ungraceful death forces the Driver to treat it as a crash and resubmit tasks.*

---

## Part 3: Small Twist Questions (15)

**26. Scenario:** A Spark Dockerfile uses Java 17. A developer adds `ENV JAVA_TOOL_OPTIONS="-XX:-UseContainerSupport"`. What happens?
*Answer: The JVM becomes container-blind again. It reads the host's `/proc/meminfo`, sizes its heap based on the massive host RAM, and gets OOM killed by the cgroup controller almost instantly.*
*Mastery Explanation: Explicitly disabling `UseContainerSupport` undoes the protections added in Java 8u191+, reverting to legacy behavior.*

**27. Scenario:** You set `spark.executor.memory=4g` and the cgroup limit to exactly 4g. You explicitly set `spark.memory.offHeap.enabled=false`. Will the container survive?
*Answer: No, it will still fail with exit code 137.*
*Mastery Explanation: Even without off-heap, the JVM requires native memory for metaspace, JIT code cache, and thread stacks. `executor.memory` only covers the heap. The total RSS will exceed 4g.*

**28. Scenario:** A Docker Compose file maps a named volume `spark-shuffle` to `/tmp/spark-shuffle`, but does not use a host bind mount. Does this bypass OverlayFS?
*Answer: Yes.*
*Mastery Explanation: Docker named volumes bypass the container's copy-on-write OverlayFS writable layer and write directly to a local directory in `/var/lib/docker/volumes/` on the host.*

**29. Scenario:** The Kubernetes manifest defines `requests.memory="4Gi"` and `limits.memory="2Gi"`. What happens?
*Answer: The manifest is invalid and the Pod will not be scheduled.*
*Mastery Explanation: Kubernetes strictly enforces that a container's `requests` cannot exceed its `limits`.*

**30. Scenario:** You set `-XX:MaxRAMPercentage=100.0` in your Docker deployment. What is the risk?
*Answer: Guaranteed cgroup OOM kill (exit code 137).*
*Mastery Explanation: Setting heap to 100% of the container limit leaves 0 bytes for JVM metaspace, thread stacks, direct NIO, or Tungsten off-heap. Any native allocation will breach the cgroup limit.*

**31. Scenario:** You change the layer order in your Dockerfile to: `COPY app.jar` -> `RUN apt-get install procps` -> `ADD spark.tgz`. What is the impact?
*Answer: Massive increase in deployment time.*
*Mastery Explanation: `app.jar` changes every build. This invalidates the layer cache for `apt-get` and the massive Spark `.tgz` download, forcing workers to re-download hundreds of megabytes on every minor code change.*

**32. Scenario:** A PySpark script runs inside an Executor. It reads `/sys/fs/cgroup/memory/memory.limit_in_bytes` and receives the value `9223372036854771712`. What does this mean?
*Answer: The container is running under cgroup v1 and has NO memory limit applied (unconstrained).*
*Mastery Explanation: This specific value (~2^63) is the cgroup v1 standard representation for an unlimited container.*

**33. Scenario:** `SPARK_WORKER_CORES=4` is configured, but the Docker cgroup `cpu.max` is set to `2.0`. Does Spark fail?
*Answer: No, but it suffers severe CPU throttling.*
*Mastery Explanation: Spark will schedule 4 parallel tasks, but the kernel's Completely Fair Scheduler (CFS) will aggressively throttle the container to 50% CPU time, extending task duration without crashing.*

**34. Scenario:** A developer looks at the Spark UI and notices "Shuffle Write Time" spiked 45% after migrating from YARN to Docker, but there are no crashes. What is the twist?
*Answer: The shuffle directory is mapped to the default container filesystem (OverlayFS) instead of a volume.*
*Mastery Explanation: The copy-on-write penalty silently degrades I/O performance without causing application failures.*

**35. Scenario:** The Kubernetes `emptyDir` lacks a `sizeLimit`. A severe data skew causes 100GB of spill. What happens?
*Answer: The pod continues writing until the physical Kubernetes worker node runs out of disk space.*
*Mastery Explanation: Without `sizeLimit`, `emptyDir` consumes the host's root filesystem, potentially crashing the `kubelet` and bringing down the entire node.*

**36. Scenario:** A container exits with code 1 instead of 137. Is this a memory limit issue?
*Answer: No, this is an application-level failure.*
*Mastery Explanation: Exit code 1 indicates an exception was thrown in user code or the driver, not a kernel-level cgroup SIGKILL.*

**37. Scenario:** You set `spark.executor.memoryOverhead=3g` on a 4g heap, but the Docker cgroup limit is 5g. What happens?
*Answer: Spark calculates 7g total needed. If it allocates past 5g, it is OOM killed.*
*Mastery Explanation: `memoryOverhead` does not magically increase the cgroup limit. The cgroup limit is a hard kernel boundary; if the sum of actual allocations exceeds it, the kernel terminates the process.*

**38. Scenario:** A developer uses `FROM alpine:latest` for the Spark base image to save space. What is the hidden danger?
*Answer: Alpine uses `musl libc` instead of `glibc`.*
*Mastery Explanation: Many native Hadoop libraries (like snappy compression or HDFS native clients) are compiled against `glibc` and will fail to load or segfault on Alpine Linux.*

**39. Scenario:** The application sets both `-Xmx4g` and `-XX:MaxRAMPercentage=75.0`. The container limit is 8g. What size is the heap?
*Answer: 4g.*
*Mastery Explanation: Explicit `-Xmx` (MaxHeapSize) overrides dynamic sizing parameters like `MaxRAMPercentage`, defeating the purpose of container-aware ergonomics.*

**40. Scenario:** The `tini` init system is present, but the Dockerfile `ENTRYPOINT` uses shell form: `ENTRYPOINT /usr/bin/tini -- spark-class ...`. What happens on `docker stop`?
*Answer: Graceful shutdown fails; tasks are resubmitted.*
*Mastery Explanation: Shell form executes as `/bin/sh -c`, meaning the shell is PID 1, not `tini`. The shell absorbs the SIGTERM and does not forward it, leading to a SIGKILL timeout.*

---

## Part 4: Coding & Debugging Questions (10)

**41. Debug this Docker Compose snippet:**
```yaml
  spark-worker:
    environment:
      - SPARK_WORKER_MEMORY=8g
    deploy:
      resources:
        limits:
          memory: 8192M
```
*Correction & Explanation:* The cgroup limit (`8192M`) exactly matches the worker memory offering. When an executor claims 8g for heap, the native overhead will exceed 8192M. The cgroup limit must be raised to at least `8g + (8g * 0.1) + 256m ≈ 9.2g` to avoid an exit code 137 OOM kill.

**42. Debug this Dockerfile layer cache:**
```dockerfile
FROM openjdk:17-slim
COPY target/my-app.jar /opt/
RUN apt-get update && apt-get install -y procps
ADD spark-3.5.0.tgz /opt/
```
*Correction & Explanation:* The heavily-changed `my-app.jar` is copied before stable dependencies (`procps`, `spark.tgz`). Every code change invalidates all subsequent layers, forcing massive re-downloads. Fix: Move the `COPY` instruction to the very end of the Dockerfile.

**43. Identify the missing component in this Kubernetes manifest that prevents dynamic JVM scaling:**
```yaml
env:
  - name: JAVA_TOOL_OPTIONS
    value: "-XX:+UseG1GC"
resources:
  limits:
    memory: "4Gi"
```
*Correction & Explanation:* It is missing `-XX:MaxRAMPercentage=75.0` (or similar). Without it, the JVM relies on default ergonomics (often 25% of the container limit), resulting in a severely undersized 1GB heap.

**44. Debug the RPC Timeout:**
```yaml
  spark-master:
    environment:
      - SPARK_LOCAL_IP=localhost
```
*Correction & Explanation:* `localhost` resolves to `127.0.0.1`. The Master will advertise `127.0.0.1` to Workers. When Workers try to connect, they route to their own internal loopback, failing to reach the Master. Fix: `SPARK_LOCAL_IP=spark-master` (the container DNS name).

**45. Calculate the proper Kubernetes `limits.memory` for the following Spark configuration:**
`spark.executor.memory = 10g`
`spark.executor.memoryOverhead = 2g`
`spark.memory.offHeap.size = 3g`
*Correction & Explanation:* Heap (10g) + Overhead (2g) + Off-Heap (3g) + JVM Code Cache buffer (0.25g) = `15.25g`. The limit should be set to `16Gi` for safety.

**46. Fix the OverlayFS degradation in this compose configuration:**
```yaml
    volumes:
      - /tmp/spark-shuffle
```
*Correction & Explanation:* This defines an anonymous volume, which Docker places in `/var/lib/docker/volumes`. While it bypasses OverlayFS, it is not explicitly mapped to a fast NVMe host path, potentially writing to a slow OS disk. Fix: Use a bind mount `- /mnt/nvme-disk/spark-shuffle:/tmp/spark-shuffle`.

**47. Debug the PID 1 Issue in this Dockerfile:**
```dockerfile
FROM eclipse-temurin:17
# ... (Spark setup) ...
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker"]
```
*Correction & Explanation:* The container lacks an init system. `spark-class` becomes PID 1. On `docker stop`, it receives SIGTERM but JVMs often handle PID 1 signals poorly. Fix: `RUN apt-get install tini` and add `ENTRYPOINT ["/usr/bin/tini", "--"]`.

**48. Debug this PySpark Diagnostic script snippet:**
```python
raw = pathlib.Path("/sys/fs/cgroup/memory.max").read_text().strip()
cgroup_limit_bytes = int(raw)
```
*Correction & Explanation:* If the cgroup v2 limit is unbounded, `memory.max` contains the string literal `"max"`. Calling `int("max")` will throw a `ValueError` and crash the diagnostic script. Fix: `cgroup_limit_bytes = -1 if raw == "max" else int(raw)`.

**49. Why does this Kubernetes manifest risk node failure?**
```yaml
  volumes:
    - name: spark-local-dir
      emptyDir: {}
```
*Correction & Explanation:* An unconstrained `emptyDir` allows shuffle spill to consume the physical node's disk. Fix: Add `sizeLimit: 50Gi` to force Pod eviction instead of node starvation.

**50. Identify the conflict in these JVM flags:**
```yaml
env:
  - name: JAVA_TOOL_OPTIONS
    value: "-XX:-UseContainerSupport -XX:MaxRAMPercentage=80.0"
```
*Correction & Explanation:* `UseContainerSupport` is explicitly disabled (`-XX:-UseContainerSupport`). Therefore, the JVM reads host memory. `MaxRAMPercentage=80.0` will set the heap to 80% of the *host's* RAM, entirely ignoring the container limit, guaranteeing a cgroup OOM kill on any shared node. Fix: Change to `-XX:+UseContainerSupport`.
