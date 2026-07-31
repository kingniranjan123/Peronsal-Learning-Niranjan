# 🔥 Master Class: Running Spark with Docker

## Overview

Apache Spark was designed in an era of bare-metal clusters and static resource managers like YARN and Mesos. Docker fundamentally disrupts that model by abstracting the host OS into isolated, reproducible, portable units of compute. When you containerize Spark, you gain three critical properties that bare-metal deployments cannot provide: **environment reproducibility** (the exact same JVM version, native libraries, and Python environment on every node), **resource isolation** (Linux cgroups enforce hard CPU and memory limits so one runaway executor cannot starve another), and **deployment portability** (the same image runs on a developer's laptop, a CI pipeline, and a production Kubernetes cluster without change).

The challenge is that Spark's execution model — a long-lived JVM Driver coordinating many short-lived Executor JVMs — does not map trivially onto Docker's single-process-per-container philosophy. Each Spark executor is a full JVM process that allocates heap memory (controlled by `spark.executor.memory`), off-heap memory (Tungsten's `sun.misc.Unsafe` allocations via `spark.memory.offHeap.size`), and shuffle spill disk. When cgroup memory limits are set incorrectly, the Linux kernel OOM killer terminates the container silently, producing a cryptic `ExecutorLostFailure` in the Driver with no stack trace. Getting the interplay between JVM heap, Tungsten off-heap, overhead memory (`spark.executor.memoryOverhead`), and the container's cgroup limit right is the single most common production failure point.

Docker also introduces a layered filesystem (OverlayFS on modern Linux kernels) that has direct implications for Spark's shuffle and spill I/O. Shuffle data written to a container's writable layer goes through OverlayFS copy-on-write semantics, adding latency on every first write. Production deployments always mount shuffle directories as Docker volumes backed by a host path, bypassing OverlayFS entirely and recovering the full sequential write throughput of the underlying disk.

---

## 🏗️ Architectural Deep Dive

### How It Works Under the Hood

When a Spark application runs inside Docker, the Driver JVM starts inside one container and registers with either the built-in standalone cluster manager (a separate Master container) or an external scheduler like Kubernetes. The cluster manager allocates Executor containers from the available worker pool. Each Executor container is an independent Linux namespace with its own PID, network, and mount namespaces, but shares the host kernel. The Linux kernel's **cgroup v2** subsystem enforces resource limits: `memory.max` for the total RSS + page cache limit, `cpu.max` for CPU quota across a scheduling period, and `blkio.weight` for disk I/O priority.

The JVM inside each Executor container does not natively understand cgroup limits — it reads `/proc/meminfo` for total system RAM, which reflects the host's memory, not the container's limit. Before Java 8u191, this caused the JVM to size its internal structures (GC thread count, JIT compiler thread count, heap ergonomics) based on the full host RAM, leading to immediate OOM kills when the cgroup limit was hit. **Java 8u191+ and Java 10+ added `UseContainerSupport`** (enabled by default), which makes the JVM read cgroup memory and CPU limits directly from `/sys/fs/cgroup` and size itself accordingly. Always verify this is active by checking executor logs for `container memory limit: Xg` at JVM startup.

Spark's Tungsten engine allocates memory in two zones: the JVM heap (managed by the garbage collector) and off-heap (raw `ByteBuffer` or `Unsafe` allocations outside GC control). Both zones count toward the container's cgroup memory limit. The formula for the cgroup memory limit that must be set on the container is: `spark.executor.memory` + `spark.executor.memoryOverhead` + `spark.memory.offHeap.size` + a 10% safety buffer. Setting the cgroup limit to exactly `spark.executor.memory` is the most common misconfiguration, and it causes the kernel OOM killer to fire the moment Tungsten starts using off-heap memory for sort operations or Window functions.

Image layering strategy directly impacts cluster startup latency. Docker images are built in layers. If the JRE base layer, the Spark distribution layer, and the application JAR layer are all in one `RUN` command, any change to the application JAR invalidates and re-downloads the entire multi-gigabyte image on every worker node. The correct approach is to separate layers by change frequency: `FROM openjdk:17-slim` → `ADD spark-3.x.tgz` → `COPY app.jar`. The first two layers are cached permanently on workers; only the 50MB application JAR layer is re-pulled on each deployment, reducing cold-start time from 10+ minutes to under 30 seconds.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Docker Host (Linux Kernel)                          │
│                                                                              │
│  ┌──────────────────────┐     ┌──────────────────────────────────────────┐  │
│  │   Driver Container   │     │          Worker Node (Executor Pool)      │  │
│  │  ┌────────────────┐  │     │  ┌─────────────┐  ┌─────────────┐       │  │
│  │  │  SparkContext  │──┼─────┼─▶│  Executor-1 │  │  Executor-2 │  ...  │  │
│  │  │  DAGScheduler  │  │     │  │  JVM Heap   │  │  JVM Heap   │       │  │
│  │  │  TaskScheduler │  │     │  │  Off-Heap   │  │  Off-Heap   │       │  │
│  │  └────────────────┘  │     │  └──────┬──────┘  └──────┬──────┘       │  │
│  │  cgroup limit:       │     │         │                 │              │  │
│  │  driver.memory +     │     │  cgroup limit:            │              │  │
│  │  memoryOverhead      │     │  executor.memory +         │              │  │
│  └──────────────────────┘     │  overhead + offHeap        │              │  │
│                               │         │                  │              │  │
│  ┌──────────────────────┐     │         ▼                  ▼              │  │
│  │  Standalone Master   │     │  ┌───────────────────────────────────┐   │  │
│  │  Container           │     │  │  Host Volume (shuffle/spill disk) │   │  │
│  │  (port 7077)         │     │  │  Bypasses OverlayFS copy-on-write │   │  │
│  └──────────────────────┘     │  └───────────────────────────────────┘   │  │
│                               └──────────────────────────────────────────┘  │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  OverlayFS Image Layers                                               │   │
│  │  Layer 4 (R/W): Container writable layer  ← Shuffle/spill AVOID this │   │
│  │  Layer 3 (RO):  app.jar                   ← Changes per deployment   │   │
│  │  Layer 2 (RO):  spark-3.5.0 distribution  ← Cached on workers       │   │
│  │  Layer 1 (RO):  openjdk:17-slim base      ← Cached permanently      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Internal Components

- **cgroup v2 Memory Controller:** Enforces `memory.max` (hard limit that triggers OOM kill) and `memory.high` (soft limit that triggers memory reclaim). Spark's total container footprint must include JVM heap, metaspace, off-heap Tungsten buffers, and the JVM's internal thread stacks (~512KB per thread × executor cores).
- **UseContainerSupport (JVM flag):** Enabled by default in Java 8u191+. Makes the JVM query `/sys/fs/cgroup/memory/memory.limit_in_bytes` (cgroup v1) or `/sys/fs/cgroup/memory.max` (cgroup v2) to correctly size GC ergonomics, heap defaults, and CPU thread pools relative to container limits, not host limits.
- **OverlayFS Writable Layer:** Each container has a private copy-on-write writable layer managed by OverlayFS. First writes to a file require copying the page from the lower read-only layer, adding latency. Shuffle spill data must be redirected to host-mounted volumes (`-v /mnt/fast-disk:/spark/shuffle`) to avoid this penalty entirely.
- **Docker BuildKit Layer Cache:** Each `RUN`, `COPY`, and `ADD` instruction in a Dockerfile creates a new immutable layer. BuildKit caches layers by their instruction hash; changing a later instruction only invalidates layers from that point forward, making image rebuild and distribution to workers proportional to the size of changed layers only.

---

## ⚠️ Critical Concepts & Common Pitfalls

### The Memory Overhead Misconfiguration Trap

The single most dangerous Spark-on-Docker misconfiguration is setting the Docker container memory limit equal to `spark.executor.memory`. When `spark.executor.memory=4g`, the JVM heap is 4GB. However, the JVM itself requires additional native memory for metaspace (class metadata, ~200–400MB for Spark's classpath), code cache (JIT-compiled native code, ~256MB), and direct NIO buffers. Spark adds `spark.executor.memoryOverhead` (default: `max(executor_memory * 0.1, 384MB)`) to account for this. If `offHeap` is enabled, Tungsten claims another `spark.memory.offHeap.size` bytes of native memory. All of this counts toward the cgroup limit.

The failure is silent and lethal: the Linux kernel OOM killer sends `SIGKILL` to the container, bypassing the JVM's shutdown hooks. The Spark Driver receives `ExecutorLostFailure(executor 2, exit code: 137)`. Exit code 137 = 128 + 9 (SIGKILL), which is the canonical signature of a cgroup OOM kill. The correct formula: **Container limit = `executor.memory` + `memoryOverhead` + `offHeap.size` + 256MB (code cache buffer)**. For a 4g executor with defaults and no off-heap: container limit = 4g + 768MB + 256MB = **~5.1GB**.

### Network Mode and Spark's Internal Communication

Spark Executors and the Driver communicate over RPC (Netty-based, port 7078 by default) and must be able to resolve each other's hostnames bidirectionally. Docker's default bridge network uses NAT, and containers advertise their internal Docker network IPs to the Driver. If the Driver is on the host network or in a different subnet, Executor RPC connections fail with `org.apache.spark.SparkException: Could not find CoarseGrainedSchedulerBackend` after the `spark.network.timeout` (default 120s).

In docker-compose multi-container setups, all Spark services must be on the same user-defined bridge network. User-defined networks provide DNS resolution by container name, so `spark://spark-master:7077` resolves correctly from any executor container. The `SPARK_LOCAL_IP` and `SPARK_PUBLIC_DNS` environment variables in the Spark container must be set to the container's network alias (not `localhost` or `127.0.0.1`) so that Executors advertise an address the Driver can actually reach. Misconfigurations here produce tasks that appear scheduled in the Spark UI but never produce output.

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|-----------|---------|-------|
| Container cold start | O(image_layers × pull_size) | No | Cached layers skip download; only new layers are pulled. Target <30s with proper layering. |
| OverlayFS first write | O(page_size) | No | Copy-on-write penalty on first write to any page. Shuffle dirs must use host volumes. |
| cgroup memory reclaim (`memory.high`) | O(working_set) | No | Triggers `kswapd` to reclaim page cache; adds latency spikes to Tungsten sort phases. |
| Cross-container shuffle | O(records × serialized_size) | Yes | Same as bare-metal if host-network or host volumes used; 15-30% slower on bridge NAT due to iptables overhead. |

---

## 💻 Code Examples

### Example 1: Production-Grade Spark Dockerfile with Correct Layer Ordering

> **What this demonstrates:** How to structure a multi-stage Dockerfile so that the JRE and Spark distribution layers are cached permanently on worker nodes, and only the application JAR is re-pushed on each deployment, while also correctly configuring `UseContainerSupport` and non-root execution.

```dockerfile
# ─── Stage 1: Build stage — resolves dependencies, does not ship in final image ───
FROM maven:3.9-eclipse-temurin-17 AS builder
WORKDIR /build
COPY pom.xml .
# Download all dependencies into the Docker layer cache BEFORE copying source.
# If pom.xml is unchanged, this entire layer is skipped on next build.
RUN mvn dependency:go-offline -q
COPY src/ ./src/
# Package the application, skipping tests (tests run in CI, not image build)
RUN mvn package -DskipTests -q

# ─── Stage 2: Minimal runtime image ───
# openjdk:17-slim is ~220MB vs openjdk:17 at ~470MB; metaspace and code cache
# are not affected by the image choice — only the base system libraries differ.
FROM eclipse-temurin:17-jre-jammy

# Layer 1 (most stable): OS packages required by Spark and Hadoop native libs
RUN apt-get update && apt-get install -y --no-install-recommends \
    procps \           
    # procps provides 'ps', used by Spark's worker process monitoring
    tini \             
    # tini is a minimal init system that correctly forwards SIGTERM to the JVM,
    # allowing graceful executor shutdown instead of abrupt SIGKILL on stop.
    && rm -rf /var/lib/apt/lists/*

# Layer 2 (stable, changes ~quarterly): Spark distribution
# ADD automatically extracts .tgz archives, saving an extra RUN layer
ARG SPARK_VERSION=3.5.1
ARG HADOOP_VERSION=3
ADD https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /opt/
RUN ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Layer 3 (changes per deployment): Application JAR — only this layer is
# re-downloaded on worker nodes when the application changes.
COPY --from=builder /build/target/my-spark-app.jar /opt/spark/jars/

# Layer 4: Configuration — separate from JAR to avoid invalidating JAR layer
# on config-only changes.
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Run as non-root: Spark workers do not need root; running as root in Docker
# is a security risk and violates PodSecurityPolicy in Kubernetes clusters.
RUN groupadd -r spark && useradd -r -g spark spark \
    && chown -R spark:spark /opt/spark
USER spark

# Shuffle and spill dirs are declared as volumes — Docker will mount these
# as host paths in docker-compose/Kubernetes, bypassing OverlayFS.
VOLUME ["/opt/spark/work", "/tmp/spark-shuffle"]

# tini as PID 1 ensures correct signal handling; without it, SIGTERM from
# 'docker stop' is not forwarded to the JVM and Spark cannot deregister executors.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.worker.Worker", "spark://spark-master:7077"]
```

> **Mastery Note:** The multi-stage build ensures the Maven compiler and full JDK never ship in the runtime image, reducing the final image from ~700MB to ~380MB. The `tini` init process as PID 1 is non-negotiable in production: without it, `docker stop` sends SIGTERM to the Spark JVM, which is intercepted by tini and forwarded correctly, allowing the executor to deregister from the Driver, persist shuffle data markers, and release BlockManager resources gracefully. Without tini, Docker waits 10 seconds (the default stop timeout) then sends SIGKILL, causing the Driver to mark all tasks on that executor as failed and re-submit them. The `VOLUME` declarations ensure that when orchestration tools mount host paths for shuffle directories, the intent is explicit and documented.

---

### Example 2: docker-compose Standalone Cluster with cgroup Memory Limits

> **What this demonstrates:** A complete docker-compose topology for a Spark standalone cluster with explicit cgroup memory limits calculated using the correct formula, shared user-defined networks for DNS resolution, and host-volume shuffle mounts.

```yaml
# docker-compose.yml
# Requires Docker Compose v2.x+ for 'deploy.resources' syntax support.
# Launch with: docker compose up -d --scale spark-worker=3

version: "3.8"

# Named volume for shuffle data — maps to a host directory, bypassing OverlayFS.
# In production, replace with a fast NVMe host path via a bind mount.
volumes:
  spark-shuffle:
    driver: local

networks:
  spark-net:
    # User-defined bridge network provides DNS resolution by container name.
    # Containers on this network resolve "spark-master" to the master container IP.
    driver: bridge

services:
  spark-master:
    image: my-org/spark:3.5.1
    container_name: spark-master
    # Override CMD to start the Master process instead of default Worker
    command: /opt/spark/bin/spark-class org.apache.spark.deploy.master.Master
    ports:
      - "8080:8080"   # Spark Master Web UI
      - "7077:7077"   # Spark cluster communication port
    environment:
      # SPARK_LOCAL_IP must be the container's network alias, not 127.0.0.1.
      # This is the address the Master advertises to Workers and the Driver.
      - SPARK_LOCAL_IP=spark-master
      - SPARK_MASTER_HOST=spark-master
    networks:
      - spark-net
    deploy:
      resources:
        limits:
          # Master is not a compute node; 1g heap + 512MB overhead is sufficient.
          memory: 1536M
          cpus: "1.0"

  spark-worker:
    image: my-org/spark:3.5.1
    # The default CMD in our Dockerfile already starts the Worker process.
    depends_on:
      - spark-master
    environment:
      - SPARK_LOCAL_IP={{.Task.Name}}
        # {{.Task.Name}} resolves to the container's DNS name in Swarm mode.
        # For plain Compose, use the container hostname instead.
      - SPARK_WORKER_CORES=2
      - SPARK_WORKER_MEMORY=4g
        # SPARK_WORKER_MEMORY tells the Spark Worker how much memory to offer.
        # The cgroup limit (below) must be LARGER than this value to account
        # for JVM overhead, off-heap, and Tungsten buffers.
    volumes:
      # Bind-mount the host's fast disk to the shuffle directory.
      # This completely bypasses OverlayFS copy-on-write overhead.
      - /mnt/nvme/spark-shuffle:/tmp/spark-shuffle
      - /mnt/nvme/spark-work:/opt/spark/work
    networks:
      - spark-net
    deploy:
      resources:
        limits:
          # Formula: SPARK_WORKER_MEMORY (4g)
          #        + memoryOverhead (max(4g*0.1, 384MB) = 512MB)
          #        + JVM code cache buffer (256MB)
          #        = 4864MB → round up to 5120MB (5g) for safety.
          # Setting this to 4096M is the #1 mistake and produces exit code 137.
          memory: 5120M
          # 2 CPUs for 2 cores; Docker enforces this via cpu.max cgroup.
          cpus: "2.0"
```

> **Mastery Note:** The `deploy.resources.limits.memory` value is enforced by the Linux kernel's cgroup `memory.max` controller, which is a hard limit. When the container's RSS + page cache exceeds `memory.max`, the kernel OOM killer fires immediately with no warning, producing exit code 137 on the container. Spark's `spark.executor.memoryOverhead` does not protect against this — it is a Spark-level accounting value that tells the cluster manager how much to request, not an enforcement mechanism. The only protection is setting the Docker memory limit large enough to encompass all JVM native memory regions. The host-volume shuffle mounts are the other critical element: without them, every shuffle write goes through OverlayFS, and in benchmarks against a 100GB shuffle dataset, OverlayFS write throughput is 30–45% lower than direct disk writes due to the copy-on-write page fault overhead.

---

### Example 3: Kubernetes Deployment via Docker Desktop with Resource Requests and Limits

> **What this demonstrates:** A production-pattern Kubernetes manifest for running a Spark Driver in cluster mode, using `spark-submit` with Kubernetes as the cluster manager. This shows the correct Pod template with `requests` vs `limits` asymmetry and the `UseContainerSupport` verification pattern.

```yaml
# spark-driver-pod.yaml
# Submit with: spark-submit \
#   --master k8s://https://kubernetes.docker.internal:6443 \
#   --deploy-mode cluster \
#   --conf spark.kubernetes.container.image=my-org/spark:3.5.1 \
#   --conf spark.kubernetes.driver.podTemplateFile=spark-driver-pod.yaml \
#   --conf spark.kubernetes.executor.podTemplateFile=spark-executor-pod.yaml \
#   local:///opt/spark/jars/my-spark-app.jar

apiVersion: v1
kind: Pod
metadata:
  name: spark-driver
  labels:
    app: spark
    role: driver
spec:
  # serviceAccountName must have RBAC permissions to create/delete Executor pods.
  # Spark's Kubernetes mode creates one pod per executor dynamically.
  serviceAccountName: spark-service-account
  restartPolicy: Never   # Driver pod must not restart; Spark handles re-submission.

  containers:
    - name: spark-driver
      image: my-org/spark:3.5.1
      imagePullPolicy: IfNotPresent  # Use cached image on Docker Desktop

      env:
        # Verify UseContainerSupport is active at startup.
        # This JVM flag causes Java to log "container memory limit: Xg" on startup,
        # confirming cgroup-aware heap sizing is in effect.
        - name: JAVA_TOOL_OPTIONS
          value: >-
            -XX:+UseContainerSupport
            -XX:MaxRAMPercentage=75.0
            -XX:+UseG1GC
            -XX:G1HeapRegionSize=16m
            -XX:+PrintFlagsFinal
            # MaxRAMPercentage=75 means the JVM will use 75% of the container's
            # cgroup memory limit as the max heap, automatically calculated.
            # For a 4Gi container limit: max heap ≈ 3072MB. No -Xmx needed.

      resources:
        requests:
          # Requests are used by Kubernetes scheduler for bin-packing.
          # Setting requests < limits allows overcommit on the node.
          memory: "2Gi"
          cpu: "500m"    # 500 millicores = 0.5 CPU
        limits:
          # Limits are enforced by cgroups. UseContainerSupport reads THIS value.
          # JVM MaxRAMPercentage=75 of 4Gi limit = 3Gi max heap.
          memory: "4Gi"
          cpu: "2000m"   # 2 full CPUs available for JIT, GC threads, Netty I/O

      volumeMounts:
        # Shuffle data written to emptyDir is on the node's local disk, not
        # OverlayFS. Kubernetes emptyDir is backed by the node's filesystem.
        - name: spark-local-dir
          mountPath: /tmp/spark-local

  volumes:
    - name: spark-local-dir
      emptyDir:
        # SizeLimit prevents a runaway sort/spill from filling the node disk.
        # When the limit is exceeded, Kubernetes evicts the pod cleanly instead
        # of the node running out of disk space.
        sizeLimit: 50Gi
```

> **Mastery Note:** The asymmetry between `requests` and `limits` is intentional and critical for cluster efficiency on Docker Desktop Kubernetes (which typically runs on a VM with 4–8GB RAM). `requests` determine pod scheduling; `limits` determine cgroup enforcement. Setting `requests == limits` (Guaranteed QoS class) prevents overcommit but wastes resources on a developer machine. `MaxRAMPercentage=75.0` delegates JVM heap sizing to the cgroup limit, making the manifest self-adjusting: if you change the `limits.memory`, the heap scales automatically without changing the JVM flags. Without `UseContainerSupport`, the JVM reads the Docker Desktop VM's total RAM (e.g., 8GB) and sizes GC thread pools for an 8GB heap, spawning far too many parallel GC threads for a 4Gi-limited container, which wastes CPU time on unnecessary GC coordination.

---

### Example 4: Diagnosing cgroup OOM Kills and OverlayFS Shuffle Degradation at Runtime

> **What this demonstrates:** A diagnostic PySpark script and shell command set that detects cgroup memory misconfiguration, validates that shuffle directories are on host volumes (not OverlayFS), and measures the actual container memory headroom available to Tungsten's off-heap allocator.

```python
# diagnostics/spark_docker_health_check.py
# Run as: spark-submit --master spark://spark-master:7077 \
#           spark_docker_health_check.py
#
# This script validates the Docker memory configuration on every executor node
# and reports whether shuffle directories are on OverlayFS or host volumes.

from pyspark.sql import SparkSession
from pyspark import TaskContext
import subprocess, os, pathlib

spark = SparkSession.builder \
    .appName("DockerHealthCheck") \
    .config("spark.executor.memory", "4g") \
    .config("spark.executor.memoryOverhead", "768m") \
    # offHeap enabled for Tungsten sort — must be included in cgroup limit calc
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "1g") \
    .getOrCreate()

sc = spark.sparkContext

def check_executor_environment(_):
    """
    This function runs ON THE EXECUTOR inside the container.
    It reads cgroup limits directly from the kernel filesystem and checks
    whether the shuffle directory is on OverlayFS or a real filesystem.
    """
    ctx = TaskContext.get()
    executor_id = ctx.partitionId()

    # ── 1. Read cgroup v2 memory limit ──────────────────────────────────────
    cgroup_v2_path = pathlib.Path("/sys/fs/cgroup/memory.max")
    cgroup_v1_path = pathlib.Path("/sys/fs/cgroup/memory/memory.limit_in_bytes")

    if cgroup_v2_path.exists():
        raw = cgroup_v2_path.read_text().strip()
        # "max" means no cgroup limit is set — container is unconstrained.
        cgroup_limit_bytes = -1 if raw == "max" else int(raw)
        cgroup_version = "v2"
    elif cgroup_v1_path.exists():
        cgroup_limit_bytes = int(cgroup_v1_path.read_text().strip())
        cgroup_version = "v1"
    else:
        cgroup_limit_bytes = -1
        cgroup_version = "unknown"

    # ── 2. Calculate required container memory vs actual cgroup limit ────────
    # These must match what is set in docker-compose/Kubernetes manifest.
    executor_heap_mb  = 4 * 1024          # spark.executor.memory
    overhead_mb       = 768               # spark.executor.memoryOverhead
    off_heap_mb       = 1 * 1024          # spark.memory.offHeap.size
    code_cache_mb     = 256               # JVM JIT code cache buffer
    required_mb       = executor_heap_mb + overhead_mb + off_heap_mb + code_cache_mb
    required_bytes    = required_mb * 1024 * 1024

    headroom_bytes = cgroup_limit_bytes - required_bytes if cgroup_limit_bytes > 0 else -1

    # ── 3. Check if shuffle dir is on OverlayFS ──────────────────────────────
    shuffle_dir = "/tmp/spark-shuffle"
    os.makedirs(shuffle_dir, exist_ok=True)

    # /proc/mounts shows the filesystem type for each mount point.
    mounts = pathlib.Path("/proc/mounts").read_text()
    shuffle_on_overlay = False
    for line in mounts.splitlines():
        parts = line.split()
        if len(parts) >= 3 and parts[1] == shuffle_dir and parts[2] == "overlay":
            # overlay filesystem type means OverlayFS — shuffle writes will be
            # subject to copy-on-write overhead. THIS IS A MISCONFIGURATION.
            shuffle_on_overlay = True
            break

    # ── 4. Validate UseContainerSupport is active ────────────────────────────
    # If UseContainerSupport is off, JVM reads host /proc/meminfo, not cgroup.
    # We check the JVM flags that are actually in effect at runtime.
    jvm_flags = subprocess.run(
        ["java", "-XX:+PrintFlagsFinal", "-version"],
        capture_output=True, text=True, stderr=subprocess.STDOUT
    ).stdout
    container_support_active = "UseContainerSupport" in jvm_flags and \
                               "true" in [l for l in jvm_flags.splitlines()
                                          if "UseContainerSupport" in l][0].lower()

    return {
        "executor_id":              executor_id,
        "cgroup_version":           cgroup_version,
        "cgroup_limit_gb":          round(cgroup_limit_bytes / (1024**3), 2) if cgroup_limit_bytes > 0 else "UNCONSTRAINED",
        "required_gb":              round(required_bytes / (1024**3), 2),
        "headroom_mb":              round(headroom_bytes / (1024**2), 1) if headroom_bytes >= 0 else "N/A",
        "oom_risk":                 headroom_bytes < 0 if cgroup_limit_bytes > 0 else False,
        "shuffle_on_overlayfs":     shuffle_on_overlay,
        "container_support_active": container_support_active,
    }

# Run the diagnostic on every partition (one per executor core)
num_executors = int(spark.conf.get("spark.executor.instances", "4"))
results = sc.parallelize(range(num_executors * 2), num_executors * 2) \
             .map(check_executor_environment) \
             .collect()

print("\n=== Spark Docker Health Check ===")
for r in results:
    oom_flag    = "🔴 OOM RISK"  if r["oom_risk"]                else "🟢 OK"
    overlay_flag = "🔴 OVERLAYFS" if r["shuffle_on_overlayfs"]  else "🟢 HOST VOL"
    cs_flag      = "🟢 ACTIVE"   if r["container_support_active"] else "🔴 INACTIVE"
    print(f"Executor {r['executor_id']:>3} | cgroup {r['cgroup_version']} "
          f"limit={r['cgroup_limit_gb']}g req={r['required_gb']}g "
          f"headroom={r['headroom_mb']}MB | {oom_flag} | {overlay_flag} | "
          f"UseContainerSupport={cs_flag}")

spark.stop()
```

> **Mastery Note:** Reading `/sys/fs/cgroup/memory.max` (cgroup v2) or `/sys/fs/cgroup/memory/memory.limit_in_bytes` (cgroup v1) directly from within the executor container is the only authoritative way to know the actual kernel-enforced memory limit — Spark's own `SparkEnv` and `MemoryManager` do not expose this value. A value of `9223372036854771712` (approximately 2^63) from cgroup v1 means no limit is set, which is reported as `"UNCONSTRAINED"` above. The shuffle OverlayFS check by parsing `/proc/mounts` is critical: if the Docker host does not have a bind mount configured for the shuffle directory, shuffle writes silently fall back to the container's writable OverlayFS layer, causing a 30–45% I/O throughput regression that shows up as elevated shuffle write time in the Spark UI's Stage Detail view under "Shuffle Write" metrics, but is otherwise invisible without this diagnostic.

---

## 🎯 Mastery Checklist

To achieve true mastery of Running Spark with Docker:
- [ ] Know the exact formula for cgroup memory limits: `executor.memory + memoryOverhead + offHeap.size + JVM code cache buffer` and why setting the limit to `executor.memory` alone causes exit code 137
- [ ] Understand how `UseContainerSupport` and `MaxRAMPercentage` eliminate the need for `-Xmx` in containerized deployments and why Java < 8u191 is dangerous in Docker
- [ ] Be able to diagnose an OOM kill from exit code 137, distinguish it from exit code 1 (application error) and exit code 143 (SIGTERM / graceful shutdown), using both Spark UI and `docker inspect` output
- [ ] Know when shuffle directories on OverlayFS are causing I/O degradation by correlating Spark UI shuffle write times against a baseline, and how to fix it with host bind mounts
- [ ] Understand the Dockerfile layer ordering strategy: stable base layers first, application artifact last, and why this reduces cold-start time from minutes to seconds on a 10-node worker cluster
- [ ] Know how `tini` as PID 1 enables graceful JVM shutdown and why its absence causes task resubmission storms when a worker container is restarted
- [ ] Understand the Kubernetes `requests` vs `limits` asymmetry and the QoS class implications (Guaranteed, Burstable, BestEffort) for Spark executor eviction priority on a shared cluster
- [ ] Be able to reproduce and fix the Spark RPC `CoarseGrainedSchedulerBackend` connection failure caused by `SPARK_LOCAL_IP` being set to `127.0.0.1` in a Docker bridge network

---

## 📚 Summary

Running Apache Spark inside Docker is not a matter of wrapping `spark-submit` in a `docker run` command. The fundamental challenge is bridging the gap between Docker's single-process, immutable-image philosophy and Spark's multi-process, long-lived-JVM execution model. The cgroup memory accounting mismatch — where the JVM heap is only one of four distinct memory zones that count against the container limit — is responsible for the majority of production container crashes. The corrective formula (`executor.memory + memoryOverhead + offHeap.size + code cache buffer`) must be treated as a hard constraint applied to every Docker memory limit and Kubernetes resource limit in the deployment. [[1]](spark_book.pdf#page=384)

Image layering strategy determines operational velocity. A monolithic Dockerfile image that bundles the JDK, Spark distribution, and application JAR into a single layer means every code change triggers a multi-gigabyte image pull on every worker node, serializing the cluster startup. The multi-stage build pattern — frozen base layer, frozen Spark layer, hot-swappable application JAR layer — reduces deployment-time image distribution to tens of megabytes and cold-start times from ten minutes to under thirty seconds on a typical ten-node cluster. [[2]](spark_book.pdf#page=125)

Kubernetes via Docker Desktop adds a third dimension of complexity: the Kubernetes scheduler, cgroup v2 enforcement, and Spark's dynamic executor allocation must all agree on memory and CPU accounting. `UseContainerSupport` and `MaxRAMPercentage` are the bridge between the Kubernetes resource model and JVM ergonomics. The diagnostic patterns — reading `/sys/fs/cgroup/memory.max` from within executors, checking `/proc/mounts` for OverlayFS on shuffle directories, verifying `SPARK_LOCAL_IP` resolves across the Docker network — are the production debugging toolkit that separates an engineer who deploys Spark in Docker from one who operates it reliably at scale. [[3]](spark_book.pdf#page=133)

