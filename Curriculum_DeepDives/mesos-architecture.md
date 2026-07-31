# 🔥 Master Class: Mesos Architecture

## Overview

Apache Mesos is a distributed systems kernel that abstracts CPU, memory, disk, and network resources across an entire datacenter, presenting them as a single unified pool to frameworks that run on top of it. Born out of UC Berkeley's AMPLab in 2009, Mesos was designed to answer a specific, hard problem: how do you efficiently multiplex dozens of heterogeneous workloads — batch analytics, long-running services, machine learning jobs — across thousands of physical machines without partitioning the cluster into isolated silos? Static partitioning wastes 30-50% of cluster capacity in practice; Mesos's answer is fine-grained, dynamic resource sharing via a two-level scheduling model.

Spark on Mesos is one of the most powerful deployment modalities available. Instead of Spark's standalone scheduler or YARN managing resources, Mesos acts as the global arbiter. The Spark driver registers as a Mesos framework, receives resource **offers** from the Mesos master, and launches executor tasks directly on Mesos agents. This architecture allows Spark to coexist on the same machines as Kafka, Cassandra, TensorFlow jobs, and Marathon-managed microservices — all sharing the same physical hardware with strict isolation guarantees and fairness policies enforced at the kernel level.

The central design insight is **separation of concerns**: Mesos handles *where* and *how much* to allocate; individual frameworks decide *what* to run on those resources. This two-level delegation is what makes the system horizontally scalable — the Mesos master never needs to understand Spark's internal scheduling semantics, and Spark never needs to negotiate with Kafka for machine time. [Ref: 451](spark_book.pdf#page=451)

--- [Ref: 457](spark_book.pdf#page=457)

## 🏗️ Architectural Deep Dive [Ref: 461](spark_book.pdf#page=461)

### How It Works Under the Hood

The Mesos master is the global resource broker. It maintains a live view of every agent's available resources via heartbeat messages sent every `--agent_ping_timeout` seconds (default: 15s). When resources become available — a task completes, an agent registers, or a framework rejects an offer — the master's **allocation module** triggers. The default allocator implements the **Dominant Resource Fairness (DRF)** algorithm, which extends max-min fairness to multiple resource dimensions. DRF identifies each framework's *dominant share* — the resource dimension (CPU, RAM, GPU, disk) where the framework consumes the highest percentage of total cluster capacity. The framework with the lowest dominant share is offered resources next. This prevents any single framework from monopolizing a scarce resource type even when it has slack in other dimensions.

Resource offers flow from the master to registered framework schedulers over a persistent TCP connection (Mesos's own binary protocol, or HTTP/2 via the v1 API). An offer is a structured message containing agent ID, hostname, and a list of `Resource` protobufs with scalar values and optional **role** labels. The Spark framework scheduler inspects each offer, computes whether it can satisfy a pending task's resource requirements, and either **accepts** the offer with a list of `TaskInfo` protobufs, or **declines** it with an optional refuse duration. Declined offers with a long refuse period (e.g., `5m`) tell the master not to re-offer those resources to this framework, preventing the busy-wait anti-pattern.

Mesos agents enforce isolation using **cgroups** at the Linux kernel level — each task runs inside a cgroup that the agent creates with the exact CPU shares (`cpu.shares`) and memory hard limits (`memory.limit_in_bytes`) specified in the accepted offer. This is not JVM-level isolation; it is OS-level enforcement. If a Spark executor exceeds its memory limit, the OOM killer fires on the executor process, the agent marks the task as `TASK_FAILED`, and the Spark driver's `TaskScheduler` receives a `StatusUpdate` and reschedules the lost task. The entire lifecycle — from offer to execution to failure detection — is event-driven over the master-agent and master-scheduler persistent connections, with no polling.

Frameworks register with the Mesos master by connecting and sending a `SUBSCRIBE` call containing a `FrameworkInfo` protobuf. This protobuf carries the framework's **role** (e.g., `spark`, `marathon`), **failover timeout** (how long the master preserves the framework's resources after a scheduler disconnect), and **capabilities** (e.g., `PARTITION_AWARE`, `MULTI_ROLE`, `GPU_RESOURCES`). The master's **replicated log** — a Paxos-based distributed log built on LevelDB — persists registered framework state, agent registrations, and resource reservations so that a master failover (via ZooKeeper leader election among standby masters) does not lose cluster state.

```text
ZooKeeper Ensemble (Leader Election)
 │
 ▼
┌───────────────────────────────────────────────────┐
│ Mesos Master (Active) │
│ ┌──────────────┐ ┌────────────────────────┐ │
│ │ Allocator │ │ Replicated Log │ │
│ │ (DRF) │ │ (Paxos / LevelDB) │ │
│ └──────┬───────┘ └────────────────────────┘ │
│ │ Resource Offers (HTTP/2 or Protobuf) │
└─────────┼─────────────────────────────────────────┘
 │
 ┌─────┴──────────────────────────┐
 │ │
 ▼ ▼
┌──────────────────────┐ ┌──────────────────────┐
│ Spark Framework │ │ Marathon Framework │
│ Scheduler (Driver) │ │ Scheduler │
│ - Accept offers │ │ - Launch app tasks │
│ - Launch executors │ │ - Health checking │
└──────────┬───────────┘ └──────────┬───────────┘
 │ LaunchTasks │ LaunchTasks
 ┌──────┴───────────────────────────┴──────┐
 │ Mesos Agents (Workers) │
 │ ┌───────────────┐ ┌───────────────┐ │
 │ │ Agent Node 1 │ │ Agent Node 2 │ │
 │ │ cgroup: Task1 │ │ cgroup: Task3│ │
 │ │ cgroup: Task2 │ │ cgroup: Task4│ │
 │ └───────────────┘ └───────────────┘ │
 └──────────────────────────────────────────┘ [Ref: 469](spark_book.pdf#page=469)
```

### Key Internal Components

- **Mesos Master Allocator (DRF):** Runs as a single-threaded process inside the master. Every allocation cycle it sorts frameworks by dominant share, iterates the sorted list, and constructs offers from available agent resources. The sort is O(F log F) where F is the number of registered frameworks — at 1,000 frameworks this remains sub-millisecond.

- **Mesos Agent (formerly Slave):** Runs on every worker node. It manages a pool of **containerizers** (Mesos containerizer using cgroups, Docker containerizer, or the unified containerizer). On task launch it writes cgroup configuration, pulls the executor binary (or Docker image), and forks the executor process. Executor-to-agent communication happens over a local Unix domain socket.

- **Framework Scheduler:** The component that lives inside the application driver (e.g., Spark's `MesosCoarseGrainedSchedulerBackend` or `MesosFineGrainedSchedulerBackend`). It implements the Mesos scheduler HTTP API, managing offer acceptance, task status callbacks, and reschedule logic. Spark's coarse-grained mode acquires executors once and holds them for the application lifetime; fine-grained mode releases resources after every task.

- **ZooKeeper (Leader Election & Discovery):** Mesos masters form a quorum (typically 3 or 5 nodes). ZooKeeper holds a single ephemeral znode containing the active master's endpoint. Frameworks and agents watch this znode — on master failover, they reconnect to the new leader within the `--zk_session_timeout` window (default: 10s). The replicated log reconstructs cluster state from disk, not from ZooKeeper, so ZooKeeper carries no resource state. [Ref: 452](spark_book.pdf#page=452)

--- [Ref: 458](spark_book.pdf#page=458)

## ⚠️ Critical Concepts & Common Pitfalls [Ref: 463](spark_book.pdf#page=463)

### The Offer Declined / Resource Hoarding Deadlock

A subtle failure mode emerges when Spark is launched in coarse-grained mode with `spark.cores.max` set higher than the cluster can satisfy. The Spark scheduler accepts every offer that arrives, accumulating partial executor slots. Meanwhile, Marathon or another framework is also waiting for offers. Because Spark has accepted (and is holding) large chunks of resources without running tasks yet (executors are still launching), Marathon starves. The Mesos master's DRF sees Spark's dominant share growing and stops offering to it, yet Spark hasn't reached its target executor count.

The fix is two-fold: set `spark.mesos.rejectOfferDuration` (e.g., `120s`) so that Spark refuses offers it cannot use rather than holding them, and configure `spark.executor.cores` and `spark.executor.memory` to match the agent's cgroup granularity. Additionally, using **reservations** (static or dynamic) guarantees Spark always has a minimum resource floor while respecting fairness for other frameworks. Without reservations, Spark in a shared cluster will oscillate between resource starvation and resource monopolization depending on load timing. [Ref: 470](spark_book.pdf#page=470)

### DRF Weight Misconfiguration and Fairness Collapse

Mesos roles support **weights** — a role with `weight=2.0` receives offers at twice the rate of a role with `weight=1.0` during DRF sorting. A common misconfiguration is assigning very high weights to a production Spark role (`weight=10`) while leaving the Marathon role at the default (`weight=1`). During a burst of Spark job submissions, DRF will allocate 90% of cluster resources to Spark before Marathon even receives its first offer cycle, causing Marathon-managed services (REST APIs, databases) to miss their SLAs. The correct pattern is to use **quota** (`mesos-master --quota`) to guarantee Marathon a minimum resource floor regardless of weights, and to use weights only for *surplus* resource distribution above that floor. Monitor the Mesos UI's `/weights` and `/quota` endpoints — mismatches between configured quota and actual allocation always indicate a scheduling misconfiguration, not a framework bug. [Ref: 455](spark_book.pdf#page=455)

--- [Ref: 459](spark_book.pdf#page=459)

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|-----------|---------|-------|
| DRF Allocation Cycle | O(F log F) | No | F = number of frameworks; sub-millisecond at F=1,000 |
| Offer generation per agent | O(R) | No | R = number of resource types; typically 4-6 |
| ZooKeeper master failover | O(1) reconnect | No | Agents reconnect within zk_session_timeout (10s default) |
| Framework re-registration after failover | O(T) state replay | No | T = tasks in replicated log; can be seconds at scale |
| cgroup enforcement per task | O(1) kernel call | No | `cgroupv2` write; < 1ms; hard memory limit is synchronous OOM |
| Offer decline propagation | O(A) | No | A = agents; master re-marks refused resources after refuse duration | [Ref: 464](spark_book.pdf#page=464)

---

## 💻 Code Examples

### Example 1: Launching Spark on Mesos in Coarse-Grained Mode with Resource Constraints

> **What this demonstrates:** How Spark's `MesosCoarseGrainedSchedulerBackend` negotiates with the Mesos master — showing the exact configuration knobs that control offer acceptance, executor sizing, and Docker containerization via the Mesos unified containerizer.

```python
# spark_mesos_submit.py
# Submitting a Spark job to a Mesos cluster using the Python API.
# Coarse-grained mode: executors are acquired once and held for the job duration,
# preventing the per-task offer negotiation overhead of fine-grained mode.

from pyspark.sql import SparkSession

spark = (
 SparkSession.builder
 .appName("MesosCoarseGrainedDemo")

 # --- Mesos Master Connection ---
 # Use ZooKeeper for master discovery — never hardcode the master IP.
 # Mesos master failover is transparent when using the zk:// URI.
 .master("mesos://zk://zk1:2181,zk2:2181,zk3:2181/mesos")

 # --- Executor Resource Sizing ---
 # These values become the TaskInfo.resources protobuf fields in the Mesos offer.
 # CRITICAL: Must be <= the smallest agent's available resources to avoid offer starvation.
 .config("spark.executor.memory", "4g") # maps to mesos memory resource
 .config("spark.executor.cores", "2") # maps to mesos cpus resource

 # --- Coarse-Grained Mode: Hold executors for the application lifetime ---
 # Setting max cores limits total cluster consumption.
 # Without this, Spark will consume ALL offered resources (greedy acquisition).
 .config("spark.cores.max", "20") # max 10 executors (20 cores / 2 per executor)

 # --- Offer Rejection: CRITICAL for shared clusters ---
 # Refuse offers that don't meet executor requirements for 2 minutes.
 # Without this, Spark holds unusable offers and starves other frameworks.
 .config("spark.mesos.rejectOfferDuration", "120s")

 # --- Docker containerization via Mesos unified containerizer ---
 # The agent pulls this image and runs the executor inside it.
 .config("spark.mesos.executor.docker.image", "apache/spark:3.5.0")

 # --- Role: maps to Mesos role for DRF fairness and quota enforcement ---
 .config("spark.mesos.role", "spark-production")

 # --- Principal: authenticates the framework with the master ---
 .config("spark.mesos.principal", "spark")
 .config("spark.mesos.secret", "/etc/mesos/spark.secret")

 .getOrCreate()
)

# Verify executor allocation via Mesos REST API (check after this point in Mesos UI)
df = spark.range(1_000_000).selectExpr("id", "id * id AS squared")
df.groupBy((df.id % 10).alias("bucket")).count().show()

spark.stop()
```

> **Mastery Note:** The `spark.mesos.role` configuration is the bridge between Spark and Mesos's DRF fairness model — without it, Spark registers under the `*` (wildcard) role and competes with every other framework on equal terms, ignoring any quota you've configured for a named role. When `spark.cores.max` is omitted, `MesosCoarseGrainedSchedulerBackend` will greedily accept every offer until the cluster is full, which is catastrophic in shared clusters. The `spark.mesos.rejectOfferDuration` setting directly controls the `Filters.refuse_seconds` field in the Mesos `DECLINE` call — setting this to `0` causes the master to re-offer rejected resources within milliseconds, creating a tight CPU-burning polling loop inside the master's allocator.

---

### Example 2: Implementing a Custom DRF-Aware Offer Filter in a Spark Mesos Scheduler Extension

> **What this demonstrates:** How to intercept and reason about Mesos resource offers programmatically using the Mesos HTTP Scheduler API v1, mirroring what Spark's internal `MesosSchedulerBackend` does — exposing the offer evaluation logic that Spark hides behind configuration.

```python
# mesos_offer_inspector.py
# Uses the Mesos v1 Scheduler HTTP API to subscribe as a framework and log
# incoming resource offers — demonstrates the raw offer structure that Spark
# evaluates internally in MesosCoarseGrainedSchedulerBackend.

import json
import requests

MESOS_MASTER = "http://mesos-master:5050"
FRAMEWORK_NAME = "OfferInspectorFramework"

# Step 1: Subscribe to the master — this is what Spark does when the driver starts.
# The master assigns a framework_id and begins sending offers.
subscribe_payload = {
 "type": "SUBSCRIBE",
 "subscribe": {
 "framework_info": {
 "user": "root",
 "name": FRAMEWORK_NAME,
 # Role must match a configured Mesos role for quota/weight to apply.
 "roles": ["spark-dev"],
 # Failover timeout: master preserves framework state for 60s after disconnect.
 # Spark sets this to a large value (e.g., 1 week) so executor state survives
 # brief driver restarts without losing running tasks.
 "failover_timeout": 60.0,
 "capabilities": [
 {"type": "MULTI_ROLE"}, # Accept offers from multiple roles
 {"type": "PARTITION_AWARE"}, # Distinguish agent unreachable vs gone
 ]
 }
 }
}

headers = {
 "Content-Type": "application/json",
 "Accept": "application/json",
 "Mesos-Stream-Id": "" # Will be populated from response header
}

# The v1 API uses a persistent streaming HTTP response (Server-Sent Events style).
# Each event is a RecordIO-encoded JSON message.
response = requests.post(
 f"{MESOS_MASTER}/api/v1/scheduler",
 data=json.dumps(subscribe_payload),
 headers=headers,
 stream=True # Keep connection open to receive offer stream
)

print(f"Subscribed. Stream-Id: {response.headers.get('Mesos-Stream-Id')}")

# Step 2: Process the streaming event response.
# Each offer contains agent_id, hostname, and a list of resources.
for line in response.iter_lines():
 if not line:
 continue
 # RecordIO format: first line is message length, second is JSON body.
 try:
 event = json.loads(line)
 except json.JSONDecodeError:
 continue # length prefix line, skip

 event_type = event.get("type")

 if event_type == "OFFERS":
 for offer in event["offers"]["offers"]:
 agent_id = offer["agent_id"]["value"]
 hostname = offer["hostname"]

 # Parse available resources from the offer protobuf
 resources = {r["name"]: r["scalar"]["value"]
 for r in offer.get("resources", [])
 if r.get("type") == "SCALAR"}

 cpus = resources.get("cpus", 0)
 mem_mb = resources.get("mem", 0)
 disk_mb = resources.get("disk", 0)

 print(f"OFFER from {hostname} ({agent_id[:8]}...): "
 f"cpus={cpus}, mem={mem_mb}MB, disk={disk_mb}MB")

 # Spark's offer evaluation logic (simplified):
 # Accept if cpus >= spark.executor.cores AND mem >= spark.executor.memory
 REQUIRED_CPUS = 2.0
 REQUIRED_MEM_MB = 4096

 if cpus >= REQUIRED_CPUS and mem_mb >= REQUIRED_MEM_MB:
 print(f" -> ACCEPT: Can launch an executor on {hostname}")
 else:
 print(f" -> DECLINE: Insufficient resources (need {REQUIRED_CPUS} cpus, "
 f"{REQUIRED_MEM_MB}MB; got {cpus} cpus, {mem_mb}MB)")

 elif event_type == "SUBSCRIBED":
 framework_id = event["subscribed"]["framework_id"]["value"]
 print(f"Framework registered with ID: {framework_id}")
```

> **Mastery Note:** The `PARTITION_AWARE` capability is essential for production Spark deployments on Mesos. Without it, the master sends `TASK_LOST` for all tasks on an unreachable agent immediately, causing Spark to immediately reschedule those tasks and potentially run duplicate work. With `PARTITION_AWARE`, the master sends `TASK_UNREACHABLE` first, allowing Spark to wait for a configurable period before declaring the agent truly gone. This distinction is the difference between a network partition causing duplicate computation and it being handled gracefully. The `failover_timeout` value in `FrameworkInfo` is the master's guarantee window — set it too low (< 60s) and a Spark driver GC pause that exceeds it will cause the master to kill all the framework's executors.

---

### Example 3: Configuring Mesos Roles, Weights, and Quota via the Master REST API

> **What this demonstrates:** The operational workflow for configuring DRF fairness parameters — roles, weights, and quota — that govern how the Mesos master distributes resources between Spark and co-located frameworks like Marathon.

```bash
#!/bin/bash
# mesos_fairness_config.sh
# Configure Mesos DRF weights and resource quota via the operator HTTP API.
# Run this against the ACTIVE Mesos master (check ZooKeeper or /redirect endpoint).

MASTER="http://mesos-master-active:5050"

# ─── Step 1: Create roles with weights ───────────────────────────────────────
# Weight=4 means 'spark-production' receives 4x the surplus resources of
# 'spark-dev' (weight=1) after quota floors are satisfied.
# Marathon uses weight=2 — it gets 2x more surplus than spark-dev.

curl -s -X PUT "${MASTER}/roles" \
 -H "Content-Type: application/json" \
 -d '{
 "roles": [
 {
 "name": "spark-production",
 "weight": 4.0
 },
 {
 "name": "spark-dev",
 "weight": 1.0
 },
 {
 "name": "marathon",
 "weight": 2.0
 }
 ]
 }'

echo "Roles and weights configured."

# ─── Step 2: Set resource QUOTA for critical roles ────────────────────────────
# Quota guarantees a MINIMUM resource floor regardless of DRF weights.
# Marathon is a long-running service framework — it must always have resources
# for its health checks and restarts even if Spark is submitting heavily.
# CRITICAL: Quota is subtracted FIRST from the cluster, then DRF distributes
# the remaining surplus. Total quota must be <= total cluster capacity.

# Guarantee Marathon at least 16 CPUs and 32GB RAM at all times.
curl -s -X PUT "${MASTER}/quota" \
 -H "Content-Type: application/json" \
 -d '{
 "role": "marathon",
 "guarantee": [
 {"name": "cpus", "type": "SCALAR", "scalar": {"value": 16.0}},
 {"name": "mem", "type": "SCALAR", "scalar": {"value": 32768.0}}
 ],
 "limit": [
 {"name": "cpus", "type": "SCALAR", "scalar": {"value": 64.0}},
 {"name": "mem", "type": "SCALAR", "scalar": {"value": 131072.0}}
 ]
 }'

echo "Marathon quota set: guarantee 16 cpus / 32GB, limit 64 cpus / 128GB."

# Guarantee spark-production a minimum floor for critical jobs.
curl -s -X PUT "${MASTER}/quota" \
 -H "Content-Type: application/json" \
 -d '{
 "role": "spark-production",
 "guarantee": [
 {"name": "cpus", "type": "SCALAR", "scalar": {"value": 32.0}},
 {"name": "mem", "type": "SCALAR", "scalar": {"value": 65536.0}}
 ],
 "limit": [
 {"name": "cpus", "type": "SCALAR", "scalar": {"value": 200.0}},
 {"name": "mem", "type": "SCALAR", "scalar": {"value": 409600.0}}
 ]
 }'

echo "spark-production quota set: guarantee 32 cpus / 64GB, limit 200 cpus / 400GB."

# ─── Step 3: Verify current allocation state ─────────────────────────────────
echo ""
echo "=== Current Roles ==="
curl -s "${MASTER}/roles" | python3 -m json.tool | grep -E '"name"|"weight"|"allocated"'

echo ""
echo "=== Current Quota ==="
curl -s "${MASTER}/quota" | python3 -m json.tool
```

> **Mastery Note:** The interplay between `guarantee` and `limit` in Mesos quota is frequently misunderstood. The `guarantee` is a hard reservation — those resources are set aside for the role before DRF even runs, and no other role can consume them even if this role has zero active frameworks. The `limit` is a soft cap — the role cannot consume beyond this value even if the cluster has idle capacity, preventing runaway burst consumption. If you set a `limit` without a `guarantee`, you get a ceiling but no floor, which is correct for dev/test roles. If you set neither, the role participates in pure DRF, which means heavy frameworks starve lighter ones during load spikes. Always instrument `/metrics/snapshot` on the Mesos master to watch `allocator/mesos/quota/roles/<role>/resources/cpus/offered_or_allocated` — a value consistently at the guarantee floor indicates the role is resource-starved and the guarantee is doing its job.

---

### Example 4: Marathon Integration — Deploying a Spark History Server as a Marathon Application with Health Checks

> **What this demonstrates:** How Marathon, as a Mesos framework, launches and supervises a long-running Spark History Server — illustrating the full framework co-existence model and how Marathon's health checking integrates with Mesos task lifecycle management.

```json
// marathon_spark_history_server.json
// Deploy the Spark History Server as a Marathon application.
// Marathon submits this as a TaskInfo to Mesos agents via accepted offers.
// Mesos agents enforce resource limits via cgroups; Marathon handles restart policy.
{
 "id": "/spark/history-server",
 "description": "Spark History Server — reads completed application event logs from S3",

 // ─── Resource requirements (become the Mesos offer resource request) ──────
 // These values are matched against incoming resource offers by Marathon's scheduler.
 // Marathon uses the same DRF offer-accept cycle as Spark — it's just another framework.
 "cpus": 2.0,
 "mem": 4096,
 "disk": 1024,
 "instances": 1, // Marathon ensures exactly 1 instance is running at all times

 // ─── Role: maps to 'marathon' Mesos role for quota and weight enforcement ──
 "role": "marathon",

 // ─── Container: Mesos unified containerizer with Docker image ─────────────
 "container": {
 "type": "MESOS", // Use Mesos containerizer (not Docker daemon) — avoids Docker socket
 "docker": {
 "image": "apache/spark:3.5.0",
 "forcePullImage": false
 },
 "volumes": [
 {
 // Mount the event log directory. In production, use a shared filesystem
 // (e.g., HDFS fuse mount) or S3-backed path. The history server scans this path.
 "hostPath": "/mnt/spark-eventlogs",
 "containerPath": "/opt/spark/work-dir/eventlogs",
 "mode": "RO" // Read-only: history server never writes event logs
 }
 ]
 },

 // ─── Command: start the history server process ────────────────────────────
 "cmd": "/opt/spark/sbin/start-history-server.sh",
 "env": {
 "SPARK_HISTORY_OPTS": "-Dspark.history.fs.logDirectory=s3a://my-bucket/spark-logs -Dspark.history.ui.port=18080",
 "SPARK_DAEMON_MEMORY": "3g" // Leave 1GB for JVM metaspace + off-heap overhead
 },

 // ─── Health checks: Marathon polls this endpoint to detect failures ────────
 // If 3 consecutive checks fail, Marathon calls the Mesos master to kill the task
 // and re-schedules it on a healthy agent. This is NOT Spark internals — this is
 // Marathon+Mesos task lifecycle management.
 "healthChecks": [
 {
 "protocol": "HTTP",
 "path": "/", // History server UI root returns 200 when healthy
 "portIndex": 0,
 "gracePeriodSeconds": 120, // Don't check for 2min after start (JVM warmup)
 "intervalSeconds": 30,
 "timeoutSeconds": 10,
 "maxConsecutiveFailures": 3
 }
 ],

 // ─── Port mapping: Marathon asks Mesos for a dynamic port from the agent's port range
 "networks": [{"mode": "host"}],
 "portDefinitions": [
 {
 "port": 18080,
 "protocol": "tcp",
 "name": "ui",
 "labels": {"VIP_0": "/spark-history:18080"} // Minuteman/DC/OS service discovery
 }
 ],

 // ─── Constraints: anti-affinity to avoid single point of failure ──────────
 // Place this task on an agent that doesn't already have another instance.
 "constraints": [["hostname", "UNIQUE"]],

 // ─── Upgrade strategy: Marathon's rolling update behavior ─────────────────
 "upgradeStrategy": {
 "minimumHealthCapacity": 0.0, // Kill old instance before starting new one (1 instance only)
 "maximumOverCapacity": 0.0
 }
}
```

```bash
# Deploy the Marathon application via the Marathon REST API
curl -s -X POST \
 "http://marathon-master:8080/v2/apps" \
 -H "Content-Type: application/json" \
 -d @marathon_spark_history_server.json | python3 -m json.tool

# Check deployment status — Marathon returns the app's task state
curl -s "http://marathon-master:8080/v2/apps/spark/history-server" \
 | python3 -m json.tool | grep -E '"tasksRunning"|"tasksHealthy"|"tasksUnhealthy"'
```

> **Mastery Note:** Marathon's health check failure cascade is a critical integration point between three systems: Marathon, Mesos, and the application. When `maxConsecutiveFailures` is breached, Marathon does **not** kill the task itself — it sends a `KillTask` request to the Mesos master, which forwards it to the agent. The agent sends `SIGTERM` to the container process and waits `--executor_shutdown_grace_period` (default: 5s) before sending `SIGKILL`. The Mesos agent then reports `TASK_KILLED` back to Marathon, which records the failure, applies the `backoffSeconds` delay, and re-issues a new launch task via a fresh offer cycle. If the Spark History Server's S3A connector is mis-configured (wrong endpoint, bad credentials), it will fail on startup, consume its grace period, fail all health checks, and loop in a kill-restart cycle — observable in both the Marathon UI as "Unhealthy" and in the Mesos UI as rapidly cycling `TASK_KILLED` / `TASK_RUNNING` transitions. The `gracePeriodSeconds: 120` setting is specifically sized for JVM startup + S3A metadata initialization, which typically takes 30-90 seconds on a cold start.

---

## 🎯 Mastery Checklist

To achieve true mastery of Mesos Architecture:

- [ ] Understand how DRF calculates dominant share across multiple resource dimensions simultaneously and why it outperforms per-resource max-min fairness
- [ ] Know when coarse-grained Spark mode outperforms fine-grained mode (long-running batch, low task overhead) and when fine-grained wins (interactive queries, fast tasks, mixed workloads)
- [ ] Be able to diagnose offer starvation from the Mesos master's `/metrics/snapshot` — specifically `master/frameworks_inactive` spiking and `allocator/mesos/allocation_run_ms` increasing
- [ ] Understand the tradeoff between quota `guarantee` (reserved floor) and pure DRF weights (surplus distribution), and when to use each
- [ ] Know how Mesos `PARTITION_AWARE` capability changes task status semantics (`TASK_UNREACHABLE` vs `TASK_LOST`) and its impact on Spark's speculative execution decisions
- [ ] Be able to trace a Spark executor failure through all four systems: Spark TaskScheduler → Mesos agent cgroup OOM → `TASK_FAILED` status update → driver reschedule
- [ ] Understand how ZooKeeper leader election interacts with the Mesos replicated log during master failover, and why framework state survives but in-flight offer state does not

---

## 📚 Summary

Mesos's two-level scheduling architecture achieves something that monolithic schedulers cannot: it allows fundamentally different computation paradigms — Spark batch analytics, Marathon microservices, TensorFlow training jobs — to share physical hardware at high efficiency without the scheduler itself needing to understand any of them. The DRF algorithm is the mathematical foundation that makes this work fairly; dominant share normalization across resource dimensions prevents any single framework from monopolizing a scarce resource type, and the weight and quota systems allow operators to encode business priorities directly into the resource allocation layer. 

The Mesos master's design as a thin offer broker — maintaining only cluster state and allocation policy, never application semantics — is what gives it its linear scalability. The Catalyst optimizer and Tungsten execution engine inside Spark's driver operate completely independently of Mesos's allocation cycle; Mesos simply provides the physical slots and enforces the cgroup boundaries, while Spark decides what tasks fill those slots and how data moves between them. 

For production Spark deployments on Mesos, the critical engineering decisions are: sizing executor resources to match offer granularity (avoiding partial offer acceptance), configuring roles and quota to protect co-located services from Spark's greedy offer consumption, and selecting the right containerizer (Mesos unified containerizer over Docker daemon for reduced agent overhead). Mastery of Mesos architecture means understanding the full event chain from ZooKeeper leader election through DRF allocation cycles to cgroup enforcement — every link in that chain is a potential failure point and a performance lever. 

