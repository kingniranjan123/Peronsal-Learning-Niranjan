# Mesos Architecture Assessment

## 1. True/False Questions

**1. In a Mesos cluster, the Mesos Master uses its replicated log to store active task execution states and executor logs, ensuring zero data loss upon failover.**
*Answer: False*
**Mastery Explanation:** The replicated log (built on LevelDB) persists registered framework state, agent registrations, and resource reservations, but NOT active task states. Active task state is reconstructed when agents and frameworks reconnect to the new master after a ZooKeeper leader election.

**2. Setting `spark.mesos.rejectOfferDuration` to `0s` is recommended for latency-sensitive applications to ensure they immediately get another chance to evaluate rejected resources.**
*Answer: False*
**Mastery Explanation:** Setting this to `0s` causes the Mesos master to instantly re-offer the rejected resources, creating a tight CPU-burning polling loop in the master's allocator. It must be set to a reasonable duration (e.g., `120s`) to prevent busy-waiting.

**3. When Spark is deployed on Mesos in coarse-grained mode without `spark.cores.max` configured, it will greedily accept all available resource offers until the cluster is completely full.**
*Answer: True*
**Mastery Explanation:** The `MesosCoarseGrainedSchedulerBackend` in Spark is greedy by default. Without a maximum core limit, it will continuously accept offers, hoarding cluster resources and starving other frameworks like Marathon.

**4. The Dominant Resource Fairness (DRF) algorithm determines the next framework to receive an offer by identifying the framework with the lowest absolute resource usage in bytes or cores.**
*Answer: False*
**Mastery Explanation:** DRF identifies the framework with the lowest *dominant share* (the resource dimension where the framework consumes the highest *percentage* of the total cluster capacity), not absolute usage.

**5. Mesos cgroups isolation enforces memory limits at the OS level, meaning if a Spark executor breaches the limit, the Linux OOM killer terminates the process rather than throwing a JVM OutOfMemoryError.**
*Answer: True*
**Mastery Explanation:** Mesos uses Linux cgroups (`memory.limit_in_bytes`). Exceeding this triggers the kernel OOM killer, which marks the task as `TASK_FAILED`, bypassing the JVM entirely.

**6. If a framework registers with the `PARTITION_AWARE` capability, the Mesos Master will immediately send a `TASK_LOST` status if an agent becomes unreachable due to a network partition.**
*Answer: False*
**Mastery Explanation:** With `PARTITION_AWARE`, the master sends `TASK_UNREACHABLE` instead of `TASK_LOST`. This allows the framework to wait gracefully before declaring the agent permanently gone, preventing duplicate task executions.

**7. In a Mesos quota configuration, setting a `limit` without a `guarantee` provides a resource ceiling but no reserved floor during high cluster contention.**
*Answer: True*
**Mastery Explanation:** The `guarantee` reserves minimum capacity, while the `limit` caps maximum consumption. A `limit` without a `guarantee` is purely a soft cap, meaning the framework competes via DRF but cannot exceed the limit.

**8. ZooKeeper in a Mesos architecture stores all resource offers, agent states, and framework queues to coordinate the master leader election.**
*Answer: False*
**Mastery Explanation:** ZooKeeper only holds a single ephemeral znode for leader election and discovery. Cluster state is stored in the master's replicated log and reconstructed via agent/framework reconnections, not ZK.

**9. When a Mesos agent restarts, Marathon applications running on that agent are instantly terminated and rescheduled on another node by the Mesos master.**
*Answer: False*
**Mastery Explanation:** Marathon, not the Mesos master, handles rescheduling. Furthermore, if the agent process restarts but the executor containers are still running, they can be re-adopted by the agent without terminating the tasks.

**10. Assigning a DRF weight of 10.0 to a Spark role and leaving Marathon at 1.0 ensures Spark always preempts Marathon's running tasks when Spark jobs are submitted.**
*Answer: False*
**Mastery Explanation:** DRF weights govern the distribution of *available/surplus* resources during allocation cycles. Mesos does not preempt running tasks based on weights.

## 2. Multiple Choice Questions

**11. What is the Big-O time complexity of the DRF Allocation Cycle in the Mesos Master per allocation run?**
A) O(1)
B) O(R) where R is resource types
C) O(F log F) where F is the number of registered frameworks
D) O(A * F) where A is agents and F is frameworks
*Answer: C*
**Mastery Explanation:** The allocator sorts the frameworks by their dominant share every cycle, which requires O(F log F) time. At 1,000 frameworks, this is still sub-millisecond.

**12. A Spark job running on Mesos in coarse-grained mode stops receiving offers despite the cluster having 50% free capacity. Which of the following is the most likely cause?**
A) Spark's `spark.mesos.role` was omitted, so it defaulted to the Marathon role.
B) Spark has reached its `spark.cores.max` limit.
C) The Mesos master's replicated log is full.
D) ZooKeeper's `zk_session_timeout` was exceeded by the driver.
*Answer: B*
**Mastery Explanation:** Once `spark.cores.max` is reached, Spark stops accepting offers for new executors. If this is not the case, another common cause is DRF starvation due to missing roles/quotas, but B is a direct functional limit.

**13. In Mesos, what happens when a framework declines an offer without setting a refuse duration (Filters.refuse_seconds)?**
A) The resources are blacklisted for the lifetime of the framework.
B) The master immediately re-offers the resources in the next millisecond, causing high CPU load.
C) The resources are given exclusively to the Marathon framework.
D) The agent hosting the resources is restarted.
*Answer: B*
**Mastery Explanation:** Without a refuse duration (equivalent to 0s), the DRF allocator immediately attempts to satisfy the framework's dominant share again in the next cycle, leading to an infinite offer-reject loop.

**14. Which metric should an administrator monitor to determine if a specific Mesos role is being resource-starved despite having a configured quota guarantee?**
A) `master/frameworks_inactive`
B) `allocator/mesos/quota/roles/<role>/resources/cpus/offered_or_allocated`
C) `master/cpus_revocable`
D) `agent/cgroups/memory_limit_breaches`
*Answer: B*
**Mastery Explanation:** If the `offered_or_allocated` metric is consistently sitting at the quota guarantee floor, it means the role is consuming everything it is guaranteed but is not receiving any surplus from DRF, indicating starvation.

**15. How does Mesos's two-level scheduling architecture solve the cluster utilization problem?**
A) By allowing frameworks to directly query agent hardware.
B) By dynamically partitioning agents into static silos every 24 hours.
C) By having the Master dictate where and how much to allocate, while frameworks decide what tasks to run.
D) By embedding application-specific scheduling logic inside the Mesos Master.
*Answer: C*
**Mastery Explanation:** This is the core principle of Mesos: separation of concerns. The master offers resources (where/how much), and the framework accepts them to run tasks (what/when), enabling fine-grained multiplexing.

**16. Which configuration allows Spark executors to survive brief Mesos master network disconnects without being killed?**
A) `spark.mesos.rejectOfferDuration`
B) `spark.mesos.executor.docker.image`
C) The `failover_timeout` in the FrameworkInfo protobuf
D) `spark.mesos.coarse`
*Answer: C*
**Mastery Explanation:** The `failover_timeout` dictates how long the master will preserve the framework's resources (and keep its tasks running) after a scheduler disconnects before terminating the executors.

**17. What is the primary difference between setting a Mesos quota `guarantee` vs a DRF `weight`?**
A) Guarantee caps maximum usage; weight sets minimum usage.
B) Guarantee reserves a hard floor before DRF runs; weight dictates proportional share of the surplus.
C) Guarantee applies only to memory; weight applies only to CPU.
D) Guarantee is managed by ZooKeeper; weight is managed by the agent.
*Answer: B*
**Mastery Explanation:** Quota guarantees are subtracted from available cluster capacity first. Only the remaining resources are distributed via DRF according to role weights.

**18. Why is the Mesos unified containerizer often preferred over the Docker daemon for running Spark tasks?**
A) It bypasses Linux cgroups for faster execution.
B) It avoids the Docker daemon socket, reducing agent overhead and eliminating a single point of failure.
C) It allows Spark to run without any container image.
D) It automatically tunes Spark GC parameters.
*Answer: B*
**Mastery Explanation:** The unified containerizer native to Mesos pulls Docker images and sets up namespaces/cgroups directly, avoiding dependency on the external Docker daemon which can become a bottleneck or failure point.

**19. What triggers Marathon to send a `KillTask` command to Mesos for a Spark History Server instance?**
A) The Mesos master detecting a high dominant share.
B) The Spark History Server completing its event log processing.
C) The application failing its configured HTTP health checks `maxConsecutiveFailures` times.
D) ZooKeeper electing a new master.
*Answer: C*
**Mastery Explanation:** Marathon relies on health checks. If the Spark History Server fails to return a 200 OK consecutively beyond the threshold, Marathon instructs Mesos to kill and reschedule the task.

**20. A Spark Mesos framework registers with `spark.executor.memory=32g` but the cluster only has agents with 16GB of RAM. What is the result?**
A) Spark launches two 16GB executors per agent.
B) Mesos aggregates memory across two agents to satisfy the 32GB request.
C) Spark receives no valid offers and is starved indefinitely.
D) Spark automatically scales down to 16GB.
*Answer: C*
**Mastery Explanation:** Offers are per-agent. If no single agent has 32GB of available RAM, the Spark framework will decline all offers (or ignore them) and the job will never launch tasks.

**21. In coarse-grained mode, when does Spark release Mesos executor resources back to the cluster?**
A) Immediately after a task finishes.
B) When the DRF allocator requests preemption.
C) Only when the entire Spark application terminates.
D) When the `spark.mesos.rejectOfferDuration` expires.
*Answer: C*
**Mastery Explanation:** Coarse-grained mode acquires executors and holds them for the lifetime of the SparkContext. Fine-grained mode (deprecated in modern Spark) releases resources per task.

**22. Which protocol does a Spark framework use to receive resource offers from a modern Mesos master?**
A) HTTP/2 or Mesos's binary protobuf over a persistent TCP connection.
B) REST polling every 5 seconds.
C) UDP broadcasts.
D) ZooKeeper watches.
*Answer: A*
**Mastery Explanation:** Offers stream over a persistent connection (either HTTP/2 using Server-Sent Events/RecordIO or native protobuf TCP), eliminating the need for client polling.

**23. If a Mesos agent goes offline entirely, which system is responsible for rescheduling the failed tasks?**
A) The Mesos Master
B) ZooKeeper
C) The Framework Scheduler (e.g., Spark Driver or Marathon)
D) The Agent itself upon reboot
*Answer: C*
**Mastery Explanation:** Mesos Master merely reports the `TASK_LOST` or `TASK_UNREACHABLE` status. The framework scheduler contains the business logic to decide if, when, and how to reschedule the task.

**24. What is the role of `cpu.shares` in Mesos cgroup enforcement?**
A) It strictly limits the maximum CPU cycles an executor can use.
B) It guarantees a proportional weight of CPU time during contention, but allows bursting if idle.
C) It triggers the Linux OOM killer if exceeded.
D) It pins the executor to specific NUMA nodes.
*Answer: B*
**Mastery Explanation:** Unlike `memory.limit_in_bytes` which is a hard cap, `cpu.shares` is a soft limit. An executor can consume unused CPU cycles beyond its requested amount unless `cfs_quota` (hard CPU limiting) is strictly enforced.

**25. When setting `spark.cores.max=20` and `spark.executor.cores=2`, how many executors will Spark attempt to launch on Mesos?**
A) 20
B) 2
C) 10
D) 40
*Answer: C*
**Mastery Explanation:** Spark divides the total requested max cores (20) by the cores per executor (2) to determine the target number of executors (10).

## 3. Small Twist Questions

**26. Scenario:** A Marathon app has `gracePeriodSeconds: 10`. It takes the JVM 30 seconds to start.
**Twist:** You increase `gracePeriodSeconds` to `120`. What happens?
A) The app starts successfully because Marathon waits up to 120s before counting health check failures.
B) Mesos kills the app after 10s because the agent's timeout overrides Marathon's.
C) Marathon kills the app after 30s.
D) The JVM startup time drops to 10s.
*Answer: A*
**Mastery Explanation:** The `gracePeriodSeconds` tells Marathon to ignore failed health checks during the initial warmup window. Increasing it allows the JVM time to initialize before Marathon starts enforcing the `maxConsecutiveFailures` threshold.

**27. Scenario:** Spark runs with `spark.mesos.role=*` (default).
**Twist:** You change it to `spark.mesos.role=spark-production` which has a Mesos quota guarantee of 100 CPUs. What changes in offer behavior?
A) Spark receives offers purely based on weight, competing equally with all frameworks.
B) Spark is guaranteed up to 100 CPUs from the cluster before DRF calculates surplus for other roles.
C) Spark is limited to exactly 100 CPUs and can never burst.
D) Mesos master rejects the framework registration.
*Answer: B*
**Mastery Explanation:** Registering under a role with a guarantee means the master sets aside those resources. Spark will be offered up to 100 CPUs unconditionally, insulating it from starvation by other greedy frameworks.

**28. Scenario:** A Spark job has `spark.executor.memory=4g`. Agents have 4GB total RAM.
**Twist:** Spark requires 384MB of overhead memory per executor. Will the executor launch?
A) Yes, Mesos ignores overhead.
B) No, Spark requests 4g + 384MB (4480MB), which exceeds the 4GB agent capacity. Offers are declined.
C) Yes, the agent uses swap space automatically.
D) No, because Mesos requires 8GB minimum per agent.
*Answer: B*
**Mastery Explanation:** Spark always adds `spark.executor.memoryOverhead` (default 10% or 384MB min) to the executor memory request. The total requested in the Mesos `TaskInfo` protobuf will exceed the 4GB agent size, meaning the offer cannot satisfy the request.

**29. Scenario:** Two roles exist: A (weight 1.0) and B (weight 2.0).
**Twist:** You add a quota guarantee to Role A for 80% of the cluster. Who gets more resources during heavy load?
A) Role B, because its weight is higher.
B) Role A, because its guarantee is satisfied first before weights apply to the remaining 20%.
C) They share equally.
D) The cluster deadlocks.
*Answer: B*
**Mastery Explanation:** Quota guarantees take absolute precedence over DRF weights. Role A gets its 80% floor, and the remaining 20% is distributed via DRF where Role B gets twice the share of Role A for that surplus.

**30. Scenario:** A framework declines an offer. The master re-offers it to the next framework in the DRF sort.
**Twist:** The framework accepted the offer but the agent crashed immediately before launching the cgroup. What does the Master do?
A) Assumes the resources are in use forever.
B) Receives a connection drop from the agent, invalidates the agent's resources, and sends `TASK_LOST`.
C) Silently re-offers the resources on a different agent.
D) Restarts the master process.
*Answer: B*
**Mastery Explanation:** Master-Agent communication uses persistent connections. If the agent crashes, the master detects the disconnect, marks the agent as unreachable, and updates the framework with `TASK_LOST` (or `TASK_UNREACHABLE`).

**31. Scenario:** Spark driver sets `failover_timeout` to 0.
**Twist:** The driver experiences a 1-second network blip to the Mesos master. What happens to the running executors?
A) Nothing, they continue running.
B) The master immediately terminates all executors associated with the framework because the failover window is zero.
C) The driver crashes.
D) ZooKeeper restarts the driver.
*Answer: B*
**Mastery Explanation:** A `failover_timeout` of 0 tells the master NOT to preserve framework state upon disconnect. A brief network blip causes the master to tear down the entire application immediately.

**32. Scenario:** Mesos agents are configured with `--agent_ping_timeout=15s`.
**Twist:** You deploy a deep learning framework that causes 30-second network stalls on the agent NIC. What happens?
A) The Master considers the agent dead and sends `TASK_LOST` for all tasks on it.
B) The Master pauses DRF allocation.
C) The agent kills the deep learning job.
D) ZooKeeper evicts the agent.
*Answer: A*
**Mastery Explanation:** If the master misses heartbeats beyond the timeout, it assumes the agent has failed. In a shared cluster, noisy neighbors causing network stalls can inadvertently cause cluster-wide task evictions if heartbeats fail.

**33. Scenario:** Spark is configured with `spark.mesos.rejectOfferDuration=5s`.
**Twist:** You change it to `5m`. What is the impact on job startup time if the cluster is highly fragmented?
A) Startup is faster because Spark holds offers longer.
B) Startup is much slower. If Spark declines a fragmented offer, it will not see those resources again for 5 minutes, even if they become defragmented.
C) No impact, DRF ignores this setting.
D) The master crashes due to memory overflow.
*Answer: B*
**Mastery Explanation:** A long refuse duration prevents busy-waiting but means Spark is blind to those resources for the entire duration. If resources free up and defragment, Spark won't get them until the 5 minutes expire, delaying startup.

**34. Scenario:** A Marathon app specifies `"constraints": [["hostname", "UNIQUE"]]` with 5 instances.
**Twist:** The cluster only has 3 healthy agents. How many instances run?
A) 5, Mesos ignores constraints if resources are tight.
B) 5, placing 2 instances on two of the agents.
C) 3 instances run, 2 stay in 'Waiting' state indefinitely because the UNIQUE constraint prevents co-location.
D) 0, the deployment fails immediately.
*Answer: C*
**Mastery Explanation:** Framework constraints are hard rules evaluated against the offer's attributes. Marathon will accept offers for the first 3 agents, but decline all further offers because they violate the UNIQUE constraint.

**35. Scenario:** Spark Coarse-Grained mode accepts an offer and launches an executor.
**Twist:** You switch to Fine-Grained mode (legacy). What is the main architectural difference in offer handling?
A) Fine-Grained mode uses HTTP/2 exclusively.
B) Fine-Grained mode accepts offers to launch individual Spark tasks as Mesos tasks, incurring heavy latency per task.
C) Fine-Grained mode bypasses DRF.
D) Fine-Grained mode ignores cgroups.
*Answer: B*
**Mastery Explanation:** In coarse-grained mode, Mesos tasks are long-running Spark Executors. In fine-grained mode, each Mesos task corresponds to a single Spark Task, meaning the driver must negotiate with Mesos for every single map/reduce partition, adding massive latency.

**36. Scenario:** A framework specifies `cpus: 10` and `mem: 1024`.
**Twist:** Another framework specifies `cpus: 1` and `mem: 10240`. If the cluster has 100 CPUs and 100000 MB RAM, who has the higher dominant share?
A) The first framework (10/100 = 10% CPU vs 1% Mem -> 10% Dominant).
B) The second framework (1/100 = 1% CPU vs 10240/100000 = 10.24% Mem -> 10.24% Dominant).
C) They are equal.
D) DRF cannot compare CPU and Memory.
*Answer: B*
**Mastery Explanation:** Framework 1's dominant share is 10% (CPU). Framework 2's dominant share is 10.24% (Memory). Therefore, Framework 2 has a higher dominant share and will be deprioritized compared to Framework 1 in the next allocation cycle.

**37. Scenario:** ZooKeeper session timeout is 10s.
**Twist:** A GC pause on the Mesos Master lasts 12s. What happens?
A) Master recovers instantly with no side effects.
B) ZK drops the master's ephemeral node. Standby master takes over. Agents and frameworks undergo a reconnection storm.
C) Agents kill all running tasks.
D) ZK restarts.
*Answer: B*
**Mastery Explanation:** If the master fails to ping ZK within the session timeout, ZK expires the session. A standby master becomes leader, triggering a massive wave of framework and agent reconnections to reconstruct the state.

**38. Scenario:** A Marathon app has `minimumHealthCapacity: 1.0` during an upgrade.
**Twist:** You change it to `0.0`. How does the deployment behavior change?
A) It spawns double the instances before killing old ones.
B) It kills the old instances FIRST before launching the new ones, causing downtime but requiring no extra cluster capacity.
C) It ignores health checks entirely.
D) It rolls back to the previous version.
*Answer: B*
**Mastery Explanation:** `minimumHealthCapacity: 0.0` tells Marathon it is acceptable to have 0% healthy instances during an upgrade. It will tear down the old version to free up resources before launching the new one, which is useful in resource-constrained clusters but causes downtime.

**39. Scenario:** The cluster has 5 ZooKeeper nodes.
**Twist:** 2 ZooKeeper nodes lose power. Does Mesos continue to function?
A) No, it requires 100% ZK availability.
B) Yes, 3 nodes form a majority quorum (3 out of 5), so leader election and discovery continue normally.
C) No, Mesos enters read-only mode.
D) Yes, but no new frameworks can register.
*Answer: B*
**Mastery Explanation:** ZooKeeper requires a strict majority `(N/2) + 1` to operate. With 5 nodes, the quorum is 3. Losing 2 nodes leaves 3, which is sufficient to maintain consensus.

**40. Scenario:** A Spark task causes a native memory leak (e.g., in a JNI C++ library) inside the executor.
**Twist:** The `spark.executor.memory` is tuned perfectly for the JVM heap, but `memoryOverhead` is left at default. What happens?
A) The JVM throws OutOfMemoryError.
B) The C++ library gracefully degrades.
C) The native leak causes total container memory to exceed the Mesos cgroup limit, triggering an OS OOM kill (`TASK_FAILED`).
D) Mesos dynamically increases the cgroup limit.
*Answer: C*
**Mastery Explanation:** Mesos cgroups track total process tree memory, including native allocations outside the JVM heap. If the native leak pushes total usage past the cgroup hard limit, the kernel kills it, resulting in a sudden executor exit without a JVM stack trace.

## 4. Coding & Debugging Questions

**41. Debugging a Deadlock:**
You observe that Spark has accepted 100 CPUs but is launching 0 tasks. The Mesos master UI shows Spark's dominant share is 50%, but `master/frameworks_inactive` is false. What configuration is missing?
*Answer & Mastery Explanation:*
`spark.cores.max` is likely missing or set too high, AND `spark.executor.cores` is misaligned with the agent size, causing Spark to hold partial resources. Because it hasn't reached its target to launch executors, it hoards offers. Setting `spark.mesos.rejectOfferDuration` forces it to return unusable offers, breaking the deadlock.

**42. Identifying an OOM source:**
A Spark job on Mesos fails. The Spark UI shows Executor Lost with exit code 137. The JVM logs show no `java.lang.OutOfMemoryError`. What happened and how do you fix it?
*Answer & Mastery Explanation:*
Exit code 137 (128 + 9 for SIGKILL) is the Linux OOM Killer destroying the cgroup. The executor exceeded its physical memory limit. Because there is no JVM OOM, the memory was consumed off-heap (e.g., PySpark memory, native libraries, or Tungsten off-heap execution). Fix: Increase `spark.executor.memoryOverhead`.

**43. Tuning DRF Weights:**
You execute `curl -X PUT http://master/roles -d '{"roles":[{"name":"analytics", "weight": 99.0}]}'`. Immediately, production web services in the `marathon` role begin to time out and tasks enter `TASK_LOST`. Why?
*Answer & Mastery Explanation:*
By assigning an extreme weight to analytics without first ensuring Marathon has a `quota guarantee`, DRF starves Marathon of any available resources. Any Marathon task that restarts or scales up cannot get an offer because the analytics role's dominant share calculation suppresses other roles. Fix: Always set quota guarantees for production services.

**44. ZK Disconnect Storm:**
Every 24 hours at 2 AM, all Spark jobs on Mesos fail with `Disconnected from Master`. Network metrics show a brief 15-second latency spike on the ZK ensemble at 2 AM due to backup snapshots. How do you fix the Mesos configuration?
*Answer & Mastery Explanation:*
The `--zk_session_timeout` on the Mesos master and agents is likely set to the default (10s). The 15s latency spike causes ZK to expire the leader session, triggering a failover. Fix: Increase `--zk_session_timeout` to `30s` to tolerate the backup-induced latency.

**45. Analyzing Offer Declines:**
Your custom Mesos scheduler script logs:
`-> DECLINE: Insufficient resources (need 4 cpus, 8192MB; got 8 cpus, 4096MB)` continuously.
The cluster has 100 agents, each with 8 CPUs and 4GB RAM. How do you fix the framework request?
*Answer & Mastery Explanation:*
The framework is requesting 8GB of RAM per task, but the maximum physical capacity of any single agent is 4GB. Mesos offers cannot span multiple agents. The framework must be reconfigured to request `<= 4096MB` (accounting for overhead) or deployed to a cluster with larger VMs.

**46. Handling `TASK_UNREACHABLE`:**
Spark is registered with `PARTITION_AWARE`. An agent goes offline for 2 minutes and returns. Spark had a running task on it. Does Spark re-run the task?
*Answer & Mastery Explanation:*
It depends on Spark's configuration. If the agent returns before Spark's internal timeout for unreachable tasks expires, the task status is updated to `TASK_RUNNING` (or completed) and duplicate execution is avoided. If the timeout expired, Spark already scheduled a speculative duplicate.

**47. Containerizer Selection:**
A cluster uses the Docker containerizer (`type: DOCKER`). Operators notice that when the `dockerd` daemon hangs, Mesos agents cannot launch any tasks and report `TASK_FAILED`, even though the agent process is healthy. What is the architectural fix?
*Answer & Mastery Explanation:*
Migrate from the Docker containerizer to the Mesos unified containerizer (`type: MESOS` with `docker.image`). The unified containerizer uses native Mesos isolators (cgroups/namespaces) to run Docker images without relying on the external Docker daemon, removing the single point of failure.

**48. Quota Limit vs Guarantee:**
You configure:
`"guarantee": [{"name": "cpus", "scalar": {"value": 10}}], "limit": [{"name": "cpus", "scalar": {"value": 10}}]`
For a Spark role. A user submits a job requesting 20 CPUs. What happens?
*Answer & Mastery Explanation:*
The framework will only ever be offered 10 CPUs total. If `spark.cores.max` is 20 and `spark.executor.cores` is 2, it will launch 5 executors and then hang indefinitely waiting for more offers (which it will never receive due to the limit).

**49. The `*` (Wildcard) Role Pitfall:**
A user submits a Spark job but forgets to set `spark.mesos.role`. The job runs fine in dev, but in production, it is severely throttled. Why?
*Answer & Mastery Explanation:*
Without a role, the framework defaults to the `*` (wildcard) role. In production, named roles likely have quota guarantees taking up most of the cluster. The wildcard role gets zero guarantee and only receives the scrap surplus resources via DRF, leading to starvation.

**50. Analyzing DRF sorting:**
Framework A (Role: dev) uses 10/100 CPUs and 50/1000 GB RAM. (Dom share: 10% CPU).
Framework B (Role: prod) uses 5/100 CPUs and 100/1000 GB RAM. (Dom share: 10% RAM).
Role prod has a weight of 2.0. Role dev has a weight of 1.0.
Who gets the next offer?
*Answer & Mastery Explanation:*
DRF divides the dominant share by the weight.
A's weighted share: 10% / 1.0 = 10%.
B's weighted share: 10% / 2.0 = 5%.
Because 5% < 10%, Framework B (prod) has the lower weighted dominant share and is sorted first to receive the next offer.
