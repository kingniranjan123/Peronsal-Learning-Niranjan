# Standalone Cluster Web UI

**Spark's built-in web interfaces that provide real-time visibility into cluster health, application progress, and executor logs.**

## Why It Matters

In a distributed environment, visibility is everything. When a Spark application spans tens or hundreds of machines, relying solely on terminal output or raw log files becomes impossible. Spark provides a suite of Web User Interfaces (UIs) that expose the internal state of the cluster and running applications. Knowing how to navigate these interfaces is an essential skill for any data engineer. The Web UI allows you to identify resource bottlenecks, find out exactly why an application is pending, analyze task execution skew, and locate the exact log files for a failed executor. Without the UI, tuning and debugging Spark jobs is akin to flying blind.

## How It Works

The Standalone cluster exposes three distinct but interconnected Web UIs:

**1. The Master Web UI (Port 8080)**
The Master UI is the dashboard for the entire cluster's health and resource allocation. By default, it runs on port `8080` (or `8081` if 8080 is taken). 
*   **Workers List:** It displays all registered Worker nodes, their IP addresses, total cores, used cores, total memory, and used memory. If a worker dies, its state changes to `DEAD` here.
*   **Running Applications:** Shows applications currently executing, including how many cores and memory they have been granted versus what they requested.
*   **Completed Applications:** A historical view of recently finished applications and their final state (FINISHED, FAILED, KILLED).

**2. The Worker Web UI (Port 8081)**
Each Worker node runs its own UI, typically on port `8081`. You can access it directly or by clicking a worker's link in the Master UI.
*   **Running Executors:** Shows the specific Executor JVMs currently running on this physical node, which application they belong to, and their resource consumption.
*   **Logs Access:** This is perhaps the most critical feature of the Worker UI. It provides direct hyperlinks to the `stdout` and `stderr` logs for every Executor. When a task throws an exception, the stack trace is written to the Executor's `stderr` log, which you can view directly through this web interface without SSHing into the node.

**3. The Application Web UI (Port 4040)**
The Application UI is bound to the Driver program and runs on port `4040` (incrementing to 4041, 4042 if multiple drivers run on the same host). This UI is arguably the most complex and important for performance tuning.
*   **Jobs Tab:** Shows a timeline of Spark Jobs, broken down into Stages.
*   **Stages Tab:** Displays the Directed Acyclic Graph (DAG) of the execution plan. It highlights task execution times, data shuffled, and data read/written. It is vital for spotting "straggler" tasks (data skew).
*   **Executors Tab:** Shows granular details about each executor assigned to the application, including GC (Garbage Collection) time, active tasks, and memory usage.
*   **SQL Tab:** If using Spark SQL or DataFrames, this tab shows the physical query plans and logical query trees.

When a task fails, you first use the Application UI (port 4040) to identify *which* task failed and on *which* Executor/Worker it was running. You then navigate to the Worker UI (port 8081) for that specific node to inspect the `stderr` logs and find the actual Java/Python exception.

## Flow Diagram

```mermaid
graph TD
    User([Data Engineer])
    
    subgraph Cluster Master Node
        MasterUI[Master Web UI<br>http://master:8080]
    end
    
    subgraph Worker Node 1
        WorkerUI1[Worker Web UI<br>http://worker1:8081]
        Exec1Logs[Executor 1 Logs<br>stdout / stderr]
    end
    
    subgraph Worker Node 2
        WorkerUI2[Worker Web UI<br>http://worker2:8081]
        Exec2Logs[Executor 2 Logs<br>stdout / stderr]
    end
    
    subgraph Driver Machine
        AppUI[Application Web UI<br>http://driver:4040]
    end

    User -->|Views Cluster Health & Registered Workers| MasterUI
    MasterUI -.->|Links to| WorkerUI1
    MasterUI -.->|Links to| WorkerUI2
    
    User -->|Inspects Stage/Task DAG| AppUI
    AppUI -.->|Shows failure on Worker 1| WorkerUI1
    
    User -->|Reads Stack Traces| WorkerUI1
    WorkerUI1 -.->|Serves| Exec1Logs
```

## Data Visualization

| UI Component | Default Port | Lifecycle | Primary Purpose | Key Metrics Shown |
| :--- | :--- | :--- | :--- | :--- |
| **Master UI** | `8080` | Continuous (Daemon) | Cluster-wide resource overview | Total/Used Cores & Memory, Worker Status |
| **Worker UI** | `8081` | Continuous (Daemon) | Node-level execution details | Local Executors, `stdout`/`stderr` logs |
| **Application UI**| `4040` | Tied to App Runtime | Job/Task execution monitoring | DAGs, Shuffle Read/Write, GC Time, Skew |
| **History Server**| `18080` | Continuous (Daemon) | Post-execution analysis | Reconstructs App UI from persisted event logs |

## Code Example

```bash
# Spark automatically assigns ports, but you can override them using environment variables
# or configuration properties in spark-defaults.conf

# 1. Starting a Master with a custom Web UI port (e.g., 9090 instead of 8080)
SPARK_MASTER_WEBUI_PORT=9090 ./sbin/start-master.sh

# 2. Starting a Worker with a custom Web UI port (e.g., 9091 instead of 8081)
SPARK_WORKER_WEBUI_PORT=9091 ./sbin/start-worker.sh spark://master:7077

# 3. Configuring the Application UI port in a PySpark script
```
```python
from pyspark.sql import SparkSession

# By default, the UI binds to 4040. If running multiple jobs on one machine, 
# you can force a specific port or let it auto-increment.
spark = SparkSession.builder \
    .appName("UI-Config-App") \
    .master("spark://master:7077") \
    .config("spark.ui.port", "4050") \
    .config("spark.ui.killEnabled", "true") \
    .getOrCreate()

# The UI is now accessible at http://<driver-ip>:4050
# The spark.ui.killEnabled flag allows users to kill stages directly from the UI.
```

## Common Pitfalls

*   **UI Disappears on Completion:** The Application UI (port 4040) is hosted by the Driver JVM. When the application completes (successfully or fails), the Driver JVM shuts down, and the UI immediately becomes inaccessible. You must use the Spark History Server to view UIs of completed jobs.
*   **Port Collisions:** If port 4040 is in use, Spark will attempt 4041, 4042, etc., up to `spark.port.maxRetries`. If you launch many concurrent `spark-shell` sessions on a single machine, you might exceed the retries and the application will fail to start.
*   **Reverse Proxy Issues:** In cloud environments or behind corporate firewalls, the links provided in the Master UI to the Worker UIs might point to internal IP addresses (e.g., `10.0.x.x`) that your local browser cannot reach. You often need to set `SPARK_PUBLIC_DNS` on the workers to ensure the UI generates resolvable URLs.
*   **Heavy UI Overhead:** By default, the Application UI retains information for the last 1000 jobs/stages. For massive, long-running streaming applications, retaining too much UI data can cause the Driver to run out of memory. This can be tuned via `spark.ui.retainedJobs` and similar configs.

## Key Takeaway

The Spark Web UIs form a triage hierarchy: use the Master UI for resource issues, the Application UI for logical execution and performance bottlenecks, and the Worker UI for low-level stack traces and error logs.
