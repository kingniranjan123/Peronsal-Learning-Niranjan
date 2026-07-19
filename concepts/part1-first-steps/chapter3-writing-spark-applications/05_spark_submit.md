# Submitting Applications with spark-submit

**The universal entry point and command-line utility for deploying packaged Apache Spark applications to any supported cluster manager.**

## Why It Matters
Writing Spark code in an IDE is only half the battle. To leverage the distributed power of Spark, your code must execute on a cluster. `spark-submit` is the bridge between your compiled application (the uberjar) and the cluster hardware. It is the standardized command-line tool used by data engineers, schedulers (like Airflow or Oozie), and CI/CD pipelines to launch Spark jobs. Mastering `spark-submit` means understanding how to allocate resources (memory and CPU), define the execution environment, and troubleshoot deployment issues across different cluster managers like YARN, Kubernetes, or Standalone. Without it, your application remains trapped on your local machine.

## How It Works
The `spark-submit` script is bundled with every Spark installation (located in the `bin/` directory). When you run it, it acts as a client that communicates with the chosen Cluster Manager to negotiate resources and launch the Driver and Executor JVMs.

The command relies heavily on a series of flags and arguments to configure the execution. 
The syntax generally follows this pattern:
`./bin/spark-submit [options] <application-jar> [application-arguments]`

**Key Execution Flags:**
*   `--class`: The fully qualified name of the main class containing the `main()` method (e.g., `com.example.MyApp`).
*   `--master`: The cluster manager to connect to. 
    *   `local[*]`: Run locally using all logical cores.
    *   `spark://HOST:PORT`: Connect to a Spark Standalone cluster.
    *   `yarn`: Connect to a Hadoop YARN cluster.
    *   `k8s://...`: Connect to a Kubernetes cluster.
*   `--deploy-mode`: Defines where the Driver program runs.
    *   `client`: The Driver runs on the machine where you execute the `spark-submit` command. Useful for interactive debugging, as logs print to your console. However, if your laptop disconnects, the job dies.
    *   `cluster`: The Driver runs inside a container on the cluster itself. The `spark-submit` command simply requests the job and exits. This is the mandatory mode for production, ensuring fault tolerance if the submitting machine goes down.

**Resource Allocation Flags:**
*   `--executor-memory`: Memory allocated per executor (e.g., `4G`).
*   `--num-executors`: The total number of executor JVMs to launch (YARN only).
*   `--executor-cores`: The number of concurrent tasks an executor can run (e.g., `2`).
*   `--driver-memory`: Memory allocated for the Driver program (important if you are using `collect()` or broadcasting large variables).

**Configuration Flags:**
*   `--conf`: Used to pass arbitrary Spark configuration properties dynamically (e.g., `--conf spark.sql.shuffle.partitions=200`).

When submitting, `spark-submit` uploads your JAR file and any specified dependencies to the cluster, initializes the environment, and triggers the execution pipeline defined in your code.

## Flow Diagram

```
# Architecture Diagram
# (See MD source for diagram code)
graph TD
    A[Developer / Airflow] -->|Executes| B(spark-submit CLI);
    
    B --> C{--deploy-mode};
    
    C -- "client" --> D[Driver JVM starts on Local Machine];
    D --> E[Driver contacts Cl...
```

## Data Visualization

**Deploy Mode Comparison:**

| Feature | Client Mode | Cluster Mode |
| :--- | :--- | :--- |
| **Driver Location** | Submitting machine (e.g., Gateway node or laptop) | Inside a cluster node (e.g., ApplicationMaster in YARN) |
| **Network Latency** | High (Driver must communicate with Executors across network) | Low (Driver is on the same internal network as Executors) |
| **Fault Tolerance** | Low (If submitting machine dies, job fails) | High (Cluster manager can restart Driver if it fails) |
| **Use Case** | Development, Debugging, Spark Shell | Production jobs, Scheduled pipelines |
| **Log Accessibility** | Printed directly to standard output/console | Stored in cluster logs (e.g., YARN resource manager UI) |

## Code Example

**Example 1: Running Locally (Testing)**
```bash
# Good for quick tests on your local machine
./bin/spark-submit \
  --class com.manning.sparkinaction.chapter3.GitHubApp \
  --master local[*] \
  target/scala-2.12/spark-in-action-assembly-1.0.jar \
  /path/to/local/input.json # App arguments follow the jar
```

**Example 2: Deploying to YARN in Production (Cluster Mode)**
```bash
# Standard production deployment
./bin/spark-submit \
  --class com.manning.sparkinaction.chapter3.GitHubApp \
  --master yarn \
  --deploy-mode cluster \
  --driver-memory 2g \
  --executor-memory 4g \
  --num-executors 10 \
  --executor-cores 2 \
  --conf spark.default.parallelism=40 \
  --conf spark.yarn.maxAppAttempts=2 \
  target/scala-2.12/spark-in-action-assembly-1.0.jar \
  hdfs://namenode:8020/data/input.json \
  hdfs://namenode:8020/data/output_dir
```

**Example 3: Submitting a Python (PySpark) Application**
```bash
# PySpark submission doesn't need --class, just the script path
./bin/spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --py-files dependencies.zip \
  my_pyspark_job.py
```

## Common Pitfalls

*   **Hardcoding `master` in Code:** If your Scala code has `.master("local[*]")` explicitly defined in the `SparkSession.builder()`, it will override whatever you pass to `spark-submit --master`. Always remove the master definition from production code.
*   **Using Client Mode in Production:** Running heavy ETL jobs in client mode from an edge node. If the SSH session drops or the edge node is restarted, the entire Spark job fails because the Driver dies.
*   **Over-allocating Resources:** Requesting `--executor-memory 64G` and `--num-executors 100` on a small cluster. The job will sit in the queue forever (ACCEPTED state) because YARN cannot satisfy the resource request.
*   **Misplacing Application Arguments:** Putting application arguments (like file paths) *before* the JAR file in the command. Everything after the JAR file path is passed to your `main()` method; everything before it is intercepted by `spark-submit`.
*   **Forgetting to distribute Python dependencies:** When submitting PySpark, failing to use `--py-files` to distribute custom Python modules, resulting in `ModuleNotFoundError` on the executors.

## Key Takeaway
`spark-submit` is the definitive tool for transitioning a Spark application from local development to a robust, scalable production environment; mastering its configuration flags is crucial for efficient cluster resource utilization.
