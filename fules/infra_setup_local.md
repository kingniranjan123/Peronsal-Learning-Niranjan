# Local Infrastructure Setup Guide for Apache Spark (Windows)

This guide covers how to set up Apache Spark manually on your local Windows machine without using a Virtual Machine (VM) or Docker.

## 💻 Infrastructure Requirements
- **OS:** Windows 10/11
- **RAM:** Minimum 8GB (16GB recommended for running clusters and Spark Web UI locally)
- **CPU:** Minimum 4 Cores (to allow multiple executors and background tasks)
- **Disk Space:** At least 20GB free space

---

## 🛠️ Step-by-Step Installation Guide

### Step 1: Install Java (JDK 8 or 11)
Spark runs on the Java Virtual Machine (JVM).
1. Download and install **Java SE Development Kit 8 (JDK 8)** or **JDK 11** from Oracle or AdoptOpenJDK.
2. Set the `JAVA_HOME` environment variable:
   - Go to System Properties -> Environment Variables.
   - Add a new System Variable named `JAVA_HOME` pointing to your JDK installation path (e.g., `C:\Program Files\Java\jdk1.8.0_xxx`).
   - Add `%JAVA_HOME%\bin` to your `Path` variable.

### Step 2: Download and Extract Apache Spark
1. Go to the [Apache Spark Downloads page](https://spark.apache.org/downloads.html).
2. Choose a recent Spark release (e.g., Spark 3.x) and a package type pre-built for Apache Hadoop (e.g., "Pre-built for Apache Hadoop 3.3 and later").
3. Download the `.tgz` file.
4. Extract the file to a directory with no spaces in the path, for example, `C:\spark`. (You can use a tool like 7-Zip or Git Bash to extract `.tgz` files on Windows).

### Step 3: Set Up Hadoop `winutils.exe` (Windows Specific)
Spark requires Hadoop libraries to access the local file system properly. On Windows, this requires a helper executable called `winutils.exe`.
1. Create a folder named `hadoop` in your `C:` drive (e.g., `C:\hadoop\bin`).
2. Download the `winutils.exe` binary that matches the Hadoop version of your Spark download from a reliable GitHub repository (e.g., `cdarlint/winutils`).
3. Place `winutils.exe` inside `C:\hadoop\bin`.
4. Set the `HADOOP_HOME` environment variable to `C:\hadoop`.

### Step 4: Configure Spark Environment Variables
1. Set the `SPARK_HOME` system variable to point to your Spark directory (e.g., `C:\spark`).
2. Add `%SPARK_HOME%\bin` to your `Path` variable.

---

## 🚀 Manual Operations: Running Spark

### 1. Starting the Spark Shell (Interactive Mode)
To test if Spark is working, open a Command Prompt or PowerShell and type:
```cmd
spark-shell
```
This will launch the Spark REPL (Read-Eval-Print Loop) in Scala. You should see the Spark ASCII art logo and a `scala>` prompt. You can now write Spark code interactively.
*To exit, type `:quit`.*

### 2. Starting a Local Standalone Cluster Manually
For testing distributed operations, you can start a standalone cluster on your local machine.

**Start the Master:**
Open a Command Prompt and run:
```cmd
spark-class org.apache.spark.deploy.master.Master
```
Look at the logs to find the master URL, which will look something like `spark://<your-ip>:7077`.

**Start a Worker:**
Open a *new* Command Prompt window and run the worker, passing the master URL:
```cmd
spark-class org.apache.spark.deploy.worker.Worker spark://<your-ip>:7077
```

### 3. Accessing the Web UIs
Once your applications or cluster are running, you can monitor them through your web browser:
- **Spark Application Web UI:** `http://localhost:4040` (Available whenever a SparkContext is running, like when `spark-shell` is active).
- **Standalone Master Web UI:** `http://localhost:8080` (Shows registered workers and running applications).
- **Standalone Worker Web UI:** `http://localhost:8081` (Shows the executors running on this specific worker).

### 4. Submitting a Compiled Application
Once you compile your Spark application into a JAR file (using Maven or SBT), you submit it to your local cluster using `spark-submit`:
```cmd
spark-submit --class com.yourcompany.YourMainClass --master spark://<your-ip>:7077 path/to/your/app.jar
```
