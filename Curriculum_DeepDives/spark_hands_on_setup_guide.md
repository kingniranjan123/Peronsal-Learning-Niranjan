# Apache Spark Hands-On Lab Architecture and Setup Guide

This guide gives you a complete local architecture for practicing Spark concepts from the book with real datasets, repeatable commands, and a clear understanding of where each concept fits. The goal is not merely to install Spark. The goal is to build a small production-style data engineering workstation where you can ingest data, transform it, persist curated datasets, run SQL and ML exercises, and visualize outputs through a lightweight application.

## Full Lab Architecture

```mermaid
flowchart LR
    subgraph DS["Data Sources"]
        CSV["CSV / JSON downloads\nNYC Taxi, MovieLens, e-commerce logs"]
        API["Optional APIs\nweather, events, clickstream simulators"]
        KAFKA["Optional Kafka topic\nstreaming records"]
    end

    subgraph LAND["Landing Zone: data/raw"]
        RAW["Raw immutable files\nkeep original downloaded data"]
    end

    subgraph SPARK["Spark Processing Layer"]
        SESSION["SparkSession\nentry point"]
        BRONZE["Bronze validation\nschema, corrupt records, counts"]
        SILVER["Silver transformation\nclean, typed, deduplicated"]
        GOLD["Gold marts\naggregates, features, dashboards"]
        ML["MLlib exercises\nfeatures, training, evaluation"]
    end

    subgraph STORAGE["Local Storage and Serving"]
        PARQUET["Parquet datasets\ndata/silver and data/gold"]
        PG["PostgreSQL\nserving tables"]
        FILES["Output files\nreports, checkpoints, model artifacts"]
    end

    subgraph APP["User-Facing Layer"]
        STREAMLIT["Streamlit dashboard\ncharts and filters"]
        NOTEBOOK["Jupyter / PyCharm\ninteractive exploration"]
    end

    CSV --> RAW
    API --> RAW
    KAFKA --> SESSION
    RAW --> SESSION --> BRONZE --> SILVER --> GOLD
    SILVER --> PARQUET
    GOLD --> PARQUET
    GOLD --> PG
    SILVER --> ML --> FILES
    PG --> STREAMLIT
    PARQUET --> NOTEBOOK
```

## What Each Layer Teaches

The `data/raw` layer teaches ingestion discipline. Raw data should be preserved exactly as downloaded so you can rerun experiments when parsing logic changes. The `bronze` layer teaches schema handling, bad-record capture, null analysis, and row-count validation. The `silver` layer teaches transformations, joins, partitioning, caching, and file format choices. The `gold` layer teaches user-facing aggregation: facts, dimensions, metrics, dashboards, and feature tables. The optional PostgreSQL layer teaches JDBC reads and writes. The Streamlit layer teaches how Spark output becomes a product experience.

This architecture also makes every Spark concept visible. RDDs and DataFrames appear when you load and transform data. Actions appear when you call `count`, `show`, `write`, or `collect`. Shuffles appear during joins and groupBy operations. Partitioning appears when you decide how many output files are produced. Broadcast variables appear when joining a small lookup table to a large fact table. Spark SQL appears when querying the curated Parquet data. MLlib appears when converting cleaned business columns into feature vectors.

## Windows Setup Commands

Open **PowerShell as Administrator** for system installs. Use normal PowerShell inside your project folder for Python commands.

### 1. Install Java 11

Spark runs on the JVM. Install Eclipse Temurin JDK 11 from Adoptium, then verify:

```powershell
java -version
```

Set `JAVA_HOME` if the installer did not do it:

```powershell
[Environment]::SetEnvironmentVariable("JAVA_HOME", "C:\Program Files\Eclipse Adoptium\jdk-11", "User")
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-11"
$env:Path = "$env:JAVA_HOME\bin;$env:Path"
java -version
```

### 2. Install Python and Create a Project

Install Python 3.10 or 3.11 from `python.org`, then create a clean lab folder:

```powershell
mkdir C:\spark-labs
cd C:\spark-labs
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install pyspark==3.5.1 pandas pyarrow jupyter streamlit plotly sqlalchemy psycopg2-binary
```

If PowerShell blocks venv activation, run:

```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
.\.venv\Scripts\Activate.ps1
```

### 3. Install Apache Spark Locally

Download Spark pre-built for Hadoop 3 from the Apache Spark downloads page. Extract it to `C:\spark\spark-3.5.1-bin-hadoop3`.

```powershell
[Environment]::SetEnvironmentVariable("SPARK_HOME", "C:\spark\spark-3.5.1-bin-hadoop3", "User")
$env:SPARK_HOME = "C:\spark\spark-3.5.1-bin-hadoop3"
$env:Path = "$env:SPARK_HOME\bin;$env:Path"
spark-submit --version
```

On Windows, some Spark file operations expect Hadoop native utilities. Create `C:\hadoop\bin`, place a matching `winutils.exe` there, and set:

```powershell
[Environment]::SetEnvironmentVariable("HADOOP_HOME", "C:\hadoop", "User")
$env:HADOOP_HOME = "C:\hadoop"
$env:Path = "$env:HADOOP_HOME\bin;$env:Path"
```

### 4. Create the Lab Folder Structure

```powershell
cd C:\spark-labs
mkdir data
mkdir data\raw
mkdir data\bronze
mkdir data\silver
mkdir data\gold
mkdir jars
mkdir notebooks
mkdir src
mkdir checkpoints
mkdir models
```

### 5. Download Practice Datasets

Start with a small dataset so the pipeline works, then scale up.

MovieLens small dataset:

```powershell
cd C:\spark-labs\data\raw
Invoke-WebRequest -Uri "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip" -OutFile "ml-latest-small.zip"
Expand-Archive .\ml-latest-small.zip -DestinationPath .
```

NYC Taxi data is better for partitioning and file-format labs. Download one month first:

```powershell
cd C:\spark-labs\data\raw
Invoke-WebRequest -Uri "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet" -OutFile "yellow_tripdata_2024-01.parquet"
```

For larger practice, add more months later:

```powershell
Invoke-WebRequest -Uri "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet" -OutFile "yellow_tripdata_2024-02.parquet"
Invoke-WebRequest -Uri "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet" -OutFile "yellow_tripdata_2024-03.parquet"
```

## First Spark Processing Exercise

Create `src\01_taxi_bronze_to_gold.py`:

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, to_date

spark = (
    SparkSession.builder
    .appName("TaxiBronzeSilverGold")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

raw_path = "data/raw/yellow_tripdata_2024-01.parquet"
silver_path = "data/silver/taxi_trips"
gold_path = "data/gold/taxi_daily_metrics"

raw = spark.read.parquet(raw_path)

bronze = raw.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "total_amount",
)

silver = (
    bronze
    .filter(col("trip_distance") > 0)
    .filter(col("fare_amount") >= 0)
    .withColumn("pickup_date", to_date(col("tpep_pickup_datetime")))
)

silver.write.mode("overwrite").partitionBy("pickup_date").parquet(silver_path)

gold = (
    silver
    .groupBy("pickup_date")
    .agg(
        count("*").alias("trip_count"),
        avg("trip_distance").alias("avg_distance"),
        avg("total_amount").alias("avg_total_amount"),
    )
    .orderBy("pickup_date")
)

gold.write.mode("overwrite").parquet(gold_path)
gold.show(20, truncate=False)

spark.stop()
```

Run it:

```powershell
cd C:\spark-labs
.\.venv\Scripts\Activate.ps1
python .\src\01_taxi_bronze_to_gold.py
```

What to observe:

1. The read path demonstrates DataFrame loading and Parquet vectorized scans.
2. The filters demonstrate transformations and lazy evaluation.
3. The `write` calls are actions.
4. `partitionBy("pickup_date")` demonstrates physical layout.
5. `groupBy` creates a shuffle.
6. `spark.sql.shuffle.partitions` controls reduce-side task count.

## PostgreSQL Serving Layer

Install PostgreSQL 15 or 16, create a database named `spark_lab`, and download the PostgreSQL JDBC driver into `C:\spark-labs\jars`.

Create a database:

```powershell
psql -U postgres
CREATE DATABASE spark_lab;
\q
```

Write a gold table to PostgreSQL:

```python
gold.write.mode("overwrite").format("jdbc").options(
    url="jdbc:postgresql://localhost:5432/spark_lab",
    driver="org.postgresql.Driver",
    dbtable="taxi_daily_metrics",
    user="postgres",
    password="YOUR_PASSWORD"
).save()
```

Run with the driver:

```powershell
spark-submit --jars .\jars\postgresql-42.7.3.jar .\src\01_taxi_bronze_to_gold.py
```

## Streamlit Dashboard

Create `src\app.py`:

```python
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

st.set_page_config(page_title="Spark Taxi Metrics", layout="wide")
st.title("Spark Gold Metrics Dashboard")

engine = create_engine("postgresql+psycopg2://postgres:YOUR_PASSWORD@localhost:5432/spark_lab")
df = pd.read_sql("select * from taxi_daily_metrics order by pickup_date", engine)

st.metric("Days Loaded", len(df))
st.line_chart(df.set_index("pickup_date")[["trip_count", "avg_total_amount"]])
st.dataframe(df, use_container_width=True)
```

Start it:

```powershell
streamlit run .\src\app.py
```

This dashboard is intentionally downstream of Spark. Spark performs the heavy distributed work; Streamlit reads curated serving data. That separation mirrors production architectures where batch jobs produce trusted tables and applications consume them.

## Optional Kafka Streaming Lab

Use Docker Desktop and create a Kafka topic for streaming concepts:

```powershell
docker run -d --name kafka -p 9092:9092 apache/kafka:3.7.0
```

For a more realistic multi-service setup, use Docker Compose with Kafka and PostgreSQL. Streaming exercises should write checkpoints:

```python
stream.writeStream \
    .format("parquet") \
    .option("path", "data/bronze/events") \
    .option("checkpointLocation", "checkpoints/events") \
    .start()
```

The checkpoint folder is where Spark stores streaming progress and state. It connects directly to the concepts of fault tolerance, exactly-once sinks, window operations, and stateful streaming.

## Concept Fit Map

| Concept | Where It Appears in This Lab |
| :--- | :--- |
| SparkSession | Created at the start of every script as the driver entry point. |
| DataFrames | Used for taxi and MovieLens transformations. |
| Actions | `show`, `count`, `write.parquet`, and `write.jdbc`. |
| Transformations | `filter`, `select`, `withColumn`, `groupBy`, and `agg`. |
| RDD Lineage / DAG | Built lazily until an action triggers execution. |
| Shuffling | Created by `groupBy`, joins, repartitioning, and sorting. |
| Partitioning | Controlled by input files, `repartition`, `coalesce`, and `partitionBy`. |
| Broadcast Variables | Used when joining a small lookup table such as taxi zones. |
| Spark SQL | Used by registering temp views and querying curated data. |
| File Formats | CSV/JSON as raw ingestion, Parquet as curated analytical storage. |
| MLlib | Uses silver data to build features, train models, and persist model artifacts. |
| Streaming | Kafka input plus checkpointed file or database sink. |
| Spark UI | Used to inspect jobs, stages, tasks, shuffle read/write, and storage. |

## Execution Checklist

1. Install Java 11 and verify `java -version`.
2. Install Python and create `.venv`.
3. Install `pyspark`, `pandas`, `pyarrow`, `streamlit`, `plotly`, and database libraries.
4. Download Spark and set `SPARK_HOME`.
5. Create the project folder structure.
6. Download MovieLens and one month of NYC Taxi data.
7. Run the bronze-to-gold taxi script.
8. Open Spark UI at `http://localhost:4040` while a job is running.
9. Write gold metrics to PostgreSQL.
10. Start Streamlit and validate the dashboard.

If a command fails, check environment variables first: `JAVA_HOME`, `SPARK_HOME`, `HADOOP_HOME`, and your active Python virtual environment. Most local Spark issues come from version mismatch, missing Java, or Windows paths not being available in the terminal session that runs Spark.
