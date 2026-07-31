# Apache Spark Full-Stack Local Setup Guide

This guide outlines the professional steps required to set up a comprehensive Apache Spark development environment. This architecture will allow you to practice all core Spark concepts using real-world datasets, a backend database, and a frontend interface.

## Architecture Overview

> [!NOTE]
> **Why this architecture?** A true production-like data engineering project doesn't exist in isolation. Connecting PySpark to a relational database (for staging data) and a frontend (for visualization/interaction) simulates real-world workflows.

```text
┌────────────────────────────────────────────────────────────────────────────────────────┐
│                        REAL-WORLD DATA ENGINEERING ECOSYSTEM                           │
│                                                                                        │
│  [1. INGESTION]          [2. DEVELOPMENT & ORCHESTRATION]          [4. SERVING]        │
│                                                                                        │
│  ┌────────────┐               📍 YOU ARE HERE                      ┌─────────────┐     │
│  │ Kaggle API │               ┌─────────────┐                      │ Streamlit   │     │
│  │ (Raw CSV)  │────────┐      │  Local Dev  │                      │ Dashboards  │     │
│  └────────────┘        │      │ (PyCharm)   │─────────┐            └─────────────┘     │
│                        │      └─────────────┘         │                   ▲            │
│  ┌────────────┐        │             │                │                   │            │
│  │ Event Hub  │        ▼             ▼                ▼                   │            │
│  │ (Streams)  │   ┌─────────┐   ┌─────────┐    ┌──────────────┐    ┌─────────────┐     │
│  └────────────┘   │   Raw   │   │ GitHub  │    │ Apache Spark │    │ PostgreSQL  │     │
│                   │ Storage │   │ (CI/CD) │    │   Cluster    │───>│ Data Whse   │     │
│  ┌────────────┐   │ (HDFS/  │   └─────────┘    │ (YARN/Local) │    │ (Analytics) │     │
│  │ OLTP DB    │──>│  S3)    │─────────────────>│  Processing  │    └─────────────┘     │
│  │ (Postgres) │   └─────────┘                  └──────────────┘                        │
│  └────────────┘                                 [3. COMPUTE]                           │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

| Component | Technology / Recommended Version | Download Link & Purpose |
| :--- | :--- | :--- |
| **IDE** | PyCharm Professional / Community (v2023+) | [Download PyCharm](https://www.jetbrains.com/pycharm/download/)<br>Writing, debugging, and managing the PySpark codebase. |
| **Compute Engine** | Apache Spark v3.5.x (Pre-built for Hadoop 3.3) | [Download Spark](https://spark.apache.org/downloads.html)<br>Processing large datasets, performing ETL, and ML pipelines. |
| **Real-world Data** | Kaggle Datasets (e.g., NYC Taxi) | [Download NYC Taxi Data](https://www.kaggle.com/c/nyc-taxi-trip-duration/data)<br>Providing realistic volume and complexity for data manipulation. |
| **Backend Database** | PostgreSQL v15+ & JDBC Driver (v42.x.x) | [Download PostgreSQL](https://www.postgresql.org/download/)<br>Serving as a source/sink for Spark data processing. |
| **Frontend UI** | Streamlit | [Streamlit Docs](https://docs.streamlit.io/)<br>Rapidly building interactive web apps to visualize Spark outputs. |

---

## Step-by-Step Installation & Setup

### OS-Specific Infrastructure Setup

The following table breaks down the installation steps specifically for your Operating System.

> [!IMPORTANT]
> Any step marked **Download** requires fetching a file from the web. Steps marked **Command** should be executed in your terminal/command prompt.

| Component | Windows OS Setup | macOS Setup |
| :--- | :--- | :--- |
| **1. Java 11 (OpenJDK)**<br>Spark runs on the JVM. | **Download:** [Adoptium OpenJDK 11](https://adoptium.net/temurin/releases/?version=11)<br><br>**Action:** Run installer. Ensure "Set JAVA_HOME variable" is checked. | **Command:**<br>`brew install openjdk@11` |
| **2. Python 3.9+**<br>Required for PySpark. | **Download:** [python.org](https://www.python.org/downloads/)<br><br>**Action:** Run installer. Ensure "Add Python to PATH" is checked. | **Download:** [python.org](https://www.python.org/downloads/) |
| **3. Apache Spark**<br>The compute engine. | **Download:** [Spark 3.5.x tgz](https://spark.apache.org/downloads.html)<br>**Download:** [winutils.exe](https://github.com/cdarlint/winutils)<br><br>**Action:**<br>1. Extract Spark to `C:\spark`<br>2. Set `SPARK_HOME` to `C:\spark`<br>3. Place `winutils.exe` in `C:\hadoop\bin`<br>4. Set `HADOOP_HOME` to `C:\hadoop`<br>5. Add `%SPARK_HOME%\bin` and `%HADOOP_HOME%\bin` to system `PATH` | **Command:**<br>`brew install apache-spark` |
| **4. PostgreSQL 15**<br>Backend database. | **Download:** [PostgreSQL Installer](https://www.enterprisedb.com/downloads/postgres-postgresql-downloads)<br><br>**Action:** Run wizard, remember your superuser password. Create a database named `spark_db`. | **Command:**<br>`brew install postgresql@15`<br>`brew services start postgresql@15`<br><br>**Action:** Run `psql` to create a DB named `spark_db`. |
| **5. JDBC Driver**<br>Connects Spark to DB. | **Download:** [jdbc.postgresql.org](https://jdbc.postgresql.org/download/)<br><br>**Action:** Place `.jar` in PyCharm project folder (e.g., `jars/`). | **Download:** [jdbc.postgresql.org](https://jdbc.postgresql.org/download/)<br><br>**Action:** Place `.jar` in PyCharm project folder (e.g., `jars/`). |

### PyCharm & Project Dependencies

Once the infrastructure above is installed:
1. Download and install [PyCharm Community or Professional](https://www.jetbrains.com/pycharm/download/).
2. Create a new PyCharm project and initialize a dedicated Virtual Environment (`venv`).
3. Open the PyCharm terminal and run the following **Command** to install Python dependencies:

```bash
pip install pyspark pandas psycopg2-binary streamlit plotly
```

### Phase 4: Acquiring Real-World Datasets

> [!TIP]
> The **NYC Taxi Trip Duration** dataset or **Kaggle's E-Commerce Data** are perfect for Spark because they offer millions of rows and complex data types.

1. Create a `data/` folder in your PyCharm project.
2. Download a multi-gigabyte dataset in `.csv` or `.parquet` format and place it inside the `data/` directory.

### Phase 5: Building the Pipeline (The Workflow)

Your PyCharm project should now follow this workflow to practice all concepts:

1. **Data Ingestion (SparkContext / SparkSession)**
   - Write a PySpark script to read the raw Kaggle CSV/Parquet files.
2. **Data Transformation (DataFrames & SQL)**
   - Clean the data, handle nulls, and aggregate metrics (e.g., finding average trip times per hour).
3. **Database Sink (JDBC)**
   - Use the DataFrame `.write.jdbc()` method to push the aggregated, clean data into your PostgreSQL backend.
4. **Frontend Visualization (Streamlit)**
   - Write a separate Python file (`app.py`) using Streamlit.
   - Connect Streamlit to the PostgreSQL database to query the aggregated data.
   - Render interactive charts showing the results of your Spark job.

## Summary Checklist

- `[ ]` `JAVA_HOME` and `SPARK_HOME` variables configured.
- `[ ]` PyCharm project created with `pyspark` and `streamlit` installed.
- `[ ]` PostgreSQL running locally with `spark_db` created.
- `[ ]` JDBC Driver downloaded and referenced in Spark configurations.
- `[ ]` Real-world dataset downloaded to the local workspace.
