# 100-Day Spark Engineering Training Bootcamp

## 🎯 A Clear Learning Objective
To transform from a beginner to a production-ready Data Engineer capable of designing, building, tuning, and deploying scalable distributed data processing pipelines using Apache Spark, Kafka, and modern cluster managers.

## 📚 Structured Training Methodology
1.  **Read & Digest (20%):** Read the designated chapters from *Spark in Action* to understand the theoretical underpinnings.
2.  **Code & Experiment (50%):** Type out the examples. Do not copy-paste. Experiment with the Spark Shell (`spark-shell`).
3.  **Build & Break (20%):** Apply the concepts to recommended datasets. Intentionally cause out-of-memory errors or massive shuffles to learn how to debug them using the Spark Web UI.
4.  **Review & Document (10%):** Maintain a GitHub repository with your notes, scripts, and project code.

## 💾 Recommended Datasets for Hands-on Practice
*   **NYC Taxi Trip Data:** Great for testing Spark SQL, aggregations, and DataFrames on large tabular data.
*   **GitHub Archive:** Used in the book; excellent for JSON parsing and time-series aggregations.
*   **Twitter/X Public Stream:** Perfect for real-time Spark Streaming and Kafka integration.
*   **UCI Machine Learning Repository (e.g., Adult Census, Boston Housing):** Ideal for practicing MLlib pipelines.

---

## 🗺️ A 100-Day Learning Roadmap

### Phase 1: Spark Foundations & Core API (Days 1 - 20)
*   **Days 1-5:** Introduction to Distributed Computing & Spark Ecosystem (Chapter 1). Set up your local environment (IDE, Spark, Java).
*   **Days 6-10:** Spark Fundamentals (Chapter 2). Master the Spark Shell. Learn RDDs, transformations (`map`, `filter`, `flatMap`), and actions (`collect`, `count`).
*   **Days 11-15:** Writing Spark Applications (Chapter 3). Build a standalone project using Maven/SBT. Learn how to package an *uberjar* and submit it using `spark-submit`.
*   **Days 16-20:** Advanced Spark Core (Chapter 4). Deep dive into Pair RDDs, data partitioning, avoiding shuffles, broadcast variables, and accumulators.

### Phase 2: The Modern Spark Data Engineer (Days 21 - 45)
*   **Days 21-30:** Spark SQL & DataFrames (Chapter 5). This is critical. Learn to ingest JSON/Parquet, register temp tables, use the Catalyst optimizer, and write complex SQL queries.
*   **Days 31-40:** Spark Streaming (Chapter 6). Understand discretized streams, window operations, and stateful processing.
*   **Days 41-45:** Streaming Integrations. Focus heavily on connecting Spark Streaming with Apache Kafka (Direct Stream approach).

### Phase 3: Cluster Operations & DevOps (Days 46 - 65)
*   **Days 46-50:** Spark Runtime Architecture (Chapter 10). Master the Driver/Executor model, memory fractions, and the Spark Web UI.
*   **Days 51-55:** Standalone Clusters & Cloud (Chapter 11). Deploy a cluster on AWS EC2. Configure the Spark History Server.
*   **Days 56-65:** YARN & Mesos (Chapter 12). Learn how to deploy Spark on YARN (cluster vs. client mode) and configure dynamic resource allocation.

### Phase 4: Machine Learning & Graphs (Days 66 - 80)
*   **Days 66-70:** MLlib Basics (Chapter 7). Understand feature scaling, linear regression, and evaluating models.
*   **Days 71-75:** Classification & Clustering (Chapter 8). Decision trees, Random Forests, and K-Means.
*   **Days 76-80:** GraphX (Chapter 9). Constructing graphs, PageRank, and connected components. *(Note: Skim if your role does not require graph processing).*

### Phase 5: Capstone Projects & Portfolio (Days 81 - 100)
*   **Days 81-90:** Capstone 1: Real-Time Dashboard (Chapter 13). Build an end-to-end pipeline: Log Generator -> Kafka -> Spark Streaming -> WebSockets -> D3.js Dashboard.
*   **Days 91-95:** Capstone 2: Deep Learning Integration (Chapter 14). Integrate H2O (Sparkling Water) with Spark for advanced analytics.
*   **Days 96-100:** Code refinement, GitHub portfolio cleanup, documentation, and final review.

---

## 🏆 GitHub Portfolio Goals
1.  **Batch Processing Pipeline:** A repository demonstrating ETL processing on a large dataset (e.g., NYC Taxi data) using Spark SQL and Parquet.
2.  **Real-Time Analytics App:** A repository showcasing Kafka + Spark Streaming, preferably with a live dashboard.
3.  **Cluster Deployment Scripts:** Bash/Terraform scripts demonstrating how you spin up and configure a Spark cluster on AWS.

## ✅ A Success Checklist
- [ ] Can successfully run `spark-submit` in both `client` and `cluster` modes.
- [ ] Understands the difference between a narrow and wide transformation (shuffle).
- [ ] Can read and write Parquet files partitioned by specific columns.
- [ ] Can debug a failing Spark job using the Spark Web UI and logs.
- [ ] Successfully integrated Spark Streaming with a Kafka topic.
- [ ] Understands Spark memory management (Storage vs. Execution memory).

## 🧠 Learning Philosophy for Production-Ready Data Engineers
1.  **Failures are inevitable:** In distributed systems, nodes die, network partitions occur, and disks fill up. Code defensively. Assume tasks will fail and be retried.
2.  **Optimize for I/O, not just CPU:** Shuffling data across the network is the most expensive operation in Spark. Always think about data locality and partitioning.
3.  **Monitor everything:** A job that runs successfully but takes 10 hours instead of 10 minutes is a failure. Always use the Spark Web UI and History Server to profile your jobs.
4.  **Embrace immutability:** Functional programming principles (like immutable RDDs/DataFrames) prevent hard-to-track bugs in concurrent, distributed environments.
