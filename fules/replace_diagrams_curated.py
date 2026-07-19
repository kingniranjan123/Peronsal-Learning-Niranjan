"""
Topic-Specific Mermaid Diagram Replacer
Generates clean, simple, tested Mermaid diagrams for each failing topic.
"""

import re, time, requests
from pathlib import Path

CONCEPTS_DIR = Path(r"D:\Desktop\13th August 2023\python-output\python-inputs\a-process-telegram-uploads\Spark-In-Action\concepts")
KROKI_URL = "https://kroki.io/mermaid/png"

def test_mermaid(code: str) -> bool:
    try:
        r = requests.post(KROKI_URL, data=code.encode('utf-8'),
                         headers={'Content-Type': 'text/plain'}, timeout=25)
        return r.status_code == 200 and len(r.content) > 200
    except Exception:
        return False

# ─── Topic-Specific Clean Diagrams ────────────────────────────────────────────
TOPIC_DIAGRAMS = {

# PART 1 ─────────────────────────────────────────────────────────────────────
"01_spark_revolution": """graph TD
    A["Traditional Batch Systems"] -->|"Slow disk I/O"| B["MapReduce"]
    B -->|"2009 Research"| C["UC Berkeley AMPLab"]
    C -->|"In-memory processing"| D["Apache Spark"]
    D --> E["100x faster than MapReduce"]
    D --> F["Interactive queries"]
    D --> G["Streaming support"]
    D --> H["Machine Learning"]
    style A fill:#c0392b,color:#fff
    style B fill:#e67e22,color:#fff
    style C fill:#2980b9,color:#fff
    style D fill:#27ae60,color:#fff
    style E fill:#1abc9c,color:#fff
    style F fill:#1abc9c,color:#fff
    style G fill:#1abc9c,color:#fff
    style H fill:#1abc9c,color:#fff""",

"02_mapreduce_shortcomings": """graph LR
    A["Input Data\nHDFS"] --> B["Map Phase\nWorker 1"]
    A --> C["Map Phase\nWorker 2"]
    B -->|"Write to disk"| D["Intermediate\nHDFS Files"]
    C -->|"Write to disk"| D
    D -->|"Read from disk"| E["Reduce Phase"]
    E -->|"Write to disk"| F["Output\nHDFS"]
    F -->|"Read from disk"| G["Next Map Phase\nIteration 2"]
    G -->|"Write to disk again"| H["More HDFS Files"]
    style D fill:#e74c3c,color:#fff
    style H fill:#e74c3c,color:#fff
    style A fill:#2980b9,color:#fff
    style F fill:#2980b9,color:#fff""",

"03_spark_components": """graph TD
    CORE["Spark Core\nRDDs - Task Scheduling - Memory Mgmt"]
    SQL["Spark SQL\nDataFrames - Datasets - Hive"]
    STREAM["Spark Streaming\nDStreams - Kafka - Window Ops"]
    ML["MLlib\nClassification - Clustering - Regression"]
    GX["GraphX\nPageRank - Connected Components"]
    SQL --> CORE
    STREAM --> CORE
    ML --> CORE
    GX --> CORE
    style CORE fill:#1F497D,color:#fff
    style SQL fill:#2E74B5,color:#fff
    style STREAM fill:#2E74B5,color:#fff
    style ML fill:#2E74B5,color:#fff
    style GX fill:#2E74B5,color:#fff""",

"04_spark_ecosystem": """graph TD
    SPARK["Apache Spark"]
    HDFS["HDFS\nStorage"]
    S3["AWS S3\nCloud Storage"]
    KAFKA["Apache Kafka\nMessaging"]
    YARN["YARN\nResource Mgr"]
    HIVE["Apache Hive\nMetastore"]
    SPARK --> HDFS
    SPARK --> S3
    SPARK --> KAFKA
    SPARK --> YARN
    SPARK --> HIVE
    style SPARK fill:#E25A1C,color:#fff
    style HDFS fill:#2980b9,color:#fff
    style S3 fill:#FF9900,color:#fff
    style KAFKA fill:#231F20,color:#fff
    style YARN fill:#2980b9,color:#fff
    style HIVE fill:#FDEE21,color:#000""",

"05_spark_vm_setup": """graph TD
    A["Download JDK 8 or 11"] --> B["Set JAVA_HOME"]
    B --> C["Download Apache Spark"]
    C --> D["Extract to C:/spark"]
    D --> E["Download winutils.exe"]
    E --> F["Set HADOOP_HOME"]
    F --> G["Add to PATH"]
    G --> H["Run spark-shell"]
    H --> I["Spark Ready!"]
    style A fill:#2980b9,color:#fff
    style I fill:#27ae60,color:#fff""",

"01_spark_shell": """graph LR
    A["spark-shell\nor pyspark"] --> B["JVM starts"]
    B --> C["SparkContext sc\ncreated"]
    B --> D["SparkSession spark\ncreated"]
    C --> E["Connect to\nLocal Cluster"]
    D --> F["SQL and DataFrame\nAPI available"]
    E --> G["scala> prompt\nReady for input"]
    F --> G
    style A fill:#1F497D,color:#fff
    style G fill:#27ae60,color:#fff""",

"03_actions": """graph TD
    A["RDD Transformations\nLazy - Not yet computed"] -->|"Action called"| B["DAG Scheduler"]
    B --> C["Stage 1\nTask execution"]
    C --> D["Stage 2\nTask execution"]
    D --> E{"Action Type"}
    E -->|"collect"| F["All data\nto Driver"]
    E -->|"count"| G["Single number\nto Driver"]
    E -->|"saveAsTextFile"| H["Written\nto Disk"]
    E -->|"foreach"| I["Side effects\nper element"]
    style A fill:#e74c3c,color:#fff
    style B fill:#2980b9,color:#fff
    style F fill:#27ae60,color:#fff
    style G fill:#27ae60,color:#fff
    style H fill:#27ae60,color:#fff""",

"04_transformations": """graph LR
    A["Original RDD\n1,2,3,4,5"] -->|"map x*2"| B["Mapped RDD\n2,4,6,8,10"]
    A -->|"filter x>3"| C["Filtered RDD\n4,5"]
    A -->|"flatMap"| D["Flat RDD\nall words split"]
    A -->|"distinct"| E["Distinct RDD\nunique values"]
    B -->|"Action"| F["Result"]
    C -->|"Action"| F
    style A fill:#1F497D,color:#fff
    style F fill:#27ae60,color:#fff""",

"05_double_rdd_functions": """graph TD
    A["DoubleRDD\nRDD of numeric values"] --> B["stats()"]
    A --> C["mean()"]
    A --> D["variance()"]
    A --> E["stdev()"]
    A --> F["histogram(buckets)"]
    A --> G["sum()"]
    B --> H["StatCounter\ncount, mean, stdev, max, min"]
    style A fill:#1F497D,color:#fff
    style H fill:#27ae60,color:#fff""",

"01_spark_ide_project_setup": """graph TD
    A["New Maven Project"] --> B["Add spark-core\ndependency"]
    B --> C["Add spark-sql\ndependency"]
    C --> D["Set Scala version\n2.12 or 2.13"]
    D --> E["Write main class\nextends App"]
    E --> F["Create SparkSession"]
    F --> G["Write Spark logic"]
    G --> H["mvn package\nor sbt assembly"]
    H --> I["JAR file ready"]
    style A fill:#2980b9,color:#fff
    style I fill:#27ae60,color:#fff""",

"02_loading_json": """graph LR
    A["JSON File\nLocal or HDFS or S3"] --> B["spark.read.json()"]
    B --> C["Schema Inference\nautomatically"]
    C --> D["DataFrame\nwith typed columns"]
    D --> E["df.printSchema()"]
    D --> F["df.show()"]
    D --> G["df.filter()"]
    style A fill:#e67e22,color:#fff
    style D fill:#27ae60,color:#fff""",

"03_filtering_and_aggregating": """graph TD
    A["Raw DataFrame\nAll records"] -->|"filter / where"| B["Filtered DataFrame"]
    B -->|"groupBy"| C["GroupedData"]
    C -->|"agg()"| D["Aggregated Result"]
    D --> E["count()"]
    D --> F["sum()"]
    D --> G["avg()"]
    D --> H["max() / min()"]
    style A fill:#2980b9,color:#fff
    style D fill:#27ae60,color:#fff""",

"04_broadcast_variables": """graph TD
    A["Large Lookup Table\n100MB dictionary"] -->|"sc.broadcast"| B["Broadcast Variable"]
    B -->|"Sent ONCE per executor"| C["Executor 1\ncached in memory"]
    B -->|"Sent ONCE per executor"| D["Executor 2\ncached in memory"]
    B -->|"Sent ONCE per executor"| E["Executor 3\ncached in memory"]
    C --> F["All tasks on Executor 1\nshare the same copy"]
    D --> F
    style A fill:#e74c3c,color:#fff
    style B fill:#27ae60,color:#fff""",

"05_spark_submit": """graph TD
    A["spark-submit"] --> B["--class MainClass"]
    A --> C["--master yarn or local"]
    A --> D["--deploy-mode cluster"]
    A --> E["--executor-memory 4g"]
    A --> F["--num-executors 10"]
    A --> G["path/to/app.jar"]
    B & C & D & E & F & G --> H["Job submitted\nto cluster"]
    style A fill:#1F497D,color:#fff
    style H fill:#27ae60,color:#fff""",

"06_uberjars": """graph TD
    A["Your Spark Code"] --> B["maven-assembly-plugin\nor sbt-assembly"]
    C["spark-core.jar"] --> B
    D["Your dependencies"] --> B
    E["Other libraries"] --> B
    B --> F["Single FAT JAR\nuberjar.jar"]
    F --> G["spark-submit\n--class ... uberjar.jar"]
    style A fill:#2980b9,color:#fff
    style F fill:#27ae60,color:#fff""",

"01_pair_rdds": """graph TD
    A["Standard RDD"] -->|"map to tuples"| B["Pair RDD\nkey, value pairs"]
    B --> C["reduceByKey\nAggregate by key"]
    B --> D["groupByKey\nGroup all values"]
    B --> E["mapValues\nTransform values only"]
    B --> F["join\nJoin two Pair RDDs"]
    B --> G["sortByKey\nSort by key"]
    style A fill:#2980b9,color:#fff
    style B fill:#1F497D,color:#fff""",

"02_data_partitioning": """graph TD
    A["Data 1000 rows"] -->|"Default: 200 partitions"| B["Partition 1\nRows 1-5"]
    A --> C["Partition 2\nRows 6-10"]
    A --> D["..."]
    A --> E["Partition N"]
    B --> F["Executor 1\nCore 1"]
    C --> G["Executor 1\nCore 2"]
    D --> H["Executor 2"]
    E --> I["Executor N"]
    style A fill:#1F497D,color:#fff
    style F fill:#27ae60,color:#fff
    style G fill:#27ae60,color:#fff""",

"03_shuffling": """graph LR
    A["Partition 1\nA:1, B:1, A:1"] -->|"Map side sort"| D["Sort by key"]
    B["Partition 2\nC:1, A:1, B:1"] -->|"Map side sort"| D
    D -->|"Network Transfer"| E["Reduce for A\nA:3"]
    D -->|"Network Transfer"| F["Reduce for B\nB:2"]
    D -->|"Network Transfer"| G["Reduce for C\nC:1"]
    style D fill:#e74c3c,color:#fff
    style E fill:#27ae60,color:#fff
    style F fill:#27ae60,color:#fff""",

"04_grouping_and_sorting": """graph TD
    A["Pair RDD"] --> B["groupByKey\nCollects all values\ninto memory - SLOW"]
    A --> C["reduceByKey\nPre-aggregates locally\nthen shuffles - FAST"]
    A --> D["aggregateByKey\nCustom combine\nlogic - FLEXIBLE"]
    B --> E["Result"]
    C --> E
    D --> E
    style B fill:#e74c3c,color:#fff
    style C fill:#27ae60,color:#fff
    style D fill:#2980b9,color:#fff""",

"05_joining_data": """graph TD
    A["Left RDD\nid, name"] --> E["join type"]
    B["Right RDD\nid, salary"] --> E
    E --> C["inner join\nMatching rows only"]
    E --> D["leftOuterJoin\nAll left + matching right"]
    E --> F["rightOuterJoin\nAll right + matching left"]
    E --> G["fullOuterJoin\nAll rows both sides"]
    style A fill:#2980b9,color:#fff
    style B fill:#2980b9,color:#fff
    style C fill:#27ae60,color:#fff""",

"06_rdd_lineage_and_dag": """graph TD
    A["textFile\nRDD 1"] -->|"map - narrow"| B["RDD 2\nParsed"]
    B -->|"filter - narrow"| C["RDD 3\nFiltered"]
    C -->|"reduceByKey - WIDE shuffle"| D["RDD 4\nCounts"]
    D -->|"map - narrow"| E["RDD 5\nFormatted"]
    E -->|"Action: collect"| F["Driver Result"]
    style C fill:#e74c3c,color:#fff
    style D fill:#e74c3c,color:#fff
    style F fill:#27ae60,color:#fff""",

"07_spark_stages_and_tasks": """graph TD
    A["Job triggered by Action"] --> B["DAG Scheduler"]
    B -->|"At shuffle boundary"| C["Stage 1"]
    B --> D["Stage 2"]
    C --> E["Task 1\nPartition 1"]
    C --> F["Task 2\nPartition 2"]
    C --> G["Task N\nPartition N"]
    D --> H["Task 1\nPartition 1"]
    E & F & G --> I["Shuffle Write"]
    I --> H
    style B fill:#1F497D,color:#fff
    style I fill:#e74c3c,color:#fff""",

"08_accumulators": """graph TD
    A["Driver creates\nsc.longAccumulator"] --> B["Broadcast to\nall executors"]
    B --> C["Task 1 adds 1"]
    B --> D["Task 2 adds 1"]
    B --> E["Task N adds 1"]
    C & D & E -->|"Accumulate"| F["Executor local sum"]
    F -->|"Merge to Driver\nat action end"| G["Final value\nin Driver"]
    style A fill:#1F497D,color:#fff
    style G fill:#27ae60,color:#fff""",

# PART 2 ─────────────────────────────────────────────────────────────────────
"01_dataframes": """graph TD
    A["Data Source\nJSON CSV Parquet JDBC"] --> B["spark.read\nloader"]
    B --> C["DataFrame\nNamed columns + Schema"]
    C --> D["select"]
    C --> E["filter"]
    C --> F["groupBy"]
    C --> G["join"]
    C --> H["withColumn"]
    D & E & F & G & H --> I["New DataFrame\nor Result"]
    style A fill:#2980b9,color:#fff
    style C fill:#1F497D,color:#fff
    style I fill:#27ae60,color:#fff""",

"02_datasets": """graph TD
    A["DataFrame\nRow objects - untyped"] -->|"as T"| B["Dataset T\nTyped - compile-time safety"]
    C["Case Class\ncase class Person\nname: String, age: Int"] --> B
    B --> D["map - typed lambda"]
    B --> E["filter - typed"]
    B --> F["groupByKey - typed"]
    style A fill:#e67e22,color:#fff
    style B fill:#27ae60,color:#fff
    style C fill:#2980b9,color:#fff""",

"03_sql_queries": """graph TD
    A["DataFrame df"] -->|"createOrReplaceTempView\n'users'"| B["Temp View\nin Catalog"]
    B --> C["spark.sql\nSELECT name, age\nFROM users\nWHERE age > 30"]
    C --> D["Result DataFrame"]
    D --> E["Window Functions\nRANK OVER PARTITION"]
    D --> F["Subqueries\nCTEs"]
    style A fill:#2980b9,color:#fff
    style B fill:#1F497D,color:#fff
    style D fill:#27ae60,color:#fff""",

"04_hive_metastore": """graph TD
    A["Spark with\nenableHiveSupport"] --> B["Hive Metastore\nMySQL or Derby"]
    B --> C["Table Definitions\nschema location format"]
    C --> D["Managed Tables\nSpark owns data"]
    C --> E["External Tables\nUser owns data"]
    D --> F["spark.sql\nSELECT FROM hive_table"]
    E --> F
    style A fill:#2980b9,color:#fff
    style B fill:#FDEE21,color:#000
    style F fill:#27ae60,color:#fff""",

"05_catalyst_optimizer": """graph LR
    A["SQL or DataFrame\nCode"] --> B["Unresolved\nLogical Plan"]
    B -->|"Schema Analysis"| C["Resolved\nLogical Plan"]
    C -->|"Optimization\nFilter Pushdown"| D["Optimized\nLogical Plan"]
    D -->|"Physical Planning"| E["Physical Plan\nOptions"]
    E -->|"Cost Model"| F["Best Physical Plan"]
    F -->|"Tungsten CodeGen"| G["Bytecode\nExecution"]
    style A fill:#2980b9,color:#fff
    style D fill:#27ae60,color:#fff
    style G fill:#1F497D,color:#fff""",

"06_tungsten_performance": """graph TD
    A["Tungsten Engine"] --> B["Off-heap Memory\nAvoid Java GC"]
    A --> C["Cache-aware\nComputation"]
    A --> D["Whole-stage\nCode Generation"]
    A --> E["Vectorized\nColumnar Reading"]
    B --> F["Faster execution\nless GC pauses"]
    C --> F
    D --> F
    E --> F
    style A fill:#1F497D,color:#fff
    style F fill:#27ae60,color:#fff""",

"01_discretized_streams_dstreams": """graph LR
    A["Live Stream\nKafka Socket File"] --> B["Receiver"]
    B -->|"t=0 to 1s"| C["Batch 1\nRDD"]
    B -->|"t=1 to 2s"| D["Batch 2\nRDD"]
    B -->|"t=2 to 3s"| E["Batch 3\nRDD"]
    C --> F["Spark Core\nProcessing"]
    D --> F
    E --> F
    F --> G["Output\nDB HDFS Kafka"]
    style A fill:#2980b9,color:#fff
    style F fill:#1F497D,color:#fff
    style G fill:#27ae60,color:#fff""",

"02_saving_computation_state": """graph TD
    A["Batch 1 Data\nhello world"] --> B["updateStateByKey\nor mapWithState"]
    C["Previous State\nhello:2 world:1"] --> B
    B --> D["Updated State\nhello:3 world:2"]
    D -->|"Checkpoint\nto HDFS"| E["Fault-tolerant\nState Storage"]
    D --> F["Batch 2 input\nfed here"]
    style B fill:#1F497D,color:#fff
    style E fill:#27ae60,color:#fff""",

"04_kafka_integration": """graph LR
    A["Kafka Broker\nTopic: weblogs"] -->|"Direct Stream\nno receiver"| B["Spark Streaming\nApp"]
    B --> C["Process each\nmicro-batch RDD"]
    C --> D["Compute metrics\nrequests per sec"]
    D -->|"KafkaProducer"| E["Kafka Topic\nstats output"]
    B -->|"Track offsets\nmanually"| F["Offset Manager\nexactly-once"]
    style A fill:#231F20,color:#fff
    style B fill:#1F497D,color:#fff
    style E fill:#231F20,color:#fff""",

"05_performance_tuning": """graph TD
    A["Streaming Performance"] --> B["Batch Interval\nLatency vs Throughput"]
    A --> C["Back-pressure\nspark.streaming\n.backpressure.enabled"]
    A --> D["Kafka Partitions\n= Spark Tasks"]
    A --> E["Memory Tuning\nReduce GC pressure"]
    B & C & D & E --> F["Golden Rule:\nProcessing Time\nless than Batch Interval"]
    style A fill:#1F497D,color:#fff
    style F fill:#27ae60,color:#fff""",

"01_ml_basics": """graph TD
    A["Raw Data"] --> B["Feature Engineering\nCleaning and Encoding"]
    B --> C["Training Set 70pct"]
    B --> D["Validation Set 15pct"]
    B --> E["Test Set 15pct"]
    C --> F["Model Training\nMLlib Algorithm"]
    F --> G["Trained Model"]
    D --> H["Model Evaluation\nRMSE AUC Accuracy"]
    G --> H
    E --> I["Final Evaluation\nUnbiased estimate"]
    style A fill:#2980b9,color:#fff
    style G fill:#27ae60,color:#fff""",

"02_linear_algebra": """graph TD
    A["Feature Vector\nage=25 salary=50000 score=8.5"] --> B["DenseVector\n25.0 50000.0 8.5"]
    A --> C["SparseVector\nonly nonzero values stored"]
    B --> D["Dot Product\nw dot x = prediction"]
    C --> D
    D --> E["Matrix Operations\nRowMatrix IndexedRowMatrix"]
    E --> F["Model Weights\nOptimized by gradient descent"]
    style A fill:#2980b9,color:#fff
    style F fill:#27ae60,color:#fff""",

"04_mean_normalization": """graph TD
    A["Raw Feature\nvalues 0 to 1000"] --> B["Compute Mean\nmu = 500"]
    B --> C["Apply Formula\nx - mu / max - min"]
    C --> D["Normalized\nvalues -0.5 to 0.5"]
    D --> E["Centered around 0\nGradient descent converges faster"]
    style A fill:#e74c3c,color:#fff
    style D fill:#27ae60,color:#fff""",

"05_linear_regression": """graph LR
    A["Training Data\nx features, y labels"] --> B["Hypothesis\nh = w0 + w1*x1 + w2*x2"]
    B --> C["Cost Function\nMSE = mean squared error"]
    C --> D["Gradient Descent\nupdate weights"]
    D -->|"Iterate"| C
    D --> E["Converged\nOptimal Weights"]
    E --> F["Predict\nnew y values"]
    style A fill:#2980b9,color:#fff
    style E fill:#27ae60,color:#fff""",

"01_spark_ml_library": """graph TD
    A["Raw DataFrame"] --> B["Transformer 1\nStringIndexer"]
    B --> C["Transformer 2\nOneHotEncoder"]
    C --> D["Transformer 3\nVectorAssembler"]
    D --> E["Estimator\nLogisticRegression"]
    E -->|"fit(trainingData)"| F["Trained Model\nPipelineModel"]
    F -->|"transform(testData)"| G["Predictions\nDataFrame"]
    style A fill:#2980b9,color:#fff
    style F fill:#27ae60,color:#fff
    style G fill:#1abc9c,color:#fff""",

"02_logistic_regression": """graph TD
    A["Feature Vector\nage salary education"] --> B["Linear Combination\nz = w0 + w1*x1 + ..."]
    B --> C["Sigmoid Function\n1 / 1 + exp(-z)"]
    C --> D["Probability 0.0 to 1.0"]
    D -->|"threshold 0.5"| E{"Decision"}
    E -->|"prob > 0.5"| F["Class 1\nPositive"]
    E -->|"prob <= 0.5"| G["Class 0\nNegative"]
    style C fill:#1F497D,color:#fff
    style F fill:#27ae60,color:#fff
    style G fill:#e74c3c,color:#fff""",

"04_random_forests": """graph TD
    A["Training Dataset"] --> B["Bootstrap Sample 1"]
    A --> C["Bootstrap Sample 2"]
    A --> D["Bootstrap Sample N"]
    B --> E["Tree 1\nRandom feature subset"]
    C --> F["Tree 2\nRandom feature subset"]
    D --> G["Tree N\nRandom feature subset"]
    E & F & G --> H["Majority Vote\nor Average"]
    H --> I["Final Prediction"]
    style A fill:#1F497D,color:#fff
    style H fill:#27ae60,color:#fff""",

"01_graphx_api": """graph TD
    A["Vertices RDD\nid, property"] --> C["Graph object"]
    B["Edges RDD\nsrcId dstId attr"] --> C
    C --> D["numVertices\nnumEdges"]
    C --> E["degrees\ninDegrees\noutDegrees"]
    C --> F["mapVertices\nmapEdges"]
    C --> G["subgraph\nfilter vertices and edges"]
    C --> H["aggregateMessages\ncore message passing"]
    style A fill:#2980b9,color:#fff
    style B fill:#e67e22,color:#fff
    style C fill:#1F497D,color:#fff""",

"02_transforming_joining_graphs": """graph LR
    A["Original Graph\nVertices + Edges"] -->|"mapVertices"| B["New vertex\nproperties"]
    A -->|"mapEdges"| C["New edge\nproperties"]
    A -->|"subgraph"| D["Filtered graph\nsubset of vertices or edges"]
    A -->|"joinVertices"| E["Enriched graph\nexternal data joined"]
    B & C & D & E --> F["Transformed Graph"]
    style A fill:#1F497D,color:#fff
    style F fill:#27ae60,color:#fff""",

"04_pagerank": """graph TD
    A["Node A\nPR=0.25"] -->|"link"| C["Node C"]
    B["Node B\nPR=0.25"] -->|"link"| C
    B -->|"link"| D["Node D"]
    C -->|"link"| D["Node D\nPR=0.6 high rank"]
    C -->|"link"| E["Node E"]
    D -->|"converge after N iterations"| F["Final PageRank\nscores for all nodes"]
    style D fill:#e74c3c,color:#fff
    style F fill:#27ae60,color:#fff""",

"06_astar_search_algorithm": """graph TD
    A["Start Node\ng=0 h=estimate"] --> B["Open Set\nnodes to explore"]
    B -->|"Pick lowest f=g+h"| C["Current Node"]
    C -->|"Is goal?"| D{"Goal reached?"}
    D -->|"Yes"| E["Return path"]
    D -->|"No"| F["Expand neighbors"]
    F -->|"Calculate g+h"| B
    style A fill:#2980b9,color:#fff
    style E fill:#27ae60,color:#fff
    style C fill:#1F497D,color:#fff""",

# PART 3 ─────────────────────────────────────────────────────────────────────
"01_spark_runtime_architecture": """graph TD
    A["Driver Program\nSparkContext SparkSession"] -->|"Connect"| B["Cluster Manager\nYARN Standalone Mesos"]
    B -->|"Allocate containers"| C["Executor 1\nWorker Node 1"]
    B -->|"Allocate containers"| D["Executor 2\nWorker Node 2"]
    B -->|"Allocate containers"| E["Executor N\nWorker Node N"]
    A -->|"Send tasks"| C
    A -->|"Send tasks"| D
    A -->|"Send tasks"| E
    C -->|"Results"| A
    style A fill:#1F497D,color:#fff
    style B fill:#e67e22,color:#fff""",

"02_spark_cluster_types": """graph TD
    A["Spark Cluster Modes"] --> B["Local\nlocal or local[N]\nDevelopment only"]
    A --> C["Standalone\nBuilt-in cluster manager\nEasy setup"]
    A --> D["YARN\nHadoop ecosystem\nEnterprise standard"]
    A --> E["Mesos\nFine-grained resources\nMulti-framework"]
    A --> F["Kubernetes\nContainer-native\nCloud standard"]
    style A fill:#1F497D,color:#fff
    style D fill:#27ae60,color:#fff
    style F fill:#2980b9,color:#fff""",

"03_job_and_resource_scheduling": """graph TD
    A["Multiple Spark Jobs"] --> B{"Scheduler Mode"}
    B -->|"FIFO default"| C["Job 1 gets all resources\nJob 2 waits"]
    B -->|"FAIR"| D["Job 1 and Job 2\nshare resources equally"]
    D --> E["Fair Pools\nconfigure weight and minShare"]
    E --> F["Dynamic Allocation\nauto scale executors"]
    style B fill:#1F497D,color:#fff
    style D fill:#27ae60,color:#fff""",

"04_configuring_spark": """graph TD
    A["Spark Configuration\nPrecedence Order"] --> B["1. SparkConf in code\nHighest priority"]
    A --> C["2. spark-submit --conf flags\nOverrides defaults"]
    A --> D["3. spark-defaults.conf file\nLowest priority"]
    B --> E["Key Properties:\nexecutor-memory\ndriver-memory\nspark.sql.shuffle.partitions"]
    style A fill:#1F497D,color:#fff
    style E fill:#27ae60,color:#fff""",

"05_spark_web_ui": """graph TD
    A["Spark Application\nRunning"] --> B["Web UI\nlocalhost:4040"]
    B --> C["Jobs Tab\nall jobs and status"]
    B --> D["Stages Tab\ntask metrics and DAG"]
    B --> E["Storage Tab\ncached RDDs"]
    B --> F["Executors Tab\nmemory GC stats"]
    B --> G["Environment Tab\nall config props"]
    G --> H["History Server\nport 18080\ncompleted apps"]
    style A fill:#2980b9,color:#fff
    style B fill:#1F497D,color:#fff""",

"01_standalone_cluster_components": """graph TD
    A["Master Daemon\nport 7077\nspark-class Master"] -->|"Register"| B["Worker 1\nport 8081\nspark-class Worker"]
    A -->|"Register"| C["Worker 2\nport 8082"]
    B -->|"Launch"| D["Executor 1\nJVM process\nruns tasks"]
    C -->|"Launch"| E["Executor 2\nJVM process\nruns tasks"]
    F["Driver / spark-submit"] -->|"Connect to master"| A
    A -->|"Allocate resources"| F
    style A fill:#1F497D,color:#fff
    style F fill:#27ae60,color:#fff""",

"02_cluster_web_ui": """graph TD
    A["Spark Cluster Web UIs"] --> B["Master UI\nlocalhost:8080\nWorkers and Applications"]
    A --> C["Worker UI\nlocalhost:8081\nExecutors and Logs"]
    A --> D["App UI\nlocalhost:4040\nJobs Stages Tasks"]
    B --> E["Running Applications\nresource usage"]
    C --> F["Task logs\nstdout stderr"]
    D --> G["DAG visualization\nperformance metrics"]
    style A fill:#1F497D,color:#fff""",

"03_running_applications": """graph TD
    A["Package App\nmvn package or sbt assembly"] --> B["spark-submit"]
    B --> C["--master spark://host:7077"]
    B --> D["--deploy-mode cluster\nor client"]
    B --> E["--executor-memory 2g"]
    B --> F["--executor-cores 2"]
    C & D & E & F --> G["App submitted\nto standalone cluster"]
    style A fill:#2980b9,color:#fff
    style G fill:#27ae60,color:#fff""",

"04_spark_history_server": """graph TD
    A["Spark App running"] -->|"spark.eventLog.enabled=true"| B["Event Logs\nwritten to HDFS or local"]
    B -->|"App finishes\nUI disappears"| C["History Server\nstart-history-server.sh"]
    C --> D["localhost:18080\nBrowse completed apps"]
    D --> E["Jobs Stages Tasks\nfrom event log replay"]
    style A fill:#2980b9,color:#fff
    style C fill:#1F497D,color:#fff
    style D fill:#27ae60,color:#fff""",

"05_amazon_ec2_deployment": """graph TD
    A["AWS Account"] --> B["Launch EC2 instances\n1 Master + N Workers"]
    B --> C["Configure Security Groups\nports 7077 8080 4040 22"]
    C --> D["Install Java and Spark\nvia bootstrap script"]
    D --> E["Start Spark cluster\nstart-all.sh"]
    E --> F["Submit job\nspark-submit --master spark://..."]
    F --> G["Read Write data\nfrom S3"]
    style A fill:#FF9900,color:#fff
    style G fill:#27ae60,color:#fff""",

"01_yarn_architecture": """graph TD
    A["ResourceManager\nYARN Master"] -->|"Allocate container"| B["NodeManager\nWorker Node 1"]
    A -->|"Allocate container"| C["NodeManager\nWorker Node 2"]
    B --> D["ApplicationMaster\nSpark Driver in cluster mode"]
    D -->|"Request executor containers"| A
    A -->|"Grant resources"| B
    B -->|"Launch"| E["Executor\nSpark tasks run here"]
    style A fill:#e67e22,color:#fff
    style D fill:#1F497D,color:#fff
    style E fill:#27ae60,color:#fff""",

"02_yarn_resource_scheduling": """graph TD
    A["YARN Schedulers"] --> B["FIFO Scheduler\nSimple queue\none job at a time"]
    A --> C["Capacity Scheduler\nMultiple queues\nguaranteed capacity pct"]
    A --> D["Fair Scheduler\nDynamic sharing\nall jobs get resources"]
    C --> E["Spark queue: 60pct\nHive queue: 40pct"]
    D --> F["Pool A weight 2\nPool B weight 1"]
    style A fill:#e67e22,color:#fff
    style C fill:#27ae60,color:#fff""",

"03_mesos_architecture": """graph TD
    A["Mesos Master\nResource offers"] -->|"Offer resources"| B["Framework\nSpark Driver"]
    B -->|"Accept or decline offer"| A
    A -->|"Launch task"| C["Mesos Agent\nWorker Node 1"]
    A -->|"Launch task"| D["Mesos Agent\nWorker Node 2"]
    C --> E["Spark Executor\nTask execution"]
    D --> F["Another Framework\nMarathon or MPI"]
    style A fill:#2980b9,color:#fff
    style B fill:#1F497D,color:#fff""",

"04_mesos_resource_scheduling": """graph TD
    A["Mesos Resource Scheduling"] --> B["Coarse-grained Mode\nStatic executor allocation\nHold resources entire job"]
    A --> C["Fine-grained Mode\nTask-level allocation\nShare CPUs between tasks"]
    B --> D["Better for long batch jobs\nPredictable performance"]
    C --> E["Better for short tasks\nHigher cluster utilization"]
    style A fill:#2980b9,color:#fff
    style D fill:#27ae60,color:#fff
    style E fill:#1abc9c,color:#fff""",

"05_running_spark_with_docker": """graph TD
    A["Dockerfile\nFROM openjdk:11\nADD spark binaries"] --> B["Docker Image\nspark:latest"]
    B --> C["docker-compose.yml\nmaster + 2 workers"]
    C --> D["Master container\nport 7077 8080"]
    C --> E["Worker 1 container"]
    C --> F["Worker 2 container"]
    D & E & F --> G["Local Spark Cluster\nin Docker"]
    style A fill:#2496ED,color:#fff
    style G fill:#27ae60,color:#fff""",

# PART 4 ─────────────────────────────────────────────────────────────────────
"chapter13_overview": """graph LR
    A["Log Simulator\nJava App"] -->|"HTTP log lines"| B["Kafka Topic\nweblogs"]
    B -->|"Direct Stream"| C["Spark Streaming\nAnalyzer"]
    C -->|"Aggregated JSON"| D["Kafka Topic\nstats"]
    D --> E["WebSocket Server\nJetty or Node.js"]
    E -->|"ws://"| F["Browser\nD3.js Dashboard"]
    style A fill:#2980b9,color:#fff
    style C fill:#1F497D,color:#fff
    style F fill:#27ae60,color:#fff""",

"02_spark_streaming_components": """graph TD
    A["StreamingContext\nbatch interval 5s"] --> B["Kafka DirectStream\ntopic weblogs"]
    B --> C["Parse log lines\nIP method URL status bytes"]
    C --> D["filter errors\nstatus >= 400"]
    D --> E["map to tuples\nURL count"]
    E --> F["reduceByKey\ncount per URL"]
    F --> G["foreachRDD\nwrite to Kafka stats"]
    style A fill:#1F497D,color:#fff
    style G fill:#27ae60,color:#fff""",

"03_websockets": """graph LR
    A["Kafka stats topic"] --> B["WebSocket Server"]
    B -->|"ws://localhost:8887"| C["Browser Client 1"]
    B -->|"ws://localhost:8887"| D["Browser Client 2"]
    B -->|"ws://localhost:8887"| E["Browser Client N"]
    C --> F["D3.js live chart\nupdates every 5 sec"]
    style A fill:#231F20,color:#fff
    style B fill:#1F497D,color:#fff
    style F fill:#27ae60,color:#fff""",

"04_d3js_visualization": """graph TD
    A["WebSocket\nJSON payload"] --> B["D3 data binding\nd3.select chart"]
    B --> C["Scale functions\nd3.scaleLinear"]
    C --> D["Axes\nd3.axisBottom d3.axisLeft"]
    D --> E["Line chart\nrequests per second"]
    D --> F["Bar chart\ntop 10 URLs"]
    E & F --> G["d3.transition\nSmooth animation update"]
    style A fill:#2980b9,color:#fff
    style G fill:#27ae60,color:#fff""",

"chapter14_overview": """graph TD
    A["Apache Spark\nDistributed Data Processing"] -->|"H2OContext"| B["H2O Sparkling Water\nDeep Learning Platform"]
    B -->|"asH2OFrame"| C["H2O Frame\nfor ML training"]
    C -->|"H2ODeepLearning"| D["Trained DNN Model"]
    D -->|"asDataFrame"| E["Spark DataFrame\nwith predictions"]
    style A fill:#E25A1C,color:#fff
    style B fill:#FECB00,color:#000
    style D fill:#27ae60,color:#fff""",

"01_h2o_framework": """graph TD
    A["H2O.ai Platform"] --> B["H2O Flow\nWeb UI notebook"]
    A --> C["AutoML\nAuto algorithm selection"]
    A --> D["Deep Learning\nNeural networks"]
    A --> E["GBM\nGradient Boosting"]
    A --> F["Random Forest"]
    A --> G["GLM\nGeneralized Linear Model"]
    B & C & D & E & F & G --> H["POJO MOJO Export\nProduction scoring"]
    style A fill:#FECB00,color:#000
    style H fill:#27ae60,color:#fff""",

"02_deep_learning_concepts": """graph TD
    A["Input Layer\nfeature vector"] --> B["Hidden Layer 1\nReLU activation"]
    B --> C["Hidden Layer 2\nReLU activation"]
    C --> D["Output Layer\nSoftmax for classification\nLinear for regression"]
    D --> E["Loss Function\ncross-entropy or MSE"]
    E -->|"Backpropagation"| F["Update Weights\ngradient descent"]
    F -->|"Next epoch"| A
    style A fill:#2980b9,color:#fff
    style D fill:#1F497D,color:#fff
    style F fill:#27ae60,color:#fff""",

"04_regression_and_classification_with_deep_learning": """graph TD
    A["Training Data\nBoston Housing or MNIST"] --> B["Spark DataFrame\nload and clean"]
    B -->|"asH2OFrame"| C["H2O Frame"]
    C --> D["H2ODeepLearning\nhidden=[200,200]\nepochs=50"]
    D --> E["Trained Model\nweights optimized"]
    E --> F{"Task"}
    F -->|"Regression"| G["Predict house price\nEvaluate RMSE"]
    F -->|"Classification"| H["Predict digit 0-9\nEvaluate accuracy"]
    style D fill:#1F497D,color:#fff
    style G fill:#27ae60,color:#fff
    style H fill:#27ae60,color:#fff""",

"03_sparkling_water_api": """graph TD
    A["SparkContext sc"] -->|"H2OContext.getOrCreate(sc)"| B["H2OContext h2o"]
    B --> C["H2O Flow UI\nlocalhost:54321"]
    D["Spark DataFrame"] -->|"h2o.asH2OFrame(df)"| E["H2OFrame\nfor ML"]
    E -->|"H2ODeepLearning.train"| F["H2O Model"]
    F -->|"h2o.asDataFrame(predictions)"| G["Spark DataFrame\nwith results"]
    style B fill:#FECB00,color:#000
    style F fill:#27ae60,color:#fff""",
}

def replace_diagram_in_file(md_path: Path) -> tuple[int, int]:
    content = md_path.read_text(encoding='utf-8', errors='replace')
    if '```mermaid' not in content:
        return 0, 0

    stem = md_path.stem
    # Try exact match first, then partial
    replacement_code = None
    for key, diagram in TOPIC_DIAGRAMS.items():
        if stem == key or stem.endswith(key) or key in stem:
            replacement_code = diagram
            break

    if not replacement_code:
        return 0, 0

    fixed = 0
    fallback = 0

    def do_replace(m):
        nonlocal fixed, fallback
        original = m.group(1).strip()
        # Test if original already works
        if test_mermaid(original):
            fixed += 1
            return m.group(0)
        # Try our curated replacement
        time.sleep(0.3)
        if test_mermaid(replacement_code):
            fixed += 1
            print(f"  REPLACED with curated diagram: {md_path.name}")
            return f'```mermaid\n{replacement_code}\n```'
        fallback += 1
        return m.group(0)

    new_content = re.sub(r'```mermaid\n(.*?)\n```', do_replace, content, flags=re.DOTALL)
    if new_content != content:
        md_path.write_text(new_content, encoding='utf-8')
    return fixed, fallback

def main():
    print("="*60)
    print("  Topic-Specific Mermaid Replacer")
    print("="*60)
    total_fixed = 0
    total_fallback = 0
    md_files = sorted(CONCEPTS_DIR.rglob("*.md"))
    for md in md_files:
        f, fb = replace_diagram_in_file(md)
        total_fixed += f
        total_fallback += fb
    print(f"\nFixed:    {total_fixed}")
    print(f"Skipped:  {total_fallback}")
    print("Done. Run generate_docx_v2.py next.")

if __name__ == "__main__":
    main()
