# 🔥 Elite Spark Assessment: Hive Metastore

## Section 1: True/False Questions

1. **Question**: Spark's `HiveExternalCatalog` bypasses the Thrift service in production and directly queries the underlying RDBMS (e.g., MySQL) for table metadata.
   - **Answer**: False
   - **Mastery Explanation**: The `HiveExternalCatalog` wraps a `HiveClientImpl` which opens a Thrift connection to the HMS Thrift server. The Thrift server (not Spark) issues JDBC calls to the backing RDBMS. This separates the metadata plane and prevents Spark drivers from directly coupling to the RDBMS schema.

2. **Question**: `MSCK REPAIR TABLE` operates in O(1) time complexity by adding all unmapped partitions in a single atomic database transaction.
   - **Answer**: False
   - **Mastery Explanation**: `MSCK REPAIR TABLE` operates in O(n) time, where n is the total number of partitions. It recursively lists all directories under the table's root location (e.g., via S3 LIST calls) and compares them against the HMS `PARTITIONS` table, making it extremely slow for large tables.

3. **Question**: Catalyst's partition pruning happens at the Executor level during the Task execution phase.
   - **Answer**: False
   - **Mastery Explanation**: Partition pruning happens at the Driver during the Logical Optimization and Physical Planning phases. Catalyst eliminates non-matching partitions using metastore statistics *before* any executor task is launched, reducing planned I/O to zero for pruned partitions.

4. **Question**: The embedded Apache Derby metastore uses a file lock that prevents a second JVM from connecting, making it strictly a single-writer development tool.
   - **Answer**: True
   - **Mastery Explanation**: Derby's `EmbeddedDriver` creates a local `metastore_db/` and exclusively locks it. Any second Spark application connecting to the same directory fails with `ERROR XJ040`, enforcing the need for a networked RDBMS in shared clusters.

5. **Question**: `spark.catalog.refreshTable()` synchronizes the HMS MySQL database with the underlying HDFS/S3 filesystem.
   - **Answer**: False
   - **Mastery Explanation**: `refreshTable()` only invalidates the Driver-side `SessionCatalog` in-memory cache for the current session. It does not touch the HMS or perform any filesystem reconciliation (unlike `MSCK REPAIR TABLE`).

6. **Question**: Without column-level statistics, Catalyst defaults to a 200-row estimate, which typically forces the Cost-Based Optimizer to choose `SortMergeJoin` instead of `BroadcastHashJoin`.
   - **Answer**: True
   - **Mastery Explanation**: The 200-row heuristic lacks byte-size estimations (`rawDataSize`). Without accurate byte size stored in `TAB_COL_STATS`, Catalyst cannot guarantee the dataset fits in `autoBroadcastJoinThreshold`, safely falling back to a `SortMergeJoin` which adds two full shuffle stages.

7. **Question**: Running `ANALYZE TABLE t COMPUTE STATISTICS FOR ALL COLUMNS` is an O(1) metadata operation.
   - **Answer**: False
   - **Mastery Explanation**: This command triggers a full table scan (O(data size)) to compute column-level metrics (min, max, nulls, distincts, histograms). It involves shuffle operations internally and should be run strategically after ETL loads.

8. **Question**: `ALTER TABLE ADD PARTITION` is the preferred production method for registering partitions because it operates in O(1) without scanning the filesystem.
   - **Answer**: True
   - **Mastery Explanation**: It simply issues an `INSERT` into the HMS `PARTITIONS` and `PARTITION_KEY_VALS` tables. Because it requires no recursive S3 listing, it finishes in milliseconds regardless of the table's total partition count.

9. **Question**: The `VectorizedParquetRecordReader` relies on metastore schema definitions to project only requested columns directly into off-heap memory.
   - **Answer**: True
   - **Mastery Explanation**: Metastore schemas dictate column projection. Tungsten's vectorized reader reads columnar batches (`ColumnarBatch`) into off-heap memory via `sun.misc.Unsafe` for only the referenced columns, bypassing JVM heap allocation entirely.

10. **Question**: The `SessionCatalog` caches global temp views and session-scoped functions by writing them to the Hive Metastore Thrift Service.
    - **Answer**: False
    - **Mastery Explanation**: The `SessionCatalog` sits above the `HiveExternalCatalog` and manages temporary views and session UDFs entirely in JVM memory. Resolving these entities never touches the Thrift wire.

## Section 2: Multiple Choice Questions

11. **Question**: When a `SparkSession` is created with `enableHiveSupport()`, which class replaces the default catalog to manage persistent metadata?
    - A) `SessionCatalog`
    - B) `HiveExternalCatalog`
    - C) `HiveThriftCatalog`
    - D) `RDBMSCatalog`
    - **Answer**: B
    - **Mastery Explanation**: `enableHiveSupport()` loads the `HiveSessionStateBuilder`, which instantiates `HiveExternalCatalog` instead of `InMemoryCatalog`, allowing connectivity to the Thrift service.

12. **Question**: Which Spark component utilizes the partition location URIs fetched from the metastore to schedule tasks efficiently?
    - A) `TaskScheduler`
    - B) `BlockManager`
    - C) `DAGScheduler`
    - D) `Tungsten Optimizer`
    - **Answer**: B
    - **Mastery Explanation**: The `BlockManager` uses partition URIs to determine data locality (node-local vs rack-local). Missing partition entries force remote network reads, destroying data locality and severely degrading performance.

13. **Question**: To prevent connection exhaustion in a production HMS backed by MySQL under concurrent DDL workloads, which property should be tuned?
    - A) `hive.metastore.client.socket.timeout`
    - B) `datanucleus.connectionPool.maxPoolSize`
    - C) `spark.sql.cbo.enabled`
    - D) `spark.sql.hive.metastore.barrierPrefixes`
    - **Answer**: B
    - **Mastery Explanation**: The Thrift service manages an internal connection pool (like c3p0 or HikariCP) to the backing RDBMS. `datanucleus.connectionPool.maxPoolSize` caps the JDBC connections to prevent database lockups under high concurrency.

14. **Question**: What occurs when an external pipeline writes new partitioned data to S3 but fails to register it in the metastore?
    - A) Spark throws a `MissingPartitionException` at query time.
    - B) Spark falls back to a full filesystem scan.
    - C) Spark silently ignores the new data, returning incomplete results.
    - D) Catalyst disables CBO statistics automatically.
    - **Answer**: C
    - **Mastery Explanation**: Spark's Analyzer relies strictly on the partition map returned by the metastore. Unregistered partitions are completely invisible to the execution plan, resulting in silent correctness bugs (data loss in query results).

15. **Question**: How is high availability and automatic failover configured for the HMS Thrift client in Spark?
    - A) `spark.sql.hive.metastore.barrierPrefixes`
    - B) Providing a comma-separated list of URIs to `hive.metastore.uris`
    - C) `hive.metastore.client.socket.timeout`
    - D) `spark.sql.cbo.joinReorder.enabled`
    - **Answer**: B
    - **Mastery Explanation**: Setting `hive.metastore.uris` to multiple endpoints (e.g., `thrift://hms1:9083,thrift://hms2:9083`) natively enables the client to fallback to secondary servers if the primary times out.

16. **Question**: What is the purpose of configuring `spark.sql.hive.metastore.barrierPrefixes`?
    - A) To block unauthorized access to the RDBMS.
    - B) To isolate HMS client classes from the executor classpath, preventing Hive version conflicts.
    - C) To define S3 bucket boundaries for `MSCK REPAIR`.
    - D) To limit the memory overhead of off-heap Tungsten vectors.
    - **Answer**: B
    - **Mastery Explanation**: Different Hive versions bring conflicting dependencies. `barrierPrefixes` ensures that Hive client libraries loaded in the Driver for metastore communication do not pollute the Executor's application classpath.

17. **Question**: In Catalyst's physical planning, which information allows the `VectorizedParquetRecordReader` to minimize I/O?
    - A) RDD Lineage graphs
    - B) Schema column definitions from the metastore for projection pushdown
    - C) `InMemoryCatalog` statistics
    - D) Task locality preferences
    - **Answer**: B
    - **Mastery Explanation**: The metastore provides the strict schema, enabling Catalyst to instruct the vectorized reader to materialize only the specific columns requested by the query, deeply reducing memory allocation.

18. **Question**: Without `TAB_COL_STATS` generated by `ANALYZE TABLE`, how does the Catalyst optimizer behave regarding joins?
    - A) It defaults to a 200-row cardinality estimate, typically forcing `SortMergeJoin`.
    - B) It skips Logical Optimization and forces a `BroadcastHashJoin`.
    - C) It throws a `StatisticsMissingException`.
    - D) It relies on filesystem byte-size to accurately estimate cardinality.
    - **Answer**: A
    - **Mastery Explanation**: The 200-row fallback lacks precise memory sizing, so Catalyst conservatively falls back to `SortMergeJoin` to avoid Driver OOMs, incurring two full network shuffles.

19. **Question**: Why is `MSCK REPAIR TABLE` considered an anti-pattern for large-scale production pipelines?
    - A) It requires a global write-lock on the RDBMS backend.
    - B) It issues O(n) S3 LIST calls to map every subdirectory, which scales terribly and incurs high API costs.
    - C) It deletes unrecognized partitions.
    - D) It ignores Parquet footers.
    - **Answer**: B
    - **Mastery Explanation**: A table with 100,000 partitions forces `MSCK REPAIR` to execute thousands of sequential S3 `LIST` calls. `ALTER TABLE ADD PARTITION` is O(1) and executes in milliseconds.

20. **Question**: During which Catalyst phase is a raw table name string resolved into a `CatalogTable` object?
    - A) Logical Optimization
    - B) Analysis
    - C) Physical Planning
    - D) Code Generation
    - **Answer**: B
    - **Mastery Explanation**: The Analysis phase traverses the unresolved Logical Plan, querying the `HiveExternalCatalog` to bind raw strings to `LogicalRelation` objects imbued with schema and partition data.

21. **Question**: Which HMS table identifies whether a column can be used for partition pruning pushdown?
    - A) `COLUMNS_V2`
    - B) `PARTITION_KEYS`
    - C) `TAB_COL_STATS`
    - D) `DBS`
    - **Answer**: B
    - **Mastery Explanation**: Partition columns are stored explicitly in the `PARTITION_KEYS` table, separate from regular data columns in `COLUMNS_V2`. Catalyst checks this list to apply partition filters.

22. **Question**: What is the performance penalty of querying a 10,000-partition table versus a 10-partition table, assuming the query filters down to a single partition?
    - A) 1000x slower due to filesystem iteration.
    - B) Identical, because partition pruning happens via metadata before any file I/O.
    - C) 10x slower due to RDBMS latency.
    - D) Negligible, handled by Spark's memory cache.
    - **Answer**: B
    - **Mastery Explanation**: Because pruning is metadata-driven and happens at the Driver, the execution plan contains exactly one partition's worth of files in both scenarios. The I/O profile is identical.

23. **Question**: Which HMS table is populated by `ANALYZE TABLE ... COMPUTE STATISTICS` (without `FOR ALL COLUMNS`)?
    - A) `TAB_COL_STATS`
    - B) `PARTITION_PARAMS`
    - C) `COLUMNS_V2`
    - D) `TBLS`
    - **Answer**: B
    - **Mastery Explanation**: Table-level/partition-level metrics like `numRows`, `numFiles`, and `rawDataSize` are stored in `PARTITION_PARAMS` (or `TABLE_PARAMS`). `TAB_COL_STATS` requires `FOR ALL COLUMNS`.

24. **Question**: How does `spark.catalog.tableExists()` behave differently than executing `DESCRIBE TABLE` via Spark SQL?
    - A) It triggers a filesystem scan.
    - B) It returns a boolean and does not throw an exception if the table is missing.
    - C) It requires a full RDBMS lock.
    - D) It only checks the Driver's `InMemoryCatalog`.
    - **Answer**: B
    - **Mastery Explanation**: `tableExists()` is an idempotent API method designed for programmatic pipeline logic, safely returning `False` instead of throwing an `AnalysisException` when a table isn't found.

25. **Question**: Why should you avoid inferring schema from Parquet files in production (`spark.read.parquet()`)?
    - A) Parquet does not contain schema data.
    - B) Inferring schema requires reading the footer of every file, triggering O(n) Thrift and S3 calls.
    - C) Spark Catalyst is incompatible with inferred schemas.
    - D) It forces `BroadcastHashJoin`.
    - **Answer**: B
    - **Mastery Explanation**: Relying on the metastore provides an O(1) schema retrieval. Inferring from files requires expensive I/O across the cluster just to build the Logical Plan, stalling job initialization.

## Section 3: "Small Twist" Scenario Questions

26. **Scenario**: You configure `spark.sql.cbo.enabled=true` and run `ANALYZE TABLE COMPUTE STATISTICS`. Twist: You forget to add `FOR ALL COLUMNS`.
    - **Question**: What is the impact on a join query involving this table?
    - **Answer**: The CBO lacks column-level data and defaults to a 200-row cardinality estimate, likely falling back to `SortMergeJoin`.
    - **Mastery Explanation**: Without `FOR ALL COLUMNS`, Catalyst gets table row counts but lacks the distinct value counts and column distributions needed to confidently size the join, disabling broadcast optimizations.

27. **Scenario**: A background task adds a new date partition to S3. To make it visible, you run `spark.catalog.refreshTable("my_table")`. Twist: You expected this to act like `MSCK REPAIR`.
    - **Question**: What happens when you query the new date?
    - **Answer**: The data is silently ignored and missing from results.
    - **Mastery Explanation**: `refreshTable()` only clears the Driver's local cache. It does not scan S3 or insert rows into the HMS. Without an explicit `ALTER TABLE ADD PARTITION`, the HMS remains unaware of the data.

28. **Scenario**: Your ETL job writes data with `df.write.partitionBy("hour").parquet(...)` and finishes with `MSCK REPAIR TABLE`. Twist: The table has grown to 500,000 partitions.
    - **Question**: What is the immediate operational consequence?
    - **Answer**: The pipeline hangs for 30+ minutes making thousands of S3 API calls.
    - **Mastery Explanation**: The O(n) complexity of `MSCK REPAIR` breaks down completely at scale. Replacing it with an O(1) `ALTER TABLE ADD PARTITION` fixes the bottleneck instantly.

29. **Scenario**: You configure `hive.metastore.uris` to `thrift://hms-1:9083`. Twist: The node reboots during your Spark job's initialization.
    - **Question**: How does the Spark application behave?
    - **Answer**: It crashes with a `HiveMetaException` during Catalog operations.
    - **Mastery Explanation**: Because no secondary URI was provided in the comma-separated string, the Thrift client has no failover mechanism and aborts.

30. **Scenario**: You aggressively optimize your application by setting `spark.serializer` to `KryoSerializer`. Twist: You expect this to speed up the metadata retrieval from the Thrift client.
    - **Question**: Why does this not improve HMS interaction speed?
    - **Answer**: Hive client objects live exclusively in the Driver JVM and communicate via Thrift, bypassing Spark's internal RDD/task serialization entirely.
    - **Mastery Explanation**: Kryo only optimizes data serialized across the network to Executors during shuffles or task closures. Metastore interactions are Thrift RPCs from the Driver.

31. **Scenario**: You query a highly partitioned table without specifying a partition filter in the `WHERE` clause. Twist: You assume Tungsten's vectorized reader will optimize the read.
    - **Question**: What actually happens?
    - **Answer**: Spark executes a full table scan, bypassing partition pruning and incurring massive I/O.
    - **Mastery Explanation**: Vectorization only optimizes column projection (vertical reading). Without a partition filter, Catalyst cannot eliminate partitions (horizontal pruning), causing every file in every directory to be scanned.

32. **Scenario**: You launch a Jupyter notebook with Spark and it works perfectly. Twist: A colleague launches a second notebook on the same master node, but it crashes with `ERROR XJ040: Failed to start database 'metastore_db'`.
    - **Question**: What is the root cause?
    - **Answer**: The cluster is using the default embedded Derby database.
    - **Mastery Explanation**: Derby uses an exclusive local file lock. A production cluster must configure an external RDBMS via `hive.metastore.uris` to support concurrent `SparkSession`s.

33. **Scenario**: You connect an external MySQL metastore but set `datanucleus.connectionPool.maxPoolSize=2`. Twist: 50 concurrent Spark applications try to create tables simultaneously.
    - **Question**: What is the resulting bottleneck?
    - **Answer**: Connection pool exhaustion, causing DDL operations to time out and Spark apps to hang.
    - **Mastery Explanation**: The HMS Thrift server uses a JDBC connection pool. If threads exceed `maxPoolSize`, they block waiting for a database connection. Production defaults should be 20-50.

34. **Scenario**: A Python ETL script writes files to `s3://bucket/table/dt=2024/`. Twist: Instead of `ALTER TABLE ADD PARTITION`, it runs `spark.catalog.createTable()` pointing to the same path.
    - **Question**: Does this make the partition queryable?
    - **Answer**: No, `createTable` defines the schema but does not auto-discover partitions.
    - **Mastery Explanation**: `createTable` inserts into `DBS` and `TBLS`, but without `MSCK REPAIR` or `ADD PARTITION`, the `PARTITIONS` table remains empty, leading to silent data omission.

35. **Scenario**: You correctly analyze table statistics. Twist: The data volume doubles over the next week, but `ANALYZE` is never run again.
    - **Question**: How does this impact performance over time?
    - **Answer**: The CBO uses stale statistics, potentially miscalculating broadcast thresholds and choosing degraded physical plans.
    - **Mastery Explanation**: CBO statistics are static snapshots. If they drift significantly from reality, Catalyst makes incorrect join sizing decisions. `ANALYZE` must be continuous.

36. **Scenario**: You read a dataset using `spark.read.parquet("s3://bucket/table")`. Twist: This table is registered in HMS, but you didn't use `spark.table("table")`.
    - **Question**: What optimization do you lose?
    - **Answer**: You lose metadata-driven partition pruning.
    - **Mastery Explanation**: Bypassing the catalog forces Spark to perform a physical filesystem listing to discover files and schemas, losing the O(1) metadata lookup and statistics-based CBO optimizations.

37. **Scenario**: Your Dataproc cluster runs Hive 3.1.2. Twist: You set `spark.sql.hive.metastore.version` to `2.3.9` in your Spark config.
    - **Question**: What occurs when Spark connects?
    - **Answer**: Class/method incompatibilities and Thrift RPC deserialization errors.
    - **Mastery Explanation**: The Thrift protocol schema evolves. If the Spark client speaks a different protocol version than the HMS server, RPC calls fail catastrophically.

38. **Scenario**: You want to check if a table exists. Twist: You use a `try-except` block catching `Exception` around `spark.sql("DESCRIBE my_table")`.
    - **Question**: Why is `spark.catalog.tableExists()` vastly superior here?
    - **Answer**: Exception handling for control flow is slow and brittle; `tableExists()` issues a clean, silent Thrift call.
    - **Mastery Explanation**: `DESCRIBE` throws an `AnalysisException` which carries heavy JVM stack trace overhead. The Catalog API provides programmatic, O(1) checks.

39. **Scenario**: You specify a schema manually using `StructType` but infer it across 10,000 files during your job's first read. Twist: You thought providing a schema prevents Spark from reading footers.
    - **Question**: Does providing a schema prevent O(n) S3 calls?
    - **Answer**: It prevents schema inference, but without the HMS, Spark still issues O(n) calls to list the directory contents.
    - **Mastery Explanation**: Defining a schema skips footer reads, but the raw `spark.read` still requires listing files. The metastore bypasses both by providing exact URIs.

40. **Scenario**: You explicitly declare a partition column in `StructField("event_date", DateType())`. Twist: You forget to pass `partitionColumnNames=["event_date"]` to `createTable`.
    - **Question**: What is the architectural outcome in HMS?
    - **Answer**: The column is inserted into `COLUMNS_V2` instead of `PARTITION_KEYS`.
    - **Mastery Explanation**: The metastore treats it as a normal data column. Consequently, Catalyst will never push down predicates on `event_date` as partition filters, destroying performance.

## Section 4: Coding & Debugging Questions

41. **Code Snippet**:
```scala
val spark = SparkSession.builder()
  .appName("App")
  .config("hive.metastore.uris", "thrift://hms:9083")
  .getOrCreate()
spark.sql("SELECT * FROM sales.events").show()
```
- **Bug**: Missing `.enableHiveSupport()`.
- **Mastery Explanation**: Without this flag, Spark initializes `InMemoryCatalog` instead of `HiveExternalCatalog`. It will ignore the `hive.metastore.uris` config and fail to find `sales.events` because the session-scoped memory catalog is empty.

42. **Code Snippet**:
```python
df.write.partitionBy("date").parquet("s3a://data/t/")
spark.sql("MSCK REPAIR TABLE t")
```
- **Bug**: Using O(n) `MSCK REPAIR` in a write pipeline.
- **Mastery Explanation**: This works logically but fails operationally. As the table grows to thousands of partitions, this step will dominate pipeline execution time. Replace with `ALTER TABLE ADD PARTITION`.

43. **Code Snippet**:
```python
spark.sql("""
  ALTER TABLE t ADD PARTITION (dt='2024-01-01')
  LOCATION 's3a://data/dt=2024-01-01'
""")
```
- **Bug**: Missing `IF NOT EXISTS`.
- **Mastery Explanation**: If the pipeline is retried due to a downstream failure, this statement throws a `PartitionAlreadyExistsException` and crashes the pipeline. `IF NOT EXISTS` makes it safely idempotent.

44. **Code Snippet**:
```scala
spark.sql("ANALYZE TABLE t COMPUTE STATISTICS")
// ... later ...
val query = spark.sql("SELECT * FROM t JOIN small_t ON t.id = small_t.id")
```
- **Bug**: Missing `FOR ALL COLUMNS` in the `ANALYZE` statement.
- **Mastery Explanation**: The CBO requires column-level statistics (distinct counts, nulls) to accurately estimate join cardinalities. Without `FOR ALL COLUMNS`, it reverts to heuristics and likely forces a shuffle-heavy `SortMergeJoin`.

45. **Code Snippet**:
```python
# Upstream pipeline writes new partition to S3
spark.catalog.refreshTable("sales.events")
df = spark.sql("SELECT count(*) FROM sales.events WHERE dt='new_date'")
```
- **Bug**: Misunderstanding `refreshTable` vs HMS mutation.
- **Mastery Explanation**: `refreshTable` only clears the Driver cache. It does not inform the HMS about the new S3 directory. The query will return 0 rows. Explicit partition registration is required.

46. **Code Snippet**:
```scala
// Running in a multi-tenant production cluster
val spark = SparkSession.builder()
  .enableHiveSupport()
  // No hive.metastore.uris specified
  .getOrCreate()
```
- **Bug**: Defaulting to embedded Derby in production.
- **Mastery Explanation**: Missing the URI config forces Spark to spawn a local Derby `metastore_db`. The second application launched on this node will crash due to Derby's exclusive file lock.

47. **Code Snippet**:
```scala
val tables = spark.catalog.listTables("sales").collect()
tables.foreach(t => println(t.name))
```
- **Bug**: This code is correct, but what happens if the Thrift server is offline?
- **Mastery Explanation**: The `listTables()` call issues a synchronous Thrift RPC. If the server is offline, this throws a `HiveMetaException` immediately, acting as a fast-fail health check before submitting executor work.

48. **Code Snippet**:
```python
spark.sql("CREATE TABLE t (id INT, name STRING) USING PARQUET")
# Table is created, now insert
spark.sql("INSERT INTO t VALUES (1, 'Alice')")
```
- **Bug**: Creating unpartitioned tables for analytics.
- **Mastery Explanation**: While syntactically valid, a table with no partitions means every query will scan the entire dataset. In Hive/Spark data lakes, failing to define `PARTITIONED BY` leads to catastrophic O(data size) I/O profiles.

49. **Code Snippet**:
```python
schema = StructType([StructField("id", LongType()), StructField("dt", StringType())])
spark.catalog.createTable("t", "s3a://data/t", "parquet", schema, ["dt"])
```
- **Bug**: Logical assumption that `createTable` writes data files.
- **Mastery Explanation**: `createTable` is purely an O(1) metadata operation over Thrift. It writes rows to HMS `TBLS` and `DBS`, but zero bytes to S3. To query it, data must be explicitly written or partitions added.

50. **Code Snippet**:
```scala
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "104857600") // 100MB
spark.sql("SELECT * FROM fact JOIN dim ON fact.id = dim.id")
```
- **Bug**: Assuming `autoBroadcastJoinThreshold` guarantees a broadcast join.
- **Mastery Explanation**: The threshold only works if Catalyst knows the table size. If the metastore lacks `TAB_COL_STATS` (because `ANALYZE` wasn't run), Catalyst uses the default 200-row fallback, which is inherently untrustworthy, forcing a `SortMergeJoin` regardless of the 100MB limit.
