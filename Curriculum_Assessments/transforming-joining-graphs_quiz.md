# Elite Assessment: Transforming & Joining Graphs

## Section 1: True/False Questions

1. **Question**: In Spark GraphFrames, graph edges and vertices bypass the Catalyst optimizer because they are represented as legacy RDDs.
**Answer**: False
**Mastery Explanation**: GraphFrames are built on DataFrames, meaning they natively benefit from the Catalyst optimizer for predicate pushdown and query planning, unlike RDD-based GraphX.

2. **Question**: The Tungsten engine mitigates JVM object overhead during graph processing by encoding vertices and edges into flat binary formats like `UnsafeRow`.
**Answer**: True
**Mastery Explanation**: Tungsten uses off-heap memory and binary encoding (UnsafeRow) to avoid expensive Java serialization and unpredictable JVM garbage collection pauses, which is vital for massive graph topology data.

3. **Question**: When performing attribute joins in GraphFrames, if data exceeds the broadcast threshold, Catalyst will generally default to a Broadcast Hash Join.
**Answer**: False
**Mastery Explanation**: If data exceeds the `spark.sql.autoBroadcastJoinThreshold`, Catalyst defaults to a Sort Merge Join (SMJ), which involves an expensive, blocking shuffle and sort phase.

4. **Question**: Dropping isolated vertices using an inner join before running iterative algorithms like PageRank is a critical optimization step to save memory and CPU cycles.
**Answer**: True
**Mastery Explanation**: Isolated vertices (nodes with no edges) consume memory and CPU during iterative message-passing. Dropping them via an inner join with the `degrees` table drastically prunes the computational search space.

5. **Question**: Using Python UDFs for mutating edge weights in GraphFrames is highly recommended because it leverages Tungsten's off-heap memory execution natively.
**Answer**: False
**Mastery Explanation**: Python UDFs break Tungsten execution by forcing data to be deserialized from the JVM and serialized into a Python process. Catalyst-optimized expressions (`expr`) should be used instead.

6. **Question**: In a distributed graph environment, high-degree vertices (e.g., celebrity nodes) can lead to severe data skew and straggler tasks during a message-passing phase.
**Answer**: True
**Mastery Explanation**: High-degree vertices cause massive amounts of edge data to be shuffled to a single executor, overwhelming its memory and CPU, leading to classic data skew and stragglers.

7. **Question**: A `BroadcastHashJoin` avoids the shuffle phase entirely by copying the smaller dataset to the driver, then broadcasting it to all executors' local hash tables.
**Answer**: True
**Mastery Explanation**: This technique is crucial in graph enrichment because it keeps the massive partitioned vertex/edge structures intact on their respective nodes while performing the join locally.

8. **Question**: Motif finding in GraphFrames directly executes the graph query using Spark's legacy GraphX Pregel API under the hood, bypassing SQL optimizations.
**Answer**: False
**Mastery Explanation**: Catalyst translates the Domain-Specific Language (DSL) of motif finding into a series of highly optimized multi-way DataFrame joins, allowing for column projection and early filtering.

9. **Question**: Adaptive Query Execution (AQE) dynamically coalesces shuffle partitions and mitigates data skew at runtime during multi-hop graph aggregations.
**Answer**: True
**Mastery Explanation**: AQE monitors shuffle file sizes at runtime and dynamically splits skewed partitions (skew join optimization), preventing OOM errors on high-degree nodes during complex aggregations.

10. **Question**: Sorting both datasets by ID across partitions for a Sort Merge Join in graph processing is a low-latency, non-blocking operation that requires minimal disk I/O.
**Answer**: False
**Mastery Explanation**: Sorting is a highly blocking, expensive operation that requires extensive disk I/O. This is why bucketing by ID or forcing broadcast joins is preferred in graph attribute joins.

## Section 2: Multiple Choice Questions

11. **Question**: Which Spark engine feature is primarily responsible for preventing unpredictable garbage collection pauses during large-scale graph transformations?
- A) Catalyst Optimizer
- B) Tungsten Execution Engine
- C) Adaptive Query Execution
- D) Pregel API
**Answer**: B
**Mastery Explanation**: Tungsten stores data in off-heap memory using binary formats, keeping it out of the JVM's garbage collector. Catalyst optimizes logical plans, AQE optimizes at runtime, and Pregel is an API, making B the only correct choice.

12. **Question**: When translating a motif like `(a)-[e1]->(b); (b)-[e2]->(c)` into a physical execution plan, what does Catalyst convert this into?
- A) A sequence of MapReduce jobs
- B) A recursive Pregel traversal
- C) A series of multi-way DataFrame joins
- D) A localized breadth-first search
**Answer**: C
**Mastery Explanation**: GraphFrames leverage the DataFrame API. Catalyst translates the structural motif into a sequence of inner joins between the `vertices` and `edges` DataFrames.

13. **Question**: What is the primary architectural bottleneck when joining edges to vertices for message-passing in a distributed graph?
- A) Tungsten binary serialization
- B) Catalyst logical plan translation
- C) Network serialization and shuffling
- D) JVM heap space allocation for UnsafeRow
**Answer**: C
**Mastery Explanation**: Graph structures do not natively align with partitions. Operations requiring edge traversals inevitably require moving data across nodes, making network shuffling the biggest bottleneck.

14. **Question**: If you have a massive graph partitioned by user ID and frequently perform attribute joins, which storage technique allows Catalyst to skip the expensive sort phase of a Sort Merge Join?
- A) Compressing with Snappy
- B) Bucketing the Parquet files by user ID
- C) Using JSON format instead of Parquet
- D) Salting the vertex IDs
**Answer**: B
**Mastery Explanation**: Bucketing pre-sorts and pre-partitions data on disk. When Catalyst reads bucketed tables, it recognizes the pre-existing sort order and safely skips the blocking sort phase of an SMJ.

15. **Question**: In GraphFrames, extracting a subgraph and using an inner join on `degrees` with the `vertices` table is a technique used to:
- A) Increase the degree of highly connected vertices.
- B) Drop isolated vertices that have no edges.
- C) Convert a directed graph into an undirected graph.
- D) Trigger a BroadcastHashJoin implicitly.
**Answer**: B
**Mastery Explanation**: An inner join requires matches in both tables. Since isolated vertices have a degree of 0 (and are absent from the `degrees` dataframe), the inner join safely drops them from the vertices dataframe.

16. **Question**: Why should you use `pyspark.sql.functions.expr` instead of a Python UDF when applying a decay function to edge weights?
- A) `expr` enables recursive graph traversal natively.
- B) Python UDFs trigger network shuffles.
- C) `expr` ensures the operation remains within Tungsten, avoiding JVM-Python serialization overhead.
- D) Python UDFs cannot read edge attributes.
**Answer**: C
**Mastery Explanation**: `expr` parses the SQL string into Catalyst expressions, which are compiled into JVM bytecode via Tungsten. UDFs force data out of Tungsten's off-heap memory into a Python process via sockets, destroying performance.

17. **Question**: When joining a massive vertex DataFrame with a 50MB dimension table, which operation eliminates the shuffle phase?
- A) Sort Merge Join
- B) Broadcast Hash Join
- C) Shuffle Hash Join
- D) Cartesian Join
**Answer**: B
**Mastery Explanation**: By broadcasting the 50MB table to all executors, Spark avoids shuffling the massive vertex DataFrame, performing the lookup directly in each executor's local memory.

18. **Question**: High-degree vertices in a social network graph lead to what specific distributed computing problem during a structural join?
- A) Cyclic graph errors
- B) Data skew and straggler tasks
- C) Broadcast timeout exceptions
- D) Catalyst parsing errors
**Answer**: B
**Mastery Explanation**: A high-degree node directs an enormous volume of edges to a single hash partition, causing that specific executor task (straggler) to take significantly longer than others or OOM entirely.

19. **Question**: How does Adaptive Query Execution (AQE) specifically handle data skew caused by a celebrity vertex with millions of followers?
- A) By broadcasting the entire edge table to all nodes.
- B) By dynamically splitting oversized shuffle partitions at runtime.
- C) By deleting edges connected to the celebrity vertex.
- D) By switching the execution engine from Tungsten to JVM objects.
**Answer**: B
**Mastery Explanation**: AQE's `skewJoin` feature monitors shuffle statistics. If a partition exceeds a threshold, it splits that single skewed partition into multiple sub-partitions handled by different tasks.

20. **Question**: What is the purpose of the "Filter-Early, Filter-Often" paradigm in Spark graph transformations?
- A) To maximize the amount of data cached in memory.
- B) To heavily prune the search space and reduce edge volume before complex algorithms run.
- C) To force Spark to use RDDs instead of DataFrames.
- D) To disable Catalyst optimization rules.
**Answer**: B
**Mastery Explanation**: Because graph joins are exceptionally expensive, filtering data early reduces the cardinality of the datasets participating in the multi-way joins, saving massive amounts of network I/O.

21. **Question**: A GraphFrame motif is defined as `"(a)-[e1]->(b); (b)-[e2]->(c); (c)-[e3]->(a)"`. Which DataFrame operation should immediately follow to optimize the execution plan?
- A) `.cache()` on the resulting DataFrame.
- B) `.filter()` to push down edge relationship predicates before the joins execute.
- C) `.count()` to materialize the motif.
- D) `.checkpoint()` to truncate the lineage.
**Answer**: B
**Mastery Explanation**: Catalyst evaluates lazily. Applying a `.filter()` immediately after motif generation allows Catalyst to push the predicate down to the initial table scan, preventing unnecessary data from entering the expensive 3-way join.

22. **Question**: Structural joins in GraphFrames are distinct from attribute joins because structural joins:
- A) Rely strictly on broadcast joins.
- B) Are implicitly handled by the graph engine to traverse topology (e.g., executing motifs).
- C) Only occur when reading external Parquet files.
- D) Do not require shuffle partitions.
**Answer**: B
**Mastery Explanation**: Structural joins define the graph's physical connectivity (joining source/dest IDs between vertices and edges). Attribute joins enrich these structures with external dimensions (like joining a geo-lookup table).

23. **Question**: You are calculating transitive trust across a two-hop path `(a)-[ab]->(b); (b)-[bc]->(c)`. To avoid infinite loops or incorrect trust assignment, what filter must you apply to the motif output?
- A) `col("ab.trust_weight") > 0`
- B) `col("b.id").isNotNull()`
- C) `col("a.id") != col("c.id")`
- D) `col("transitive_trust") < 1.0`
**Answer**: C
**Mastery Explanation**: Without this filter, reciprocal relationships (where A connects to B, and B connects back to A) will be counted as a 2-hop path to oneself, severely skewing transitive aggregations. 

24. **Question**: During an aggregation after a two-hop motif search, executors run out of memory. AQE is enabled. What is preventing AQE from saving the job?
- A) Using `F.sum()` on integer columns.
- B) Using memory-intensive UDAFs like `F.collect_set()` on a skewed key, which forces all elements into a single JVM array regardless of AQE splitting.
- C) The `spark.sql.adaptive.enabled` flag cannot be used with GraphFrames.
- D) Catalyst drops the aggregation.
**Answer**: B
**Mastery Explanation**: AQE splits shuffle data, but for aggregations like `collect_set` or `collect_list`, the final state must still be materialized as a single Array object in one executor's heap memory for that key, leading to OOM.

25. **Question**: What represents the underlying physical data structure for a GraphFrame vertex table before an action is called?
- A) An array of Python dictionaries.
- B) A Catalyst-optimized logical plan pointing to a distributed collection of Tungsten UnsafeRows.
- C) A GraphX `EdgeRDD`.
- D) A localized adjacency list.
**Answer**: B
**Mastery Explanation**: Because GraphFrames use DataFrames, they are backed by the Catalyst logical plan, which upon execution, physically manages data via the Tungsten engine using off-heap `UnsafeRow` arrays.

## Section 3: Small Twist Questions

26. **Scenario**: You change a graph join from `vertices.join(geo_table, ...)` to `vertices.join(broadcast(geo_table), ...)`. The `geo_table` is 15GB, and executor memory is 8GB. 
**Twist**: What happens at runtime?
- A) The join completes instantly with zero shuffle.
- B) The driver crashes with an OutOfMemoryError because it must collect the 15GB table to broadcast it.
- C) Tungsten caches the 15GB in off-heap memory safely.
- D) Spark automatically switches back to Sort Merge Join.
**Answer**: B
**Mastery Explanation**: The broadcast hint forces Spark to collect the entire table to the driver's memory before distributing it. A 15GB table easily overwhelms standard driver and executor memory limits.

27. **Scenario**: You explicitly apply `spark.sql.shuffle.partitions = 2` on a GraphFrame with 50 billion edges before computing multi-hop trust. 
**Twist**: How does this affect execution?
- A) It optimizes network traffic by reducing network connections.
- B) It triggers massive data spill to disk and OOM errors because 25 billion edges per partition overwhelm the two executor tasks.
- C) It automatically enables AQE.
- D) It skips the shuffle phase.
**Answer**: B
**Mastery Explanation**: With only 2 partitions, all 50 billion edges are funneled into 2 tasks. The tasks cannot fit the data in memory, causing fatal spills and out-of-memory exceptions.

28. **Scenario**: You change a motif query from `(a)-[e]->(b)` to `(a)-[e1]->(b); (b)-[e2]->(a)`. 
**Twist**: What topological structure are you now finding?
- A) A one-way directed edge.
- B) A bidirectional edge (mutual relationship).
- C) A 3-node triangle.
- D) A disconnected vertex.
**Answer**: B
**Mastery Explanation**: This specific motif requires an edge from A to B, AND an edge from B back to A, successfully identifying reciprocal/mutual connections in the graph.

29. **Scenario**: Instead of dropping isolated vertices using `sub_g.degrees.join(sub_g.vertices, "id", "inner")`, you accidentally use a `left_outer` join with `sub_g.vertices` on the left. 
**Twist**: What is the result?
- A) All isolated vertices are successfully removed.
- B) The graph becomes entirely empty.
- C) The isolated vertices are retained in the resulting DataFrame, nullifying the optimization.
- D) The degree column becomes negative for isolated vertices.
**Answer**: C
**Mastery Explanation**: A left outer join retains all rows from the left table (vertices). Even if a vertex has no degree (null on the right side), it remains in the dataset, completely failing to prune isolated nodes.

30. **Scenario**: You replace `expr("weight * 0.9")` with a Python UDF `lambda w: w * 0.9`. 
**Twist**: How does Catalyst execution change?
- A) The query plan remains physically identical.
- B) Tungsten is bypassed; data is serialized out of the JVM to a Python process, drastically reducing performance.
- C) The UDF is automatically rewritten to `expr` by Catalyst.
- D) The join strategy changes from Broadcast to Sort Merge.
**Answer**: B
**Mastery Explanation**: Python UDFs cannot be executed by Tungsten in the JVM. Spark must serialize the row, send it over a socket to a Python worker, execute the lambda, and serialize it back, destroying throughput.

31. **Scenario**: You enable AQE, but your multi-hop group-by involves `F.collect_list("b.id")` on a highly skewed key `c.id`. 
**Twist**: Why does the executor still crash?
- A) AQE cannot be enabled on group-by queries.
- B) `collect_list` gathers all values into a single array in executor memory; AQE splitting shuffle blocks does not prevent the final array from exceeding heap space.
- C) AQE only optimizes Broadcast Hash Joins.
- D) Catalyst drops the `collect_list` function automatically.
**Answer**: B
**Mastery Explanation**: AQE splits data during the shuffle, but an aggregation like `collect_list` forces the final result to be merged into a single JVM object on a single executor for that key. If the list is massive, OOM is inevitable.

32. **Scenario**: You have bucketed your edge table by `src` and `dst`, and your vertex table by `id`. 
**Twist**: To ensure a Sort-Merge Join skips the sort phase, what else MUST be true?
- A) Both tables must have the exact same number of buckets.
- B) Both tables must be compressed with GZIP.
- C) The tables must be joined using a Cross Join.
- D) The cluster must have AQE disabled.
**Answer**: A
**Mastery Explanation**: For Catalyst to avoid sorting and shuffling during a Sort-Merge Join, the two tables must be bucketed identically (same number of buckets) so the file boundaries align perfectly 1-to-1.

33. **Scenario**: You filter a 10TB edge table with `filter("date == '2024'")` *after* running a motif `g.find("(a)-[e1]->(b)")`. 
**Twist**: How does Catalyst handle this seemingly late filter?
- A) It executes the join on 10TB of data, then filters, taking hours.
- B) Catalyst's predicate pushdown automatically pushes the filter down to the table scan before the join, ensuring identical performance to filtering early.
- C) It throws a schema error.
- D) It converts it to a Broadcast Hash Join.
**Answer**: B
**Mastery Explanation**: Catalyst analyzes the logical plan. Since the filter condition applies directly to base columns, it intelligently rewrites the physical plan to scan and filter the parquet files before any shuffles or joins occur.

34. **Scenario**: You run `GraphFrame(vertices, edges).edges.cache()`. 
**Twist**: Later, you create a new `sub_g = GraphFrame(vertices.filter("active=true"), edges)`. Does `sub_g` benefit from the cache?
- A) Yes, because `sub_g` uses the same underlying `edges` DataFrame which is cached.
- B) No, caching in GraphFrames binds strictly to the GraphFrame object ID.
- C) Yes, but only if you cache `sub_g.vertices` as well.
- D) No, filtering creates a new lineage, so the cache is ignored.
**Answer**: A
**Mastery Explanation**: Caching in Spark is tied to the physical execution plan of the DataFrame. Since `sub_g` references the exact same `edges` DataFrame lineage that was cached, Spark will hit the cache.

35. **Scenario**: You define a motif `(a)-[e1]->(b); (c)-[e2]->(d)`. 
**Twist**: What kind of operation does this trigger?
- A) An inner join.
- B) A cross join (Cartesian product), leading to massive data explosion.
- C) A Broadcast Hash Join.
- D) An Anti-Join.
**Answer**: B
**Mastery Explanation**: Because the two components of the motif share no common vertex (like 'b' or 'c' bridging them), Spark has no join key. It must perform a Cartesian product of all edges, destroying the cluster.

36. **Scenario**: You run `motifs = g.find("(a)-[e1]->(b)")`. The vertices table has 100 columns, but you only `select("a.id", "b.id")` AFTER the motif find. 
**Twist**: How does Tungsten handle the other 98 columns?
- A) Tungsten serializes all 100 columns through the shuffle phase.
- B) Catalyst's column projection drops the 98 columns at the scan phase before the shuffle, saving massive memory.
- C) Tungsten keeps them in on-heap memory.
- D) The columns are compressed into a single binary blob.
**Answer**: B
**Mastery Explanation**: Catalyst uses Column Pruning (Projection Pushdown). It realizes the 98 columns are never used in the action, so it never reads them from disk or passes them through the shuffle.

37. **Scenario**: Your `spark.sql.autoBroadcastJoinThreshold` is 10MB. `geo_lookup` is 50MB. 
**Twist**: You write `vertices.join(geo_lookup, ...)`. What is the execution plan?
- A) Broadcast Hash Join, because 50MB is small.
- B) Sort Merge Join, because 50MB exceeds the 10MB threshold.
- C) Shuffle Hash Join.
- D) Cross Join.
**Answer**: B
**Mastery Explanation**: Because the 50MB table exceeds the strict 10MB default threshold, Catalyst will fall back to a Sort Merge Join, triggering a shuffle of both datasets.

38. **Scenario**: Same threshold as above (10MB), but you write `vertices.join(broadcast(geo_lookup), ...)`. 
**Twist**: What happens?
- A) Catalyst ignores the broadcast hint because 50MB > 10MB.
- B) Catalyst respects the explicit hint and executes a Broadcast Hash Join, overriding the threshold.
- C) Spark throws an exception.
- D) AQE automatically cancels the query.
**Answer**: B
**Mastery Explanation**: The explicit `broadcast()` hint forces Catalyst to execute a Broadcast Hash Join regardless of the threshold, assuming the driver can handle the memory allocation.

39. **Scenario**: A user has 10 million followers. During `groupBy("c.id").agg(F.sum("trust"))`, the shuffle partition for this user reaches 500MB. AQE is enabled. 
**Twist**: What does AQE do?
- A) Kills the task for exceeding memory.
- B) Detects the skew and dynamically splits the 500MB partition into multiple smaller tasks, summing them partially before a final aggregation.
- C) Rebroadcasts the vertex to all executors.
- D) Drops the user from the aggregation.
**Answer**: B
**Mastery Explanation**: This is the exact mechanism of AQE's `skewJoin` and dynamic partition coalescing optimization. It identifies the abnormally large block and maps it to multiple reducers to distribute the CPU/memory load.

40. **Scenario**: You extract a subgraph `recent_edges = g.edges.filter(col("date") >= "2023")`. 
**Twist**: You forget to drop isolated vertices and run a 10-iteration PageRank. What is the impact?
- A) PageRank fails instantly.
- B) The isolated vertices are entirely ignored by PageRank.
- C) The isolated vertices needlessly consume memory, participate in shuffles, and waste CPU cycles calculating rank for nodes with zero topology.
- D) PageRank automatically drops them on the first iteration.
**Answer**: C
**Mastery Explanation**: Iterative graph algorithms operate on the entire vertex dataframe. If you don't drop isolated vertices, they are passed back and forth through shuffles for all 10 iterations, bloating execution time.

## Section 4: Coding & Debugging Questions

41. **Code Snippet**:
```python
edges = edges.withColumn("weight", udf(lambda w: w * 2)(col("weight")))
g = GraphFrame(vertices, edges)
```
**Bug/Error**: What is the architectural flaw here?
**Answer**: The UDF breaks Tungsten off-heap execution.
**Mastery Explanation**: Python UDFs force Spark to serialize data out of the JVM into a Python worker. This should be rewritten using Catalyst functions: `edges.withColumn("weight", col("weight") * 2)`.

42. **Code Snippet**:
```python
# Massive 200GB dimension table
dim_table = spark.read.parquet(...)
enriched = g.vertices.join(broadcast(dim_table), "id")
```
**Bug/Error**: What happens at runtime?
**Answer**: Driver and Executor OutOfMemoryError.
**Mastery Explanation**: Broadcasting a 200GB table attempts to pull all 200GB into the driver's heap space, immediately crashing it. The `broadcast()` hint must be removed.

43. **Code Snippet**:
```python
motifs = g.find("(a)-[e1]->(b); (b)-[e2]->(c)")
print(motifs.count())
filtered = motifs.filter("e1.type = 'follows'")
```
**Bug/Error**: What is the optimizer blocker here?
**Answer**: The `.count()` action forces early execution.
**Mastery Explanation**: Calling `.count()` executes the massive unrestricted multi-way motif join *before* the filter is applied. The filter should be applied before any action is triggered.

44. **Code Snippet**:
```python
sub_g = GraphFrame(
    g.vertices, 
    g.edges.filter("date >= '2023-01-01'")
)
# Run heavy ML algorithm directly on sub_g
```
**Bug/Error**: What optimization step is missing?
**Answer**: Dropping isolated vertices.
**Mastery Explanation**: Filtering edges does not filter vertices. Many vertices now have zero edges (isolated) but remain in the DataFrame, bloating downstream algorithms.

45. **Code Snippet**:
```python
# Trust calculation over 2 hops
path = g.find("(a)-[e1]->(b); (b)-[e2]->(c)")
trust = path.withColumn("score", col("e1.weight") * col("e2.weight"))
total_trust = trust.groupBy("a.id").sum("score")
```
**Bug/Error**: What logical graph error exists in this aggregation?
**Answer**: It does not filter out cyclic paths.
**Mastery Explanation**: Without `filter(col("a.id") != col("c.id"))`, User A's trust can loop back to themselves if A and B have mutual connections, artificially inflating the score.

46. **Code Snippet**:
```python
spark.conf.set("spark.sql.shuffle.partitions", "10")
# 100 billion edge motif find
motifs = g.find("(a)-[e1]->(b); (b)-[e2]->(c)")
```
**Bug/Error**: What debugging symptom will you observe?
**Answer**: Massive disk spill and OOM errors.
**Mastery Explanation**: 10 partitions for 100 billion edges means 10 billion edges per task. This crushes executor memory. Partitions should be increased (e.g., 2000+) and AQE enabled.

47. **Code Snippet**:
```python
active_users = g.vertices.filter("status = 'active'")
g2 = GraphFrame(active_users, g.edges)
```
**Bug/Error**: What happens to edges whose `src` or `dst` are NOT active?
**Answer**: They become dangling edges.
**Mastery Explanation**: GraphFrames does not automatically delete edges when vertices are dropped. These dangling edges can cause NullPointerExceptions. Edges must be filtered using a join or subgraph API.

48. **Code Snippet**:
```python
# Group by highly skewed celebrity ID
agg_df = g.edges.groupBy("dst").agg(F.collect_list("src"))
```
**Bug/Error**: AQE is enabled, but the executor crashes with OOM. Why?
**Answer**: `collect_list` bypasses AQE's final memory protections.
**Mastery Explanation**: Even if AQE splits the shuffle, `collect_list` demands that the final list of millions of sources be constructed in a single JVM array on one executor, guaranteeing an OOM.

49. **Code Snippet**:
```python
df1 = g.vertices.repartition(100, "id")
df2 = g.edges.repartition(100, "src")
joined = df1.join(df2, df1.id == df2.src)
```
**Bug/Error**: Why does Catalyst still perform a shuffle before the Sort Merge Join?
**Answer**: `repartition` does not sort the data.
**Mastery Explanation**: SMJ requires data to be both partitioned AND sorted. `repartition` distributes data but leaves it unsorted. Catalyst must insert a sort phase and potential shuffle to guarantee order.

50. **Code Snippet**:
```python
res = g.find("(a)-[e1]->(b)")
res.cache()
res.count()
res.unpersist()
res.show()
```
**Bug/Error**: What performance penalty happens during `res.show()`?
**Answer**: The entire motif search is recomputed from scratch.
**Mastery Explanation**: `unpersist()` clears the cached memory block. When `show()` is called, Spark has no cache to read from and must traverse the entire physical plan again, recalculating the joins.
