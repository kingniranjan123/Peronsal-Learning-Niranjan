# Connected Components Master Class Quiz

## Part 1: True/False Questions (1-10)

1. **Question:** In GraphFrames, checkpointing is strictly required for the Connected Components algorithm to prevent `StackOverflowError` and massive lineage accumulation.
**Answer:** True
**Mastery Explanation:** Connected Components is an iterative algorithm that builds an increasingly long lineage graph during message passing. Without checkpointing, the logical and physical plans become too large for the driver to handle, eventually throwing a StackOverflowError or causing executor OOM during lineage resolution.

2. **Question:** Tungsten's off-heap memory management stores JVM objects directly to avoid Garbage Collection (GC) pauses during massive graph shuffles.
**Answer:** False
**Mastery Explanation:** Tungsten does not store JVM objects; it stores binary data in a highly optimized format off-heap. By bypassing JVM object overhead, it eliminates GC pauses. Storing standard JVM objects would still incur GC overhead.

3. **Question:** Strongly Connected Components (SCC) can be computed without specifying `maxIter` in GraphFrames because the algorithm will naturally terminate upon convergence in a directed graph.
**Answer:** False
**Mastery Explanation:** SCC in GraphFrames requires a `maxIter` parameter to bound execution. Without it, the algorithm could theoretically run indefinitely on large cyclic graphs, leading to wasted compute or OOM.

4. **Question:** When resolving data skew in Connected Components, repartitioning the edges DataFrame based solely on the source vertex hash will perfectly distribute super-node workloads.
**Answer:** False
**Mastery Explanation:** Hashing on just the source vertex will send all outgoing edges of a super-node to a single partition, still causing stragglers. You must hash on both source and destination (e.g., `hash(col("src")), hash(col("dst"))`) to scatter the edges.

5. **Question:** Catalyst optimizations apply to both GraphFrames (DataFrame API) and GraphX (RDD API).
**Answer:** False
**Mastery Explanation:** Catalyst is the query optimizer for Spark SQL and DataFrames. Since GraphX is built on the RDD API, it bypasses Catalyst entirely, meaning it cannot leverage automatic filter pushdowns or whole-stage code generation.

6. **Question:** Using the default Java serialization for GraphFrames shuffling in Connected Components is a viable alternative if Kryo is unavailable, with minimal performance overhead.
**Answer:** False
**Mastery Explanation:** Java serialization is notoriously slow and bloated. In iterative graph algorithms that generate massive intermediate data (like CC), using Java serialization introduces severe network and disk I/O bottlenecks. Kryo is imperative.

7. **Question:** The Pregel API model in Spark relies on an iterative, message-passing paradigm where vertices send states to neighbors.
**Answer:** True
**Mastery Explanation:** Pregel is the foundational distributed graph processing model used in Spark for algorithms like Connected Components, where vertices exchange messages (like minimum component ID) in super-steps.

8. **Question:** GraphX's `EdgePartition2D` strategy bounds the size of the routing table, significantly reducing communication overhead for large graphs.
**Answer:** True
**Mastery Explanation:** `EdgePartition2D` clusters edges in a 2D grid, ensuring that any vertex's edges are spread across at most `2 * sqrt(partitions)` partitions. This bounds the routing table size and reduces shuffle data during Pregel iterations.

9. **Question:** Caching the initial GraphFrame before running Connected Components is an anti-pattern because the framework automatically persists intermediate states.
**Answer:** False
**Mastery Explanation:** While intermediate states are checkpointed, caching the *initial* GraphFrame prevents Spark from re-reading and re-computing the base vertex and edge DataFrames from distributed storage (like S3) on every iteration.

10. **Question:** In an undirected graph, Connected Components and Strongly Connected Components will always yield identical component structures.
**Answer:** True
**Mastery Explanation:** By definition, an undirected edge implies a bi-directional path. If vertices are connected in an undirected graph, they are strongly connected, meaning CC and SCC results are identical (though SCC is far more expensive to run).

## Part 2: Multiple Choice Questions (11-25)

11. **Question:** Which internal Spark component is bypassed when using GraphX instead of GraphFrames?
A) BlockManager
B) DAGScheduler
C) Catalyst Optimizer
D) TaskScheduler
**Answer:** C
**Mastery Explanation:** GraphX operates on RDDs, which are lower-level distributed collections that do not benefit from the Catalyst Optimizer's logical/physical query plan optimizations or Tungsten's whole-stage code generation.

12. **Question:** What is the primary cause of a `StackOverflowError` on the driver during iterative graph processing without checkpointing?
A) The heap memory of the driver is too small to store the graph.
B) The DAG lineage grows linearly with each iteration, exceeding JVM stack depth during resolution.
C) The broadcast variables exceed driver memory limits.
D) The Kryo serializer buffer overflows.
**Answer:** B
**Mastery Explanation:** Each iteration adds transformations to the execution plan. Without checkpointing, Spark attempts to evaluate this massive lineage graph during an action, recursively traversing the DAG and overflowing the driver's JVM call stack.

13. **Question:** How does `EdgePartition2D` optimize message passing in GraphX?
A) It broadcasts all edges to all executors.
B) It guarantees that all edges for a given vertex reside on a single partition.
C) It bounds the routing table by clustering edges into a 2D grid, ensuring vertices only replicate to a subset of partitions.
D) It converts all wide dependencies into narrow dependencies.
**Answer:** C
**Mastery Explanation:** EdgePartition2D places an edge (u, v) into a partition based on the hash of u and v on a 2D grid. This guarantees a vertex is replicated to at most `2 * sqrt(N)` partitions, keeping routing tables small.

14. **Question:** In the context of Tungsten, what does "off-heap" memory management solve during Connected Components execution?
A) Network latency during shuffles.
B) Garbage Collection (GC) pauses caused by massive object creation.
C) Disk I/O bottlenecks during checkpointing.
D) Catalyst optimizer timeouts.
**Answer:** B
**Mastery Explanation:** Iterative graph algorithms create billions of intermediate state objects. Storing them on the JVM heap triggers frequent, long GC pauses. Tungsten stores this data in binary format off-heap, bypassing the GC completely.

15. **Question:** When calling `g_large.connectedComponents(broadcastThreshold=10485760)`, what is the developer attempting to optimize?
A) Broadcasting the entire graph to all executors.
B) Forcing a sort-merge join for all iterations.
C) Broadcasting small sub-components or lookup tables during the join phases to avoid shuffles.
D) Increasing the Kryo buffer size.
**Answer:** C
**Mastery Explanation:** `broadcastThreshold` instructs Catalyst to use BroadcastHashJoins when one side of a join (like a small state DataFrame) is below 10MB, bypassing the expensive shuffle phase entirely for that operation.

16. **Question:** Why is calculating the graph diameter important when tuning Strongly Connected Components (SCC)?
A) It dictates the `spark.sql.shuffle.partitions` value.
B) It determines the optimal `maxIter` parameter, as the longest path dictates the maximum iterations needed for convergence.
C) It configures the checkpoint interval.
D) It sets the broadcast threshold.
**Answer:** B
**Mastery Explanation:** In SCC, messages must propagate from one end of a component to the other. The maximum iterations required for the algorithm to fully converge is directly proportional to the graph diameter.

17. **Question:** What happens if `sparkContext.setCheckpointDir()` is omitted in GraphFrames Connected Components?
A) The algorithm falls back to RDDs.
B) An explicit `AnalysisException` is thrown before execution begins.
C) The job runs but saves data locally to the executor disk.
D) The Catalyst optimizer disables whole-stage code generation.
**Answer:** B
**Mastery Explanation:** GraphFrames explicitly requires checkpointing for the `connectedComponents` algorithm. The API checks for a configured checkpoint directory and will fail fast if it is missing, preventing a catastrophic lineage explosion.

18. **Question:** To mitigate data skew from a "super-node" in GraphFrames, which repartitioning strategy is most effective for the edges DataFrame?
A) `repartition(1000)`
B) `repartition(hash(col("src")))`
C) `repartition(hash(col("src")), hash(col("dst")))`
D) `coalesce(10)`
**Answer:** C
**Mastery Explanation:** Hashing on both `src` and `dst` ensures that the massive number of edges originating from or terminating at a super-node are scattered across multiple partitions, rather than choking a single reducer.

19. **Question:** In the Pregel API, what operation inherently triggers a network shuffle?
A) Computing the vertex attribute.
B) Checking the `maxIter` condition.
C) Sending messages to neighboring vertices.
D) Checkpointing the lineage.
**Answer:** C
**Mastery Explanation:** Message passing requires grouping messages by the destination vertex ID. Because vertices and their neighbors reside on different partitions, this routing requires a wide transformation (shuffle) across the network.

20. **Question:** Which serialization format is practically mandatory for iterative graph algorithms in Spark?
A) Java Native Serialization
B) Kryo Serialization
C) JSON Serialization
D) Avro Serialization
**Answer:** B
**Mastery Explanation:** Kryo serialization is significantly faster and produces much smaller binary representations than Java serialization. This reduces both network transfer time during shuffles and disk footprint during spills.

21. **Question:** When GraphFrames executes a join between vertex state and edge lists, how does Tungsten improve execution?
A) By generating Java bytecode at runtime to fuse operators and bypass virtual function calls.
B) By converting all DataFrames to RDDs.
C) By avoiding network transmission altogether.
D) By automatically increasing heap size.
**Answer:** A
**Mastery Explanation:** Tungsten's Whole-Stage Code Generation collapses an entire query plan (or substantial parts of it) into a single optimized Java function, eliminating virtual function calls and reducing CPU cycles.

22. **Question:** What is the primary disadvantage of using Strongly Connected Components (SCC) over standard Connected Components on an undirected graph?
A) SCC requires GraphX; it's not supported in GraphFrames.
B) SCC cannot utilize Kryo serialization.
C) SCC is significantly more computationally expensive and provides no additional information for undirected graphs.
D) SCC requires broadcast joins for all edges.
**Answer:** C
**Mastery Explanation:** In undirected graphs, all connected components are inherently strongly connected. Running SCC wastes resources (multiple forward/backward passes) to yield the exact same result as the cheaper CC algorithm.

23. **Question:** Why might a Senior Architect choose GraphX over GraphFrames for a specific workload?
A) To leverage Catalyst query pushdowns.
B) Because GraphX supports SQL natively.
C) To gain granular control over data locality via custom PartitionStrategies (e.g., EdgePartition2D).
D) Because GraphX automatically handles checkpointing without configuration.
**Answer:** C
**Mastery Explanation:** GraphX allows explicit control over how edges and vertices are partitioned across the cluster. Specialized algorithms or unique graph topologies can benefit immensely from strategies like `EdgePartition2D`, which GraphFrames abstracts away.

24. **Question:** What role does the Catalyst Optimizer play during the message-passing phase of GraphFrames?
A) It translates Scala code to Python.
B) It determines the optimal join strategy (e.g., SortMergeJoin vs. BroadcastHashJoin) for combining vertex and edge DataFrames.
C) It manages the off-heap memory allocations.
D) It serializes the checkpoints to HDFS.
**Answer:** B
**Mastery Explanation:** Catalyst analyzes the logical plan of the message-passing joins. Based on statistics and configurations (like `broadcastThreshold`), it selects the most efficient physical execution plan (e.g., choosing a BroadcastHashJoin if the message payload is small).

25. **Question:** A Spark executor repeatedly dies with OOM (Out Of Memory) during the shuffle phase of Connected Components. What is the most likely culprit?
A) Lineage overflow.
B) Java serialization is enabled.
C) Data skew caused by super-nodes overwhelming a single partition during a SortMergeJoin.
D) The checkpoint directory is full.
**Answer:** C
**Mastery Explanation:** Super-nodes have millions of edges. If partitioned naively (e.g., hash on `src` only), all edges for that node land on one executor. During the `groupBy` or `join` in the shuffle, that single executor runs out of memory attempting to process the massive partition.

## Part 3: "Small Twist" Questions (26-40)

26. **Question:** You run Connected Components on GraphFrames with checkpointing enabled. It takes 2 hours. You change `spark.graphframes.optimizer.enabled` to `false`. What happens?
**Answer:** Execution time drastically increases or the job fails.
**Mastery Explanation:** Disabling the GraphFrames optimizer prevents it from utilizing Catalyst's advanced DataFrame joins, defaulting to less efficient execution paths or RDD conversions, causing massive performance degradation.

27. **Question:** You have an undirected graph and compute CC. You then convert all edges to directed edges (one-way) and compute SCC. Will the number of components increase, decrease, or stay the same?
**Answer:** Increase.
**Mastery Explanation:** In a one-way directed graph, vertices rarely form cycles. Since SCC requires paths in both directions (A->B and B->A), most vertices will now become their own individual strongly connected component.

28. **Question:** You configure `sparkContext.setCheckpointDir("s3a://bucket/chkpt")`. You notice S3 costs skyrocket due to PUT requests. You switch to a local HDFS cluster for checkpointing. What is the architectural tradeoff?
**Answer:** Lower cost and latency, but dependent on HDFS cluster stability and storage capacity.
**Mastery Explanation:** HDFS provides faster, localized disk I/O and no per-PUT API costs compared to S3. However, S3 provides virtually infinite scaling and decoupling of compute/storage, whereas HDFS requires managing local disk limits on the cluster.

29. **Question:** You use `repartition(hash(col("src")))` on edges to fix skew. It fails. You change it to `repartition(hash(col("src")), hash(col("dst")))`. Why does the job now succeed?
**Answer:** A super-node's outgoing edges are scattered.
**Mastery Explanation:** Hashing on just `src` sends all edges of node X to one partition. Hashing on `src` AND `dst` creates a compound key, distributing node X's edges across many partitions based on who it connects to, alleviating the OOM.

30. **Question:** You run GraphX CC with `RandomVertexCut`. You switch to `EdgePartition2D`. Network shuffle size drops by 40%. Why?
**Answer:** Routing table bounding.
**Mastery Explanation:** `RandomVertexCut` scatters edges randomly, meaning a vertex's state must be replicated to almost all partitions. `EdgePartition2D` guarantees a vertex only replicates to `2 * sqrt(P)` partitions, slashing the shuffle payload.

31. **Question:** In GraphFrames, you set `maxIter=5` for SCC. The graph has a diameter of 20. What is the state of the output?
**Answer:** Inaccurate/Incomplete components.
**Mastery Explanation:** Messages only propagate 5 hops. Vertices 10 hops away that belong in the same SCC will not receive the component ID, resulting in a fractured output where one true component is split into multiple smaller ones.

32. **Question:** You are running CC on a small graph (1 million edges) on a 100-node cluster. It runs slowly. You change `spark.sql.shuffle.partitions` from 2000 (default) to 50. Performance improves 10x. Why?
**Answer:** Reduced task overhead and scheduling latency.
**Mastery Explanation:** 2000 partitions for a tiny dataset creates 2000 micro-tasks. The overhead of the DAGScheduler tracking and launching these tasks outweighs the actual computation time. 50 partitions reduces this overhead.

33. **Question:** You cache the initial `GraphFrame` vertices and edges. The job speeds up. You also cache the intermediate DataFrames inside the Pregel loop. The job crashes with OOM. Why?
**Answer:** Caching inside a loop causes memory pressure.
**Mastery Explanation:** The initial cache prevents re-reading from disk. Caching inside the iterative loop means you are retaining every intermediate state in memory concurrently. Without evicting old iterations, the executor heap fills up and crashes.

34. **Question:** You use Kryo serialization. You switch to a custom Kryo Registrator to pre-register your Vertex and Edge classes. What is the impact?
**Answer:** Network and memory footprint decreases further.
**Mastery Explanation:** Without registration, Kryo must write the full class name (e.g., `com.example.graph.MyEdge`) alongside every single object. Pre-registering maps the class to an integer ID, saving massive amounts of bytes per object.

35. **Question:** You are using GraphX. You use `.cache()` on the CC output graph, then call `.vertices.count()` and `.edges.count()`. How many times is CC computed?
**Answer:** Once.
**Mastery Explanation:** `cache()` lazily marks the RDD for persistence. The first action (`vertices.count()`) triggers the CC computation and caches the result. The second action reads directly from the cache.

36. **Question:** You remove `.cache()` from the previous scenario. How many times is CC computed?
**Answer:** Twice.
**Mastery Explanation:** Without caching, RDDs are ephemeral. Spark will re-evaluate the entire DAG lineage from the source files, re-running the entire Connected Components algorithm for both the vertex count and edge count actions.

37. **Question:** In GraphFrames, you change `spark.sql.autoBroadcastJoinThreshold` from 10MB to 10GB. The cluster has 16GB RAM per executor. What happens during CC?
**Answer:** Executor OOM.
**Mastery Explanation:** Setting the broadcast threshold too high tricks Catalyst into broadcasting massive DataFrames. The driver pulls the 10GB data, then sends it to all executors, instantly blowing out the JVM heap and crashing the cluster.

38. **Question:** You run CC on GraphFrames. You notice high GC pauses. You switch off Tungsten by disabling `spark.sql.codegen.wholeStage`. What happens?
**Answer:** Performance collapses and GC gets worse.
**Mastery Explanation:** Tungsten prevents GC by managing off-heap memory. Disabling it forces Spark to fall back to the Volcano Iterator model, creating millions of JVM objects per row, exacerbating GC pressure and CPU overhead.

39. **Question:** You are running CC. The graph contains 10 disjoint subgraphs of equal size. You add a single edge connecting two of the subgraphs. How does the final output change?
**Answer:** The number of unique component IDs decreases by 1.
**Mastery Explanation:** Connecting two separate components merges them into a single larger component. All vertices in both original subgraphs will now share the same minimum component ID.

40. **Question:** You run GraphX CC. You use `outerJoinVertices` instead of `innerJoin` when attaching component IDs back to the original graph. Does it matter?
**Answer:** Yes, if vertices have no edges.
**Mastery Explanation:** `innerJoin` will drop vertices that exist in the vertex RDD but have no corresponding edges (islands). `outerJoinVertices` preserves them, assigning them their own vertex ID as the component ID.

## Part 4: Coding & Debugging Questions (41-50)

41. **Question:** Analyze this code:
```python
g = GraphFrame(v, e)
for i in range(10):
    g = GraphFrame(g.vertices, g.edges.sample(0.9))
g.connectedComponents().show()
```
What architectural failure will occur at runtime?
**Answer:** Driver StackOverflowError / Lineage Explosion.
**Mastery Explanation:** The python loop creates a deeply nested DAG of 10 GraphFrames built on top of each other. Because `connectedComponents` also builds a massive DAG, the combined logical plan will be too large for the Catalyst optimizer to parse, crashing the Driver.

42. **Question:** A developer reports that `cc_result = g.connectedComponents()` throws an `IllegalArgumentException` complaining about a missing checkpoint directory, even though they ran `spark.conf.set("spark.cleaner.referenceTracking.cleanCheckpoints", "true")`. How do you fix it?
**Answer:** Call `spark.sparkContext.setCheckpointDir("/path/to/dir")`.
**Mastery Explanation:** Setting checkpoint cleaner configs does not actually establish the checkpoint location. The SparkContext requires an explicit distributed file system path where the intermediate DataFrames will be materialized to truncate the lineage.

43. **Question:** 
```python
spark.conf.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
g.connectedComponents().write.parquet("s3a://out")
```
What is the primary bottleneck in this job?
**Answer:** Serialization CPU and Network I/O.
**Mastery Explanation:** JavaSerializer is highly inefficient. During the shuffle phases of CC, Spark will spend the majority of its CPU cycles serializing objects, and the resulting payload will saturate the network bandwidth. Must use `KryoSerializer`.

44. **Question:** You notice stragglers in your CC job. You inspect the Spark UI and see one task out of 2000 is taking 4 hours, while others take 2 minutes. What code change is required?
**Answer:** Implement edge repartitioning using multiple keys.
**Mastery Explanation:** This is classic data skew (a super-node). You must repartition the edges before passing them to the GraphFrame: `edges = edges.repartition(2000, hash(col("src")), hash(col("dst")))` to scatter the workload.

45. **Question:** 
```scala
val graph = Graph(vertices, edges)
val cc = graph.connectedComponents()
```
The job is shuffling 500GB of data per iteration. How can you optimize the graph construction to reduce shuffle size?
**Answer:** Add `.partitionBy(PartitionStrategy.EdgePartition2D)`.
**Mastery Explanation:** The default partitioning leaves edges scattered arbitrarily. Applying `EdgePartition2D` clusters the edges, drastically reducing the size of the routing tables sent across the network during the Pregel message-passing steps.

46. **Question:** Review this code:
```python
cc = g.connectedComponents()
cc.filter("component = 1").show()
cc.filter("component = 2").show()
```
What performance disaster is occurring here?
**Answer:** The entire Connected Components algorithm is executing twice.
**Mastery Explanation:** DataFrames are lazily evaluated. Without calling `cc.cache()` before the filters, each `.show()` action triggers the full re-computation of the iterative CC algorithm from scratch.

47. **Question:** You are running SCC on GraphFrames:
```python
scc = g.stronglyConnectedComponents(maxIter=2)
```
The graph has long chain-like structures. What is the logical flaw?
**Answer:** `maxIter=2` is far too low for convergence.
**Mastery Explanation:** A chain structure has a large diameter. With `maxIter=2`, a component ID can only travel 2 hops. The resulting SCC will be heavily fractured, failing to identify the true strongly connected components.

48. **Question:** 
```python
spark = SparkSession.builder.config("spark.sql.shuffle.partitions", "10").getOrCreate()
# ... load 10 billion edges ...
g.connectedComponents().show()
```
What exception will this code throw?
**Answer:** Executor OutOfMemoryError (OOM) or FetchFailedException.
**Mastery Explanation:** 10 partitions for 10 billion edges means each reducer task is attempting to process 1 billion edges (roughly tens of GBs). This will instantly blow out the executor heap memory during the SortMergeJoin phase.

49. **Question:** 
```scala
val edges = sc.textFile("edges.txt").map(l => {
  val p = l.split(","); Edge(p(0).toLong, p(1).toLong, 1)
})
Graph(vertices, edges).connectedComponents()
```
The input text file contains duplicate edges (e.g., A->B appears 50 times). How does this affect CC?
**Answer:** It inflates shuffle size and join complexity without changing the result.
**Mastery Explanation:** CC only cares if *a* path exists. Duplicate edges force Spark to process, shuffle, and join redundant data. The fix is to add `.distinct()` to the edges RDD/DataFrame before constructing the graph.

50. **Question:** A junior developer attempts to clear disk space by deleting the HDFS checkpoint directory *while* the GraphFrames CC job is on iteration 5 of 10. What happens?
**Answer:** The job fails with a FileNotFoundException during the next lineage resolution or task failure retry.
**Mastery Explanation:** Spark relies on those checkpoint files to truncate the lineage. If a task fails and Spark attempts to recompute a partition, it will look for the checkpointed RDD. If deleted, the execution chain is broken, crashing the job.
