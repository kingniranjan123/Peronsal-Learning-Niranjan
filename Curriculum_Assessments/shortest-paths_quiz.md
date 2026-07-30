# Master Class: Shortest Paths - Elite Assessment

## Section 1: True/False Questions (1-10)

1. **True/False:** In Spark GraphX, the Pregel API minimizes Java Garbage Collection overhead by automatically utilizing Tungsten's off-heap binary memory format.
- **Answer:** False
- **Mastery Explanation:** GraphX is based on the RDD API, meaning it stores vertices and edges as Java objects directly on the JVM heap. It does not utilize Tungsten's off-heap binary memory format, which is a feature of GraphFrames and the DataFrame API.

2. **True/False:** The built-in `shortestPaths` method in GraphFrames is natively optimized to compute both weighted and unweighted shortest paths.
- **Answer:** False
- **Mastery Explanation:** The built-in `shortestPaths` algorithm in GraphFrames computes only the unweighted shortest path (hop count) to a set of landmark vertices. Weighted shortest paths require custom iterative implementations like AggregateMessages.

3. **True/False:** Iterative graph algorithms in Spark without checkpointing can cause the Spark Driver to crash with a `StackOverflowError` due to logical execution plan growth.
- **Answer:** True
- **Mastery Explanation:** Each iteration in Spark adds to the logical and physical execution plan lineage. Without checkpointing to truncate this lineage, the plan can grow to thousands of nodes, causing the Driver to fail during task scheduling.

4. **True/False:** When using GraphFrames' `AggregateMessages`, Catalyst optimizer is bypassed since graph traversal is intrinsically non-relational.
- **Answer:** False
- **Mastery Explanation:** The `AggregateMessages` API in GraphFrames translates message payloads into column expressions, allowing Catalyst to compile the message generation and aggregation step into optimized relational physical plans (like joins and aggregations).

5. **True/False:** To truncate the lineage graph during iterative DataFrame joins, setting `spark.sql.shuffle.partitions` to a high number is sufficient.
- **Answer:** False
- **Mastery Explanation:** `spark.sql.shuffle.partitions` only dictates the number of partitions during shuffles. It does not sever the lineage graph. Lineage must be truncated using `.checkpoint()` or `.localCheckpoint()`.

6. **True/False:** Applying `PartitionStrategy.EdgePartition2D` in GraphX minimizes network shuffling by ensuring all edges for a given vertex are co-located on the same partition.
- **Answer:** False
- **Mastery Explanation:** `EdgePartition2D` provides a 2D grid partitioning strategy that bounds the communication for any vertex to a subset of partitions (row and column). It does not guarantee all edges for a vertex are co-located, but provides an upper bound on network replication to minimize shuffle volume.

7. **True/False:** Using Motif Finding in GraphFrames for 2-hop distances translates to a massive self-join on the edge DataFrame, which can generate extreme data skew in dense graphs.
- **Answer:** True
- **Mastery Explanation:** Motifs like `(a)-[e1]->(b); (b)-[e2]->(c)` are executed as self-joins on the edge table. In dense graphs, certain nodes (hubs) have many edges, leading to massive intermediate data and severe skew.

8. **True/False:** GraphFrames avoids object-instantiation costs during breadth-first search by utilizing Tungsten's specialized bytecode generation for joins.
- **Answer:** True
- **Mastery Explanation:** Because GraphFrames represents graphs as DataFrames, Tungsten can manage memory off-heap and generate specialized bytecode (whole-stage code generation) for iterative joins, dramatically reducing GC pauses compared to RDDs.

9. **True/False:** `localCheckpoint()` is preferred over `checkpoint()` in iterative GraphFrames because it guarantees strict fault tolerance by writing to distributed storage (HDFS).
- **Answer:** False
- **Mastery Explanation:** `localCheckpoint()` writes to the executors' local disks, not distributed storage. It is faster and avoids network I/O, but sacrifices the strict fault tolerance provided by `checkpoint()`.

10. **True/False:** In the GraphX Pregel API, messages are routed to all vertices in every superstep regardless of whether they changed state.
- **Answer:** False
- **Mastery Explanation:** GraphX optimizes bulk-synchronous execution by maintaining active vertex sets. Messages are routed only to vertices that changed state (received a message) in the previous superstep.

## Section 2: Multiple Choice Questions (11-25)

11. Which component is directly responsible for rewriting GraphFrames queries into optimized relational joins?
A) Pregel API
B) Catalyst Optimizer
C) Tungsten Engine
D) DAGScheduler
- **Answer:** B
- **Mastery Explanation:** Catalyst is Spark's query optimizer for DataFrames, rewriting logical graph motifs and AggregateMessages into optimized physical relational joins and aggregations.

12. Why do iterative shortest path algorithms in GraphX frequently cause severe OutOfMemory (OOM) errors?
A) Data is broadcasted to all nodes continuously
B) Catalyst fails to optimize RDDs
C) Vertices and edges are stored as Java objects, creating massive garbage collection overhead
D) Shuffle partitions default to 1
- **Answer:** C
- **Mastery Explanation:** GraphX is RDD-based, meaning frequent vertex updates instantiate millions of Java objects on the heap, leading to prolonged "Stop-The-World" GC pauses and OOMs.

13. In GraphFrames `AggregateMessages`, which function should be used to find the minimum distance between two column values dynamically?
A) `min()`
B) `least()`
C) `math.min()`
D) `reduce()`
- **Answer:** B
- **Mastery Explanation:** `least()` is a PySpark SQL function that evaluates multiple columns in the same row and returns the minimum value, making it perfect for comparing `distance` and `min_msg` during dataframe joins.

14. What partitioning strategy in GraphX is recommended to minimize network shuffling across executor boundaries?
A) HashPartitioner
B) RangePartitioner
C) RandomVertexCut
D) EdgePartition2D
- **Answer:** D
- **Mastery Explanation:** `EdgePartition2D` places edges into a 2D grid of partitions, drastically reducing the number of executors a single vertex's state needs to be replicated to during message passing.

15. What physical operation does the Motif `(a)-[e]->(b)` primarily translate to in GraphFrames?
A) Map-Reduce
B) Edge self-join
C) Vertex-to-Edge Join
D) Broadcast Variable lookup
- **Answer:** C
- **Mastery Explanation:** Finding a single edge motif inherently requires joining the vertex DataFrame (a) with the edge DataFrame (e) and the destination vertex DataFrame (b).

16. What is the output of the built-in `shortestPaths` method in GraphFrames?
A) A single integer representing graph diameter
B) A DataFrame with an array of all path edges
C) A DataFrame where each vertex contains a map of destination IDs and minimum hop counts
D) An RDD of Edge triplets
- **Answer:** C
- **Mastery Explanation:** The built-in method calculates unweighted hop counts to landmarks and stores them in a MapType column within the returned vertex DataFrame.

17. To prevent a `StackOverflowError` on the Driver during iteration 100 of a GraphFrames SSSP, you must:
A) Increase driver memory
B) Use `localCheckpoint()` or `checkpoint()` every 10-15 iterations
C) Use `broadcast()` on the edge DataFrame
D) Increase `spark.sql.shuffle.partitions`
- **Answer:** B
- **Mastery Explanation:** Checkpointing materializes the DataFrame, severing the exponentially growing Catalyst lineage graph that otherwise overwhelms the driver's JVM stack.

18. In the GraphX Pregel API, what is the role of the "Merge Message" function?
A) To update the vertex attributes
B) To filter out vertices that don't need updates
C) To combine multiple incoming messages sent to the same vertex in a single superstep
D) To join the vertex and edge RDDs
- **Answer:** C
- **Mastery Explanation:** The message combiner (`(a, b) => math.min(a, b)`) reduces network overhead by merging multiple incoming distances intended for the same destination vertex before applying the vertex program.

19. Which Spark configuration is critical to tune to keep active iteration data in memory while minimizing disk I/O?
A) `spark.executor.instances`
B) `spark.memory.fraction`
C) `spark.sql.autoBroadcastJoinThreshold`
D) `spark.task.cpus`
- **Answer:** B
- **Mastery Explanation:** `spark.memory.fraction` dictates the proportion of executor heap dedicated to execution vs. storage. Tuning it helps keep large graph joins in memory during iteration.

20. Why might a failure in iteration 50 of an uncheckpointed iterative graph algorithm be catastrophic?
A) The cluster shuts down completely
B) Spark must recompute the entire lineage from iteration 1 due to lazy evaluation
C) Catalyst cannot optimize failed tasks
D) Tungsten memory becomes permanently corrupted
- **Answer:** B
- **Mastery Explanation:** Without checkpointing, Spark relies on lineage for fault tolerance. A failure requires re-evaluating the entire massive lineage chain from the beginning.

21. How does Tungsten accelerate GraphFrames compared to GraphX?
A) By using Java serialization
B) By skipping Catalyst
C) By storing data in an off-heap binary format and generating custom bytecode
D) By disabling garbage collection entirely
- **Answer:** C
- **Mastery Explanation:** Tungsten manages memory natively (off-heap) to avoid JVM GC overhead and uses whole-stage code generation to fuse operations for CPU cache efficiency.

22. In dense graphs where Motifs generate extreme skew, what join type is best if one side (e.g., target nodes) is small?
A) SortMergeJoin
B) ShuffleHashJoin
C) BroadcastHashJoin
D) CartesianJoin
- **Answer:** C
- **Mastery Explanation:** A `BroadcastHashJoin` avoids massive shuffle read/writes by broadcasting the small target DataFrame to all executors, neutralizing data skew on the large edge DataFrame.

23. What does `AM.src` represent in GraphFrames `AggregateMessages`?
A) A reference to the spark context
B) A Column reference to the source vertex of an edge
C) A string literal for the ID
D) The RDD representing edges
- **Answer:** B
- **Mastery Explanation:** `AM.src` and `AM.dst` are DataFrame Column accessors provided by GraphFrames to refer to attributes of the source and destination vertices during message aggregation.

24. What happens when you invoke `g.shortestPaths(landmarks=["A", "B"])` on a weighted graph in GraphFrames?
A) It uses the weights to find Dijkstra's path
B) It ignores weights and finds the unweighted hop count
C) It throws an UnsupportedOperationException
D) It multiplies the weights by hop count
- **Answer:** B
- **Mastery Explanation:** GraphFrames' built-in `shortestPaths` only implements BFS for unweighted paths and completely ignores any `weight` column on edges.

25. Which library requires translating algorithms into a Bulk-Synchronous Parallel (BSP) messaging paradigm natively?
A) Catalyst
B) Tungsten
C) GraphX
D) Spark SQL
- **Answer:** C
- **Mastery Explanation:** GraphX implements the Pregel API, which dictates algorithm design via a bulk-synchronous parallel messaging system (supersteps, vertex programs, message sending).

## Section 3: "Small Twist" Scenario Questions (26-40)

26. **Scenario:** You have a working `shortestPaths(landmarks)` pipeline. A new requirement asks to account for traffic `delay` on edges. 
**Twist:** You swap the built-in method for `shortestPaths(landmarks, weightCol="delay")`. What happens?
- **Answer:** It fails/throws an error.
- **Mastery Explanation:** The built-in `shortestPaths` does not accept a `weightCol`. You must completely rewrite the logic using custom `AggregateMessages` to track weighted SSSP.

27. **Scenario:** Your GraphFrames SSSP job checkpoints to HDFS every 5 iterations, but network I/O is bottlenecking execution. 
**Twist:** You change `.checkpoint()` to `.localCheckpoint()`. What is the architectural tradeoff?
- **Answer:** Execution speeds up, but you lose strict node-failure fault tolerance.
- **Mastery Explanation:** `localCheckpoint()` writes to local executor disks. If an executor dies, the checkpoint data is lost, and lineage recomputation will fail if previous RDDs are cleared.

28. **Scenario:** You use a Motif `(a)-[e]->(b)`. 
**Twist:** You change it to `(a)-[e1]->(b); (b)-[e2]->(c)`. What physical execution change occurs?
- **Answer:** Spark introduces a massive self-join on the Edge DataFrame.
- **Mastery Explanation:** Expanding the structural pattern requires joining the edge table with itself where destination of `e1` equals source of `e2`.

29. **Scenario:** In GraphX Pregel, a vertex receives 5 messages in iteration 1, so it runs the vertex program. 
**Twist:** In iteration 2, it receives 0 messages. What happens to this vertex?
- **Answer:** It becomes inactive and its vertex program is not executed.
- **Mastery Explanation:** GraphX optimizes BSP by only scheduling vertex programs for vertices that actually received a message in the current superstep.

30. **Scenario:** Your Spark job fails with OOM. 
**Twist:** You notice it is the Driver OOMing, not the Executors. What is the most likely cause?
- **Answer:** Exponential logical plan lineage due to a lack of checkpointing.
- **Mastery Explanation:** The driver memory holds the Catalyst execution plan. Iterative DataFrame operations without checkpointing cause this plan to grow indefinitely until it exhausts driver memory.

31. **Scenario:** Your iterative join is spilling to disk. 
**Twist:** You decrease `spark.sql.shuffle.partitions` from 2000 to 20. What is the immediate effect?
- **Answer:** Spilling becomes significantly worse or tasks fail with OOM.
- **Mastery Explanation:** Fewer partitions mean larger individual data chunks per task. This exceeds the execution memory per core, forcing massive disk spills or executor OOMs.

32. **Scenario:** You optimize a motif with a `BroadcastHashJoin`. 
**Twist:** The priority targets DataFrame grows from 100 rows to 50 million rows. What happens?
- **Answer:** The driver crashes with OOM or the broadcast times out.
- **Mastery Explanation:** Broadcasting requires the driver to collect the DataFrame and send it to all executors. A 50M row DataFrame will exceed `spark.driver.memory` and `autoBroadcastJoinThreshold`.

33. **Scenario:** You are aggregating messages in GraphFrames. 
**Twist:** You use `pyspark.sql.functions.min` instead of `least` to compare the old distance and new message distance. What happens?
- **Answer:** Syntax error or incorrect aggregation.
- **Mastery Explanation:** `min()` is an aggregation function for rows within a grouped column. `least()` is designed to find the minimum between multiple column values *within the same row*.

34. **Scenario:** GraphX is running slowly due to shuffle overhead. 
**Twist:** You switch partitioning from `EdgePartition1D` to `EdgePartition2D`. What improves?
- **Answer:** Network shuffle volume drastically decreases, especially for hub nodes.
- **Mastery Explanation:** 1D partitioning hashes by source or destination, meaning hub nodes must broadcast to all partitions. 2D bounds replication to `2 * sqrt(N)` partitions.

35. **Scenario:** You process a graph using RDDs. 
**Twist:** You rewrite the same logic using DataFrames/GraphFrames. What happens to JVM heap utilization?
- **Answer:** Heap utilization drops massively.
- **Mastery Explanation:** GraphFrames leverages Tungsten, which uses an off-heap binary format, entirely bypassing the instantiation of millions of Java objects required by RDDs.

36. **Scenario:** You call `.checkpoint()` in GraphFrames iteration loop. 
**Twist:** You forgot to set `spark.sparkContext.setCheckpointDir()`. What happens?
- **Answer:** The Spark application throws an exception immediately upon the first `.checkpoint()` call.
- **Mastery Explanation:** Reliable checkpointing requires a configured HDFS/S3 directory to write to. Without it, the method fails.

37. **Scenario:** SSSP requires 20 iterations to converge on your graph. 
**Twist:** You set `maxIterations = 10` in Pregel. What does the output represent?
- **Answer:** Partial shortest paths up to a maximum of 10 hops from the source.
- **Mastery Explanation:** The algorithm simply halts after 10 supersteps, returning intermediate distances that haven't fully converged.

38. **Scenario:** Updating vertex distances in GraphFrames using `AM`. 
**Twist:** You use an `inner join` instead of a `left_outer join` when joining old vertices with new aggregated messages. What happens?
- **Answer:** Vertices that received no messages are entirely dropped from the graph.
- **Mastery Explanation:** An inner join discards any vertex that didn't get a message this iteration, effectively destroying the graph topology. A left outer join preserves them.

39. **Scenario:** To be safe, you decide to checkpoint to HDFS. 
**Twist:** You `.checkpoint()` every 1 iteration instead of every 10 iterations. What happens?
- **Answer:** The job slows to a crawl due to severe disk/network I/O bottlenecks.
- **Mastery Explanation:** HDFS writes are expensive. Checkpointing every single iteration incurs more I/O latency than the computation itself. 10-15 is the sweet spot.

40. **Scenario:** You define a 100-iteration `weighted_sssp` loop in GraphFrames. 
**Twist:** You never call `.show()` or `.count()` at the end. What executes?
- **Answer:** Absolutely nothing.
- **Mastery Explanation:** Spark DataFrames are lazily evaluated. Without an action like `.show()`, Catalyst never executes the physical plan.

## Section 4: Coding & Debugging Questions (41-50)

41. **Debug Scenario:** A developer submits a GraphFrames algorithm. It runs fine for 30 minutes, then the Driver throws `java.lang.StackOverflowError`. 
- **Bug:** The developer implemented an iterative loop using DataFrames but forgot to include `localCheckpoint()` or `checkpoint()`.
- **Fix/Mastery Explanation:** Add `new_v = new_v.localCheckpoint()` every ~5-10 iterations to truncate the Catalyst logical plan lineage and free the driver's JVM stack.

42. **Debug Scenario:** A GraphX Pregel job runs smoothly for 2 iterations, but then tasks begin to pause for 45 seconds at a time before eventually throwing `java.lang.OutOfMemoryError: Java heap space` on the executors.
- **Bug:** The graph is highly connected, and generating new edge triplets creates too many Java objects for the Garbage Collector to handle.
- **Fix/Mastery Explanation:** Increase executor memory, utilize `PartitionStrategy.EdgePartition2D`, or migrate to GraphFrames to leverage Tungsten's off-heap memory management.

43. **Debug Scenario:** In GraphFrames AM, the code reads:
`new_v = cached_g.vertices.withColumn("distance", min(col("distance"), col("min_msg")))`
It throws an error: `Column is not iterable` or complains about function signatures.
- **Bug:** The Python built-in `min` or `pyspark.sql.functions.min` is being misused. 
- **Fix/Mastery Explanation:** Replace `min` with `pyspark.sql.functions.least` to compute the row-wise minimum between two columns.

44. **Debug Scenario:** A user calls `g.shortestPaths(landmarks=["NodeX"])` on a graph with heavily weighted edges (representing hours of travel time). The result returns paths that take physically longer but have fewer stops.
- **Bug:** The built-in method ignores weights.
- **Fix/Mastery Explanation:** Implement custom SSSP using `AggregateMessages` to accumulate the `weight` column values, as the built-in method only calculates unweighted hop-counts.

45. **Debug Scenario:** A developer looks at the Spark UI for an iterative motif query `(a)-[e]->(b);(b)-[e]->(c)` and notices one specific task takes 10x longer than the rest, while most finish instantly.
- **Bug:** Data skew caused by a "hub" node (a vertex with an exceptionally high degree of edges).
- **Fix/Mastery Explanation:** Handle skew by salting the join keys, or if filtering for specific end targets, utilize a `BroadcastHashJoin` to bypass the sort-merge shuffle.

46. **Debug Scenario:** In a GraphFrames SSSP loop, the developer writes: 
`new_v.localCheckpoint()`
But the driver still crashes with `StackOverflowError` after 50 iterations.
- **Bug:** DataFrames are immutable. Calling `.localCheckpoint()` returns a new DataFrame, but the developer didn't assign it back to the variable.
- **Fix/Mastery Explanation:** The code must read `new_v = new_v.localCheckpoint()` so subsequent loop iterations use the truncated lineage plan.

47. **Debug Scenario:** After a left outer join in AggregateMessages, vertices that received no messages have their `distance` set to `null` instead of preserving their previous distance.
- **Bug:** The code does `least(col("distance"), col("min_msg"))`. If `min_msg` is null (due to left join mismatch), `least` might yield null or require coalescing.
- **Fix/Mastery Explanation:** Ensure `min_msg` is coalesced to infinity or use `.withColumn("distance", when(col("min_msg").isNull(), col("distance")).otherwise(least(col("distance"), col("min_msg"))))`.

48. **Debug Scenario:** In GraphX Pregel, the message sender function looks like this:
`if (triplet.srcAttr + triplet.attr > triplet.dstAttr) Iterator((triplet.dstId, triplet.srcAttr + triplet.attr))`
The algorithm terminates immediately without finding paths.
- **Bug:** The logical condition for sending a message is reversed.
- **Fix/Mastery Explanation:** It should be `<`. You only send a message if the newly calculated path (`srcAttr + attr`) is strictly *less than* the destination's current known distance (`dstAttr`).

49. **Debug Scenario:** You execute Motif finding:
`g.find("(a)-[e1]->(b)").filter("a.age > 30").show()`
In the Spark UI, it shuffles the entire edge and vertex tables BEFORE filtering out 99% of the nodes.
- **Bug:** Catalyst failed to push down the predicate due to complex expression parsing or bad statistics.
- **Fix/Mastery Explanation:** Pre-filter the vertex DataFrame *before* creating the GraphFrame (e.g., `GraphFrame(v.filter("age > 30"), e)`), ensuring the shuffle volume is minimized preemptively.

50. **Debug Scenario:** The GraphX job fails on iteration 1 with `NullPointerException` on `triplet.dstAttr`.
- **Bug:** The initial graph was not properly initialized with default values (like Infinity for distances), leaving attributes null.
- **Fix/Mastery Explanation:** Use `graph.mapVertices` before starting Pregel to ensure the source is set to `0.0` and all other vertices are explicitly initialized to `Double.PositiveInfinity`.
