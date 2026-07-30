import os
import json

file_content = """# 🔥 Master Class Assessment: GraphX API

## Section 1: True/False Questions (10 Questions)

1. **Question:** Tungsten's off-heap memory management natively applies to GraphX `EdgePartition` arrays, reducing JVM Garbage Collection pressure.
   **Answer:** False
   **Mastery Explanation:** GraphX `EdgePartition` arrays sit on the JVM heap and are subject to GC pressure. GraphX operates at the RDD layer and entirely bypasses the DataFrame/Tungsten stack.

2. **Question:** The `EdgePartition2D` strategy distributes edges across a `sqrt(P) × sqrt(P)` grid to guarantee that vertex attributes are replicated at most `2 * sqrt(P)` times.
   **Answer:** True
   **Mastery Explanation:** This grid distribution bounds vertex replication for hub vertices in power-law graphs, cutting memory usage and network I/O compared to `EdgePartition1D`.

3. **Question:** GraphX automatically leverages the Catalyst optimizer to push down predicates when the `subgraph` method is called.
   **Answer:** False
   **Mastery Explanation:** GraphX operates on raw RDDs and bypassed the Catalyst query planner. Predicate pushdown does not occur automatically; users must manually order operations (e.g., calling `subgraph` early).

4. **Question:** The Pregel API terminates automatically when no messages are generated in a superstep, or when `maxIterations` is reached.
   **Answer:** True
   **Mastery Explanation:** Pregel relies on quiescence (no active messages) or a hard `maxIterations` cap to exit the BSP (Bulk Synchronous Parallel) loop.

5. **Question:** The `graph.triplets` view triggers a full cross-partition shuffle of the `VertexRDD` against the `EdgeRDD` on every iteration.
   **Answer:** False
   **Mastery Explanation:** GraphX uses a routing table (a compressed bitset in the `VertexRDD`) to replicate vertex attributes only to the specific edge partitions that need them, avoiding a full shuffle.

6. **Question:** Passing `TripletFields.Src` to `aggregateMessages` minimizes network I/O if the computation only requires source vertex attributes.
   **Answer:** True
   **Mastery Explanation:** By default, GraphX routes both source and destination attributes. Using `TripletFields.Src` instructs the routing table to skip broadcasting destination attributes, saving up to 50% network I/O.

7. **Question:** A non-commutative `mergeMsg` function in `aggregateMessages` will cause Spark to throw an `UnsupportedOperationException` during the shuffle phase.
   **Answer:** False
   **Mastery Explanation:** Spark cannot statically verify commutativity. It will execute the job, but because messages are merged in tree-reduction order (which is arbitrary), the results will be nondeterministic.

8. **Question:** Calling `outerJoinVertices` on a Graph and a `VertexRDD` always triggers a shuffle to align vertex IDs.
   **Answer:** False
   **Mastery Explanation:** If both RDDs are already hash-partitioned with the same partitioner and partition count, Spark detects the co-partitioning and eliminates the shuffle, replacing it with a local zip.

9. **Question:** The BSP barrier between Pregel supersteps is implemented internally via an RDD `count()` action.
   **Answer:** True
   **Mastery Explanation:** Spark enforces the barrier by invoking `count()` on the updated Vertex RDD, which forces materialization and ensures exactly-once message delivery per superstep.

10. **Question:** `RandomVertexCut` is the optimal partitioning strategy for real-world social network graphs to prevent executor OOMs.
    **Answer:** False
    **Mastery Explanation:** `RandomVertexCut` uniformly hashes edges but does not bound vertex replication. `EdgePartition2D` is the optimal strategy for power-law (social network) graphs.

---

## Section 2: Multiple Choice Questions (15 Questions)

11. **Question:** What is the primary purpose of the GraphX routing table?
    A) To store the physical memory addresses of edges
    B) To map each vertex ID to the set of edge partitions that require its attribute
    C) To push down filters to the Catalyst optimizer
    D) To schedule DAG execution for iterative algorithms
    **Answer:** B
    **Mastery Explanation:** The routing table is a bitset structure that allows GraphX to selectively broadcast vertex attributes only to the edge partitions containing adjacent edges, preventing full shuffles.

12. **Question:** Why does `EdgePartition1D` cause catastrophic failures on power-law graphs?
    A) It triggers whole-stage codegen bugs
    B) It requires Tungsten memory limits to be doubled
    C) All edges originating from a massive hub vertex land in a single JVM partition, causing OOMs
    D) It forces the graph to be undirected
    **Answer:** C
    **Mastery Explanation:** `EdgePartition1D` partitions solely by source vertex ID. A celebrity with 50M followers will place 50M edges in one partition, overwhelming a single executor's heap memory.

13. **Question:** When implementing Single-Source Shortest Paths (SSSP) via Pregel, what is the most appropriate value for the `initialMsg`?
    A) `0.0`
    B) `Double.PositiveInfinity`
    C) `Double.NaN`
    D) `Double.MinValue`
    **Answer:** B
    **Mastery Explanation:** The `initialMsg` is sent to all vertices before superstep 0. Using `+Infinity` ensures that unreachable vertices maintain their infinite distance when evaluated by `vprog`.

14. **Question:** Which GraphX operator should you use to modify edge attributes based solely on the current edge attribute and vertex attributes, WITHOUT requiring a shuffle?
    A) `aggregateMessages`
    B) `mapTriplets`
    C) `joinVertices`
    D) `groupEdges`
    **Answer:** B
    **Mastery Explanation:** `mapTriplets` generates a new edge attribute locally within the triplet view. Unlike `aggregateMessages`, it does not aggregate data to vertices and therefore requires no shuffle.

15. **Question:** What happens if you forget to use a conditional send (`if (changed) Iterator(msg) else Iterator.empty`) in a Pregel `sendMsg` function?
    A) The graph becomes disconnected.
    B) Spark throws an `EmptyIteratorException`.
    C) The algorithm loops infinitely or until `maxIterations`, performing a full shuffle every superstep.
    D) The DAG planner skips the supersteps.
    **Answer:** C
    **Mastery Explanation:** Unconditional sends prevent the graph from reaching quiescence. Each superstep will trigger a `count()` barrier and shuffle, wasting hours of compute.

16. **Question:** What is the worst-case time complexity (in supersteps) for `graph.connectedComponents()`?
    A) O(1)
    B) O(E)
    C) O(V · diameter)
    D) O(log V)
    **Answer:** C
    **Mastery Explanation:** In a worst-case path graph topology (a straight line of vertices), the minimum ID must propagate one hop per superstep, taking O(V) iterations.

17. **Question:** In GraphX, the `VertexRDD[VD]` is physically backed by:
    A) `Parquet` columnar files
    B) An `RDD[(VertexId, VD)]` indexed by a `VertexAttributeBlock`
    C) A broadcast variable of a HashMap
    D) Tungsten UnsafeRows
    **Answer:** B
    **Mastery Explanation:** `VertexRDD` maintains an index (`VertexAttributeBlock`) per partition to enable O(log n) attribute lookups and efficient local aggregations (`aggregateUsingIndex`).

18. **Question:** Why should you call `graph.cache()` before entering a GraphX iterative algorithm?
    A) To convert the RDDs to DataFrames
    B) To force Spark to use off-heap memory
    C) To prevent the JVM from re-reading source data and rebuilding the routing table on every superstep
    D) To enable Catalyst predicate pushdown
    **Answer:** C
    **Mastery Explanation:** Iterative algorithms reuse the graph lineage. Without caching, the entire RDD lineage (including disk reads and routing table construction) is re-evaluated on every superstep.

19. **Question:** If you have a graph where edge weights denote distance, and you want to sum the distances of all incoming edges per vertex, which `mergeMsg` is correct?
    A) `(a, b) => math.min(a, b)`
    B) `(a, b) => a + b`
    C) `(a, b) => Seq(a, b)`
    D) `(a, b) => a.toDouble + b.toDouble`
    **Answer:** B
    **Mastery Explanation:** `(a, b) => a + b` is the associative and commutative sum operation required to correctly accumulate weights at the destination vertex.

20. **Question:** What does the `defaultVertexAttr` parameter in `Graph(...)` construction handle?
    A) Vertices with negative IDs
    B) Vertices with a degree of 0
    C) Dangling edges that reference a destination or source ID not present in the `vertices` RDD
    D) Missing edge attributes
    **Answer:** C
    **Mastery Explanation:** It provides a fallback attribute so GraphX can instantiate a complete triplet even when the underlying data has referential integrity issues (edges pointing to non-existent vertices).

21. **Question:** How does `subgraph` improve performance when called before `connectedComponents`?
    A) It triggers Catalyst optimizations.
    B) It eliminates irrelevant vertices and edges, shrinking the routing table and reducing Pregel message volume.
    C) It automatically caches the graph in memory.
    D) It skips the BSP barrier.
    **Answer:** B
    **Mastery Explanation:** Filtering early removes data from the triplet view. When the routing table is rebuilt, fewer vertices are replicated, and Pregel processes fewer active edges.

22. **Question:** Which statement about `EdgePartition` storage in GraphX is correct?
    A) It uses Java objects for every edge, causing massive heap overhead.
    B) It uses parallel primitive arrays (`srcIds`, `dstIds`, `attrs`) stored on the JVM heap.
    C) It stores data off-heap using Java NIO DirectBuffers.
    D) It serializes edges using Kryo into a single byte array.
    **Answer:** B
    **Mastery Explanation:** `EdgePartition` uses a columnar format of primitive arrays on the heap. This avoids per-object overhead but is still subject to JVM garbage collection.

23. **Question:** If you pass `TripletFields.None` to `aggregateMessages`, what data is broadcasted to the edge partitions?
    A) Both source and destination vertex attributes
    B) Only source attributes
    C) Only destination attributes
    D) Neither; only edge attributes are accessible in the `sendMsg` context
    **Answer:** D
    **Mastery Explanation:** `TripletFields.None` optimizes the routing table to broadcast absolutely no vertex attributes, drastically reducing network traffic when only edge data is needed.

24. **Question:** The `outerJoinVertices` method is typically used to:
    A) Remove vertices that have no edges
    B) Join the results of a graph algorithm (like PageRank) back into the original graph's vertex attributes
    C) Perform a cross-join of two graphs
    D) Convert a Graph into a DataFrame
    **Answer:** B
    **Mastery Explanation:** Algorithms produce a `VertexRDD[Result]`. `outerJoinVertices` allows you to align this result RDD with the original graph to produce an enriched `Graph[VD_New, ED]`.

25. **Question:** What is the fundamental limitation of GraphX that makes GraphFrames more appealing for some workloads?
    A) GraphX only supports undirected graphs.
    B) GraphX does not support iterative algorithms.
    C) GraphX relies on RDDs and lacks Catalyst optimizer integration and off-heap execution.
    D) GraphX cannot handle graphs larger than 1 million edges.
    **Answer:** C
    **Mastery Explanation:** GraphX bypassed the DataFrame/SQL engine. GraphFrames wraps GraphX in DataFrames, enabling Catalyst optimizations, Tungsten execution, and SQL-based motif finding.

---

## Section 3: "Small Twist" Scenario Questions (15 Questions)

26. **Scenario:** You run `Pregel` with `activeDirection = EdgeDirection.Out`. 
    **Twist:** You change it to `EdgeDirection.Either`. What happens?
    **Answer:** `sendMsg` is evaluated for both incoming and outgoing edges of active vertices.
    **Mastery Explanation:** `EdgeDirection.Out` halves the number of triplets evaluated on directed graphs. Changing it to `Either` doubles the triplet evaluation overhead but is necessary for undirected traversals.

27. **Scenario:** You apply a `subgraph` filter where `epred` drops 90% of edges.
    **Twist:** You immediately call `graph.triplets.count()`. Does this trigger a shuffle?
    **Answer:** No.
    **Mastery Explanation:** `subgraph` applies the predicate locally to the `EdgePartition` arrays. While the routing table becomes stale and is rebuilt locally, no cross-partition shuffle is required for a simple filter.

28. **Scenario:** In `aggregateMessages`, your `mergeMsg` is `(a, b) => a - b`.
    **Twist:** You run the job on a cluster with 50 executors. What is the result?
    **Answer:** Nondeterministic and incorrect results.
    **Mastery Explanation:** Subtraction is not commutative or associative. Because Spark merges messages in arbitrary tree-reduction order across partitions, the final result will vary unpredictably on every run.

29. **Scenario:** A power-law graph ingestion job fails with `OutOfMemoryError` using `EdgePartition1D`.
    **Twist:** You switch to `PartitionStrategy.RandomVertexCut`. Does this solve the OOM?
    **Answer:** Yes, but it causes excessive network I/O.
    **Mastery Explanation:** `RandomVertexCut` hashes edges uniformly, solving the single-executor OOM. However, unlike `EdgePartition2D`, it does not mathematically bound vertex replication, leading to massive routing table broadcast costs.

30. **Scenario:** You join two `VertexRDD`s using `outerJoinVertices`.
    **Twist:** The second `VertexRDD` was created by filtering the first one, but you explicitly called `.repartition(100)` on it before the join.
    **Answer:** A full shuffle occurs.
    **Mastery Explanation:** By calling `.repartition()`, you destroyed the co-partitioning guarantee (same hash partitioner). Spark is forced to shuffle data to align the Vertex IDs, degrading performance.

31. **Scenario:** You are writing SSSP in Pregel. 
    **Twist:** The graph contains negative weight edges and a negative cycle. You leave `maxIterations = Int.MaxValue`.
    **Answer:** The job loops infinitely.
    **Mastery Explanation:** In a negative cycle, the candidate distance strictly decreases on every iteration. `sendMsg` will endlessly emit messages, preventing quiescence and causing an infinite loop.

32. **Scenario:** You cache a Graph with `graph.cache()`.
    **Twist:** You forget to call `graph.vertices.count()` or another action immediately after.
    **Answer:** The caching is lazy; the first actual iteration of Pregel will bear the full cost of materialization.
    **Mastery Explanation:** RDD caching in Spark is lazy. Eager cache warm-up via a forcing action (`count()`) is a best practice to ensure the JVM heap is populated before the complex BSP loop begins.

33. **Scenario:** Your `sendMsg` function unconditionally returns `Iterator((triplet.dstId, msg))`.
    **Twist:** You set `maxIterations = 5`. What happens?
    **Answer:** The algorithm executes exactly 5 supersteps and returns the intermediate graph state.
    **Mastery Explanation:** Because it never quiesces, Pregel hits the `maxIterations` hard limit. It terminates gracefully but returns a graph that has likely not fully converged.

34. **Scenario:** You define a Graph with `vertices` and `edges`. Vertex 99 is in the `edges` RDD but missing from `vertices`.
    **Twist:** You pass `defaultVertexAttr = "Unknown"` during `Graph()` construction. What does the triplet look like?
    **Answer:** The triplet will have `srcAttr` or `dstAttr` as `"Unknown"`.
    **Mastery Explanation:** GraphX ensures referential integrity by dynamically injecting the missing vertex into the `VertexRDD` with the provided `defaultVertexAttr`, allowing the triplet to form.

35. **Scenario:** You call `mapTriplets` to update edge weights.
    **Twist:** You realize you need to aggregate these weights by destination vertex. Can you do this inside `mapTriplets`?
    **Answer:** No.
    **Mastery Explanation:** `mapTriplets` only yields a new `EdgeRDD`; it cannot route or reduce data across vertices. You must use `aggregateMessages` to perform destination-side reduction.

36. **Scenario:** You use `TripletFields.EdgeOnly` in `aggregateMessages`.
    **Twist:** Inside `sendMsg`, you attempt to read `triplet.srcAttr`.
    **Answer:** A NullPointerException (or unexpected default value) occurs at runtime.
    **Mastery Explanation:** The `TripletFields` hint tells the routing table not to populate vertex attributes in the triplet. Reading them when excluded leads to invalid state or exceptions.

37. **Scenario:** You have a heavily skewed graph. You apply `EdgePartition2D`.
    **Twist:** You configure `spark.default.parallelism` to a prime number, like `101`.
    **Answer:** `EdgePartition2D` scales the grid to `ceil(sqrt(101)) = 11`, generating a grid of 121 partitions.
    **Mastery Explanation:** GraphX requires a perfect square for 2D partitioning. It rounds up the partition count, meaning 20 partitions might end up empty, slightly affecting load balancing.

38. **Scenario:** You execute `connectedComponents` on a disconnected graph consisting of two isolated cliques.
    **Twist:** You use Pregel with `maxIterations = Int.MaxValue`.
    **Answer:** The algorithm converges quickly and terminates safely.
    **Mastery Explanation:** Quiescence is evaluated per-component. Once both cliques independently reach internal consensus on their minimum vertex ID, messages stop, and the BSP loop exits.

39. **Scenario:** You modify an edge property using `mapTriplets`.
    **Twist:** You want to persist the modified graph back to HDFS as an edge list. You use `graph.edges.saveAsTextFile()`. Does it include vertex properties?
    **Answer:** No.
    **Mastery Explanation:** The `EdgeRDD` only contains `(srcId, dstId, attr)`. To include vertex properties, you would need to save `graph.triplets`, which contains the co-located attributes.

40. **Scenario:** In an iterative PageRank algorithm using manual `aggregateMessages`, you update the `VertexRDD` in a `var` loop.
    **Twist:** You forget to call `unpersist()` on the previous iteration's `VertexRDD`.
    **Answer:** The driver runs out of memory tracking lineage, and executors hold stale RDD blocks until memory pressure forces LRU eviction.
    **Mastery Explanation:** RDD lineages in while-loops build up rapidly. Failing to unpersist intermediate RDDs causes memory leaks and DAGScheduler overhead (StackOverflowError during planning).

---

## Section 4: Coding & Debugging Questions (10 Questions)

41. **Debugging: The "Infinite Superstep" Trap**
    **Code:** 
    ```scala
    graph.pregel(0.0)(
      vprog = (id, attr, msg) => attr + msg,
      sendMsg = triplet => Iterator((triplet.dstId, 1.0)),
      mergeMsg = (a, b) => a + b
    )
    ```
    **Error/Issue:** The job never finishes.
    **Mastery Explanation:** The `sendMsg` unconditionally returns `1.0` for every triplet on every iteration. Since messages are always generated, quiescence is never reached. Fix it by adding a condition (e.g., only send if state changed).

42. **Coding: Optimal Subgraph Filtering**
    **Scenario:** You need to compute PageRank on a subgraph of "active" users.
    **Bad Practice:** Run PageRank, then use `subgraph` to filter.
    **Correct Fix:** Call `subgraph` first, then run PageRank.
    **Mastery Explanation:** Running `subgraph` first removes edges and vertices from the routing table and Pregel iterations, reducing the computational matrix drastically. Doing it after wastes CPU on discarded entities.

43. **Debugging: Nondeterministic Results in Message Merge**
    **Code:** 
    ```scala
    graph.aggregateMessages[List[String]](
      sendMsg = ctx => ctx.sendToDst(List(ctx.srcAttr.name)),
      mergeMsg = (a, b) => a ::: b 
    )
    ```
    **Error/Issue:** The resulting `List[String]` has a different order on every run.
    **Mastery Explanation:** List concatenation (`:::`) is associative but NOT commutative. Spark applies reductions in non-deterministic tree orders. Use a `Set` or sort the list post-aggregation.

44. **Coding: Preventing Lineage StackOverflow**
    **Scenario:** Implementing custom PageRank with a manual 50-iteration `while` loop using `aggregateMessages`.
    **Fix:** Implement lineage truncation using `localCheckpoint()` or periodic `.checkpoint()` every 10 iterations.
    **Mastery Explanation:** A 50-iteration loop creates a DAG with hundreds of stages. The Spark driver will throw a `StackOverflowError` during query planning. Checkpointing truncates the lineage DAG.

45. **Debugging: NullPointerException in sendMsg**
    **Code:** 
    ```scala
    graph.aggregateMessages(
      sendMsg = ctx => ctx.sendToDst(ctx.srcAttr * ctx.attr),
      mergeMsg = _ + _,
      TripletFields.EdgeOnly
    )
    ```
    **Error/Issue:** Fails with NPE.
    **Mastery Explanation:** `TripletFields.EdgeOnly` prevents `srcAttr` from being routed to the edge partition. When `ctx.srcAttr` is accessed in `sendMsg`, it evaluates to null. Fix by changing to `TripletFields.Src` or `TripletFields.All`.

46. **Coding: Eager Graph Caching**
    **Scenario:** `graph.cache()` is called, but the first Pregel iteration still takes 45 minutes, while subsequent ones take 2 minutes.
    **Fix:** Append `.vertices.count()` and `.edges.count()` immediately after `.cache()`.
    **Mastery Explanation:** Caching is lazy. The first Pregel superstep forces materialization, bearing the cost of parsing, partitioning, and routing table creation. Eager evaluation isolates this cost.

47. **Debugging: OOM on Power-Law Ingestion**
    **Symptom:** Ingesting a Twitter dataset. Stage 1 succeeds, but Stage 2 fails with `java.lang.OutOfMemoryError: Java heap space` on exactly one executor.
    **Fix:** `Graph(vertices, edges).partitionBy(PartitionStrategy.EdgePartition2D)`
    **Mastery Explanation:** `EdgePartition1D` placed all edges of a massive influencer into a single JVM partition. `EdgePartition2D` shatters this hub across a 2D grid, capping heap memory per executor.

48. **Coding: Safe Initialization in Pregel**
    **Scenario:** You want to compute connected components, initializing `vprog` only if it's the very first superstep.
    **Implementation:** Pass a specific sentinel value as `initialMsg` (e.g., `Long.MaxValue`). In `vprog`, check `if (msg == Long.MaxValue) { init() } else { update() }`.
    **Mastery Explanation:** Pregel evaluates `vprog` for ALL vertices before superstep 0 using the `initialMsg`. This sentinel pattern is the standard way to distinguish initialization from standard message merging.

49. **Debugging: Missing Vertices in aggregateMessages Result**
    **Symptom:** `aggregateMessages` returns a `VertexRDD` with 10,000 rows, but the original graph has 15,000 vertices.
    **Fix:** Use `graph.outerJoinVertices(aggregatedRdd) { case (id, oldAttr, Some(msg)) => ... case (id, oldAttr, None) => fallback }`.
    **Mastery Explanation:** `aggregateMessages` only yields rows for vertices that *received* a message. Vertices with an in-degree of 0 are omitted. `outerJoinVertices` safely handles the `None` case.

50. **Coding: Extracting Edge Properties without Shuffles**
    **Scenario:** You have a graph and want a new graph where edge attributes are boolean flags (weight > 10).
    **Fix:** Use `val newGraph = graph.mapEdges(e => e.attr > 10)`.
    **Mastery Explanation:** Avoid `aggregateMessages` and `mapTriplets` if you don't need vertex attributes. `mapEdges` operates strictly on the `EdgeRDD` without touching the routing table, offering maximum performance.
"""

file_path = r"d:\Desktop\13th August 2023\python-output\python-inputs\a-process-telegram-uploads\Spark-In-Action\Curriculum_Assessments\graphx-api_quiz.md"

os.makedirs(os.path.dirname(file_path), exist_ok=True)

with open(file_path, "w", encoding="utf-8") as f:
    f.write(file_content)

print(f"Successfully wrote {len(file_content)} characters to {file_path}")
