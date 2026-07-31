# 🔥 Master Class: A* Search Algorithm in Apache Spark

## Overview

A* (A-Star) is an informed heuristic search algorithm that finds the shortest path between two nodes in a weighted graph by combining the actual cost of reaching a node (`g(n)`) with an admissible heuristic estimate of the remaining cost to the goal (`h(n)`). The combined score `f(n) = g(n) + h(n)` guides a priority queue — the open set — so that the most promising nodes are always expanded first. Unlike Dijkstra's algorithm, which explores outward uniformly, A* focuses its frontier toward the goal, dramatically pruning the search space when a tight, admissible heuristic is available.

In the Spark ecosystem, A* presents a fundamental architectural tension: the algorithm is inherently sequential and stateful — each expansion depends on the current minimum of the priority queue — yet Spark is built for massively parallel, stateless transformations. Resolving this tension requires choosing between three distinct deployment patterns: (1) running classical A* on the **driver JVM** using graph data pulled from distributed RDDs, (2) adapting A* into the **Pregel superstep model** via GraphX for graph-parallel traversal, and (3) launching **multiple independent A* searches in parallel** across the cluster using `mapPartitions` or `RDD.map`. Each pattern carries radically different memory, serialization, and fault-tolerance implications.

The reason A* matters at scale is that modern production graphs — road networks, knowledge graphs, logistics networks — contain billions of edges. Fetching a subgraph to the driver is impractical. GraphX Pregel allows the graph to remain distributed while still converging on shortest paths through iterative message passing, at the cost of per-superstep shuffle overhead and a relaxed execution model that approximates, rather than exactly replicates, classical A* ordering. 

---

## 🏗️ Architectural Deep Dive 

### How It Works Under the Hood

On the **driver JVM**, classical A* requires three data structures: a min-heap priority queue ordered by `f(n)`, a `gScore` map from node ID to best-known cost, and a `cameFrom` map for path reconstruction. All three live in driver heap memory, which is configured via `spark.driver.memory` (default: 1 GB). For graphs with millions of nodes, the open set can grow to consume hundreds of megabytes — each `PriorityQueue` entry in Scala is a boxed JVM object, typically 48–96 bytes per entry with header, pointer, and field overhead. At one million nodes, this becomes 48–96 MB of live heap, triggering frequent young-generation GC cycles. The driver's G1GC pauses directly delay task scheduling since the driver runs the `DAGScheduler` on the same JVM. Setting `-XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:MaxGCPauseMillis=200` in `spark.driver.extraJavaOptions` mitigates this.

The **GraphX Pregel API** adapts A* into a bulk-synchronous parallel (BSP) model. In each superstep, every active vertex receives messages from its neighbors, merges them using a user-defined `mergeMsg` function, updates its local `gScore`, and emits new messages along edges where relaxation improved the score. Catalyst is not involved here — GraphX operates on RDDs, not DataFrames, so there is no query optimizer. Instead, Spark's `DAGScheduler` materializes the RDD lineage of each superstep as a new stage, with a shuffle boundary between the `aggregateMessages` and the vertex join. Each superstep therefore incurs one full shuffle, with data serialized using Kryo (if configured via `spark.serializer=org.apache.spark.serializer.KryoSerializer`) or the default Java serializer — a 3–5x difference in serialization throughput.

**Kryo serialization** is critical for A* in GraphX because vertex attributes (gScore, fScore, predecessor, open/closed status) are transmitted across the network on every superstep. Java serialization of a case class with four fields produces ~300–400 bytes per message. Kryo with explicit class registration reduces this to 40–60 bytes — a 6–8x reduction — directly cutting shuffle write volume and network I/O. Registering classes via `spark.kryo.classesToRegister` or a custom `KryoRegistrator` ensures Kryo uses integer class IDs rather than full class names. Failure to register forces Kryo into fallback mode, adding ~50 bytes of class-name overhead per object and negating most of the serialization gain.

The Pregel model does **not** guarantee A*'s node-expansion order. Classical A* expands nodes in strict `f(n)` order, ensuring optimality with an admissible heuristic. In Pregel, all active vertices in a superstep expand simultaneously regardless of their `f(n)` value. This means Pregel-based A* behaves more like a parallel Bellman-Ford with a heuristic bias — it still converges to the optimal path (given consistent heuristics and sufficient supersteps), but it may process more relaxations than necessary, increasing total work by a factor proportional to the graph diameter.

```text
Driver JVM (spark.driver.memory) Executor JVM (spark.executor.memory)
┌──────────────────────────────────┐ ┌──────────────────────────────────────────┐
│ Classical A* (single-source) │ │ GraphX Pregel A* (distributed) │
│ │ │ │
│ PriorityQueue<Node> [heap] │ │ VertexRDD[(gScore, fScore, pred)] │
│ gScore: HashMap<Long,Double> │ │ EdgeRDD[(weight)] │
│ cameFrom: HashMap<Long,Long> │ │ │
│ │ Superstep │ ┌──────────────┐ ┌──────────────┐ │
│ graph data ◀──── collect() ──── │◀─────────────│ │ Partition 0 │ │ Partition 1 │ │
│ (entire graph in driver heap!) │ │ │ vprog() │ │ vprog() │ │
│ │ │ │ sendMsg() │ │ sendMsg() │ │
│ DAGScheduler │ │ └──────┬───────┘ └──────┬───────┘ │
│ TaskScheduler │ │ │ Shuffle/Kryo │ │
│ BlockManager │ │ ▼ (aggregateMsg) ▼ │
└──────────────────────────────────┘ │ ┌──────────────────────────────────┐ │
 │ │ mergeMsg: min(gScore) │ │
 ┌──────────────────────────┐ │ │ (one shuffle per superstep) │ │
 │ Parallel Multi-Source │ │ └──────────────────────────────────┘ │
 │ A* via mapPartitions │ └──────────────────────────────────────────┘
 │ │
 │ RDD[QueryPair] ──────▶ │ Each partition runs
 │ .mapPartitions { │ a full in-memory A*
 │ localAstar(...) │ on a subgraph shard
 │ } │
 └──────────────────────────┘ 
```

### Key Internal Components

- **PriorityQueue (Driver Heap):** Scala's `scala.collection.mutable.PriorityQueue` is a binary max-heap; for A* you must negate `f(n)` or use a custom `Ordering` to get min-heap behavior. Each enqueue/dequeue is O(log n) against the open set size. At 10 million enqueues, this is ~230 million comparisons — entirely single-threaded on the driver.

- **GraphX VertexRDD / EdgeRDD:** Internally backed by `ShuffledRDD` and stored in executor off-heap memory when `spark.memory.offHeap.enabled=true`. Off-heap storage for graph attributes eliminates GC pressure from large vertex arrays but requires Unsafe-based serialization — Kryo registration becomes mandatory, not optional.

- **Pregel `vprog` / `sendMsg` / `mergeMsg`:** These three lambda functions are serialized by Kryo and shipped to executors. Closures that capture large driver-side objects (e.g., a broadcast heuristic lookup table) will serialize the entire captured object on every superstep unless explicitly wrapped in a `Broadcast[T]` variable via `sc.broadcast()`.

- **ShuffleManager (SortShuffleManager):** Each Pregel superstep's `aggregateMessages` triggers a sort-based shuffle. With 100 million edges and 50 active vertices per partition, shuffle write volume per superstep can reach 4–8 GB uncompressed. Enabling `spark.shuffle.compress=true` with `LZ4` codec reduces this by 40–60%, at ~2% CPU overhead per task. 

---

## ⚠️ Critical Concepts & Common Pitfalls 

### The Admissibility vs. Consistency Trap in Pregel

Classical A* requires an *admissible* heuristic (never overestimates) for optimality and a *consistent* (monotone) heuristic for guaranteed single-expansion per node. In the Pregel model, the single-expansion guarantee is impossible because multiple supersteps can relax the same vertex. If your heuristic is admissible but not consistent, Pregel A* may still converge to the correct answer but requires re-opening already-settled vertices across supersteps, dramatically increasing iteration count.

The practical failure mode is infinite or near-infinite superstep loops when the heuristic is inadmissible — even slightly. A heuristic that overestimates by just 0.1% can cause Pregel to continue issuing messages to already-optimal vertices because the local `f(n)` comparison never settles. Always validate your heuristic against ground-truth distances on a 10,000-node subgraph before running at scale, and cap Pregel iterations with a `maxIterations` bound to prevent runaway jobs that consume cluster resources indefinitely. 

### Driver Memory Overflow with Large Open Sets

When running A* on the driver, the open set size is bounded in the worst case by the number of reachable nodes — which for a road network graph with 50 million nodes means a `PriorityQueue` holding up to 50 million entries. At 80 bytes per entry (object header, four primitive fields, heap array pointer), this is 4 GB — exceeding the typical `spark.driver.memory=4g` setting and triggering `java.lang.OutOfMemoryError: Java heap space`. This error surfaces in the Spark UI as a failed stage with "Driver lost" or "Executor lost (driver)" and the SparkContext becomes invalid, requiring a full application restart.

The mitigation is a beam-search approximation: cap the open set at `K` entries (e.g., K=100,000) by evicting high-`f(n)` nodes when the heap exceeds the cap. This converts A* into a bounded-memory approximation that trades optimality for survivability at scale. Alternatively, partition the graph spatially (by geographic bounding box or community detection) and run A* on a compressed "highway" graph with precomputed boundary costs, a technique used in production map routing systems. 

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|------------|----------|-------|
| Driver-side A* (heap) | O((V + E) log V) | No | Single-threaded; bottleneck is driver GC and heap size |
| Pregel superstep (aggregateMessages) | O(E / P) per step | Yes | One full shuffle per superstep; P = parallelism |
| Kryo serialize VertexAttr | O(fields) ≈ O(1) | N/A | ~50 bytes/vertex with registration vs ~350 bytes Java |
| Parallel multi-source A* (mapPartitions) | O((V/P + E/P) log(V/P)) | No | Embarrassingly parallel; requires graph pre-partitioned by query |
| Path reconstruction (collect + trace) | O(path length) | No | Runs on driver after convergence; negligible for short paths | 

---

## 💻 Code Examples

### Example 1: Classical A* on the Driver Using a Broadcast Heuristic Table

> **What this demonstrates:** How to run single-source A* on the driver JVM using a graph materialized from a distributed EdgeRDD, with the heuristic function served from a broadcast variable to avoid closure-capture serialization of the full heuristic map.

```scala
import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable
import org.apache.spark.rdd.RDD

// -- Step 1: Define the vertex attribute (node ID → heuristic h(n) to goal)
// This map is large (millions of entries) — broadcast prevents re-serializing
// it on every closure invocation. spark.broadcast uses TorrentBroadcast,
// which chunks the object into 4 MB blocks and distributes via BitTorrent-style protocol.
val heuristicMap: Map[Long, Double] = buildHeuristicMap() // e.g., Euclidean distances
val bcHeuristic: Broadcast[Map[Long, Double]] = sc.broadcast(heuristicMap)

// -- Step 2: Collect the adjacency list to the driver.
// WARNING: Only safe if the graph fits in driver heap (spark.driver.memory).
// For a 1M-node graph with avg degree 4, this is ~32 MB of edge data — acceptable.
val edges: RDD[(Long, Long, Double)] = loadEdgesFromParquet(spark, "/data/graph/edges")
val adjacencyList: Map[Long, List[(Long, Double)]] =
 edges
 .groupBy(_._1) // Group by source node
 .mapValues(_.map(e => (e._2, e._3)).toList) // (dest, weight) pairs
 .collect()
 .toMap // Now in driver heap as HashMap

// -- Step 3: Classic A* on the driver — pure Scala, no Spark shuffle
def astar(
 graph: Map[Long, List[(Long, Double)]],
 start: Long,
 goal: Long,
 h: Long => Double // Admissible heuristic: h(n) <= true_cost(n, goal)
): Option[List[Long]] = {

 // Min-heap: Scala's PriorityQueue is a MAX-heap by default.
 // We negate f(n) to simulate min-heap behavior — a common Scala gotcha.
 implicit val ord: Ordering[(Double, Long)] = Ordering.by(-_._1)
 val openSet = mutable.PriorityQueue[(Double, Long)]()

 val gScore = mutable.HashMap[Long, Double]().withDefaultValue(Double.MaxValue)
 val cameFrom = mutable.HashMap[Long, Long]()

 gScore(start) = 0.0
 openSet.enqueue((h(start), start)) // f(start) = 0 + h(start)

 while (openSet.nonEmpty) {
 val (_, current) = openSet.dequeue()

 // Goal check: dequeuing the goal guarantees the optimal path (admissible h)
 if (current == goal) {
 // Reconstruct path by tracing cameFrom chain
 val path = mutable.ListBuffer[Long](current)
 var node = current
 while (cameFrom.contains(node)) {
 node = cameFrom(node)
 path.prepend(node)
 }
 return Some(path.toList)
 }

 // Expand neighbors: for each outgoing edge (neighbor, edgeWeight)
 graph.getOrElse(current, Nil).foreach { case (neighbor, weight) =>
 val tentativeG = gScore(current) + weight

 // Only relax if we found a strictly better path to 'neighbor'
 if (tentativeG < gScore(neighbor)) {
 cameFrom(neighbor) = current
 gScore(neighbor) = tentativeG
 // f(neighbor) = g(neighbor) + h(neighbor) — push new entry, don't update
 // This "lazy deletion" pattern avoids O(n) heap decrease-key operations.
 openSet.enqueue((tentativeG + h(neighbor), neighbor))
 }
 }
 }
 None // No path found
}

// -- Step 4: Execute A* using the broadcast heuristic
val result = astar(
 adjacencyList,
 start = 1001L,
 goal = 9999L,
 h = nodeId => bcHeuristic.value.getOrElse(nodeId, 0.0)
)

result.foreach(path => println(s"Optimal path: ${path.mkString(" → ")}"))
```

> **Mastery Note:** The "lazy deletion" pattern used here — pushing duplicate entries into the priority queue rather than performing decrease-key — is the standard JVM implementation because Java's `PriorityQueue` and Scala's equivalent do not support O(log n) decrease-key. This means the open set can grow to O(E) in the worst case rather than O(V), increasing memory pressure on the driver heap. A senior engineer would add a `closedSet: mutable.HashSet[Long]` and skip dequeued nodes already in it, bounding redundant expansions. The broadcast variable avoids the anti-pattern of capturing `heuristicMap` directly in the closure, which would cause Spark to Java-serialize the entire map — potentially hundreds of megabytes — on every task submission.

---

### Example 2: GraphX Pregel A* with Kryo-Registered Vertex Attributes

> **What this demonstrates:** How to model A* as a Pregel computation on a distributed GraphX graph, with fully registered Kryo serialization for vertex attributes to minimize per-superstep shuffle volume.

```scala
import org.apache.spark.graphx._
import org.apache.spark.serializer.KryoRegistrator
import com.esotericsoftware.kryo.Kryo

// -- Step 1: Define a compact vertex attribute.
// Every field must be primitive or Kryo-registered to avoid fallback serialization.
// Kryo serializes this as: 1 byte tag + 8+8+8+1 bytes fields = ~26 bytes vs ~340 bytes Java.
case class AStarAttr(
 gScore: Double, // Best known cost from source to this vertex
 fScore: Double, // g + h(v): priority for expansion
 pred: Long, // Predecessor vertex ID (-1L if none)
 active: Boolean // Is this vertex in the "open set"?
)

// -- Step 2: Register all custom classes with Kryo.
// Without this, Kryo falls back to writing the fully-qualified class name
// (~50 extra bytes per object) — negating serialization savings.
class AStarKryoRegistrator extends KryoRegistrator {
 override def registerClasses(kryo: Kryo): Unit = {
 kryo.register(classOf[AStarAttr]) // Assigns integer class ID
 kryo.register(classOf[Array[AStarAttr]])
 // Also register Scala collection types used in messages
 kryo.register(classOf[scala.Tuple2[_, _]])
 }
}

// spark.conf must set these BEFORE SparkContext creation:
// spark.serializer = org.apache.spark.serializer.KryoSerializer
// spark.kryo.registrator = com.example.AStarKryoRegistrator
// spark.kryo.unsafe = true // Enables Unsafe-based field access: 2x faster

// -- Step 3: Build the GraphX graph with AStarAttr on each vertex
val sourceId: VertexId = 1001L
val goalId: VertexId = 9999L

// heuristicBC: Broadcast[Map[Long, Double]] — precomputed h(n) for all vertices
val graph: Graph[AStarAttr, Double] = rawGraph.mapVertices { (vid, _) =>
 val h = heuristicBC.value.getOrElse(vid, 0.0)
 if (vid == sourceId)
 AStarAttr(gScore = 0.0, fScore = h, pred = -1L, active = true)
 else
 // Unvisited vertices: infinite cost, inactive
 AStarAttr(gScore = Double.MaxValue, fScore = Double.MaxValue, pred = -1L, active = false)
}

// -- Step 4: Define the three Pregel functions
// vprog: Called on every vertex that receives a message.
// Merges the incoming best-known gScore with current state.
def vprog(vid: VertexId, attr: AStarAttr, msg: Double): AStarAttr = {
 if (msg < attr.gScore) {
 val h = heuristicBC.value.getOrElse(vid, 0.0)
 // Relaxation: update gScore and reactivate vertex
 attr.copy(gScore = msg, fScore = msg + h, active = true)
 } else attr.copy(active = false) // No improvement: deactivate
}

// sendMsg: Called on every active edge. Propagates relaxed gScore along edge.
// Only active (open-set) vertices send messages — mirrors A* expansion.
def sendMsg(triplet: EdgeTriplet[AStarAttr, Double]): Iterator[(VertexId, Double)] = {
 val srcAttr = triplet.srcAttr
 if (srcAttr.active) {
 val newG = srcAttr.gScore + triplet.attr // triplet.attr = edge weight
 // Only send if this would improve the destination's gScore
 if (newG < triplet.dstAttr.gScore)
 Iterator((triplet.dstId, newG))
 else
 Iterator.empty
 } else Iterator.empty
}

// mergeMsg: When a vertex receives multiple messages in one superstep,
// take the minimum gScore — equivalent to selecting the cheapest incoming path.
def mergeMsg(a: Double, b: Double): Double = math.min(a, b)

// -- Step 5: Run Pregel with a bounded iteration cap
// maxIterations should be set to graph diameter + safety margin.
// Without a cap, non-convergent heuristics cause infinite superstep loops.
val result: Graph[AStarAttr, Double] = Pregel(
 graph,
 initialMsg = Double.MaxValue, // Initial message to all vertices (before superstep 0)
 maxIterations = 50, // Bound: prevent infinite loops on bad heuristics
 activeDirection = EdgeDirection.Out
)(vprog, sendMsg, mergeMsg)

// -- Step 6: Extract the goal vertex's gScore (optimal cost)
val goalAttr = result.vertices.filter(_._1 == goalId).collect().headOption
goalAttr.foreach { case (_, attr) =>
 println(s"Optimal cost to goal: ${attr.gScore}")
}
```

> **Mastery Note:** Each call to `Pregel(...)` internally invokes `graph.aggregateMessages` followed by a `joinVertices`, forming one Spark stage with a shuffle boundary. For a graph with 500 million edges across 1,000 partitions, each superstep generates approximately 500 MB of shuffle write data with Java serialization — Kryo with `spark.kryo.unsafe=true` drops this to ~65 MB, reducing superstep wall-clock time from ~45 seconds to ~8 seconds in practice. The `initialMsg = Double.MaxValue` seed is critical: it triggers `vprog` on all vertices in superstep 0, initializing the graph state uniformly before source relaxation begins. Omitting the `maxIterations` cap on graphs with inadmissible heuristics has caused production jobs to run for 72+ hours before being manually killed.

---

### Example 3: Embarrassingly Parallel Multi-Source A* via `mapPartitions`

> **What this demonstrates:** How to run thousands of independent A* queries in parallel across the cluster — the pattern used in logistics and ride-sharing platforms — where each partition holds a shard of queries and a co-partitioned subgraph.

```python
from pyspark.sql import SparkSession
from pyspark import SparkContext
import heapq
import pickle

spark = SparkSession.builder \
 .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
 .config("spark.executor.memory", "8g") \
 .config("spark.executor.cores", "4") \
 .getOrCreate()

sc: SparkContext = spark.sparkContext

# -- Step 1: Load the graph into a broadcast variable.
# For a city-scale road network (~2M nodes, ~5M edges), the adjacency dict
# is ~400 MB — within the 512 MB broadcast limit (spark.broadcast.blockSize).
# TorrentBroadcast splits this into 4 MB chunks distributed peer-to-peer.
graph_dict = load_graph_as_dict() # {node_id: [(neighbor, weight), ...]}
heuristic_dict = load_heuristic_dict() # {node_id: euclidean_dist_to_goal}

bc_graph = sc.broadcast(graph_dict)
bc_heuristic = sc.broadcast(heuristic_dict)

# -- Step 2: Define partition-local A* — pure Python, no Spark calls inside.
# This function runs entirely within a single executor task.
def local_astar(query_iter):
 """
 Runs A* for each (source, goal) pair in this partition's query batch.
 graph and heuristic are accessed from broadcast — deserialized once per
 executor JVM via Python's pickle protocol via Py4J bridge.
 """
 graph = bc_graph.value # Deserialized once per executor process
 h_map = bc_heuristic.value

 for source, goal in query_iter:
 # Min-heap: (f_score, node_id)
 open_heap = [(h_map.get(source, 0.0), source)]
 g_score = {source: 0.0}
 came_from = {}
 closed = set()

 found_path = None

 while open_heap:
 f, current = heapq.heappop(open_heap)

 if current in closed:
 # Lazy deletion: skip stale heap entries — O(1) check
 continue
 closed.add(current)

 if current == goal:
 # Trace back through came_from to reconstruct path
 path = []
 node = goal
 while node in came_from:
 path.append(node)
 node = came_from[node]
 path.append(source)
 found_path = list(reversed(path))
 break

 for neighbor, weight in graph.get(current, []):
 if neighbor in closed:
 continue
 tentative_g = g_score[current] + weight
 if tentative_g < g_score.get(neighbor, float('inf')):
 g_score[neighbor] = tentative_g
 came_from[neighbor] = current
 f_new = tentative_g + h_map.get(neighbor, 0.0)
 # Push new entry — heap may have stale entries for 'neighbor'
 # which will be filtered by the 'closed' set check above
 heapq.heappush(open_heap, (f_new, neighbor))

 # Yield result tuple for this query — collected by Spark action
 yield (source, goal, g_score.get(goal, float('inf')), found_path)

# -- Step 3: Create RDD of (source, goal) query pairs
queries = [(1001, 9999), (2002, 8888), (3003, 7777)] # In production: millions of pairs
queries_rdd = sc.parallelize(queries, numSlices=200) # 200 partitions → 200 parallel tasks

# -- Step 4: Execute in parallel — each task is an independent A* run
results_rdd = queries_rdd.mapPartitions(local_astar)

# -- Step 5: Collect results or write to Parquet
results_df = spark.createDataFrame(
 results_rdd,
 schema="source LONG, goal LONG, cost DOUBLE, path ARRAY<LONG>"
)
results_df.write.mode("overwrite").parquet("/output/astar_results/")
```

> **Mastery Note:** The broadcast variable is deserialized **once per executor process** (not once per task), because PySpark caches the unpickled value in the Python worker's memory space after the first access. For a 400 MB graph dict, this means 200 tasks sharing the same executor incur only one deserialization cost — roughly 3–5 seconds for pickle, amortized to near-zero per query. The `numSlices=200` setting should match the number of available executor cores to maximize parallelism; over-partitioning (e.g., 10,000 slices for 200 cores) adds task scheduling overhead of ~5–10 ms per task from the `DAGScheduler`, which accumulates to significant wall-clock delay when query paths are short and fast. The pattern fails if the graph does not fit in broadcast (>8 GB) — in that case, use GraphX with the subgraph partitioned to co-locate edges with their source vertices.

---

### Example 4: Memory-Bounded A* with Off-Heap Serialization and Beam Pruning

> **What this demonstrates:** A production-hardened A* variant that caps open-set memory using beam search pruning and stores vertex state in off-heap memory via `sun.misc.Unsafe`, preventing driver OOM on graphs with tens of millions of nodes.

```scala
import java.nio.ByteBuffer
import sun.misc.Unsafe
import scala.collection.mutable

// -- Step 1: Access JVM Unsafe for off-heap allocation.
// Off-heap memory is NOT subject to GC — it lives outside the JVM heap,
// eliminating GC pause contributions from the open set data structure.
// spark.memory.offHeap.enabled=true and spark.memory.offHeap.size must be set.
val unsafe: Unsafe = {
 val f = classOf[Unsafe].getDeclaredField("theUnsafe")
 f.setAccessible(true)
 f.get(null).asInstanceOf[Unsafe]
}

// -- Step 2: Off-heap node record layout (32 bytes per node):
// Offset 0: node_id (Long, 8 bytes)
// Offset 8: g_score (Double, 8 bytes)
// Offset 16: f_score (Double, 8 bytes)
// Offset 24: predecessor (Long, 8 bytes)
val RECORD_SIZE = 32L // bytes per node record

// Allocate off-heap buffer for up to MAX_OPEN_SET_SIZE records.
// This is the BEAM SIZE — the hard cap on open set entries.
val MAX_BEAM_SIZE = 500_000
val offHeapBuffer: Long = unsafe.allocateMemory(MAX_BEAM_SIZE * RECORD_SIZE)

// -- Step 3: Helper functions to read/write node records to off-heap
def writeRecord(slot: Int, nodeId: Long, g: Double, f: Double, pred: Long): Unit = {
 val base = offHeapBuffer + slot.toLong * RECORD_SIZE
 unsafe.putLong(base, nodeId)
 unsafe.putDouble(base + 8, g)
 unsafe.putDouble(base + 16, f)
 unsafe.putLong(base + 24, pred)
}

def readFScore(slot: Int): Double =
 unsafe.getDouble(offHeapBuffer + slot.toLong * RECORD_SIZE + 16)

def readNodeId(slot: Int): Long =
 unsafe.getLong(offHeapBuffer + slot.toLong * RECORD_SIZE)

// -- Step 4: Beam-search A* — bounded open set with eviction of worst-f entries
def beamAstar(
 graph: Map[Long, List[(Long, Double)]],
 start: Long,
 goal: Long,
 heuristic: Long => Double,
 beamWidth: Int = MAX_BEAM_SIZE
): Option[Double] = {

 // On-heap index structures: only node IDs and gScores, minimally sized
 val gScore = mutable.HashMap[Long, Double](beamWidth * 2).withDefaultValue(Double.MaxValue)
 val cameFrom = mutable.HashMap[Long, Long](beamWidth * 2)

 // Min-heap tracks (f_score, slot_index) — stays on heap but is small
 val heap = mutable.PriorityQueue[(Double, Int)]()(Ordering.by(-_._1))

 // Write source into off-heap buffer at slot 0
 val h0 = heuristic(start)
 writeRecord(slot = 0, nodeId = start, g = 0.0, f = h0, pred = -1L)
 gScore(start) = 0.0
 heap.enqueue((h0, 0))
 var nextSlot = 1
 var beamCount = 1 // Track how many records are in the beam

 while (heap.nonEmpty) {
 val (_, slot) = heap.dequeue()
 val currentId = readNodeId(slot)

 if (currentId == goal) {
 // Off-heap memory must be freed explicitly — no GC for off-heap!
 unsafe.freeMemory(offHeapBuffer)
 return Some(gScore(goal))
 }

 graph.getOrElse(currentId, Nil).foreach { case (neighbor, weight) =>
 val tentativeG = gScore(currentId) + weight
 if (tentativeG < gScore(neighbor)) {
 gScore(neighbor) = tentativeG
 cameFrom(neighbor) = currentId
 val fNew = tentativeG + heuristic(neighbor)

 if (beamCount < beamWidth) {
 // Space available: write new record to off-heap buffer
 writeRecord(nextSlot, neighbor, tentativeG, fNew, currentId)
 heap.enqueue((fNew, nextSlot))
 nextSlot += 1
 beamCount += 1
 } else {
 // Beam full: evict the WORST (highest f) entry to make room.
 // This converts A* into a memory-bounded approximation.
 // Optimality is NOT guaranteed when eviction occurs.
 val worstSlot = (0 until beamWidth).maxBy(readFScore)
 writeRecord(worstSlot, neighbor, tentativeG, fNew, currentId)
 // heap already has a stale entry for worstSlot — lazy deletion handles it
 heap.enqueue((fNew, worstSlot))
 }
 }
 }
 }

 unsafe.freeMemory(offHeapBuffer) // Always free — no try/finally here for brevity
 None
}

// -- Step 5: Run with explicit Spark configuration for off-heap
// spark.memory.offHeap.enabled = true
// spark.memory.offHeap.size = 2g // 2 GB off-heap per executor
// spark.driver.extraJavaOptions = -XX:+UseG1GC -XX:MaxGCPauseMillis=100
val cost = beamAstar(adjacencyList, start = 1001L, goal = 9999L, heuristic = vid => heuristicMap(vid))
cost.foreach(c => println(s"Best found cost (beam-approximate): $c"))
```

> **Mastery Note:** `sun.misc.Unsafe.allocateMemory` allocates native memory outside the JVM heap — it is not tracked by `Runtime.getRuntime.totalMemory()` and is invisible to G1GC. This means the open set's memory footprint does not contribute to young-generation GC pressure, eliminating stop-the-world pauses that otherwise delay the driver's `DAGScheduler` thread. However, off-heap memory **must be freed manually** via `unsafe.freeMemory()` — failing to do so causes a native memory leak that persists for the lifetime of the JVM process, growing with each A* invocation until the OS OOM killer terminates the driver. In production, wrap the allocation in a `scala.util.Using` block or a `try/finally` to guarantee deallocation. The beam eviction strategy makes this algorithm an approximation of A*; for routes where path cost approximations within 5–10% are acceptable (e.g., delivery route previews), this trades optimality for a guaranteed O(beamWidth × 32 bytes) = 16 MB memory ceiling regardless of graph size.

---

## 🎯 Mastery Checklist

To achieve true mastery of A* Search in Apache Spark:

- [ ] Understand why Pregel's bulk-synchronous model breaks A*'s strict node-expansion ordering and how this affects convergence iteration count relative to classical A*
- [ ] Know when driver-side A* outperforms Pregel A* (small graphs < 5M nodes, single source-goal queries) and when Pregel is necessary (100M+ node distributed graphs, many concurrent supersteps)
- [ ] Be able to diagnose driver OOM (`Java heap space`) from the Spark UI's Executors tab by correlating driver memory usage with open set growth rate
- [ ] Understand the tradeoff between beam width (memory ceiling) and path optimality loss when using memory-bounded A* with eviction
- [ ] Know how Kryo class registration reduces per-superstep shuffle volume by 6–8x and be able to configure a `KryoRegistrator` with `spark.kryo.unsafe=true` for maximum throughput
- [ ] Understand why `spark.kryo.unsafe=true` requires all registered fields to be non-null and how null field violations produce cryptic `KryoException: Buffer underflow` errors at runtime
- [ ] Be able to diagnose inadmissible heuristics by observing non-converging superstep counts in the Spark UI's Stages tab, where active message counts plateau rather than decrease toward zero
- [ ] Know how `TorrentBroadcast` chunks large graph broadcast variables and why the 8 GB effective limit (`spark.broadcast.blockSize` × max blocks) constrains which graphs can use the multi-source mapPartitions pattern

---

## 📚 Summary

A* Search in Apache Spark is not a single algorithm but a family of architectural patterns, each suited to a different graph scale and query volume. For small graphs (under 5 million nodes), driver-side A* using a Scala `PriorityQueue` is the most efficient choice — it avoids shuffle overhead entirely and runs in seconds. The critical engineering concerns at this scale are heap sizing (`spark.driver.memory`), GC tuning (G1GC with bounded pause targets), and broadcast variable management to prevent closure-capture serialization of large heuristic maps. 

For distributed graphs exceeding driver memory capacity, GraphX Pregel provides a principled adaptation of A* into Spark's parallel execution model. The BSP superstep model sacrifices strict expansion ordering for parallelism, requiring careful heuristic validation (admissibility and consistency) and mandatory Kryo serialization to control per-superstep shuffle volume. The combination of `KryoSerializer`, registered `KryoRegistrator`, and `spark.kryo.unsafe=true` reduces shuffle data volume from ~350 bytes to ~26 bytes per vertex message — a 13x reduction that translates directly to faster supersteps and lower network I/O costs. 

For workloads requiring thousands to millions of simultaneous shortest-path queries (logistics, ride-sharing, network analysis), the `mapPartitions` multi-source pattern turns A* into an embarrassingly parallel operation, scaling linearly with cluster size. Memory-bounded beam search further extends A* to arbitrarily large graphs on the driver by capping heap usage with off-heap Unsafe allocation, trading provable optimality for guaranteed memory safety. The unifying thread across all patterns is serialization discipline, heuristic correctness, and an explicit accounting of where state lives — driver heap, executor heap, off-heap, or shuffle storage — because in production Spark, the location of your data is the first determinant of your performance. 



<br><div style="font-size: 0.85rem; color: #64748b; border-top: 1px solid #334155; padding-top: 10px; margin-top: 20px;"><strong>Source References:</strong> <em>[Ref: 451](spark_book.pdf#page=451) [Ref: 456](spark_book.pdf#page=456) [Ref: 459](spark_book.pdf#page=459) [Ref: 463](spark_book.pdf#page=463) [Ref: 452](spark_book.pdf#page=452) [Ref: 457](spark_book.pdf#page=457) [Ref: 461](spark_book.pdf#page=461) [Ref: 464](spark_book.pdf#page=464) [Ref: 455](spark_book.pdf#page=455) [Ref: 458](spark_book.pdf#page=458) [Ref: 462](spark_book.pdf#page=462) [Ref: 469](spark_book.pdf#page=469)</em></div>
