# MapReduce Shortcomings - Senior Architect Assessment

## Part 1: True/False Questions

**1. True or False:** In MapReduce, the only way to achieve fault tolerance during the shuffle phase is by eagerly materializing the map output to HDFS, whereas Spark leverages lineage to avoid this.
*Correct Answer:* False.
*Mastery Explanation:* MapReduce materializes map outputs to the *local disk* of the TaskTracker/NodeManager, not HDFS. Spark also writes shuffle files to local disk to serve them to reducers; the difference is that Spark avoids HDFS for *intermediate* jobs in an iterative algorithm, not the shuffle itself.

**2. True or False:** A fundamental architectural limitation of MapReduce is its inability to pipeline operations without writing to persistent storage between every Map and Reduce phase.
*Correct Answer:* True.
*Mastery Explanation:* MapReduce forces a strict Map -> Sort/Shuffle -> Reduce boundary. Multiple stages cannot be pipelined in memory (like Catalyst's whole-stage code generation in Spark) without crossing a disk-I/O boundary.

**3. True or False:** The lack of off-heap memory management in MapReduce means that garbage collection (GC) pauses scale linearly with the amount of data processed per JVM container.
*Correct Answer:* True.
*Mastery Explanation:* MapReduce relies on on-heap Java objects (like `Writable`). High object churn leads to massive JVM GC pauses. Modern engines like Spark use Tungsten off-heap memory (UnsafeRow) to bypass the JVM GC entirely.

**4. True or False:** In a Map-Side Join in MapReduce, both datasets must be partitioned using the exact same partitioner and sorted by the join key.
*Correct Answer:* False.
*Mastery Explanation:* What is described is a *Reduce-Side* join optimized with a secondary sort. A Map-Side join in MapReduce requires one dataset to be small enough to be broadcasted via the Distributed Cache to all Mappers, or both datasets to be bucketed/sorted identically (a specialized case, but generally Distributed Cache is the standard Map-Side join). Wait, technically a CompositeInputFormat requires identically sorted/partitioned data for a map-side join. Let's frame it as: False, the standard Map-Side join (Broadcast hash join equivalent) only requires one dataset to fit in memory via Distributed Cache. 

**5. True or False:** JVM startup overhead is a primary reason why MapReduce is unsuitable for interactive micro-batch streaming.
*Correct Answer:* True.
*Mastery Explanation:* MapReduce spins up a new JVM container for each task (unless JVM reuse is enabled, which has its own issues). This startup takes seconds, making sub-second interactive analysis impossible. Spark solves this with long-running executors.

**6. True or False:** MapReduce's inability to handle data skew is primarily due to its lack of a query optimizer to dynamically inject salting.
*Correct Answer:* True.
*Mastery Explanation:* MapReduce is just an API. It has no Catalyst-like optimizer to detect skew and rewrite the execution plan (e.g., AQE skew join optimization). Developers must manually implement salting.

**7. True or False:** Iterative algorithms like PageRank perform poorly in MapReduce solely because of network replication during the shuffle phase.
*Correct Answer:* False.
*Mastery Explanation:* The primary bottleneck is writing the *output* of each iteration to HDFS (which includes 3x network replication and disk I/O), not just the shuffle phase. Spark avoids this by caching datasets in memory across iterations.

**8. True or False:** MapReduce Combiners guarantee a reduction in shuffle data volume across the network.
*Correct Answer:* False.
*Mastery Explanation:* Combiners are executed on a best-effort basis and only work for commutative and associative operations. If there are mostly unique keys, a Combiner will not reduce the shuffle size and just adds CPU overhead.

**9. True or False:** MapReduce forces developers to implement sorting logic manually in the Reducer if they want secondary sorting.
*Correct Answer:* True.
*Mastery Explanation:* Secondary sorting in MapReduce requires custom composite keys, custom partitioners, and custom grouping comparators. It is highly verbose compared to Spark's `repartitionAndSortWithinPartitions`.

**10. True or False:** The Hadoop Distributed Cache is functionally equivalent to Spark's Broadcast Variables, but it relies on HDFS localization rather than a peer-to-peer torrent protocol.
*Correct Answer:* True.
*Mastery Explanation:* Distributed Cache localizes files to the node's disk prior to task execution via HDFS, causing bottlenecks on the NameNode. Spark's Broadcast uses a decentralized Torrent protocol to distribute variables to executors in memory.

## Part 2: Multiple Choice Questions

**11. Why does a Reduce-Side Join in MapReduce often lead to OutOfMemory (OOM) errors in the presence of data skew?**
A) The Mapper must buffer the entire dataset in memory before shuffling.
B) The Reducer receives an iterator of all values for a single key and may need to buffer the "many" side of the join in an ArrayList.
C) The Distributed Cache exceeds the JVM heap limit.
D) The Shuffle phase stores all intermediate data on the NameNode.
*Correct Answer:* B.
*Mastery Explanation:* In a Reduce-side join, the reducer gets `(Key, Iterator<Values>)`. To join two datasets for the same key, you must iterate through and buffer at least one of the datasets in memory (usually the smaller one per key). If a key is skewed, this buffer exceeds the heap size. Other options are architecturally incorrect.

**12. How does Spark's Catalyst Optimizer address the verbosity and rigidity seen in MapReduce code (e.g., manual tag parsing for joins)?**
A) By generating JVM bytecode at runtime for custom Writable classes.
B) By using Whole-Stage Code Generation to collapse a logical plan of relational operators into a single Java function, eliminating manual data parsing.
C) By replacing HDFS with an in-memory file system.
D) By disabling the shuffle phase entirely.
*Correct Answer:* B.
*Mastery Explanation:* Catalyst takes declarative queries (DataFrames/SQL) and compiles them into optimized physical Java bytecode (Whole-Stage CodeGen). This abstracts away the manual tag checking, buffering, and object creation required in MapReduce.

**13. In iterative algorithms like K-Means, MapReduce writes iteration outputs to HDFS. What is the precise architectural cost of this?**
A) 1 disk write, 1 network transfer.
B) 1 disk write, no network transfers (data locality).
C) 1 local disk write, plus network transfers to DataNodes for replication (typically 3x), plus disk writes on those DataNodes.
D) Only RAM allocation on the NameNode.
*Correct Answer:* C.
*Mastery Explanation:* HDFS writes are persistent and fault-tolerant. Writing to HDFS means the client writes to one DataNode, which pipelines to a second, which pipelines to a third. This incurs massive network and disk I/O costs, which Spark avoids by caching in memory (RDD).

**14. Which MapReduce component is directly replaced by Spark's Tungsten memory format to solve GC pauses?**
A) The JobTracker.
B) Java's `Writable` serialization interface and on-heap object creation.
C) The `Partitioner` class.
D) The Distributed Cache.
*Correct Answer:* B.
*Mastery Explanation:* MapReduce creates millions of on-heap `Writable` objects (e.g., `Text`, `IntWritable`), leading to massive GC overhead. Tungsten encodes data into off-heap byte arrays (`UnsafeRow`), completely bypassing the JVM garbage collector.

**15. What architectural feature allows Spark to perform interactive queries (REPL) that MapReduce cannot?**
A) Spark skips the Map phase and only does Reduces.
B) Spark Executor JVMs are long-lived and pre-warmed, and data can be cached in their memory.
C) Spark does not use a Resource Manager (like YARN).
D) Spark writes results to SSDs instead of HDDs.
*Correct Answer:* B.
*Mastery Explanation:* MapReduce provisions new JVM containers for *every* task/job, adding tens of seconds of latency. Spark spins up Executors once per application; they stay alive to execute multiple queries and hold cached data in RAM, enabling sub-second responses.

**16. If a MapReduce job uses a Combiner, what happens if the operation is NOT commutative and associative (e.g., calculating an average)?**
A) The job fails at compile time.
B) The framework automatically disables the Combiner.
C) The final output will be mathematically incorrect because the Combiner executes unpredictably on partial data.
D) The Reducer will re-calculate the average correctly.
*Correct Answer:* C.
*Mastery Explanation:* MapReduce does not guarantee if or how many times a Combiner runs. If the operation isn't commutative/associative (like average), partial averages combined with other partial averages yield the wrong result. Spark handles this elegantly with `aggregateByKey` which separates the intra-partition and inter-partition logic.

**17. What is the MapReduce equivalent of Spark's `repartition(n)`?**
A) Changing the number of Reducers via `job.setNumReduceTasks(n)`.
B) Using a custom `InputFormat`.
C) Modifying the `RecordReader`.
D) It cannot be done.
*Correct Answer:* A.
*Mastery Explanation:* In MapReduce, a shuffle (repartition) is strictly tied to the Reduce phase. You change the number of partitions by setting the number of reduce tasks. There is no standalone "repartition" operator without invoking a full Map-and-Reduce cycle.

**18. Why does MapReduce struggle with complex DAGs (Directed Acyclic Graphs) compared to Spark?**
A) MapReduce can only represent linear chains of Map -> Reduce, requiring intermediate HDFS writes between every stage.
B) YARN does not support DAGs.
C) MapReduce jobs cannot be chained together.
D) Mappers cannot read from Reducers.
*Correct Answer:* A.
*Mastery Explanation:* Spark builds a full DAG of operations, allowing the physical planner (Catalyst) to pipeline operations (e.g., Map -> Filter -> Map) into a single stage. MapReduce forces a rigid Map -> Reduce paradigm; any subsequent operation requires a completely new job, forcing a write to HDFS in between.

**19. How does the Shuffle architecture differ fundamentally between MapReduce and Spark (Sort Shuffle Manager)?**
A) MapReduce pushes data to Reducers, Spark pulls data.
B) MapReduce stores shuffle data in HDFS; Spark stores it in RAM.
C) They are fundamentally similar (Mappers write to local disk, Reducers pull via network), but Spark optimizes the execution plan and serialization (Tungsten).
D) Spark does not use local disks for shuffling.
*Correct Answer:* C.
*Mastery Explanation:* A common misconception is that Spark does in-memory shuffling. Spark's SortShuffleManager, like MapReduce, writes map outputs to *local disk*. The performance gap comes from Tungsten's off-heap sorting, whole-stage codegen, and avoiding HDFS for job chains, not from doing network shuffles entirely in RAM.

**20. In MapReduce, how do you broadcast a 1GB lookup table to all Mappers?**
A) By passing it as a variable in the `Context`.
B) By using the Distributed Cache to copy the file to the local disk of each TaskTracker.
C) By using HDFS symlinks.
D) By wrapping it in a `Writable` object.
*Correct Answer:* B.
*Mastery Explanation:* The Distributed Cache is the Hadoop mechanism for broadcasting files. However, it relies on HDFS and local disk I/O, which is slow and can bottleneck the NameNode, unlike Spark's memory-mapped BitTorrent broadcast.

**21. A developer complains that their MapReduce job is spending 80% of its time in the "Sort" phase even though they only want to do a simple group-by count. Why?**
A) Hadoop automatically sorts all data by value.
B) MapReduce's architecture mandates that all data transferred between Map and Reduce is sorted by key, even if the user only needs a hash-based aggregation.
C) The Combiner is malfunctioning.
D) The `GroupingComparator` is misconfigured.
*Correct Answer:* B.
*Mastery Explanation:* MapReduce forces a sort-merge shuffle. Even for a simple hash aggregation (which doesn't require ordering), MapReduce sorts the keys. Spark's Catalyst optimizer can choose a `HashAggregate` physical plan, bypassing the expensive sort entirely.

**22. Which of the following best describes the "Fat Mapper" anti-pattern in MapReduce?**
A) A Mapper that outputs too many key-value pairs.
B) A Mapper that attempts to load a massive dataset into its JVM heap during `setup()`, causing an OOM.
C) A Mapper that runs for too long.
D) A Mapper that skips the Reduce phase.
*Correct Answer:* B.
*Mastery Explanation:* Developers often try to bypass the shuffle by doing Map-side joins or lookups, loading massive datasets into the Mapper's heap memory. Because MapReduce lacks off-heap memory management, this instantly causes JVM Garbage Collection spirals and OOMs.

**23. MapReduce writes output to HDFS. What is the typical default block size and replication factor?**
A) 64MB / 128MB block size, 3x replication.
B) 1GB block size, 1x replication.
C) 4KB block size, 2x replication.
D) 10MB block size, 5x replication.
*Correct Answer:* A.
*Mastery Explanation:* HDFS typically uses 128MB (or 64MB in older versions) blocks and replicates them 3 times across the cluster to ensure fault tolerance. This makes the inter-job HDFS writes in iterative MapReduce extremely heavy.

**24. Spark's RDD lineage graph provides fault tolerance. How does MapReduce handle the failure of a Reducer node?**
A) It restarts the entire job.
B) It re-runs the failed Reducer task on another node, fetching the materialized Map outputs from the local disks of the Mappers.
C) It reads the RDD lineage.
D) It skips the failed partition.
*Correct Answer:* B.
*Mastery Explanation:* MapReduce is fault-tolerant. Mappers write their output to local disk. If a Reducer fails, the ApplicationMaster schedules a new Reducer on another node, which simply re-pulls the data from the Mappers' local disks.

**25. Why is MapReduce considered an "imperative" API compared to Spark SQL's "declarative" API?**
A) Because it is written in Java.
B) Because you specify *how* to process the data (e.g., step-by-step Map and Reduce functions) rather than *what* result you want, preventing automatic optimization.
C) Because it runs imperatively on the NameNode.
D) Because it requires strict schema definitions.
*Correct Answer:* B.
*Mastery Explanation:* Imperative means defining the exact control flow. Declarative (like SQL) means defining the desired output. Because MapReduce is imperative, it cannot automatically apply rule-based optimizations (like predicate pushdown or join reordering) the way Spark's Catalyst can.

## Part 3: Small Twist Questions

**26. Twist:** You have a MapReduce job performing a Reduce-Side Join. You change the framework to Spark, but you use `rdd.cogroup().map(...)` to manually replicate the exact MapReduce logic. Will this perform significantly better than MapReduce?
A) Yes, because it uses Catalyst.
B) Yes, because Spark does the shuffle entirely in RAM.
C) No, because RDDs do not use the Catalyst optimizer or Tungsten, and manual cogroups suffer from the same JVM serialization and buffering bottlenecks as MapReduce.
D) No, because Spark cannot handle cogroups.
*Correct Answer:* C.
*Mastery Explanation:* If you use the low-level RDD API and mimic MapReduce's imperative cogroup/join logic, you bypass Spark's two main advantages: Catalyst (query optimization) and Tungsten (off-heap memory). It will still create millions of Java objects and suffer GC pauses, performing only marginally better due to cached RDDs (if used).

**27. Twist:** In K-Means clustering, you use MapReduce. To optimize, you write the output of each iteration to the *local filesystem* instead of HDFS. What is the immediate consequence?
A) It becomes as fast as Spark.
B) The next iteration's Mappers cannot find the data because it is not globally distributed or replicated; the job will fail or lose data.
C) YARN automatically replicates local data.
D) The NameNode crashes.
*Correct Answer:* B.
*Mastery Explanation:* HDFS provides a global namespace. If you write to `file:///tmp/`, the next MapReduce job (which runs on different nodes) will not find the output. MapReduce *requires* a distributed file system to chain jobs.

**28. Twist:** You enable JVM Reuse in Hadoop (setting `mapreduce.job.jvm.numtasks = -1`). Does this completely eliminate the latency gap between MapReduce and Spark for interactive queries?
A) Yes, tasks now start instantly.
B) No. While it amortizes JVM startup costs across tasks, the job submission overhead (YARN container allocation, ApplicationMaster negotiation) and HDFS inter-job I/O still cause massive latency.
C) Yes, it enables in-memory caching.
D) No, JVM reuse causes memory leaks that crash the cluster.
*Correct Answer:* B.
*Mastery Explanation:* JVM reuse helps with task startup, but submitting a MapReduce job still requires interacting with YARN's ResourceManager to get an ApplicationMaster, which negotiates containers. This entire orchestration takes seconds, precluding true interactive analytics.

**29. Twist:** You are doing a Map-Side join in MapReduce. You mistakenly put a 50GB file into the Distributed Cache. Your NodeManagers have 64GB of RAM, but the Map containers are allocated 4GB each. What happens?
A) The NameNode crashes.
B) The job succeeds because Distributed Cache uses virtual memory.
C) The Map containers throw `OutOfMemoryError` as soon as they try to load the 50GB file into their 4GB heap.
D) MapReduce automatically switches to a Reduce-Side join.
*Correct Answer:* C.
*Mastery Explanation:* The Distributed Cache copies the 50GB file to the node's disk. But when the Mapper's `setup()` method tries to parse and store that file in memory (a `HashMap`), it vastly exceeds the 4GB JVM heap, causing an OOM. It does not automatically switch strategies.

**30. Twist:** In Spark, you execute `df.groupBy("key").count()`. Catalyst optimizes this using a HashAggregate. If you force Spark to use a SortAggregate, how does the performance profile compare to MapReduce?
A) It becomes identical to MapReduce.
B) It is slower than MapReduce.
C) It is still faster because Tungsten sorts off-heap byte arrays and Catalyst generates custom Java bytecode, bypassing the `Writable` object overhead.
D) Spark crashes because SortAggregate is not supported.
*Correct Answer:* C.
*Mastery Explanation:* Even if forced into a sort-based aggregation (which MapReduce uses natively), Spark is orders of magnitude faster because Tungsten operates on binary data directly off-heap (cache-aware sorting), whereas MapReduce creates and garbage-collects millions of on-heap Java objects.

**31. Twist:** A MapReduce job processes 1TB of data with 10,000 Mappers. You set the number of Reducers to 1. What happens during the Shuffle phase?
A) All 10,000 Mappers stream their data directly into the Reducer's memory, causing an OOM.
B) The Mappers write to local disk, but the single Reducer must pull 1TB of data across the network, leading to massive network congestion and a massive local sort on a single node.
C) MapReduce prevents you from setting Reducers to 1.
D) The Combiner automatically parallelizes the Reducer.
*Correct Answer:* B.
*Mastery Explanation:* A single Reducer becomes a massive bottleneck. It must establish HTTP connections to all 10,000 Mappers to pull the shuffle data, and then it must perform an on-disk merge sort of 1TB of data. This will take hours or days, even if it doesn't OOM immediately.

**32. Twist:** You implement a Combiner in MapReduce to calculate the *maximum* value per key. You notice the shuffle size over the network barely decreased. What is the most likely cause?
A) Calculating max is not commutative.
B) The keys are highly unique (e.g., UUIDs), so the Combiner finds very few records to group together locally.
C) Combiners cannot calculate max.
D) The Reducer is misconfigured.
*Correct Answer:* B.
*Mastery Explanation:* A Combiner only reduces data volume if there are multiple identical keys processed by the *same* Mapper. If the keys are UUIDs (high cardinality), the Combiner does nothing but waste CPU cycles.

**33. Twist:** MapReduce utilizes `SequenceFiles` for intermediate HDFS storage between chained jobs. You switch to `Parquet` format for these intermediate files. What is the architectural impact?
A) The jobs run instantly in memory.
B) You save disk space and I/O due to columnar compression, but you still pay the YARN overhead and HDFS network replication penalties between every job.
C) Parquet cannot be used in MapReduce.
D) The Reducer automatically utilizes Spark Catalyst.
*Correct Answer:* B.
*Mastery Explanation:* Parquet is just a file format. It optimizes the I/O payload, but the MapReduce architecture still dictates that the JVM shuts down, data is written to HDFS (replicated), and a new YARN app is launched for the next step.

**34. Twist:** You configure a MapReduce job to use exactly 0 Reducers (`setNumReduceTasks(0)`). What is the output?
A) The job fails.
B) The Mappers write their output directly to HDFS, bypassing the shuffle and sort phases entirely.
C) The job outputs nothing.
D) The NameNode takes over the reduce phase.
*Correct Answer:* B.
*Mastery Explanation:* Setting reducers to 0 creates a "Map-Only" job. The framework skips the sort/shuffle phase, and Mappers write `OutputFormat` directly to HDFS. This is highly efficient for ETL tasks that don't require aggregation (like filtering or type casting).

**35. Twist:** In Spark, a shuffle partition size is determined by `spark.sql.shuffle.partitions`. In MapReduce, you have 500 Reducers. You notice that 499 Reducers finish in 2 minutes, but 1 Reducer takes 2 hours. What MapReduce limitation are you hitting, and how would Catalyst fix it?
A) Network failure; Catalyst uses UDP.
B) Data Skew. MapReduce relies on manual salting; Spark's Adaptive Query Execution (AQE) dynamically splits skewed partitions at runtime.
C) Disk failure; Catalyst uses RAM.
D) Combiner failure; Catalyst forces combiners.
*Correct Answer:* B.
*Mastery Explanation:* This is textbook data skew. MapReduce's static execution plan means that one Reducer gets stuck with a massive key. Spark 3.x AQE detects this during the shuffle and dynamically splits the skewed partition into smaller sub-partitions.

**36. Twist:** You attempt to cache an RDD in Spark, but it exceeds cluster memory. Spark spills it to disk. Does Spark now perform identically to MapReduce?
A) Yes, disk I/O makes them the same.
B) No. Spark spills to *local* disk (not replicated HDFS), avoids YARN job startup overhead, and still uses Tungsten binary formats.
C) No, Spark crashes immediately when memory is full.
D) Yes, Spark switches to the MapReduce execution engine.
*Correct Answer:* B.
*Mastery Explanation:* Spark spilling to disk is still vastly superior to MapReduce. Spark spills to local disk (no network replication), keeps the JVM executor alive, and uses highly compressed off-heap Tungsten memory formats for the spill files.

**37. Twist:** You implement a custom `WritableComparable` in MapReduce to perform a secondary sort. It takes 150 lines of Java. You replicate this in Spark SQL. How does Spark execute this underneath?
A) Spark compiles your SQL into a MapReduce job.
B) Catalyst generates a physical plan utilizing `SortExec` with a composite sort key, writing optimized Java bytecode via Whole-Stage CodeGen.
C) Spark writes an anonymous inner class in Scala.
D) Spark forces you to use `WritableComparable`.
*Correct Answer:* B.
*Mastery Explanation:* Spark SQL takes declarative SQL/DataFrames and translates them into a physical plan. It uses Whole-Stage CodeGen to write highly optimized, loop-unrolled Java bytecode at runtime, completely eliminating the need for boilerplate `Writable` classes.

**38. Twist:** A MapReduce cluster has nodes with 128 cores and 1TB of RAM. You allocate 100GB to each Mapper container to avoid OOMs. What is the side effect?
A) Lightning fast execution.
B) Massive "Stop-The-World" Garbage Collection pauses, as the JVM struggles to manage a 100GB heap full of millions of small `Writable` objects, effectively freezing the task.
C) The TaskTracker crashes.
D) HDFS automatically caches the data.
*Correct Answer:* B.
*Mastery Explanation:* The JVM garbage collector (especially older ones like CMS or ParallelGC used in Hadoop days) scales poorly with massive heaps filled with small objects. A 100GB heap will experience GC pauses lasting minutes. This is why Spark invented off-heap Tungsten memory.

**39. Twist:** You use MapReduce for a machine learning pipeline. You replace HDFS with Amazon S3. Does this solve the inter-job I/O bottleneck?
A) Yes, S3 is in-memory.
B) No. S3 is an object store. Writing intermediate data to S3 between MapReduce jobs is actually *slower* than HDFS due to network latency and the lack of data locality.
C) Yes, S3 eliminates the shuffle phase.
D) No, S3 does not support MapReduce.
*Correct Answer:* B.
*Mastery Explanation:* Moving from HDFS to S3 for intermediate MR job data exacerbates the problem. S3 has higher latency and completely destroys any data locality (computing where the data lives). Spark's in-memory RDDs are required to solve this.

**40. Twist:** You run a MapReduce job on a dataset that is already perfectly partitioned and sorted by the join key. Does MapReduce automatically optimize the join?
A) Yes, it skips the shuffle.
B) No. Unless you explicitly write a custom `CompositeInputFormat`, MapReduce will ignorantly re-shuffle and re-sort the entire dataset.
C) Yes, it uses Catalyst.
D) No, MapReduce cannot process pre-sorted data.
*Correct Answer:* B.
*Mastery Explanation:* MapReduce has no query optimizer. It doesn't "know" the data is pre-sorted. You must manually orchestrate a Map-side join using specific input formats, otherwise it defaults to a full Reduce-side shuffle. Spark's optimizer reads metadata and automatically avoids the shuffle if data is bucketed/sorted.

## Part 4: Coding & Debugging Questions

**41. Debugging:** Your MapReduce Reducer throws `java.lang.OutOfMemoryError: Java heap space`. The code is:
```java
public void reduce(Text key, Iterable<Text> values, Context context) {
    List<String> cache = new ArrayList<>();
    for(Text t : values) { cache.add(t.toString()); }
    // ... logic ...
}
```
*Identify the architectural flaw:*
*Answer:* The Reducer is fully materializing the `Iterable` into an in-memory `ArrayList`. If a single key has millions of records (skew), it exceeds the heap.
*Mastery Explanation:* MapReduce passes an `Iterable` specifically so you *don't* hold everything in memory. You must stream through the values. In Spark, you avoid this by using `reduceByKey` (which combines locally) or handling skew dynamically via AQE.

**42. Debugging:** A Spark developer converts an old MapReduce script. They write:
```scala
val data = spark.read.text("hdfs://...")
data.map(line => line.split(","))
    .groupByKey() // Direct translation of MR shuffle
    .mapValues(iter => iter.size)
```
*Why is this considered a severe anti-pattern compared to modern Spark, and how does it mimic MR flaws?*
*Answer:* `groupByKey` behaves exactly like a MapReduce shuffle without a Combiner—it shuffles all raw data across the network before aggregating.
*Mastery Explanation:* The developer should use `reduceByKey(_ + _)` or DataFrames (`df.groupBy().count()`), which perform map-side partial aggregations (like MR Combiners, but guaranteed), drastically reducing shuffle I/O.

**43. Architecture Check:** In MapReduce, you want to perform a broadcast join. You write:
```java
protected void setup(Context context) {
    URI[] cacheFiles = context.getCacheFiles();
    // read file into HashMap
}
```
*What happens if the file in the Distributed Cache is larger than the JVM heap? How does Spark's Broadcast variable differ?*
*Answer:* The Mapper OOMs. Spark stores broadcast variables as deserialized Java objects or Tungsten blocks, and shares them across tasks in the *same executor JVM*.
*Mastery Explanation:* MapReduce spins up a JVM *per task*. If a node has 10 map tasks, it loads that HashMap into heap memory 10 times. Spark runs multiple tasks in one Executor JVM, so the Broadcast variable is loaded into memory exactly once per node.

**44. Debugging:** A chained MapReduce workflow runs 5 jobs sequentially: A -> B -> C -> D -> E. Job C consistently fails due to a hardware error on a DataNode.
*How much of the pipeline must be recomputed? How does this differ from Spark?*
*Answer:* Only Job C needs to be rerun. Spark would recompute from the last cached RDD or the source.
*Mastery Explanation:* Because Job B wrote its output to replicated HDFS, Job C can just read from HDFS again. Spark, if memory is lost and not checkpointed, uses the RDD lineage to recompute the lost partitions all the way from the source (or last cache). This is the tradeoff of in-memory computing.

**45. Code Analysis:** 
```java
// MapReduce Mapper
public void map(LongWritable key, Text value, Context context) {
    String[] parts = value.toString().split(",");
    context.write(new Text(parts[0]), new IntWritable(Integer.parseInt(parts[1])));
}
```
*Identify the GC bottleneck in this code and how Spark Tungsten avoids it.*
*Answer:* `new Text()` and `new IntWritable()` are instantiated for *every single record*. A billion records = 2 billion objects for the GC to clean up.
*Mastery Explanation:* Tungsten avoids object instantiation entirely. It reads the CSV string, converts it directly to UTF-8 bytes, and writes the integer into an 8-byte slot in an off-heap `UnsafeRow`. No Java objects are created per row.

**46. Debugging:** You notice your MapReduce job is taking 5 minutes, but the actual data processing takes 10 seconds. The logs show: `INFO mapreduce.Job:  map 0% reduce 0%` for 4 minutes.
*What is the system doing?*
*Answer:* YARN container allocation and JVM startup.
*Mastery Explanation:* MapReduce has severe cold-start latency. The ApplicationMaster must negotiate resources with YARN, NodeManagers must pull dependencies, and JVMs must boot. Spark mitigates this by starting Executors once per application.

**47. Architecture Check:** You write a Spark application using `DataFrame` API, and you execute `df.filter(...).join(...)`. You do not write any `map` or `reduce` functions.
*Why is this fundamentally impossible in MapReduce?*
*Answer:* MapReduce lacks an execution engine capable of translating logical operators into physical execution steps.
*Mastery Explanation:* MapReduce is just a framework for executing `map()` and `reduce()` methods. It has no AST (Abstract Syntax Tree), no rule-based optimizer, and no physical planner. You *must* write the imperative code. Spark Catalyst provides the database-like intelligence.

**48. Code Analysis:**
```python
# Iterative MapReduce (Pseudo)
for i in range(10):
    os.system("hadoop jar myapp.jar KMeans iter" + str(i))
```
*Why does this script put immense pressure on the Hadoop NameNode?*
*Answer:* Submitting a job requires the NameNode to manage the creation, replication, and tracking of new HDFS files for job JARs, Distributed Cache, and intermediate outputs for every iteration.
*Mastery Explanation:* The NameNode manages HDFS metadata in RAM. 10 iterations mean 10x the files, block reports, and job setup calls. Spark's DAG scheduler handles iterations internally without hitting the NameNode for intermediate storage.

**49. Debugging:** You have a MapReduce job with 1,000 Mappers. You do not define a Reducer. The job finishes, but produces 1,000 tiny files (1KB each) in HDFS.
*What is the problem, and how does Spark handle this better?*
*Answer:* The "Small Files Problem". Map-only jobs output one file per Mapper.
*Mastery Explanation:* HDFS block size is 128MB; storing 1,000 1KB files chokes the NameNode's RAM. In MapReduce, you'd need a second job (Identity Reducer) to merge them. In Spark, you simply append `.coalesce(10)` before writing, which avoids a full shuffle but merges the partitions.

**50. Architecture Check:** MapReduce uses a "pull" based shuffle (Reducers pull from Mappers via HTTP). Early Spark used a "hash" shuffle, which created $M \times R$ files, and later switched to Sort Shuffle.
*Why did Spark adopt MapReduce's sort-based shuffle architecture?*
*Answer:* To avoid creating too many open file handlers and exhausting disk IOPS.
*Mastery Explanation:* If you have 10,000 mappers and 10,000 reducers, a hash shuffle creates 100,000,000 files simultaneously. Sort Shuffle (like MapReduce) creates only 1 data file and 1 index file per Mapper, vastly improving disk I/O efficiency, proving that MapReduce's *shuffle architecture* was sound, even if its JVM and HDFS reliance was flawed.
