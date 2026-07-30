# Master Class: Tungsten Performance Assessment

## Section 1: True/False Questions

**1. Whole-Stage Code Generation (WSCG) eliminates the Volcano iterator model entirely from all Spark operators.**
**Answer:** False. 
**Mastery Explanation:** While WSCG fuses many operators (like Filter, Project, and certain Joins) into a single function to avoid the virtual method call overhead of the Volcano model, it does not eliminate it entirely. Blocking operators (like Sort) or unsupported operations (like certain opaque UDFs or complex fallbacks) still rely on the iterator model to pass data across WSCG boundaries.

**2. Tungsten's UnsafeRow format stores strings as standard `java.lang.String` objects to maintain compatibility.**
**Answer:** False. 
**Mastery Explanation:** Tungsten stores strings in a binary format (UTF8String), completely bypassing `java.lang.String` object instantiation. This eliminates JVM object header overhead and avoids placing pressure on the Garbage Collector.

**3. When using off-heap memory, Tungsten allocations completely bypass JVM Garbage Collection.**
**Answer:** True. 
**Mastery Explanation:** Off-heap memory is allocated via `Unsafe` directly from the OS (similar to C's `malloc`). Because these memory pages reside outside the JVM heap, the Garbage Collector is unaware of them, virtually eliminating GC pauses for large dataset manipulations like aggregations.

**4. Tungsten uses Janino to compile physical plans into native C++ code.**
**Answer:** False. 
**Mastery Explanation:** Tungsten uses the Janino compiler to compile the generated Java source code into JVM bytecode on the fly, not native C++ code. The bytecode is then executed by the JVM (and JIT compiled to machine code by the JVM).

**5. SIMD instructions can be leveraged by Tungsten when reading Parquet files in batches.**
**Answer:** True. 
**Mastery Explanation:** Vectorized Parquet reading loads columnar data in batches directly into Tungsten's `ColumnarBatch` structures. This contiguous columnar layout allows modern CPUs to use SIMD (Single Instruction, Multiple Data) to decode and process multiple primitive values simultaneously.

**6. Tungsten’s memory page pointers are encoded as 128-bit integers.**
**Answer:** False. 
**Mastery Explanation:** Tungsten encodes pointers as 64-bit integers. These 64 bits consist of a page number and an offset within that page. This compact representation is highly cache-friendly and extremely fast to sort.

**7. Encoders translate standard JVM objects into Tungsten's binary UnsafeRow format.**
**Answer:** True. 
**Mastery Explanation:** `ExpressionEncoder` is the bridge between JVM objects (like Scala case classes) and Catalyst's internal binary `UnsafeRow` representation, allowing Tungsten to operate directly on the raw bytes.

**8. Whole-Stage Code Generation guarantees that Spark will never spill to disk during a Hash Aggregate.**
**Answer:** False. 
**Mastery Explanation:** WSCG optimizes CPU instruction throughput and pipeline utilization, but it does not magically increase available memory. If the high-cardinality hash map exceeds the allocated Tungsten memory (on-heap or off-heap), Spark will still spill to disk.

**9. In a physical plan, WSCG boundaries are marked by an asterisk followed by an ID, like `*(1)`.**
**Answer:** True. 
**Mastery Explanation:** The `*(id)` notation in the `explain()` output signifies that the operator is participating in Whole-Stage Code Generation. Operators sharing the same ID are fused into a single generated Java function (a single `while` loop).

**10. Tungsten stores complex types like Maps by immediately deserializing them even if the query only selects primitive fields.**
**Answer:** False. 
**Mastery Explanation:** Tungsten operates on lazy deserialization. If a query only filters on a primitive field like `id`, Tungsten directly offsets into the binary `UnsafeRow` to read the integer/long without ever deserializing the complex `Map` or `Array` fields, saving massive CPU cycles.

---

## Section 2: Multiple Choice Questions

**11. What is the primary purpose of Tungsten's UnsafeRow format?**
A) Cross-language support between Python and JVM
B) Minimizing JVM object overhead and GC pressure
C) Maximizing disk compression
D) Serializing RDDs across the network
**Answer:** B.
**Mastery Explanation:** UnsafeRow's flat binary format eliminates object headers and references, drastically reducing memory footprint and hiding data from the Garbage Collector, which is Tungsten's core objective.

**12. Which Java compiler does WSCG use to compile fused queries on the fly?**
A) javac
B) Eclipse JDT
C) Janino
D) GraalVM
**Answer:** C.
**Mastery Explanation:** Spark leverages Janino, a super-fast, lightweight embedded Java compiler, to compile the synthesized Java code into JVM bytecode at runtime during query execution.

**13. How does Tungsten identify memory locations when performing highly-optimized sorts?**
A) Object references
B) 64-bit pointers (page + offset)
C) Hash codes
D) JVM Heap Addresses
**Answer:** B.
**Mastery Explanation:** Tungsten represents memory locations using 64-bit primitives that encode both the memory page and the offset. Sorting an array of these primitive 64-bit integers is incredibly cache-friendly and fast compared to sorting object references.

**14. What Spark configuration enables off-heap memory for Tungsten execution?**
A) spark.sql.offheap
B) spark.memory.offHeap.enabled
C) spark.executor.offHeap
D) spark.tungsten.offHeap
**Answer:** B.
**Mastery Explanation:** `spark.memory.offHeap.enabled=true` enables it, but it must be paired with `spark.memory.offHeap.size` to actually allocate the OS-level memory pages.

**15. What does the 'Batched: true' flag in a physical plan's FileScan node indicate?**
A) Data is processed via Spark Streaming micro-batches
B) Vectorized Parquet/ORC reading is active
C) WSCG is enabled for the scan
D) Adaptive Query Execution is batching tasks
**Answer:** B.
**Mastery Explanation:** 'Batched: true' means the scan operator is reading columns of data in chunks (typically 4096 rows) into `ColumnarBatch` memory structures, leveraging SIMD rather than processing row-by-row.

**16. Which of these operations heavily benefits from Tungsten's off-heap Hash Aggregate?**
A) High-cardinality group by
B) Selecting the top 10 rows
C) Filter by primary key
D) `df.count()`
**Answer:** A.
**Mastery Explanation:** High-cardinality aggregations create massive hash maps. Storing these on-heap creates millions of map entry objects, thrashing the GC. Off-heap memory stores them as contiguous bytes, completely evading the GC.

**17. Why does WSCG dramatically improve CPU pipeline utilization?**
A) It spawns multiple threads per task
B) It eliminates virtual function calls per row
C) It bypasses the L1 cache
D) It pre-computes aggregate metrics
**Answer:** B.
**Mastery Explanation:** In the Volcano model, each row triggers multiple virtual method calls (`next()`) up the operator tree. This breaks instruction locality. WSCG fuses operators into a single tight loop, eliminating virtual dispatches and allowing the CPU's branch predictor and instruction pipeline to run at full speed.

**18. What is the typical default batch size for Tungsten vectorized Parquet reading?**
A) 1 row
B) 4096 rows
C) 1 million rows
D) The entire file
**Answer:** B.
**Mastery Explanation:** Spark processes columnar batches in chunks of 4096. This size is large enough to benefit from SIMD instructions but small enough to fit neatly into L1/L2 CPU caches, ensuring cache locality.

**19. If an `Encoders.product` case class is used, how are unused fields handled during a filter operation?**
A) Deserialized into JVM but ignored
B) Kept in binary format and never deserialized
C) Pushed to the L3 cache
D) Nullified by Catalyst
**Answer:** B.
**Mastery Explanation:** Tungsten directly accesses the required field's offset in the binary byte array. Unused fields remain untouched as raw bytes, skipping the expensive deserialization process entirely.

**20. What happens if `spark.memory.offHeap.enabled=true` but `spark.memory.offHeap.size` is not set?**
A) Spark uses a default of 1GB
B) Spark throws an exception on startup
C) It silently falls back to on-heap memory
D) It uses 50% of available RAM
**Answer:** B.
**Mastery Explanation:** Spark strictly requires the user to explicitly define the off-heap size when it is enabled. Failure to do so throws an `IllegalArgumentException`.

**21. What is the primary benefit of contiguous memory layouts in Tungsten's UnsafeRow format?**
A) Hardware prefetchers can load data into CPU caches efficiently
B) Allows for seamless Python UDF execution
C) Compresses data up to 10x
D) Eliminates network latency during shuffles
**Answer:** A.
**Mastery Explanation:** Modern CPUs use hardware prefetchers that look for sequential memory access patterns. Contiguous binary formats allow the CPU to predict and load upcoming data into the L1/L2 cache before the instruction needs it, preventing CPU stalls.

**22. Which data structure does Tungsten use to represent vectorized columnar data in memory?**
A) InternalRow
B) ColumnarBatch
C) RDD
D) DataFrame
**Answer:** B.
**Mastery Explanation:** While `InternalRow`/`UnsafeRow` represents row-based binary data, `ColumnarBatch` represents columnar memory chunks (arrays of primitives), which is what the vectorized Parquet reader outputs.

**23. How does Tungsten avoid object allocation during a simple `filter(id > 1)` where `id` is a Long?**
A) It reads the ID as a primitive directly from the UnsafeRow byte array
B) It caches the JVM object
C) It uses Object Pooling
D) It serializes the lambda function
**Answer:** A.
**Mastery Explanation:** The generated Java code will use `sun.misc.Unsafe` to read a 64-bit primitive `long` directly from a calculated memory offset in the byte array. No `Long` or `Row` objects are ever allocated inside the loop.

**24. What does Tungsten's page-based memory manager resemble?**
A) A JVM Heap
B) A miniature Operating System memory manager
C) A Hadoop HDFS cluster
D) A B-Tree index
**Answer:** B.
**Mastery Explanation:** Tungsten implements its own memory page allocator, allocating raw blocks of memory (pages) and managing pointers, much like an OS kernel manages physical memory for user-space programs.

**25. What happens when you execute `df.debugCodegen()`?**
A) Spark prints the logical execution plan
B) Spark prints the actual Janino-generated Java source code
C) Spark analyzes and debugs memory leaks
D) Spark compiles Python code into Java
**Answer:** B.
**Mastery Explanation:** `debugCodegen()` is an internal utility that prints out the raw Java classes synthesized by WSCG. It is the ultimate tool for verifying that operators have been fused into tight `while` loops.

---

## Section 3: Small Twist Questions

**26. You have `spark.memory.offHeap.enabled=true` but apply a standard PySpark UDF in your `map`. Does Tungsten still avoid object allocation?**
**Answer:** No. 
**Mastery Explanation:** Standard PySpark UDFs force Spark to deserialize the binary Tungsten row, serialize it via Pickle, send it to a Python worker process, and then deserialize the result back. This breaks the Tungsten execution pipeline and causes massive overhead.

**27. You read a Parquet file with `spark.sql.parquet.enableVectorizedReader=true`, but the schema contains deeply nested Array and Struct types. Is it vectorized?**
**Answer:** No (or partially, depending on Spark version).
**Mastery Explanation:** Historically, the vectorized Parquet reader falls back to row-by-row reading when encountering highly complex nested types, disabling SIMD optimizations. You must check the physical plan for `Batched: true`.

**28. You have WSCG enabled. You perform a `HashAggregate`, but you add a Custom Scala UDF (extending `UserDefinedFunction`) in the aggregation expression. Does the entire stage still fuse?**
**Answer:** No.
**Mastery Explanation:** Opaque UDFs are black boxes to Catalyst and WSCG. The code generator cannot synthesize the logic inside a compiled Scala function, causing a WSCG boundary break. (Native Catalyst expressions, however, fuse perfectly).

**29. You set `spark.memory.offHeap.size=4g` but leave `spark.memory.offHeap.enabled=false`. Does Spark use off-heap memory for Tungsten?**
**Answer:** No.
**Mastery Explanation:** The `size` configuration is ignored unless `enabled` is explicitly set to `true`. Tungsten will fall back to using on-heap large arrays for its page manager.

**30. You perform `df.filter`. You then add an `orderBy`. Does the filter process in the exact same WSCG `while` loop as the output of the sort?**
**Answer:** No.
**Mastery Explanation:** `orderBy` triggers an Exchange (shuffle) and a Sort, both of which are blocking operators. The pipeline is broken: one WSCG loop will filter and feed the shuffle write, and a completely separate WSCG loop will read the sorted data post-shuffle.

**31. You read JSON data instead of Parquet. Does Tungsten use the vectorized reader?**
**Answer:** No.
**Mastery Explanation:** The vectorized reader requires columnar storage formats like Parquet or ORC. Row-based text formats like JSON or CSV are inherently parsed row-by-row and cannot be read via columnar batching.

**32. You are doing a join. One side is broadcasted. Does WSCG fuse the `BroadcastHashJoin`? If changed to `SortMergeJoin`, is it fused exactly the same?**
**Answer:** BroadcastHashJoin fuses seamlessly. SortMergeJoin does not fuse identically.
**Mastery Explanation:** `BroadcastHashJoin` operates entirely in-memory and fuses nicely into a single loop. `SortMergeJoin` requires sorted inputs and an iterator-based fallback for the merge phase, breaking WSCG boundaries into separate sub-pipelines.

**33. You use `ExpressionEncoder` on a case class containing a `java.sql.Timestamp`. Does Tungsten store it as an object?**
**Answer:** No.
**Mastery Explanation:** Tungsten intelligently maps `java.sql.Timestamp` to a primitive `long` representing microseconds since the epoch. It remains a primitive in memory, avoiding GC.

**34. You perform a `groupBy` that easily fits within 100MB of on-heap memory. You switch to off-heap memory. Will your query speed up significantly due to GC elimination?**
**Answer:** No.
**Mastery Explanation:** If the dataset is small enough that no major GC collections are triggered, the performance difference between on-heap Tungsten (large byte arrays) and off-heap Tungsten (direct Unsafe memory) is negligible. Off-heap shines under memory pressure.

**35. You view the physical plan and see `*(1) Filter` and `*(2) HashAggregate`. Are they fused into a single Java method?**
**Answer:** No.
**Mastery Explanation:** The different ID numbers (`1` vs `2`) indicate they are in different WSCG domains. They will be compiled into separate Java classes and use an iterator to pass data between the domains.

**36. You configure `spark.sql.inMemoryColumnarStorage.batchSize` to 1. How does this affect vectorized reading?**
**Answer:** Performance plummets.
**Mastery Explanation:** Setting batch size to 1 defeats the purpose of columnar reading. It destroys SIMD applicability and ruins CPU cache locality, essentially reverting back to row-by-row overhead.

**37. You use `Encoders.kryo[MyClass]`. Does Tungsten process its fields directly in binary?**
**Answer:** No.
**Mastery Explanation:** Kryo serialization treats the entire object as an opaque binary blob within a single `UnsafeRow` column. Tungsten cannot offset into this blob to extract fields; it must fully deserialize the Kryo blob back into `MyClass` to evaluate filters or projections.

**38. A Tungsten sort runs out of memory and spills to disk. Does it spill JVM objects or binary data?**
**Answer:** Raw binary data.
**Mastery Explanation:** Tungsten spills its raw memory pages and the 64-bit pointers directly to disk. This is extremely fast because it requires zero object serialization/deserialization overhead during the spill.

**39. You use `df.cache()`. Does Tungsten store the cached data as JVM objects in memory?**
**Answer:** No.
**Mastery Explanation:** Spark's in-memory cache uses columnar formatting. Tungsten stores the cached DataFrame in `ColumnarBatch` format, compressing data and keeping it off the GC's radar even when stored on-heap.

**40. You run a query on 1 million rows and inspect `debugCodegen`. You run the identical query on 10 billion rows. Does Janino recompile the code?**
**Answer:** No.
**Mastery Explanation:** WSCG generates and compiles the code based strictly on the *query plan structure*, not the data size. The compiled bytecode loop simply executes 10 billion times instead of 1 million times.

---

## Section 4: Coding & Debugging Questions

**41. A developer complains their high-cardinality aggregation is causing OOM on the driver. They added `spark.memory.offHeap.size=10g`. Why is it still failing?**
**Answer:** The configuration applies to the executors.
**Mastery Explanation:** Aggregations happen on executors. If the driver is getting an OOM, the developer likely called `.collect()` or `.toPandas()`, pulling millions of aggregated rows back to the driver JVM. Driver memory must be increased via `spark.driver.memory`, off-heap executor settings won't help.

**42. Looking at `explain()`, a user sees `SortMergeJoin` without a WSCG asterisk next to it, but the preceding `Filter` has `*(1)`. What is the WSCG blocker?**
**Answer:** SortMergeJoin relies on iterator fallbacks.
**Mastery Explanation:** The merge phase of a SMJ is highly complex and relies on streaming iterators from both sorted sides. It cannot be easily flattened into a single stateless `while` loop, thus WSCG falls back to the Volcano model for the join itself.

**43. A user writes a UDF: `udf((s: String) => s.toUpperCase)`. They notice severe CPU cache misses. How to fix using Tungsten?**
**Answer:** Replace the UDF with native functions: `functions.upper(col("s"))`.
**Mastery Explanation:** Scala UDFs force Tungsten to deserialize the UTF8String into a `java.lang.String`, apply the function, and reserialize. Native Catalyst expressions translate directly to inline Janino Java code manipulating raw bytes, retaining WSCG speed.

**44. A Spark job fails on startup with `java.lang.IllegalArgumentException: requirement failed: spark.memory.offHeap.size must be > 0`. What did they do?**
**Answer:** Enabled off-heap without allocating size.
**Mastery Explanation:** The user set `spark.memory.offHeap.enabled=true` but forgot to define `spark.memory.offHeap.size`. Spark requires explicitly reserving off-heap OS memory.

**45. A PySpark user reads Parquet and applies a Pandas UDF. The plan shows `ArrowEvalPython`. Is Tungsten bypassed?**
**Answer:** Tungsten is used to load data, but execution leaves the JVM.
**Mastery Explanation:** Apache Arrow enables zero-copy transfer of columnar data. Tungsten's `ColumnarBatch` is converted efficiently to Arrow format and shipped to a Python worker. While Tungsten optimizes the read, the actual logic executes in Python, completely outside JVM/Janino space.

**46. A user uses `rdd.map(row => row.getAs[Long]("id") * 2).toDF()`. Why is WSCG not generating a tight loop for the multiplication?**
**Answer:** RDDs bypass Catalyst and Tungsten.
**Mastery Explanation:** The RDD API operates on raw Java objects and is invisible to Catalyst. To leverage WSCG, they must use the DataFrame API: `df.withColumn("id", col("id") * 2)`.

**47. In `debugCodegen`, a user sees `/* MISSING WSCG FOR: ... */` deep in a complex query. Why?**
**Answer:** The generated code hit the JVM 64KB bytecode limit.
**Mastery Explanation:** Janino compiles code into Java methods. The JVM has a hard limit of 64KB bytecode per method. If a query is massively complex (e.g., projecting hundreds of columns), Spark detects the code will be too large and automatically disables WSCG for that segment, falling back to iterators.

**48. A user allocates `spark.executor.memory=4g` and `spark.memory.offHeap.size=4g` on a YARN instance with an 8GB hard limit. The container is immediately killed by the OOM killer. Why?**
**Answer:** Total memory exceeds the OS container limit.
**Mastery Explanation:** Total container memory = `executor.memory` + `offHeap.size` + `memoryOverhead`. 4GB + 4GB + Overhead > 8GB. The OS-level Out Of Memory killer terminates the container for exceeding its cgroup limit.

**49. A user implements a custom `ExpressionEncoder` but their query fails with `ClassCastException: java.lang.String cannot be cast to org.apache.spark.unsafe.types.UTF8String` inside Janino code. What is the root cause?**
**Answer:** Mismatch in expected Catalyst internal types.
**Mastery Explanation:** The custom encoder placed a native `String` object into the `InternalRow`. Tungsten's generated code strictly expects the binary `UTF8String` representation for strings and casts blindly, causing a runtime crash.

**50. A query reading a JSON file has `Batched: false` in the `FileScan` node. The user tries to force it with `spark.sql.parquet.enableVectorizedReader=true`. Why does it not change?**
**Answer:** Vectorized reading requires columnar storage.
**Mastery Explanation:** JSON is a row-oriented format. The `enableVectorizedReader` setting strictly applies to Parquet (and similarly for ORC). Spark physically cannot batch-read columnar arrays from a sequential text file.
