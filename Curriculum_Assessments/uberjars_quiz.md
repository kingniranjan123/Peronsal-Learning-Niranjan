# Elite Spark Assessment: Uberjars, Classloading, and JVM Memory

## Part 1: True/False Questions (10 Questions)

**1. Uberjar classes are loaded into the driver's heap memory before being distributed to executors via the BlockManager.**
- **Answer:** False
- **Mastery Explanation:** Classes are loaded into the JVM's native memory region called Metaspace (specifically as `Klass` structures), not the heap. Only the physical JAR file is distributed via the HTTP server or HDFS, and executors load it into their own isolated Metaspace.

**2. Spark's `MutableURLClassLoader` on the driver uses a child-first delegation model by default, ensuring your application dependencies always override Spark's bundled libraries.**
- **Answer:** False
- **Mastery Explanation:** The default delegation model is parent-first. Spark's bundled libraries (e.g., Guava, Jackson) take precedence, which is the primary root cause of "Jar Hell" version conflicts.

**3. Setting `spark.files.useFetchCache=true` enables executors on the same Kubernetes node to share a single downloaded copy of the uberjar.**
- **Answer:** True
- **Mastery Explanation:** This deduplicates downloads at the node level, drastically reducing HDFS read load and scale-out latency during dynamic allocation events.

**4. Discarding duplicate `META-INF/services/*` files using `MergeStrategy.discard` is a safe way to shrink your uberjar because Spark only needs the first service definition.**
- **Answer:** False
- **Mastery Explanation:** Discarding these files breaks Java SPI (Service Provider Interface) discovery. Frameworks like Jackson and Avro will silently fail to discover their modules, resulting in runtime serialization errors. They must be concatenated using `MergeStrategy.concat`.

**5. Without explicit configuration, Kryo's internal `Class.forName()` resolution will successfully find domain classes packaged in an uberjar because it uses the `ExecutorClassLoader`.**
- **Answer:** False
- **Mastery Explanation:** Kryo's default deserializer uses the thread's context classloader, which on executors is the system classloader, not the `ExecutorClassLoader`. This causes `ClassNotFoundException` unless a custom registrator explicitly registers classes using class literals.

**6. An `OOMKilled` (exit code 137) event on a Kubernetes executor pod before tasks start processing is almost always due to Metaspace inflation exceeding the pod's memory overhead budget.**
- **Answer:** True
- **Mastery Explanation:** Loading a massive uberjar creates thousands of `Klass` structs in Metaspace. If `spark.executor.memoryOverhead` is not sized to accommodate this native memory spike, the container's total RSS exceeds the Kubernetes limit, triggering the OOM killer before a JVM `OutOfMemoryError` can be thrown.

**7. When applying `ShadeRule.rename()` in sbt-assembly, you must manually update your application's source code imports to use the new `shaded.*` package name.**
- **Answer:** False
- **Mastery Explanation:** The shade plugin rewrites the compiled bytecode (.class files) automatically during the assembly phase. Your source code remains unchanged.

**8. Whole-Stage CodeGen dynamically generates new Java bytecode which is stored in Tungsten's off-heap memory.**
- **Answer:** False
- **Mastery Explanation:** The Janino compiler generates JVM bytecode for the query plans, which is loaded as standard Java classes into the executor's Metaspace, directly competing with the uberjar's classes for native memory limits.

**9. Relying on `MergeStrategy.deduplicate` (the default) is the safest approach for handling class file conflicts between dependencies because it guarantees the newest version is used.**
- **Answer:** False
- **Mastery Explanation:** It does not pick the newest version; it throws a build error if the file contents differ. If they differ, you must explicitly handle them via shading or exclusion. If a custom strategy silently picks the first one encountered, it introduces non-determinism.

**10. To definitively prove which JAR provided a specific class at runtime (e.g., diagnosing a Guava conflict), printing the `java.class.path` property on the executor is the best method.**
- **Answer:** False
- **Mastery Explanation:** `java.class.path` only shows the system classpath. In Spark, the definitive way is to call `Class.forName(name).getProtectionDomain.getCodeSource.getLocation`, which queries the JVM's actual classloader mapping for that loaded class.

## Part 2: Multiple Choice Questions (15 Questions)

**11. Why does an uberjar deployed to a Kubernetes cluster often require an explicit increase to `spark.executor.memoryOverheadFactor`?**
- A) To provide more heap space for broadcast variables.
- B) To allocate off-heap memory for Tungsten execution.
- C) To account for the Metaspace consumed by loading thousands of `Klass` structs.
- D) To buffer Kryo serialization streams for shuffle writes.
- **Answer:** C
- **Mastery Explanation:** The memory overhead budget accounts for non-heap native memory. An uberjar with 30,000 classes can consume 200-400MB of Metaspace on classloading, easily exceeding the default 10% overhead and triggering a silent `OOMKilled` (exit 137).

**12. When two dependencies in your sbt project pull in different versions of `com.google.common.collect.ImmutableMap`, and you do not shade Guava, which version does Spark use at runtime on the cluster?**
- A) The version specified in your uberjar, due to `ExecutorClassLoader` semantics.
- B) The version bundled internally with Spark.
- C) The newest version, based on semantic versioning.
- D) It throws a `ClassNotFoundException` at startup.
- **Answer:** B
- **Mastery Explanation:** Because Spark's `MutableURLClassLoader` uses parent-first delegation, the system classloader loads Spark's bundled Guava (e.g., v14) before scanning your uberjar. This leads to `NoSuchMethodError` if your code expects a newer Guava API (e.g., v31).

**13. A Spark query fails with `NoSuchMethodError` during Whole-Stage CodeGen execution, but unit tests pass perfectly. What is the most likely architectural cause?**
- A) Unit tests use Tungsten, but the cluster does not.
- B) Janino code generation on the cluster runs in a different classloader that resolves to Spark's internal dependency versions rather than the uberjar's versions.
- C) The uberjar was built with `assemblyOption in assembly := copy(includeScala = true)`.
- D) Kryo fallback serialization corrupted the method table.
- **Answer:** B
- **Mastery Explanation:** Unit tests run on a flat classpath where your dependencies take precedence. On the cluster, Janino compiles code dynamically against the executor's parent-first classloader hierarchy, binding to Spark's version of conflicting libraries (like Guava or Jackson) at runtime.

**14. What is the catastrophic performance impact of forgetting to set `spark.kryo.registrationRequired=true` when using a shaded Kryo in an uberjar?**
- A) Spark crashes immediately with an `IllegalArgumentException`.
- B) The BlockManager refuses to distribute the JAR.
- C) Kryo silently falls back to Java serialization, expanding shuffle payloads by 3-5x and crushing performance.
- D) Whole-Stage CodeGen is disabled, falling back to Volcano iteration.
- **Answer:** C
- **Mastery Explanation:** Without this strict flag, unregistered classes (often due to classloader context mismatches with shaded namespaces) fall back to writing full class name strings for every object, destroying serialization efficiency and multiplying network/disk I/O during shuffles.

**15. You are applying a ShadeRule in sbt-assembly to relocate Protobuf: `ShadeRule.rename("com.google.protobuf.**" -> "shaded.com.google.protobuf.@1")`. Why MUST you append `.inAll` to this rule?**
- A) To ensure the rule is applied across all nodes in the cluster.
- B) To rewrite bytecode references inside transitive dependencies that also call Protobuf, not just your direct code.
- C) To copy the original Protobuf classes alongside the shaded ones for backward compatibility.
- D) To instruct the Janino compiler to recognize the shaded namespace during CodeGen.
- **Answer:** B
- **Mastery Explanation:** If you omit `.inAll`, only your direct project's classes are shaded. Transitive dependencies (e.g., a proprietary gRPC client) will still contain bytecode calling `com.google.protobuf`, which will link against Spark's conflicting Protobuf version at runtime.

**16. How does setting `spark.executor.extraJavaOptions="-XX:+ClassUnloading -XX:+ClassUnloadingWithConcurrentMark"` help stabilize long-running Spark structured streaming applications?**
- A) It removes unused DataFrames from the heap.
- B) It allows the JVM to reclaim Metaspace occupied by dynamically generated Janino classes for queries that are no longer running.
- C) It evicts old shuffle blocks from Tungsten off-heap memory.
- D) It forces the driver to garbage collect broadcast variables.
- **Answer:** B
- **Mastery Explanation:** In streaming or long-running sessions, Whole-Stage CodeGen continuously generates new classes. Without class unloading enabled for the garbage collector (especially CMS or G1GC), Metaspace grows monotonically until an OOM crash occurs.

**17. What is the specific role of the `MutableURLClassLoader` in the Spark driver?**
- A) To fetch JARs from HDFS on demand.
- B) To dynamically append JAR URLs to the classpath after the `SparkContext` has been initialized, supporting `sc.addJar()`.
- C) To isolate each task's class definitions to prevent static state leakage.
- D) To parse the `META-INF/MANIFEST.MF` of the uberjar.
- **Answer:** B
- **Mastery Explanation:** Standard Java `URLClassLoader` instances are immutable regarding their URLs once instantiated. Spark wraps it in `MutableURLClassLoader` to allow runtime injection of user JARs via `spark-submit --jars` or `sc.addJar()`.

**18. Why does a failure to define `MergeStrategy.concat` for `reference.conf` in sbt-assembly cause subtle runtime failures in Spark?**
- A) Spark's SQL parser relies on `reference.conf` for dialect definitions.
- B) The BlockManager uses it to resolve HDFS topology.
- C) Spark's internal RPC system relies on Akka/Typesafe config default bindings. Discarding them causes `NullPointerException`s during ActorSystem startup.
- D) Kryo uses it to discover registrator classes.
- **Answer:** C
- **Mastery Explanation:** Libraries like Akka and Play (and historically Spark's RPC) use Typesafe Config, which loads default settings from `reference.conf` files across all JARs on the classpath. Deduplicating them discards essential framework defaults, causing cryptic initialization crashes.

**19. When defining Kubernetes pod resources for a Spark executor, how is the container's hard memory limit calculated by Spark's submission client?**
- A) `spark.executor.memory` only.
- B) `spark.executor.memory` + `spark.memory.offHeap.size`
- C) `spark.executor.memory` + `spark.executor.memoryOverhead` + `spark.memory.offHeap.size` + `spark.executor.pyspark.memory`
- D) `spark.executor.memory` * `spark.executor.cores`
- **Answer:** C
- **Mastery Explanation:** The K8s pod's memory limit is the sum of the JVM heap (`executor.memory`), the explicit overhead budget (which must cover Metaspace, JVM internals, and native libs), off-heap Tungsten memory, and PySpark memory if applicable. Exceeding this exact sum triggers Kubernetes `OOMKilled`.

**20. A 50MB uberjar is deployed to a 1000-node cluster without `spark.files.useFetchCache=true`. What is the immediate physical consequence?**
- A) Metaspace OOM on the driver.
- B) 1000 concurrent network downloads of 50MB from the driver's port 4040 or HDFS, potentially saturating the driver's NIC or HDFS NameNode.
- C) Class duplication errors in the `ExecutorClassLoader`.
- D) Kryo buffer overflow.
- **Answer:** B
- **Mastery Explanation:** The JAR fetch phase is O(jar_size * n_executors). Without the fetch cache (which shares a single local disk copy per physical Kubernetes/YARN node), every single executor pod fetches the JAR independently, causing massive network spikes and slow startup times.

**21. Why is it recommended to mark `spark-core` and `spark-sql` as `provided` scope in your build tool when creating an uberjar?**
- A) To force the Janino compiler to optimize the bytecode.
- B) Because bundling them causes "Cannot run multiple SparkContexts" errors and inflates the JAR size unnecessarily since the executor classpath already provides them.
- C) To enable Tungsten memory management.
- D) To allow Kryo to serialize Spark's internal structs.
- **Answer:** B
- **Mastery Explanation:** The cluster runtime environment already includes all Spark libraries. Bundling them in the uberjar creates massive bloat, duplicates classes, and can cause fatal runtime collisions (e.g., attempting to initialize a second SparkContext).

**22. If you see `java.lang.OutOfMemoryError: Metaspace` on the driver before any tasks execute, what is the most mathematically sound tuning response?**
- A) Increase `spark.driver.memory`.
- B) Use `jar tf` to count the `.class` files in the assembly, multiply by ~10KB, and set `-XX:MaxMetaspaceSize` to at least that value.
- C) Enable `spark.memory.offHeap.enabled=true`.
- D) Change the Garbage Collector to ZGC.
- **Answer:** B
- **Mastery Explanation:** Metaspace size is directly proportional to the number of loaded classes. A rough heuristic is 8-12KB per class. Increasing heap (`spark.driver.memory`) does nothing for Metaspace exhaustion.

**23. When you shade a library like Guava, how does it affect the Spark framework itself?**
- A) Spark is forced to use your shaded version.
- B) Spark crashes because it cannot find its internal Guava.
- C) It has zero effect on Spark; Spark's code continues to reference the unshaded `com.google.common` namespace, safely isolating the two versions.
- D) It disables Whole-Stage CodeGen.
- **Answer:** C
- **Mastery Explanation:** Shading isolates your dependency by rewriting its package name. Spark's compiled classes still look for the original namespace, which is provided by Spark's internal JARs, allowing both versions to coexist peacefully in the same JVM.

**24. What is the impact of setting `spark.kryoserializer.buffer.max=256m`?**
- A) It limits the total memory Kryo can use across all tasks.
- B) It sets the maximum size of a single object graph that Kryo can serialize before throwing a buffer overflow exception.
- C) It dictates the size of the Tungsten off-heap page.
- D) It reserves 256MB of Metaspace for Kryo registrators.
- **Answer:** B
- **Mastery Explanation:** If a single record (e.g., a massive array or highly nested struct) serializes to a size larger than this buffer, Kryo fails with "Buffer overflow". It does not allocate 256MB upfront; it represents the maximum allowable growth of the buffer.

**25. A developer accidentally includes the `.SF`, `.DSA`, and `.RSA` files in the `META-INF` directory of the assembled uberjar. What happens at runtime?**
- A) The JVM throws a `SecurityException: Invalid signature file digest for Manifest main attributes`.
- B) The BlockManager rejects the file.
- C) Kryo refuses to serialize the classes.
- D) The Janino compiler throws a CodeGen exception.
- **Answer:** A
- **Mastery Explanation:** These files are cryptographic signatures of the original component JARs. When merged into an uberjar whose contents have changed (classes added/removed), the signatures no longer match the manifest digest. The JVM's classloader verifies this and aborts with a SecurityException. They must be discarded via `MergeStrategy.discard`.

## Part 3: "Small Twist" Scenario Questions (15 Questions)

**26. Scenario:** You have a working Spark job. You add a new dependency that transitively pulls in a custom `KryoRegistrator`. You shade Kryo to avoid version conflicts.
**Twist:** You forget to set `spark.kryo.registrator`. What happens during shuffle?
- A) Spark throws a `ClassNotFoundException` for the registrator.
- B) Two isolated instances of Kryo are loaded (Spark's and yours); classes registered in your code are unregistered during Spark's internal shuffle write, falling back to Java serialization silently.
- C) Spark crashes with `ClassCastException`.
- D) Tungsten takes over serialization automatically.
- **Answer:** B
- **Mastery Explanation:** Shading Kryo creates a parallel universe. If you don't explicitly tell Spark to use your shaded registrator class, Spark's internal Kryo instance handles the shuffle without knowing about your registrations, causing massive performance degradation.

**27. Scenario:** Your cluster has nodes with 64GB RAM. You configure an executor with `spark.executor.memory=4g` and `spark.executor.memoryOverhead=600m`.
**Twist:** You set `-XX:MaxMetaspaceSize=1g`. The uberjar loads 60,000 classes. What failure occurs?
- A) `OutOfMemoryError: Metaspace`
- B) Kubernetes `OOMKilled` (exit 137)
- C) YARN Container preempted
- D) Normal execution
- **Answer:** B
- **Mastery Explanation:** The JVM is permitted to grow Metaspace up to 1GB. However, the container's overhead budget is only 600MB. Once Metaspace grows beyond ~400MB (leaving 200MB for JVM internals), the total RSS exceeds 4.6GB (Heap + Overhead limit), and the OS/K8s kills the container instantly.

**28. Scenario:** You use `MergeStrategy.concat` for all files in `META-INF/services`.
**Twist:** Two different JARs provide a service file for `com.fasterxml.jackson.databind.Module`, but one of the JARs was shaded. What happens when Jackson scans the SPI?
- A) Jackson successfully loads both modules.
- B) Jackson throws a `ClassNotFoundException` because the SPI file still references the original unshaded class name, which no longer exists.
- C) Jackson ignores the shaded module.
- D) Spark's MutableURLClassLoader deduplicates them automatically.
- **Answer:** B
- **Mastery Explanation:** Shading tools rewrite bytecode, but they often do NOT rewrite text files in `META-INF/services/`. If the SPI file points to `com.example.MyModule` but the class was shaded to `shaded.com.example.MyModule`, Jackson's `ServiceLoader` instantiation will crash.

**29. Scenario:** You explicitly register an array type: `kryo.register(classOf[Array[SensorReading]])`.
**Twist:** You use a Python PySpark wrapper to submit the job, setting `spark.kryo.classesToRegister="com.example.model.SensorReading[]"` instead of the JVM internal descriptor. What happens?
- A) Kryo registers it successfully.
- B) Kryo throws a `ClassNotFoundException` because the JVM internal name for an array is `[Lcom.example.model.SensorReading;`.
- C) PySpark intercepts and translates the name correctly.
- D) It falls back to Java serialization.
- **Answer:** B
- **Mastery Explanation:** The `classesToRegister` conf expects exact JVM class names. For arrays, this is the internal bytecode descriptor format (`[L...;`). Using Java source syntax like `Class[]` causes resolution failure.

**30. Scenario:** Your driver logic prints `Class.forName("com.google.common.base.Preconditions").getProtectionDomain.getCodeSource.getLocation` and confirms it points to `myapp-assembly.jar`.
**Twist:** You submit to the cluster. A task executes a UDF containing the exact same reflection code. What does it print?
- A) `myapp-assembly.jar`
- B) `spark-assembly.jar` or the internal Spark Guava JAR path.
- C) Null
- D) `TungstenOffHeap.jar`
- **Answer:** B
- **Mastery Explanation:** The driver's context classloader is controlled by your submission script. On the executor, the `ExecutorClassLoader` delegates to its parent (the system classloader) *first*, which loads Spark's bundled Guava JAR instead of yours.

**31. Scenario:** You set `assemblyOption in assembly := copy(includeScala = false)` to drop the Scala standard library.
**Twist:** You submit the JAR to an environment running Spark compiled with Scala 2.13, but your uberjar was compiled with Scala 2.12. What is the immediate symptom?
- A) The driver starts, but tasks fail with `NoSuchMethodError` on basic Scala collection operations.
- B) The JAR is rejected by the BlockManager.
- C) Metaspace OOM.
- D) Janino compilation fails.
- **Answer:** A
- **Mastery Explanation:** Without bundling Scala, your code relies on the executor's Scala runtime. Scala 2.12 and 2.13 are not binary compatible (collections API changed heavily). Code compiled for 2.12 calling 2.13 collections will throw method not found errors.

**32. Scenario:** You configure `spark.dynamicAllocation.enabled=true`.
**Twist:** You set `spark.files.useFetchCache=false`. A sudden spike in data causes Spark to request 200 new executors simultaneously. What is the primary bottleneck?
- A) Metaspace allocation latency.
- B) HDFS or Driver HTTP network saturation as 200 nodes simultaneously download the 150MB uberjar.
- C) Janino CodeGen compilation time.
- D) Kryo registrator instantiation.
- **Answer:** B
- **Mastery Explanation:** Without the fetch cache sharing local node copies, 200 isolated executor pods execute a full O(N) network fetch of the JAR, easily saturating the 10Gbps NIC of the driver or hitting HDFS connection limits, stalling scale-out for minutes.

**33. Scenario:** You are diagnosing OOMs and add `-XX:+PrintGCDetails`.
**Twist:** The logs show GC pauses taking 10 seconds, but heap usage is only at 40% immediately before the pauses. What is triggering the massive GC?
- A) Tungsten off-heap memory eviction.
- B) Metadata GC thresholds (Metaspace resizing) triggering Full GCs to unload classes.
- C) Kryo buffer reallocation.
- D) RDD caching.
- **Answer:** B
- **Mastery Explanation:** By default, when Metaspace reaches its `MetaspaceSize` (high-water mark), the JVM halts execution to perform a Full GC to attempt class unloading before expanding Metaspace. If `MetaspaceSize` is too low (default is ~21MB), a large uberjar causes multiple devastating Full GCs during startup.

**34. Scenario:** You shade `com.google.protobuf` to `shaded.com.google.protobuf` using `.inAll`.
**Twist:** Your application writes data to the Hive Metastore using Spark SQL. The Hive client internally uses Protobuf 2.5, which is now completely shaded in your JAR. What happens when Spark communicates with Hive?
- A) The Hive RPCs fail because they expect the unshaded `com.google.protobuf` classes, but your code forces the shaded ones.
- B) It succeeds, because Spark's internal classpath still contains the unshaded Protobuf 2.5 JAR which the Hive client binds to.
- C) Spark uses JSON instead of Protobuf for Hive.
- D) Janino recompiles the Hive client.
- **Answer:** B
- **Mastery Explanation:** This is the exact reason shading works! Your app uses the shaded namespace. Spark's internal systems (like the Hive client) still look for the unshaded namespace, find it in Spark's parent classloader, and function perfectly without collision.

**35. Scenario:** You set `-XX:MaxMetaspaceSize=256m` and `spark.executor.memoryOverhead=1024m`.
**Twist:** Your uberjar requires 300MB of Metaspace. What specific error message terminates the application?
- A) `OOMKilled` (exit 137) from Kubernetes.
- B) `java.lang.OutOfMemoryError: Metaspace` in the executor logs.
- C) `java.lang.OutOfMemoryError: Java heap space`
- D) `java.lang.StackOverflowError`
- **Answer:** B
- **Mastery Explanation:** Because the container overhead (1024MB) is safely larger than the requested Metaspace, the OS/K8s will not kill the pod. Instead, the JVM hits its explicit internal limit (256MB) and gracefully throws a diagnosable `OutOfMemoryError: Metaspace`.

**36. Scenario:** You implement a custom Kryo registrator in Scala.
**Twist:** You use `kryo.register(Class.forName("com.example.model.SensorReading"))` instead of `kryo.register(classOf[SensorReading])`. What happens on the executor?
- A) It works identically.
- B) It throws `ClassNotFoundException` because `Class.forName` without a classloader argument uses the caller's classloader (often system), missing the `ExecutorClassLoader`.
- C) It registers the class as a String.
- D) It bypasses Kryo and uses Java serialization.
- **Answer:** B
- **Mastery Explanation:** `Class.forName(String)` is context-dependent. On executors, it binds to the wrong classloader. `classOf[T]` (or `T.class` in Java) is resolved at compile time and natively binds to the classloader that loaded the registrator itself (the `ExecutorClassLoader`).

**37. Scenario:** You configure `sbt-assembly` to `MergeStrategy.first` for all unknown conflicts.
**Twist:** Two transitive dependencies include different versions of `log4j.properties`. Spark's logging is configured via the driver. What happens to executor logging?
- A) Executors stop logging completely.
- B) Executors use the first `log4j.properties` found in the uberjar, potentially changing log levels (e.g., from INFO to DEBUG) unpredictably across builds.
- C) Spark forces the driver's log configuration over RPC.
- D) Log4j crashes with a merge conflict.
- **Answer:** B
- **Mastery Explanation:** `MergeStrategy.first` introduces build non-determinism. Whichever JAR gets scanned first provides the config file. This can silently enable massive DEBUG logging on executors, filling local disks and crashing nodes.

**38. Scenario:** You write a static Singleton object in Scala to hold a database connection pool: `object DbPool { val pool = new ConnectionPool() }`.
**Twist:** You deploy this in an uberjar to a cluster with 50 executors, each running 4 cores (200 tasks total). How many connection pools are created?
- A) 1 (Driver only).
- B) 50 (One per executor JVM).
- C) 200 (One per task).
- D) 0 (Singletons cannot be serialized).
- **Answer:** B
- **Mastery Explanation:** Static/Object initialization occurs once per Classloader. Because each executor JVM creates its own `ExecutorClassLoader`, the singleton is instantiated exactly once per executor (50 times), shared among the 4 tasks running within that JVM.

**39. Scenario:** You set `spark.kryo.registrationRequired=true`.
**Twist:** You forget to register `scala.collection.immutable.Map$Map1`. Your code only uses standard Scala maps. What happens?
- A) It succeeds because Scala standard library classes are automatically registered by Spark.
- B) It fails with `IllegalArgumentException: Class is not registered`.
- C) It falls back to Java serialization for the Map.
- D) It serializes the Map as a JSON string.
- **Answer:** B
- **Mastery Explanation:** While Spark auto-registers many primitive arrays, it does *not* auto-register specific optimized internal implementations of Scala collections (like `Map1`, `Map2`, `Map3`). Strict registration will aggressively fail the job, forcing you to register the exact internal type.

**40. Scenario:** Your uberjar contains a deeply nested Protobuf object.
**Twist:** The serialized size of one record is 120MB. `spark.kryoserializer.buffer.max` is left at the default 64MB. How does Spark handle this during a shuffle?
- A) Spark fragments the record across multiple 64MB buffers.
- B) Kryo throws a "Buffer overflow" exception and the task fails.
- C) Spark falls back to Tungsten off-heap serialization.
- D) The record is skipped and logged as a warning.
- **Answer:** B
- **Mastery Explanation:** Kryo cannot fragment individual object graphs. The buffer must be contiguous and large enough to hold the entire serialized representation of a single object. If max buffer size is exceeded, the serialization hard crashes.

## Part 4: Coding & Debugging Questions (10 Questions)

**41. Debugging Scenario:**
You observe that your Spark application takes 3 minutes to execute the first task on every newly allocated executor, but subsequent tasks take milliseconds.
*Code Context:*
```scala
val df = spark.read.parquet("s3://data/")
df.map(row => enrichWithDb(row)).count()
```
*Question:* How does the uberjar's classloading mechanism explain this latency, and how do you prove it?
- **Answer / Mastery Explanation:** Frameworks like Spring, Hibernate, or Jackson (often initialized inside `enrichWithDb`) eagerly scan the classpath on initialization. On the first task, the `ExecutorClassLoader` lazily pulls thousands of classes from the 100MB uberjar into Metaspace. To prove this, take thread dumps during the 3-minute freeze; you will see threads blocked in `java.lang.ClassLoader.loadClass` or `URLClassLoader.findClass`.

**42. Debugging Scenario:**
You receive this stack trace in an executor log:
```
java.lang.NoSuchMethodError: com.google.common.base.Stopwatch.createStarted()Lcom/google/common/base/Stopwatch;
```
Your `build.sbt` includes `"com.google.guava" % "guava" % "31.1-jre"`, which definitely has this method.
*Question:* What specifically went wrong in the `MutableURLClassLoader` hierarchy, and what is the code-level fix?
- **Answer / Mastery Explanation:** Spark's system classloader loaded an older Guava (e.g., v14) bundled with Spark/Hadoop, which lacks `createStarted()`. Parent-first delegation caused Spark's version to eclipse yours. The fix is adding `ShadeRule.rename("com.google.common.**" -> "shaded.guava.@1").inAll` to your `assemblyShadeRules`.

**43. Architecture Scenario:**
You are asked to review this PySpark Kryo configuration:
```python
conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
conf.set("spark.kryo.classesToRegister", "com.mycompany.FraudModel")
# registrationRequired is NOT set
```
*Question:* The developer claims Kryo is making their shuffles 3x faster. However, you look at the Spark UI and shuffle write size is identical to Java serialization. Why? Identify the silent failure.
- **Answer / Mastery Explanation:** Without `registrationRequired=true`, if `com.mycompany.FraudModel` contains nested complex types (e.g., `java.util.ArrayList`, or a custom `Transaction` class) that are NOT registered, Kryo silently falls back to writing the fully qualified class name for every instance of the unregistered type. This bloats the payload back to Java serialization sizes without throwing an error.

**44. Debugging Scenario:**
On Kubernetes, a pod terminates instantly with `Exit Code 137`.
*Pod YAML Context:*
```yaml
resources:
  requests: { memory: "4Gi" }
  limits: { memory: "4Gi" }
env:
  - name: SPARK_EXECUTOR_MEMORY
    value: "4g"
```
*Question:* Identify the mathematical impossibility in this configuration related to uberjar classloading.
- **Answer / Mastery Explanation:** The JVM heap is set to 4GB. The pod limit is exactly 4GB. There is literally 0 bytes remaining for Metaspace, JVM native memory, or CodeCache. When the executor loads the uberjar, Metaspace allocates native memory outside the heap, pushing RSS over 4GB, triggering an immediate `OOMKilled`.

**45. Code Review:**
```scala
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
```
*Question:* Identify two catastrophic runtime failures this build configuration will cause for an uberjar heavily utilizing Jackson and Akka.
- **Answer / Mastery Explanation:**
  1. Discarding all of `META-INF` drops `META-INF/services/com.fasterxml.jackson.databind.Module`. Jackson will fail to deserialize JSON into Scala case classes because it cannot discover the `DefaultScalaModule`.
  2. `MergeStrategy.first` for `reference.conf` means Akka/Typesafe configs are truncated. The actor system will throw `ConfigException$Missing` for fundamental dispatcher keys that were in the discarded config files.

**46. Debugging Scenario:**
You run `jar tf myapp-assembly.jar | grep -c "\.class"` and see 85,000 classes. The JAR size is 150MB.
*Question:* Calculate the approximate minimum Metaspace size required, explain why, and write the necessary `-XX` flag to protect the JVM.
- **Answer / Mastery Explanation:** At roughly ~10KB per class, 85,000 classes require ~850MB of Metaspace. To prevent unbounded Metaspace growth from causing silent container OOMs, you must cap it: `-XX:MaxMetaspaceSize=1024m` (adding safety margin) and ensure `spark.executor.memoryOverhead` is at least 1500m to fit it.

**47. Code Interpretation:**
```scala
def detectSource() = {
    val cl = Thread.currentThread().getContextClassLoader
    val clazz = Class.forName("org.apache.avro.Schema", false, cl)
    println(clazz.getProtectionDomain.getCodeSource.getLocation)
}
```
*Question:* If this code is executed inside an RDD `map` partition on the cluster, and it prints `file:/opt/spark/jars/spark-core_2.12-3.3.0.jar`, what critical fact have you just proven about your uberjar's dependency on Avro?
- **Answer / Mastery Explanation:** You have proven that despite your uberjar potentially containing a newer version of Avro, the executor is binding to Spark's internal Avro version at runtime via parent-first classloading. Any calls to newer Avro APIs will fail.

**48. Architecture Scenario:**
You have shaded Guava successfully. However, your custom Spark `DataSourceV2` plugin, written in your shaded project, returns a `com.google.common.collect.ImmutableList` (the unshaded version from Spark's API) back to the Spark SQL engine.
*Question:* Will this compile? Will it run? Explain the boundary between shaded code and framework APIs.
- **Answer / Mastery Explanation:** It will not compile if you shaded Guava globally. Spark's API expects the literal `com.google.common.collect.ImmutableList`. If your code is rewritten to return `shaded.com.google.common.collect.ImmutableList`, the JVM sees them as completely different types. You cannot shade types that are part of the public API boundary between your code and Spark.

**49. Debugging Scenario:**
A developer complains that their `spark.files.useFetchCache=true` setting isn't speeding up dynamic allocation on Kubernetes.
*Infrastructure Context:* Kubernetes schedules each executor pod onto a completely different physical EC2 instance across a 500-node autoscaling group.
*Question:* Why is the fetch cache ineffective in this specific topology?
- **Answer / Mastery Explanation:** The fetch cache stores the downloaded JAR on the host node's local disk (via K8s `emptyDir` or `hostPath` mechanisms mapped by Spark). If every pod lands on a *different* physical node, there is no cache hit. The cache only accelerates startup when multiple pods are co-located on the same underlying Kubernetes worker node.

**50. Code Modification:**
```yaml
# Current GC flags
-XX:+UseG1GC
-XX:MaxMetaspaceSize=512m
```
Your structured streaming app dies after 4 days with Metaspace OOM, even though static uberjar classes only consume 200MB.
*Question:* Modify the GC flags to fix the memory leak caused by Janino Whole-Stage CodeGen continuously compiling new plans.
- **Answer / Mastery Explanation:** You must enable class unloading for Metaspace, otherwise dynamically generated Janino classes are never evicted.
Add: `-XX:+ClassUnloading -XX:+ClassUnloadingWithConcurrentMark`. This allows G1GC to sweep dead `Klass` structs from Metaspace during concurrent cycles, stabilizing the streaming app.
