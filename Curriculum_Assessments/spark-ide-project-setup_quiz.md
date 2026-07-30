# Master Class Assessment: Spark IDE Project Setup

## Part 1: True/False Questions

**1. Setting `spark.master` to `local[*]` completely eliminates all network serialization overhead in Spark because both driver and executors share the same JVM.**
**Answer:** False
**Mastery Explanation:** While a local IDE setup runs the driver and executor in a single JVM (making memory boundaries fluid), this masks serialization issues. Spark's architecture still simulates these boundaries, and if serialization configs (like Kryo) aren't enforced, `NotSerializableException` errors will be missed during local development but fail in a real cluster.

**2. Tungsten's execution engine relies entirely on standard JVM Garbage Collection for its query execution data structures.**
**Answer:** False
**Mastery Explanation:** Tungsten utilizes the `sun.misc.Unsafe` API to allocate memory directly from the host OS (off-heap memory), effectively bypassing standard JVM GC overhead for query execution.

**3. Setting `-opt:l:inline` in `build.sbt` enables the Catalyst Optimizer to generate faster physical execution plans.**
**Answer:** False
**Mastery Explanation:** This SBT flag optimizes the Scala compiler's byte-code generation via aggressive method inlining, not Catalyst. Catalyst operates on the AST of Spark SQL/DataFrame transformations, independent of Scala compiler inlining.

**4. The default `spark.sql.shuffle.partitions` value of 200 is highly optimal for local IDE testing as it maximizes concurrency.**
**Answer:** False
**Mastery Explanation:** 200 partitions for small local datasets creates massive thread scheduling overhead. It should be reduced (e.g., to 4) for local testing to avoid inefficient task orchestration.

**5. Setting `spark.kryo.registrationRequired=true` in a local IDE setup helps catch distributed execution errors by forcing the application to crash if unregistered classes are serialized.**
**Answer:** True
**Mastery Explanation:** In a local JVM, serialization errors might be masked. Forcing Kryo registration ensures that any class captured in a closure or sent over the simulated network is explicitly registered, mirroring strict cluster serialization constraints.

**6. When testing Structured Streaming in a local IDE, `local[1]` is the recommended master URL to minimize context switching.**
**Answer:** False
**Mastery Explanation:** `local[1]` will cause deadlocks in Structured Streaming and complex joins because one thread is required for the receiver/driver and another for active processing. `local[2]` is the minimum required.

**7. Disabling the Spark UI during test execution (`spark.ui.enabled="false"`) helps speed up the test suite and saves JVM resources.**
**Answer:** True
**Mastery Explanation:** The Spark UI spins up a Jetty web server. Disabling it avoids unnecessary port bindings, speeds up initialization, and reduces memory overhead in the IDE test runner.

**8. PyArrow enables zero-copy memory transfer between the JVM and Python worker processes in PySpark.**
**Answer:** True
**Mastery Explanation:** PyArrow vectorization bypasses standard Pickle serialization, allowing data to be shared directly in memory between the JVM (Catalyst/Tungsten) and Python, drastically reducing Inter-Process Communication (IPC) overhead.

**9. SBT `assemblyShadeRules` are primarily used to optimize Catalyst AST traversal by renaming internal Spark packages.**
**Answer:** False
**Mastery Explanation:** Shading is used to resolve "classpath hell" (dependency conflicts), such as when a local project uses a different version of Jackson or Guava than the Spark cluster environment.

**10. Calling `SparkSession.clearActiveSession()` and `SparkSession.clearDefaultSession()` in a test suite's `afterAll()` method prevents Catalyst caching issues between parallel tests.**
**Answer:** True
**Mastery Explanation:** Spark uses static singletons for the active context. Failing to clear them can pollute the JVM state, causing race conditions and unpredictable plan generation in subsequent test suites running in the same IDE JVM.

## Part 2: Multiple Choice Questions

**11. Why is `spark.kryo.registrationRequired` explicitly set to `true` in a local IDE setup?**
A) To bypass Tungsten off-heap allocation
B) To increase Kryo serialization speed by 10x
C) To preemptively catch `NotSerializableException` errors that usually only occur in distributed clusters
D) To force Catalyst to use RDDs instead of DataFrames
**Answer:** C
**Mastery Explanation:** Local mode often masks serialization errors since objects reside in the same JVM. Enforcing registration crashes the local job if an unregistered class is serialized, simulating distributed cluster strictness.

**12. What is the primary purpose of the `assemblyMergeStrategy` in a Spark `build.sbt` file?**
A) To merge small files in HDFS output
B) To handle conflicting files (like `META-INF` or `reference.conf`) when creating a fat JAR
C) To merge Catalyst logical plans into physical plans
D) To optimize Python Pickle closures
**Answer:** B
**Mastery Explanation:** During fat-jar assembly, multiple dependencies may contain identical files (e.g., `reference.conf` from Akka/Typesafe). The merge strategy dictates how to handle these collisions to prevent build failures.

**13. How does Tungsten manage memory differently than standard JVM objects?**
A) It uses G1GC for all allocations
B) It uses `sun.misc.Unsafe` to allocate memory off-heap, bypassing JVM Garbage Collection
C) It serializes all objects to disk
D) It relies exclusively on Python PyArrow
**Answer:** B
**Mastery Explanation:** Tungsten achieves bare-metal performance by managing memory off-heap via Unsafe, avoiding standard JVM GC pauses and object overhead.

**14. In PySpark IDE testing, why is `spark.sql.execution.arrow.pyspark.enabled` critical?**
A) It allows Python to run without a JVM
B) It converts Python code into Scala byte-code
C) It enables zero-copy memory transfer between JVM and Python workers, reducing IPC serialization overhead
D) It forces Python closures to be serialized via Pickle
**Answer:** C
**Mastery Explanation:** Arrow provides columnar, zero-copy memory transfer, heavily outperforming standard Pickle IPC serialization between the JVM and Python worker processes.

**15. What happens if a Structured Streaming test suite uses `.master("local[1]")`?**
A) The test runs twice as fast
B) Tungsten is disabled
C) The test will deadlock because streaming requires at least two threads
D) Catalyst falls back to rule-based optimization
**Answer:** C
**Mastery Explanation:** Streaming requires one thread for the receiver/driver and at least one for processing. `local[1]` provides only one thread, causing a deadlock.

**16. Which SBT scalac flag is used for aggressive method inlining?**
A) `-Ywarn-dead-code`
B) `-opt:l:inline`
C) `-target:jvm-1.8`
D) `-Xfatal-warnings`
**Answer:** B
**Mastery Explanation:** `-opt:l:inline` instructs the Scala compiler to aggressively inline methods, improving bare-metal performance before Spark even runs.

**17. What is the consequence of omitting `% "provided"` for `spark-core` in `build.sbt`?**
A) Catalyst will fail to compile
B) The resulting fat JAR will be bloated with Spark binaries already present on the cluster
C) The IDE will not be able to run local tests
D) Tungsten will be disabled
**Answer:** B
**Mastery Explanation:** Spark is provided by the cluster runtime. Including it in the compiled JAR swells the file size massively and can cause classpath collisions.

**18. What role does `sys.stdout` play in the provided PySpark closure serialization example?**
A) It speeds up PyArrow
B) It is an example of an un-serializable object that will crash the job if captured in a UDF closure
C) It redirects Spark UI logs to the console
D) It forces Tungsten to flush memory
**Answer:** B
**Mastery Explanation:** `sys.stdout`, open files, or sockets cannot be pickled and sent across a network. If captured in a Python UDF closure, it causes serialization failures.

**19. Which garbage collector is highly recommended for handling short-lived object lifecycles in local Spark JVMs?**
A) SerialGC
B) ParallelGC
C) ZGC
D) G1GC (Garbage First)
**Answer:** D
**Mastery Explanation:** Spark creates many short-lived objects. G1GC is optimized to handle this unpredictable lifecycle efficiently, avoiding long pause times in the IDE.

**20. Why might a Catalyst optimizer timeout during a local IDE execution?**
A) The Spark UI port is bound
B) The IDE's Run Configuration didn't allocate enough JVM memory (-Xmx), causing Catalyst to struggle with plan generation
C) PyArrow is disabled
D) The SBT version is outdated
**Answer:** B
**Mastery Explanation:** Catalyst's rule-based and cost-based optimizations are memory-intensive. Insufficient heap memory can cause GC thrashing and timeouts during plan generation.

**21. What does the `@transient` keyword do for the `_spark` variable in the Scala test trait?**
A) It makes the SparkSession faster
B) It prevents the SparkSession object from being serialized by the JVM during test suite execution
C) It enables Catalyst to optimize the session
D) It forces off-heap allocation
**Answer:** B
**Mastery Explanation:** In Scala, `@transient` prevents a variable from being serialized. `SparkSession` is not serializable; preventing its serialization avoids test framework errors.

**22. How does setting `spark.sql.shuffle.partitions` to 4 improve local IDE execution compared to the default?**
A) It increases Tungsten's off-heap size
B) It forces Catalyst to bypass shuffle operations entirely
C) It reduces the massive thread scheduling overhead of managing 200 empty partitions for a small local dataset
D) It disables Kryo
**Answer:** C
**Mastery Explanation:** 200 partitions is the default for distributed processing. Locally, creating 200 tasks for tiny data dominates execution time with task scheduling overhead.

**23. What is the main architectural difference masked by local IDE Spark versus distributed Spark?**
A) Local Spark uses Python, distributed uses Scala
B) Local Spark uses a single JVM for driver and executors, masking network RPC and serialization boundaries
C) Local Spark disables Catalyst entirely
D) Distributed Spark cannot use Tungsten
**Answer:** B
**Mastery Explanation:** The single JVM in an IDE means memory is shared. Network serialization (converting objects to bytes for network transit) is merely simulated, leading to masked serialization bugs.

**24. In the fat-jar `assemblyMergeStrategy`, what does `MergeStrategy.discard` typically do for `META-INF` files?**
A) It compresses them
B) It discards conflicting security signatures and manifests from dependencies to prevent runtime security exceptions
C) It merges them into `reference.conf`
D) It throws a fatal warning
**Answer:** B
**Mastery Explanation:** Retaining multiple signed `META-INF` files from dependencies causes `SecurityException` during JAR execution. Discarding them resolves this.

**25. Why must we configure `spark.ui.port` in local IDE setups?**
A) To enable Catalyst UI debugging
B) To prevent port binding conflicts (`Address already in use`) when running multiple test suites or IDE runs repeatedly
C) To allow executors to communicate with the driver
D) To enable PyArrow zero-copy
**Answer:** B
**Mastery Explanation:** Successive or parallel IDE test runs might attempt to bind to the default port (4040), causing failures if the port isn't released quickly enough by the OS.

## Part 3: "Small Twist" Questions

**26. Scenario A: `spark.kryo.registrationRequired` is false. Scenario B: it's true. An unregistered class is passed into a map closure. What happens in the IDE?**
A) Both succeed.
B) Scenario A succeeds (masking the error). Scenario B throws KryoException, correctly failing the local test.
C) Both fail.
D) Scenario A fails, Scenario B succeeds.
**Answer:** B
**Mastery Explanation:** When false, Kryo will serialize the unregistered class using full class names (or standard Java serialization fallback might occur locally). When true, it strictly enforces registration, exposing the flaw.

**27. Scenario A: `master("local[*]")`. Scenario B: `master("local[1]")`. A test runs a standard `df.count()`. What happens?**
A) A succeeds, B deadlocks.
B) Both succeed, but B uses only 1 thread.
C) A deadlocks, B succeeds.
D) Both fail.
**Answer:** B
**Mastery Explanation:** For a simple batch `count()`, 1 thread is sufficient. The deadlock twist only applies to Streaming or complex operations requiring a separate driver/receiver thread.

**28. Scenario A: `spark-core % provided`. Scenario B: `spark-core % compile`. You build an assembly JAR and submit to a Kubernetes cluster. What happens in Scenario B?**
A) Nothing, it runs faster.
B) The JAR size inflates massively and causes `LinkageError` or `NoSuchMethodError` due to JVM classpath collisions with the cluster's Spark binaries.
C) Catalyst disables Tungsten.
D) Scala compiler inlining fails.
**Answer:** B
**Mastery Explanation:** The cluster already has Spark jars. Packing them in your fat JAR causes duplicate class conflicts and massive upload times.

**29. Scenario A: `spark.memory.offHeap.enabled = false`. Scenario B: `true`, with `size=1g`. How does Tungsten memory management change locally?**
A) In A, Tungsten doesn't run at all.
B) In A, Tungsten falls back to allocating its Unsafe memory structures on the standard JVM heap, increasing GC pressure. In B, it bypasses GC.
C) Both are identical.
D) B disables Catalyst.
**Answer:** B
**Mastery Explanation:** Tungsten can operate on-heap, but it's less efficient as it subjects those internal structures to GC. Enabling off-heap unlocks true bare-metal performance.

**30. Scenario A: PySpark UDF references a local string variable. Scenario B: UDF references an active database connection object. What happens?**
A) Both succeed.
B) Both fail.
C) A succeeds. B fails with a Pickle serialization error.
D) A fails, B succeeds.
**Answer:** C
**Mastery Explanation:** A string is serializable. A database connection (socket/file descriptor) is stateful to the machine and cannot be pickled/serialized across processes.

**31. Scenario A: `spark.sql.shuffle.partitions` = 200. Scenario B: 4. You aggregate a 10MB local dataframe. What happens?**
A) A executes much faster than B.
B) B executes much faster than A due to vastly reduced task scheduling and metadata overhead.
C) Both take identical time.
D) B throws an OOM error.
**Answer:** B
**Mastery Explanation:** 200 partitions for 10MB means 200 tasks processing 50KB each. The overhead of scheduling 200 tasks in the JVM vastly outweighs the processing time.

**32. Scenario A: `clearActiveSession()` is called in `afterAll()`. Scenario B: It is NOT called. The next test suite runs in the same JVM.**
A) In B, the next suite will crash immediately.
B) In B, Catalyst might use cached logical plans or configuration from the previous suite, causing unpredictable flaky tests.
C) Both run identical.
D) A throws a NullPointerException.
**Answer:** B
**Mastery Explanation:** The JVM retains the static active session. The next test suite might inadvertently inherit configs or state, breaking isolation.

**33. Scenario A: `reference.conf` merge strategy is `concat`. Scenario B: `discard`. You use Akka/Typesafe config in your project.**
A) B will fail at runtime because essential configuration definitions are missing.
B) Both work fine.
C) A fails during compilation.
D) B causes a security exception.
**Answer:** A
**Mastery Explanation:** `reference.conf` files define defaults for libraries like Akka. If discarded, the library won't initialize. They must be concatenated.

**34. Scenario A: `-opt:l:inline`. Scenario B: Flag removed. How does Catalyst physical plan generation change?**
A) A generates a better plan.
B) B generates a better plan.
C) No change to Catalyst plans; this flag affects Scala compiler byte-code before Catalyst evaluation.
D) A throws an error.
**Answer:** C
**Mastery Explanation:** Scalac flags optimize the JVM byte-code of your UDFs/Scala logic, but Catalyst relies on AST rules, independent of Scala's byte-code inlining.

**35. Scenario A: Jackson is shaded in `assemblyShadeRules`. Scenario B: Not shaded. The cluster uses Jackson 2.6, your code uses 2.12.**
A) Both succeed.
B) B fails at runtime on the cluster with `NoSuchMethodError` when your code calls a 2.12 specific method, due to the cluster's 2.6 classes taking precedence.
C) A fails to compile.
D) B disables PyArrow.
**Answer:** B
**Mastery Explanation:** This is classic classpath hell. Shading renames your Jackson to `shaded.jackson`, allowing both versions to coexist safely in the JVM.

**36. Scenario A: `spark.ui.enabled="false"`. Scenario B: `true`. You run 50 test suites sequentially in the IDE.**
A) B will take significantly longer due to spinning up and tearing down the Jetty web server 50 times.
B) A will crash due to missing UI.
C) Both take the same time.
D) A disables Tungsten.
**Answer:** A
**Mastery Explanation:** The UI server is resource-heavy to initialize. In unit tests, it's useless and massively inflates suite duration.

**37. Scenario A: `local[*]`. Scenario B: `local[4]`. Your machine has 16 logical cores.**
A) A creates 16 worker threads, B creates 4.
B) A creates 1 worker thread, B creates 4.
C) Both create 4.
D) Both create 16.
**Answer:** A
**Mastery Explanation:** `*` translates to "all logical cores available on the local machine."

**38. Scenario A: PyArrow enabled. Scenario B: PyArrow disabled. You run a Pandas UDF.**
A) B will fall back to standard Pickle IPC serialization, heavily degrading performance.
B) Both fail.
C) B will throw an immediate error because Pandas UDFs strictly require Arrow.
D) A throws an error.
**Answer:** C
**Mastery Explanation:** Actually, standard Python UDFs fall back to Pickle, but modern Pandas UDFs (Vectorized UDFs) explicitly require Arrow to be enabled. If disabled, Pandas UDFs fail.

**39. Scenario A: SBT `-Xfatal-warnings` enabled. Scenario B: disabled. You have a deprecated method call.**
A) A warns, B ignores.
B) A fails compilation entirely; B compiles with a warning.
C) A optimizes Catalyst; B does not.
D) Both fail compilation.
**Answer:** B
**Mastery Explanation:** `-Xfatal-warnings` treats all compiler warnings as fatal errors, enforcing strict code hygiene before Catalyst is even invoked.

**40. Scenario A: `META-INF` merge strategy is `first`. Scenario B: `discard`. A dependency contains a signed RSA file.**
A) A will retain the signature; if you modified any classes from that jar, the JVM will throw a SecurityException at runtime.
B) B retains the signature.
C) Both fail to compile.
D) A disables Kryo.
**Answer:** A
**Mastery Explanation:** Retaining signatures (`first`) for jars that you bundle into a fat jar invalidates the signature hash. You must `discard` them.

## Part 4: Coding & Debugging Questions

**41. Debugging a Fat JAR bloat:** Your local IDE assembly JAR is 250MB. When deployed to EMR, it fails with `java.lang.LinkageError`. 
*Fix:* You forgot to set `% "provided"` on `spark-core` and `spark-sql` in `build.sbt`. Spark runtime classes collided with your JAR.

**42. Debugging PySpark Serialization:** 
```python
logger = logging.getLogger("my_logger")
def my_udf(val):
    logger.info(val)
    return val
```
*Error:* PicklingError.
*Fix:* The `logger` object is instantiated outside the UDF and is non-serializable. Move the logger instantiation inside the UDF or use a serializable logging wrapper.

**43. Debugging Test Pollution:** Test Suite A passes. Test Suite B fails unpredictably with weird plan schemas. If you run Suite B alone, it passes.
*Fix:* Test Suite A is not calling `SparkSession.clearActiveSession()` and `SparkSession.clearDefaultSession()` in its `afterAll()` block, leaking Catalyst singleton state.

**44. Debugging Streaming Deadlock:**
```scala
val spark = SparkSession.builder().master("local[1]").getOrCreate()
val query = df.writeStream.start()
query.awaitTermination()
```
*Error:* The application hangs forever and processes zero micro-batches.
*Fix:* Change `.master("local[1]")` to `.master("local[2]")`. Streaming requires a thread for the continuous execution trigger and another for data processing.

**45. Debugging IDE Port Conflicts:** You run tests, and they randomly fail with `java.net.BindException: Address already in use: Service 'SparkUI'`.
*Fix:* Configure `.config("spark.ui.enabled", "false")` in your test SparkSession builder.

**46. Debugging Tungsten OOM:** You enabled `spark.memory.offHeap.enabled = true` but the job crashes locally with OS-level memory termination.
*Fix:* You must also explicitly configure `spark.memory.offHeap.size` (e.g., `"1g"`). Without a size, the OS limit isn't requested properly or defaults to 0.

**47. Debugging Assembly Merge Error:** `sbt assembly` fails with: `deduplicate: different file contents found in the following: .../reference.conf`
*Fix:* Add an `assemblyMergeStrategy` for `reference.conf` using `MergeStrategy.concat`.

**48. Debugging Local Shuffle Slowness:** A simple `groupBy().count()` on 100 rows in your IDE takes 15 seconds.
*Fix:* The default `spark.sql.shuffle.partitions` is 200. Add `.config("spark.sql.shuffle.partitions", "2")` to the local session to stop scheduling 200 tasks for 100 rows.

**49. Debugging Kryo Masking:** Code works locally but fails on the cluster with `NotSerializableException: com.example.MyCustomClass`. 
*Fix:* Your local IDE didn't enforce registration. Add `.config("spark.kryo.registrationRequired", "true")` locally. This will make it fail in the IDE, forcing you to register `MyCustomClass`.

**50. Debugging G1GC Configuration:** Catalyst plan generation times out locally on complex queries. JConsole shows massive GC pauses.
*Fix:* Your IDE Run Configuration is using the default JVM GC (likely Serial or Parallel). Edit the IDE JVM flags to include `-XX:+UseG1GC -Xmx4g` to handle Spark's short-lived AST objects efficiently.
