# 🔥 Master Class: Uberjars — Fat JARs, Classpath Conflicts, and Distributed Classloading in Apache Spark

## Overview

An uberjar (also called a "fat JAR" or "assembly JAR") is a single, self-contained Java Archive that bundles your application code together with every transitive dependency it requires at runtime. In the context of Apache Spark, uberjars solve a fundamental distribution problem: when Spark submits a job to a cluster — whether YARN, Kubernetes, or standalone — the driver and every executor must have access to identical classpath resources. Without an uberjar, you would need to manually provision each node with every library your job depends on, a process that is error-prone, slow, and incompatible with elastic autoscaling environments where nodes may be provisioned on-demand.

The uberjar pattern emerged from the reality that Spark's distributed execution model requires bytecode to be physically shipped from the driver to executors. When you pass `--jars` or set `spark.jars`, Spark uploads those files to HDFS or the Kubernetes staging area and redistributes them via the `BlockManager`. A single fat JAR simplifies this to one atomic operation. The trade-off is significant: combining dozens of dependency JARs into one archive introduces class relocation conflicts, Metaspace inflation, and classloading races that can silently corrupt runtime behavior in ways that are extraordinarily difficult to diagnose without deep JVM knowledge.

Production Spark engineering demands you understand not just *how* to build an uberjar, but *what happens inside the JVM* when that JAR is loaded — across every executor, simultaneously, under GC pressure. 

---

## 🏗️ Architectural Deep Dive 

### How It Works Under the Hood

When `spark-submit` is invoked with `--class com.example.MyJob myapp-assembly.jar`, the driver JVM's bootstrap classloader hands control to Spark's `MutableURLClassLoader`, which loads the fat JAR's entries into the JVM's Metaspace (not the heap). Each class definition — its bytecode, constant pool, and method table — occupies a `Klass` structure in Metaspace. A 50MB uberjar containing 30,000 class files can easily consume 200–400MB of Metaspace on the driver alone. If `MaxMetaspaceSize` is not tuned (default is unbounded on JDK 8, but constrained by the OS), loading an oversized uberjar triggers a `java.lang.OutOfMemoryError: Metaspace` that kills the driver before a single task is scheduled.

On the executor side, the TaskScheduler instructs executors to fetch the JAR via `SparkContext.addJar()`, which triggers the `BlockManager` to download the file from the driver's HTTP server (port 4040) or from HDFS. Each executor unpacks the JAR into its local working directory and creates its own `ExecutorClassLoader` — a child of the system classloader — to load classes from it. This means class definitions are loaded independently per-executor-JVM; there is no shared Metaspace across the cluster. If 500 executors each load a 400MB worth of class metadata, your cluster is consuming 200GB of Metaspace collectively. Tungsten's off-heap memory sits entirely outside this, but Metaspace pressure directly competes with the executor's heap for physical RAM, triggering OS-level memory pressure and potential container eviction in Kubernetes (OOMKilled).

The Catalyst optimizer's code generation phase — Whole-Stage CodeGen — dynamically compiles generated query plans into JVM bytecode at runtime using Janino. These generated classes are loaded into the executor's classloader alongside your uberjar's classes. If your uberjar has relocated or shadowed a version of a library that Janino or the Catalyst runtime expects (e.g., `com.google.common` from Guava), a `NoSuchMethodError` or `ClassCastException` emerges at plan execution time, not at job submission — making the failure appear non-deterministic. Kryo serialization compounds this: Kryo resolves class names to `Class` objects via `Class.forName()`, which uses the current thread's context classloader. If the uberjar's classloader hierarchy is misconfigured, Kryo silently falls back to Java serialization, inflating shuffle payload sizes by 3–5× and crashing on non-`Serializable` types.

```text
spark-submit (Driver JVM)
┌────────────────────────────────────────────────────────────┐
│ Bootstrap ClassLoader │
│ └── System ClassLoader (spark-assembly.jar, Scala rt) │
│ └── MutableURLClassLoader │
│ └── [myapp-assembly.jar → Metaspace] │
│ 30,000 Klass structs ~350MB │
└─────────────────────────────┬──────────────────────────────┘
 │ JAR upload via HTTP / HDFS
 ┌───────────────▼───────────────┐
 │ BlockManager (Driver) │
 │ spark.files.useFetchCache=T │
 └───────────────┬───────────────┘
 │ fetch (per executor)
 Executor JVM #1 │ Executor JVM #N
 ┌──────────────────┐ │ ┌──────────────────┐
 │ ExecutorClassLdr │◀──────┴────▶│ ExecutorClassLdr │
 │ [Metaspace: 350M]│ │ [Metaspace: 350M]│
 │ Tungsten off-heap│ │ Tungsten off-heap│
 │ (managed memory) │ │ (managed memory) │
 └──────────────────┘ └──────────────────┘
 Whole-Stage CodeGen Whole-Stage CodeGen
 (Janino → new Klass structs) (Janino → new Klass structs) 
```

### Key Internal Components

- **`MutableURLClassLoader`:** Spark's primary classloader on the driver. It wraps a `URLClassLoader` with mutation support so that `sc.addJar()` can append JAR URLs at runtime after `SparkContext` initialization. Its delegation model follows parent-first by default, meaning Spark's own bundled libraries take precedence over your application's — a common source of version conflicts.

- **`ExecutorClassLoader`:** Created fresh per-executor-JVM by `CoarseGrainedExecutorBackend`. It downloads the fat JAR from the driver's FileServer or from the distributed cache and loads classes lazily on first use. Because each executor has an isolated classloader, static singletons (e.g., a database connection pool) initialized via static initializers are replicated N times across the cluster, not shared.

- **sbt-assembly / Maven Shade Plugin:** Build-time tools that merge all dependency JARs into a single archive. Both support *relocation* (rewriting bytecode to move packages to new namespaces, e.g., `com.google.common` → `shaded.com.google.common`) to isolate your dependency versions from Spark's own dependencies that co-exist on the classpath.

- **Metaspace (JVM Native Memory):** The off-heap region storing class metadata (`Klass`, `Method`, `ConstantPool` structures). Unlike the PermGen of JDK 7, Metaspace grows dynamically. Each unique loaded class consumes memory here permanently until its classloader is GC'd. Fat JARs with many classes that are never actually instantiated still incur Metaspace cost upon first reference during classpath scanning by frameworks like Spring or Jackson's `ObjectMapper`. 

---

## ⚠️ Critical Concepts & Common Pitfalls 

### Classpath Conflict: The "Jar Hell" Failure Mode

When two JARs inside your uberjar contain the same fully-qualified class name — for example, `org.apache.commons.lang3.StringUtils` from `commons-lang3:3.9` (your dependency) and `commons-lang3:3.4` (Spark's internal dependency) — the classloader loads whichever version appears first in the JAR's internal entry order. This is non-deterministic across build runs if `mergeStrategy` is not set explicitly in sbt-assembly. The loaded class may have a different binary API than the caller expects, producing a `NoSuchMethodError` at runtime that references a method that exists in the wrong version.

The insidious failure mode is that unit tests pass (running against your build classpath with your version first) while the cluster job fails (Spark's version wins at runtime). The fix requires either shading the conflicting dependency — rewriting all bytecode references with a tool like `jarjar` — or explicitly excluding your version and relying on Spark's bundled copy with `provided` scope. Shading is safer for libraries like Guava, Protobuf, and Jackson, which Spark ships internally and which undergo frequent breaking API changes between minor versions. 

### Metaspace Pressure and Container Eviction in Kubernetes

A 70MB uberjar containing Jackson, Guava, Protobuf, Kryo, Avro, and Hive metastore client can load 45,000+ classes, consuming 500–700MB of Metaspace. On Kubernetes, an executor pod configured with `executor.memory=4g` and no explicit native memory overhead will have its container memory limit set to approximately 4.4GB by Spark's default overhead formula (`spark.executor.memoryOverhead = max(executor.memory × 0.10, 384MB)`). With 700MB of Metaspace, 300MB of JVM internal structures, and 4GB of heap, the process's actual RSS easily exceeds 5GB, triggering `OOMKilled` with exit code 137. The pod restarts, the task retries, and the Spark UI shows mysterious "executor lost" events rather than an explicit OOM.

The correct fix is to set `spark.executor.memoryOverheadFactor` to account for Metaspace (typically 0.20–0.30 for large uberjars) and to explicitly bound Metaspace with `-XX:MaxMetaspaceSize=512m`. Setting `MaxMetaspaceSize` without adequate overhead causes `OutOfMemoryError: Metaspace`, but at least produces a diagnosable JVM error rather than a silent container kill. 

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|-----------|---------|-------|
| JAR upload to HDFS/staging | O(jar_size) | No | Amortized once per `spark-submit`; bottlenecks on driver NIC if JAR > 200MB |
| Per-executor JAR fetch | O(jar_size × n_executors) | No | Parallel; `spark.files.useFetchCache=true` deduplicates on same node |
| Class loading into Metaspace | O(n_classes) | No | Lazy per first-reference; framework scanners (Spring, Jackson) eagerly load all |
| Kryo class registration lookup | O(1) amortized | No | Degrades to O(n) scan if unregistered; fallback to Java serialization kills shuffle perf |
| Shade relocation bytecode rewrite | O(n_class_files) | No | Build-time only; zero runtime cost once JAR is assembled |
| Duplicate class merge at assembly | O(n_entries) | No | Incorrect `MergeStrategy` silently drops classes; validate with `jarjar inspect` | 

---

## 💻 Code Examples 

### Example 1: Correct sbt-assembly Configuration with Shading and Merge Strategies

> **What this demonstrates:** How to configure `sbt-assembly` to produce a production-safe uberjar that relocates conflicting Guava and Protobuf versions and defines explicit merge strategies for every known conflict category, rather than accepting the dangerous default `MergeStrategy.deduplicate`.

```scala
// build.sbt — Production uberjar configuration for a Spark 3.3 job
// Every non-trivial directive is annotated with its runtime consequence.

ThisBuild / scalaVersion := "2.12.17"
ThisBuild / version := "1.0.0"

lazy val root = (project in file("."))
 .settings(
 name := "my-spark-job",

 libraryDependencies ++= Seq(
 // Mark spark-core as "provided" — it MUST NOT be bundled into the uberjar.
 // The executor classpath already contains Spark's own JARs. Bundling them
 // causes class duplication and can load two SparkContext implementations,
 // producing "Cannot run multiple SparkContexts" errors.
 "org.apache.spark" %% "spark-core" % "3.3.2" % "provided",
 "org.apache.spark" %% "spark-sql" % "3.3.2" % "provided",

 // Your application dependencies — these WILL be bundled.
 "com.google.guava" % "guava" % "31.1-jre", // conflicts with Spark's guava-14
 "com.google.protobuf" % "protobuf-java" % "3.21.12", // conflicts with Spark's protobuf-2.5
 "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.14.2"
 ),

 // assemblyShadeRules: rewrite bytecode to relocate conflicting packages.
 // After shading, all references to com.google.common.* inside YOUR code
 // and YOUR dependencies are rewritten to shaded.com.google.common.*
 // Spark's classpath still uses the original com.google.common.* namespace,
 // so the two versions coexist in the same JVM without collision.
 assemblyShadeRules in assembly := Seq(
 ShadeRule.rename("com.google.common.**" -> "shaded.com.google.common.@1")
 .inAll, // applies to all JARs in the assembly, not just direct deps
 ShadeRule.rename("com.google.protobuf.**" -> "shaded.com.google.protobuf.@1")
 .inAll,
 // Do NOT shade Scala standard library or Spark API classes —
 // shading scala.collection.* would break interoperability with Spark's own code.
 ShadeRule.keep("org.apache.spark.**").inAll
 ),

 // assemblyMergeStrategy: define what to do when two JARs contribute
 // the same file path. The default MergeStrategy.deduplicate throws an
 // error if the files differ — safer than silently picking one, but
 // requires explicit handling of every conflict category.
 assemblyMergeStrategy in assembly := {

 // META-INF/MANIFEST.MF: each JAR has one; keep only the assembly's manifest.
 case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard

 // Service provider files (META-INF/services/*): MUST be concatenated,
 // not discarded. Discarding these breaks Java SPI discovery —
 // e.g., Jackson's ObjectMapper will fail to find its modules.
 case PathList("META-INF", "services", _*) => MergeStrategy.concat

 // License and notice files: discard duplicates to reduce JAR size.
 case PathList("META-INF", xs @ _*) if xs.exists(_.endsWith(".SF")) =>
 MergeStrategy.discard // discard signature files — they invalidate JAR signing
 case PathList("META-INF", xs @ _*) if xs.exists(_.endsWith(".DSA")) =>
 MergeStrategy.discard
 case PathList("META-INF", xs @ _*) if xs.exists(_.endsWith(".RSA")) =>
 MergeStrategy.discard

 // reference.conf: Akka/Typesafe config files MUST be concatenated,
 // not discarded. Spark's internal Akka uses reference.conf for defaults;
 // your app may also have one. Discarding either silently removes config keys
 // that cause NullPointerExceptions in ActorSystem initialization.
 case "reference.conf" => MergeStrategy.concat

 // For all other duplicate paths, prefer the first occurrence.
 // Log a warning in CI to catch unexpected merges.
 case x =>
 val oldStrategy = (assemblyMergeStrategy in assembly).value
 oldStrategy(x)
 },

 // Exclude the scala-library from the uberjar — it is provided by Spark.
 // Including it doubles startup time and triggers "incompatible Scala binary" warnings.
 assemblyOption in assembly :=
 (assemblyOption in assembly).value.copy(includeScala = false)
 ) 
```

> **Mastery Note:** The `ShadeRule.rename(...).inAll` directive is critical — without `.inAll`, only classes in the *directly matched dependency* are relocated, but transitive dependencies that call into Guava's API are not rewritten, leaving dangling references to the original `com.google.common` namespace that collide with Spark's Guava. The `META-INF/services` concat strategy is the most commonly forgotten: discarding SPI descriptors silently breaks Jackson's `ObjectMapper` module discovery, producing `InvalidDefinitionException` that manifests only when serializing certain types, not during `SparkSession` initialization. Always validate your assembly with `jar tf myapp-assembly.jar | grep -c "\.class"` — if the class count drops significantly between builds, a `MergeStrategy.discard` is silently removing class files.

---

### Example 2: Detecting and Diagnosing Classpath Conflicts at Runtime

> **What this demonstrates:** A programmatic approach to introspecting the classloader hierarchy at driver startup to detect version conflicts before they manifest as cryptic `NoSuchMethodError` or `ClassCastException` failures deep in task execution.

```text
import org.apache.spark.sql.SparkSession
import java.net.URLClassLoader

object ClasspathDiagnostics {

 def main(args: Array[String]): Unit = {

 val spark = SparkSession.builder()
 .appName("ClasspathDiagnostics")
 .getOrCreate()

 // --- Driver-side classloader introspection ---

 // Get the context classloader — on the driver this is Spark's
 // MutableURLClassLoader, which wraps the system classloader.
 val cl = Thread.currentThread().getContextClassLoader

 // Walk up the classloader hierarchy to understand delegation order.
 // Parent-first delegation means Spark's JARs are searched BEFORE your
 // uberjar's classes, which is the root cause of version conflict failures.
 def printClassloaderChain(loader: ClassLoader, depth: Int = 0): Unit = {
 if (loader != null) {
 val prefix = " " * depth + "├── "
 val urls = loader match {
 case ucl: URLClassLoader =>
 ucl.getURLs.map(_.toString).mkString(", ")
 case other => other.getClass.getName
 }
 println(s"$prefix${loader.getClass.getName}: $urls")
 printClassloaderChain(loader.getParent, depth + 1)
 }
 }

 println("=== Classloader Hierarchy (Driver) ===")
 printClassloaderChain(cl)

 // --- Detect which version of a class is actually loaded ---
 // This resolves the class and prints the JAR it was loaded from.
 // If it shows spark-assembly.jar instead of myapp-assembly.jar,
 // Spark's version is winning and your code may call wrong method signatures.
 def detectClassSource(className: String): Unit = {
 try {
 val clazz = Class.forName(className, false, cl)
 val source = clazz.getProtectionDomain.getCodeSource
 val location = if (source != null) source.getLocation.toString else "bootstrap/JDK"
 println(s"[OK] $className → $location")
 } catch {
 case _: ClassNotFoundException =>
 println(s"[MISSING] $className → NOT FOUND on classpath")
 }
 }

 println("\n=== Critical Class Source Resolution ===")
 // Check which Guava is loaded — Spark bundles guava-14, you may need guava-31.
 detectClassSource("com.google.common.collect.ImmutableMap")
 // Check Protobuf version — Spark 3.x bundles protobuf-3.21 internally but
 // older distributions ship 2.5; a version mismatch here corrupts Hive metastore RPCs.
 detectClassSource("com.google.protobuf.GeneratedMessageV3")
 // Check Jackson databind version — Spark bundles 2.13.x; mixing with 2.14.x
 // causes JsonMappingException on schema inference for complex nested types.
 detectClassSource("com.fasterxml.jackson.databind.ObjectMapper")

 // --- Executor-side Metaspace reporting ---
 // Spark's executor metrics include native memory stats; log them for sizing.
 val sc = spark.sparkContext
 sc.parallelize(Seq(1), numSlices = 1).foreachPartition { _ =>
 // Inside executor JVM: report Metaspace usage via JMX
 import java.lang.management.ManagementFactory
 ManagementFactory.getMemoryPoolMXBeans.forEach { pool =>
 if (pool.getName.contains("Metaspace")) {
 val used = pool.getUsage.getUsed / (1024 * 1024)
 val committed = pool.getUsage.getCommitted / (1024 * 1024)
 // This prints inside the executor log — look for it in YARN/K8s logs
 println(s"[EXECUTOR METASPACE] used=${used}MB committed=${committed}MB")
 }
 }
 }

 spark.stop()
 }
}
```

> **Mastery Note:** The `ProtectionDomain.getCodeSource.getLocation` pattern is the definitive way to determine which physical JAR a loaded class originates from — far more reliable than inspecting the classpath string, which only shows what was *offered* to the classloader, not what was *actually loaded*. When `detectClassSource("com.google.common.collect.ImmutableMap")` returns a path to `spark-assembly.jar` instead of your `myapp-assembly.jar`, you have confirmed that Spark's Guava is winning the classloader race. The correct fix is to shade your Guava dependency in sbt-assembly, not to reorder the classpath — classpath ordering is fragile and silently overridden by Spark's internal `URLClassLoader` construction logic in `SparkEnv`. The executor Metaspace JMX probe via `foreachPartition` is a production-grade technique for diagnosing container OOM kills before they happen; if the reported Metaspace usage exceeds 400MB, increase `spark.executor.memoryOverheadFactor` to at least 0.25.

---

### Example 3: Configuring Kryo Serialization to Survive Uberjar Classloading

> **What this demonstrates:** The interaction between uberjar classloading and Kryo serialization — specifically, how an incorrectly configured Kryo falls back to Java serialization silently when class registration fails due to classloader context mismatches, and how to prevent it.

```python
# PySpark version — demonstrates the Kryo configuration and a validation pattern.
# The driver submits class registrations; executors must resolve them via the
# SAME classloader that loaded the uberjar, or Kryo falls back to Java serialization.

from pyspark.sql import SparkSession
from pyspark import SparkConf

def build_spark_session() -> SparkSession:
 conf = SparkConf()

 # Use Kryo serialization instead of Java serialization for shuffle data.
 # Kryo is 3-10x faster and produces 50-80% smaller shuffle blocks,
 # directly reducing GC pressure on the executor heap during shuffles.
 conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

 # Register all application-specific classes upfront.
 # Unregistered classes trigger Kryo's fallback serialization path,
 # which uses class name strings (expensive) and may fail across classloader
 # boundaries when the uberjar's ExecutorClassLoader differs from the
 # context classloader Kryo uses for Class.forName() resolution.
 conf.set(
 "spark.kryo.classesToRegister",
 ",".join([
 "com.example.model.SensorReading",
 "com.example.model.AggregatedMetric",
 "com.example.model.DeviceMetadata",
 # Always register array types explicitly — Kryo does not auto-register
 # primitive array wrappers when your uberjar contains a shaded Kryo.
 "[Lcom.example.model.SensorReading;",
 ])
 )

 # CRITICAL: set to True to FAIL FAST if a class is unregistered,
 # rather than silently falling back to Java serialization.
 # In production, this surfaces missing registrations during integration tests
 # rather than manifesting as unexplained shuffle slowdowns on the cluster.
 conf.set("spark.kryo.registrationRequired", "true")

 # When using a shaded uberjar that relocates Kryo itself (e.g., shaded.com.esotericsoftware.kryo),
 # you must tell Spark which KryoRegistrator class to use from the shaded namespace.
 # If this is not set and Kryo is shaded, the executor loads TWO Kryo instances —
 # Spark's built-in and your shaded one — which cannot serialize into each other's buffers.
 conf.set(
 "spark.kryo.registrator",
 "com.example.serialization.AppKryoRegistrator" # see registrator below
 )

 # Increase the Kryo buffer size to accommodate large domain objects.
 # Default is 64KB; if a serialized object exceeds this, Kryo throws
 # "Buffer overflow" which is misdiagnosed as a data problem, not a config problem.
 conf.set("spark.kryoserializer.buffer.max", "256m")

 # Executor memory overhead: accounts for Metaspace (loaded from uberjar)
 # + Kryo off-heap buffers + JVM internal structures.
 # For a 60MB uberjar on K8s, 0.25 overhead factor prevents OOMKilled.
 conf.set("spark.executor.memoryOverheadFactor", "0.25")

 return (
 SparkSession.builder()
 .appName("UberjarKryoDemo")
 .config(conf=conf)
 .getOrCreate()
 )

def validate_kryo_serialization(spark: SparkSession) -> None:
 """
 Sends a known object through the Spark serialization pipeline (driver → executor → driver)
 to verify Kryo can resolve classes via the uberjar's ExecutorClassLoader.
 If registrationRequired=True and this succeeds, Kryo is correctly configured.
 If it raises SerializationException, a class registration is missing.
 """
 sc = spark.sparkContext

 # Create a simple RDD of tuples and force a shuffle (reduceByKey) to exercise Kryo.
 # The shuffle write path serializes the data with Kryo on the executor;
 # the shuffle read path deserializes it. Both sides must see the same class definitions.
 test_data = [("sensor_a", 1.0), ("sensor_b", 2.5), ("sensor_a", 3.1)]
 rdd = sc.parallelize(test_data, numSlices=3)

 # reduceByKey forces a full shuffle: map-side combine → sort → write → fetch → reduce.
 # This exercises the entire Kryo serialization round-trip across executor boundaries.
 result = rdd.reduceByKey(lambda a, b: a + b).collect()
 print(f"Kryo serialization validated. Results: {result}")

if __name__ == "__main__":
 spark = build_spark_session()
 validate_kryo_serialization(spark)
 spark.stop()
```

> **Mastery Note:** Setting `spark.kryo.registrationRequired=true` is the single most impactful configuration change for uberjar deployments because it converts a silent performance degradation into a loud, immediate failure. Without it, Kryo silently falls back to Java serialization for unregistered classes — a fallback that is up to 10× slower, produces shuffle blocks 3–5× larger, and can cause `java.io.NotSerializableException` only when a non-serializable field is actually traversed, which may only happen on certain data distributions. The classloader context problem for Kryo is subtle: `Class.forName(name)` inside Kryo's default deserializer uses `Thread.currentThread().getContextClassLoader()`, which on executors is the system classloader — *not* the `ExecutorClassLoader` that loaded your uberjar. If your domain class `com.example.model.SensorReading` is in the uberjar, this call throws `ClassNotFoundException`. The fix is always to implement a custom `KryoRegistrator` that calls `kryo.register(classOf[SensorReading])` using the class literal, which is resolved at compile time against the correct classloader context.

---

### Example 4: Advanced — Diagnosing and Tuning Metaspace for Large Uberjars on Kubernetes

> **What this demonstrates:** Production-grade JVM tuning directives and a Kubernetes pod template configuration that correctly sizes native memory overhead for a large uberjar, preventing the OOMKilled failures that look identical to heap OOM in cluster logs.

```yaml
# kubernetes-executor-pod-template.yaml
# Applied via spark.kubernetes.executor.podTemplateFile
# This template configures Metaspace bounds and GC tuning for a large uberjar deployment.

apiVersion: v1
kind: Pod
metadata:
 name: spark-executor-template
spec:
 containers:
 - name: spark-kubernetes-executor
 # Resource requests and limits must account for:
 # - Heap (spark.executor.memory)
 # - Metaspace (class definitions from uberjar — NOT part of heap)
 # - Tungsten off-heap (spark.memory.offHeap.size)
 # - JVM internal structures, code cache, JIT-compiled methods (~100-200MB)
 # - Kryo serialization buffers (spark.kryoserializer.buffer.max)
 resources:
 requests:
 memory: "6Gi" # 4GB heap + ~2GB native (Metaspace + overhead)
 cpu: "2"
 limits:
 memory: "6Gi" # Hard limit: exceeding this triggers OOMKilled (exit 137)
 cpu: "4"
 env:
 - name: SPARK_EXECUTOR_JAVA_OPTS
 value: >-
 -XX:+UseG1GC
 -XX:G1HeapRegionSize=16m
 -XX:+UseStringDeduplication

 -XX:MaxMetaspaceSize=600m
 -XX:MetaspaceSize=256m
 -XX:+CMSClassUnloadingEnabled

 -XX:ReservedCodeCacheSize=256m

 -XX:+PrintGCDetails
 -XX:+PrintGCDateStamps
 -Xloggc:/var/log/spark/gc-executor.log

 -Dcom.sun.management.jmxremote
 -Dcom.sun.management.jmxremote.port=9010
 -Dcom.sun.management.jmxremote.authenticate=false
 -Dcom.sun.management.jmxremote.ssl=false
```

```scala
// Scala spark-submit configuration companion to the pod template above.
// These settings must be consistent with the pod template's resource limits.

object UberjarK8sSubmit {

 // Build a SparkSession with Kubernetes-optimized settings for a large uberjar.
 def configuredSession(): SparkSession = {
 SparkSession.builder()
 .appName("LargeUberjarK8sJob")
 .master("k8s://https://k8s-api-server:6443")
 .config("spark.kubernetes.container.image", "myregistry/spark:3.3.2")

 // Point to the pod template above — this merges our JVM flags into every executor pod.
 .config("spark.kubernetes.executor.podTemplateFile", "/conf/executor-pod-template.yaml")

 // 4GB heap per executor. Combined with the 600MB MaxMetaspaceSize, 256MB CodeCache,
 // and ~200MB JVM overhead, total RSS per executor is ~5.1GB — within the 6GB limit.
 .config("spark.executor.memory", "4g")

 // memoryOverhead is added on TOP of executor.memory by the K8s scheduler.
 // Setting it explicitly overrides the default formula (10% of executor memory)
 // which would produce only 400MB — insufficient for a large uberjar's Metaspace.
 // 1536MB = 600MB Metaspace + 256MB CodeCache + 200MB JVM + 480MB buffer.
 .config("spark.executor.memoryOverhead", "1536m")

 // Tungsten off-heap: disabled here since total memory is already at limit.
 // Enable only if executor memory > 8GB and shuffle data dominates the workload.
 .config("spark.memory.offHeap.enabled", "false")

 // Dynamic allocation: when enabled, new executor pods are launched on-demand.
 // Each new pod must fetch the uberjar fresh from the staging area.
 // With fetchCache=true, pods on the same K8s node share the downloaded JAR,
 // reducing repeated downloads from HDFS and cutting executor startup from
 // ~30 seconds to ~3 seconds on subsequent allocations on the same node.
 .config("spark.dynamicAllocation.enabled", "true")
 .config("spark.dynamicAllocation.minExecutors", "2")
 .config("spark.dynamicAllocation.maxExecutors", "50")
 .config("spark.files.useFetchCache", "true")

 // Shuffle service for dynamic allocation on K8s — required since executors
 // can be decommissioned before shuffle data is consumed.
 .config("spark.shuffle.service.enabled", "false") // use external shuffle service or RSSs
 .config("spark.kubernetes.shuffle.namespace", "spark-shuffle")

 // Force class unloading when executor idles — reclaims Metaspace
 // for classes loaded during job phases that have completed.
 .config("spark.executor.extraJavaOptions",
 "-XX:+ClassUnloading -XX:+ClassUnloadingWithConcurrentMark")

 .getOrCreate()
 }
}
```

> **Mastery Note:** The most critical insight here is that `spark.executor.memoryOverhead` is **not** the same as Metaspace — it is Kubernetes's *container memory reservation above the heap*, and you must manually ensure it is large enough to cover Metaspace, CodeCache, JVM internals, and native library allocations simultaneously. A common production mistake is setting `MaxMetaspaceSize=600m` in the JVM flags but leaving `memoryOverhead` at the default 400MB — the JVM will grow Metaspace up to 600MB, the container's total RSS will exceed the limit, and Kubernetes will issue `OOMKilled` before the JVM can even log an error. The `spark.files.useFetchCache=true` directive is specifically valuable in dynamic allocation scenarios: without it, each of the 50 executor pods downloads a 70MB uberjar independently from HDFS, consuming 3.5GB of network bandwidth and 50–90 seconds of startup latency per scale-out event. With fetch cache enabled, pods co-scheduled on the same node share a single on-disk copy, reducing scale-out latency by 90% and HDFS read load proportionally to the number of executors per node.

---

## 🎯 Mastery Checklist

To achieve true mastery of Uberjars in Apache Spark:

- [ ] Understand the `MutableURLClassLoader` → `ExecutorClassLoader` delegation chain and how parent-first resolution causes version conflict failures
- [ ] Know when shading outperforms exclusion: shade when you need your version of a library that conflicts with Spark's internal usage; exclude when you can tolerate Spark's version
- [ ] Be able to diagnose `NoSuchMethodError` and `ClassCastException` from Spark UI executor logs by correlating them to `ProtectionDomain.getCodeSource` output
- [ ] Understand the tradeoff between `spark.executor.memoryOverhead` (K8s container budget) and `MaxMetaspaceSize` (JVM limit) — they are additive, not the same
- [ ] Know how `spark.kryo.registrationRequired=true` surfaces silent serialization fallbacks that inflate shuffle block sizes by 3–5×
- [ ] Understand how `spark.files.useFetchCache` interacts with dynamic allocation and node co-scheduling to reduce uberjar distribution latency
- [ ] Know the exact `MergeStrategy` required for `META-INF/services/*` (concat) and why discarding it silently breaks Jackson, Avro, and Java SPI-based components
- [ ] Be able to size `MaxMetaspaceSize` from `jar tf myapp-assembly.jar | grep -c "\.class"` output (roughly 8–12KB of Metaspace per class)
- [ ] Understand how Whole-Stage CodeGen's Janino-compiled classes add to Metaspace at runtime, independent of the uberjar's static class count

---

## 📚 Summary

Uberjars are not merely a packaging convenience — they are the primary mechanism by which Apache Spark achieves classpath determinism across a distributed cluster of heterogeneous JVMs. The fat JAR is uploaded once, distributed via the `BlockManager`'s file server to every executor's `ExecutorClassLoader`, and loaded into each JVM's Metaspace as a collection of `Klass` structures that persist for the lifetime of the executor process. Getting this distribution correct requires understanding the full chain from build tool (sbt-assembly or Maven Shade) through class relocation (shade rules), JAR manifest merging (merge strategies), executor classloading (parent delegation order), and Kubernetes resource sizing (memory overhead vs. MaxMetaspaceSize). 

The failure modes of a poorly assembled uberjar are among the most difficult to diagnose in production Spark: `NoSuchMethodError` appearing only on certain data distributions, `OOMKilled` pods with no JVM heap dumps, Kryo silently falling back to Java serialization and producing 5× larger shuffle blocks, and Spring or Jackson failing to discover SPI extensions because `META-INF/services` files were discarded at assembly time. Each of these failures has a specific, preventable root cause traceable to a single build configuration directive or JVM flag. 

Mastery of uberjars means treating JAR assembly as a first-class engineering concern with the same rigor applied to query optimization or shuffle tuning. This means: explicit shade rules for every dependency that conflicts with Spark's internal classpath, explicit merge strategies for every known file category, `registrationRequired=true` for Kryo, Metaspace-aware memory overhead sizing on Kubernetes, and automated validation of the assembled JAR's class count and SPI service files in CI pipelines — before the job reaches the cluster and fails in ways that cost hours to diagnose. 



<br><div style="font-size: 0.85rem; color: #64748b; border-top: 1px solid #334155; padding-top: 10px; margin-top: 20px;"><strong>Source References:</strong> <em>[Ref: 451](spark_book.pdf#page=451) [Ref: 455](spark_book.pdf#page=455) [Ref: 458](spark_book.pdf#page=458) [Ref: 462](spark_book.pdf#page=462) [Ref: 469](spark_book.pdf#page=469) [Ref: 452](spark_book.pdf#page=452) [Ref: 456](spark_book.pdf#page=456) [Ref: 459](spark_book.pdf#page=459) [Ref: 463](spark_book.pdf#page=463) [Ref: 470](spark_book.pdf#page=470) [Ref: 453](spark_book.pdf#page=453) [Ref: 457](spark_book.pdf#page=457) [Ref: 461](spark_book.pdf#page=461) [Ref: 464](spark_book.pdf#page=464)</em></div>
