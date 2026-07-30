# Spark Datasets Quiz

## Part 1: True/False Questions

1. **Question:** Catalyst operates differently on Datasets than on DataFrames at the logical plan level.
   **Answer:** False
   **Mastery Explanation:** DataFrames are simply `Dataset[Row]`. The logical plan operations for both are identical; Catalyst applies the same Analysis, Logical Optimization, and Physical Planning phases.

2. **Question:** `ExpressionEncoder` uses runtime reflection on every row to convert between JVM objects and Tungsten binary format.
   **Answer:** False
   **Mastery Explanation:** The `ExpressionEncoder` uses reflection only once at construction time to build a Catalyst expression tree. It generates bytecode (via Whole-Stage Code Gen) that accesses the `InternalRow` directly without per-row reflection.

3. **Question:** Using `.filter(_.salary > 100000)` on a Dataset guarantees a `DeserializeToObject` node will be inserted in the physical plan.
   **Answer:** True
   **Mastery Explanation:** Catalyst cannot introspect opaque Scala lambdas. It must promote the Tungsten off-heap row into a full JVM heap object (`DeserializeToObject`) before evaluating the lambda.

4. **Question:** Whole-Stage Code Generation (WSCG) cannot apply to the serialization/deserialization operators surrounding a lambda-based filter.
   **Answer:** False
   **Mastery Explanation:** WSCG still fuses the surrounding operators (including `DeserializeToObject` and `SerializeFromObject`) into a single Java class, but the lambda function itself remains a black box call inside that loop.

5. **Question:** Kryo serialization for a Dataset (e.g., using `Encoders.kryo[T]`) preserves schema visibility for Catalyst optimizations like column pruning.
   **Answer:** False
   **Mastery Explanation:** Kryo encodes the entire object as an opaque binary blob. The Dataset's schema collapses to a single `value: binary` column, eliminating any possibility of predicate pushdown or column pruning.

6. **Question:** Using `Dataset.joinWith` preserves both original objects intact in the output and avoids `DeserializeToObject` overhead compared to a standard DataFrame join.
   **Answer:** False
   **Mastery Explanation:** `joinWith` creates a `Dataset[(T, U)]`, a struct of two structs. It forces `DeserializeToObject` on *both* sides of the join output, significantly increasing GC overhead compared to a DataFrame join.

7. **Question:** A `TypedColumn` expression (e.g. `ds("salary").as[Int]`) allows Catalyst to apply column pruning and predicate pushdown.
   **Answer:** True
   **Mastery Explanation:** Because it's an expression tree rather than a black-box lambda, Catalyst can parse the exact columns needed and push the predicate down to the file format level (e.g., Parquet).

8. **Question:** `groupByKey().mapGroups()` uses Tungsten's `UnsafeExternalSorter` which spills to disk to prevent OOM errors for large groups.
   **Answer:** False
   **Mastery Explanation:** `mapGroups` exposes an `Iterator[T]` of deserialized JVM objects. If a developer calls `.toList`, it pulls all group records into the heap, risking OOM. The aggregation DSL uses Tungsten's disk-spilling sorter, not `mapGroups`.

9. **Question:** The DataFrame is technically defined as `Dataset[Row]` in the Spark source code.
   **Answer:** True
   **Mastery Explanation:** A DataFrame is literally a type alias (`type DataFrame = Dataset[Row]`) representing an untyped view over Spark's internal relational representation.

10. **Question:** `Encoders.bean()` uses Java reflection to construct a schema automatically and is backed by `GetStructField` / `Invoke` expressions.
    **Answer:** True
    **Mastery Explanation:** It bridges Java POJOs (with standard getters/setters) into Spark's Catalyst engine, creating a proper `ExpressionEncoder` that preserves schema visibility and Tungsten optimizations.

---

## Part 2: Multiple Choice Questions

11. **Question:** Which Catalyst physical plan node is inserted when using a typed Scala lambda for filtering?
    - A) `TypedFilter`
    - B) `UnsafeRowFilter`
    - C) `DeserializeToObject`
    - D) `LambdaEvaluationNode`
    **Answer:** C
    **Mastery Explanation:** Opaque Scala lambdas cannot be introspected by Catalyst, forcing Spark to materialize JVM objects via `DeserializeToObject` before the lambda runs, incurring a heavy GC tax.

12. **Question:** What happens to the logical plan when you call `.as[MyRecord]` on a DataFrame without performing any lambda-based transformations?
    - A) A full data copy is triggered into JVM objects.
    - B) The dataset schema is dropped and replaced with a binary BLOB.
    - C) No data movement occurs; it only swaps the Encoder on the same `InternalRow`.
    - D) The `SerializeFromObject` node is added immediately.
    **Answer:** C
    **Mastery Explanation:** The `.as[T]` cast is an O(1) metadata operation that registers the Encoder to validate schema at runtime; it does not scan, copy, or deserialize the underlying off-heap data.

13. **Question:** Why is `Dataset.filter($"salary" > 100000)` preferred over `Dataset.filter(_.salary > 100000)`?
    - A) The SQL expression avoids Whole-Stage Code Gen.
    - B) The Scala lambda forces `DeserializeToObject`, allocating one full JVM object per row.
    - C) The SQL expression requires Kryo serialization.
    - D) Scala lambdas cannot be executed on executors.
    **Answer:** B
    **Mastery Explanation:** The SQL column expression evaluates directly against Tungsten's `UnsafeRow` binary format. The lambda triggers full JVM object materialization for every row, massively increasing GC overhead.

14. **Question:** Which memory structure is utilized by the Tungsten execution engine for off-heap columnar storage?
    - A) `GenericRowWithSchema`
    - B) `UnsafeRow`
    - C) `ObjectArray`
    - D) `HeapByteBuffer`
    **Answer:** B
    **Mastery Explanation:** `UnsafeRow` is Tungsten's concrete `InternalRow` format backed by raw memory (`sun.misc.Unsafe`), allowing zero-deserialization data access.

15. **Question:** If a developer uses `kryo.register(classOf[MyRecord])` along with `Encoders.kryo[MyRecord]`, what is the consequence for Catalyst predicate pushdown?
    - A) Predicates are pushed down faster due to binary format.
    - B) Catalyst predicate pushdown is completely disabled.
    - C) Only integer predicates can be pushed down.
    - D) Pushdown works normally but requires a full shuffle.
    **Answer:** B
    **Mastery Explanation:** Kryo stores objects as opaque binary blobs. Spark sees this as `Dataset[binary]` and loses all field-level schema knowledge, making predicate pushdown into sources like Parquet impossible.

16. **Question:** In which scenario should you use `groupByKey().mapGroups()` instead of the standard aggregation DSL?
    - A) When calculating the sum of a column.
    - B) When calculating the mode of a column.
    - C) When the aggregation logic cannot be expressed as Spark built-in column operations.
    - D) When dealing with massive groups to prevent OOM.
    **Answer:** C
    **Mastery Explanation:** `mapGroups` incurs massive performance penalties (deserialization and OOM risks). It should strictly be used when complex, stateful, or non-relational logic (e.g., custom ML scoring per group) cannot be mapped to the DSL.

17. **Question:** What typically causes an `AnalysisException: No encoder found` error at runtime when creating a Dataset?
    - A) Missing `spark.sql.encoder.enabled` configuration.
    - B) Attempting to encode a generic type with type erasure or a pure Scala class with private fields.
    - C) A typo in a DataFrame column name.
    - D) A cluster losing an executor.
    **Answer:** B
    **Mastery Explanation:** `ExpressionEncoder` uses Scala reflection to build the struct mapper. It fails if the class structure cannot be introspected, such as having private constructors or missing JavaBean conventions.

18. **Question:** How does Whole-Stage Code Generation (WSCG) treat a typed Scala lambda function (e.g., `_.age > 25`) inside a filter?
    - A) It compiles the lambda into optimized off-heap binary operations.
    - B) It treats the lambda as a black box and surrounds it with serialize/deserialize operators.
    - C) It throws a compile-time error.
    - D) It pushes the lambda directly into the Parquet reader.
    **Answer:** B
    **Mastery Explanation:** WSCG optimizes the iteration and the conversion to/from objects, but the lambda execution itself remains an opaque JVM method call surrounded by `DeserializeToObject` and `SerializeFromObject`.

19. **Question:** What is the fundamental problem with using `mapGroups` on a dataset with severely skewed group sizes?
    - A) It limits the number of partitions generated.
    - B) It triggers a SortMergeJoin instead of a BroadcastHashJoin.
    - C) Calling `.toList` on the iterator materializes all rows for a group on the heap simultaneously, causing an OutOfMemoryError.
    - D) It writes too many small files to disk.
    **Answer:** C
    **Mastery Explanation:** Unlike the aggregation DSL, which uses disk-spilling Tungsten sorters, `mapGroups` hands the user an `Iterator`. If the group doesn't fit in memory and the user materializes it, the executor crashes.

20. **Question:** When converting a Dataset pipeline from Scala lambdas to column expressions, what happens to the GC (Garbage Collection) overhead?
    - A) It remains the same as objects still need to be stored in JVM heap.
    - B) It decreases significantly because column expressions operate directly on off-heap `UnsafeRow` memory.
    - C) It increases because column expressions require more reflection.
    - D) It throws an OOM error faster.
    **Answer:** B
    **Mastery Explanation:** Column expressions bypass the JVM heap entirely. The execution engine reads raw bytes off-heap, meaning no short-lived JVM objects are created, drastically dropping GC times.

21. **Question:** What is the primary difference in the output of `joinWith` compared to a standard `join` followed by `.as[T]`?
    - A) `joinWith` returns a `Dataset[(T, U)]` forcing both objects to be deserialized, while `join` + `as` returns `Dataset[T]` preserving Tungsten format.
    - B) `joinWith` operates purely off-heap.
    - C) `joinWith` supports Broadcast Hash Joins while `join` does not.
    - D) There is no difference; they produce identical physical plans.
    **Answer:** A
    **Mastery Explanation:** `joinWith` guarantees you retain the original typed objects on both sides, outputting a tuple. Accessing this tuple forces full JVM deserialization for both sides of the join.

22. **Question:** Which encoder should be used for a Java POJO containing standard getter and setter methods?
    - A) `Encoders.product`
    - B) `Encoders.kryo`
    - C) `Encoders.bean`
    - D) `Encoders.javaSerialization`
    **Answer:** C
    **Mastery Explanation:** `Encoders.bean()` is specifically designed to use Java reflection to map standard getter/setter pairs into a Catalyst `ExpressionEncoder`, preserving schema optimizations.

23. **Question:** What happens when a Dataset aggregation is performed using the built-in DSL (e.g., `.agg(sum($"amount"))`)?
    - A) Data is fully deserialized into JVM objects.
    - B) A hash-aggregate physical plan is generated over `UnsafeRow` utilizing Tungsten's off-heap sorter, which can spill to disk.
    - C) The dataset throws an `AnalysisException`.
    - D) Spark converts it to an RDD implicitly.
    **Answer:** B
    **Mastery Explanation:** The DSL aggregation executes entirely in the off-heap columnar domain. It is immune to OOM errors from skewed groups because Tungsten gracefully spills to disk.

24. **Question:** Which configuration controls whether Spark's internal Tungsten sorter spills to disk during aggregations or sorts?
    - A) `spark.sql.autoBroadcastJoinThreshold`
    - B) `spark.serializer`
    - C) `spark.memory.fraction` and `spark.sql.shuffle.partitions`
    - D) `spark.driver.memory`
    **Answer:** C
    **Mastery Explanation:** Memory limits for off-heap execution are governed by the execution memory fraction (`spark.memory.fraction`). When this limit is hit, the sorter spills chunks to disk based on the configured partitions.

25. **Question:** How do you instruct Catalyst to broadcast a table that is larger than the `autoBroadcastJoinThreshold` but still fits in executor memory?
    - A) Use `.hint("BROADCAST")` on the Dataset.
    - B) Use `dataset.repartition(1)`.
    - C) Set `spark.sql.join.preferSortMergeJoin` to false.
    - D) Convert the Dataset to an RDD and use `Broadcast[T]`.
    **Answer:** A
    **Mastery Explanation:** The `.hint("BROADCAST")` overrides Catalyst's size estimates and forces a BroadcastHashJoin plan regardless of the `autoBroadcastJoinThreshold`.

---

## Part 3: "Small Twist" Questions

26. **Question:** Developer A writes `ds.filter($"age" > 25)`. Developer B writes `ds.filter(col("age") > 25)`. Developer C writes `ds.filter(_.age > 25)`. Which developer will experience a `DeserializeToObject` node in their physical plan?
    **Answer:** Developer C
    **Mastery Explanation:** A and B use identical Catalyst column expressions (string interpolation vs explicit `col()`), executing off-heap. Developer C uses a typed lambda, breaking Catalyst introspection and forcing object materialization.

27. **Question:** You have a Dataset with a `List[String]` field. You use `.as[MyClass]` where `MyClass` expects an `Array[String]`. Will this fail at compile-time or runtime?
    **Answer:** Runtime
    **Mastery Explanation:** While Datasets provide compile-time safety for *subsequent* lambda operations, the initial `.as[T]` cast validates the untyped DataFrame schema against the case class at runtime, throwing an `AnalysisException` instantly.

28. **Question:** You switch from `spark.serializer = org.apache.spark.serializer.JavaSerializer` to `KryoSerializer`. You notice your `.filter($"salary" > 100000)` performance does not change. Why?
    **Answer:** `ExpressionEncoder` is independent of `spark.serializer`.
    **Mastery Explanation:** Column expressions and standard case class Encoders always use Catalyst's generated code for binary formatting. Kryo only impacts task closure serialization and shuffle RDD formats, not Tungsten row execution.

29. **Question:** You have `ds.joinWith(ds2, ...)`. To avoid the `DeserializeToObject` penalty, you add `.select("_1.id", "_2.name")` immediately after. Does this prevent object materialization?
    **Answer:** No.
    **Mastery Explanation:** `joinWith` generates a logical plan explicitly requesting a `Dataset[(T, U)]`. `DeserializeToObject` is injected *before* the `.select` runs, meaning the massive GC penalty is already paid.

30. **Question:** You use `Encoders.kryo[ComplexState]` to store complex objects. You then attempt to write the Dataset to Parquet format. What does the Parquet schema look like?
    **Answer:** A single column named `value` of type `binary`.
    **Mastery Explanation:** The Kryo encoder collapses the entire object graph into an opaque byte array. Catalyst cannot read the internal fields, so Parquet just gets a binary BLOB column.

31. **Question:** You are processing 50M records per group. You switch from `.groupByKey(_.region).mapGroups(...)` to `.groupByKey(_.region).flatMapGroups(...)`. Will this prevent an OutOfMemoryError?
    **Answer:** No.
    **Mastery Explanation:** Both methods yield an `Iterator[T]` of deserialized JVM objects. If the lambda body attempts to materialize the iterator (e.g., via `.toList` or `.toSeq`), it will still crash the heap.

32. **Question:** You have `val ds = df.as[Employee]`. You then call `ds.select($"id", $"name")`. Does the output remain a `Dataset[Employee]`?
    **Answer:** No, it becomes a `DataFrame` (i.e., `Dataset[Row]`).
    **Mastery Explanation:** `.select()` alters the schema. Since the output schema no longer matches the full `Employee` case class structure, Spark dynamically downcasts the type back to `Row`.

33. **Question:** You configure `spark.sql.autoBroadcastJoinThreshold = -1`. You then use `.hint("BROADCAST")` on a 5GB Dataset before joining. What happens?
    **Answer:** The table is broadcasted (risking OOM on the driver/executors).
    **Mastery Explanation:** A threshold of `-1` disables *automatic* broadcasting. However, `.hint("BROADCAST")` explicitly forces it, overriding the threshold configuration entirely.

34. **Question:** You register a case class with `kryo.register(classOf[MyRecord])`. Then you use `ds.filter(_.salary > 100)`. Does the filter lambda use Kryo or Catalyst serialization to read the row?
    **Answer:** Catalyst serialization.
    **Mastery Explanation:** Converting the `InternalRow` back to `MyRecord` for the lambda evaluation relies entirely on `ExpressionEncoder`'s generated `DeserializeToObject` routine. Kryo is only used to serialize the lambda itself for shipping across the network.

35. **Question:** A developer replaces `.dropDuplicates()` with `.groupByKey(identity).mapGroups((k, iter) => iter.next())`. How does memory pressure change?
    **Answer:** Memory pressure skyrockets.
    **Mastery Explanation:** `.dropDuplicates()` utilizes a highly optimized shuffle-based hash aggregation directly on off-heap `UnsafeRow`. The `mapGroups` rewrite forces a complete serialization/deserialization cycle and object allocation for every row in the dataset.

36. **Question:** A query plan shows `SortMergeJoin`. You run `ANALYZE TABLE COMPUTE STATISTICS` and the plan suddenly changes to `BroadcastHashJoin` without any code changes. Why?
    **Answer:** The table size estimate was stale.
    **Mastery Explanation:** Catalyst relies on table statistics to determine if a table is below `autoBroadcastJoinThreshold`. Without recent stats, Spark overestimates the size and falls back to a safer, more expensive SortMergeJoin.

37. **Question:** You execute `ds.map(e => e.copy(salary = e.salary * 1.1))`. You then rewrite it as `ds.withColumn("salary", $"salary" * 1.1).as[Employee]`. What physical plan node is eliminated?
    **Answer:** `DeserializeToObject` (and `SerializeFromObject`).
    **Mastery Explanation:** The `.withColumn` rewrite shifts the logic from a black-box Scala lambda to a Catalyst column expression, allowing Spark to manipulate the off-heap binary data directly.

38. **Question:** You have a `Dataset[Address]` where `Address(city: String, zip: String)` has a private constructor. What error do you encounter?
    **Answer:** `AnalysisException: No encoder found`.
    **Mastery Explanation:** The `ExpressionEncoder` uses Scala reflection to build the internal mapping. If the constructor is private, the reflection mechanism fails to generate the deserializer expression.

39. **Question:** You use `spark.createDataset(Seq(Employee(...)))`. Is Tungsten binary format bypassed entirely since the data starts as local JVM objects?
    **Answer:** No.
    **Mastery Explanation:** `createDataset` triggers the `ExpressionEncoder` immediately. It serializes the local JVM objects into Tungsten's `InternalRow` format prior to distributed execution on the executors.

40. **Question:** You have a `Dataset[Employee]` and call `ds.orderBy($"salary".desc)`. Are the `Employee` objects deserialized to perform the sort?
    **Answer:** No.
    **Mastery Explanation:** The sort operation utilizes column expressions. Tungsten's `UnsafeExternalSorter` executes the sort purely on the `UnsafeRow` binary buffers off-heap.

---

## Part 4: Coding & Debugging Questions

41. **Identify the Memory Leak:**
```scala
val result = ds.groupByKey(_.department).mapGroups { (dept, emps) =>
  val empList = emps.toList
  DepartmentStats(dept, empList.map(_.salary).sum)
}
```
**Answer & Mastery Explanation:**
Calling `emps.toList` pulls every employee for a department into the executor's JVM heap simultaneously. If "department" is heavily skewed, this instantly triggers an `OutOfMemoryError`.
**Fix:** Use standard aggregation DSL: `ds.groupBy($"department").agg(sum($"salary"))`.

42. **Identify the Optimizer Blocker:**
```scala
val activeUsers = usersDS.filter(u => u.isActive == true && u.age > 18)
```
**Answer & Mastery Explanation:**
The typed lambda `u => ...` prevents Catalyst from inspecting the predicate. It disables Parquet predicate pushdown, prevents partition pruning, and forces `DeserializeToObject` on every row.
**Fix:** Replace with column expressions: `.filter($"isActive" === true && $"age" > 18)`.

43. **Identify the Serialization Error:**
```scala
class ConnectionConfig(url: String)
case class User(id: Int, name: String)
val config = new ConnectionConfig("jdbc:...")
val enriched = usersDS.map(u => u.name + config.url)
```
**Answer & Mastery Explanation:**
`ConnectionConfig` does not implement `Serializable`. Because it is referenced inside the `.map` lambda, Spark tries to serialize it into the task closure to ship to the executors, throwing a `NotSerializableException`.
**Fix:** Change to `class ConnectionConfig(url: String) extends Serializable`, or broadcast the config.

44. **Identify the Join Trap:**
```scala
val ds1 = spark.read.parquet("...").as[A]
val ds2 = spark.read.parquet("...").as[B]
val joined = ds1.joinWith(ds2, ds1("id") === ds2("id")).select("_1.name", "_2.value")
```
**Answer & Mastery Explanation:**
`joinWith` creates a tuple dataset, triggering `DeserializeToObject` for both sides. Immediately following it with `.select` discards the objects anyway, meaning the massive GC penalty was incurred for zero benefit.
**Fix:** Use a standard DataFrame join: `ds1.join(ds2, Seq("id")).select($"name", $"value")`.

45. **Identify the Encoder Issue:**
```scala
class UserStats(val id: Int, val visits: Int)
val ds = df.as[UserStats]
```
**Answer & Mastery Explanation:**
`UserStats` is a standard Scala class (not a `case class`) and lacks JavaBean getter/setter conventions. The `ExpressionEncoder` fails to map this, throwing `AnalysisException: No encoder found`.
**Fix:** Change it to a `case class UserStats(...)`.

46. **Identify the Logical Bug:**
```scala
val ds = df.as[Record]
ds.select($"id", $"value" * 2).as[Record]
```
**Answer & Mastery Explanation:**
The expression `$"value" * 2` creates an anonymous column name (like `(value * 2)`). The subsequent `.as[Record]` throws a runtime `AnalysisException` because it cannot find a column named `value` to map back to the case class.
**Fix:** Alias the column: `($"value" * 2).as("value")`.

47. **Optimize this code:**
```scala
implicit val enc = Encoders.kryo[MyState]
val ds = spark.read.parquet("path").as[MyState]
ds.filter(col("status") === "ACTIVE")
```
**Answer & Mastery Explanation:**
Kryo collapses the schema into a single `value: binary` column. The `.filter(col("status")...)` will fail because Catalyst no longer knows what "status" is.
**Fix:** Do not use Kryo if you need columnar operations. Use a standard `Product` encoder (case class) to maintain schema visibility.

48. **Identify the Performance Degradation:**
```scala
val parsed = rawDS.map(r => r.copy(timestamp = parseDate(r.dateString)))
```
**Answer & Mastery Explanation:**
The `.map` with a case class `copy` forces full object materialization via `DeserializeToObject`. This dramatically inflates GC pressure compared to native off-heap processing.
**Fix:** Shift to a column expression: `rawDS.withColumn("timestamp", to_date($"dateString")).as[NewCaseClass]`.

49. **Identify the Missing Broadcast:**
```scala
val smallLookupDS = spark.read.parquet("small_lookup").as[Lookup]
val largeDS = spark.read.parquet("large_data").as[Data]
val result = largeDS.join(smallLookupDS, Seq("key")).as[Enriched]
```
**Answer & Mastery Explanation:**
Reading directly from raw Parquet files means Spark lacks table statistics. Catalyst might default to a SortMergeJoin, causing a massive unnecessary network shuffle for the lookup table.
**Fix:** Provide a manual hint: `largeDS.join(smallLookupDS.hint("BROADCAST"), Seq("key"))`.

50. **Identify the OOM Risk:**
```scala
val grouped = ds.groupByKey(_.customerId)
val result = grouped.mapGroups { (id, records) =>
  val sorted = records.toSeq.sortBy(_.timestamp)
  ProcessResult(id, sorted.head, sorted.last)
}
```
**Answer & Mastery Explanation:**
Calling `.toSeq.sortBy` pulls all elements for a single `customerId` into an in-memory Scala collection. A heavily skewed customer will blow out the executor heap.
**Fix:** Use standard aggregation DSL with Tungsten's disk-spilling sorters: `ds.groupBy($"customerId").agg(min($"timestamp"), max($"timestamp"))`.
