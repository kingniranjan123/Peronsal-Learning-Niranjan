# 🔥 Master Class: Accumulators Assessment

## Part 1: True/False Questions (10 Questions)

1. **True/False**: Accumulators guarantee exact counting semantics even when task retries occur due to node failures.
   - **Answer**: False.
   - **Mastery Explanation**: Accumulators are not idempotent. If a task fails after committing partial work and shipping its delta to the driver, or if transient failures cause stage retries, the driver may apply the same task's delta multiple times.

2. **True/False**: In a custom `AccumulatorV2` implementation, the `add()` method executes on the executor JVM, while the `merge()` method executes on the driver JVM.
   - **Answer**: True.
   - **Mastery Explanation**: Executors invoke `add()` to update their task-local copies. Once a task completes, the serialized delta is sent to the driver, which calls `merge()` on the canonical accumulator instance to fold the task's state in.

3. **True/False**: An accumulator's value can be safely read on an executor JVM during a `foreachPartition` action.
   - **Answer**: False.
   - **Mastery Explanation**: Accumulators are write-only on the executors. The canonical value is only maintained on the driver JVM and can only be safely read there after the action completes.

4. **True/False**: Spark automatically calls `reset()` on all registered accumulators between stage boundaries to clear their values.
   - **Answer**: False.
   - **Mastery Explanation**: `reset()` is not automatically called between stages unless explicitly invoked by the developer. Accumulator state persists across actions unless manually reset.

5. **True/False**: Accumulators are serialized and shipped to executors as complete objects containing the accumulated driver-side value.
   - **Answer**: False.
   - **Mastery Explanation**: Executors do not receive the full accumulator object with the driver's accumulated value. They only receive the accumulator ID, its zero value, and the update logic via `copyAndReset()` at task launch.

6. **True/False**: Accumulator updates performed inside transformations like `map` or `filter` might never be executed if an action is not called.
   - **Answer**: True.
   - **Mastery Explanation**: Transformations are lazy. If no action forces evaluation, the DAG is never executed, and the accumulator updates inside the transformations are never triggered.

7. **True/False**: Accumulator deltas from executors are shipped back to the driver via shuffle files.
   - **Answer**: False.
   - **Mastery Explanation**: Accumulator deltas are shipped back to the driver as part of the `DirectTaskResult` payload over Netty RPC, not via shuffle files. This means zero shuffle overhead.

8. **True/False**: When caching an RDD, subsequent actions on the cached RDD will re-trigger the accumulator updates defined in the transformations preceding the cache.
   - **Answer**: False.
   - **Mastery Explanation**: If an RDD is cached, the accumulator is only updated during the first action that materializes the cache. Subsequent actions read from the cache, bypassing the transformations (and their `add()` calls) entirely.

9. **True/False**: Speculative execution (`spark.speculation=true`) can cause an accumulator's value to be double-counted if both the original and speculative tasks complete successfully.
   - **Answer**: True.
   - **Mastery Explanation**: If both tasks complete and send their `DirectTaskResult` back, the `DAGScheduler` applies both deltas to the canonical accumulator. It does not retroactively subtract the slower task's delta when it kills it.

10. **True/False**: The internal registry `AccumulatorContext` on the driver uses strong references to prevent accumulators from being garbage collected.
    - **Answer**: False.
    - **Mastery Explanation**: `AccumulatorContext` is backed by a `WeakHashMap`. This ensures that unreferenced accumulators are garbage collected without leaking memory in long-running driver JVMs.

## Part 2: Multiple Choice Questions (15 Questions)

11. **Where does Spark store the canonical instance of an accumulator that holds the merged total?**
    a) In the BlockManager of every executor.
    b) In the `AccumulatorContext` on the driver JVM.
    c) In the `TaskMetrics` object of the `TaskContext`.
    d) Distributed across shuffle files.
    - **Answer**: b
    - **Mastery Explanation**: The canonical accumulator instance resides exclusively on the driver JVM inside the `AccumulatorContext`, which is a global registry. Executors only hold task-local copies.

12. **Which method is responsible for initializing a task-local accumulator copy on the executor?**
    a) `merge()`
    b) `reset()`
    c) `copyAndReset()`
    d) `isZero()`
    - **Answer**: c
    - **Mastery Explanation**: At task launch, Spark calls `copyAndReset()` (which internally calls `copy()` followed by `reset()`) to initialize a fresh, isolated, zero-state accumulator for the task.

13. **Why is it dangerous to rely on an accumulator for exact billing counts inside a `.map()` transformation?**
    a) `map` operations are parallelized and accumulators are not thread-safe.
    b) `map` operations do not support accumulators.
    c) Transformations are lazy, and retries/recomputations will cause double-counting.
    d) Accumulators in `map` trigger expensive shuffle operations.
    - **Answer**: c
    - **Mastery Explanation**: The "lazy semantics trap" and the lack of idempotency mean transformations can be evaluated multiple times due to node failures or recomputation of evicted cached partitions, inflating the accumulator value.

14. **How are accumulator deltas transmitted from executors to the driver?**
    a) As part of the `DirectTaskResult` payload over Netty RPC.
    b) Written to HDFS and read by the driver.
    c) Through the Spark Broadcast variable mechanism.
    d) Using JDBC connections to the Spark SQL engine.
    - **Answer**: a
    - **Mastery Explanation**: Executors serialize the accumulator deltas and ship them back to the driver as part of the `DirectTaskResult` payload over Netty RPC upon task completion.

15. **What happens if a task produces a large result exceeding `spark.driver.maxResultSize` and returns an `IndirectTaskResult`?**
    a) The accumulator deltas are lost.
    b) The accumulator deltas are stored in a BlockManager block.
    c) The accumulator deltas are still included inline in the `DirectTaskResult`.
    d) The accumulator throws an `OutOfMemoryError`.
    - **Answer**: c
    - **Mastery Explanation**: Even for large results that fall back to `IndirectTaskResult` via BlockManager, the accumulator deltas are small enough to always be included in the inline `DirectTaskResult` payload.

16. **If you have a custom accumulator `StringSetAccumulator`, what happens if the driver calls `merge()` but 10,000 tasks each return a set of 10,000 elements?**
    a) The executors run out of memory.
    b) The driver experiences a serial bottleneck and significant GC pressure.
    c) Spark automatically triggers a shuffle to distributed the merge.
    d) The `merge()` method is skipped.
    - **Answer**: b
    - **Mastery Explanation**: `merge()` executes sequentially on the driver. O(T * |set|) merge cost means the driver must perform 10,000 sequential `HashSet` union operations, causing massive heap utilization and GC pauses.

17. **What is the purpose of the `isZero` method in `AccumulatorV2`?**
    a) To reset the accumulator to zero.
    b) To check if the driver accumulator has overflowed.
    c) To decide whether to ship the delta back to the driver to save bandwidth.
    d) To initialize a new accumulator instance.
    - **Answer**: c
    - **Mastery Explanation**: If `isZero` returns true on a task's accumulator copy, Spark elides it from the `DirectTaskResult`, saving serialization cost and network bandwidth.

18. **Which Spark UI tab natively supports displaying accumulator values alongside task metrics?**
    a) Storage
    b) Environment
    c) SQL
    d) Stages
    - **Answer**: d
    - **Mastery Explanation**: Registered accumulators appear in the Spark UI under the "Stages" tab, where their per-task delta contributions are displayed in the "Accumulables" section.

19. **What must you do before using a custom `AccumulatorV2` instance?**
    a) Call `.cache()` on it.
    b) Register it with the `SparkContext` using `sc.register()`.
    c) Broadcast it to all executors.
    d) Serialize it using Kryo.
    - **Answer**: b
    - **Mastery Explanation**: Every `AccumulatorV2` must be registered via `sc.register(myAccumulator, "name")` so that the `DAGScheduler` and `TaskScheduler` can track it and it appears in the UI.

20. **If you read `accumulator.value` immediately after defining a `.map()` that increments it (but before calling any action), what value will you get?**
    a) The total count of increments.
    b) Zero.
    c) A `NullPointerException`.
    d) The driver throws an `IllegalStateException`.
    - **Answer**: b
    - **Mastery Explanation**: Because `.map()` is a lazy transformation, no code has executed yet. The accumulator has not been touched, so reading its value returns zero.

21. **What is the recommended design pattern to track multiple metrics (e.g., read_count, parse_errors, nulls_skipped) in a Spark job?**
    a) Register 50 different `LongAccumulator` instances.
    b) Use a custom `AccumulatorV2[Map[String, Long], Map[String, Long]]`.
    c) Write the metrics to HDFS from the executor.
    d) Use standard DataFrame `.count()` operations for each metric.
    - **Answer**: b
    - **Mastery Explanation**: A multi-metric map accumulator avoids Spark UI clutter, reduces `DirectTaskResult` serialization payload size, and allows a single `merge` call to aggregate multiple metrics simultaneously.

22. **Why is `foreachPartition` considered the safest place to update an accumulator for exact counting?**
    a) It executes on the driver.
    b) It forces Spark to disable speculative execution.
    c) It is an action, and Spark guarantees each task's update is applied exactly once (barring task retries).
    d) It skips the Netty RPC serialization.
    - **Answer**: c
    - **Mastery Explanation**: `foreachPartition` is an action. The Spark documentation explicitly guarantees that accumulator updates performed inside actions only will be applied exactly once, though developers must still disable speculation and avoid retries for true exactly-once semantics.

23. **What is the internal backing structure of Spark's built-in `LongAccumulator` on the executor to handle concurrent task threads?**
    a) `LongAdder`
    b) `java.util.concurrent.atomic.AtomicLong`
    c) `synchronized` blocks
    d) `ThreadLocal` variables
    - **Answer**: b
    - **Mastery Explanation**: `LongAccumulator` uses `AtomicLong` internally, ensuring thread safety when multiple task threads within the same executor JVM concurrently update the local copy.

24. **In a Custom Accumulator, why must `value` return an immutable copy of the state?**
    a) To save memory on the driver.
    b) To prevent callers from accidentally mutating the internal canonical state after reading it.
    c) Because executors can only serialize immutable objects.
    d) To make it compatible with Kryo.
    - **Answer**: b
    - **Mastery Explanation**: If `value` returned a mutable reference (like a `mutable.HashSet`), user code running on the driver could modify it, corrupting the canonical metric tracker outside of Spark's controlled `merge()` lifecycle.

25. **How does structured streaming handle accumulators between micro-batch triggers?**
    a) It automatically resets them before every trigger.
    b) It throws an exception if an accumulator is used.
    c) It does NOT automatically reset them; the developer must manually call `reset()` in `foreachBatch`.
    d) It registers a new accumulator for every batch.
    - **Answer**: c
    - **Mastery Explanation**: Accumulators in structured streaming retain their state indefinitely unless manually reset. You must explicitly call `reset()` inside the `foreachBatch` handler to zero them out per-micro-batch.

## Part 3: Small Twist Questions (15 Questions)

26. **Twist**: Developer A calls `acc.add(1)` inside a `.map()`, then calls `.count()`. The result is 100. Developer B changes `.count()` to `.take(10)`. What is the accumulator's value now?
    - **Answer**: It will be less than 100 (typically 10, or slightly more depending on partition size).
    - **Mastery Explanation**: `.take(10)` optimizes the DAG to only evaluate the first few partitions required to yield 10 records. The `map` transformation is not executed for all records, so the accumulator only increments for the evaluated records.

27. **Twist**: A custom accumulator's `isZero` method is hardcoded to return `true`. What happens during execution?
    - **Answer**: The driver accumulator will always report zero.
    - **Mastery Explanation**: Spark checks `isZero` on the executor's task-local copy. If it returns true, Spark assumes no updates occurred and skips sending the delta in the `DirectTaskResult`. The driver never receives updates.

28. **Twist**: A developer caches a DataFrame: `df.cache()`. They run `df.count()`, and the accumulator shows 500. They immediately run `df.filter(x > 0).count()`. What does the accumulator show?
    - **Answer**: Still 500.
    - **Mastery Explanation**: The first action (`count`) materialized the cache and executed the `add()` calls. The second action reads directly from the cache, skipping the transformations and avoiding any further accumulator increments.

29. **Twist**: A custom `merge(other)` method is implemented as `this._set = other._set`. Ten tasks each process 100 distinct records. What is the driver's accumulator value?
    - **Answer**: Only 100 records (the result of the last task merged).
    - **Mastery Explanation**: Instead of folding/unioning the sets (`this._set ++= other._set`), the assignment `this._set = other._set` overwrites the canonical state with the last processed task's delta, losing all prior aggregations.

30. **Twist**: You implement a `CustomAcc.copy()` that returns a new instance but forgets to copy the internal state over. How does this affect executor task tracking?
    - **Answer**: It doesn't affect executors, but it breaks driver checkpointing/cloning.
    - **Mastery Explanation**: Executors initialize their state via `copyAndReset()`, which expects an empty state anyway. However, if the driver attempts to clone the canonical accumulator (e.g., for checkpointing), the state is lost.

31. **Twist**: A developer creates an accumulator and passes it to an executor. Instead of calling `add()`, the executor attempts to read `acc.value`. What happens?
    - **Answer**: An `UnsupportedOperationException` is thrown.
    - **Mastery Explanation**: The `value` method explicitly throws an exception if called on executors. Accumulators are write-only outside the driver.

32. **Twist**: `spark.speculation` is set to `true`. A task takes too long, so Spark launches a speculative copy. The speculative copy finishes first, and its delta is merged. Then the original task also finishes right before it gets killed. What happens to the accumulator?
    - **Answer**: Both deltas are merged, resulting in double-counting.
    - **Mastery Explanation**: Spark does not retroactively remove or ignore deltas from tasks that complete successfully if both manage to ship their `DirectTaskResult` before the other is killed.

33. **Twist**: A developer uses a single `LongAccumulator` and calls `acc.add(1)` inside a `foreachPartition` loop for every single record, processing 10 million records per task. What is the performance impact?
    - **Answer**: High executor CPU overhead due to atomic operations.
    - **Mastery Explanation**: `LongAccumulator` uses `AtomicLong`. Calling it 10 million times introduces significant synchronization overhead. The fix is to use a local `var count = 0`, increment it in the loop, and call `acc.add(count)` once per partition.

34. **Twist**: You call `sc.register(acc, "my_acc")` AFTER triggering the first DataFrame action. What happens?
    - **Answer**: The accumulator will not track metrics for that first action, and it may not appear correctly in the Spark UI for that stage.
    - **Mastery Explanation**: Accumulators must be registered before the DAG is materialized and tasks are dispatched, so the ID is properly embedded in the task closure.

35. **Twist**: A custom `MetricsAccumulator` expects an `OUT` type of `Map[String, Long]`. In the `merge()` method, a developer forgets to pattern match and just casts `other` to `MetricsAccumulator`. Another developer passes a `LongAccumulator` to `merge`. What happens?
    - **Answer**: A `ClassCastException` occurs on the driver, crashing the job.
    - **Mastery Explanation**: Spark's `Accumulables.update()` blindly calls `merge()`. If you don't pattern-match and safely handle incompatible types, invalid merges will crash the DAGScheduler thread.

36. **Twist**: A developer uses an accumulator inside a UDF (User Defined Function) in Spark SQL. They run a `SELECT` query and write it to Parquet. The query fails halfway and Spark retries the failed tasks. Does Spark guarantee exactly-once accumulator updates for UDFs?
    - **Answer**: No.
    - **Mastery Explanation**: UDFs are evaluated as part of transformations (like `select` or `withColumn`). They carry the same lazy, non-idempotent risks as `.map()`. Task retries will inflate the accumulator.

37. **Twist**: A task processes zero records. Its local accumulator copy remains at its zero state. Will Spark send this accumulator delta over Netty to the driver?
    - **Answer**: No.
    - **Mastery Explanation**: Spark checks `isZero` on the task-local copy. Since it is true, Spark optimizes the `DirectTaskResult` by eliding the empty accumulator update, saving network overhead.

38. **Twist**: A developer wants to track all distinct IPs hitting an endpoint. They use a `Set[String]` accumulator. 100,000 tasks run, each finding 50,000 unique IPs. What happens to the driver JVM?
    - **Answer**: The driver likely crashes with an `OutOfMemoryError` or severe GC stalls.
    - **Mastery Explanation**: The driver has to sequentially perform 100,000 `HashSet` unions of size 50,000. This requires massive heap allocation. A `HyperLogLog` sketch accumulator should be used instead.

39. **Twist**: An accumulator is instantiated but never registered via `sc.register()`. What happens when it is used in a `.map()`?
    - **Answer**: It throws a `SparkException: Accumulator must be registered before send to executor`.
    - **Mastery Explanation**: Spark serialization explicitly checks if the accumulator has a valid ID (assigned during registration) before shipping the closure.

40. **Twist**: A developer calls `acc.reset()` on the executor JVM inside `foreachPartition`. What is the result?
    - **Answer**: It only resets the executor's task-local copy, losing the task's aggregated metrics, but does not affect the driver or other tasks.
    - **Mastery Explanation**: Executors only hold task-local copies. Resetting it clears that specific task's delta, meaning the driver will receive a zero update from that task.

## Part 4: Coding & Debugging Questions (10 Questions)

41. **Bug**: 
```scala
val acc = sc.longAccumulator("count")
val rdd = sc.parallelize(1 to 10).map { x => acc.add(1); x * 2 }
println(acc.value)
rdd.count()
```
- **Identify the bug**: `acc.value` is read before the action `rdd.count()` is called.
- **Mastery Explanation**: Transformations are lazy. At the time `println` executes, the DAG hasn't run. The accumulator will print `0`. The read must be moved after `rdd.count()`.

42. **Bug**:
```scala
class BadAcc extends AccumulatorV2[Long, Long] {
  private var _sum = 0L
  override def isZero: Boolean = _sum == 0
  override def copy(): BadAcc = new BadAcc() 
  override def reset(): Unit = _sum = 0
  override def add(v: Long): Unit = _sum += v
  override def merge(other: AccumulatorV2[Long, Long]): Unit = _sum += other.value
  override def value: Long = _sum
}
```
- **Identify the bug**: `copy()` does not carry over the `_sum` state.
- **Mastery Explanation**: While executors use `copyAndReset()`, the driver occasionally uses `copy()` for checkpointing or cloning the canonical state. Returning `new BadAcc()` drops the driver's merged total. It should be `val newAcc = new BadAcc(); newAcc._sum = this._sum; newAcc`.

43. **Bug**:
```scala
df.foreachPartition { rows =>
  rows.foreach { row =>
    globalAccumulator.add(1)
  }
}
```
- **Identify the bug**: High synchronization overhead inside the tight loop.
- **Mastery Explanation**: `globalAccumulator.add(1)` uses atomic synchronization internally. Calling it per record in a partition with millions of rows causes thread contention. Use a local `var count = 0`, increment it, and call `globalAccumulator.add(count)` once at the end of the partition.

44. **Bug**:
```scala
val myAcc = new CustomMapAccumulator()
df.map { row => 
  myAcc.add(Map("read" -> 1L))
  row 
}.count()
```
- **Identify the bug**: `myAcc` is used inside a closure without being registered.
- **Mastery Explanation**: Custom accumulators must be registered with `sc.register(myAcc, "name")` before being referenced in a distributed operation, otherwise Spark will throw an un-registered exception during closure serialization.

45. **Bug**:
```scala
class SetAcc extends AccumulatorV2[String, mutable.Set[String]] {
  private val _set = mutable.Set.empty[String]
  // ... other methods implemented ...
  override def value: mutable.Set[String] = _set
}
```
- **Identify the bug**: `value` returns a mutable reference to the internal state.
- **Mastery Explanation**: The canonical accumulator state must be protected from external modification. Returning `_set` directly allows driver-side code to accidentally mutate it. It should return `_set.toSet` (an immutable copy).

46. **Bug**:
```scala
val cachedDf = df.map(row => { acc.add(1); row }).cache()
cachedDf.count()
println(acc.value) // Prints 100
cachedDf.filter(...).count()
println(acc.value) // Expecting 200, but prints 100
```
- **Identify the bug**: The developer expects the accumulator to increment on the second action.
- **Mastery Explanation**: Because the DataFrame is cached, the second `.count()` reads from the cache rather than re-evaluating the `.map()`. The accumulator is never touched again.

47. **Bug**:
```scala
override def merge(other: AccumulatorV2[String, Set[String]]): Unit = {
  this._set = other.asInstanceOf[SetAcc]._set
}
```
- **Identify the bug**: `merge` overwrites state instead of aggregating it.
- **Mastery Explanation**: `merge` must combine the current driver state with the incoming task state. `this._set = other...` destroys all previously merged task deltas. It must be `this._set ++= other.asInstanceOf[SetAcc]._set`.

48. **Bug**:
```scala
val acc = sc.longAccumulator("errors")
spark.conf.set("spark.speculation", "true")
df.foreachPartition { p => 
  if (p.hasNext) acc.add(1) 
}
```
- **Identify the bug**: Speculation is enabled while relying on exact accumulator counts.
- **Mastery Explanation**: If a task is slow, Spark launches a speculative copy. If both complete before the slower one is killed, `acc.add(1)` is merged twice. For exact metrics, `spark.speculation` must be `false` or native `.agg()` operations used.

49. **Bug**:
```scala
class BadAcc extends AccumulatorV2[Long, Long] {
  private var _sum = 100L
  override def isZero: Boolean = _sum == 0
  // ...
}
```
- **Identify the bug**: Initial state is non-zero, making `isZero` return `false` on a fresh copy.
- **Mastery Explanation**: A newly instantiated accumulator task copy must represent an empty delta. If it starts at `100L`, `isZero` will be false, and every task will ship an extra `100L` baseline delta to the driver, massively inflating the total.

50. **Bug**:
```scala
df.foreachPartition { partition =>
  val result = partition.map(processRow)
  myAcc.add(result.size)
}
```
- **Identify the bug**: Calling `.size` on an Iterator consumes it.
- **Mastery Explanation**: While the accumulator safely increments, the `result` Iterator is completely exhausted. If you attempt to write `result` to a database after calling `.size`, nothing will be written. You must increment a local counter during processing, not by measuring the iterator size blindly.
