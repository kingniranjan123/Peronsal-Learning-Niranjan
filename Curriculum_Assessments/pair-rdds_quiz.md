# Pair RDDs Assessment

## True/False Questions

1. **Question**: `mapValues` will never trigger a shuffle, regardless of the upstream RDD's partitioner.
   **Correct Answer**: True
   **Mastery Explanation**: `mapValues` only transforms the values while preserving the original keys and the partitioner. Because the keys and partition assignments are intact, Spark guarantees no shuffle is needed.

2. **Question**: A `RangePartitioner` requires a single pass over the data to sample and determine the partition boundaries.
   **Correct Answer**: True
   **Mastery Explanation**: `RangePartitioner` uses reservoir sampling in a single pass to estimate boundaries that distribute the data evenly based on key ordering, before shuffling.

3. **Question**: If RDD A and RDD B have the same `HashPartitioner` (with the same number of partitions), joining them will still trigger a shuffle for at least one of them.
   **Correct Answer**: False
   **Mastery Explanation**: If both RDDs are co-partitioned with the exact same partitioner instance, Spark will perform a narrow dependency join (co-located join) without shuffling either RDD.

4. **Question**: `groupByKey` uses map-side combining to reduce the amount of data shuffled across the network.
   **Correct Answer**: False
   **Mastery Explanation**: `groupByKey` does NOT use map-side combining. All values for a key are shuffled across the network, making it highly susceptible to OutOfMemory (OOM) errors compared to `reduceByKey`.

5. **Question**: `reduceByKey` can return a different value type than the input RDD's value type.
   **Correct Answer**: False
   **Mastery Explanation**: `reduceByKey` requires the associative and commutative reduce function to have the signature `(V, V) => V`. Thus, the output type must be the same as the input value type.

6. **Question**: You can define a custom partitioner by extending `org.apache.spark.Partitioner` and overriding `numPartitions` and `getPartition`.
   **Correct Answer**: True
   **Mastery Explanation**: This is the exact API for custom partitioners in Spark, allowing you to route specific keys to specific partition indices.

7. **Question**: Calling `sortByKey` on a Pair RDD partitioned by a `HashPartitioner` will preserve the `HashPartitioner` in the resulting RDD.
   **Correct Answer**: False
   **Mastery Explanation**: `sortByKey` internally applies a `RangePartitioner` so that records are globally sorted across partitions.

8. **Question**: `aggregateByKey` allows the zero value to be applied multiple times per partition during map-side combine.
   **Correct Answer**: False
   **Mastery Explanation**: The zero value in `aggregateByKey` is applied exactly once per key per partition during the map-side aggregation step.

9. **Question**: `cogroup` can be used to group data from more than two RDDs sharing the same key.
   **Correct Answer**: True
   **Mastery Explanation**: Spark provides overloaded `cogroup` methods that can group up to four RDDs together by key.

10. **Question**: If a key is an array, `HashPartitioner` will partition identical arrays to the same partition consistently.
    **Correct Answer**: False
    **Mastery Explanation**: In Java/Scala, arrays do not override `hashCode()` or `equals()` based on their contents. Thus, two identical arrays might yield different hash codes, breaking the partitioner.

## Multiple Choice Questions

11. **Question**: Which of the following operations allows the return type to differ from the input value type AND uses map-side combining?
    A) `groupByKey`
    B) `combineByKey`
    C) `reduceByKey`
    D) `mapValues`
    **Correct Answer**: B
    **Mastery Explanation**: `combineByKey` allows the accumulator type (C) to differ from the value type (V), and it performs map-side combining using `createCombiner`, `mergeValue`, and `mergeCombiners`.

12. **Question**: When joining two RDDs, RDD A (100 partitions) and RDD B (200 partitions) with no pre-existing partitioners, what will be the number of partitions in the resulting RDD?
    A) 100
    B) 200
    C) 300
    D) Determined by `spark.default.parallelism`
    **Correct Answer**: D
    **Mastery Explanation**: When neither RDD has a known partitioner, Spark defaults to `spark.default.parallelism` (if set) or the max of the number of partitions of the upstream RDDs, though in many configurations it falls back to `defaultPartitioner`, which often uses the max partitions or `spark.default.parallelism`.

13. **Question**: Why is `reduceByKey` generally preferred over `groupByKey` followed by a map/reduce?
    A) It executes asynchronously.
    B) It performs map-side combining, reducing shuffle read/write.
    C) It avoids creating an RDD lineage.
    D) It uses a sort-merge join internally.
    **Correct Answer**: B
    **Mastery Explanation**: Map-side combining drastically reduces the volume of data sent across the network during the shuffle phase.

14. **Question**: If an RDD is already partitioned with a `HashPartitioner(100)`, what happens if you call `partitionBy(new HashPartitioner(100))` on it?
    A) It triggers a full shuffle.
    B) It returns the RDD as-is without a shuffle.
    C) It throws an IllegalArgumentException.
    D) It coalesces the partitions.
    **Correct Answer**: B
    **Mastery Explanation**: `partitionBy` checks if the new partitioner equals the current partitioner. Since `HashPartitioner` implements `equals` based on partition count, it recognizes they are identical and avoids the shuffle.

15. **Question**: Which method is most appropriate for counting the exact number of occurrences of each key in a Pair RDD?
    A) `countByKey()`
    B) `count()`
    C) `reduceByKey(_ + _)`
    D) `aggregateByKey(0)(_ + _, _ + _)`
    **Correct Answer**: A
    **Mastery Explanation**: `countByKey()` is an action that returns a `Map[K, Long]` directly to the driver, which is optimal for small key spaces.

16. **Question**: How does `foldByKey` differ from `reduceByKey`?
    A) `foldByKey` allows changing the output type.
    B) `foldByKey` does not perform map-side combining.
    C) `foldByKey` provides a zero value for initialization.
    D) `foldByKey` is an action, while `reduceByKey` is a transformation.
    **Correct Answer**: C
    **Mastery Explanation**: Both are transformations and both map-side combine. `foldByKey` accepts a zero value, but still requires the output type to match the input value type.

17. **Question**: What happens to the partitioner of a Pair RDD after a `map` operation?
    A) It is preserved.
    B) It is lost (set to None).
    C) It is converted to a HashPartitioner.
    D) It causes an immediate shuffle.
    **Correct Answer**: B
    **Mastery Explanation**: Because `map` can alter both keys and values, Spark cannot guarantee that the original partitioner's properties still hold. Thus, the partitioner is dropped.

18. **Question**: Which join type will keep keys present in the right RDD but missing in the left RDD?
    A) `join`
    B) `leftOuterJoin`
    C) `rightOuterJoin`
    D) `innerJoin`
    **Correct Answer**: C
    **Mastery Explanation**: `rightOuterJoin` includes all keys from the right RDD, returning `(K, (Option[V], W))`.

19. **Question**: Which of the following partitioners is NOT built into Spark Core?
    A) HashPartitioner
    B) RangePartitioner
    C) RoundRobinPartitioner
    D) All of the above are built in
    **Correct Answer**: C
    **Mastery Explanation**: While round-robin is a concept used in data distribution (like repartition), there is no explicit `RoundRobinPartitioner` class exposed in the public RDD API for pair RDDs.

20. **Question**: You have an RDD of `(CustomerID, PurchaseAmount)`. You want to find the top 5 largest purchases per customer. Which operation is best?
    A) `groupByKey().mapValues(_.toList.sortBy(-_).take(5))`
    B) `aggregateByKey` keeping a bounded priority queue of size 5
    C) `reduceByKey` returning a list
    D) `sortByKey().groupByKey()`
    **Correct Answer**: B
    **Mastery Explanation**: `aggregateByKey` allows maintaining a small bounded queue per key during map-side combining, avoiding shuffling all data and avoiding OOMs associated with `groupByKey`.

21. **Question**: When is a `ShuffledRDD` instantiated in Spark?
    A) When reading from HDFS.
    B) Immediately after calling `mapValues`.
    C) After calling operations that require a shuffle like `reduceByKey` or `partitionBy`.
    D) When an action like `collect` is called.
    **Correct Answer**: C
    **Mastery Explanation**: `ShuffledRDD` represents the result of a shuffle dependency, instantiated by transformations that trigger shuffles (e.g., `partitionBy`, `reduceByKey`).

22. **Question**: What does the `keys` method on a Pair RDD return?
    A) `RDD[(K, K)]`
    B) `RDD[K]`
    C) `Array[K]`
    D) `Map[K, Iterable[V]]`
    **Correct Answer**: B
    **Mastery Explanation**: It returns an RDD containing just the keys of the original Pair RDD.

23. **Question**: `lookup(key)` on a Pair RDD:
    A) Returns an RDD of values for that key.
    B) Triggers a full cluster scan if the RDD is partitioned with a `HashPartitioner`.
    C) Returns all values associated with the given key to the driver.
    D) Is a transformation that filters by the key.
    **Correct Answer**: C
    **Mastery Explanation**: `lookup` is an action that returns a `Seq[V]` to the driver. If the RDD has a partitioner, it optimizes by only scanning the partition where the key resides.

24. **Question**: To ensure two RDDs are co-partitioned before a join, you should:
    A) Call `cache()` on both.
    B) Partition both with the same `Partitioner` instance.
    C) Ensure they have the same number of records.
    D) Call `repartition` with the same integer on both.
    **Correct Answer**: B
    **Mastery Explanation**: Co-partitioning requires both RDDs to be partitioned by the exact same partitioner (e.g., same `HashPartitioner` with the same number of partitions).

25. **Question**: In `combineByKey(createCombiner, mergeValue, mergeCombiners)`, when is `mergeCombiners` called?
    A) During map-side aggregation within a single partition.
    B) During the shuffle read phase to combine accumulators from different partitions.
    C) Only when `createCombiner` fails.
    D) When joining two different RDDs.
    **Correct Answer**: B
    **Mastery Explanation**: `mergeCombiners` is used to merge the intermediate combined results (accumulators) that were generated across different partitions.

## Small Twist Questions

26. **Question**: RDD A is partitioned by `HashPartitioner(10)`. You call `map(x => (x._1, x._2 + 1))`. Then you join it with RDD B (also `HashPartitioner(10)`). Does a shuffle occur?
    A) No, both had HashPartitioner(10).
    B) Yes, for RDD A only.
    C) Yes, for RDD B only.
    D) Yes, for both.
    **Correct Answer**: B
    **Mastery Explanation**: The `map` transformation strips the partitioner from RDD A. Spark will then use B's partitioner for the join, forcing RDD A to shuffle to align with B.

27. **Question**: RDD A is partitioned by `HashPartitioner(10)`. You call `mapValues(x => x + 1)`. Then you join it with RDD B (`HashPartitioner(10)`). Does a shuffle occur?
    A) No.
    B) Yes, for RDD A only.
    C) Yes, for both.
    D) Yes, for RDD B only.
    **Correct Answer**: A
    **Mastery Explanation**: `mapValues` preserves the `HashPartitioner`. Since both retain `HashPartitioner(10)`, the join is a narrow dependency (no shuffle).

28. **Question**: You have a custom partitioner where `numPartitions` is 100, but `getPartition(key)` accidentally always returns `0`. What happens during `reduceByKey`?
    A) Spark automatically balances the partitions.
    B) Data is correctly reduced, but severe data skew causes partition 0 to process all data (OOM likely).
    C) `IllegalArgumentException` is thrown.
    D) Only 1/100th of the data is processed.
    **Correct Answer**: B
    **Mastery Explanation**: Spark respects the `getPartition` logic implicitly. Sending all keys to partition 0 destroys parallelism and causes massive skew.

29. **Question**: You use `groupByKey(10)` on an RDD with 100 partitions. What happens to the map-side data?
    A) It is combined into lists on the map side.
    B) No map-side combining occurs; all data is shuffled to 10 partitions.
    C) The data remains in 100 partitions.
    D) An error is thrown due to partition count mismatch.
    **Correct Answer**: B
    **Mastery Explanation**: `groupByKey` never map-side combines. Passing `10` changes the downstream partition count, meaning all raw data is shuffled across the network into 10 partitions.

30. **Question**: RDD A and RDD B have no partitioner. You call `A.join(B, new HashPartitioner(50))`. What shuffles?
    A) Neither.
    B) Only A.
    C) Only B.
    D) Both A and B.
    **Correct Answer**: D
    **Mastery Explanation**: Because neither has a partitioner matching `HashPartitioner(50)`, Spark must shuffle both RDDs into the new partitioner to align the keys for the join.

31. **Question**: You apply `reduceByKey(_ + _)` on an RDD of strings `(String, String)`. What is the memory implication compared to integers?
    A) Strings are automatically interned during shuffle.
    B) String concatenation allocates new objects, heavily stressing the Garbage Collector (GC).
    C) Map-side combining is disabled for Strings.
    D) No difference; Tungsten optimizes strings perfectly in RDDs.
    **Correct Answer**: B
    **Mastery Explanation**: `_ + _` on Strings creates a new String object for every reduction. In RDDs, this generates immense JVM object churn and GC pressure.

32. **Question**: RDD A is cached in memory. It is partitioned by `HashPartitioner(100)`. You call `A.repartition(100)`. What happens?
    A) Nothing, it's a no-op since partitions equal 100.
    B) A full shuffle occurs, using RoundRobin partitioning, dropping the HashPartitioner.
    C) A shuffle occurs, but HashPartitioner is preserved.
    D) Spark throws an exception.
    **Correct Answer**: B
    **Mastery Explanation**: `repartition(n)` is an alias for `coalesce(n, shuffle = true)`. It shuffles data randomly (round-robin) to achieve equal sizes, thus stripping the `HashPartitioner`.

33. **Question**: You call `cogroup` on RDD A (keys: 1, 2) and RDD B (keys: 2, 3). What is the value for key 1 in the output?
    A) `(Iterable(valA), Iterable(valB))`
    B) `(Iterable(valA), Iterable())`
    C) Key 1 is omitted.
    D) `(valA, null)`
    **Correct Answer**: B
    **Mastery Explanation**: `cogroup` acts as a full outer join grouped by key. Key 1 exists in A but not B, so B's iterable is empty.

34. **Question**: You use a mutable data structure (like `ArrayBuffer`) as the zero value in `aggregateByKey`. Is this safe?
    A) Yes, if you only mutate it within the merge functions, it avoids object allocation.
    B) No, the zero value must be immutable because Spark may reuse the same instance, causing concurrent modification or cross-key contamination.
    C) Yes, Spark deeply clones the zero value automatically.
    D) No, `aggregateByKey` only accepts primitive types.
    **Correct Answer**: B
    **Mastery Explanation**: Spark provides the zero value to multiple keys. If it is mutable and modified, you will corrupt the aggregations of other keys.

35. **Question**: RDD A (`HashPartitioner(10)`) is joined with RDD B (`HashPartitioner(20)`). What is the partitioner of the result?
    A) `HashPartitioner(10)`
    B) `HashPartitioner(20)`
    C) None
    D) The one with the larger number of partitions (i.e., `HashPartitioner(20)`).
    **Correct Answer**: D
    **Mastery Explanation**: Spark's default behavior for joins between RDDs with different partitioners is to use the partitioner with the most partitions to maximize parallelism.

36. **Question**: You call `subtractByKey` on RDD A (100 partitions) using RDD B (5 partitions). Neither has a partitioner. What is the parallelism of the result?
    A) 5
    B) 100
    C) 105
    D) `spark.default.parallelism`
    **Correct Answer**: B
    **Mastery Explanation**: Operations like `subtractByKey` typically preserve the parallelism of the first RDD (the one being operated on) unless explicitly specified otherwise.

37. **Question**: A `RangePartitioner` is created for an RDD with heavily skewed data (90% of keys are identical). What happens to the partition boundaries?
    A) It fails with an exception.
    B) It creates boundaries that ensure all identical keys go to a single partition, making that partition massive.
    C) It splits the identical keys across multiple partitions automatically.
    D) It falls back to a HashPartitioner.
    **Correct Answer**: B
    **Mastery Explanation**: A `RangePartitioner` guarantees that all records with the same key end up in the same partition. If keys are skewed, the target partition will be skewed.

38. **Question**: You use `reduceByKey(_ + _)` on `RDD[(Int, Int)]`. You notice it is very slow. You change it to `reduceByKey(new HashPartitioner(100))(_ + _)`. Why might this speed it up?
    A) It enables map-side combining.
    B) It changes the shuffle from sort-based to hash-based.
    C) It increases shuffle parallelism if the original RDD had very few partitions.
    D) It bypasses Tungsten.
    **Correct Answer**: C
    **Mastery Explanation**: If the upstream RDD had very few partitions, `reduceByKey` might default to that small number. Explicitly passing a partitioner with 100 increases downstream parallelism, reducing the per-task load.

39. **Question**: RDD A `(K, V)` and RDD B `(K, W)`. You want an inner join, but RDD B is very small (10 MB). What is the fastest RDD-level technique?
    A) `A.join(B)`
    B) Broadcast B as a Map and use `A.flatMap` or `A.mapPartitions` to do a map-side join.
    C) `A.cogroup(B)`
    D) `B.join(A)`
    **Correct Answer**: B
    **Mastery Explanation**: RDDs do not have an automatic Broadcast Hash Join optimizer like DataFrames. To achieve a map-side join and avoid shuffling A, you must manually broadcast B.

40. **Question**: You apply `filter(x => x._2 > 10)` on a `HashPartitioned` Pair RDD. Does it retain its partitioner?
    A) Yes.
    B) No.
    C) Only if fraction > 0.5.
    D) Only if `withReplacement` is true.
    **Correct Answer**: A
    **Mastery Explanation**: `filter` only removes elements. Since keys are unmodified and their hash codes are unchanged, the remaining elements are still in the correct partitions. Spark preserves the partitioner.

## Coding & Debugging Questions

41. **Question**: A developer writes: `rdd.groupByKey().mapValues(iter => iter.reduce(_ + _))`. Identify the flaw.
    **Correct Answer**: Memory leak/OOM risk due to `groupByKey`.
    **Mastery Explanation**: `groupByKey` shuffles all values for a key across the network without map-side reduction. For keys with many values, the `Iterable` can exceed memory. It should be replaced with `reduceByKey(_ + _)`.

42. **Question**: `val rdd = sc.textFile("...").map(line => (line.split(",")(0), line))`
    The developer complains joins on this RDD are shuffling both sides every time. Identify the optimizer blocker.
    **Correct Answer**: The RDD has no partitioner.
    **Mastery Explanation**: `map` creates a `MapPartitionsRDD` with `partitioner == None`. To avoid repeated shuffles, the developer should call `partitionBy(new HashPartitioner(N)).cache()` before joining.

43. **Question**: 
    ```scala
    var counter = 0
    rdd.foreach { case (k, v) => counter += v }
    ```
    Why does `counter` print `0` on the driver?
    **Correct Answer**: The closure captures a copy of the variable.
    **Mastery Explanation**: The `counter` inside `foreach` executes on the executors, mutating local copies. The driver's `counter` remains untouched. An `Accumulator` should be used instead.

44. **Question**: You have a custom object `class Person(val name: String)`. You use it as a key in `PairRDD[Person, Int]`. You use `reduceByKey`. The output has multiple records for the exact same name. Why?
    **Correct Answer**: Missing `equals` and `hashCode` methods.
    **Mastery Explanation**: Without overriding `hashCode` and `equals`, `HashPartitioner` and the map-side combine hash map will treat two different `Person` objects with the same name as distinct keys.

45. **Question**:
    ```scala
    rdd1.partitionBy(new HashPartitioner(100))
    val joined = rdd1.join(rdd2)
    ```
    Why does `rdd1` still undergo a shuffle during the join?
    **Correct Answer**: `partitionBy` is missing assignment/caching.
    **Mastery Explanation**: `partitionBy` creates a new RDD. Since it isn't assigned to a variable or cached, the join operates on the original unmodified `rdd1` which lacks the partitioner.

46. **Question**: A developer wants to group by key and sort the values. 
    `rdd.groupByKey().mapValues(_.toList.sorted)`
    What is the primary physical execution bottleneck?
    **Correct Answer**: Full materialization of values in memory per key.
    **Mastery Explanation**: `_.toList.sorted` requires pulling every single value for a key into memory at once to perform the sort. If a key has millions of values, the executor will throw an OutOfMemoryError.

47. **Question**: 
    ```scala
    rdd.combineByKey(
      (v) => ArrayBuffer(v),
      (buf: ArrayBuffer[Int], v) => buf += v,
      (buf1: ArrayBuffer[Int], buf2: ArrayBuffer[Int]) => buf1 ++= buf2
    )
    ```
    What is a more memory-efficient native way to achieve this exact same output without `combineByKey`?
    **Correct Answer**: `groupByKey()`
    **Mastery Explanation**: Since the developer is simply appending all values into an ArrayBuffer without any reduction, they are manually reimplementing `groupByKey`. `combineByKey` is overkill here and offers no map-side reduction advantage.

48. **Question**: You run `rdd.sortByKey()`. Your job fails with `NullPointerException` during the shuffle phase inside a comparison method. What is wrong?
    **Correct Answer**: Null keys exist in the RDD.
    **Mastery Explanation**: `sortByKey` relies on implicit `Ordering`. If your RDD contains `(null, value)` records, comparing `null` to other keys throws an NPE during the sort phase. Filter out nulls first.

49. **Question**: 
    ```scala
    val partitioned = rdd.partitionBy(new HashPartitioner(10))
    val result = partitioned.map(x => (x._1, x._2 * 2)).reduceByKey(_ + _)
    ```
    How many shuffles occur in this lineage?
    **Correct Answer**: Two.
    **Mastery Explanation**: `partitionBy` triggers the first shuffle. `map` strips the partitioner. `reduceByKey` sees no partitioner and triggers a second shuffle. Use `mapValues` to prevent dropping the partitioner and avoid the second shuffle.

50. **Question**: 
    ```scala
    rdd.aggregateByKey(new HashSet[String]())(
      (set, v) => { set.add(v); set },
      (set1, set2) => { set1.addAll(set2); set1 }
    )
    ```
    What critical correctness bug exists in this map-side combiner?
    **Correct Answer**: Re-use of a mutable shared zero value.
    **Mastery Explanation**: `new HashSet[String]()` is evaluated once on the driver and serialized. The same mutable instance is passed to all keys in a partition. Keys will cross-contaminate their sets.
