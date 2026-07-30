# Master Class: Feature Scaling - Assessment

## Part 1: True/False Questions (1-10)

1. **Question:** Tungsten stores MLlib vectors natively in a columnar format that does not require serialization.
**Answer:** False
**Mastery Explanation:** Tungsten stores rows in raw binary format (`UnsafeRow`). MLlib vectors (VectorUDTs) are serialized into these byte arrays, meaning extracting and manipulating them incurs CPU serialization/deserialization overhead. Other options do not apply.

2. **Question:** Applying StandardScaler with `withMean=True` on a SparseVector can cause OOM errors on executors due to densification.
**Answer:** True
**Mastery Explanation:** Shifting the mean changes implicit zeros to non-zero values, expanding a highly sparse matrix into a dense matrix and blowing up JVM heap memory.

3. **Question:** RobustScaler computes exact quantiles across the distributed dataset without requiring a network shuffle.
**Answer:** False
**Mastery Explanation:** Computing exact quantiles distributedly requires sorting (massive shuffle). RobustScaler uses the Greenwald-Khanna algorithm for approximate quantiles.

4. **Question:** treeAggregate reduces driver memory bottlenecks by aggregating data at intermediate executors.
**Answer:** True
**Mastery Explanation:** By aggregating in a tree structure, it avoids sending every partition's arrays directly to the driver, reducing network and memory bottlenecks.

5. **Question:** VectorSlicer is designed specifically to prune features with zero variance before they reach the scaling stage.
**Answer:** False
**Mastery Explanation:** VectorSlicer extracts specific feature indices. VarianceThresholdSelector is used to prune zero-variance features.

6. **Question:** Spark's MinMaxScaler throws a division-by-zero exception if a feature has zero variance.
**Answer:** False
**Mastery Explanation:** Spark handles this edge case robustly, assigning a secure mapping (e.g., `0.5 * (max + min)`) instead of throwing an exception.

7. **Question:** `withCentering=True` in RobustScaler should be used for SparseVectors to maintain their sparsity.
**Answer:** False
**Mastery Explanation:** It must be `False`. Subtracting a non-zero median converts implicit zeros to non-zeros, causing densification.

8. **Question:** The Greenwald-Khanna algorithm's memory consumption on the driver increases when the `relativeError` parameter is set lower.
**Answer:** True
**Mastery Explanation:** Lower relative error requires the algorithm to maintain more state/sketches, increasing driver memory consumption.

9. **Question:** Spark MLlib operates on DataFrames containing VectorUDT which bypass Tungsten deserialization overhead entirely.
**Answer:** False
**Mastery Explanation:** VectorUDTs require deserialization from Tungsten's UnsafeRow byte arrays to perform operations, incurring CPU overhead.

10. **Question:** Applying scaling to One-Hot Encoded (OHE) variables using StandardScaler preserves their binary sparsity.
**Answer:** False
**Mastery Explanation:** Standard scaling OHE variables destroys their binary sparsity and shifts their distribution unnecessarily.

## Part 2: Multiple Choice Questions (11-25)

11. **Question:** Why is applying `StandardScaler(withMean=True)` to a SparseVector dangerous in Spark?
A) It triggers an immediate Catalyst optimization failure.
B) It converts implicit zeros to non-zero values, leading to memory explosion (densification).
C) It computes exact quantiles, causing a massive network shuffle.
D) It drops all zero-variance features.
**Answer:** B
**Mastery Explanation:** A is wrong because Catalyst plans it fine. C is wrong as StandardScaler computes mean/std, not quantiles. D is wrong. B is correct because mean shifting replaces implicit zeros.

12. **Question:** Which algorithm does RobustScaler use in Spark to compute approximate quantiles?
A) T-Digest
B) Greenwald-Khanna
C) HyperLogLog
D) Count-Min Sketch
**Answer:** B
**Mastery Explanation:** Spark uses Greenwald-Khanna for quantiles in RobustScaler. T-Digest is used in SQL `approx_percentile`. HLL is for distinct counts.

13. **Question:** What happens when a feature has zero variance in Spark's MinMaxScaler?
A) It throws a DivideByZero exception.
B) It assigns NaN to the scaled values.
C) It securely maps the value within target bounds without crashing.
D) It automatically drops the feature from the dataset.
**Answer:** C
**Mastery Explanation:** Spark handles min==max without crashing by assigning a valid mapping, protecting downstream operations from NaNs (B) and exceptions (A).

14. **Question:** What is the primary purpose of `treeAggregate` when computing scaling statistics?
A) To serialize data into Tungsten's UnsafeRow format.
B) To compute exact quantiles.
C) To aggregate partially at intermediate executors, reducing network/memory bottlenecks at the driver.
D) To convert DenseVectors to SparseVectors.
**Answer:** C
**Mastery Explanation:** `treeAggregate` avoids the O(N) bottleneck at the driver by doing multi-level reductions.

15. **Question:** How does Tungsten store MLlib rows under the hood?
A) As Java Objects on the JVM heap.
B) In raw binary format as `UnsafeRow`.
C) As serialized JSON strings.
D) As Parquet columnar batches in memory.
**Answer:** B
**Mastery Explanation:** Tungsten uses off-heap `UnsafeRow` to avoid JVM GC overhead. MLlib vectors are serialized into these byte arrays.

16. **Question:** What is a consequence of setting `relativeError` to a very low value in RobustScaler?
A) The model trains significantly faster.
B) Executor CPU utilization decreases.
C) Memory consumption on the driver node increases.
D) Spark forces the dataset to be cached in memory.
**Answer:** C
**Mastery Explanation:** A lower error requires larger sketch data structures, which are gathered at the driver, leading to higher memory usage.

17. **Question:** To preserve the sparsity of categorical features when scaling continuous features, which combination is recommended?
A) VectorIndexer and StandardScaler
B) VectorSlicer, StandardScaler, and VectorAssembler
C) StringIndexer, OneHotEncoder, and RobustScaler
D) PCA and StandardScaler
**Answer:** B
**Mastery Explanation:** VectorSlicer extracts continuous features for scaling, leaving categorical features untouched, and VectorAssembler reunites them.

18. **Question:** What is the memory footprint of a `DenseVector` containing 10,000 `Double`s in Spark?
A) ~10,000 bytes
B) ~40,000 bytes
C) ~80,000 bytes
D) ~160,000 bytes
**Answer:** C
**Mastery Explanation:** A Double is 8 bytes. 10,000 * 8 = 80,000 bytes.

19. **Question:** Which is true about VectorUDT in Spark?
A) It requires deserialization from UnsafeRow to perform operations.
B) It operates directly on off-heap memory without serialization.
C) Catalyst cannot optimize DataFrames containing VectorUDT.
D) It is only compatible with SparseVectors.
**Answer:** A
**Mastery Explanation:** VectorUDT objects must be deserialized into Java objects to apply mathematical operations, incurring CPU costs.

20. **Question:** What is the recommended architectural decision for handling features with zero variance before scaling?
A) Use RobustScaler instead of StandardScaler.
B) Use VarianceThresholdSelector to prune them early.
C) Impute the features with the mean of the dataset.
D) Convert them to SparseVectors.
**Answer:** B
**Mastery Explanation:** Pruning dead features early saves CPU cycles during scaling and memory during assembly.

21. **Question:** What causes a massive network shuffle when computing exact statistics like medians?
A) Calculating the mean and standard deviation.
B) Sorting the entire distributed dataset.
C) Applying VectorAssembler.
D) Using `treeAggregate`.
**Answer:** B
**Mastery Explanation:** Exact quantiles require global sorting, which necessitates moving data across the network (shuffling).

22. **Question:** Why shouldn't you scale One-Hot Encoded (OHE) variables with StandardScaler?
A) Catalyst will crash.
B) It destroys binary sparsity and shifts distribution needlessly.
C) It converts them into strings.
D) Spark throws an error.
**Answer:** B
**Mastery Explanation:** OHE vectors are sparse. Scaling them removes their binary nature and sparsity, harming models like Decision Trees.

23. **Question:** In `treeAggregate`, what does the `depth` parameter control?
A) The maximum depth of a Decision Tree model.
B) The number of levels in the multi-level aggregation tree.
C) The precision of the Greenwald-Khanna algorithm.
D) The depth of Tungsten's memory pool.
**Answer:** B
**Mastery Explanation:** `depth` defines how many intermediate executor-level reductions occur before sending data to the driver.

24. **Question:** What is the main disadvantage of a simple `reduce` for high-dimensional feature vectors?
A) Executors cannot process high-dimensional vectors.
B) It computes approximate statistics.
C) It overwhelms the driver's network and memory.
D) It requires sorting the dataset first.
**Answer:** C
**Mastery Explanation:** Every partition sends its full array to the driver at once, causing OOM or network saturation.

25. **Question:** If an executor configured with 8GB of memory experiences catastrophic GC pauses during scaling, what is the most likely cause?
A) Broadcasting a small model.
B) Densification of highly sparse data.
C) Dropping zero-variance features.
D) Using MinMaxScaler.
**Answer:** B
**Mastery Explanation:** Shifting means of sparse vectors instantiates millions of Double arrays on the heap, triggering massive GC.

## Part 3: Small Twist Questions (26-40)

26. **Question (Twist):** You change `withCentering=False` to `withCentering=True` in RobustScaler for a SparseVector dataset. What is the immediate physical execution impact?
**Answer:** OOM Exception on Executors.
**Mastery Explanation:** `withCentering=True` subtracts the median, converting implicit zeros to non-zeros (Densification), blowing up memory.

27. **Question (Twist):** You change `relativeError=0.001` to `relativeError=0.0` in RobustScaler. What happens?
**Answer:** Massive Shuffles or Driver OOM.
**Mastery Explanation:** Error 0.0 forces exact quantile computation, leading to a global sort (shuffle) and huge sketch memory usage.

28. **Question (Twist):** You replace `treeAggregate(depth=2)` with `treeAggregate(depth=1)`. What happens?
**Answer:** Driver Memory/Network Bottleneck.
**Mastery Explanation:** Depth=1 disables intermediate reduction, mimicking a standard `reduce`, sending all arrays directly to the driver.

29. **Question (Twist):** You scale OHE variables using `StandardScaler(withMean=False, withStd=True)`. Why is this still suboptimal?
**Answer:** Alters Relative Importance.
**Mastery Explanation:** Sparsity is preserved (`withMean=False`), but `withStd=True` scales binary values by their inverse stddev, distorting feature importance for tree models.

30. **Question (Twist):** You apply MinMaxScaler to a feature column containing only the value `5.0` (zero variance). Does the job crash?
**Answer:** No.
**Mastery Explanation:** Spark intelligently handles `max == min` without division-by-zero, mapping it centrally, avoiding a crash.

31. **Question (Twist):** You swap `VectorSlicer` with a Python UDF to extract array elements before scaling. What performance metric degrades?
**Answer:** CPU Utilization (Deserialization Overhead).
**Mastery Explanation:** UDFs force Tungsten to deserialize `UnsafeRow` into Python objects, bypassing Catalyst and causing massive CPU overhead.

32. **Question (Twist):** You increase executor heap memory to 32GB to handle SparseVector densification. What new problem arises?
**Answer:** Massive Stop-The-World GC Pauses.
**Mastery Explanation:** While avoiding immediate OOM, sweeping millions of short-lived Double arrays in a 32GB heap will cause severe GC pauses, crippling execution time.

33. **Question (Twist):** You use StandardScaler(withMean=True) on a DenseVector dataset, but one partition has 10x more rows. What happens during scaling?
**Answer:** Straggler Tasks.
**Mastery Explanation:** The second pass (transformation) will bottleneck on the skewed partition, leading to uneven execution times.

34. **Question (Twist):** A developer adds `VarianceThresholdSelector` *after* the `StandardScaler` step. Why is this an anti-pattern?
**Answer:** Wasted CPU/Memory on Dead Features.
**Mastery Explanation:** Pruning after scaling means you've already paid the serialization and computation cost for features with zero information.

35. **Question (Twist):** You increase vector dimensionality from 1K to 1M and use `treeAggregate(depth=2)`. Is the driver safe?
**Answer:** Not necessarily.
**Mastery Explanation:** Intermediate arrays take 8MB each. Depending on the number of partitions, depth=2 might still overwhelm the driver. A higher depth is needed.

36. **Question (Twist):** You use RobustScaler with `lower=0.0` and `upper=1.0`. What algorithm does this mimic?
**Answer:** MinMaxScaler.
**Mastery Explanation:** It computes min and max bounds instead of IQR, mimicking MinMaxScaler but utilizing approximate quantiles.

37. **Question (Twist):** You broadcast a trained StandardScaler model with 10 million feature scalers. What happens?
**Answer:** Severe Network/Memory Overhead on Executors.
**Mastery Explanation:** Broadcasting an 80MB model to thousands of executors eats up network bandwidth and memory per executor.

38. **Question (Twist):** You apply `StandardScaler` to an integer-based SparseVector without casting to Double. What happens?
**Answer:** Schema Validation Error.
**Mastery Explanation:** MLlib VectorUDTs only support Double types. Integers will trigger an error before physical execution begins.

39. **Question (Twist):** You change `withStd=False` and `withMean=True` on a SparseVector dataset. Does it avoid OOM?
**Answer:** No.
**Mastery Explanation:** Subtracting the mean still occurs, which converts zeros to non-zeros, causing densification and OOM.

40. **Question (Twist):** You use `VectorAssembler` to combine two scaled SparseVectors. Do they become dense?
**Answer:** No.
**Mastery Explanation:** `VectorAssembler` intelligently merges indices and values, outputting a new SparseVector and maintaining memory efficiency.

## Part 4: Coding & Debugging Questions (41-50)

41. **Scenario:** A PySpark ML job crashes with OOMs. The code uses a UDF to convert Vectors to lists, modifies them, and converts back.
**Fix:** Remove the UDF. Use built-in Transformers like `VectorSlicer` or SQL functions to avoid massive serialization overhead and object creation on the heap.
**Mastery Explanation:** UDFs defeat Tungsten's off-heap memory, creating millions of Python objects and causing GC limits/OOMs.

42. **Scenario:** `RobustScaler.fit()` fails with Driver OOM on a 10TB dataset.
**Fix:** Increase `relativeError` (e.g., to 0.01) or increase driver memory.
**Mastery Explanation:** Too low `relativeError` forces the Greenwald-Khanna algorithm to retain too many sketches, exhausting driver memory.

43. **Scenario:** A custom Transformer implements `transform` by using `df.rdd.collect()` to find the max value, then scaling.
**Fix:** Use `DataFrame` aggregations or `treeAggregate` instead of `collect()`.
**Mastery Explanation:** `collect()` pulls all data to the driver, causing OOM. Distributed aggregations keep data on executors.

44. **Scenario:** After applying StandardScaler, downstream Logistic Regression weights explode to `NaN`.
**Fix:** Filter out `NaN` or `Null` values before scaling.
**Mastery Explanation:** Standard aggregators propagate `NaN`s. If one row has `NaN`, the column's mean becomes `NaN`, poisoning the entire feature.

45. **Scenario:** A pipeline applies PCA, then StandardScaler. The model performs poorly.
**Fix:** Swap the order: StandardScaler *then* PCA.
**Mastery Explanation:** PCA is highly sensitive to scale. If not scaled first, features with larger magnitudes will dominate the principal components.

46. **Scenario:** Executors throw "GC overhead limit exceeded" during `treeAggregate`.
**Fix:** Mutate the accumulator array in-place inside `seqOp` instead of instantiating new arrays inside the loop.
**Mastery Explanation:** Allocating arrays per row in a tight `while` loop overwhelms the garbage collector. In-place mutation prevents this.

47. **Scenario:** `VectorAssembler` throws "IllegalArgumentException: requirement failed: Column ... does not exist".
**Fix:** Ensure the Transformer output columns correctly match the Assembler's `inputCols` and that no upstream steps dropped them.
**Mastery Explanation:** Spark Pipelines require exact column name matching. Catalyst validation fails early if columns are missing.

48. **Scenario:** A trained pipeline fails in production because new categorical values appeared, shifting the indices extracted by `VectorSlicer`.
**Fix:** Set `handleInvalid="keep"` on StringIndexer, or use a robust feature selection method instead of hardcoded slice indices.
**Mastery Explanation:** Hardcoded indices in `VectorSlicer` break when OHE vector sizes change dynamically due to unseen categories.

49. **Scenario:** The Spark UI shows one task in `RobustScaler.fit()` taking 10x longer than others.
**Fix:** Handle data skew by salting or repartitioning the dataset before fitting.
**Mastery Explanation:** Skewed partitions force one executor to process significantly more data during quantile approximation.

50. **Scenario:** `VectorAssembler` + `StandardScaler(withMean=True)` causes OOM on a dataset of TF-IDF vectors.
**Fix:** Set `withMean=False` on the StandardScaler.
**Mastery Explanation:** TF-IDF vectors are highly sparse. Centering them densifies the vectors, leading to catastrophic memory expansion (OOM).

---
*End of Assessment*
