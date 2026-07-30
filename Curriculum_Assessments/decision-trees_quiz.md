# 🔥 Master Class Assessment: Decision Trees

This assessment evaluates Senior/Staff-level knowledge of Apache Spark's distributed Decision Tree architecture, including the PLANET algorithm, Catalyst/Tungsten optimizations, and memory tuning.

## Part 1: True/False Questions (10)

**1. In Spark MLlib, continuous features are sorted at each active node during tree construction to evaluate exact split boundaries.**
- **Answer:** False
- **Mastery Explanation:** Spark MLlib uses a distributed histogram-based approach (PLANET algorithm). It discretizes continuous features into `maxBins` buckets before training begins, eliminating the need to sort data at every node, which would be impossible at scale due to network shuffling.

**2. Increasing `maxBins` linearly increases the memory footprint on the executors but has a negligible effect on the driver.**
- **Answer:** False
- **Mastery Explanation:** The driver must hold the global histogram in memory during the `treeAggregate` step. The memory scales as $O(N_{active} \times F \times B \times S)$. Increasing `maxBins` ($B$) directly inflates the driver's memory footprint and is the primary cause of Driver OOMs.

**3. Spark's Decision Tree algorithm uses Tungsten's off-heap memory to manage vector math with minimal JVM GC overhead.**
- **Answer:** True
- **Mastery Explanation:** Executors process `VectorUDT` in tight loops updating local arrays. Tungsten's off-heap memory management and optimized binary formats prevent massive JVM garbage collection overhead during these CPU-bound operations.

**4. For multi-class classification with a categorical feature of cardinality $C$, Spark evaluates splits in $O(C)$ time by ordering categories by target mean.**
- **Answer:** False
- **Mastery Explanation:** The target-mean ordering trick only works for binary classification and regression. For multi-class classification, Spark cannot use this heuristic and must evaluate all $2^{C-1} - 1$ possible subsets, leading to an exponential explosion if cardinality is high.

**5. `maxMemoryInMB` dictates the absolute maximum RAM an executor is allowed to use during the map-reduce phase of tree training.**
- **Answer:** False
- **Mastery Explanation:** `maxMemoryInMB` is a driver-side safety threshold. It estimates the size of the global histogram. If the size exceeds this value, Spark splits the active nodes into groups and processes them in multiple sequential passes over the data to prevent Driver OOM.

**6. If tree depth is configured to 15 and `checkpointInterval` is not set, the application may fail with a `StackOverflowError`.**
- **Answer:** True
- **Mastery Explanation:** Deep trees create massive, deeply nested RDD lineage DAGs (one map-reduce per level). Without `checkpointInterval` truncating the lineage, the DAGScheduler crashes with a `StackOverflowError` during physical planning.

**7. Applying One-Hot Encoding (OHE) to categorical features before training a Spark Decision Tree is a recommended best practice.**
- **Answer:** False
- **Mastery Explanation:** OHE creates sparse vectors that bloat memory and destroy the algorithm's native ability to make binary splits on categorical subsets. `VectorIndexer` should be used instead.

**8. The `NodeIdCache` tracks which tree node each training row currently belongs to, avoiding root-to-leaf traversals at deeper levels.**
- **Answer:** True
- **Mastery Explanation:** The `NodeIdCache` stores an RDD of arrays representing node assignments. This prevents executors from having to re-evaluate the entire tree path for every row during every level's pass.

**9. The `treeAggregate` operation sends all raw local data partitions to the driver to calculate global splits.**
- **Answer:** False
- **Mastery Explanation:** Executors compute sufficient statistics (histograms) locally. `treeAggregate` only sends these aggregated histograms (counts, sums, squares) to the driver, transforming an I/O-bound problem into a CPU-bound reduction.

**10. `VectorSlicer` performs dimensionality reduction natively on the Tungsten binary row format without requiring a UDF.**
- **Answer:** True
- **Mastery Explanation:** `VectorSlicer` applies a Catalyst projection step directly on the underlying `UnsafeRow` bytes, projecting out irrelevant features at memory-bandwidth speeds without the massive serialization overhead of UDFs.

---

## Part 2: Multiple Choice Questions (15)

**11. Which algorithmic architecture does Spark MLlib adapt for its distributed decision tree implementation?**
- A) XGBoost Hist-approx
- B) PLANET
- C) C4.5
- D) CART-Sort
- **Answer:** B
- **Mastery Explanation:** Spark adapts the PLANET (Parallel Learner for Assembling Numerous Ensemble Trees) architecture, which shifts from sorting data to distributed histogram aggregations.

**12. What is the most common cause of a `java.lang.OutOfMemoryError: Java heap space` on the Driver during tree training?**
- A) RDD lineage being too long
- B) Executor data partitions being heavily skewed
- C) Aggregated global histogram size exceeding driver heap space
- D) High cardinality of the target label variable
- **Answer:** C
- **Mastery Explanation:** At deeper levels, the number of active nodes doubles. The driver must hold $O(N_{active} \times F \times B)$ statistics. If `maxBins` or features are high, the histogram exhausts the driver's memory.

**13. How does Spark mitigate the risk of Driver OOM when the estimated histogram size exceeds `maxMemoryInMB`?**
- A) It spills the remaining histogram directly to the driver's local disk
- B) It groups active nodes and processes them in multiple sequential passes over the training data
- C) It randomly samples features to forcefully reduce the histogram size
- D) It halts training and returns the tree at the current depth
- **Answer:** B
- **Mastery Explanation:** Instead of crashing, Spark groups a subset of nodes that fit within `maxMemoryInMB` and launches a Spark job to aggregate them. It repeats this scanning the data multiple times until all nodes for that level are processed.

**14. You have a categorical feature with 15 categories for a binary classification problem. How does Spark optimize the split search?**
- A) It evaluates all $2^{14} - 1$ possible combinations
- B) It applies One-Hot Encoding implicitly
- C) It converts the categories to continuous floats
- D) It orders categories by target mean and evaluates exactly 14 splits
- **Answer:** D
- **Mastery Explanation:** For binary classification and regression, Spark orders categories by the proportion of positive labels or the target mean, reducing the exponential search space $O(2^{C-1})$ to linear $O(C)$.

**15. What is the Big-O time complexity of the Level Histogram Aggregation step?**
- A) $O(N \times F \times B)$
- B) $O(K \times F \times B)$
- C) $O(N \times K)$
- D) $O(B^2)$
- **Answer:** B
- **Mastery Explanation:** Where K is active nodes, F is features, and B is bins. The driver reduces histograms of this size, making it a function of the tree width and configuration, not the total row count N.

**16. What crucial metadata tagging function does `VectorIndexer` perform for Decision Trees?**
- A) It One-Hot Encodes categories with cardinality less than `maxCategories`
- B) It discretizes continuous features into exactly `maxBins` buckets
- C) It identifies features with < `maxCategories` unique values and tags them as categorical in the DataFrame schema
- D) It normalizes vector values to a [0, 1] range
- **Answer:** C
- **Mastery Explanation:** Spark relies on schema metadata to distinguish categorical from continuous variables inside the dense `VectorUDT`. `VectorIndexer` provides this metadata so the tree optimizer triggers subset splits.

**17. How is the `featureImportances` SparseVector computed in Spark?**
- A) By performing permutation importance on a held-out validation set
- B) By summing the total Gini/impurity reduction provided by each feature across all internal nodes
- C) By counting the absolute number of times a feature is used as a split
- D) By calculating SHAP values on the driver post-training
- **Answer:** B
- **Mastery Explanation:** Feature importance is a free byproduct of training. The impurity calculator sums the exact impurity reduction (e.g., Gini decrease) for every feature used at an `InternalNode`, normalized to sum to 1.0.

**18. What mechanism does Spark use to prevent DAGScheduler crashes when building deep trees?**
- A) `NodeIdCache`
- B) `treeAggregate`
- C) `checkpointInterval`
- D) `maxMemoryInMB`
- **Answer:** C
- **Mastery Explanation:** Checkpointing cuts the RDD lineage DAG. Since a deep tree adds complex map-reduce dependencies at every level, checkpointing every 10 levels prevents the DAG from growing too large and causing a `StackOverflowError`.

**19. Which metric does Spark use by default to evaluate exact information gain for regression trees?**
- A) Gini Impurity
- B) Entropy
- C) Variance Reduction
- D) Log-loss
- **Answer:** C
- **Mastery Explanation:** For continuous targets (regression), Spark evaluates the reduction in variance across the left and right child nodes compared to the parent node.

**20. During the Analysis phase, how does Spark determine the bin boundaries for continuous features?**
- A) Greenwald-Khanna algorithm on a data fraction to find approximate quantiles
- B) Exact distributed sorting of the entire dataset
- C) Random uniform distribution over the feature range
- D) K-Means clustering ($k = maxBins$)
- **Answer:** A
- **Mastery Explanation:** Spark uses a scalable approximate quantile algorithm (like Greenwald-Khanna) over a sample of the data to find thresholds, converting continuous features into discrete bin indices.

**21. What happens if you set `maxBins` to a value smaller than the cardinality of a categorical feature?**
- A) Spark groups the least frequent categories into a single bin
- B) Spark converts the feature to a continuous float
- C) Spark silently ignores the extra categories
- D) Spark throws an `IllegalArgumentException`
- **Answer:** D
- **Mastery Explanation:** Spark inherently limits categorical cardinality to `maxBins`. If $C > maxBins$, it cannot allocate enough bins in the histogram and fails fast with an error.

**22. The histogram aggregator tracks which sufficient statistics?**
- A) Raw feature values and row IDs
- B) Class counts (classification) or target sums/squares (regression)
- C) Information gain ratios per bin
- D) Residual errors for gradient boosting
- **Answer:** B
- **Mastery Explanation:** These sufficient statistics are all that is required to compute Gini impurity or variance for any split combination without needing the raw data.

**23. Why does increasing `maxMemoryInMB` from 256MB to 2048MB often reduce training time drastically?**
- A) It gives executors more RAM to cache data partitions
- B) It parallelizes the `treeAggregate` operation more effectively
- C) It allows the driver to hold larger histograms, preventing multi-pass data scans per level
- D) It increases the broadcast size limit for the model
- **Answer:** C
- **Mastery Explanation:** By raising the threshold, the driver can compute all histograms for a level in a single pass instead of forcing executors to scan the same training data 5-6 times per level.

**24. How is the updated global tree structure communicated from the Driver to Executors after a level is processed?**
- A) The driver broadcasts the new tree topology to all executors
- B) Spark sends the tree via Kryo serialization within RDD map closures
- C) Executors pull the tree from the `NodeIdCache`
- D) It is saved to HDFS and loaded by executors dynamically
- **Answer:** A
- **Mastery Explanation:** The driver determines the best splits globally, updates the tree AST, and uses Spark's broadcast variables to efficiently distribute the new tree structure to all executors for the next level.

**25. When extracting tree structure programmatically, what does an `InternalNode` contain?**
- A) The final prediction value
- B) The exact feature index and boundary threshold
- C) The RDD partition ID
- D) The raw data row indices
- **Answer:** B
- **Mastery Explanation:** An `InternalNode` contains a `split` object, which defines the optimal feature index and the threshold value (or categorical subsets) computed by the driver's ImpurityCalculator.

---

## Part 3: Small Twist Scenario Questions (15)

**26. Scenario:** You change a model from binary classification to multi-class (10 classes). One categorical feature has 20 categories.
- **Twist Consequence:** The training job hangs endlessly or crashes with high CPU/Memory on the driver.
- **Mastery Explanation:** The $O(C)$ target-mean trick is disabled for multi-class. Spark evaluates all $2^{19} - 1$ subsets on the driver, causing an exponential explosion in compute time.

**27. Scenario:** To capture finer patterns, you increase `maxBins` from 32 to 1024.
- **Twist Consequence:** The driver crashes with an `OutOfMemoryError` during `treeAggregate`.
- **Mastery Explanation:** Histogram size is directly proportional to `maxBins`. A 32x increase in bins causes a 32x increase in the driver memory required to hold the global stats for active nodes.

**28. Scenario:** You deploy to a massive cluster with 128GB RAM per executor, but you mistakenly set `maxMemoryInMB=16`.
- **Twist Consequence:** The job takes hours to process a single tree level despite massive resources.
- **Mastery Explanation:** The driver estimates histograms exceed 16MB instantly. It processes only a tiny fraction of nodes at a time, forcing executors to scan the entire dataset dozens of times per level (multi-pass degradation).

**29. Scenario:** You run `OneHotEncoder` on a "City" column (100 cities) before passing `features` to the DecisionTree.
- **Twist Consequence:** Model performance drops, and tree memory bloats.
- **Mastery Explanation:** OHE creates 100 sparse continuous features. Spark loses the ability to perform binary splits on subsets of cities, forcing highly unbalanced, inefficient trees and massive histogram bloat.

**30. Scenario:** You set `cacheNodeIds=True` and `checkpointInterval=5`, but forget to call `spark.sparkContext.setCheckpointDir()`.
- **Twist Consequence:** The application crashes with a `StackOverflowError` at deep levels.
- **Mastery Explanation:** `checkpointInterval` silently does nothing if a checkpoint directory is not configured on HDFS/S3, failing to truncate the massive RDD lineage.

**31. Scenario:** You train a deep tree (`maxDepth=20`) on a very small dataset (10,000 rows).
- **Twist Consequence:** The DAGScheduler crashes before data is even fully processed.
- **Mastery Explanation:** The depth of the RDD lineage (20 map-reduces) causes physical planning to stack overflow, regardless of the dataset being tiny.

**32. Scenario:** You set `maxBins=10`. You use `StringIndexer` on a column which outputs 15 unique indices, and feed it via `VectorIndexer`.
- **Twist Consequence:** `IllegalArgumentException` is thrown.
- **Mastery Explanation:** Spark requires `maxBins` to be at least as large as the maximum cardinality of any categorical feature to allocate enough histogram slots.

**33. Scenario:** You slice features using `featureImportances` indices, but use a Python UDF instead of `VectorSlicer`.
- **Twist Consequence:** Execution time increases by 10x due to serialization.
- **Mastery Explanation:** A UDF forces Spark to deserialize Tungsten `UnsafeRow` objects into Python objects, destroying the vector-accelerated metadata projection native to `VectorSlicer`.

**34. Scenario:** You run Spark locally (`local[*]`) with 32GB RAM, but leave `maxMemoryInMB=256`.
- **Twist Consequence:** Multi-pass data degradation still heavily impacts performance.
- **Mastery Explanation:** `maxMemoryInMB` governs the algorithmic logic of the histogram map-reduce, treating local mode exactly like a cluster and artificially throttling histogram aggregation.

**35. Scenario:** All your features are continuous floats. You add a `VectorIndexer(maxCategories=32)` to the pipeline anyway.
- **Twist Consequence:** Negligible performance impact; no features are tagged.
- **Mastery Explanation:** `VectorIndexer` safely scans the data, realizes all columns have > 32 unique values, and leaves the schema metadata as continuous. 

**36. Scenario:** In a custom Scala AST traversal, you attempt to access `split.featureIndex` on a `LeafNode`.
- **Twist Consequence:** Compile-time or Runtime error.
- **Mastery Explanation:** A `LeafNode` represents a terminal prediction and has no `split` condition object attached to it.

**37. Scenario:** You have a `user_id` column with 1,000,000 unique values. You `StringIndex` it and set `maxBins=1000000`.
- **Twist Consequence:** Driver OOM instantly during histogram allocation.
- **Mastery Explanation:** Creating an array of $1,000,000 \times \text{features} \times \text{nodes}$ stats objects easily requires terabytes of driver RAM. High-cardinality IDs should never be fed into tree algorithms.

**38. Scenario:** You over-partition your training data into 20,000 partitions to increase parallelism.
- **Twist Consequence:** The `treeAggregate` step hangs or crashes the driver.
- **Mastery Explanation:** The driver must receive and reduce 20,000 massive histogram arrays over the network. Network I/O and CPU reduction on the single driver node becomes a massive bottleneck.

**39. Scenario:** You call `df.cache()` to fix a `StackOverflowError` instead of using `checkpointInterval`.
- **Twist Consequence:** The `StackOverflowError` still occurs.
- **Mastery Explanation:** `df.cache()` materializes the initial DataFrame, but it does NOT truncate the massive nested lineage generated internally *during* the loop of level-by-level map-reduces.

**40. Scenario:** You set `maxDepth=35`.
- **Twist Consequence:** `IllegalArgumentException` is thrown.
- **Mastery Explanation:** Spark MLlib hard-limits `maxDepth` to 30. Depths beyond 30 would require over 1 billion active nodes, guaranteeing an integer overflow and immediate driver OOM.

---

## Part 4: Coding & Debugging (10)

**41. Debugging Scenario:**
```python
# Code snippet
encoder = OneHotEncoder(inputCol="city_idx", outputCol="city_vec")
assembler = VectorAssembler(inputCols=["city_vec", "age"], outputCol="features")
dt = DecisionTreeClassifier(featuresCol="features")
```
- **Flaw:** Using `OneHotEncoder` bloats memory and prevents binary categorical splits.
- **Fix:** Remove `OneHotEncoder`. Use `VectorIndexer(maxCategories=...)` on the assembled output containing the raw `city_idx`.

**42. Debugging Scenario:**
- **Log Error:** `java.lang.OutOfMemoryError: Java heap space` on the Driver at Depth 8.
- **Flaw:** The global histogram size at Depth 8 ($2^8 = 256$ active nodes) multiplied by `maxBins` exceeded Driver memory.
- **Fix:** Decrease `maxBins`, drop noisy features, or increase `--driver-memory`.

**43. Debugging Scenario:**
- **Log Error:** `StackOverflowError` in `DAGScheduler` at Depth 12.
- **Flaw:** RDD lineage exceeded JVM stack limits during physical planning.
- **Fix:** Define a checkpoint directory `spark.sparkContext.setCheckpointDir("path/")` and configure the tree with `checkpointInterval=10`.

**44. Debugging Scenario:**
- **Symptom:** Spark UI shows 45 separate jobs executed just for Depth 7. Training takes hours.
- **Flaw:** The histogram size exceeded `maxMemoryInMB` (default 256MB), causing Spark to process active nodes in 45 sequential passes.
- **Fix:** Set `maxMemoryInMB=2048` or higher on the `DecisionTreeClassifier`.

**45. Debugging Scenario:**
- **Log Error:** `IllegalArgumentException: DecisionTree requires maxBins (32) >= maxCategories (50)`
- **Flaw:** A categorical feature was indexed with 50 unique values, but bins were constrained to 32.
- **Fix:** Set `maxBins=50` or higher, or filter out rare categories prior to indexing.

**46. Coding Task:** Write logic to extract the top feature index based on importance.
- **Solution:**
```python
importances = model.featureImportances
top_feature_idx = int(importances.indices[importances.values.argmax()])
```

**47. Coding Task:** Optimize the DataFrame by dropping all features except the top 5 using native Tungsten methods.
- **Solution:**
```python
top_5_indices = sorted(zip(importances.indices, importances.values), key=lambda x: x[1], reverse=True)[:5]
indices_only = [idx for idx, val in top_5_indices]
slicer = VectorSlicer(inputCol="features", outputCol="pruned", indices=indices_only)
df_optimized = slicer.transform(df)
```

**48. Debugging Scenario:**
- **Log Error:** `java.lang.OutOfMemoryError: GC overhead limit exceeded` on the *Executors* during the first action (before level processing).
- **Flaw:** The Greenwald-Khanna quantile calculation on continuous features is exhausting executor memory on massive partitions.
- **Fix:** Increase `--executor-memory`, increase data partitions (repartition), or sub-sample the data prior to training.

**49. Coding Task:** Configure a `DecisionTreeClassifier` for maximum memory efficiency on a deep tree (Depth 15).
- **Solution:**
```python
dt = DecisionTreeClassifier(
    maxDepth=15, 
    maxBins=32, 
    cacheNodeIds=True, 
    checkpointInterval=5, 
    maxMemoryInMB=2048
)
```

**50. Debugging Scenario:**
- **Symptom:** Training AUC is 0.99, but Test AUC is 0.55. Depth is 20.
- **Flaw:** Catastrophic overfitting due to unbounded leaf nodes.
- **Fix:** Constrain the tree by reducing `maxDepth`, or setting `minInstancesPerNode=100` and `minInfoGain=0.01` to enforce early stopping and regularization.
