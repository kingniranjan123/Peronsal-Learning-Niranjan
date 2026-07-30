# K-Means Clustering: Elite Technical Assessment

## Part 1: True/False Questions

1. **True/False:** In Spark MLlib's K-Means implementation, partial sums and counts are accumulated within executors using Tungsten's unsafe binary row format to avoid JVM GC overhead.
**Answer:** False. 
**Mastery Explanation:** Accumulation happens entirely in JVM heap memory, bypassing Tungsten's unsafe row format because centroids are mutable floating-point accumulators. Tungsten is used for reading the input rows, not accumulating.

2. **True/False:** K-Means in Spark MLlib uses Catalyst optimizer to push down distance computations into the data source.
**Answer:** False. 
**Mastery Explanation:** MLlib's K-Means operates below the DataFrame abstraction at the RDD level after the initial `fit` triggers a `dataset.rdd` conversion through the `RowToVector` internal transformer, meaning Catalyst is not involved in the iterative loop.

3. **True/False:** The assignment step in Spark's K-Means implementation triggers a shuffle to group all points belonging to the same cluster centroid.
**Answer:** False. 
**Mastery Explanation:** The assignment step is embarrassingly parallel and requires no shuffle. Data never leaves its partition. Instead, the centroids are broadcast to each partition, and local points are assigned and aggregated.

4. **True/False:** By default, `treeAggregate` with a merge depth of 2 is used to combine partial aggregates from executors, reducing driver ingress from P messages to log₂(P) messages.
**Answer:** True. 
**Mastery Explanation:** This two-level binary-tree reduce on executors drastically cuts down Driver GC pressure and receive operations, preventing driver OOM when k·d is large.

5. **True/False:** Spark’s implementation of K-Means reinitializes empty clusters by stealing a point from the largest cluster to prevent a reduction in effective k.
**Answer:** False. 
**Mastery Explanation:** Spark retains the empty centroid unchanged, leaving it with a count of zero. This silent failure results in effectively k-1 clusters and higher WCSS.

6. **True/False:** When using `initMode="k-means||"`, the initialization phase runs exactly k full-dataset passes to select the initial centroids.
**Answer:** False. 
**Mastery Explanation:** `k-means||` over-samples in each pass, reducing the number of full-dataset passes from k to O(log n), which is much more efficient than sequential k-means++.

7. **True/False:** The `ClusteringEvaluator` in Spark computes the silhouette score in O(n²) time by computing pairwise distances between all points in the dataset.
**Answer:** False. 
**Mastery Explanation:** It avoids O(n²) by exploiting the squared Euclidean identity, computing per-cluster statistics via broadcast, reducing complexity to O(n·k).

8. **True/False:** `BisectingKMeans` guarantees a lower or equal WCSS compared to standard K-Means for the same k.
**Answer:** False. 
**Mastery Explanation:** BisectingKMeans is a greedy divisive algorithm. Once a split is made, it is not reconsidered globally. It is computationally faster but often produces a slightly suboptimal WCSS compared to Lloyd's algorithm.

9. **True/False:** During the iteration loop of Spark's K-Means, if the maximum L2 distance between old and new centroids falls below `tol`, the loop terminates early.
**Answer:** True. 
**Mastery Explanation:** The Driver computes the centroid shift after each iteration, and convergence is reached when this shift falls below the `tol` parameter, bypassing `maxIter`.

10. **True/False:** To optimize memory during hyperparameter tuning of k, you should persist the feature DataFrame using `MEMORY_AND_DISK_SER` with Kryo serialization rather than default `MEMORY_AND_DISK` when dealing with very wide feature vectors.
**Answer:** True. 
**Mastery Explanation:** Java object serialization (the default) has a massive memory footprint. Kryo serialization halves the in-memory footprint, essential for very wide vectors.

## Part 2: Multiple Choice Questions

11. Which Spark mechanism is used to distribute the centroid array to all executors during the main iteration loop?
A) Shuffle
B) Catalyst BroadcastHashJoin
C) TorrentBroadcast
D) RDD treeAggregate
**Answer:** C
**Mastery Explanation:** `TorrentBroadcast` uses a BitTorrent-like P2P mechanism to distribute the `k × d` centroid array without bottlenecking the driver.

12. How does `treeAggregate` prevent the Driver from becoming a bottleneck during the centroid update step?
A) It performs partial reduction directly on the data source.
B) It triggers a shuffle to sort the aggregates before sending to the driver.
C) It performs a binary tree reduction on the executors before sending results to the Driver.
D) It drops smaller partial sums to save bandwidth.
**Answer:** C
**Mastery Explanation:** `treeAggregate` does a multi-level reduce on the executors, reducing the number of messages the driver receives from P to log₂(P).

13. What library does Spark MLlib delegate distance computations to for optimal performance?
A) Apache Commons Math
B) Tungsten BLAS
C) native BLAS (OpenBLAS/MKL) via netlib-java
D) Catalyst JIT Compiler
**Answer:** C
**Mastery Explanation:** MLlib uses BLAS-accelerated `DDOT` operations via `netlib-java`. Without it, it falls back to pure-Java F2J which is 3-10x slower.

14. How many Spark jobs are triggered during the main K-Means iteration loop if `maxIter` is 20 and convergence is NOT reached?
A) 1 job
B) 20 jobs
C) 40 jobs (1 assignment + 1 update per iteration)
D) 0 jobs (all done via map-side Catalyst operations)
**Answer:** B
**Mastery Explanation:** Each iteration is exactly 1 Spark job containing 1 Stage with no shuffle.

15. What is the time complexity of the centroid update step (treeAggregate) per iteration?
A) O(n · k · d)
B) O(n · d + k · d · log P)
C) O(n²)
D) O(k · n)
**Answer:** B
**Mastery Explanation:** The assignment is O(n·k·d), but the update step requires O(n·d) for local sums and O(k·d·log P) for the tree reduction.

16. What happens if two initial centroids land in the exact same dense region in standard K-Means?
A) One steals half the points of the other.
B) The algorithm throws an EmptyClusterException.
C) One centroid receives zero points and remains unchanged, effectively reducing k to k-1.
D) They are merged and a new random centroid is chosen.
**Answer:** C
**Mastery Explanation:** Spark retains the centroid unchanged. It will show up with a size of zero in `model.summary.clusterSizes`.

17. Which is a true statement regarding `KMeansModel.transform()`?
A) It performs a global shuffle to colocate points in the same cluster.
B) It relies on `aggregateByKey` to assign clusters.
C) It is a single map-side operation with a broadcast, causing zero shuffles.
D) It converts the DataFrame back to an RDD to run `treeAggregate`.
**Answer:** C
**Mastery Explanation:** `transform()` broadcasts the final centroids and assigns each row its nearest cluster without any shuffle.

18. What does the `initSteps` parameter in `initMode="k-means||"` control?
A) The maximum number of Lloyd's iterations.
B) The depth of the `treeAggregate` binary tree.
C) The number of over-sampling passes run during initialization.
D) The number of random seeds to try simultaneously.
**Answer:** C
**Mastery Explanation:** It determines how many over-sampling passes to run. Higher values reduce WCSS variance across runs but add extra initialization Spark jobs.

19. Why is `StandardScaler` critical before applying K-Means?
A) K-Means cannot accept negative values.
B) Euclidean distance is scale-sensitive, so features with larger ranges will dominate.
C) It allows Catalyst to push down predicates.
D) It converts sparse vectors into dense vectors.
**Answer:** B
**Mastery Explanation:** Euclidean distance gives equal weight to absolute numerical differences. Without scaling, larger scale features dominate the distance metric.

20. What is the time complexity of a BisectingKMeans fit?
A) O(n · k · d · iters)
B) O(n · d · log k)
C) O(n² · log k)
D) O(k · n · d)
**Answer:** B
**Mastery Explanation:** It runs 2-means recursively, so each level touches fewer points. The total complexity drops to O(n·d·log k).

21. What issue occurs if `features_df.cache()` is omitted when running an elbow method sweep over 10 values of k?
A) Catalyst will throw a cache-miss exception.
B) The feature vectors will be serialized using Kryo instead of Java.
C) Spark will re-read, re-parse, and re-scale the source Parquet files for every value of k.
D) The `ClusteringEvaluator` will return -1.
**Answer:** C
**Mastery Explanation:** Without caching, the entire lineage (including StandardScaler) is recomputed for each `KMeans.fit()`.

22. How is WCSS (trainingCost) accessed after a `KMeans.fit()`?
A) By running `ClusteringEvaluator(metricName="wcss")`
B) It is free and available via `model.summary.trainingCost`
C) By triggering a custom RDD map-reduce job
D) By inspecting the Spark UI's DAG scheduler
**Answer:** B
**Mastery Explanation:** WCSS is computed as a side effect of the final iteration's aggregation and stored in the summary object without needing an extra job.

23. Which serialization protocol is recommended to minimize `TorrentBroadcast` overhead for large k and d?
A) Java Serialization
B) Kryo Serialization
C) Protobuf
D) Arrow
**Answer:** B
**Mastery Explanation:** Enabling `spark.serializer=KryoSerializer` reduces the broadcast size of centroid arrays by ~40% over default Java serialization.

24. What does the silhouette score measure?
A) The absolute sum of squared distances to centroids.
B) The ratio of intra-cluster distance to nearest-cluster distance for each point.
C) The number of empty clusters.
D) The computational cost of the `treeAggregate` step.
**Answer:** B
**Mastery Explanation:** It ranges from -1 to 1 and compares intra-cluster distance (cohesion) to nearest-cluster distance (separation).

25. Why might a Spark UI show exactly 50 jobs for a K-Means loop when `maxIter=50`?
A) It means the algorithm found the global minimum perfectly.
B) It means the algorithm hit the iteration cap without converging.
C) It means 50 different seeds were used.
D) It means `initSteps` was set to 50.
**Answer:** B
**Mastery Explanation:** If exactly `maxIter` jobs ran, it signifies early termination did not occur, meaning the centroid shift never fell below `tol`.

## Part 3: Small Twist Questions

26. **Twist:** You change `initMode` from `k-means||` to `random`. What changes in the initialization performance?
A) It requires more Spark jobs.
B) Initialization becomes O(1) Spark jobs, drastically speeding up start time but risking poor WCSS.
C) It triggers a shuffle.
D) It requires native BLAS.
**Answer:** B
**Mastery Explanation:** `random` just picks initial points without the O(log n) passes of `k-means||`, making it faster but highly prone to degenerate empty clusters.

27. **Twist:** You change the input data to use `SparseVector` instead of `DenseVector`. How does this affect distance computation?
A) It fails because MLlib K-Means only supports DenseVector.
B) BLAS `DDOT` will no longer be used; it falls back to a sparse distance algorithm, changing memory access patterns.
C) Tungsten automatically converts it to DenseVector.
D) The model will predict 0 for all clusters.
**Answer:** B
**Mastery Explanation:** Sparse vectors use specialized sparse BLAS or iterative distance calculations, which can be faster for high-sparsity data but have overhead if data isn't sparse enough.

28. **Twist:** You set `k=1`. What happens to the silhouette score?
A) It is 1.0.
B) It is 0.0.
C) It cannot be computed and will throw an error or return NaN.
D) It equals WCSS.
**Answer:** C
**Mastery Explanation:** Silhouette requires comparing to a nearest *other* cluster. With k=1, there is no other cluster.

29. **Twist:** You change `treeAggregate` depth from 2 to 1 (conceptually via custom override). What happens on a 10,000 partition cluster?
A) Executor GC pressure increases.
B) Driver GC pressure increases massively, potentially causing an OOM.
C) A shuffle is forced.
D) Nothing changes.
**Answer:** B
**Mastery Explanation:** A depth of 1 means all 10,000 executors send their k·d arrays directly to the Driver simultaneously, causing an ingress bottleneck and OOM.

30. **Twist:** You run `StandardScaler` with `withMean=False` instead of `True` on dense data. How does this impact K-Means?
A) It fails to run.
B) The features are scaled by stddev but not centered. The clustering will still group correctly relative to variance, but centroids will be shifted.
C) It triggers an extra Spark job.
D) It forces K-Means to use Cosine distance.
**Answer:** B
**Mastery Explanation:** `withMean=False` divides by stddev only. This is often used for SparseVectors (where centering ruins sparsity).

31. **Twist:** You increase `initSteps` from 2 to 10 in `k-means||`. What happens to job execution?
A) 8 additional Spark jobs are added to the initialization phase.
B) The number of iterations in the main loop decreases.
C) It forces BisectingKMeans.
D) It reduces the `TorrentBroadcast` payload size.
**Answer:** A
**Mastery Explanation:** Each `initStep` is a separate full-dataset pass (Spark job) to over-sample candidates.

32. **Twist:** You set `tol=0.0`. What happens to the iteration loop?
A) It converges immediately.
B) It guarantees the loop will run for exactly `maxIter` iterations.
C) It causes a divide-by-zero error.
D) It triggers a shuffle.
**Answer:** B
**Mastery Explanation:** With `tol=0.0`, the centroid shift will essentially never be strictly less than 0.0 due to floating-point jitter, forcing `maxIter` to be reached.

33. **Twist:** You switch `distanceMeasure` from `squaredEuclidean` to `cosine`. How does the centroid update step change conceptually?
A) Centroids are normalized to unit length after the average is computed.
B) Centroids are updated via treeAggregate with depth 4.
C) The assignment step requires a shuffle.
D) The algorithm becomes BisectingKMeans.
**Answer:** A
**Mastery Explanation:** For cosine distance (Spherical K-Means), centroids must be L2-normalized after each update step.

34. **Twist:** You run `kmeans.fit(df)` on a DataFrame with 1 partition. What is the impact of `treeAggregate`?
A) It crashes.
B) `treeAggregate` simply acts as a local fold; the multi-level tree reduction is bypassed since P=1.
C) It causes the driver to OOM.
D) It triggers a partition rebalance.
**Answer:** B
**Mastery Explanation:** If there is only 1 partition, there is no tree reduction to perform across executors. It just sends the single result to the driver.

35. **Twist:** You change `spark.sql.shuffle.partitions` from 200 to 2000 before running `KMeans.fit()`. How does this affect the K-Means loop?
A) The iteration loop takes 10x longer due to shuffle overhead.
B) It has no direct effect on the K-Means loop because K-Means operates on the underlying RDD partitions.
C) It changes the `treeAggregate` depth automatically.
D) It increases the broadcast size.
**Answer:** B
**Mastery Explanation:** `spark.sql.shuffle.partitions` only affects DataFrame operations that trigger a shuffle (like groupBy). K-Means has no shuffle.

36. **Twist:** You change `minDivisibleClusterSize` in BisectingKMeans from 20 to 0.5. What does this mean?
A) Clusters with less than 0.5 distance variance won't be split.
B) Clusters containing less than 50% of the total dataset points won't be split.
C) It throws a type error.
D) It limits the tree depth to 0.5.
**Answer:** B
**Mastery Explanation:** If it's a fraction [0,1], it's treated as a fraction of the total dataset size.

37. **Twist:** You save `KMeansModel` instead of the `PipelineModel`. What happens at inference time when scoring new data?
A) It fails because the model format is incompatible.
B) New data won't be scaled correctly unless you manually re-apply and fit a new StandardScaler, changing stats.
C) Nothing, it works identically.
D) It triggers a shuffle on inference.
**Answer:** B
**Mastery Explanation:** Saving only the `KMeansModel` loses the `StandardScalerModel` stats. A new scaler fit would use inference data stats, violating ML principles.

38. **Twist:** You run `features_df.unpersist()` before calling `ClusteringEvaluator.evaluate()`. What happens?
A) Silhouette score is 0.
B) `evaluate()` throws an error.
C) Spark re-reads and re-processes the raw data from source to compute the silhouette score.
D) The driver crashes.
**Answer:** C
**Mastery Explanation:** Unpersisting clears the cache. `evaluate()` triggers an action, forcing the re-computation of the entire lineage.

39. **Twist:** You set `k=1000` and `d=2000`. What is the most likely bottleneck?
A) The Catalyst optimizer times out.
B) `TorrentBroadcast` payload becomes very large (~16MB per task), and the driver struggles to aggregate the arrays.
C) The shuffle spill to disk.
D) Tungsten row generation.
**Answer:** B
**Mastery Explanation:** k·d = 2,000,000 doubles = ~16MB. In `treeAggregate`, summing these large arrays creates massive heap objects and GC pressure.

40. **Twist:** You forget to set a random `seed` in `KMeans()`. You run `.fit()` twice on the same cached dataset. What happens?
A) WCSS is guaranteed to be identical.
B) WCSS will likely differ because k-means|| will pick different initial centroids.
C) Spark throws an exception.
D) The iteration count is fixed to 1.
**Answer:** B
**Mastery Explanation:** Without a fixed seed, initialization is stochastic. The final clusters and WCSS will vary between runs.

## Part 4: Coding & Debugging Questions

41. **Bug Identification:** A developer complains their K-Means job is OOMing the Driver. k=5000, d=1000. `spark.driver.memory=4g`. What is the fundamental issue?
**Answer:** The partial sums arrays generated by `treeAggregate` are 5000x1000 doubles (~40MB each). Even with depth=2, the driver receives multiple 40MB arrays and tries to reduce them.
**Mastery Explanation:** High k and d cause massive partial aggregates. To fix this, increase driver memory or increase the tree depth (`treeAggregate(depth=3+)`) to push more reduction to executors.

42. **Logic Error:** You see the following code:
```python
scaler = StandardScaler(withMean=True, withStd=True)
km = KMeans(k=10)
pipe = Pipeline(stages=[km, scaler])
```
What is wrong here?
**Answer:** The pipeline stages are out of order. K-Means is receiving raw unscaled features.
**Mastery Explanation:** K-Means uses Euclidean distance, making it extremely sensitive to feature magnitude. `scaler` must be before `km` in the `stages` array.

43. **Performance Leak:**
```python
for k in [10, 20, 30]:
    km = KMeans(k=k, featuresCol="features").fit(df)
    print(km.summary.trainingCost)
```
Why is this incredibly slow?
**Answer:** `df` is not cached.
**Mastery Explanation:** K-Means triggers multiple jobs per fit. Without `df.cache()`, every single iteration of every single k-candidate re-reads the data from the source (e.g., S3/HDFS).

44. **Silent Failure:** You run a clustering job and `model.summary.clusterSizes` returns `[500000, 450000, 0, 50000]`. What happened, and what is the consequence?
**Answer:** An empty cluster occurred due to degenerate initialization. 
**Mastery Explanation:** The centroid for the 0-count cluster was placed too far away and claimed no points. It effectively means you have k=3, and the WCSS is mathematically suboptimal. You should run multiple seeds.

45. **Logic Error:** 
```python
evaluator = ClusteringEvaluator(metricName="silhouette")
wcss = evaluator.evaluate(model.transform(df))
```
What is wrong with this code?
**Answer:** The evaluator computes the silhouette score, not WCSS.
**Mastery Explanation:** Silhouette is returned by `evaluator.evaluate()`. WCSS is accessed directly via `model.summary.trainingCost` without needing an evaluator.

46. **Memory Leak / Spill:** A K-Means task log shows: `WARN netlib.BLAS: Failed to load implementation from: com.github.fommil.netlib.NativeSystemBLAS`. What is the impact?
**Answer:** The job falls back to `f2jBLAS` (pure Java).
**Mastery Explanation:** Distance computations (`DDOT`) will be 3-10x slower because native C/Fortran vectorization (OpenBLAS/MKL) is missing.

47. **Optimizer Blocker:** 
```python
df.selectExpr("cast(feat1 as double)", "cast(feat2 as double)").cache()
assembler = VectorAssembler(inputCols=["feat1", "feat2"], outputCol="features")
```
Why is caching here suboptimal compared to caching after the assembler?
**Answer:** Caching raw columns caches Row objects. 
**Mastery Explanation:** Caching after `VectorAssembler` stores `DenseVector` objects. K-Means needs the `Vector`. By caching before, Spark runs the assembler map-step on every K-Means iteration.

48. **Debugging WCSS:** You run K-Means on two datasets. Dataset A has WCSS=1,000,000. Dataset B has WCSS=500. Can you conclude Dataset B is better clustered?
**Answer:** No. 
**Mastery Explanation:** WCSS is scale-dependent and n-dependent. If Dataset A has 10x more points or features with larger variance, its WCSS will be naturally higher. WCSS cannot be compared across different datasets.

49. **Logic Error:** 
```python
bkm = BisectingKMeans(k=100, minDivisibleClusterSize=0.9)
```
What is the consequence of `minDivisibleClusterSize=0.9`?
**Answer:** The algorithm will almost immediately stop splitting.
**Mastery Explanation:** 0.9 means a cluster must contain 90% of the entire dataset to be split. After the first split (e.g., 60% / 40%), neither child is >= 90%, so bisection terminates prematurely.

50. **Resource Tuning:** During K-Means, you see `TorrentBroadcast` taking a long time. You have k=10000, d=2048. How do you mitigate the broadcast overhead without changing k or d?
**Answer:** Set `spark.serializer=org.apache.spark.serializer.KryoSerializer`.
**Mastery Explanation:** The centroid array is massive (~160MB). Java serialization is bloated. Kryo reduces this by ~40%, speeding up the P2P broadcast and reducing network/memory pressure.
