# ML Basics Assessment

## Part 1: True/False Questions (10 Questions)

**1. Spark Catalyst Optimizer natively optimizes the mathematical gradient descent steps within MLlib algorithms.**
* **Answer:** False
* **Mastery Explanation:** Catalyst optimizes the data preparation steps (predicate pushdown, column pruning, Whole-Stage CodeGen) for feature engineering. However, the mathematical optimization (like gradient math) is executed via BLAS/LAPACK natively or distributed map-reduce patterns (e.g., treeAggregate), not optimized by Catalyst.

**2. By default, standard Java serialization is used to transmit dense vectors over the network during MLlib model training.**
* **Answer:** False
* **Mastery Explanation:** MLlib relies heavily on Kryo serialization to efficiently transmit dense vectors. Standard Java serialization incurs a catastrophic performance penalty and is avoided for large vector transfers.

**3. When `netlib-java` fails to find native BLAS/LAPACK libraries, Spark ML silently falls back to a Java implementation.**
* **Answer:** True
* **Mastery Explanation:** Spark ML uses `netlib-java` to hook into native C/Fortran libraries for low-level matrix math (SIMD instructions). If missing, it silently falls back to a severely bottlenecked Java implementation, severely degrading performance.

**4. A `SparseVector` should always be converted to a `DenseVector` before feeding it into Random Forest algorithms in Spark to improve performance.**
* **Answer:** False
* **Mastery Explanation:** Random Forest and Naive Bayes in Spark heavily leverage sparse structures. Forcing a conversion from sparse to dense artificially degrades performance and causes a massive memory footprint explosion, potentially leading to OOM errors.

**5. Spark ML Transformers rely on Tungsten's Whole-Stage Codegen for rapid row-by-row mapping without breaking the Catalyst execution pipeline.**
* **Answer:** True
* **Mastery Explanation:** Transformers are deterministic functions that append new columns. Tungsten's Whole-Stage Codegen allows them to execute rapidly in a vectorized manner, fitting seamlessly into the Catalyst pipeline.

**6. Iterative algorithms like ALS can cause `OutOfMemoryError` on executor nodes due to linear DAG growth.**
* **Answer:** False
* **Mastery Explanation:** The massive DAG linear growth causes a `StackOverflowError` or memory exhaustion in the **Driver JVM** (specifically tracking task dependencies in the DAGScheduler), not typically an OOM on the executors.

**7. Checkpointing an iterative ML algorithm prevents Driver StackOverflows but incurs heavy disk I/O and materialization costs.**
* **Answer:** True
* **Mastery Explanation:** Checkpointing truncates the DAG lineage by writing intermediate RDDs to reliable storage (like HDFS). This saves the Driver from DAG explosion but forces materialization and I/O writes, meaning it must be balanced carefully.

**8. `treeAggregate` minimizes the driver bottleneck by aggregating data on the Driver JVM using multiple threads.**
* **Answer:** False
* **Mastery Explanation:** `treeAggregate` minimizes the driver bottleneck by hierarchically aggregating results *across executors* before sending the final sum to the Driver, avoiding the star-topology bottleneck of standard `reduce()`.

**9. `CrossValidator` in Spark ML parallelizes the evaluation of hyperparameter combinations across the cluster by default.**
* **Answer:** False
* **Mastery Explanation:** By default, `CrossValidator` evaluates models sequentially. Engineers must explicitly use `.setParallelism(n)` to spawn a ThreadPool on the Driver and submit asynchronous Spark Jobs.

**10. Vector UDTs in Spark ML are stored as standard Java objects on the JVM heap to ensure rapid access.**
* **Answer:** False
* **Mastery Explanation:** Vector UDTs (`DenseVector` and `SparseVector`) compress feature arrays and are internally represented in Tungsten's binary format, packed into off-heap memory to bypass JVM heap limits and reduce GC overhead.

## Part 2: Multiple Choice Questions (15 Questions)

**11. Which component is responsible for heavily optimizing data preparation phases like predicate pushdown and column pruning in Spark ML Pipelines?**
A) Tungsten execution engine
B) Catalyst optimizer
C) MLlib physical planner
D) DAGScheduler
* **Answer:** B
* **Mastery Explanation:** The Catalyst optimizer fiercely optimizes data preparation steps (feature engineering phases) through logical and physical planning before execution. Tungsten handles the low-level execution and memory formatting.

**12. When processing a dataset with 10,000 distinct categorical values, what is the consequence of applying a transformer that accidentally converts `SparseVector` to `DenseVector`?**
A) The Catalyst optimizer will prune the unnecessary dimensions.
B) The vectors will be moved to off-heap memory, improving GC performance.
C) The memory footprint will explode exponentially, likely causing fatal OOM errors.
D) The model accuracy will decrease due to zero-padding.
* **Answer:** C
* **Mastery Explanation:** Spark vectors are treated as opaque blobs by Catalyst, so it cannot optimize them. Converting sparse high-cardinality vectors to dense forces the allocation of massive arrays of zeros, exhausting JVM heap space and causing massive GC pauses and OOM errors.

**13. In iterative ML algorithms like Alternating Least Squares (ALS), what is the primary reason for utilizing `setCheckpointInterval(5)`?**
A) To save the model weights to HDFS in case of executor failure.
B) To truncate the linearly growing DAG lineage and prevent Driver JVM StackOverflows.
C) To force Spark to use Kryo serialization instead of Java serialization.
D) To trigger Catalyst's Cost-Based Optimizer (CBO) on intermediate results.
* **Answer:** B
* **Mastery Explanation:** Iterative algorithms recursively compute over the same dataset, causing the lazy-evaluation DAG to grow linearly. Without checkpointing, the DAG becomes so massive it causes a StackOverflowError in the Driver's DAGScheduler.

**14. How does Spark MLlib internally prevent network bottlenecks during the computation of distributed gradients?**
A) By using `reduceByKey` to aggregate gradients on a designated master executor.
B) By leveraging `treeAggregate` to perform hierarchical reductions on executors before returning to the Driver.
C) By serializing the gradients into Parquet files and reading them sequentially.
D) By increasing the size of the Driver's network buffer.
* **Answer:** B
* **Mastery Explanation:** Standard `reduce` blasts all partial results to the Driver directly (star-topology). `treeAggregate` hierarchically merges results on the executors (tree-topology), drastically reducing network payload to the Driver.

**15. What is the role of `netlib-java` in Spark ML execution?**
A) It serializes complex Java objects for network transmission.
B) It binds Spark ML to native C/Fortran libraries (BLAS/LAPACK) for SIMD-accelerated matrix math.
C) It manages the off-heap Tungsten memory allocations for DataFrames.
D) It provides a bridge between Python UDFs and Scala implementations.
* **Answer:** B
* **Mastery Explanation:** Spark ML uses `netlib-java` to hook into native BLAS/LAPACK libraries for low-level matrix math. This ensures vector dot products run close to bare-metal speed using hardware-specific instructions.

**16. Why do senior engineers use `while` loops and in-place array mutation in custom `seqOp` functions for `treeAggregate`?**
A) Scala `for` loops are incompatible with Catalyst CodeGen.
B) To completely bypass Scala's boxing/unboxing and iterator object creation overhead.
C) In-place mutations are required to force Tungsten into off-heap mode.
D) `treeAggregate` strictly requires mutable collections as arguments.
* **Answer:** B
* **Mastery Explanation:** In a tight loop executing millions of times per partition, standard functional paradigms (like mapping or reducing iterators) create massive object allocation overhead. In-place mutation and while loops avoid object creation, reducing GC pressure by up to 80%.

**17. When configuring a `CrossValidator` with 3 folds and a parameter grid of 6 combinations, what is the fastest way to execute this pipeline?**
A) Rely on Catalyst to automatically unroll the parameter grid into a single physical plan.
B) Set `setParallelism(4)` to allow the Driver JVM to submit asynchronous Spark jobs.
C) Increase the executor memory to allow caching of all 18 models simultaneously.
D) Use a custom `VectorAssembler` to combine hyperparameters into a dense vector.
* **Answer:** B
* **Mastery Explanation:** `CrossValidator` trains models sequentially by default. Using `setParallelism` spawns a ThreadPool on the Driver to submit jobs concurrently, leveraging cluster resources efficiently (especially with FAIR scheduling).

**18. What happens to the memory representation of data when passed through a `VectorAssembler`?**
A) It is converted into a standard Java Array[Double] on the JVM heap.
B) It combines columns into Vector UDTs tightly packed in Tungsten off-heap memory.
C) It serializes all columns into Kryo byte arrays to save memory.
D) It converts sparse features into DenseVectors for Catalyst optimization.
* **Answer:** B
* **Mastery Explanation:** `VectorAssembler` creates Vector UDTs (either Sparse or Dense, depending on efficiency) which are internally represented in Tungsten's binary format off-heap, bypassing JVM object overhead.

**19. Why does setting `setDropLast(true)` on `OneHotEncoder` matter for specific ML algorithms?**
A) It prevents the sparse vector memory from exploding by dropping the most frequent category.
B) It breaks linear dependency (the dummy variable trap) required for matrix inversion in OLS regression.
C) It prevents Catalyst from attempting to push down predicates on the encoded columns.
D) It drops the last iteration of an algorithm to prevent overfitting.
* **Answer:** B
* **Mastery Explanation:** For algorithms that require matrix inversion (like Linear Regression without regularization), keeping all one-hot encoded columns introduces perfect multicollinearity. Dropping one column breaks this linear dependency.

**20. Which phase of the Catalyst optimizer directly optimizes the mathematical gradients computed by a Logistic Regression model?**
A) Logical Optimization
B) Physical Planning
C) Whole-Stage CodeGen
D) None of the above
* **Answer:** D
* **Mastery Explanation:** Catalyst fiercely optimizes the *data preparation* phases (feature engineering), but it does *not* optimize the gradient math itself. The algorithm optimization relies on distributed map-reduce patterns and native BLAS.

**21. A Random Forest model in Spark ML is considered highly shuffle-intensive. Why?**
A) The Driver must broadcast the entire dataset to every executor for each tree.
B) Workers compute split statistics locally and driver must coordinate tree growth via shuffles.
C) Each tree is trained on a separate executor, requiring data to be partitioned by tree ID.
D) The algorithm requires sorting the entire dataset by feature importance globally.
* **Answer:** B
* **Mastery Explanation:** Random forest training involves executors computing aggregate statistics for potential splits on their local data partitions. These statistics must be shuffled/aggregated back to coordinate the global tree growth strategy.

**22. If you do not configure a checkpoint directory but attempt to use `setCheckpointInterval()` on an ALS Estimator, what happens?**
A) The algorithm falls back to caching RDDs in `MEMORY_AND_DISK`.
B) The job fails immediately because a checkpoint directory is required.
C) The job runs but checkpoints are stored in the local executor temporary directories.
D) The checkpoint interval is ignored, and the lineage grows unbounded.
* **Answer:** B
* **Mastery Explanation:** Checkpointing explicitly requires a distributed storage path (like HDFS or S3) defined via `spark.sparkContext.setCheckpointDir()`. Without it, attempting to checkpoint will result in a failure.

**23. What is the fundamental problem MLlib Pipelines solve over older RDD-based ML implementations?**
A) They replace Scala with Python as the primary execution engine.
B) They allow Catalyst and Tungsten to manage off-heap memory and optimize vectorized execution.
C) They replace Kryo serialization with optimized Java serialization.
D) They remove the need for iterative algorithms altogether.
* **Answer:** B
* **Mastery Explanation:** RDD-based ML suffered from massive GC overhead and lack of SQL optimization. Pipelines use DataFrames, benefiting natively from Tungsten's off-heap memory (reducing GC) and Catalyst's query optimization for featurization.

**24. In the context of `StandardScaler`, what is the execution complexity regarding shuffles?**
A) O(N) with zero shuffles, as it scales rows independently.
B) O(N) requiring a shuffle for the first pass (computing global stats), followed by a local second pass.
C) O(N * F) requiring iterative shuffles for every feature column independently.
D) O(N^2) requiring a full Cartesian join to compute variance.
* **Answer:** B
* **Mastery Explanation:** `StandardScaler` must first compute the global mean and variance across the entire dataset (which requires a shuffle/aggregation). Once computed, the actual scaling is a purely parallel local map operation.

**25. A pipeline uses `StringIndexer`, `OneHotEncoder`, and `VectorAssembler`. Which component(s) trigger a Spark Action/Shuffle?**
A) `StringIndexer` only
B) `OneHotEncoder` only
C) Both `StringIndexer` and `VectorAssembler`
D) None of them trigger actions during transformation.
* **Answer:** A
* **Mastery Explanation:** `StringIndexer` must perform a pass over the data to determine the global vocabulary frequencies, which requires a shuffle/aggregation. `OneHotEncoder` and `VectorAssembler` are purely local row-wise mapping functions.

## Part 3: "Small Twist" Scenario Questions (15 Questions)

**26. Scenario:** You successfully trained a Logistic Regression model on a 100GB dataset consisting mostly of One-Hot Encoded sparse vectors. You decide to add a custom Transformer using a Python UDF to normalize a single numeric column right before the algorithm. Suddenly, the job crashes with a massive Executor OOM. What twist caused this?
A) The Python UDF forced the Driver to collect the dataset.
B) The Python UDF implicitly converted the `SparseVector` column into a dense array when crossing the Py4J boundary.
C) Normalizing the numeric column increased the dimensionality of the dataset.
D) Python UDFs disable Kryo serialization cluster-wide.
* **Answer:** B
* **Mastery Explanation:** ML vectors are opaque to Catalyst. When passed through a standard Python UDF, the highly compressed `SparseVector` is often unpacked into a dense NumPy array or Python list, exploding the memory footprint and causing an immediate OOM.

**27. Scenario:** You are running ALS for recommendations. It succeeds with `setMaxIter(10)`. You increase it to `setMaxIter(25)` for better accuracy, but the job fails after 2 hours with `java.lang.StackOverflowError` in the Driver JVM. You have `spark.memory.fraction` set to 0.8. What is the fix?
A) Increase the Driver JVM heap size to 32GB.
B) Set `.setCheckpointInterval(5)` and define a checkpoint directory.
C) Change `spark.memory.fraction` to 0.4.
D) Use `treeAggregate` to compute user factors.
* **Answer:** B
* **Mastery Explanation:** Iterative algorithms at high iteration counts cause the RDD DAG lineage to grow linearly until the Driver's DAGScheduler throws a StackOverflow. Checkpointing truncates this lineage graph. Increasing driver heap only delays the inevitable.

**28. Scenario:** A `CrossValidator` grid search with 50 models takes 10 hours. You change `.setParallelism(1)` to `.setParallelism(10)`. The job time remains exactly 10 hours. Your cluster has 100 executor cores, and the dataset is 500GB. Why didn't it speed up?
A) `setParallelism` only works for Evaluators, not Estimators.
B) The dataset is so large that a single model training job fully saturates all 100 executor cores, leaving no idle slots for concurrent jobs.
C) You forgot to enable `spark.ml.parallelism.enabled=true` in the configuration.
D) Spark forces FIFO scheduling on MLlib jobs regardless of thread pools.
* **Answer:** B
* **Mastery Explanation:** Asynchronous job submission only speeds up execution if the cluster has *idle resources*. If one model training job fully utilizes all cluster cores (because the dataset is huge), concurrent jobs will just queue up, yielding no time savings.

**29. Scenario:** You implement a custom gradient descent using `reduce()`. It works on 10 executors but crashes the Driver with OOM when scaled to 500 executors. You change `reduce()` to `treeAggregate(initial)(seqOp, combOp, depth=2)`. The job now completes, but is 3x slower. What is the twist?
A) `treeAggregate` forces data to disk at every step.
B) `depth=2` is too shallow for 500 executors, causing the intermediate reducers to bottleneck.
C) Your `combOp` uses `Array.map` instead of an in-place `while` loop, generating massive garbage collection overhead on the intermediate reducers.
D) `treeAggregate` disables Native BLAS bindings.
* **Answer:** C
* **Mastery Explanation:** While `treeAggregate` fixes the Driver bottleneck, functional paradigms (like mapping arrays) inside the `combOp` create massive object allocation inside the tight reduction loops on the executors. Senior engineers use in-place while loops for these functions.

**30. Scenario:** A `StandardScaler` runs blazing fast on your local machine. In production, you enable `netlib-java` native bindings. Suddenly, the cluster CPUs are underutilized and the job runs at the same speed. What is the most likely reason?
A) `StandardScaler` does not perform matrix multiplications or dot products, so BLAS provides no acceleration.
B) The native bindings crashed and silently fell back to Java.
C) The data vectors were Sparse, and BLAS only works on DenseVectors.
D) Whole-Stage CodeGen disables `netlib-java`.
* **Answer:** A
* **Mastery Explanation:** `StandardScaler` is a row-wise scalar operation (subtracting mean, dividing by variance). It does not rely on heavy linear algebra like matrix multiplication. Therefore, optimized BLAS/LAPACK libraries offer virtually zero benefit here.

**31. Scenario:** You build a Pipeline with `StringIndexer` -> `VectorAssembler`. It trains perfectly. During streaming inference on new data, the job throws an exception because a category ("New_User") was not in the training set. You change `StringIndexer` to `.setHandleInvalid("keep")`. The inference succeeds, but the downstream Linear Regression predictions are completely skewed. Why?
A) "keep" assigns the new category to an index, which `VectorAssembler` treats as a large numeric weight.
B) The model forces all new categories to the baseline mean.
C) "keep" drops the row silently, shifting the data partitions.
D) Linear Regression cannot process StringIndexer outputs directly.
* **Answer:** A
* **Mastery Explanation:** `StringIndexer` outputs a numeric index (0.0, 1.0, 2.0). If you don't One-Hot Encode it, the algorithm treats these indices as continuous ordinal values. Assigning a new category to a new index (e.g., 50.0) feeds a massive, meaningless numeric value into the linear weights.

**32. Scenario:** You set `spark.serializer=org.apache.spark.serializer.KryoSerializer`. A custom Pipeline Transformer includes a large HashMap dictionary broadcasted to executors. The job OOMs during serialization. You switch back to Java Serialization and it works, albeit slowly. What happened?
A) Kryo cannot serialize HashMaps.
B) Kryo requires explicitly registering classes; without registration, the fully qualified class name is written for every single entry, bloating the payload beyond memory limits.
C) Java serialization automatically compresses HashMaps via Tungsten.
D) The Driver has a hardcoded Kryo buffer limit of 1MB.
* **Answer:** B
* **Mastery Explanation:** Kryo is extremely fast but if you don't register custom classes (or deep generic types inside collections), it falls back to writing the full class name string for *every single object* in the collection. A small HashMap can explode in serialized size, causing an OOM.

**33. Scenario:** You are extracting 100 features into a `DenseVector` column. You run `df.cache()` to speed up multiple model evaluations. The caching takes an eternity and fills up the memory. You switch to `df.persist(StorageLevel.MEMORY_ONLY_SER)`. It is drastically smaller. Why?
A) `MEMORY_ONLY_SER` forces Catalyst to drop unused columns.
B) `DenseVector` objects, when stored in off-heap Tungsten memory, are already binary, but standard `cache()` unpacks them into Java objects.
C) Vector UDTs have high Java object overhead. `cache()` stores them as Java objects; `MEMORY_ONLY_SER` stores them as raw bytes.
D) `cache()` automatically converts DenseVectors to SparseVectors.
* **Answer:** C
* **Mastery Explanation:** While Tungsten uses off-heap formats during *execution*, standard `cache()` (MEMORY_AND_DISK in newer Spark, or MEMORY_ONLY) stores Java objects. Vector UDTs involve wrappers. Using serialized caching prevents object inflation.

**34. Scenario:** A Random Forest model with `maxBins=32` runs out of memory on executors during training. You increase `maxBins` to `128` hoping it will chunk the data smaller. The OOM happens even faster. Why?
A) `maxBins` increases the Driver's broadcast size.
B) Larger `maxBins` means more split statistics must be computed and stored in memory simultaneously on the executors for every node in the tree.
C) 128 bins forces the data to be dense.
D) Higher bins disable Whole-Stage CodeGen.
* **Answer:** B
* **Mastery Explanation:** `maxBins` determines the granularity of histogram calculations for continuous features. Increasing it exponentially increases the memory required on executors to hold the aggregate statistics during the distributed split calculation.

**35. Scenario:** You write a pipeline: `Tokenizer` -> `HashingTF` -> `LogisticRegression`. The TF vectors are highly sparse. You add a `PCA` (Principal Component Analysis) step to reduce dimensionality from 10,000 to 100. The job crashes with a massive memory spike before Logistic Regression even starts. Why?
A) PCA requires computing the covariance matrix, which dense-ifies the 10,000-dimensional sparse vectors, causing memory explosion.
B) HashingTF creates negative values which PCA cannot handle.
C) LogisticRegression expects SparseVectors, but PCA outputs DenseVectors.
D) The Tokenizer created jagged arrays.
* **Answer:** A
* **Mastery Explanation:** PCA mathematically requires projecting the data. While the output is smaller (100 dims), computing PCA on a highly dimensional sparse dataset forces the framework to operate on dense representations of the covariance matrix, destroying the sparse memory advantage.

**36. Scenario:** Your cluster has native BLAS configured. You run a massive Matrix Multiplication between two distributed DataFrames using `BlockMatrix`. The CPUs barely hit 10% utilization and the job takes hours. What twist occurred?
A) BlockMatrix operations are not supported by BLAS.
B) Your block size (e.g., 1024x1024) is too large, causing the BLAS matrices to spill to L3 cache and destroying SIMD vectorization efficiency.
C) You didn't broadcast one of the DataFrames.
D) Native BLAS only works on local Driver operations.
* **Answer:** B
* **Mastery Explanation:** BLAS is hyper-optimized for specific matrix tile sizes that fit snugly into CPU L1/L2 caches. If the Spark `BlockMatrix` block size is too large, it forces memory thrashing, neutralizing the hardware acceleration.

**37. Scenario:** You set `spark.ml.parallelism=8` in `CrossValidator`. It submits 8 jobs, but the cluster only executes 2 at a time despite having plenty of cores. You are using Spark on YARN. What is the bottleneck?
A) YARN does not support parallel Spark Jobs.
B) The Spark Application's scheduling mode is set to FIFO (the default), causing the driver to submit jobs sequentially at the scheduler level.
C) `CrossValidator` restricts parallelism based on cross-validation folds.
D) The JVM metaspace limit is preventing thread creation.
* **Answer:** B
* **Mastery Explanation:** By default, Spark's internal `spark.scheduler.mode` is FIFO. Even if the Driver submits 8 jobs asynchronously, the DAGScheduler processes them sequentially unless the mode is explicitly changed to `FAIR`.

**38. Scenario:** A deep learning workload on Spark ML requires 50 iterations. You use `.setCheckpointInterval(1)` to be perfectly safe. The training time goes from 30 minutes to 6 hours. Why?
A) Checkpointing forces the Catalyst optimizer to re-plan the DAG every iteration.
B) Checkpointing every iteration forces massive HDFS/S3 network and disk I/O, entirely bottlenecking the distributed computation.
C) It triggers a full JVM GC cycle on every checkpoint.
D) The checkpoint files consume all HDFS space, causing task retries.
* **Answer:** B
* **Mastery Explanation:** Checkpointing materializes the RDD to disk. Doing this on *every* iteration shifts the bottleneck from CPU/RAM entirely to disk I/O. A balance (e.g., every 5-10 iterations) is required to amortize this cost.

**39. Scenario:** You have a `SparseVector` column resulting from TF-IDF. You pass it into `KMeans`. The algorithm converges, but the memory usage is 100x higher than expected during training. Why?
A) KMeans automatically normalizes the vectors, causing a dense cast.
B) The KMeans cluster centers (centroids) are inherently dense, and distance calculations against sparse data often require dense operations depending on the BLAS implementation.
C) KMeans broadcasts the entire dataset.
D) TF-IDF vectors are not supported by KMeans.
* **Answer:** B
* **Mastery Explanation:** While the input vectors are sparse, the cluster centroids computed by KMeans are averages and quickly become completely dense. Computing Euclidean distance between a sparse vector and a dense centroid requires memory bandwidth heavily skewed towards dense operations.

**40. Scenario:** You build an ML Pipeline and call `pipeline.fit(df)`. The physical plan shows `WholeStageCodegen` blocks wrapping the transformers. You add a `df.map(row => customFunction(row))` right in the middle of the pipeline. The execution time doubles. What happened?
A) The map function triggered an immediate shuffle.
B) The custom RDD-based map function broke the Catalyst Whole-Stage Codegen pipeline, forcing data to be deserialized to Java objects and serialized back into Tungsten format.
C) `pipeline.fit` ignores map functions.
D) The map function forced the DAGScheduler to checkpoint.
* **Answer:** B
* **Mastery Explanation:** Catalyst and Tungsten maintain a highly optimized internal binary format (off-heap). Using an RDD map or UDF forces Spark to break the codegen block, deserialize the Tungsten row into a Java Row object, run the function, and reserialize it, causing massive overhead.

## Part 4: Coding & Debugging Questions (10 Questions)

**41. Identify the performance flaw in this custom Estimator's network aggregation:**
```scala
val globalStats = rdd.map(computeLocalStats).reduce((a, b) => {
  val result = new Array[Double](a.length)
  for (i <- a.indices) result(i) = a(i) + b(i)
  result
})
```
* **Answer / Mastery Explanation:** There are two massive flaws. First, it uses `reduce()`, which blasts all partition results directly to the driver, creating a network star-topology bottleneck. It should use `treeAggregate`. Second, inside the reduction, it allocates a `new Array` and uses a Scala `for` loop. This creates catastrophic object allocation overhead and GC pressure. It should mutate array `a` in-place using a `while` loop.

**42. A Spark ML job fails with `OutOfMemoryError` on executors. The code is:**
```scala
val encoder = new OneHotEncoder().setInputCol("cat").setOutputCol("cat_vec")
val denseConverter = udf((v: SparseVector) => v.toArray)
val df2 = df.withColumn("dense_cat", denseConverter($"cat_vec"))
val rf = new RandomForestClassifier().setFeaturesCol("dense_cat")
```
* **Answer / Mastery Explanation:** The UDF `denseConverter` forces a highly compressed `SparseVector` (from One-Hot Encoding) into a massive dense `Array[Double]`. This causes the memory footprint to explode exponentially. Random Forest natively supports SparseVectors; the UDF should be entirely removed, and the assembler should use `cat_vec` directly.

**43. A developer writes an iterative training loop. It crashes after 15 loops with a StackOverflow in DAGScheduler.**
```scala
var currentDf = initialDf
for (i <- 1 to 20) {
  currentDf = applyCustomGradientUpdate(currentDf)
}
currentDf.write.parquet("output")
```
* **Answer / Mastery Explanation:** Because DataFrames are lazily evaluated, the loop doesn't execute anything; it just builds a massive DAG lineage. By iteration 20, the DAG is so deep it causes a StackOverflow in the Driver's metaspace. The fix is to add `currentDf.checkpoint()` every few iterations (e.g., `if (i % 5 == 0)`) to truncate the lineage.

**44. You want to speed up Hyperparameter tuning. Identify the missing configuration:**
```scala
val cv = new CrossValidator()
  .setEstimator(lr)
  .setEvaluator(eval)
  .setEstimatorParamMaps(grid)
  .setNumFolds(5)
```
* **Answer / Mastery Explanation:** The code is missing `.setParallelism(N)`. Without it, `CrossValidator` will evaluate the grid combinations strictly sequentially. Adding `.setParallelism(4)` allows the Driver to submit concurrent Spark jobs, significantly reducing training time.

**45. Why does this feature assembly logic result in poor linear regression performance?**
```scala
val ohe = new OneHotEncoder()
  .setInputCols(Array("state_idx"))
  .setOutputCols(Array("state_vec"))
  .setDropLast(false)
val lr = new LinearRegression().setFeaturesCol("state_vec")
```
* **Answer / Mastery Explanation:** `setDropLast(false)` keeps all categories in the One-Hot Encoding. For models requiring matrix inversion (like unregularized Linear Regression), this introduces perfect multicollinearity (the dummy variable trap), causing the underlying BLAS solver to fail or produce wildly unstable weights.

**46. A `StandardScaler` is applied, but downstream execution is extremely slow.**
```scala
val scaler = new StandardScaler().setInputCol("features").setOutputCol("scaled")
val scaledDf = scaler.fit(df).transform(df)
val result = scaledDf.join(otherDf, "id")
```
* **Answer / Mastery Explanation:** `scaler.fit(df)` forces an action/shuffle to compute global statistics. The `transform(df)` does a local map. However, if `df` is not cached, the complex lineage (or underlying I/O) that created `df` might be re-evaluated. More critically, the pipeline structure is raw. Using a `Pipeline` object allows Catalyst to optimize the execution graph better than manual chaining.

**47. The cluster has `netlib-java` installed, but matrix operations are slow. The UI shows no native libraries loaded. What environmental variable is likely missing?**
* **Answer / Mastery Explanation:** The worker nodes are likely missing the underlying OS-level C/Fortran libraries (e.g., `libgfortran`, `openblas`). `netlib-java` is just a JNI wrapper; if the native `.so` files are not in the `LD_LIBRARY_PATH` (or installed via `apt-get/yum`), it silently falls back to `f2jblas` (Java implementation).

**48. Identify the serialization bottleneck in this custom Transformer:**
```scala
class CustomTransformer(val dictionary: Map[String, Double]) extends Transformer {
  override def transform(dataset: Dataset[_]): DataFrame = {
    val mapUdf = udf((s: String) => dictionary.getOrElse(s, 0.0))
    dataset.withColumn("mapped", mapUdf($"col"))
  }
}
```
* **Answer / Mastery Explanation:** The `dictionary` is captured inside the UDF closure. This means Spark will attempt to serialize the entire `Map` and send it with *every single task* to the executors. If the map is large, this causes massive network overhead and Task OOMs. The `dictionary` must be wrapped in a `sparkContext.broadcast(dictionary)` variable.

**49. Look at this PySpark ML code. Why will it OOM the driver?**
```python
model = RandomForestClassifier().fit(df)
predictions = model.transform(df)
pandas_df = predictions.toPandas()
```
* **Answer / Mastery Explanation:** `toPandas()` forces Spark to collect the entirely distributed `predictions` DataFrame back to the single Driver JVM memory space to construct a Pandas DataFrame. For large ML datasets, this will immediately cause a Driver OutOfMemoryError.

**50. You are using `VectorAssembler` to merge 5 numeric columns and 1 sparse categorical vector. The output is unexpectedly dense, blowing up memory.**
```scala
val assembler = new VectorAssembler()
  .setInputCols(Array("num1", "num2", "num3", "num4", "num5", "sparse_cat"))
  .setOutputCol("features")
```
* **Answer / Mastery Explanation:** Spark's `VectorAssembler` automatically decides whether to output a `SparseVector` or `DenseVector` based on the ratio of non-zero elements. If the 5 numeric columns are entirely non-zero, the assembler calculates that a Dense representation uses less memory overhead (avoiding index arrays). To fix this, you must analyze the sparsity of your numerics or custom-merge the vectors if you strictly require sparse representation for downstream estimators.
