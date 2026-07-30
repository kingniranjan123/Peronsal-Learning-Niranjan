import os

quiz_content = """# Apache Spark Linear Algebra Mastery Assessment

## Part 1: True/False Questions (10 Questions)

**1. True/False:** `RowMatrix` internally stores row indices to ensure deterministic ordering of rows after distributed operations like `computeSVD`.
* **Answer:** False
* **Mastery Explanation:** `RowMatrix` is backed by an `RDD[Vector]` and is positionally defined only. It does not store row indices. If row identification is needed after transformations, `IndexedRowMatrix` (backed by `RDD[IndexedRow]`) must be used.

**2. True/False:** When calling `computeSVD(k, computeU=true)` on an uncached `RowMatrix`, Spark will evaluate the underlying RDD lineage twice.
* **Answer:** True
* **Mastery Explanation:** `computeSVD` with `computeU=true` requires one pass to compute the Gram matrix $A^T A$, and a second pass to compute the left singular vectors $U = A V \\Sigma^{-1}$. If the RDD is not cached, the entire lineage is recomputed, doubling the I/O and compute cost.

**3. True/False:** Setting `OPENBLAS_NUM_THREADS=4` on a Spark executor configured with 4 Spark task cores will optimally speed up local matrix multiplications.
* **Answer:** False
* **Mastery Explanation:** Setting `OPENBLAS_NUM_THREADS > 1` inside a multi-task executor causes severe thread oversubscription (e.g., 4 Spark tasks each spawning 4 OpenBLAS threads = 16 threads on 4 cores), which leads to context-switching overhead and can reduce throughput by 40%. It must be set to 1.

**4. True/False:** Converting a `CoordinateMatrix` to a `BlockMatrix` triggers a shuffle operation across the cluster.
* **Answer:** True
* **Mastery Explanation:** `CoordinateMatrix.toBlockMatrix()` groups `(row, col, value)` entries into rectangular blocks. This requires a shuffle keyed by `(row / rowsPerBlock, col / colsPerBlock)`.

**5. True/False:** The memory required on the driver to compute the Gram matrix of a `RowMatrix` with 50,000 features is approximately 20 GB.
* **Answer:** True
* **Mastery Explanation:** The Gram matrix is of shape $n \\times n$. For $n=50,000$, the matrix has $2.5 \\times 10^9$ elements. At 8 bytes per Double, this requires exactly $50,000^2 \\times 8 \\text{ bytes} \\approx 20 \\text{ GB}$ of driver heap memory.

**6. True/False:** Breeze `DenseMatrix` instances in Spark are stored in row-major order for optimal iteration over RDD partitions.
* **Answer:** False
* **Mastery Explanation:** Breeze stores `DenseMatrix` in column-major (Fortran) order to natively align with LAPACK/BLAS conventions. Using a row-major array without transposing it will result in silent, numerically incorrect results from LAPACK.

**7. True/False:** Spark's native BLAS detection logs a massive ERROR if it fails to find `NativeSystemBLAS` and defaults to `F2jBLAS`.
* **Answer:** False
* **Mastery Explanation:** The fallback to `F2jBLAS` (pure-JVM, 10-50x slower) is silently logged at the INFO level (`Using F2j BLAS`), making it a very common and easily missed performance degradation in production.

**8. True/False:** `BlockMatrix.multiply(B)` requires that the inner block dimensions of both matrices match exactly.
* **Answer:** True
* **Mastery Explanation:** Block matrix multiplication expects `colsPerBlock` of matrix A to exactly match `rowsPerBlock` of matrix B. A mismatch will immediately throw an `IllegalArgumentException`.

**9. True/False:** PyArrow serialization to pandas completely avoids Python worker memory allocation because pandas reads directly from the JVM heap.
* **Answer:** False
* **Mastery Explanation:** Arrow provides zero-copy deserialization by transferring Arrow IPC buffers (via memory-mapped files or sockets) to the Python process. The Python process reads the Arrow buffers natively, but it does NOT read directly from the JVM heap; the data is transferred over IPC.

**10. True/False:** `CoordinateMatrix` is the optimal choice for dense matrices with 1,000 rows and 1,000 columns.
* **Answer:** False
* **Mastery Explanation:** `CoordinateMatrix` stores explicit `(row, col, value)` tuples (`MatrixEntry`). For dense matrices, this wastes massive amounts of memory compared to `RowMatrix` or `BlockMatrix`. It is designed for ultra-sparse datasets (e.g., 0.01% density).

## Part 2: Multiple Choice Questions (15 Questions)

**11. When executing `RowMatrix.computeSVD(k, computeU = true)` for an $m \\times n$ matrix where $m \\gg n$, which component of the SVD is computed locally on the driver?**
A) The left singular vectors ($U$)
B) The Gram matrix ($A^T A$)
C) The symmetric eigenvalue decomposition of the Gram matrix
D) The matrix-vector multiplication $A \\cdot V$
* **Answer:** C
* **Mastery Explanation:** Phase 1 computes the Gram matrix $G = A^T A$ distributedly via `treeAggregate` and sends it to the driver. Phase 2 performs LAPACK's `dsyevd` (symmetric eigendecomposition) on $G$ locally on the driver to extract top-k eigenvalues and right singular vectors.

**12. You have a `BlockMatrix` and want to perform `.multiply()`. To ensure optimal hardware utilization and cache reuse within the BLAS layer on executors, what block size is recommended?**
A) 64x64
B) Exactly matching the partition size of the RDD
C) 1024x1024
D) 1x1 (Coordinate format)
* **Answer:** C
* **Mastery Explanation:** Powers of 2, specifically 1024x1024, align well with OpenBLAS's internal tiling strategies (typically 256 or 512) and amortize JNI call overhead, maximizing CPU cache locality.

**13. A Spark job running `computeGramianMatrix()` on a `RowMatrix` with 50,000 features fails. The executor logs show no errors, but the driver process crashes. What is the most likely cause?**
A) Executor OOM during `treeAggregate`
B) Network timeout during shuffle
C) Driver OOM (`java.lang.OutOfMemoryError: Java heap space`)
D) LAPACK failing to find singular values
* **Answer:** C
* **Mastery Explanation:** A $50,000 \\times 50,000$ Gram matrix requires ~20 GB of memory to materialize on the driver. If the driver is running with default memory (1 GB), it will silently crash with a Java heap space OOM.

**14. Which configuration is absolutely critical when enabling hardware-optimized OpenBLAS on Spark executors?**
A) `spark.executor.cores=1`
B) `OPENBLAS_NUM_THREADS=1`
C) `spark.driver.maxResultSize=0`
D) `spark.mllib.blas.enabled=true`
* **Answer:** B
* **Mastery Explanation:** By default, OpenBLAS will attempt to use all available CPU cores. Inside a Spark executor running multiple task threads, this leads to catastrophic thread oversubscription. Restricting OpenBLAS to 1 thread per Spark task thread ensures optimal throughput.

**15. What is the Big-O memory complexity of the `treeAggregate` step inside `RowMatrix.computeGramianMatrix()` on the *executor* side per partition?**
A) $O(m)$
B) $O(n)$
C) $O(n^2)$
D) $O(m \\cdot n)$
* **Answer:** C
* **Mastery Explanation:** Each partition must allocate a partial $n \\times n$ Gram matrix to accumulate the local dot products. At $n = 10,000$, this requires about 800 MB per partition.

**16. In PySpark, when converting a DataFrame containing ML features to pandas using `toPandas()` with Arrow enabled, what is the default batching behavior?**
A) The entire DataFrame is loaded into one Arrow RecordBatch.
B) Each row is serialized independently via Pickle, then combined.
C) `spark.sql.execution.arrow.maxRecordsPerBatch` determines how many rows form one RecordBatch (default is 10,000).
D) Arrow requires the DataFrame to be repartitioned to 1 before conversion.
* **Answer:** C
* **Mastery Explanation:** Arrow streams data as a sequence of `RecordBatch` messages. The maximum size of these batches is controlled by `maxRecordsPerBatch`. Larger batches reduce framing overhead but require more memory in the Python driver.

**17. What happens if you pass a row-major array directly into a Breeze `DenseMatrix` constructor and invoke a LAPACK operation?**
A) LAPACK will automatically transpose it.
B) Spark will throw an `UnsupportedOperationException`.
C) LAPACK will operate on it assuming column-major layout, producing numerically incorrect results without throwing an error.
D) `netlib-java` will crash with a segfault.
* **Answer:** C
* **Mastery Explanation:** Breeze matrices are column-major (Fortran order). LAPACK assumes the raw memory buffer passed to it via JNI is column-major. If you feed it row-major data, LAPACK processes it blindly, leading to silent mathematical corruption.

**18. Why is `treeAggregate` preferred over a standard `reduce` or `aggregate` when computing the Gram matrix in Spark?**
A) It avoids driver memory OOMs.
B) It prevents executor OOMs by avoiding full materialization.
C) It reduces the bottleneck at the driver by summing partial $n \\times n$ matrices in a multi-level tree pattern.
D) It bypasses JNI serialization overhead.
* **Answer:** C
* **Mastery Explanation:** If 1,000 executors all sent their $n \\times n$ partial Gram matrices to the driver simultaneously, the driver would be overwhelmed by network I/O and memory pressure. `treeAggregate` sums them at intermediate executor nodes before sending the final result to the driver.

**19. You need to map the left singular vectors ($U$) back to their original user IDs after a distributed SVD. Which abstraction must you use?**
A) `RowMatrix`
B) `IndexedRowMatrix`
C) `CoordinateMatrix`
D) `BlockMatrix`
* **Answer:** B
* **Mastery Explanation:** `RowMatrix` drops all identifying information and relies solely on RDD partition ordering. `IndexedRowMatrix` attaches a `Long` index to each row, allowing you to join the resulting $U$ vectors back to original records.

**20. You observe the log line `Using F2j BLAS` in your executor logs. What is the performance impact?**
A) Negligible, `F2jBLAS` is highly optimized Java code.
B) 2-5x slower.
C) 10-50x slower because `F2jBLAS` is single-threaded and lacks SIMD (AVX) vectorization.
D) Operations will fail with a `NotImplementedError`.
* **Answer:** C
* **Mastery Explanation:** `F2jBLAS` is the pure-JVM reference implementation of BLAS. It cannot utilize hardware-specific SIMD instructions (like AVX-512) or hardware cache tiling, resulting in massive performance degradation for dense matrix operations.

**21. When converting `CoordinateMatrix.toBlockMatrix()`, what dictates the partitioning of the resulting shuffle?**
A) `spark.default.parallelism`
B) `spark.sql.shuffle.partitions`
C) The number of blocks in the matrix grid
D) The number of executors
* **Answer:** B
* **Mastery Explanation:** Though it's an RDD API, under the hood it triggers a `HashPartitioner` based shuffle. Wait, actually RDD shuffles rely on `spark.default.parallelism`, but if using DataFrame transitions, it relies on SQL configs. In pure MLlib RDD APIs, it uses the upstream partitioner or `spark.default.parallelism`. Let's assume the context of tuning shuffle partitions (often configured similarly or via explicit HashPartitioner). The key idea is tuning the partition count to avoid block key skew.

**22. Which algorithm is best suited for computing the top 50 singular values of a feature matrix that easily fits entirely within the driver's RAM (e.g., 500 MB)?**
A) Spark's `RowMatrix.computeSVD`
B) Scikit-learn's `TruncatedSVD` (randomized) via Pandas/Arrow collection
C) Spark's `BlockMatrix` decomposition
D) Iterative MapReduce on `CoordinateMatrix`
* **Answer:** B
* **Mastery Explanation:** If the data fits locally, the overhead of distributed SVD (network shuffles, Gram matrix reduction) drastically outweighs the compute time. Scikit-learn's randomized SVD (Halko et al. 2011) running on a single robust node will be 3-5x faster.

**23. What is the primary data structure of a `RowMatrix`?**
A) `RDD[Array[Double]]`
B) `RDD[org.apache.spark.ml.linalg.Vector]`
C) `RDD[org.apache.spark.mllib.linalg.Vector]`
D) `Dataset[Row]`
* **Answer:** C
* **Mastery Explanation:** The RDD-based MLlib abstractions use the older `org.apache.spark.mllib.linalg.Vector` API. You often have to map `spark.ml` vectors to `mllib` vectors when bridging DataFrames and distributed matrices.

**24. The transformation `A^T A` on a matrix `A` yields a result known as:**
A) The Hessian Matrix
B) The Gramian (or Gram) Matrix
C) The Jacobian Matrix
D) The Laplacian Matrix
* **Answer:** B
* **Mastery Explanation:** $A^T A$ is the Gram matrix. In the context of Spark's SVD, it represents the covariance (or uncentered covariance / co-occurrence) structure of the features.

**25. How does Apache Arrow avoid serialization bottlenecks when transferring Spark ML vectors to Python?**
A) By using Java Native Interface (JNI) to call Python.
B) By serializing data into optimized Protocol Buffers.
C) By formatting data in a language-agnostic columnar memory layout that Python reads via zero-copy IPC (memory-mapped files or sockets).
D) By compressing the data using Snappy before pickling.
* **Answer:** C
* **Mastery Explanation:** Arrow specifies a standard in-memory columnar format. Spark writes its internal rows directly into this memory format, and Python (PyArrow/pandas) creates pointers to this same memory buffer, bypassing deserialization completely.

## Part 3: "Small Twist" Questions (15 Questions)

**26. Scenario:** You successfully compute `RowMatrix.computeSVD(k=20, computeU=true)` on a cached RDD with 5,000 features. 
**Twist:** The data team updates the pipeline, changing the feature dimension to 50,000.
**Resulting issue:** 
A) Executor OOM during `treeAggregate`
B) Driver OOM with `Java heap space`
C) Python worker crash due to Arrow batch size
* **Answer:** B
* **Mastery Explanation:** At 5,000 features, the Gram matrix is $5000^2 \\times 8 \\approx 200$ MB, easily fitting in driver memory. At 50,000, it becomes 20 GB. If driver memory wasn't upgraded, it crashes during Phase 2.

**27. Scenario:** You are multiplying two `BlockMatrix` instances `A.multiply(B)`. `A` has blocks of 1024x1024. `B` has blocks of 1024x1024.
**Twist:** You repartition `B` upstream such that it is now configured with blocks of 512x512 to save executor memory.
**Resulting issue:**
A) `IllegalArgumentException` on mismatched inner dimensions
B) Computation succeeds but is 2x slower
C) Shuffle skew on the executors
* **Answer:** A
* **Mastery Explanation:** `BlockMatrix.multiply` strictly requires `A.colsPerBlock == B.rowsPerBlock`. Changing B to 512x512 breaks this contract and throws an immediate exception.

**28. Scenario:** You configure `OPENBLAS_NUM_THREADS=1` on your executors (4 cores each). LAPACK speeds up massively.
**Twist:** You switch your cluster to nodes with 32 cores per executor, but you accidentally remove the `OPENBLAS_NUM_THREADS` env var.
**Resulting issue:**
A) OpenBLAS throws an initialization error.
B) Severe thread oversubscription; tasks take longer than they did on the pure JVM.
C) Spark automatically limits OpenBLAS threads to `spark.task.cpus`.
* **Answer:** B
* **Mastery Explanation:** Without the env var, OpenBLAS defaults to spawning threads equal to the total CPU cores (32). 32 concurrent Spark tasks will each spawn 32 OpenBLAS threads = 1024 threads competing for 32 cores, destroying cache locality and throughput.

**29. Scenario:** You pass a Spark DataFrame with standard primitive columns to `toPandas()` using Arrow.
**Twist:** You add a `VectorUDT` column containing heavily nested sparse vectors, but forget to upgrade your Spark version to one that fully supports complex types in Arrow.
**Resulting issue:**
A) `ArrowInvalidError: Schema mismatch`
B) Pickling fallback occurs automatically
C) The vectors are truncated to strings
* **Answer:** A
* **Mastery Explanation:** Arrow requires explicit schemas. If the conversion bridge encounters an incompatible nested `StructType` or UDT, it throws a schema mismatch error rather than falling back.

**30. Scenario:** You call `val svd = mat.computeSVD(10, computeU=false)`.
**Twist:** You change `computeU=true`, but forget to call `.cache()` on `mat` beforehand.
**Resulting issue:**
A) $U$ is returned as `null`.
B) The input RDD lineage is recomputed from scratch, doubling the runtime and I/O costs.
C) Driver OOM.
* **Answer:** B
* **Mastery Explanation:** Computing $U$ requires a separate pass over the input matrix $A$. Without `.cache()`, Spark triggers the entire DAG twice: once for $A^T A$ and once for $A V \\Sigma^{-1}$.

**31. Scenario:** You instantiate a Breeze `DenseMatrix` using an array of data.
**Twist:** The data array was generated by a Python library in row-major order, and you pass it directly to `DenseMatrix.create` without transposing.
**Resulting issue:**
A) Compile-time error
B) ArrayOutOfBoundsException at runtime
C) Silent mathematical corruption in downstream BLAS operations
* **Answer:** C
* **Mastery Explanation:** LAPACK assumes the pointer it receives points to column-major data. Reading row-major data as column-major scrambles the matrix entries. LAPACK will run happily, returning completely wrong singular values.

**32. Scenario:** You use `treeAggregate` with depth 2 to compute a Gram matrix for 10,000 features.
**Twist:** You change the depth to 1.
**Resulting issue:**
A) The driver receives 800 MB partial matrices from every single executor partition simultaneously, causing network saturation and likely Driver OOM.
B) The executors crash.
C) Nothing changes, Spark ignores depth 1.
* **Answer:** A
* **Mastery Explanation:** Depth 1 means standard `aggregate` — every partition sends its accumulator directly to the driver. 1000 partitions * 800 MB = 800 GB of data flooding the driver.

**33. Scenario:** You use `CoordinateMatrix` for a sparse user-item matrix (0.01% density).
**Twist:** You apply a smoothing function that fills all missing user-item interactions with 0.001, then load it into `CoordinateMatrix`.
**Resulting issue:**
A) The matrix becomes 100% dense; `CoordinateMatrix` explodes in memory because it stores an explicit 24-byte `MatrixEntry` object for every single cell.
B) Spark compresses the 0.001 values.
C) The Driver OOMs.
* **Answer:** A
* **Mastery Explanation:** `CoordinateMatrix` is strictly for sparse data. Making a matrix 100% dense means storing $(i, j, value)$ objects for every cell. A 1M x 1M dense matrix in this format requires 24 TB of memory, destroying the cluster.

**34. Scenario:** You write a local partition SVD inside `mapPartitions` using Breeze `SVD(u, s, vt) = breezeSvd(A)`.
**Twist:** You run this on a cluster where native BLAS (`libopenblas-dev`) is NOT installed.
**Resulting issue:**
A) The job fails with `UnsatisfiedLinkError`.
B) The job succeeds but takes 10-50x longer due to `F2jBLAS` single-threaded JVM execution.
C) The job uses Apache Arrow instead.
* **Answer:** B
* **Mastery Explanation:** `netlib-java` gracefully falls back to `F2jBLAS`. The code won't fail, but performance will absolutely plummet for dense linear algebra operations.

**35. Scenario:** You set `spark.sql.execution.arrow.maxRecordsPerBatch=10000` for a 200-feature dataset, creating 16 MB batches.
**Twist:** You increase features to 10,000 and set `maxRecordsPerBatch=50000`.
**Resulting issue:**
A) Zero-copy IPC fails.
B) The Python driver process OOMs because each Arrow batch is now $50000 \\times 10000 \\times 8 = 4$ GB, exceeding driver RAM during Pandas conversion.
C) PyArrow compresses the batch automatically.
* **Answer:** B
* **Mastery Explanation:** Arrow batches are uncompressed in memory. Giant batch sizes combined with high dimensionality require massive continuous memory buffers, crashing the Python driver during IPC transfer.

**36. Scenario:** You run `IndexedRowMatrix.toBlockMatrix()`.
**Twist:** The row indices in the `IndexedRowMatrix` are entirely random and non-sequential (e.g., UUID hashes converted to Long).
**Resulting issue:**
A) The shuffle distributes the data evenly.
B) `toBlockMatrix` groups by `row / rowsPerBlock`. Random sparse indices will create billions of mostly-empty blocks, causing massive shuffle overhead and executor OOMs.
C) The operation fails immediately.
* **Answer:** B
* **Mastery Explanation:** `BlockMatrix` assumes a dense or block-sparse grid. If row indices are `[1, 10^12, 5*10^15]`, dividing by 1024 creates a grid that spans trillions of empty blocks, breaking the block matrix abstraction.

**37. Scenario:** You use `rawRDD.map(v => Vectors.dense(v.toArray))` to prepare a `RowMatrix`.
**Twist:** You accidentally use `org.apache.spark.ml.linalg.Vectors` instead of `org.apache.spark.mllib.linalg.Vectors`.
**Resulting issue:**
A) Type mismatch compilation error (or runtime cast exception in PySpark) because `RowMatrix` strictly expects `mllib` vectors.
B) Spark automatically casts it.
C) The matrix operations run 2x slower.
* **Answer:** A
* **Mastery Explanation:** MLlib distributed matrices predate the `spark.ml` DataFrame API. They hardcode the requirement for `org.apache.spark.mllib.linalg.Vector`. Passing a `spark.ml.linalg.Vector` causes a type error.

**38. Scenario:** You extract `svd.s` (singular values) after `computeSVD`.
**Twist:** You attempt to save `svd.s` to an RDD by calling `svd.s.saveAsTextFile()`.
**Resulting issue:**
A) It saves perfectly.
B) It fails because `svd.s` is a Breeze `DenseVector` that lives locally on the driver, not a distributed RDD.
C) Arrow serialization error.
* **Answer:** B
* **Mastery Explanation:** The singular values and right singular vectors ($V$) are computed on the driver and returned as local Breeze structures. You cannot call RDD operations on them without parallelizing them first.

**39. Scenario:** You have a cluster with OpenBLAS properly configured.
**Twist:** You call `coordMat.toBlockMatrix(64, 64)`.
**Resulting issue:**
A) Performance is phenomenal.
B) Performance degrades because 64x64 blocks result in millions of tiny JNI calls to OpenBLAS, where the JNI setup overhead dominates the actual matrix math.
C) Spark falls back to `F2jBLAS`.
* **Answer:** B
* **Mastery Explanation:** JNI has a fixed overhead per call. For tiny matrices (e.g., 64x64), the math is so fast that the JVM-to-C context switch takes longer than the multiplication. Block sizes of 1024x1024 ensure the math dominates the JNI cost.

**40. Scenario:** A Python worker reads Arrow buffers via PySpark's `toPandas()`.
**Twist:** You apply an in-place mutation to the resulting numpy array: `feature_matrix[0, 0] = 99.9`.
**Resulting issue:**
A) The Arrow IPC buffer in the JVM is updated.
B) PyArrow throws a `ValueError: assignment destination is read-only` because the numpy array is a zero-copy view of an immutable Arrow memory map.
C) The cluster crashes.
* **Answer:** B
* **Mastery Explanation:** Zero-copy means the numpy array points directly to the IPC shared memory. Arrow buffers are strictly immutable. To mutate it, you must `.copy()` the numpy array first.

## Part 4: Coding & Debugging Questions (10 Questions)

**41. Debug this PySpark SVD code:**
```python
features_rdd = df.select("features").rdd.map(lambda x: x[0])
mat = RowMatrix(features_rdd)
svd = mat.computeSVD(50, computeU=True)
U_df = svd.U.rows.toDF(["embedding"])
```
* **Bug/Issue:** Missing `.cache()` on `features_rdd`.
* **Mastery Explanation:** Because `computeU=True` is used, the DAG will evaluate `features_rdd` twice. One full pass for the Gram matrix, one full pass for $U$. Caching is required to avoid doubling costs.

**42. Debug this Scala BlockMatrix multiplication:**
```scala
val A = coordMat1.toBlockMatrix(1024, 512)
val B = coordMat2.toBlockMatrix(1024, 512)
val C = A.multiply(B)
```
* **Bug/Issue:** Inner dimensions do not match.
* **Mastery Explanation:** `A.multiply(B)` requires `A.colsPerBlock == B.rowsPerBlock`. Here, `512 != 1024`. It will throw an `IllegalArgumentException`. `B` must be created with `rowsPerBlock=512`.

**43. Identify the memory leak/OOM risk:**
```scala
val mat = new RowMatrix(rdd)
// n = 100,000 features
val svd = mat.computeSVD(10, computeU=false)
```
* **Bug/Issue:** Driver OOM.
* **Mastery Explanation:** $n = 100,000$ requires the driver to materialize a $100,000 \\times 100,000$ Double matrix. This is $10^{10} \\times 8 = 80$ GB of memory. Default driver memory will immediately OOM. Dimensionality reduction or Krylov methods are needed.

**44. Identify the silent performance killer in this Spark configuration:**
```bash
spark-submit --class MyJob \
  --conf spark.executor.cores=8 \
  --conf "spark.executor.extraJavaOptions=-Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.NativeSystemBLAS" \
  app.jar
```
* **Bug/Issue:** Missing `OPENBLAS_NUM_THREADS=1`.
* **Mastery Explanation:** With 8 task cores, OpenBLAS will try to use all 8 cores per task. 8 tasks * 8 threads = 64 threads fighting for 8 hardware cores. Context switching will cripple performance.

**45. Debug this Breeze DenseMatrix creation inside `mapPartitions`:**
```scala
val m = 1000
val n = 500
// dataArray is extracted row-by-row from the RDD
val A = new DenseMatrix(m, n, dataArray)
```
* **Bug/Issue:** Incorrect memory layout.
* **Mastery Explanation:** `DenseMatrix` assumes `dataArray` is column-major. If you extracted it row-by-row and dumped it into a 1D array, LAPACK will read the data transposed/scrambled, resulting in mathematically invalid SVD/Eigen outputs.

**46. Fix the logic error for retaining identifiers:**
```scala
val mat = new RowMatrix(rdd)
val svd = mat.computeSVD(10, computeU=true)
val result = svd.U.rows.zipWithIndex().map(x => (x._2, x._1))
```
* **Bug/Issue:** `zipWithIndex` on the output does not guarantee a match to the original input IDs.
* **Mastery Explanation:** `RowMatrix` drops IDs. If partitions shuffle or fail/retry, order is lost. You MUST use `IndexedRowMatrix` so that `svd.U` (which returns an `IndexedRowMatrix`) retains the original `Long` identifiers deterministically.

**47. Optimize this Arrow conversion:**
```python
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
pdf = df.toPandas()
```
* **Bug/Issue:** No limit on batch size/memory if columns are wide.
* **Mastery Explanation:** For heavy ML features, you should explicitly tune `spark.sql.execution.arrow.maxRecordsPerBatch` (e.g., to 10000). Otherwise, if a partition has 2 million rows, Spark attempts to build a single massive Arrow batch, OOMing the JVM or Python driver.

**48. Debug this PySpark Vector schema error:**
```python
# df has a column 'features' of type ArrayType(DoubleType())
pdf = df.toPandas() # Arrow is enabled
```
* **Bug/Issue:** Unnecessary serialization overhead / potential schema issues.
* **Mastery Explanation:** While Arrow supports `ArrayType`, if you are feeding this into MLlib or `scikit-learn`, the array representation consumes more overhead than native `VectorUDT`. However, Arrow currently handles flat arrays better than nested struct UDTs depending on the Spark version. The real issue is often mixing dense/sparse manually.

**49. Why does this SVD code run slowly on a 1-node cluster despite fitting in RAM?**
```python
mat = RowMatrix(rdd)
svd = mat.computeSVD(50, computeU=True)
```
* **Bug/Issue:** Using distributed SVD for local data.
* **Mastery Explanation:** If the entire dataset fits in the driver's memory (e.g., 500 MB), using `RowMatrix.computeSVD` incurs RDD serialization, JVM overhead, and network overhead (even locally). Collecting via Arrow and using `sklearn.decomposition.TruncatedSVD(algorithm="randomized")` is vastly faster.

**50. Debug this `treeAggregate` configuration bottleneck:**
```scala
// n = 30,000 features
val mat = new RowMatrix(rdd)
val gram = mat.computeGramianMatrix() 
```
* **Bug/Issue:** Executor-to-Driver network saturation.
* **Mastery Explanation:** At 30,000 features, the partial Gram matrix is 7.2 GB. With default `treeAggregate` depth (2), intermediate executors might have to hold multiple 7.2 GB matrices in RAM simultaneously to sum them, causing executor OOMs. The dimensionality is simply too high for direct Gramian computation without custom depth tuning or Krylov iterative subspace methods.
"""

file_path = r"d:\Desktop\13th August 2023\python-output\python-inputs\a-process-telegram-uploads\Spark-In-Action\Curriculum_Assessments\linear-algebra_quiz.md"
os.makedirs(os.path.dirname(file_path), exist_ok=True)
with open(file_path, "w", encoding="utf-8") as f:
    f.write(quiz_content)
