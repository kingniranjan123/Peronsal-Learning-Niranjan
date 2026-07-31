# 🔥 Master Class: Linear Algebra — Distributed Matrices, SVD, BLAS/LAPACK, Breeze, and Apache Arrow

## Overview
<div style='text-align: right; margin-top: -10px; margin-bottom: 20px; font-size: 0.85rem; color: #a0aec0;'><em>References: [Ref: 451](spark_book.pdf#page=451) [Ref: 455](spark_book.pdf#page=455) [Ref: 458](spark_book.pdf#page=458) [Ref: 462](spark_book.pdf#page=462) [Ref: 469](spark_book.pdf#page=469) [Ref: 452](spark_book.pdf#page=452) [Ref: 456](spark_book.pdf#page=456) [Ref: 459](spark_book.pdf#page=459) [Ref: 463](spark_book.pdf#page=463) [Ref: 470](spark_book.pdf#page=470) [Ref: 453](spark_book.pdf#page=453) [Ref: 457](spark_book.pdf#page=457) [Ref: 461](spark_book.pdf#page=461) [Ref: 464](spark_book.pdf#page=464) [Ref: 471](spark_book.pdf#page=471)</em></div>

Linear algebra sits at the mathematical core of virtually every machine learning algorithm, from principal component analysis to neural network gradient descent. Apache Spark's MLlib exposes a suite of **distributed matrix abstractions** — `RowMatrix`, `IndexedRowMatrix`, `CoordinateMatrix`, and `BlockMatrix` — each designed for a different structural assumption about how your data is shaped and how densely it is populated. These are not wrappers around a single monolithic matrix stored on the driver; they are genuine distributed data structures whose rows or blocks live as RDD partitions across the cluster, enabling linear algebra at a scale that no single machine could accommodate.

The problem these abstractions solve is deceptively simple to state but technically brutal to implement: how do you perform operations like matrix multiplication, singular value decomposition (SVD), or Gram matrix computation when the matrix has, say, 10 billion rows and 50,000 columns? The answer is to exploit the mathematical structure of each operation — that matrix-vector products can be expressed as independent row dot-products, that the Gram matrix `A^T A` decomposes naturally across partitions, and that the top-k singular vectors of a massive matrix can be computed by solving a much smaller dense eigenproblem on the driver after a distributed reduction pass. Spark's linear algebra stack orchestrates exactly this split-level computation: distributed work on the executors via the JVM and RDD operations, and dense local computation on the driver via **Breeze** (Scala's numerical computing library) backed by **BLAS/LAPACK** native routines. Recent versions further integrate **Apache Arrow** column vectors for zero-copy data transfer between JVM and Python worker processes, eliminating serialization overhead in PySpark-based linear algebra workflows. 

---

## 🏗️ Architectural Deep Dive 

### How It Works Under the Hood

The distributed matrix abstractions in `org.apache.spark.mllib.linalg.distributed` are all backed by RDDs, not DataFrames. This is a deliberate design choice: linear algebra operations are iterative, involve repeated passes over the same data (e.g., multiple matrix-vector products in Lanczos iterations), and benefit from RDD caching semantics. A `RowMatrix` is simply an `RDD[Vector]`, where each row is a `breeze.linalg.Vector` (either dense or sparse) living in executor JVM heap memory. There are no row indices stored — the matrix is positionally defined only. An `IndexedRowMatrix` wraps `RDD[IndexedRow]`, adding a `Long` index to each row so that rows can be joined, reordered, or mapped back to their original identifiers after decomposition. A `CoordinateMatrix` stores `RDD[MatrixEntry]` — explicit `(row, col, value)` triples — and is the natural representation for ultra-sparse matrices (e.g., user-item rating matrices with 0.01% density) where storing dense rows would be catastrophically wasteful. `BlockMatrix` partitions the matrix into rectangular sub-blocks stored as `RDD[((Int,Int), Matrix)]`, enabling efficient distributed block-sparse matrix multiplication by routing blocks to the correct executors using a grid-partitioned key scheme.

The crown jewel of the linear algebra stack is **distributed Truncated SVD**. For a matrix `A` of shape `(m × n)` with `m >> n`, Spark computes the top-k singular values and vectors using a **two-phase approach**. In Phase 1, executors compute the Gram matrix `G = A^T A` (of shape `n × n`) via a distributed dot-product reduction — each partition contributes a partial `n × n` outer product, and these are summed across the cluster in a tree-reduce. Phase 2 runs entirely on the driver: Breeze invokes LAPACK's `dsyevd` (symmetric eigenvalue decomposition) on the small dense `G`, extracts the top-k eigenvectors, then computes the left singular vectors `U = A V Σ^{-1}` via another distributed pass. This architecture means the expensive `O(m·n²)` work is parallelized, while the `O(n³)` eigendecomposition — which is cheap when `n` is the feature count — runs locally.

BLAS (Basic Linear Algebra Subprograms) and LAPACK (Linear Algebra PACKage) are native Fortran/C libraries that Breeze delegates to via **netlib-java**, which at runtime auto-detects whether a hardware-optimized implementation (OpenBLAS, Intel MKL, ATLAS) is available. When OpenBLAS is loaded, DGEMM (dense matrix-matrix multiplication) achieves near-peak FLOP/s by exploiting SIMD vectorization and CPU cache locality — this is why Spark's native BLAS acceleration can be 10–50× faster than pure-JVM fallback code. Configuring `spark.driver.extraJavaOptions=-Dcom.github.fommil.netlib.BLAS=com.github.fommil.netlib.NativeSystemBLAS` is critical in production to avoid falling back to the reference BLAS implementation, which single-threads all matrix operations.

Apache Arrow enters this picture as the serialization format for the JVM-to-Python boundary. When PySpark calls `toArrow()` on a DataFrame containing feature vectors, Spark serializes the column-oriented data into Arrow's in-memory columnar format without copying individual values — the Arrow buffer is handed to the Python process via a memory-mapped file or socket, and NumPy/pandas can read it with zero deserialization overhead. For linear algebra workloads that bridge Spark preprocessing and scikit-learn or NumPy model fitting, this reduces Python worker startup overhead from seconds (with pickle serialization) to milliseconds.

```scala
Driver JVM Executor JVMs (RDD Partitions)
┌──────────────────────────────────┐ ┌─────────────────────────────────────┐
│ SparkContext / DAGScheduler │ │ Partition 0: RDD[Vector] rows 0..k │
│ │◀──────▶│ Partition 1: RDD[Vector] rows k..2k│
│ Phase 2: LAPACK dsyevd │ │ Partition N: RDD[Vector] rows ... │
│ ┌────────────────────────────┐ │ └───────────┬─────────────────────────┘
│ │ Breeze DenseMatrix (n×n) │ │ │ treeReduce (partial G=A^TA)
│ │ netlib-java → OpenBLAS │ │◀───────────────────┘
│ │ dsyevd → top-k eigenvecs │ │
│ └────────────────────────────┘ │ ┌──────────────────────────────────────┐
│ │ │ Arrow IPC Buffer (columnar) │
│ Phase 3: U = A·V·Σ⁻¹ (RDD map)│──────▶│ JVM → Python (mmap, zero-copy) │
└──────────────────────────────────┘ │ NumPy array view (no deserialization)│
 └──────────────────────────────────────┘

CoordinateMatrix BlockMatrix (2×2 grid)
RDD[MatrixEntry] RDD[((blockRow,blockCol), Matrix)]
┌───────────────┐ ┌───────────┬───────────┐
│(0,5,0.9) │ toBlockMatrix│ B(0,0) │ B(0,1) │
│(1,2,0.3) │─────────────▶│ dense │ sparse │
│(3,8,1.1) │ ├───────────┼───────────┤
│ ... │ │ B(1,0) │ B(1,1) │
└───────────────┘ └───────────┴───────────┘ 
```

### Key Internal Components

- **`RowMatrix` / `IndexedRowMatrix`:** Both store rows as `RDD[Vector]` or `RDD[IndexedRow]` in executor heap memory. `RowMatrix.computeGramianMatrix()` triggers a distributed treeAggregate that accumulates the symmetric `n×n` Gram matrix with `O(n²)` memory per executor partition — at `n = 10,000` features this is 800 MB per partition and will trigger executor OOM if not monitored.

- **`CoordinateMatrix` ↔ `BlockMatrix` Conversion:** `CoordinateMatrix.toBlockMatrix(rowsPerBlock, colsPerBlock)` performs a shuffle keyed by `(row / rowsPerBlock, col / colsPerBlock)`, grouping entries into sub-blocks. Choosing block dimensions that are powers of 2 (e.g., 1024×1024) aligns with BLAS's internal tiling strategies and maximizes cache reuse during local matrix multiplication.

- **Breeze + netlib-java BLAS/LAPACK Bridge:** Breeze's `DenseMatrix` operations (`*`, `\`, `svd`) dispatch through netlib-java's JNI layer to native BLAS routines. The JNI call overhead is negligible for matrices larger than ~100×100, but for tiny matrices (e.g., 10×10 local aggregations) pure-JVM Breeze is faster due to JNI call setup cost.

- **Apache Arrow IPC Format:** Arrow represents columnar batches as a sequence of `RecordBatch` messages, each containing a schema and flat memory buffers for validity bitmaps, offsets, and values. Spark's `ArrowConverters.toBatchIterator()` converts an RDD partition of InternalRow objects into Arrow `RecordBatch` objects in a single pass, achieving throughput of ~1 GB/s on modern hardware — versus ~50 MB/s for Python pickle serialization. 

---

## ⚠️ Critical Concepts & Common Pitfalls 

### The Gram Matrix Memory Explosion at Scale

When calling `RowMatrix.computeSVD(k, computeU = true)` on a matrix with `n` features, Spark must materialize the full `n × n` Gram matrix on the driver. At `n = 50,000` (a common NLP embedding dimension), the Gram matrix requires `50,000² × 8 bytes ≈ 20 GB` of driver heap memory. The default driver memory is 1 GB — this computation will throw `java.lang.OutOfMemoryError: Java heap space` without `--driver-memory 24g` or higher. The error is silent about the root cause; you will see a generic OOM in the driver logs. The fix is either to increase driver memory, to reduce `n` via feature selection before SVD, or to switch to iterative Krylov methods (e.g., ARPACK via `spark.ml.feature.PCA` with `setK` small) that avoid materializing the full Gram matrix.

The subtler trap is that `computeU = true` requires a second full pass over the RDD to compute the left singular vectors `U = A V Σ^{-1}`. If the input `RowMatrix` was not cached (`.cache()` called before `computeSVD`), Spark recomputes the entire RDD lineage twice — once for the Gram matrix reduction and once for the `U` computation. On a 100-node cluster with 500 GB of input data, this doubles your wall-clock time and doubles your cloud compute cost. Always call `.cache()` on the input `RowMatrix` before invoking `computeSVD`. 

### Native BLAS Detection Failure and the Silent Fallback

Spark silently falls back to the pure-JVM reference BLAS implementation (`F2jBLAS`) when native libraries are not found on the executor classpath. This fallback is single-threaded and has no SIMD optimization — a 1000×1000 DGEMM takes ~500ms in F2jBLAS versus ~5ms in OpenBLAS with AVX-512. The failure is logged at INFO level (`Using F2j BLAS`) rather than WARN or ERROR, so it is trivially missed in production deployments. To verify native BLAS is loaded, grep executor logs for `"NativeSystemBLAS"` or `"NativeRefBLAS"`. The correct fix is to pre-install `libopenblas-dev` on all worker nodes and set `OPENBLAS_NUM_THREADS=1` to prevent OpenBLAS from spawning thread pools that compete with Spark's task threads — running OpenBLAS with its default thread count inside a multi-task executor causes thread oversubscription and can reduce throughput by 40%. 

---

## 📊 Performance Characteristics

| Operation | Complexity | Shuffle? | Notes |
|-----------|-----------|---------|-------|
| `RowMatrix.computeGramianMatrix()` | O(m·n²) distributed + O(n²) driver | No | Memory: n² × 8B on driver; OOM at n > 30K with default driver memory |
| `CoordinateMatrix.toBlockMatrix()` | O(nnz · log(partitions)) | Yes | Shuffle keys are `(blockRow, blockCol)`; tune block size to 1024 for BLAS alignment |
| `BlockMatrix.multiply(B)` | O(blocks · localBlock³) | Yes | Requires matching inner dimension block sizes; mis-match throws `IllegalArgumentException` |
| `RowMatrix.computeSVD(k)` | O(m·n·k) + O(n³) driver | No (treeReduce) | 2 passes over RDD if `computeU=true`; cache input to avoid double recomputation |
| `IndexedRowMatrix.toCoordinateMatrix()` | O(m·n) | No | Explodes dense rows to (row,col,val) triples; only use for sparse downstream ops |
| Arrow `toArrow()` conversion | O(rows) | No | ~1 GB/s throughput; avoids pickle; requires `spark.sql.execution.arrow.pyspark.enabled=true` | 

---

## 💻 Code Examples 

### Example 1: Distributed Gram Matrix and Truncated SVD on a RowMatrix

> **What this demonstrates:** The full distributed SVD pipeline — constructing a `RowMatrix` from an RDD, caching it to avoid double recomputation, invoking truncated SVD, and extracting the top-k singular values and right singular vectors for dimensionality reduction.

```scala
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD

// Simulate a 1M-row × 200-column feature matrix from a DataFrame
// Each row is a dense feature vector (e.g., TF-IDF embeddings post-normalization)
val rawRDD: RDD[Vector] = spark.read.parquet("/data/features")
 .select("feature_vector")
 .rdd
 .map(row => row.getAs[org.apache.spark.ml.linalg.Vector](0))
 // Convert spark.ml Vector → spark.mllib Vector (required by RowMatrix)
 .map(v => Vectors.dense(v.toArray))

// CRITICAL: cache before SVD to prevent the RDD being recomputed twice.
// Once for computeGramianMatrix (Phase 1) and once for U = A·V·Σ⁻¹ (Phase 2).
// Without .cache(), on 1M rows × 200 cols you double your I/O cost.
rawRDD.cache()
rawRDD.count() // Materialize the cache eagerly before SVD begins

val mat = new RowMatrix(rawRDD)

// Compute top-20 singular triplets.
// computeU = true: triggers a second distributed pass to compute left singular vectors U.
// rCond = 1e-9: condition number threshold below which singular values are treated as zero.
val svd = mat.computeSVD(k = 20, computeU = true, rCond = 1e-9)

// svd.s: DenseVector of singular values (length k) — lives on driver, shape (20,)
// svd.V: DenseMatrix of right singular vectors (n × k) — lives on driver
// svd.U: RowMatrix of left singular vectors (m × k) — distributed across executors
val singularValues = svd.s // Breeze DenseVector: σ₁ ≥ σ₂ ≥ ... ≥ σ₂₀
val rightVectors = svd.V // Dense n×k matrix — the "concept" directions in feature space
val leftVectors = svd.U // RDD[Vector] — each row is the k-dim representation of one sample

// Explained variance ratio: σᵢ² / Σσᵢ²
val totalVariance = singularValues.toArray.map(s => s * s).sum
val explainedRatio = singularValues.toArray.map(s => (s * s) / totalVariance)
explainedRatio.zipWithIndex.foreach { case (r, i) =>
 println(f"PC${i+1}%2d: ${r * 100}%.2f%% variance explained")
}

// Persist the low-dimensional representation back to storage
leftVectors.rows
 .zipWithIndex() // (Vector, Long) — re-attach row index
 .map { case (vec, idx) => (idx, vec.toArray) }
 .toDF("id", "embedding")
 .write.parquet("/data/svd_embeddings") 
```

> **Mastery Note:** The `computeGramianMatrix()` call inside `computeSVD` uses `treeAggregate` with a depth of 2 by default, meaning partial `n×n` Gram matrices are summed in a two-level tree rather than shuffled to the driver in a single reduce. At `n = 200`, each partial Gram matrix is `200 × 200 × 8 = 320 KB` — trivially small. At `n = 10,000`, each partial becomes ~800 MB, and `treeAggregate` depth must be increased via `RowMatrix.computeSVD`'s internal `brzSvd` path to avoid driver memory pressure. The `svd.V` right singular vectors live entirely on the driver as a `DenseMatrix`; broadcast them to executors if you need to project new data into the SVD space without rerunning the decomposition.

---

### Example 2: CoordinateMatrix for Sparse User-Item Ratings → BlockMatrix Multiplication

> **What this demonstrates:** How to construct a `CoordinateMatrix` from a sparse ratings RDD, convert it to a `BlockMatrix` with BLAS-aligned block sizes, and perform a distributed matrix transpose-multiply to compute the item-item similarity matrix.

```scala
import org.apache.spark.mllib.linalg.distributed.{CoordinateMatrix, MatrixEntry}

// Raw ratings: (userId: Long, itemId: Long, rating: Double)
// Density: 0.01% — CoordinateMatrix is the correct abstraction here.
// A RowMatrix would waste 99.99% of memory on zeros.
val ratingsRDD: RDD[MatrixEntry] = spark.read
 .parquet("/data/ratings")
 .rdd
 .map(row => MatrixEntry(
 row.getLong(0), // userId as row index
 row.getLong(1), // itemId as column index
 row.getDouble(2) // explicit rating value
 ))

// CoordinateMatrix: no materialization yet — this is lazy until an action is called.
val coordMat = new CoordinateMatrix(ratingsRDD)

// Convert to BlockMatrix with 1024×1024 blocks.
// Block size 1024 is chosen deliberately: OpenBLAS's DGEMM tiles internally to 256 or 512,
// and 1024×1024 blocks guarantee that local BLAS calls are large enough to amortize JNI overhead.
// Too-small blocks (e.g., 64×64) cause thousands of tiny BLAS calls with high JNI overhead.
val blockMat = coordMat.toBlockMatrix(rowsPerBlock = 1024, colsPerBlock = 1024)

// Validate: ensures all blocks conform to the declared dimensions.
// Throws IllegalArgumentException if block sizes are inconsistent — call this in staging.
blockMat.validate()

// Compute A^T · A: the item-item co-occurrence / Gram matrix in the rating space.
// This is a distributed block matrix multiplication:
// For each pair of block columns (j, k), executors multiply matching block rows and sum.
// The result is an (numItems × numItems) symmetric block matrix.
val itemItemGram = blockMat.transpose.multiply(blockMat)

// Convert back to CoordinateMatrix to extract top-N similar items per item
val similarities = itemItemGram.toCoordinateMatrix()
 .entries
 .filter(e => e.i != e.j) // exclude diagonal (self-similarity = 1.0 always)
 .map(e => (e.i, (e.j, e.value))) // group by item i
 .groupByKey()
 .mapValues(_.toSeq.sortBy(-_._2).take(10)) // top-10 similar items by score
 .toDF("item_id", "top_similar")
 .write.parquet("/data/item_similarity")
```

> **Mastery Note:** `CoordinateMatrix.toBlockMatrix()` performs a shuffle keyed by `(row / rowsPerBlock, col / colsPerBlock)`. For a matrix with 100M non-zero entries, this shuffle writes and reads ~100M records. The default `spark.sql.shuffle.partitions = 200` is almost always too low for this operation — set it to `numItems / 1024` to ensure each resulting block lands in its own partition, preventing partition skew when one block key receives disproportionately many non-zero entries. The `BlockMatrix.multiply()` operation requires that the inner block dimensions match exactly; mismatched `colsPerBlock` on the left vs `rowsPerBlock` on the right throws `IllegalArgumentException: colsPerBlock of A doesn't match rowsPerBlock of B`.

---

### Example 3: Breeze Native BLAS Verification and Local Dense Linear Algebra

> **What this demonstrates:** How to verify that native BLAS (OpenBLAS/MKL) is active on executors, and how to perform high-performance local dense linear algebra within a `mapPartitions` call using Breeze — the pattern used inside MLlib's own implementations.

```scala
import breeze.linalg.{DenseMatrix, DenseVector, svd => breezeSvd, inv}
import breeze.linalg.svd.SVD
import com.github.fommil.netlib.BLAS

// --- Step 1: Verify BLAS implementation on each executor ---
// Run this as a diagnostic job before any production linear algebra workload.
val blasInfo = sc.parallelize(1 to sc.defaultParallelism, sc.defaultParallelism)
 .mapPartitions { _ =>
 // BLAS.getInstance() triggers native library detection at first call.
 // Returns NativeSystemBLAS if OpenBLAS/MKL found, F2jBLAS if not.
 val blas = BLAS.getInstance()
 val implName = blas.getClass.getName
 // "com.github.fommil.netlib.NativeSystemBLAS" = GOOD (hardware-accelerated)
 // "com.github.fommil.netlib.F2jBLAS" = BAD (pure-JVM, 10-50x slower)
 Iterator(s"${java.net.InetAddress.getLocalHost.getHostName}: $implName")
 }
 .collect()

blasInfo.foreach(println)
// Expected output: worker-1: com.github.fommil.netlib.NativeSystemBLAS
// If you see F2jBLAS: install libopenblas-dev on all workers and restart executors.

// --- Step 2: Efficient per-partition local SVD using Breeze ---
// Use case: each partition contains a "mini-batch" matrix (e.g., 10K rows × 100 cols).
// We compute a local thin SVD per partition and return only the top-5 singular values.
val partitionSVDs: RDD[Array[Double]] = rawRDD.mapPartitions { rowIter =>
 val rows = rowIter.toArray
 if (rows.isEmpty) Iterator.empty
 else {
 val m = rows.length
 val n = rows.head.size

 // Materialize partition as a Breeze DenseMatrix (column-major, required by BLAS)
 // Breeze stores DenseMatrix in Fortran column-major order to match LAPACK conventions.
 val A = DenseMatrix.zeros[Double](m, n)
 rows.zipWithIndex.foreach { case (vec, i) =>
 A(i, ::) := DenseVector(vec.toArray).t
 }

 // Thin SVD via LAPACK dgesdd (divide-and-conquer, faster than dgesvd for tall-skinny A)
 // SVD.apply returns (U: m×m, s: min(m,n), Vt: n×n) — full decomposition
 val SVD(u, s, vt) = breezeSvd(A)

 // Return only top-5 singular values — sufficient for per-partition diagnostics
 Iterator(s.toArray.take(5))
 }
}
```

> **Mastery Note:** Breeze's `DenseMatrix` is stored in column-major (Fortran) order to match LAPACK's memory layout assumptions. If you transpose a row-major array into a Breeze matrix incorrectly, LAPACK routines silently operate on the wrong memory layout and produce numerically incorrect results — a particularly insidious bug because no exception is thrown. Always construct Breeze matrices using `DenseMatrix.create(rows, cols, data)` where `data` is already column-major, or use the `(i, ::) := row` pattern which handles transposition correctly. The `breezeSvd` call dispatches through netlib-java's JNI layer to LAPACK's `dgesdd` routine — on a 10,000×100 matrix, this takes ~20ms with OpenBLAS versus ~400ms with F2jBLAS, making native BLAS verification a mandatory production prerequisite.

---

### Example 4: Apache Arrow Zero-Copy Bridge for PySpark Linear Algebra

> **What this demonstrates:** The full Arrow-accelerated pipeline from Spark DataFrame to NumPy array in Python — enabling large-scale feature matrix extraction for scikit-learn model fitting with near-zero serialization overhead.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandas as pd
import numpy as np
from sklearn.decomposition import TruncatedSVD
from sklearn.preprocessing import normalize

spark = SparkSession.builder \
 .appName("ArrowLinearAlgebra") \
 .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
 # Arrow IPC batch size: 10K rows per Arrow RecordBatch.
 # Larger batches reduce Arrow overhead but increase memory pressure on Python worker.
 # 10K rows × 200 features × 8 bytes = 16 MB per batch — safe for most configurations.
 .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000") \
 .getOrCreate()

# Load feature vectors stored as Spark ML VectorUDT columns
# The VectorUDT is stored internally as two arrays (indices, values) for sparse
# and a single flat array for dense — Arrow encodes both as fixed-size binary blobs.
features_df = spark.read.parquet("/data/ml_features") \
 .select("id", "features") \
 .repartition(200) # ensure 200 Arrow batches for the toPandas() conversion

# toPandas() with Arrow enabled:
# 1. Spark serializes each partition → Arrow RecordBatch (columnar, ~1 GB/s)
# 2. RecordBatches are transferred to Python driver via socket (not pickle)
# 3. pyarrow reconstructs a pandas DataFrame from Arrow buffers with zero data copy
# WITHOUT Arrow: each partition pickled row-by-row → ~50 MB/s, 20× slower
pdf = features_df.toPandas()

# Extract the feature vectors — stored as numpy arrays inside the pandas Series
# when Arrow is enabled; stored as Python lists when Arrow is disabled.
# The numpy array is a VIEW into the Arrow buffer memory — no allocation.
feature_matrix = np.vstack(pdf["features"].values) # shape: (num_samples, num_features)
feature_matrix = normalize(feature_matrix, norm="l2") # L2-normalize rows (in-place on numpy)

# Run scikit-learn's randomized SVD (Halko et al. 2011) locally on the collected matrix.
# For matrices that fit in driver memory (< ~10 GB), sklearn's randomized SVD
# is 3-5× faster than Spark's exact SVD because it avoids distributed overhead.
# For matrices that DO NOT fit in driver memory, use Spark's RowMatrix.computeSVD instead.
tsvd = TruncatedSVD(n_components=50, algorithm="randomized", n_iter=5, random_state=42)
embeddings = tsvd.fit_transform(feature_matrix) # shape: (num_samples, 50)

# Explained variance — equivalent to Spark SVD's singular value ratios
explained = tsvd.explained_variance_ratio_.cumsum()
print(f"Cumulative variance explained by top-50 components: {explained[-1]*100:.1f}%")

# Write embeddings back to Spark — Arrow reverses the pipeline:
# numpy → pandas → Arrow RecordBatch → Spark InternalRow, again zero-copy
embeddings_pdf = pd.DataFrame(embeddings, columns=[f"dim_{i}" for i in range(50)])
embeddings_pdf["id"] = pdf["id"].values

# createDataFrame with Arrow: pandas → Arrow → Spark (no pickle)
# Requires spark.sql.execution.arrow.pyspark.enabled = true (set above)
embeddings_sdf = spark.createDataFrame(embeddings_pdf)
embeddings_sdf.write.mode("overwrite").parquet("/data/svd50_embeddings")
```

> **Mastery Note:** The `spark.sql.execution.arrow.pyspark.enabled` flag only accelerates `toPandas()` and `createDataFrame(pandas_df)` — it does **not** affect Python UDFs or `mapInPandas`. For `toPandas()`, Arrow converts an entire Spark partition into a single Arrow `RecordBatch` columnar buffer; pandas then wraps that buffer as a `pyarrow.Table` and finally as a `pandas.DataFrame` with zero memory allocation for the data arrays themselves. At `spark.sql.execution.arrow.maxRecordsPerBatch = 10000`, a 200-feature matrix generates 16 MB batches — increase this to 50,000 rows (80 MB) if the Python driver has ≥16 GB of memory, as fewer, larger batches reduce Arrow IPC framing overhead. One critical failure mode: if any column contains a Spark `VectorUDT` or a custom `StructType` with nested nullability, Arrow will throw `ArrowInvalidError: Schema mismatch`; the fix is to explode the struct into flat primitive columns before calling `toPandas()`.

---

## 🎯 Mastery Checklist

To achieve true mastery of Distributed Linear Algebra in Apache Spark:

- [ ] Understand why `RowMatrix` has no row indices and when `IndexedRowMatrix` is required (e.g., after SVD to map `U` rows back to original record IDs)
- [ ] Know that `computeSVD(k, computeU=true)` makes **two** passes over the RDD and that `.cache()` before calling it is mandatory in production
- [ ] Be able to diagnose F2jBLAS fallback from executor logs (`grep "F2j"`) and know that the fix is `libopenblas-dev` + `OPENBLAS_NUM_THREADS=1`
- [ ] Understand the Gram matrix memory formula `n² × 8 bytes` and predict driver OOM before it happens
- [ ] Know that `CoordinateMatrix.toBlockMatrix()` triggers a shuffle and that block size should be powers of 2 aligned to BLAS tiling (1024×1024 recommended)
- [ ] Understand that `BlockMatrix.multiply()` requires matching inner block dimensions and will throw `IllegalArgumentException` on mismatch — call `.validate()` in staging
- [ ] Know how Arrow `maxRecordsPerBatch` affects memory and throughput tradeoffs in the `toPandas()` path
- [ ] Be able to explain why Breeze uses column-major (Fortran) memory layout and what goes wrong when row-major arrays are passed to LAPACK routines
- [ ] Know when to prefer sklearn's randomized SVD (data fits in driver memory) over Spark's distributed SVD (data does not fit)
- [ ] Understand how `treeAggregate` depth affects Gram matrix reduction network traffic and driver memory pressure

---

## 📚 Summary

Spark's distributed linear algebra stack is a carefully designed split-level system: the expensive, embarrassingly-parallel work (partial Gram matrix accumulation, row projections, block matrix products) runs distributed across executor JVMs as RDD transformations, while the dense, numerically-sensitive computations (eigendecomposition, SVD of small matrices) run locally on the driver via Breeze's LAPACK bindings. The four matrix abstractions — `RowMatrix`, `IndexedRowMatrix`, `CoordinateMatrix`, and `BlockMatrix` — are not interchangeable; each is optimized for a specific combination of sparsity, indexing need, and downstream operation. Choosing the wrong abstraction (e.g., `RowMatrix` for a 0.01%-dense matrix) causes catastrophic memory waste and executor OOM errors that trace to the wrong component in the Spark UI. 

The native BLAS/LAPACK stack via netlib-java and OpenBLAS is the single most impactful performance lever in the entire linear algebra pipeline. A 10–50× gap between native and reference BLAS is not an edge case — it is the baseline difference on every executor that processes local dense matrices. Verifying BLAS implementation type (`NativeSystemBLAS` vs `F2jBLAS`) should be a mandatory step in any Spark cluster health check. Similarly, the `OPENBLAS_NUM_THREADS=1` configuration is non-negotiable in multi-task executors to prevent thread oversubscription. 

Apache Arrow closes the final gap in the linear algebra workflow: the boundary between JVM and Python. By encoding columnar data in a shared-memory format that both pyarrow and pandas understand natively, Arrow eliminates the serialization bottleneck that historically made PySpark ML workflows 10–20× slower than their Scala equivalents. Combined with scikit-learn's randomized algorithms for data that fits in memory, the Arrow-accelerated pipeline enables a pragmatic hybrid architecture: use Spark for distributed preprocessing and feature engineering, collect via Arrow, and fit models locally — achieving both the scale of distributed computing and the numerical richness of the Python ML ecosystem. 

