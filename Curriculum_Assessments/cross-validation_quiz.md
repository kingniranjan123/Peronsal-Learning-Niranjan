# Cross Validation - Senior/Staff Assessment

## Part 1: True/False Questions

1. **Question**: Setting `parallelism > 1` in `CrossValidator` forces Spark to distribute the folds across different clusters instead of executors.
   **Answer**: False
   **Mastery Explanation**: `parallelism` controls the number of threads in the driver submitting jobs concurrently, which Catalyst and Tungsten execute across the same cluster's executors, not different clusters.

2. **Question**: Caching the input DataFrame before passing it to `CrossValidator` prevents redundant disk I/O because Tungsten directly accesses the binary-encoded data in memory for every hyperparameter trial.
   **Answer**: True
   **Mastery Explanation**: True, caching ensures that the Tungsten engine avoids re-evaluating the DataFrame lineage and reads directly from off-heap/on-heap memory in a binary format, crucial for iterative ML workloads.

3. **Question**: The Spark ML `CrossValidator` is implemented as a Transformer, because it transforms the hyperparameters into a best model.
   **Answer**: False
   **Mastery Explanation**: `CrossValidator` is an Estimator (specifically a meta-Estimator). It calls `fit()` on data to produce a `CrossValidatorModel`, which is a Transformer.

4. **Question**: When `parallelism` is set too high in a `CrossValidator`, the primary risk is executors running out of memory due to data duplication.
   **Answer**: False
   **Mastery Explanation**: The primary risk of extremely high parallelism in `CrossValidator` is Driver OOM or overwhelming the cluster manager with concurrent task requests, as the driver holds the threads submitting jobs.

5. **Question**: If a node fails during the cross-validation of a specific model, Spark will recompute all folds for that specific hyperparameter combination from scratch.
   **Answer**: False
   **Mastery Explanation**: Thanks to Spark's lineage tracking and fault tolerance, it will only recompute the specific lost partitions (tasks) needed for that fold, not the entire fold or all folds.

6. **Question**: Using `TrainValidationSplit` is always preferred over `CrossValidator` when the dataset size is in petabytes because it entirely bypasses the Tungsten execution engine.
   **Answer**: False
   **Mastery Explanation**: It is preferred for massive datasets because it evaluates parameters exactly once (saving computation), but it absolutely still uses the Tungsten execution engine for processing.

7. **Question**: Incorporating a `weightCol` in a LogisticRegression estimator inside a `CrossValidator` ensures the weights are automatically propagated to the training set of every fold.
   **Answer**: True
   **Mastery Explanation**: Spark's Pipeline API preserves metadata and schema lineage, seamlessly passing the weight column through the internal fold splits for imbalanced learning.

8. **Question**: Spark's `CrossValidator` evaluates all hyperparameter combinations across all folds completely asynchronously using Spark Structured Streaming internals.
   **Answer**: False
   **Mastery Explanation**: It submits them as standard batch Spark jobs using a thread pool on the driver based on the `parallelism` parameter, completely unrelated to Structured Streaming.

9. **Question**: Because `CrossValidator` trains multiple models, the final `bestModel` returned is an ensemble of the models trained across all folds for the best hyperparameter set.
   **Answer**: False
   **Mastery Explanation**: `CrossValidator` determines the best hyperparameters by averaging metrics across folds, but it then retrains a single final model on the *entire* dataset using those optimal parameters.

10. **Question**: Using Kryo serialization can mitigate network overhead during model validation when broadcasting task closures.
    **Answer**: True
    **Mastery Explanation**: Kryo is a highly optimized JVM serialization protocol that reduces the size of serialized task closures and broadcast variables compared to Java's default serialization.

## Part 2: Multiple Choice Questions

11. **Question**: Which component is responsible for optimizing the memory layout of the cached DataFrame during a `CrossValidator` run?
    A) Catalyst Optimizer
    B) Tungsten Execution Engine
    C) DAGScheduler
    D) BlockManager
    **Answer**: B
    **Mastery Explanation**: Tungsten is responsible for memory management and binary processing, using an optimized off-heap data format to avoid Java object overhead and GC pauses. Catalyst handles query plan optimization.

12. **Question**: You configure a `CrossValidator` with 4 folds, a `ParamGrid` of 5 combinations, and `parallelism=2`. How many total model trainings (excluding the final best model retraining) will occur, and how many are submitted concurrently by the driver?
    A) 20 trainings, 2 concurrent threads
    B) 9 trainings, 4 concurrent threads
    C) 20 trainings, 4 concurrent threads
    D) 5 trainings, 2 concurrent threads
    **Answer**: A
    **Mastery Explanation**: 4 folds * 5 combinations = 20 separate model trainings. `parallelism=2` means the driver maintains a thread pool of size 2, submitting up to 2 training jobs concurrently.

13. **Question**: When evaluating an imbalanced classification problem with `CrossValidator`, why is accuracy a poor metric choice for the `Evaluator`?
    A) Accuracy requires a `weightCol` to compute.
    B) It takes too long to compute accuracy using Tungsten.
    C) The model might simply predict the majority class, yielding high accuracy but failing to learn the minority class.
    D) Catalyst cannot optimize accuracy computations.
    **Answer**: C
    **Mastery Explanation**: Accuracy does not distinguish between classes. In a 99% imbalanced dataset, a model predicting only the negative class achieves 99% accuracy but is useless.

14. **Question**: What is the primary cause of Driver OOM when running `CrossValidator`?
    A) Broadcasting the input dataset to all executors.
    B) Setting the `parallelism` parameter excessively high, creating too many threads and concurrently collecting large model metadata or task results.
    C) RDD partitions being too large.
    D) Failing to use `df.cache()`.
    **Answer**: B
    **Mastery Explanation**: Each thread in the driver pool tracks a Spark job and holds metadata/results. Too many concurrent threads exhaust the driver's heap memory.

15. **Question**: In the provided Pipeline example, why is `numFeatures` of `HashingTF` tuned alongside `RandomForestClassifier` parameters?
    A) Because Spark mandates all stages to have tuned parameters.
    B) To optimize the interaction between feature space dimensionality and tree depth simultaneously.
    C) To prevent the `Tokenizer` from removing stop words.
    D) Because Tungsten requires dynamic feature sizing.
    **Answer**: B
    **Mastery Explanation**: Pipeline tuning allows joint optimization. The optimal depth of a tree is heavily dependent on the dimensionality of the feature space; tuning them together finds the global optimum.

16. **Question**: How does `TrainValidationSplit` differ from `CrossValidator` computationally?
    A) It evaluates parameters across K folds but skips retraining the final model.
    B) It trains on a single random split of the data, evaluating each parameter combination exactly once.
    C) It uses a validation DataFrame provided by the user instead of splitting the input.
    D) It bypasses Catalyst.
    **Answer**: B
    **Mastery Explanation**: `TrainValidationSplit` splits the data once (e.g., 80/20) and trains/evaluates each parameter set once, drastically reducing computation for massive datasets.

17. **Question**: If a `CrossValidator` runs out of executor memory during the fitting of a specific fold, what is the best initial mitigation?
    A) Increase `parallelism`.
    B) Decrease `parallelism`.
    C) Check partition sizes and potentially repartition the DataFrame.
    D) Switch to Java Serialization.
    **Answer**: C
    **Mastery Explanation**: Executor OOMs are usually caused by skewed partitions or partitions that are too large to fit in memory during model operations. Repartitioning balances the data. Decreasing parallelism helps Driver OOM, not Executor OOM.

18. **Question**: Which Evaluator is most appropriate for a highly imbalanced binary classification dataset?
    A) RegressionEvaluator (metricName='rmse')
    B) BinaryClassificationEvaluator (metricName='areaUnderROC')
    C) BinaryClassificationEvaluator (metricName='areaUnderPR')
    D) MulticlassClassificationEvaluator (metricName='accuracy')
    **Answer**: C
    **Mastery Explanation**: Precision-Recall (PR) curves are much more sensitive to performance on the minority positive class in imbalanced datasets compared to ROC curves.

19. **Question**: Spark's lazy evaluation benefits `CrossValidator` primarily by:
    A) Delaying model prediction until the user requests it.
    B) Allowing Catalyst to construct a single optimized physical plan for the transformation stages before execution begins on a fold.
    C) Bypassing network shuffles.
    D) Preventing Driver OOMs.
    **Answer**: B
    **Mastery Explanation**: Lazy evaluation lets Spark build up a DAG of transformations (like Pipeline stages) and optimize them via Catalyst into an efficient execution plan before triggering an action.

20. **Question**: When `df.cache()` is called before `CrossValidator.fit()`, where is the data stored by default?
    A) Disk only.
    B) Memory and Disk as Java Objects.
    C) Memory and Disk in a serialized format.
    D) In the Driver's memory.
    **Answer**: C
    **Mastery Explanation**: For DataFrames/Datasets, the default storage level is MEMORY_AND_DISK, and the data is stored in Tungsten's highly optimized binary columnar format, not as Java objects.

21. **Question**: What happens to the `Estimator` passed into `CrossValidator`?
    A) It is converted into a `Transformer`.
    B) It acts as a template; `CrossValidator` clones it and applies the `ParamMaps` for each trial.
    C) It is serialized and sent to a single executor to run a grid search locally.
    D) It is discarded after the first fold.
    **Answer**: B
    **Mastery Explanation**: The `CrossValidator` acts as a meta-estimator, using the provided Estimator as a blueprint, copying it, and injecting the specific parameters from the grid for each training job.

22. **Question**: If you forget to cache the dataset before running a `CrossValidator` with 10 folds and 100 parameter combinations, what is the architectural consequence?
    A) The driver will OOM immediately.
    B) The Spark job will fail due to MissingRDD errors.
    C) The entire DataFrame lineage (e.g., reading from HDFS/S3 and parsing) is recomputed 1000 times.
    D) Tungsten will automatically cache it for you.
    **Answer**: C
    **Mastery Explanation**: Without caching, Spark's lazy evaluation means every action (each model fit/evaluate) triggers a full recomputation of the DataFrame from the source, causing massive I/O overhead.

23. **Question**: Which of the following is NOT an advantage of Tungsten in the context of ML Pipeline tuning?
    A) Explicit memory management bypassing JVM Garbage Collection.
    B) Cache-aware computation.
    C) Code generation for expression evaluation.
    D) Automatically tuning ML hyperparameters.
    **Answer**: D
    **Mastery Explanation**: Tungsten optimizes the physical execution of data operations (CPU and memory efficiency). It does not know anything about ML hyperparameters; that is handled by `CrossValidator`.

24. **Question**: How does `CrossValidator` select the final best model?
    A) It picks the model from the fold that had the absolute highest evaluation metric.
    B) It averages the evaluation metric across all folds for each parameter combination, selects the combination with the best average, and retrains on the full dataset.
    C) It trains a meta-model to combine the predictions of all folds.
    D) It selects the parameter set that trained the fastest.
    **Answer**: B
    **Mastery Explanation**: Cross-validation assesses the average performance of parameter sets across different data subsets to ensure generalizability, then trains a final model on all data using the best set.

25. **Question**: What metadata allows the `CrossValidator` to track the lineage of a `weightCol`?
    A) The DAGScheduler graph.
    B) Spark SQL DataFrame Schema Metadata.
    C) The RDD partitioner.
    D) Catalyst rules.
    **Answer**: B
    **Mastery Explanation**: The DataFrame schema contains metadata attributes (like ML attributes, nominal/continuous flags) that Pipeline stages use to validate and track column roles (like features, labels, weights) across transformations.

## Part 3: Small Twist Questions

26. **Question**: You set `parallelism=10` on a cluster with 5 executors (4 cores each). The tuning job crashes with a Driver OOM. You change `parallelism` to `100`. What happens?
    **Answer**: The job will crash even faster with a Driver OOM.
    **Mastery Explanation**: Increasing parallelism increases the number of concurrent threads and memory footprint on the driver. To fix a Driver OOM, you must *decrease* parallelism (e.g., to 2 or 4).

27. **Question**: You switch from `CrossValidator` to `TrainValidationSplit` to save time. You set `trainRatio=1.0`. What is the result?
    **Answer**: The job will fail with an exception.
    **Mastery Explanation**: A `trainRatio` of 1.0 means 100% of the data is used for training and 0% for validation. The Evaluator will have no data to evaluate the metric, causing a runtime error. It must be strictly less than 1.0.

28. **Question**: You are tuning `RandomForestClassifier`. You include `addGrid(rf.predictionCol, ['pred1', 'pred2'])` in your `ParamGridBuilder`. What architectural impact does this have on the final model accuracy?
    **Answer**: Absolutely zero impact on accuracy.
    **Mastery Explanation**: `predictionCol` merely changes the name of the output column in the DataFrame. It does not affect the algorithm's learning, tree building, or the actual predictions made.

29. **Question**: You cache your DataFrame using `df.cache()`, but the Spark UI shows the storage level is `Disk Serialized`. Why?
    **Answer**: `df.cache()` defaults to `MEMORY_AND_DISK`. If it's only on disk, the cluster does not have enough execution/storage memory to hold the Tungsten-encoded partitions, forcing a spill to disk.
    **Mastery Explanation**: Caching is lazy and bound by memory limits. If the memory is full, Spark gracefully spills the cached data to disk.

30. **Question**: In the Scala example, what happens if `trainingData.cache()` is called, but no action (like `count()` or `cv.fit()`) is executed immediately after?
    **Answer**: The data is not actually cached in memory yet.
    **Mastery Explanation**: `cache()` is a lazy transformation in Spark. The actual materialization of the cache only occurs when the first action is triggered.

31. **Question**: You use `CrossValidator` with `numFolds=2`. You pass a DataFrame with exactly 1 row. What happens?
    **Answer**: The job fails during fold splitting.
    **Mastery Explanation**: `CrossValidator` cannot split 1 row into 2 folds where both training and validation sets have data. One fold will be empty, causing the ML algorithm or evaluator to crash.

32. **Question**: You build a Pipeline with `StandardScaler` and `LogisticRegression`. You pass this Pipeline to `CrossValidator`. Does `StandardScaler` leak information from the validation fold into the training fold?
    **Answer**: No.
    **Mastery Explanation**: Because the Pipeline is inside the `CrossValidator`, the data is split *first*, and then `fit()` is called on the Pipeline using only the training fold.

33. **Question**: You pass `StandardScaler` followed by `CrossValidator(LogisticRegression)` as a Pipeline. Does `StandardScaler` leak information?
    **Answer**: Yes.
    **Mastery Explanation**: Because `StandardScaler` is *outside* and *before* the `CrossValidator`, it is fit on the entire dataset (train + validation) before splitting, causing data leakage.

34. **Question**: You set `weightCol='class_weight'` on your Estimator, but the column contains negative values. What happens during Tungsten execution?
    **Answer**: The MLlib algorithm will throw an IllegalArgumentException.
    **Mastery Explanation**: Instance weights must be zero or positive. Negative weights are mathematically undefined for loss functions in MLlib.

35. **Question**: You configure `parallelism=4` but your cluster has only 1 executor with 1 core. How does execution proceed?
    **Answer**: Execution proceeds, but jobs run serially on the executor.
    **Mastery Explanation**: The driver launches 4 concurrent threads submitting jobs, but the cluster manager only has 1 core to offer. Thus, the jobs will queue up and run sequentially.

36. **Question**: You use a `ParamGrid` with 1 combination and `numFolds=3`. Does `CrossValidator` still retrain a final model?
    **Answer**: Yes.
    **Mastery Explanation**: Even with a single parameter combination, `CrossValidator` performs the 3-fold evaluation to compute the average metric, and then retrains the Estimator on the entire dataset.

37. **Question**: You change the `Evaluator` metricName from 'f1' to 'rmse' for a `RandomForestClassifier`. What happens?
    **Answer**: The job crashes.
    **Mastery Explanation**: 'rmse' is a regression metric expecting a continuous label. A classifier produces discrete predictions/probabilities, incompatible with `RegressionEvaluator`.

38. **Question**: In `TrainValidationSplit`, you set `parallelism=5` but the `ParamGrid` has only 2 combinations. How many threads do actual work concurrently?
    **Answer**: Only 2 threads.
    **Mastery Explanation**: There are only 2 parameter combinations to evaluate. The driver will use 2 threads to submit the 2 jobs. The remaining 3 threads in the pool will remain idle.

39. **Question**: You run `CrossValidator` and observe high GC overhead in the executors. You switch from G1GC to ParallelGC. Does this reduce the number of objects created by the ML algorithm?
    **Answer**: No.
    **Mastery Explanation**: Changing the Garbage Collector alters how dead objects are cleaned up, but it does not change the memory allocation patterns or object creation rate of the Catalyst/MLlib code.

40. **Question**: You apply `df.repartition(100)` right before `cv.fit(df)`. The dataset is 10MB. What is the performance impact?
    **Answer**: Severe degradation due to excessive task overhead.
    **Mastery Explanation**: Repartitioning a tiny dataset into 100 partitions creates micro-tasks. The overhead of task serialization, scheduling by the DAGScheduler, and network communication vastly outweighs execution time.

## Part 4: Coding & Debugging Questions

41. **Question**: Identify the memory leak/blocker:
    ```python
    for i in range(10):
        cvModel = cv.fit(df)
        predictions = cvModel.transform(test_df)
        predictions.show()
    ```
    **Answer**: Repeatedly calling `fit()` in a loop without unpersisting or managing Spark UI lineage creates a massive DAG graph and retains large metadata in the driver, leading to Driver OOM.
    **Mastery Explanation**: Spark retains UI metadata and execution graph info. Iterative loops calling ML algorithms can bloat driver memory unless intermediate structures are cleared or checkpointing is used.

42. **Question**: A developer writes:
    ```python
    df = spark.read.csv("data.csv")
    cv = CrossValidator(...)
    cvModel = cv.fit(df)
    ```
    Why is this incredibly inefficient during tuning?
    **Answer**: Missing `df.cache()`.
    **Mastery Explanation**: Without caching, every fold and parameter combination triggers a full read and parse of the CSV file from disk, completely bypassing the benefits of Tungsten in-memory execution.

43. **Question**: What is wrong with this grid setup?
    ```python
    grid = ParamGridBuilder().addGrid(rf.maxDepth, [5, 10]).addGrid(lr.regParam, [0.1, 0.01]).build()
    cv = CrossValidator(estimator=rf, estimatorParamMaps=grid, ...)
    ```
    **Answer**: The grid includes parameters for `lr` (LogisticRegression), but the estimator passed to CV is `rf` (RandomForest).
    **Mastery Explanation**: `CrossValidator` will throw an exception during `fit()` because it cannot apply `lr.regParam` to the `RandomForestClassifier` estimator.

44. **Question**: Debug the execution plan blocker:
    ```python
    df = df.withColumn("random_val", F.rand())
    df.cache()
    cvModel = cv.fit(df)
    ```
    **Answer**: `F.rand()` is non-deterministic. If partitions are lost and recomputed, the random values change, corrupting the fold consistency.
    **Mastery Explanation**: Catalyst marks `rand()` as non-deterministic. If a cached partition is evicted and recomputed, the features change. Always provide a seed to `F.rand(seed=42)` in ML workloads.

45. **Question**: A user complains tuning takes 5 hours. Code:
    ```python
    cv = CrossValidator(..., numFolds=10, parallelism=1)
    ```
    How do you optimize this using cluster architecture without changing the grid?
    **Answer**: Increase `parallelism` to a higher number (e.g., 4 or 8) and potentially reduce `numFolds` to 3 or 5.
    **Mastery Explanation**: `parallelism=1` forces purely serial execution of folds/params. Utilizing driver parallelism submits concurrent jobs, maximizing executor CPU utilization via Catalyst.

46. **Question**: Find the logic error in handling imbalance:
    ```python
    df_weighted = df.withColumn("weight", F.lit(1.0))
    lr = LogisticRegression(weightCol="weight")
    cv = CrossValidator(estimator=lr, ...)
    ```
    **Answer**: The weight is a constant `1.0` for all rows.
    **Mastery Explanation**: Assigning a static weight to all classes does absolutely nothing to address imbalance. The weight must be inversely proportional to the class frequencies.

47. **Question**: Why does this PySpark code cause an Executor OOM?
    ```python
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    hashingTF = HashingTF(inputCol="words", outputCol="features", numFeatures=2**24)
    ```
    **Answer**: `numFeatures=2**24` creates an incredibly dense/large feature vector if not highly sparse, bloating memory during matrix operations.
    **Mastery Explanation**: A massive feature space (16 million dimensions) requires significant memory per partition. When Tungsten attempts to allocate memory for the BLAS matrix operations, it exceeds execution memory limits.

48. **Question**: A pipeline contains a Custom SQLTransformer.
    ```python
    sqlTrans = SQLTransformer(statement="SELECT * FROM __THIS__ ORDER BY feature_1")
    pipeline = Pipeline(stages=[sqlTrans, lr])
    cv = CrossValidator(estimator=pipeline, ...)
    ```
    Why is this disastrous for Spark performance?
    **Answer**: `ORDER BY` forces a global network shuffle across all partitions.
    **Mastery Explanation**: Sorting the entire dataset via a global shuffle before fitting an ML model destroys partition locality and involves massive disk/network I/O.

49. **Question**: A developer writes:
    ```scala
    val paramGrid = new ParamGridBuilder().addGrid(lr.maxIter, Array(10, 100)).build()
    val cv = new CrossValidator().setEstimator(lr).setEstimatorParamMaps(paramGrid).setParallelism(100)
    ```
    The driver crashes with `java.lang.OutOfMemoryError: Java heap space`. Fix it.
    **Answer**: Reduce `setParallelism(100)` to a much lower number, e.g., `setParallelism(4)`.
    **Mastery Explanation**: 100 concurrent threads in the driver require significant heap space to track Spark jobs, task states, and model metrics. The driver JVM is overwhelmed.

50. **Question**: Debug the TrainValidationSplit logic:
    ```python
    tvs = TrainValidationSplit(estimator=lr, evaluator=evaluator, estimatorParamMaps=grid, trainRatio=0.1)
    ```
    **Answer**: `trainRatio=0.1` means only 10% of the data is used for training, while 90% is used for validation.
    **Mastery Explanation**: This is an inversion of standard ML practices. Models require sufficient data to learn patterns. The ratio should typically be 0.7 or 0.8 to ensure the Estimator learns effectively.
