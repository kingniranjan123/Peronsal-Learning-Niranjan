# Random Forests Assessment

## Part 1: True/False Questions

**Q1**: In Spark MLlib's Random Forest, `maxMemoryInMB` dictates the memory for aggregating statistics.
- True
- False
**Correct Answer:** True
*Mastery Explanation: `maxMemoryInMB` limits the memory used per node during binning aggregations.*

**Q2**: Increasing `maxBins` will always reduce the memory footprint on the executors.
- True
- False
**Correct Answer:** False
*Mastery Explanation: Increasing `maxBins` increases memory footprint since more bin thresholds are tracked.*

**Q3**: Spark MLlib supports random forests for both classification and regression.
- True
- False
**Correct Answer:** True
*Mastery Explanation: `RandomForestClassifier` and `RandomForestRegressor` are both provided in Spark MLlib.*

**Q4**: Decision trees in a Spark Random Forest are trained sequentially.
- True
- False
**Correct Answer:** False
*Mastery Explanation: Spark trains multiple trees in parallel to optimize cluster usage.*

**Q5**: Checkpointing is unnecessary for deep random forests in Spark.
- True
- False
**Correct Answer:** False
*Mastery Explanation: Deep trees can cause long lineages leading to StackOverflows; checkpointing prevents this.*

**Q6**: Feature subsampling strategy 'sqrt' is typically used for regression tasks by default.
- True
- False
**Correct Answer:** False
*Mastery Explanation: 'onethird' is typical for regression, while 'sqrt' is typical for classification.*

**Q7**: Tungsten's memory management has no impact on Random Forest training speed.
- True
- False
**Correct Answer:** False
*Mastery Explanation: Tungsten's optimized memory format reduces GC overhead, speeding up node aggregations.*

**Q8**: Setting `subsamplingRate` to 1.0 removes bootstrap sampling completely.
- True
- False
**Correct Answer:** False
*Mastery Explanation: Even with 1.0, bootstrap sampling (sampling with replacement) means each tree gets a random sample, some duplicates and some omitted.*

**Q9**: Random Forests inherently handle missing values in Spark MLlib without imputation.
- True
- False
**Correct Answer:** False
*Mastery Explanation: Spark MLlib's Random Forest does not handle missing values automatically; imputation or removal is required beforehand.*

**Q10**: Spark's Random Forest uses the Gini impurity by default for classification.
- True
- False
**Correct Answer:** True
*Mastery Explanation: Gini impurity is the default measure for node splits in classification tasks.*

## Part 2: Multiple Choice Questions

**Q11**: What is the primary cause of OOM errors during the `treeAggregate` phase?
- A) High `maxDepth`
- B) High `numTrees`
- C) Large number of features and `maxBins`
- D) Low `maxMemoryInMB`
**Correct Answer:** C
*Mastery Explanation: More features and bins require exponentially larger statistic arrays in memory.*

**Q12**: Which parameter reduces the correlation between individual trees?
- A) maxDepth
- B) featureSubsetStrategy
- C) maxBins
- D) minInstancesPerNode
**Correct Answer:** B
*Mastery Explanation: `featureSubsetStrategy` randomly samples features at each split, decorrelating trees.*

**Q13**: How does Spark handle continuous features for Random Forests?
- A) Exact splits
- B) Histogram-based binning
- C) K-Means clustering
- D) Standard scaling
**Correct Answer:** B
*Mastery Explanation: Spark bins continuous features to enable distributed finding of optimal splits without moving all data.*

**Q14**: What is a sign of overfitting in a Random Forest?
- A) High training error, high validation error
- B) Low training error, high validation error
- C) High training error, low validation error
- D) Low training error, low validation error
**Correct Answer:** B
*Mastery Explanation: Overfitting occurs when the model memorizes the training data but fails to generalize.*

**Q15**: Which parameter controls the maximum depth of each tree?
- A) maxBins
- B) numTrees
- C) maxDepth
- D) minInfoGain
**Correct Answer:** C
*Mastery Explanation: `maxDepth` stops tree growth when a certain depth is reached.*

**Q16**: What does `minInstancesPerNode` do?
- A) Limits the number of trees
- B) Requires a minimum number of samples to create a split
- C) Allocates memory per node
- D) Controls the number of classes
**Correct Answer:** B
*Mastery Explanation: It prevents creating leaf nodes with too few samples, acting as regularization.*

**Q17**: How do Random Forests make final predictions for classification?
- A) Average of probabilities
- B) Majority voting
- C) Median
- D) Minimum
**Correct Answer:** B
*Mastery Explanation: For classification, it uses majority voting among the trees.*

**Q18**: Which is not an impurity measure supported by Spark Random Forests?
- A) Gini
- B) Entropy
- C) Variance
- D) Hinge loss
**Correct Answer:** D
*Mastery Explanation: Gini and Entropy are for classification, Variance for regression. Hinge loss is not used here.*

**Q19**: Why is caching the training dataset recommended before Random Forest training?
- A) Reduces GC overhead
- B) Avoids recomputing the data during iterative training passes
- C) Increases tree depth
- D) Reduces network shuffle
**Correct Answer:** B
*Mastery Explanation: The algorithm makes multiple passes over the data; caching avoids costly re-evaluations.*

**Q20**: What happens if `maxBins` is smaller than the number of distinct values in a continuous feature?
- A) Spark throws an error
- B) The feature is binned into `maxBins` intervals
- C) The feature is ignored
- D) Spark increases `maxBins` automatically
**Correct Answer:** B
*Mastery Explanation: Spark uses approximate quantiles to group the continuous feature into `maxBins` bins.*

**Q21**: What is the default `numTrees` in Spark MLlib's Random Forest?
- A) 10
- B) 20
- C) 50
- D) 100
**Correct Answer:** B
*Mastery Explanation: The default number of trees is 20 in PySpark/Spark MLlib.*

**Q22**: How does increasing `numTrees` affect variance?
- A) Increases variance
- B) Decreases variance
- C) No effect
- D) Becomes zero
**Correct Answer:** B
*Mastery Explanation: More trees in the ensemble smooth out predictions, reducing overall variance.*

**Q23**: Which physical execution phase dominates Random Forest training time?
- A) Data loading
- B) Shuffle operations during tree aggregation
- C) Model serialization
- D) DataFrame transformation
**Correct Answer:** B
*Mastery Explanation: Distributing and aggregating split statistics across nodes requires intensive network shuffles.*

**Q24**: What is the role of the Driver in Spark Random Forest training?
- A) Training all trees locally
- B) Coordinating tree growth and aggregating statistics
- C) Executing the mapping functions
- D) Storing the entire dataset
**Correct Answer:** B
*Mastery Explanation: The Driver dictates the next nodes to split and orchestrates the aggregation passes.*

**Q25**: Can Spark MLlib Random Forests handle categorical features natively?
- A) Yes, without any preprocessing
- B) Yes, but they must be indexed (e.g., using StringIndexer)
- C) No, only continuous features
- D) No, only binary features
**Correct Answer:** B
*Mastery Explanation: Categorical features must be encoded as indices before feeding to the estimator.*

## Part 3: Small Twist Questions

**Q26**: Twist: You set `featureSubsetStrategy` to 'all'. How does this impact training?
- A) Faster training, higher variance
- B) Slower training, higher variance
- C) Faster training, lower variance
- D) Slower training, lower variance
**Correct Answer:** B
*Mastery Explanation: Using all features removes decorrelation (increasing variance) and computes more splits per node (slower).*

**Q27**: Twist: You increase `maxMemoryInMB` from 256 to 2048. What happens if Executor memory is only 2GB?
- A) Faster training
- B) OOM Error
- C) Spark automatically ignores it
- D) Data is dropped
**Correct Answer:** B
*Mastery Explanation: Requesting more memory for aggregations than available on the executor will trigger OOMs.*

**Q28**: Twist: You switch impurity from 'gini' to 'entropy'. Does it significantly change accuracy?
- A) Yes, drastically
- B) No, performance is usually similar, but entropy is slightly slower to compute
- C) Yes, entropy always wins
- D) No, gini is slower
**Correct Answer:** B
*Mastery Explanation: Entropy requires logarithmic calculations, making it slower, but generally yields very similar trees to Gini.*

**Q29**: Twist: You have highly imbalanced data and use Random Forest. Does setting `subsamplingRate` to 0.1 help?
- A) Yes, it balances classes
- B) No, it just uses less data, preserving the imbalance
- C) Yes, it oversamples the minority
- D) No, it crashes
**Correct Answer:** B
*Mastery Explanation: Subsampling samples uniformly, so the class distribution remains imbalanced.*

**Q30**: Twist: You cache your dataset, but it spills to disk. How does this affect RF training?
- A) No effect
- B) Training slows down significantly due to disk I/O on every pass
- C) Spark crashes
- D) Trees become shallower
**Correct Answer:** B
*Mastery Explanation: RF makes multiple passes. Disk spilling causes slow reads during every single pass.*

**Q31**: Twist: You decrease `maxBins` to a very small number (e.g., 2) for continuous data. Effect?
- A) Extreme overfitting
- B) High bias (underfitting) as splits become too coarse
- C) OOM Error
- D) Longer training time
**Correct Answer:** B
*Mastery Explanation: Very few bins mean continuous features are heavily discretized, missing subtle patterns and underfitting.*

**Q32**: Twist: You set `maxDepth` to 30. What is the most likely failure point?
- A) StackOverflow exception on Driver due to long lineage
- B) CPU bottleneck
- C) Disk full
- D) Spark ignores depth > 10
**Correct Answer:** A
*Mastery Explanation: Deep trees create massive lineage DAGs in Spark, often exceeding JVM stack limits without checkpointing.*

**Q33**: Twist: You use `checkpointInterval = 5`. What happens every 5 levels of depth?
- A) Data is written to HDFS to truncate lineage
- B) Model is saved to disk
- C) Executors restart
- D) Garbage collection triggers
**Correct Answer:** A
*Mastery Explanation: Checkpointing writes the current state to a reliable file system to truncate the RDD lineage.*

**Q34**: Twist: You set `minInfoGain` to a high value (e.g., 0.5). What happens?
- A) Deeper trees
- B) Shallower trees or early stopping
- C) OOM Error
- D) Faster predictions
**Correct Answer:** B
*Mastery Explanation: A split is only made if information gain exceeds the threshold. High thresholds prevent splitting.*

**Q35**: Twist: You use a cluster with 1000 cores but only 10 trees. What is the cluster utilization?
- A) 100%
- B) Very low, only 10 cores can be used effectively for tree root splits
- C) 50%
- D) 0%
**Correct Answer:** B
*Mastery Explanation: Spark parallelizes across nodes/features, but at the root level with only 10 trees, parallelism is limited.*

**Q36**: Twist: You change from Classification to Regression with Random Forests. What is the default impurity?
- A) Variance
- B) Gini
- C) Entropy
- D) MSE
**Correct Answer:** A
*Mastery Explanation: For regression, variance reduction is used as the impurity metric.*

**Q37**: Twist: You set `seed` to a fixed integer. What happens across multiple runs?
- A) Different models every time
- B) Identical models every time (deterministic)
- C) Slightly varying models
- D) Faster training
**Correct Answer:** B
*Mastery Explanation: Fixing the random seed ensures the bootstrap sampling and feature selection are deterministic.*

**Q38**: Twist: The dataset has a categorical feature with 10,000 categories. Can you use `maxBins=32`?
- A) Yes
- B) No, maxBins must be >= number of categories for categorical features
- C) Yes, but categories are grouped
- D) No, Spark crashes
**Correct Answer:** B
*Mastery Explanation: Spark requires `maxBins` to be at least the maximum categorical cardinality.*

**Q39**: Twist: You have 10 features, and set `featureSubsetStrategy` to 'sqrt'. How many features per split?
- A) 1
- B) 3
- C) 5
- D) 10
**Correct Answer:** B
*Mastery Explanation: sqrt(10) is ~3.16, which rounds down/up depending on implementation, typically 3.*

**Q40**: Twist: You run a Random Forest on a single-node local[1] cluster. Does it still perform shuffle?
- A) Yes, logic remains the same
- B) No, shuffle is bypassed
- C) Spark crashes
- D) Runs in driver memory only
**Correct Answer:** A
*Mastery Explanation: Even on local mode, Spark's physical plan includes shuffle exchanges, though they occur via local file system.*

## Part 4: Coding & Debugging Questions

**Q41**: Debug: You see high GC pauses on executors during `treeAggregate`. Fix?
- A) Increase `spark.executor.memory`
- B) Decrease `maxBins`
- C) Decrease `maxMemoryInMB`
- D) All of the above
**Correct Answer:** D
*Mastery Explanation: More memory, fewer bins (smaller arrays), or smaller memory chunks (more passes) all reduce heap pressure.*

**Q42**: Debug: Checkpointing is enabled, but the job fails with "Checkpoint directory not set". Fix?
- A) `sc.setCheckpointDir("hdfs://path")`
- B) `spark.conf.set("checkpoint", "true")`
- C) Add more disk space
- D) Use `cache()` instead
**Correct Answer:** A
*Mastery Explanation: Spark requires an explicitly set checkpoint directory via the SparkContext before training deep trees.*

**Q43**: Code: How do you extract feature importances from a trained Random Forest model in PySpark?
- A) `model.importances`
- B) `model.featureImportances`
- C) `model.coefficients`
- D) `model.weights`
**Correct Answer:** B
*Mastery Explanation: The property is named `featureImportances` and returns a sparse vector of importance scores.*

**Q44**: Debug: Random Forest predictions are taking too long on a stream. Fix?
- A) Increase maxDepth
- B) Convert trees to IF-ELSE code generation or use lower maxDepth
- C) Increase numTrees
- D) Cache the stream
**Correct Answer:** B
*Mastery Explanation: Deep/many trees take long to traverse. Shallower trees or compiled logic speeds up inference.*

**Q45**: Code: Which class is used to wrap multiple feature columns into a single vector column for RF?
- A) VectorAssembler
- B) FeatureHasher
- C) StringIndexer
- D) ColumnMerger
**Correct Answer:** A
*Mastery Explanation: Spark ML estimators require all features in a single Vector column, prepared using VectorAssembler.*

**Q46**: Debug: Job hangs infinitely during the first action after RF initialization. Cause?
- A) Data skew in transformations prior to RF
- B) Too many trees
- C) Low memory
- D) Wrong impurity
**Correct Answer:** A
*Mastery Explanation: Spark evaluates transformations lazily. Hanging at the first action points to skewed or highly complex upstream transformations.*

**Q47**: Code: You want to tune `maxDepth` over [5, 10, 15]. What tool should you use?
- A) CrossValidator with ParamGridBuilder
- B) manual for loop
- C) Pipeline
- D) Evaluator
**Correct Answer:** A
*Mastery Explanation: `CrossValidator` combined with `ParamGridBuilder` is the standard Spark ML tuning mechanism.*

**Q48**: Debug: Executor lost error (Exit code 137). What is the primary suspect in RF training?
- A) Python UDF failure
- B) YARN killing the container due to exceeding memory limits (OOM)
- C) Network timeout
- D) Disk failure
**Correct Answer:** B
*Mastery Explanation: Exit code 137 means killed by OS/YARN, almost always due to using more memory than allocated, typical in heavy RF aggregations.*

**Q49**: Code: To ensure reproducibility when splitting data before training an RF, what must be done?
- A) Cache both sets
- B) Provide a `seed` to `randomSplit`
- C) Sort the data
- D) Repartition
**Correct Answer:** B
*Mastery Explanation: Using a deterministic seed in `randomSplit` ensures the train/test sets are identical across runs.*

**Q50**: Debug: After training, the model size is 5GB. Broadcasting it to executors for inference fails. Fix?
- A) Increase `spark.sql.broadcastTimeout` and `spark.driver.maxResultSize`
- B) Decrease maxBins
- C) Use mapPartitions
- D) A and C
**Correct Answer:** A
*Mastery Explanation: Massive models exceed default broadcast timeouts and result limits. Increasing these configs allows the broadcast to succeed.*
