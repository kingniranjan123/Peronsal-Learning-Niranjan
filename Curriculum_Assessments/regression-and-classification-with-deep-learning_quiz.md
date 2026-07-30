# Master Class Assessment: Regression and Classification with Deep Learning

## Section 1: True/False Questions

**1. Horovod training tasks execute backpropagation directly within the Spark Executor JVM heap to minimize data transfer latency.**
- **Answer:** False
- **Mastery Explanation:** Horovod uses Py4J to fork a Python subprocess inside each executor. The tensors and model weights reside in off-heap native memory or GPU VRAM, invisible to the JVM garbage collector, preventing JVM GC pauses from stalling backpropagation.

**2. The communication cost of Horovod's ring-all-reduce algorithm grows exponentially as the number of GPUs in the cluster increases.**
- **Answer:** False
- **Mastery Explanation:** Ring-all-reduce transmits `2 * (N-1) / N` times the gradient data per rank, making the communication cost nearly constant regardless of cluster size, allowing linear scaling efficiency.

**3. Petastorm bypasses Spark's RDD abstraction completely by reading Parquet files directly from the distributed filesystem using Arrow's native reader.**
- **Answer:** True
- **Mastery Explanation:** Petastorm's `make_batch_reader` directly opens Parquet files via Arrow IPC, avoiding the massive serialization overhead (10-40x slower) associated with mapping over a Spark DataFrame/RDD.

**4. When scaling Horovod from 4 to 32 GPUs, keeping the learning rate constant will typically cause the model to diverge within the first 200 steps.**
- **Answer:** True
- **Mastery Explanation:** The effective batch size scales linearly with the number of ranks. If you multiply the batch size by 8 (from 4 to 32 ranks) without scaling the learning rate linearly (and using a warmup schedule), the gradient updates become unstable.

**5. Setting `spark.sql.files.maxPartitionBytes` to a very small aggressive value helps ensure perfect data distribution across Petastorm shards.**
- **Answer:** False
- **Mastery Explanation:** Aggressively tuning this setting often results in highly unequal Parquet row group counts. Petastorm shards based on row groups, not row counts, so unequal row groups lead to severe data skew and GPU stragglers.

**6. Freezing early layers of a pre-trained model in a Horovod distributed training job reduces both forward-pass FLOPS and network bandwidth consumed by all-reduce.**
- **Answer:** True
- **Mastery Explanation:** Frozen layers generate zero gradients. Horovod inspects the trainable variables and omits frozen layers from the all-reduce communication plan, reducing network bandwidth usage proportionally.

**7. `hvd.callbacks.BroadcastGlobalVariablesCallback(0)` can be omitted if you manually ensure all workers are initialized with the same random seed.**
- **Answer:** False
- **Mastery Explanation:** It is non-negotiable. Even with identical seeds, minor nondeterminism in GPU execution or initialization can lead to slight weight drift. Without broadcasting rank 0's weights, the first all-reduce averages incompatible weight landscapes.

**8. In MLflow's Model Registry, setting `archive_existing_versions=True` during a stage transition is the recommended way to prevent concurrent promotion race conditions.**
- **Answer:** False
- **Mastery Explanation:** Using `archive_existing_versions=True` silently archives ALL non-Staging/Production versions. In concurrent pipelines, it can accidentally archive versions being evaluated in parallel. It must be set to `False` for thread-safe idempotent transitions.

**9. When evaluating a holdout set with hundreds of millions of rows, `tf.keras.metrics.AUC.update_state` should be used to stream metrics without crashing the driver.**
- **Answer:** True
- **Mastery Explanation:** Using `.update_state()` evaluates the dataset batch by batch incrementally, requiring constant memory, rather than materializing the entire dataset in the Spark driver's memory.

**10. Catalyst's Logical Optimization plays a direct role in the backpropagation loop by fusing gradient operations.**
- **Answer:** False
- **Mastery Explanation:** Catalyst and Tungsten play no role in the training loop itself. They are exclusively used for upstream feature engineering and data preparation before Petastorm reads the Parquet shards.

---

## Section 2: Multiple Choice Questions

**11. What is the root cause of GPU utilization oscillating wildly between 95% and 15% during a Horovod training job?**
- A) Network congestion on the MPI/Gloo interface
- B) JVM Garbage Collection pausing the Python subprocess
- C) Unequal Parquet row group sizes causing barrier stragglers
- D) `average_aggregated_gradients=True` causing deadlocks
- **Answer:** C
- **Mastery Explanation:** Petastorm shards by row groups. If one rank receives significantly more data (due to unequal row groups), faster ranks finish their batch early and spin-wait at the Horovod barrier, tanking their GPU utilization.

**12. Which algorithm enables Horovod to achieve 85-95% scaling efficiency on clusters up to 128 GPUs?**
- A) Parameter Server architecture
- B) Asynchronous Gradient Descent
- C) Tree-all-reduce
- D) Ring-all-reduce
- **Answer:** D
- **Mastery Explanation:** Ring-all-reduce ensures each node communicates with its logical neighbors in a ring, making bandwidth consumption virtually independent of the total number of nodes, unlike centralized Parameter Servers.

**13. What is the primary purpose of setting `options={'hdfs_driver': 'libhdfs3'}` and increasing lease timeouts during Petastorm training?**
- A) To bypass Catalyst optimizations for faster IO
- B) To fix `LeaseExpiredException` caused by long epochs holding file handles open
- C) To enable Arrow columnar serialization
- D) To push down predicates into the Parquet footer
- **Answer:** B
- **Mastery Explanation:** Petastorm holds file handles open for the entire epoch. On clusters with short NameNode lease timeouts, this leads to lease expiry. Using `libhdfs3` and increasing the timeout resolves this.

**14. What effect does `average_aggregated_gradients=True` have in Horovod's `DistributedOptimizer`?**
- A) It computes the moving average of gradients across epochs.
- B) It defers the all-reduce until all micro-batches are computed, reducing communication frequency.
- C) It averages the weights rather than the gradients.
- D) It normalizes the inputs across the batch dimension.
- **Answer:** B
- **Mastery Explanation:** It uses gradient accumulation internally. By deferring the all-reduce, it dramatically improves GPU utilization on high-latency networks by transmitting fewer, larger gradient payloads.

**15. How does Tungsten's Whole-Stage Codegen accelerate the upstream feature pipeline before deep learning?**
- A) It compiles Spark DataFrame transformations into a single JIT-compiled Java class, eliminating virtual function calls.
- B) It translates Catalyst logic into TensorFlow graphs.
- C) It generates native C++ code for Gloo network transfers.
- D) It serializes Python UDFs into Arrow memory buffers.
- **Answer:** A
- **Mastery Explanation:** Tungsten fuses multiple physical plan nodes (like Cast, Filter, Project) into a single optimized Java function, avoiding intermediate object allocations and boosting feature engineering throughput.

**16. Why must `experimental_run_tf_function=False` be set in `model.compile` when using Horovod in TF 2.x?**
- A) Because Horovod requires eager execution to compute gradients.
- B) Because TF 2.x graph tracing interferes with Horovod's gradient interception hooks.
- C) Because MLflow cannot log metrics if it is True.
- D) Because tf.function is incompatible with Parquet.
- **Answer:** B
- **Mastery Explanation:** TF 2.x graph tracing can encapsulate the training step in a way that prevents Horovod from correctly injecting its `allreduce` operations via gradient hooks.

**17. What determines the number of Spark tasks emitted by `HorovodRunner`?**
- A) The number of Parquet files in the dataset
- B) The `np` parameter provided to `HorovodRunner(np=N)`
- C) `spark.default.parallelism`
- D) The batch size of the deep learning model
- **Answer:** B
- **Mastery Explanation:** `HorovodRunner` translates the `np` (number of processes/ranks) argument into exactly `N` pinned Spark tasks, one for each Horovod rank.

**18. Why must `training=False` be passed to the model during the MLflow holdout validation gate?**
- A) It disables BatchNormalization's running-mean updates and Dropout.
- B) It stops MLflow from logging the validation run.
- C) It reduces memory footprint by dropping the optimizer state.
- D) It enables distributed inference across multiple executors.
- **Answer:** A
- **Mastery Explanation:** Dropout must be turned off to evaluate full model capacity, and BatchNorm must use its frozen running statistics rather than the batch statistics of the test set, otherwise evaluation metrics are artificially skewed.

**19. How does `make_spark_converter` prepare data for Petastorm?**
- A) It converts DataFrames into PyTorch tensors in driver memory.
- B) It writes the DataFrame to Parquet format with specialized Arrow schema metadata.
- C) It serializes the data into HDF5 format on the local executor disk.
- D) It streams RDD partitions directly into GPU VRAM.
- **Answer:** B
- **Mastery Explanation:** It persists the data to a Parquet cache directory and embeds custom Arrow schema metadata so that Petastorm's reader can perform type-safe, zero-copy deserialization later.

**20. Which of the following best describes Petastorm's `cur_shard` and `shard_count` arguments?**
- A) They dictate how the neural network layers are partitioned across GPUs.
- B) They filter Parquet row groups by index so each rank reads a disjoint slice of the data.
- C) They determine the number of Spark partitions Catalyst will create.
- D) They define the ratio of train vs test split.
- **Answer:** B
- **Mastery Explanation:** Rank 0 reads `row_group % shard_count == 0`, ensuring no data overlap and no need for an expensive cluster-wide shuffle during the training loop.

**21. What happens if rank 0 does NOT save the model at the end of training?**
- A) The MLflow tracking server will crash.
- B) Ranks 1 through N will automatically save their versions instead.
- C) No model artifact is persisted, though metrics may still be logged.
- D) Horovod will trigger a rollback of the weights.
- **Answer:** C
- **Mastery Explanation:** In data-parallel training, all ranks have identical weights at the end due to synchronous all-reduce. Only one rank (usually rank 0) needs to save the artifact. If it doesn't, the weights are lost in ephemeral executor memory.

**22. Why do Catalyst and Tungsten struggle with `regexp_extract` compared to standard mathematical operations?**
- A) Regex is natively unsupported by Spark DataFrames.
- B) Regex strings are too long to fit in Tungsten's 8-byte word format.
- C) It requires calling back into the Scala regex engine, breaking Whole-Stage Codegen fusion.
- D) Catalyst cannot push down string predicates.
- **Answer:** C
- **Mastery Explanation:** Opaque UDFs or complex Scala-native functions like regex cannot be easily compiled into the fused JIT bytecode, forcing a context switch out of the optimized execution path.

**23. What is the impact of compiling a model *before* freezing layers in Horovod?**
- A) None, it works identically.
- B) Horovod will still all-reduce the frozen layers, wasting network bandwidth.
- C) The model will fail to compile.
- D) MLflow will log the parameters incorrectly.
- **Answer:** B
- **Mastery Explanation:** Horovod's `DistributedOptimizer` inspects `model.trainable_variables` during `compile()`. If you freeze layers after compilation, Horovod has already built its communication plan including those layers, wasting bandwidth.

**24. What is the architectural reason for tagging an MLflow model version with `validation_auc`?**
- A) It instructs Spark to cache the model in memory.
- B) It creates an immutable audit trail proving the model passed a quantitative evaluation gate before production.
- C) It acts as a Hyperopt metric for Bayesian optimization.
- D) It allows Petastorm to skip low-quality shards.
- **Answer:** B
- **Mastery Explanation:** For ML governance and SOC 2 compliance, the registry tag acts as cryptographic proof that the exact artifact version achieved the required threshold on a holdout set prior to promotion.

**25. Which compression technique is most effective at reducing Horovod all-reduce bandwidth?**
- A) gzip on the Parquet files
- B) Arrow Dictionary encoding
- C) FP16 or 1-bit quantization of gradients
- D) Snappy compression on the Spark shuffle map outputs
- **Answer:** C
- **Mastery Explanation:** Gradient compression like FP16 or 1-bit quantization shrinks the tensor payloads transmitted during the ring-all-reduce phase, drastically cutting bandwidth by 50-75% without touching data storage layers.

---

## Section 3: "Small Twist" Questions

**26. Scenario:** You scale your cluster from 4 to 16 GPUs. You correctly scale your learning rate by 4x.
**Twist:** You forget to include `hvd.callbacks.LearningRateWarmupCallback`.
**Result:** What happens in the first few epochs?
- **Answer:** The model suffers from severe loss spikes and potential divergence.
- **Mastery Explanation:** A linearly scaled LR is mathematically sound for large batch sizes, but at step 0, the model weights are random. A massive LR update on a random weight landscape destroys the gradients. Warmup gradually scales the LR to prevent this shock.

**27. Scenario:** You have `NUM_RANKS = 8` and `NUM_EPOCHS = 10`. You run `model_df.repartition(100)`.
**Twist:** 100 is not evenly divisible by 8.
**Result:** What happens during training?
- **Answer:** Straggler effect; some ranks get more Parquet row groups than others, causing GPU starvation while fast ranks spin-wait at the barrier.
- **Mastery Explanation:** Petastorm distributes row groups via modulo arithmetic. 100 row groups across 8 ranks means some get 13 and others get 12. The ranks with 12 will finish early and wait, wasting GPU time. It should be repartitioned to exactly a multiple of ranks.

**28. Scenario:** You write an automated promotion script running in a CI pipeline.
**Twist:** You set `archive_existing_versions=True` while two branches run the pipeline concurrently.
**Result:** What is the specific race condition?
- **Answer:** One pipeline's promotion will archive the other pipeline's freshly promoted model, or worse, archive a model still undergoing Staging evaluation.
- **Mastery Explanation:** `archive_existing_versions=True` blindly wipes the slate clean. In concurrent environments, this causes silent overwrites. Iterating over the specific active Production version and archiving it manually with `False` is idempotent and safe.

**29. Scenario:** You are evaluating the holdout set.
**Twist:** You use `preds = model(features, training=True)` by mistake.
**Result:** What happens to the AUC metric?
- **Answer:** It is artificially skewed (usually lower), and does not represent real-world performance.
- **Mastery Explanation:** `training=True` keeps Dropout active (randomly zeroing out features) and forces BatchNorm to use the mini-batch's mean/variance instead of the globally learned population statistics, crippling inference accuracy.

**30. Scenario:** Your cluster has very fast GPUs but a slow, high-latency 10GbE network.
**Twist:** You set `average_aggregated_gradients=False`.
**Result:** What happens to training throughput?
- **Answer:** It drops significantly due to network bottlenecks.
- **Mastery Explanation:** Setting it to `False` forces an all-reduce operation for every single gradient tensor immediately. On high-latency networks, the overhead of constant MPI calls stalls the GPUs. Setting it to `True` enables gradient accumulation, batching the network payloads.

**31. Scenario:** You are doing transfer learning on a massive ResNet model.
**Twist:** You freeze the TOP layers (classification head) and fine-tune the BOTTOM layers (early convolutions).
**Result:** What happens?
- **Answer:** The model overfits immediately and network bandwidth savings vanish.
- **Mastery Explanation:** Bottom layers detect fundamental features (edges, textures) which are highly transferable. Top layers are task-specific. Freezing top layers makes no sense; fine-tuning bottom layers requires computing gradients for the entire massive trunk, negating the FLOPS and Horovod communication savings.

**32. Scenario:** You want equal shard sizes, so you write `df.repartition(64)`.
**Twist:** Adaptive Query Execution (AQE) coalesces shuffle partitions down to 32 before writing to Parquet.
**Result:** How does this impact Horovod with 64 ranks?
- **Answer:** 32 ranks receive no data (empty shards), causing Horovod to crash or deadlock.
- **Mastery Explanation:** AQE automatically coalesces partitions it deems too small. If it drops the partition count below `NUM_RANKS`, some Petastorm shards get 0 row groups, breaking the synchronous ring-all-reduce barrier expectation.

**33. Scenario:** You change `BroadcastGlobalVariablesCallback(0)` to `BroadcastGlobalVariablesCallback(1)`.
**Twist:** Rank 0 is on a slow executor, Rank 1 is on a fast one.
**Result:** Does the training succeed?
- **Answer:** Yes, it succeeds completely normally.
- **Mastery Explanation:** The integer argument is just the root rank that broadcasts its initial weights to the others. Whether Rank 0 or Rank 1 acts as the source of truth for the initial random seed makes no mathematical difference to the training outcome.

**34. Scenario:** You are running Horovod on a Databricks CPU-only cluster.
**Twist:** You use `HorovodRunner(np=8, use_gloo=False)`.
**Result:** What error occurs?
- **Answer:** A crash related to missing NCCL/MPI libraries.
- **Mastery Explanation:** `use_gloo=False` defaults to MPI or NCCL, which require specialized OS-level installations and GPU hardware (for NCCL). Gloo is a pure-Python/C++ network backend that works out-of-the-box on CPU clusters.

**35. Scenario:** You define 5 sequential `withColumn` transformations.
**Twist:** One of them includes a Pandas UDF.
**Result:** How does this affect Catalyst's Whole-Stage Codegen?
- **Answer:** It breaks the projection fusion.
- **Mastery Explanation:** Catalyst will fuse the operations before the UDF, execute the UDF in Python memory (Arrow), and then fuse operations after. This context switch shatters the single JIT-compiled pipeline, drastically lowering rows/sec throughput.

**36. Scenario:** You compute the AUC by collecting all predictions to driver memory.
**Twist:** The holdout set has 200 million rows.
**Result:** What happens to the Spark job?
- **Answer:** The Spark Driver crashes with a JVM OutOfMemoryError (OOM).
- **Mastery Explanation:** 200M floats = ~800MB, but Python object overhead balloons this to several GBs, instantly breaching the typical driver heap size. Streaming metrics via `update_state` is mandatory at scale.

**37. Scenario:** You initialize Petastorm reader with `cur_shard=hvd.rank()` but omit `shard_count`.
**Twist:** By default, what data does Rank 0 see?
**Result:** Rank 0 sees the entire dataset.
- **Answer:** Every rank trains on the entire dataset.
- **Mastery Explanation:** Without `shard_count`, Petastorm does not partition the row groups. If all N ranks see all data, the effective training dataset size is multiplied by N, causing severe overfitting and duplicating effort.

**38. Scenario:** You are training for 1 epoch on 8 GPUs.
**Twist:** You `repartition(8)` but one executor has a slightly larger Parquet block size configuration.
**Result:** Are the row groups still balanced?
- **Answer:** No, the larger block size might merge rows into fewer row groups for that partition.
- **Mastery Explanation:** Petastorm shards by *row groups*. If Parquet block size varies, identical row counts yield different row group counts, re-introducing the straggler problem. Partitions and block sizes must be tightly controlled.

**39. Scenario:** You configure `LearningRateWarmupCallback`.
**Twist:** You set `warmup_epochs=0.1` (a fraction of an epoch).
**Result:** Is this valid?
- **Answer:** Yes, Horovod supports step-based fractional warmup.
- **Mastery Explanation:** In massive datasets, 5 full epochs of warmup is too long (the model might converge in 2 epochs). Warmup is calculated per-step under the hood, so fractional epochs perfectly warm up the LR over the first few thousand batches.

**40. Scenario:** You use PyTorch instead of TensorFlow.
**Twist:** You look for the equivalent of `experimental_run_tf_function=False`.
**Result:** What is the PyTorch equivalent?
- **Answer:** PyTorch doesn't need one; it operates in eager mode by default.
- **Mastery Explanation:** PyTorch intercepts gradients natively during the `loss.backward()` autograd pass using hooks (`register_hook`). There is no static graph compilation step that hides operations from Horovod like TF's `tf.function`.

---

## Section 4: Coding & Debugging Questions

**41. Bug Hunt:** A data scientist complains that their distributed model's loss plateaus at a much higher value than their single-GPU model. Reviewing their code, you see `opt = hvd.DistributedOptimizer(Adam(lr=0.001))`, but they forgot the broadcast callback. Explain the exact mathematical disaster occurring.
- **Mastery Explanation:** Without `BroadcastGlobalVariablesCallback`, Rank 0 and Rank 1 initialize with different random weights. At step 1, Horovod computes gradients on two completely different loss landscapes, averages them, and applies them. This is equivalent to taking a step in a random direction, destroying the neural network's architecture instantly. It never recovers.

**42. Bug Hunt:** You read a pipeline code block:
```python
df.repartition(hvd.size()).write.parquet(URL)
```
The job takes 10 epochs. At epoch 2, GPU utilization drops. Identify the issue.
- **Mastery Explanation:** Repartitioning to exactly `hvd.size()` means 8 partitions. However, Parquet writes row groups of a fixed size. If partition 0 is 1MB larger than partition 1, it might spawn a 2nd row group. Rank 0 now has 2 row groups, Rank 1 has 1. Rank 0 takes twice as long to process its epoch. The fix is `repartition(hvd.size() * num_epochs)` to create so many row groups that the statistical variance averages out.

**43. Bug Hunt:** A Spark cluster is dying with `java.lang.OutOfMemoryError: GC overhead limit exceeded` on the Executors. The code uses:
```python
rdd = df.rdd.map(lambda row: tf.convert_to_tensor(row))
```
How do you refactor this?
- **Mastery Explanation:** `df.rdd.map` forces Catalyst to serialize internal UnsafeRows into Python objects (Pickle), creating massive garbage collection pressure on the JVM. Refactor to write the DataFrame to Parquet, and use Petastorm's `make_batch_reader` to read directly from native Arrow memory into TF tensors, bypassing the JVM heap entirely.

**44. Bug Hunt:** A 10-hour training job fails at hour 9 with `org.apache.hadoop.hdfs.server.namenode.LeaseExpiredException`. The cluster admin refuses to increase the HDFS lease timeout. Provide a code-level workaround.
- **Mastery Explanation:** Petastorm holds the Parquet file reader open indefinitely. If the epoch takes longer than the HDFS lease (often 10 mins), HDFS revokes access. The workaround is to wrap the training loop to close and recreate the Petastorm `make_batch_reader` periodically (e.g., every 5000 steps), renewing the filesystem lease.

**45. Bug Hunt:** `loss: 0.693 -> 0.450 -> 12.5 -> NaN`. Code snippet:
```python
scaled_lr = BASE_LR * hvd.size()
opt = hvd.DistributedOptimizer(Adam(scaled_lr))
# (Callbacks only include ModelCheckpoint)
```
Fix the bug.
- **Mastery Explanation:** The linear LR scaling is correct, but without `hvd.callbacks.LearningRateWarmupCallback`, the first batch applies a massive gradient update (scaled by `N`) to random initial weights, causing the gradients to explode (`NaN`). Add the warmup callback to gently ramp the LR.

**46. Bug Hunt:** The network architecture has a 50ms latency between nodes. GPU utilization is 30%. The optimizer is:
```python
opt = hvd.DistributedOptimizer(SGD(0.01), average_aggregated_gradients=False)
```
Fix the bottleneck.
- **Mastery Explanation:** High network latency cripples synchronous all-reduce if it transmits many small tensors. Change to `average_aggregated_gradients=True`. This allows Horovod to accumulate gradients and fuse small tensor communications into a single large MPI payload, hiding the network latency and allowing the GPU to keep computing.

**47. Bug Hunt:** A user wants to freeze layers, so they write:
```python
model.compile(optimizer=hvd.DistributedOptimizer(Adam()))
for layer in model.layers[:5]: layer.trainable = False
```
Why does training network bandwidth not decrease?
- **Mastery Explanation:** Horovod builds its MPI all-reduce execution graph during `model.compile()` based on `trainable_variables`. Mutating `trainable=False` AFTER compilation does not update Horovod's graph. The optimizer still transmits the frozen layers (which just contain zeros). The fix is to freeze the layers BEFORE calling `compile()`.

**48. Bug Hunt:** The MLflow UI shows 8 identical runs for a single training job, cluttering the Model Registry.
- **Mastery Explanation:** The user placed `mlflow.start_run()` and `mlflow.tensorflow.log_model()` in the main training loop without checking the rank. Because Horovod runs 8 parallel processes, all 8 execute the MLflow code. Wrap the MLflow logic in `if hvd.rank() == 0:` so only the master rank talks to the tracking server.

**49. Bug Hunt:** You review the promotion pipeline:
```python
current = client.get_latest_versions(model, stages=["Production"])[0]
client.transition_model_version_stage(model, new_version, "Production")
client.transition_model_version_stage(model, current.version, "Archived")
```
Identify the race condition window.
- **Mastery Explanation:** The code promotes the new model BEFORE archiving the old one. For the milliseconds/seconds between lines 2 and 3, TWO models are tagged as "Production". Downstream serving systems polling the registry might pull both or error out. Always Archive the old production version FIRST, then Promote the new one.

**50. Bug Hunt:** 
```python
with make_batch_reader(URL, cur_shard=hvd.rank()) as reader:
    dataset = make_petastorm_dataset(reader)
```
Rank 0 crashes with "IndexError: out of bounds". `URL` has only 2 Parquet row groups, but `hvd.size()` is 4.
- **Mastery Explanation:** There are fewer row groups (2) than Horovod ranks (4). Ranks 2 and 3 receive absolutely zero data. When Petastorm tries to read, it faults. The fix is ensuring the Spark write job produces at least `hvd.size() * epochs` row groups using `repartition()`.
