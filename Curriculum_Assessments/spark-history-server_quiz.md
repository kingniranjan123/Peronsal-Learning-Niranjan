# Spark History Server - Elite Assessment

## Part 1: True/False Questions

1. **Question:** By default, the SHS keeps application states entirely in memory using an `InMemoryStore`, which can lead to high JVM heap usage for large numbers of applications.
   **Answer:** True
   **Mastery Explanation:** Without setting `spark.history.store.path`, the default backend is `InMemoryStore` backed by `ConcurrentHashMap`. This stores the full task metrics index in the JVM heap (~20MB per app), leading to OOMs at scale.

2. **Question:** `spark.eventLog.rolling.enabled=true` enables the SHS to avoid loading a massive 50GB monolithic event log into memory at once during replay.
   **Answer:** True
   **Mastery Explanation:** Rolling logs partition the event stream into chunks (default 128MB). This ensures the replay thread only holds one chunk in memory at a time, preventing heap exhaustion when parsing logs from long-running streaming jobs.

3. **Question:** If an application crashes before completion, the `.inprogress` file is automatically deleted by the EventLoggingListener.
   **Answer:** False
   **Mastery Explanation:** The driver crash prevents the atomic rename from occurring, but the file remains on disk with the `.inprogress` suffix. The SHS can still parse it up to the point of the crash.

4. **Question:** The `FsHistoryProvider` uses the exact same `AppStatusListener` class as the live Driver UI to rebuild the application state.
   **Answer:** True
   **Mastery Explanation:** Both the live `LiveListenerBus` and the SHS replay thread feed `SparkListenerEvent`s into `AppStatusListener`. This code sharing guarantees that the historical UI matches the live UI perfectly.

5. **Question:** Enabling `spark.history.fs.inProgressOptimization.enabled=true` prevents the SHS from polling `.inprogress` files altogether to save NFS bandwidth.
   **Answer:** False
   **Mastery Explanation:** It doesn't skip polling entirely; instead, it stores the last-read offset in the KVStore and seeks directly to that position on the next poll, reading only the delta rather than replaying from byte 0.

6. **Question:** To list S3 event logs reliably at scale, `spark.hadoop.fs.s3a.list.version=2` must be used because the v1 API returns stale listings during concurrent PUT/DELETE operations.
   **Answer:** True
   **Mastery Explanation:** The S3 v1 list API is notoriously eventually consistent for overwrites/deletes. V2 ensures strong consistency and supports efficient pagination, which is critical for the SHS polling thread.

7. **Question:** The LevelDB KVStore backend persists the full task metrics data in the SHS heap to ensure fast REST API responses.
   **Answer:** False
   **Mastery Explanation:** LevelDB caches only the *index* in the heap. The actual task metric data is serialized via Kryo and stored on disk, reducing SHS heap usage drastically (e.g., to < 512MB for thousands of apps).

8. **Question:** The `EventLoggingListener` operates synchronously on the Spark driver's main execution thread, adding significant latency to task dispatching.
   **Answer:** False
   **Mastery Explanation:** The listener bus operates asynchronously on a dedicated event dispatch thread. Writes are buffered, so I/O latency does not block the scheduler.

9. **Question:** The SHS REST API evaluates queries by re-reading the raw JSON event logs on each request to ensure up-to-date responses.
   **Answer:** False
   **Mastery Explanation:** The REST API queries the in-process `KVStore` (InMemory or LevelDB). This makes lookups O(1) or O(log n), rather than incurring the O(e) cost of re-parsing JSON strings.

10. **Question:** `SparkListenerTaskEnd` events contain the most granular metrics, such as Executor CPU Time and Shuffle Write Bytes, which are later rolled up to the stage level by the SHS.
    **Answer:** True
    **Mastery Explanation:** `Task Metrics` are attached to the `SparkListenerTaskEnd` event. `AppStatusListener` processes these events to increment the stage-level accumulators stored in the KVStore.

## Part 2: Multiple Choice Questions

11. **Question:** Which event is responsible for flushing the final codec stream and triggering the atomic rename of the `.inprogress` file?
    - A) `SparkListenerApplicationStart`
    - B) `SparkListenerApplicationEnd`
    - C) The `FsHistoryProvider` polling loop
    - D) The `EventLoggingListener` on application completion
    **Answer:** D
    **Mastery Explanation:** The `EventLoggingListener` detects application stop, flushes the compression codec, closes the stream, and executes the filesystem rename operation.

12. **Question:** Why might the History Server silently show zero applications when backed by S3, even though logs exist?
    - A) The LevelDB store path is full.
    - B) The IAM role lacks `s3:ListBucket` permissions, which `listStatus` swallows and returns an empty array.
    - C) `spark.eventLog.rolling.enabled` is false.
    - D) Zstd compression is not supported on S3.
    **Answer:** B
    **Mastery Explanation:** Due to legacy Hadoop FS semantics, a lack of list permissions on S3A returns an empty array rather than throwing an AccessDeniedException, leading to a silent failure.

13. **Question:** What is the primary benefit of setting `spark.history.store.path` to a local NVMe drive?
    - A) It allows Spark to compress event logs directly to NVMe.
    - B) It switches the KVStore backend to LevelDB, significantly reducing resident heap usage.
    - C) It speeds up the S3 `listStatus` RPC calls.
    - D) It bypasses the JSON parsing overhead entirely.
    **Answer:** B
    **Mastery Explanation:** By enabling LevelDB on fast local storage, the SHS avoids keeping all application state in the JVM heap, preventing OOM errors and allowing thousands of applications to be retained.

14. **Question:** Which compression codec is recommended for S3-backed event logs to minimize network transfer costs without bottlenecking decompression?
    - A) Snappy
    - B) LZ4
    - C) Zstd
    - D) GZIP
    **Answer:** C
    **Mastery Explanation:** Zstd provides ~40% better compression than LZ4 while maintaining very fast decompression speeds. For S3, network bandwidth is the bottleneck, so smaller files improve overall replay speed.

15. **Question:** When using NFS for event logs, what issue can occur if `spark.history.fs.inProgressOptimization.enabled=true` is used with default NFS attribute caching (`actimeo=60`)?
    - A) The History Server crashes with an `EOFException`.
    - B) The SHS reads the file from byte 0 anyway.
    - C) The SHS may not see newly appended bytes because the file size attribute is cached, delaying updates.
    - D) The NFS server drops the connection.
    **Answer:** C
    **Mastery Explanation:** The optimization relies on checking `st_size` to seek to the end. If NFS caches `st_size` for 60 seconds, the SHS thinks the file hasn't grown and skips reading new events. `acregmax=5` fixes this.

16. **Question:** How does the LevelDB backend handle application eviction when `spark.history.retainedApplications` is reached?
    - A) It deletes the underlying LevelDB files from disk permanently.
    - B) It unloads the application from memory, but LevelDB files remain on disk and reload on demand.
    - C) It moves the application to a secondary S3 bucket.
    - D) It throws an `EvictionException` when the UI is accessed.
    **Answer:** B
    **Mastery Explanation:** Eviction from the active cache simply unloads it from heap. If a user clicks an evicted app, the SHS transparently re-mounts the LevelDB files from disk, providing a seamless experience.

17. **Question:** What does `spark.ui.retainedTasks` control in the History Server?
    - A) The maximum number of tasks Spark can run concurrently.
    - B) The number of tasks per stage kept in the KVStore, preventing unbounded memory/disk growth.
    - C) The maximum number of applications retained in memory.
    - D) The total number of events read per SHS poll cycle.
    **Answer:** B
    **Mastery Explanation:** It limits the KVStore footprint per stage. For stages with millions of tasks, it drops older tasks from the index to prevent the database from bloating out of control.

18. **Question:** When querying the SHS REST API for an application's stages, what is the time complexity of the lookup?
    - A) O(n) where n is the size of the event log.
    - B) O(log n + k) for indexed lookups since it queries the LevelDB KVStore.
    - C) O(n) where n is the number of total applications in S3.
    - D) O(e) where e is the number of events.
    **Answer:** B
    **Mastery Explanation:** The REST API accesses the KVStore. With LevelDB (LSM tree), fetching a range of stages for a given job/app is O(log n + k), far faster than parsing the raw JSON event log (O(e)).

19. **Question:** Which internal class is responsible for scanning the event log directory and submitting logs to the replay thread pool?
    - A) `EventLoggingListener`
    - B) `FsHistoryProvider`
    - C) `AppStatusListener`
    - D) `ElementTrackingStore`
    **Answer:** B
    **Mastery Explanation:** `FsHistoryProvider` polls the storage backend, tracks which files are new or modified, and delegates the actual JSON parsing to a thread pool.

20. **Question:** In the raw JSON event log, what discriminator field is used to identify the type of event on each line?
    - A) `"EventType"`
    - B) `"Event"`
    - C) `"Type"`
    - D) `"SparkEvent"`
    **Answer:** B
    **Mastery Explanation:** Each JSON line contains a top-level `"Event"` key (e.g., `"Event": "org.apache.spark.scheduler.SparkListenerTaskEnd"`) which the parser uses to route the payload.

21. **Question:** What is the time complexity of a rolling log file rotation by the driver?
    - A) O(n) where n is the file size.
    - B) O(log n)
    - C) O(1)
    - D) O(e) where e is the event count.
    **Answer:** C
    **Mastery Explanation:** Rotating a file involves simply closing the current output stream, renaming it (atomic O(1)), and opening a new file descriptor.

22. **Question:** Which specific problem is solved by using `spark.eventLog.rolling.enabled=true`?
    - A) SHS UI memory leaks from Jetty.
    - B) Network throttling when writing logs to S3.
    - C) Enormous single-file event logs from streaming jobs that are slow to replay and exhaust SHS heap.
    - D) Spark driver out-of-memory errors.
    **Answer:** C
    **Mastery Explanation:** Without rolling logs, a 24/7 streaming job creates a massive monolithic file. Rolling limits file size, allowing SHS to parse chunks incrementally without ballooning memory.

23. **Question:** If a query to the SHS REST API `/api/v1/applications` causes Jetty worker thread starvation, what is the most likely cause?
    - A) The SHS is blocked scanning S3.
    - B) Missing the `limit` parameter, causing it to serialize tens of thousands of apps into a massive JSON response.
    - C) The LevelDB database is corrupted.
    - D) The Zstd decompressor is stuck in an infinite loop.
    **Answer:** B
    **Mastery Explanation:** Returning all applications in a single payload (e.g. 50MB+) locks up a Jetty thread for an extended period. With high concurrency, this starves the thread pool. Pagination is mandatory.

24. **Question:** How can you parse the event log directly in Python to extract stage-level shuffle metrics without running an SHS instance?
    - A) By parsing the LevelDB binary format using a Kryo decoder.
    - B) By identifying `SparkListenerTaskEnd` events and summing the `Task Metrics` accumulators.
    - C) By decoding the Snappy blocks using `spark-shell` internal APIs.
    - D) By parsing the `.inprogress` binary footer.
    **Answer:** B
    **Mastery Explanation:** The raw JSON stream contains the ground-truth data. Summing the `Task Metrics` block inside every `SparkListenerTaskEnd` perfectly reconstructs the UI's stage-level aggregations.

25. **Question:** What happens internally when the History Server parses a `SparkListenerEvent` from a log?
    - A) It caches the JSON string in memory for the UI to display.
    - B) It fires the event through `AppStatusListener` to mutate the in-process `KVStore`.
    - C) It immediately updates the event log file on S3.
    - D) It triggers a garbage collection on the Driver.
    **Answer:** B
    **Mastery Explanation:** The SHS does not store raw JSON. It feeds the parsed object into the state machine (`AppStatusListener`), which updates counters and metadata inside the KVStore database.

## Part 3: "Small Twist" Scenario Questions

26. **Scenario:** You configured `spark.history.store.path=/mnt/nvme` but forgot to set `spark.eventLog.rolling.enabled=true` for a 3-month streaming job. The event log reaches 80GB. What happens when SHS replays it?
    - **Answer:** The SHS replay thread will take hours to replay the single massive file, or run out of memory, because it cannot parallelize the reading of a single continuous file.
    - **Mastery Explanation:** Rolling logs allow parallel replay and bounded memory per file. A single monolithic file forces a single thread to decompress and parse 80GB sequentially.

27. **Scenario:** You use S3 for event logs and set `spark.hadoop.fs.s3a.list.version=1`. Concurrent Spark jobs are constantly writing and completing. SHS users complain some completed apps don't appear for hours. Why?
    - **Answer:** The S3 v1 listing API is eventually consistent for overwrites and deletes.
    - **Mastery Explanation:** When `PUT` (rename) and `DELETE` operations race with `LIST` on the same prefix, v1 can return stale directories. Version 2 is strongly consistent and required for accurate polling.

28. **Scenario:** You use the SHS REST API to query `/api/v1/applications`. It works perfectly in dev, but in production with 20,000 completed apps, the request times out. Twist: You didn't use any query parameters. What is the fix?
    - **Answer:** Pass `limit` and `minDate`/`maxDate` to paginate the API.
    - **Mastery Explanation:** Without parameters, the API attempts to fetch and serialize all 20,000 apps at once. This massive payload causes timeouts and GC pauses.

29. **Scenario:** A user analyzes shuffle metrics via the REST API. They see a stage with 100GB Shuffle Read and 1GB Shuffle Write. They assume a broadcast join failed. Twist: They are looking at a stage with only 1 task. What is the real issue?
    - **Answer:** The extreme ratio (100x amplification) on a single task indicates severe data skew or a massive fanout, where a single reducer fetches disproportionate data.
    - **Mastery Explanation:** While a missing broadcast join causes high shuffle read, the fact that it's localized to 1 task confirms skew. The solution is `repartition()` before the join or salting.

30. **Scenario:** An on-prem cluster uses NFS for logs. You enable `spark.history.fs.inProgressOptimization.enabled=true` to save I/O bandwidth. But SHS still appears to miss new events for up to a minute, processing them in huge bursts. What did you forget?
    - **Answer:** You forgot to set `acregmax=5` on the NFS mount to reduce attribute caching.
    - **Mastery Explanation:** The SHS relies on the file's `st_size` to seek to the end. With default `actimeo=60`, the OS caches the old file size for 60 seconds, preventing SHS from seeing the appends.

31. **Scenario:** You have `spark.history.retainedApplications=5000` and use the default `InMemoryStore`. The SHS JVM has a 4GB heap. It crashes with an `OutOfMemoryError` after a day. Why?
    - **Answer:** `InMemoryStore` keeps all task metrics in heap (~20-50MB per app). 5000 apps require ~100-250GB heap.
    - **Mastery Explanation:** The default store cannot scale to thousands of apps. LevelDB is strictly required for this retention level.

32. **Scenario:** You wrote a Python script to parse event logs and sum `Executor CPU Time`. The total time reported is 50 hours, but the stage only took 1 hour wall-clock time with 100 cores. Twist: You pulled it straight from `SparkListenerTaskEnd`. What unit is it in?
    - **Answer:** Nanoseconds.
    - **Mastery Explanation:** In the raw JSON event log, CPU time is recorded in nanoseconds. You must divide by 1e9 to get seconds. (Note: The REST API converts this to milliseconds).

33. **Scenario:** The `EventLoggingListener` begins writing an `.inprogress` log. Mid-stage, the driver JVM is OOM killed. What happens to the log file on HDFS?
    - **Answer:** The atomic rename to remove `.inprogress` is never triggered; it remains `.inprogress` indefinitely.
    - **Mastery Explanation:** Since the driver was killed abruptly, the graceful shutdown hooks never ran. SHS will still parse it, but list it as an incomplete application.

34. **Scenario:** You switch `spark.eventLog.compression.codec` from LZ4 to Zstd for S3 logs. File size drops by 40%. However, CPU usage on the SHS during replay stays exactly the same. Why?
    - **Answer:** The bottleneck on the SHS is network I/O from S3, not decompression CPU.
    - **Mastery Explanation:** The CPU is mostly idle waiting for packets. Zstd reduces network time, but the CPU time spent parsing the JSON remains the dominant processing factor.

35. **Scenario:** SHS scans a large HDFS directory with 100k applications in 2 seconds. On S3, the same 100k apps take 3 minutes to list. Why?
    - **Answer:** HDFS resolves the listing via a single NameNode RPC. S3 `ListObjectsV2` pages at 1000 keys per page.
    - **Mastery Explanation:** 100k apps require 100 sequential HTTP round trips to S3, heavily impacting polling latency. Bounding directory size or using dedicated prefixes is required.

36. **Scenario:** You parse raw event logs to calculate `Shuffle Write Bytes`. You notice the metric is completely missing for a task that threw a `FetchFailedException`, but present in the retried task. Why?
    - **Answer:** Shuffle write metrics are only committed when a task completes successfully and sends `SparkListenerTaskEnd` with the final accumulators.
    - **Mastery Explanation:** Failed tasks discard their accumulators to prevent double-counting when the task is retried.

37. **Scenario:** To decrease SHS LevelDB disk footprint, you drastically reduce `spark.ui.retainedTasks` from 100,000 to 1,000. What is the user-facing side effect?
    - **Answer:** Stages with >1,000 tasks will have older task metrics evicted from the KVStore.
    - **Mastery Explanation:** The UI will lose detailed task-level granular data for those stages, making it impossible to debug straggler tasks that ran early in the stage.

38. **Scenario:** You query `/api/v1/applications/{app_id}/stages` for an older app and get a 404 HTTP status. However, the app still appears on the main listing page. You use LevelDB. What happened?
    - **Answer:** The application's metadata index is still in memory, but its detailed LevelDB files were purged from disk due to disk cleanup or total app limits.
    - **Mastery Explanation:** The high-level listing relies on the App Meta summary. If the deep state LevelDB files are deleted to save space, detailed endpoints 404.

39. **Scenario:** You deploy SHS to Kubernetes and map `spark.history.store.path` to an `emptyDir` volume. A pod restart occurs. What is the immediate consequence?
    - **Answer:** All LevelDB files are lost. SHS must synchronously replay all event logs from the remote storage on startup.
    - **Mastery Explanation:** `emptyDir` is ephemeral. This causes a massive CPU and network spike on startup, and the UI remains unresponsive until parsing finishes. A PersistentVolume (PV) is required.

40. **Scenario:** An engineer changes `spark.eventLog.dir` to use a new S3 bucket, but forgets to update `spark.history.fs.logDirectory` in the SHS config. What happens to the `.inprogress` files in the new bucket?
    - **Answer:** They accumulate and eventually rename successfully, but the SHS never polls them.
    - **Mastery Explanation:** The SHS is unaware of the new path. It will continue polling the old bucket indefinitely.

## Part 4: Coding & Debugging Questions

41. **Code Debugging:** Look at this log parser snippet:
    ```python
    if event_type == "SparkListenerTaskEnd":
        stage_id = event["Stage ID"]
        metrics = event["Task Metrics"]
        # Accumulate shuffle write
        stage_metrics[stage_id] += metrics["Shuffle Bytes Written"]
    ```
    **Bug:** `Shuffle Bytes Written` is nested under `Shuffle Write Metrics`.
    **Mastery Fix:** It must be accessed as `metrics.get("Shuffle Write Metrics", {}).get("Shuffle Bytes Written", 0)`.

42. **Config Debugging:** Identify the memory leak in this `spark-defaults.conf`:
    ```properties
    spark.eventLog.enabled=true
    spark.eventLog.rolling.enabled=false
    spark.history.retainedApplications=1000
    ```
    **Bug:** No `spark.history.store.path` is defined, falling back to `InMemoryStore`.
    **Mastery Fix:** Retaining 1000 apps without LevelDB will crash the SHS JVM with an OOM. Define a local NVMe path for LevelDB.

43. **Config Debugging:** You have NFS mounts with `actimeo=60`:
    ```properties
    spark.history.fs.logDirectory=file:///mnt/nfs/spark-logs
    spark.history.fs.inProgressOptimization.enabled=true
    spark.history.fs.update.interval=10s
    ```
    **Bug:** The poll interval (10s) is faster than the attribute cache expiry (60s).
    **Mastery Fix:** The SHS will seek to stale EOF offsets and miss events. Reduce `actimeo`/`acregmax` on the NFS mount to `< 10s`.

44. **API Debugging:** A script hits the SHS REST API:
    ```python
    apps = requests.get("http://shs:18080/api/v1/applications?status=completed").json()
    ```
    **Bug:** Missing pagination (`limit` parameter) on a production server.
    **Mastery Fix:** If there are 50,000 apps, this single call forces the SHS to serialize a 100MB+ JSON payload, starving Jetty threads. Always pass `limit=100`.

45. **Logic Debugging:** Diagnosing a silent S3 failure:
    ```bash
    $ curl http://shs:18080/api/v1/applications
    []
    ```
    Logs definitely exist in `s3a://bucket/logs/`.
    **Bug:** The `FsHistoryProvider` is silently swallowing an S3 permissions error.
    **Mastery Fix:** The IAM role is missing `s3:ListBucket`. S3A returns an empty array instead of throwing `AccessDenied`, masking the issue.

46. **Data Type Debugging:** Parsing raw JSON for CPU Time:
    ```python
    cpu_time = metrics.get("Executor CPU Time", 0)
    cpu_seconds = cpu_time / 1000  # Convert to seconds
    ```
    **Bug:** The conversion factor is wrong for raw JSON.
    **Mastery Fix:** In the event log, `Executor CPU Time` is in **nanoseconds**. It must be divided by `1e9` (1,000,000,000).

47. **Bottleneck Debugging:** Tuning S3 performance:
    ```properties
    spark.history.fs.numReplayThreads=100
    spark.hadoop.fs.s3a.connection.maximum=15
    ```
    **Bug:** Connection pool starvation.
    **Mastery Fix:** 100 threads will fight for 15 S3 HTTP connections, causing severe blocking. Set `fs.s3a.connection.maximum` >= 100.

48. **Logic Debugging:** Calculating shuffle amplification:
    ```python
    amp = shuffle_read_bytes / shuffle_write_bytes
    if amp > 10.0:
        print("High amplification skew detected")
    ```
    **Bug:** Missing zero-division check.
    **Mastery Fix:** If a stage only reads data (e.g. a final action stage with no shuffle write), `shuffle_write_bytes` is 0, causing a `ZeroDivisionError`.

49. **Log Parsing Debugging:** Parsing a rolling log stream:
    ```python
    for line in open("events-1.lz4"):
        event = json.loads(line)
    ```
    **Bug:** Fails to handle blank lines.
    **Mastery Fix:** Rolling logs write blank separator lines. You must `strip()` and verify the line is not empty before calling `json.loads()`, or it throws `JSONDecodeError`.

50. **Config Debugging:** Conflicting S3 properties:
    ```properties
    spark.eventLog.dir=s3a://bucket/logs/
    spark.history.fs.logDirectory=s3a://bucket/logs/
    spark.hadoop.fs.s3a.list.version=1
    ```
    **Bug:** Using `version=1` for listing.
    **Mastery Fix:** S3 list v1 API is eventually consistent for overwrites/deletions, which causes SHS to miss logs or see stale directory states during job completions. Change to `version=2`.
