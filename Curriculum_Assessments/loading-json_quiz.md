# Master Class Assessment: Loading JSON in Apache Spark

This assessment is designed to test Senior/Staff-level knowledge regarding Spark's JSON processing capabilities, Catalyst optimizations, Tungsten memory interactions, and physical execution plans.

## Section 1: True/False Questions

1. **Question:** Enabling `multiLine=true` in `spark.read.json()` allows Spark to dynamically search for JSON object boundaries and split large files across multiple executors.
**Answer:** False
**Mastery Explanation:** The underlying Hadoop `TextInputFormat` relies on newline characters for safe block boundaries. When `multiLine=true` is enabled, Spark cannot safely split the file because JSON objects might span across 128MB HDFS/S3 blocks. Consequently, Catalyst forces a single executor to read the entire file sequentially, destroying parallel processing and often causing OutOfMemoryErrors.

2. **Question:** Providing an explicit `StructType` schema when reading JSON eliminates the preliminary Spark job used for schema inference.
**Answer:** True
**Mastery Explanation:** Without an explicit schema, the Spark Driver must launch a separate O(N) job to scan the entire dataset, parse tokens, infer local schemas, and reduce them globally. Providing an explicit schema allows Catalyst to skip this massive I/O overhead and immediately generate the `FileSourceScanExec` physical plan.

3. **Question:** In Spark JSON processing, predicate pushdown operates exactly like Parquet by skipping irrelevant blocks of data on disk.
**Answer:** False
**Mastery Explanation:** JSON is a text-based format without file-level metadata or statistics. The Jackson parser must read and tokenize every single character from the disk. Predicate pushdown for JSON only prevents the allocation of pruned fields into Tungsten's off-heap memory, reducing GC overhead, but it does *not* reduce disk I/O.

4. **Question:** Spark uses a DOM-based JSON parser to load the entire JSON document into the JVM heap before converting it to Tungsten `UnsafeRow` format.
**Answer:** False
**Mastery Explanation:** Spark relies on the Jackson Streaming API, which tokenizes the character stream sequentially (e.g., `START_OBJECT`, `FIELD_NAME`). A DOM-based parser would load the entire file into the JVM heap, immediately causing OutOfMemoryErrors at big data scale.

5. **Question:** By default, if a JSON record violates the provided schema, Spark operates in `FAILFAST` mode and fails the job immediately.
**Answer:** False
**Mastery Explanation:** Spark's default mode for parsing JSON is `PERMISSIVE`. Instead of failing, Spark sets the conflicting fields to `null` and attempts to place the raw JSON string into a designated error column.

6. **Question:** If you use `PERMISSIVE` mode but fail to include the `columnNameOfCorruptRecord` column (e.g., `_corrupt_record`) in your explicit `StructType` schema, Spark will silently drop the corrupted records.
**Answer:** True
**Mastery Explanation:** If the corrupt record column is not explicitly defined in your schema, Catalyst has nowhere to store the raw malformed string. The task will succeed, but the corrupted data will be entirely discarded, leading to silent data loss.

7. **Question:** The Catalyst `RowConverter` immediately encodes parsed JSON tokens into Tungsten's `UnsafeRow` binary format to minimize Garbage Collection overhead.
**Answer:** True
**Mastery Explanation:** To achieve bare-metal speeds, Catalyst maps Jackson's generic tokens directly into internal types, which are then placed into off-heap `UnsafeRow` binary format. This bypasses standard Java object creation and avoids standard JVM Garbage Collection.

8. **Question:** Pushing datetime parsing into the `spark.read.json()` options via `timestampFormat` is generally slower than loading dates as strings and using the `to_timestamp()` DataFrame function.
**Answer:** False
**Mastery Explanation:** Defining `timestampFormat` allows the Jackson parser to route the string token immediately to Spark's highly optimized `DateTimeUtils`, converting it directly to a long integer (microseconds since epoch) before entering Tungsten memory. This avoids an expensive post-load cast and is significantly faster.

9. **Question:** During JSON schema inference, if one partition identifies a field as an `Integer` and another partition identifies it as a `Double`, the Spark Driver will promote the global schema type to `String` to avoid data loss.
**Answer:** False
**Mastery Explanation:** The Spark Driver promotes types to their most restrictive common permissive form. `Integer` and `Double` will be promoted to `Double`. It only degrades to `String` if completely incompatible types (like `Boolean` and `Double`) are found.

10. **Question:** Using `spark.sparkContext.wholeTextFiles` to read multi-line JSON prevents Tungsten from utilizing Whole-Stage CodeGen for the parsing phase.
**Answer:** False
**Mastery Explanation:** While `wholeTextFiles` reads the raw file into memory, applying Catalyst's internal `from_json` function to the resulting DataFrame allows Tungsten to generate optimized, compiled code for the parsing phase via Whole-Stage CodeGen.

## Section 2: Multiple Choice Questions

11. **Question:** Which component is primarily responsible for splitting raw JSON files across HDFS/S3 block boundaries before parsing begins?
A) Jackson Streaming API
B) Catalyst RowConverter
C) Hadoop TextInputFormat
D) Tungsten UnsafeRow
**Answer:** C
**Mastery Explanation:** The Hadoop `TextInputFormat` is the storage-level abstraction that reads raw bytes and safely splits massive files based on newline characters before handing the data to Spark's execution engine.

12. **Question:** When an explicit schema is not provided, how does the Spark Driver resolve conflicting data types for the same field across different partitions during schema inference?
A) It throws a SparkException and fails the job.
B) It takes the data type of the first partition that finished.
C) It degrades all conflicting fields to `StringType` immediately.
D) It continually promotes types to their most permissive numerical/compatible form, degrading to `StringType` only if necessary.
**Answer:** D
**Mastery Explanation:** The driver performs a massive reduction operation on local partition schemas, promoting types up the hierarchy (e.g., Integer -> Double). It only falls back to String if the types are irreconcilable.

13. **Question:** Why does setting `multiLine=true` for a 50GB JSON file almost guarantee an OutOfMemoryError or Task Timeout?
A) The Jackson parser switches to a DOM-based parsing model.
B) Hadoop TextInputFormat can no longer rely on newline characters, forcing Catalyst to pin file reading to a single executor task without block-level parallelism.
C) Tungsten disables off-heap memory allocation for multi-line strings.
D) Catalyst forces a cross-cluster shuffle to reassemble the JSON objects.
**Answer:** B
**Mastery Explanation:** `multiLine=true` means an object might span block boundaries. To guarantee the object is parsed completely, Spark abandons distributed processing for that file and forces one task to read the entire 50GB sequentially.

14. **Question:** In the context of JSON schema pruning, what happens to fields that exist in the raw JSON but are omitted from the explicit `StructType` schema?
A) They are read from disk, parsed by Jackson, and then immediately discarded by the RowConverter before entering Tungsten memory.
B) They are entirely skipped during disk I/O due to predicate pushdown.
C) They are placed into the `_corrupt_record` column.
D) They cause the job to fail in `FAILFAST` mode.
**Answer:** A
**Mastery Explanation:** Disk I/O cannot be skipped because JSON is text-based. Jackson still tokenizes the characters, but the Catalyst RowConverter discards the omitted tokens immediately, preventing them from consuming off-heap memory.

15. **Question:** What is the critical danger of relying on the default `PERMISSIVE` mode without explicitly defining the `columnNameOfCorruptRecord`?
A) The job will fail dynamically halfway through execution.
B) The Catalyst Optimizer will downgrade the query to interpret all fields as Strings.
C) Malformed records are silently dropped, resulting in untracked data loss.
D) The corrupted records cause a memory leak in the Jackson parser.
**Answer:** C
**Mastery Explanation:** Without the designated column in the explicitly provided schema, Spark simply drops the rows that violate the schema rather than failing the job, resulting in silent data loss.

16. **Question:** Which internal format does Spark use to store parsed JSON tokens in off-heap memory to ensure CPU cache-friendliness?
A) Java Native Objects
B) Tungsten UnsafeRow
C) Arrow RecordBatches
D) Parquet ColumnChunks
**Answer:** B
**Mastery Explanation:** Tungsten's `UnsafeRow` is Spark's internal binary format used to store structured data off-heap, bypassing the JVM's Garbage Collector and enabling Whole-Stage CodeGen.

17. **Question:** When parsing complex datetimes in JSON, why is it critical to specify the `timeZone` option explicitly?
A) Jackson cannot parse UTC strings without it.
B) Without it, Spark uses the local timezone of the Worker JVM, which can cause silent timestamp shifts across globally distributed clusters.
C) It forces the Catalyst optimizer to broadcast the timezone offset to all executors.
D) It prevents the `FileScanRDD` from triggering a shuffle.
**Answer:** B
**Mastery Explanation:** If the JVM timezone differs across nodes (e.g., workers in different regions), parsing timestamps without an explicit timezone will yield different epoch values for the exact same input string.

18. **Question:** What happens when you apply a `filter(col("id") === "123")` operation immediately after `spark.read.json()`?
A) Spark pushes the filter to the storage layer, bypassing Jackson parsing for non-matching rows.
B) Jackson parses all fields, Catalyst converts them to `UnsafeRow`, and then evaluates the filter.
C) Jackson parses only the "id" field, evaluates the filter, and skips parsing the rest of the object if it doesn't match.
D) The filter triggers a shuffle to co-locate records with ID "123".
**Answer:** B
**Mastery Explanation:** Unlike columnar formats, JSON predicate pushdown does not filter at the storage or parser level. The full row is read and parsed; however, only requested schema fields are converted to UnsafeRow, where the filter is then applied.

19. **Question:** What is the primary advantage of NDJSON (Newline Delimited JSON) over standard pretty-printed JSON in Spark?
A) It uses fewer characters, reducing disk I/O.
B) It allows the Hadoop TextInputFormat to safely split files across block boundaries, enabling massive parallelism.
C) It contains embedded schema metadata at the top of the file.
D) It allows Jackson to use DOM-based parsing.
**Answer:** B
**Mastery Explanation:** Because each JSON object is on a single line, `TextInputFormat` can split a massive file at any newline boundary and distribute the chunks to different tasks, unlocking the distributed execution engine.

20. **Question:** Why does a schema inference job double the overall cost of a Spark application reading massive JSON datasets?
A) It requires a full cross-cluster shuffle to sort the keys.
B) It triggers a separate, full-data scan job to read, parse, and infer types before the actual processing job even begins.
C) It forces Spark to cache the entire dataset in the JVM heap.
D) It disables Whole-Stage CodeGen for the entire DAG.
**Answer:** B
**Mastery Explanation:** Schema inference is not a metadata operation for JSON; it requires physically reading and parsing every single line of data to guarantee the schema is correct, effectively doubling disk I/O.

21. **Question:** Which Catalyst physical plan node is responsible for streaming the text data into the Jackson parser during the actual read phase?
A) `ExchangeExec`
B) `HashAggregateExec`
C) `FileSourceScanExec`
D) `LocalTableScanExec`
**Answer:** C
**Mastery Explanation:** `FileSourceScanExec` is the physical node that interfaces with the data source (HDFS/S3), orchestrating the read, triggering Jackson parsing, and yielding internal rows.

22. **Question:** In the context of `from_json`, what does the `FAILFAST` option do?
A) It drops corrupted records immediately without writing to a DLQ.
B) It aborts the job immediately if any string violates the provided schema, preventing null propagation.
C) It skips the schema inference phase.
D) It forces the job to execute on a single thread to guarantee order.
**Answer:** B
**Mastery Explanation:** `FAILFAST` is a strict mode that throws an exception and fails the entire task the moment a malformed or schema-violating record is encountered.

23. **Question:** How does schema pruning in JSON impact Garbage Collection (GC)?
A) It increases GC because discarded tokens must be finalized by the JVM.
B) It reduces GC because pruned fields are discarded as Jackson tokens and never allocated as off-heap `UnsafeRow` objects.
C) It has no impact on GC since all JSON processing happens off-heap.
D) It forces GC to run synchronously after every file split.
**Answer:** B
**Mastery Explanation:** By not allocating unnecessary fields into Tungsten memory, you drastically reduce memory pressure and the associated overhead of managing large Catalyst internal structures.

24. **Question:** If a JSON file contains nested structs up to 10 levels deep, how does Spark process this by default?
A) It flattens the structs into a single top-level row using dots as delimiters.
B) It stores the entire nested structure as a single JSON string in a StringType column.
C) It maps the hierarchy recursively into nested Catalyst `StructType` and `ArrayType` internal types.
D) It drops anything beyond 3 levels deep due to Jackson parser limitations.
**Answer:** C
**Mastery Explanation:** Spark natively supports nested schemas and the Catalyst RowConverter will recursively map the Jackson tokens into corresponding Catalyst nested types.

25. **Question:** What is a "Dead Letter Queue" (DLQ) in the context of Spark JSON processing?
A) An internal buffer used by Jackson for unparseable tokens.
B) A designated storage location (e.g., an S3 path) where malformed or schema-violating records are routed for debugging and alerting.
C) A Spark UI tab showing failed tasks.
D) A Tungsten mechanism for discarding pruned fields.
**Answer:** B
**Mastery Explanation:** Elite data engineering pipelines capture the `_corrupt_record` strings and write them to a DLQ, ensuring the main pipeline succeeds with valid data while preserving corrupted data for analysis.

## Section 3: Small Twist Questions

26. **Scenario:** You have an explicit schema with `_corrupt_record`. You run `df.filter($"_corrupt_record".isNotNull).count()`.
**Twist:** You accidentally set `.option("mode", "DROPMALFORMED")` instead of `PERMISSIVE`. What is the result of the count?
A) It returns the exact number of corrupt records.
B) It throws an exception.
C) It returns 0.
D) It returns the total row count.
**Answer:** C
**Mastery Explanation:** `DROPMALFORMED` tells Spark to immediately discard the row during the read phase before it even reaches the DataFrame API. The `_corrupt_record` column will always be empty, so the count is 0.

27. **Scenario:** You are loading NDJSON. You apply an explicit schema.
**Twist:** The data team accidentally uploads a pretty-printed, multi-line JSON file to the same S3 prefix. You do NOT have `multiLine=true` enabled. What happens?
A) The job fails immediately with an OutOfMemoryError.
B) Spark safely parses the file by falling back to DOM parsing.
C) Spark parses every individual line as a separate record, treating almost every line as a `_corrupt_record` since it violates the schema.
D) The file is completely ignored.
**Answer:** C
**Mastery Explanation:** Without `multiLine=true`, Spark's `TextInputFormat` blindly splits the file by newlines. Lines like `  "id": 123,` will be treated as full JSON objects, fail schema validation, and end up in the corrupt record column.

28. **Scenario:** You use `.option("timestampFormat", "yyyy-MM-dd")`.
**Twist:** A JSON record arrives with the value `"timestamp": "2023-10-01T15:30:00Z"`. What happens to this field under `PERMISSIVE` mode?
A) Spark truncates the time and loads `2023-10-01`.
B) Spark successfully parses it because ISO-8601 is the ultimate fallback.
C) Spark fails to parse it, sets the field to `null`, and marks the record as corrupt.
D) Spark crashes the job.
**Answer:** C
**Mastery Explanation:** Because you explicitly provided a strict format that doesn't match the incoming data, Catalyst's DateTimeUtils will fail to parse it. In `PERMISSIVE` mode, it sets the value to `null` and populates the corrupt record column.

29. **Scenario:** You define a schema with `StructField("id", IntegerType)`.
**Twist:** The JSON contains `"id": "123"` (String). What does Catalyst do in `PERMISSIVE` mode?
A) Casts the string `"123"` to the integer `123` automatically.
B) Sets the field to `null` and marks it corrupt.
C) Fails the job.
D) Promotes the schema to `StringType`.
**Answer:** A
**Mastery Explanation:** Catalyst's JSON RowConverter has some built-in leniency. It will attempt to safely cast strings to numbers if they are perfectly parsable as such, mitigating minor upstream type drift.

30. **Scenario:** You are processing a 10TB dataset of NDJSON files using `spark.read.json()`.
**Twist:** You forget to provide a schema, triggering schema inference. What is the impact on the cluster?
A) The job fails because schema inference is disabled for files over 1TB.
B) A single executor is forced to read all 10TB.
C) Spark runs an initial distributed job that reads the entire 10TB, severely delaying the main execution pipeline.
D) Spark infers the schema by only reading the first file in the directory.
**Answer:** C
**Mastery Explanation:** Schema inference for JSON requires scanning 100% of the dataset to ensure no conflicting types exist. This triggers a massive O(N) preliminary Spark job across the cluster.

31. **Scenario:** You use `spark.sparkContext.wholeTextFiles` to read multi-line JSON.
**Twist:** One of the JSON files in the S3 bucket is 4GB in size. What happens?
A) Tungsten successfully streams it using Whole-Stage CodeGen.
B) The job fails with an OutOfMemoryError because `wholeTextFiles` attempts to load the entire 4GB file content into a single Java String object in the executor's heap.
C) Hadoop TextInputFormat splits the 4GB file into 128MB chunks automatically.
D) The Catalyst optimizer rewrites it into a `FileSourceScanExec`.
**Answer:** B
**Mastery Explanation:** `wholeTextFiles` fundamentally bypasses block-level splitting and attempts to read the entire file into memory as a single key-value pair (filepath -> file content). A 4GB file will instantly blow up the executor heap.

32. **Scenario:** You define a schema containing 5 fields. The raw JSON contains 50 fields.
**Twist:** You write the resulting DataFrame out to Parquet. How many fields are in the Parquet file?
A) 5
B) 50
C) 51 (including `_corrupt_record`)
D) 0
**Answer:** A
**Mastery Explanation:** Because of schema pruning, Catalyst discards the other 45 fields during the read phase. They never enter the DataFrame and are not written to Parquet.

33. **Scenario:** You set `columnNameOfCorruptRecord` to `error_col`.
**Twist:** You define your `StructType` but you do NOT add `StructField("error_col", StringType)` to it. What happens when a corrupt record is found?
A) Spark dynamically adds `error_col` to the DataFrame schema.
B) Spark sets the entire row to nulls but keeps it in the DataFrame.
C) Spark throws an `AnalysisException` during the physical plan generation.
D) Spark silently drops the corrupt record.
**Answer:** D
**Mastery Explanation:** Just setting the option is not enough. If the column is not physically present in the provided `StructType`, Spark cannot allocate memory for it and silently discards the malformed row.

34. **Scenario:** You are reading NDJSON with an explicit schema.
**Twist:** Some of the JSON files are compressed using GZIP (`.json.gz`). Do you need to unzip them first?
A) Yes, because TextInputFormat cannot read binary GZIP files.
B) No, Spark automatically detects the `.gz` extension, decompresses it on the fly, but loses block-level parallelism because GZIP is not splittable.
C) No, and Spark maintains block-level parallelism because GZIP is splittable.
D) Yes, because Jackson cannot tokenize compressed streams.
**Answer:** B
**Mastery Explanation:** Spark natively supports reading `.gz` files. However, GZIP does not contain block synchronization markers. Therefore, a single executor must decompress the entire `.gz` file sequentially, losing parallelism for that specific file.

35. **Scenario:** You infer a schema on a small JSON dataset and Spark determines a field is `LongType`.
**Twist:** Tomorrow, a new file arrives where that field contains a floating-point number (e.g., `123.45`). You are using the explicitly saved schema from yesterday (`LongType`). What happens?
A) Spark casts `123.45` to `123` via truncation.
B) Spark fails the job.
C) Spark sets the field to `null` and marks the record corrupt because `123.45` cannot be safely cast to a Long.
D) Spark dynamically promotes the schema to `DoubleType`.
**Answer:** C
**Mastery Explanation:** Because an explicit schema of `LongType` was provided, Catalyst strictly enforces it. A float cannot be safely read into a Long without data loss, so Spark considers it a schema violation and handles it according to the `PERMISSIVE` mode rules.

36. **Scenario:** You use `.option("mode", "DROPMALFORMED")`.
**Twist:** The JSON record is perfectly valid, but one of the nullable fields in your schema is missing from the JSON payload. Does Spark drop the record?
A) Yes, because the schema is not strictly met.
B) No, Spark simply populates the missing field with `null` and keeps the record.
C) Yes, unless you use `FAILFAST`.
D) No, Spark injects an empty string.
**Answer:** B
**Mastery Explanation:** Missing fields do not constitute a "malformed" record if they are nullable. `DROPMALFORMED` only triggers if there is a structural parse error or a severe type violation that cannot be cast.

37. **Scenario:** You have a schema with a deeply nested struct: `user.profile.address.zipcode`.
**Twist:** You run `df.select("user.profile.address.zipcode")`. Does Catalyst prune the rest of the `user` struct during the read?
A) No, Catalyst must allocate the entire `user` struct in Tungsten before navigating it.
B) Yes, Catalyst supports nested schema pruning and will instruct Jackson/RowConverter to discard all sibling fields (e.g., `user.profile.name`) immediately.
C) Yes, but only if you use Parquet.
D) No, Jackson cannot skip tokens.
**Answer:** B
**Mastery Explanation:** Spark 3.x features advanced nested schema pruning. While Jackson still touches the characters, the RowConverter is smart enough to extract only the `zipcode` token and discard the rest of the massive nested object before Tungsten allocation.

38. **Scenario:** You are parsing JSON with `from_json`.
**Twist:** You pass a Map of options containing `"mode" -> "PERMISSIVE"`, but you do not include a corrupt record column in the schema passed to `from_json`. What does `from_json` do with unparseable strings?
A) It throws an exception.
B) It returns a fully `null` struct for that specific row.
C) It crashes the executor.
D) It routes it to the DataFrame's main `_corrupt_record` column.
**Answer:** B
**Mastery Explanation:** The `from_json` function operates inside an expression, not a table scan. If it fails to parse a string in `PERMISSIVE` mode and has no corrupt column defined in its specific struct, it simply returns a `null` struct, silently swallowing the error.

39. **Scenario:** Two JSON files exist. File A has `{"id": 1}`. File B has `{"id": "two"}`.
**Twist:** You use schema inference (`spark.read.json()`). What is the final inferred type of `id`?
A) IntegerType
B) DoubleType
C) StringType
D) StructType
**Answer:** C
**Mastery Explanation:** During the global reduce phase of schema inference, the driver sees `IntegerType` and `StringType`. Because they are fundamentally incompatible numerically, it promotes the global type to `StringType` to prevent data loss.

40. **Scenario:** You load JSON and immediately run `df.cache()`.
**Twist:** The data is cached in memory. Is the cached data stored as raw JSON strings?
A) Yes, to preserve the original formatting.
B) No, it is stored in Catalyst's highly optimized, columnar InMemory format (which compresses the UnsafeRows).
C) Yes, but compressed with Snappy.
D) No, it is stored as Java Objects.
**Answer:** B
**Mastery Explanation:** Once data passes through the `FileSourceScanExec` and becomes `UnsafeRows`, all downstream operations (including caching) treat it as standard Catalyst tabular data. The cached representation is a highly optimized, columnar in-memory layout.

## Section 4: Coding & Debugging Questions

41. **Debugging Scenario:** You are running a Spark job reading 5TB of JSON data. The job takes 2 hours, but the Spark UI shows a massive job executing *before* your actual `saveAsTable` action even begins. What is causing this, and how do you fix it?
**Answer:** The preliminary job is the schema inference phase. It occurs because `spark.read.json()` was called without an explicit schema. To fix it, define a `StructType` explicitly and pass it via `.schema(mySchema)`. This allows Catalyst to skip the O(N) inference job entirely.

42. **Debugging Scenario:** A downstream analytics team complains they are missing about 5% of their daily events. You check the Spark UI; all tasks succeeded with no errors. Your code reads JSON using a strict `StructType` and defaults to `PERMISSIVE` mode. Identify the bug.
**Answer:** Silent data loss is occurring because the explicit `StructType` does not contain the `_corrupt_record` column. When Spark encounters schema violations (which constitute the missing 5%), it has nowhere to put the raw string, so it drops the row silently. Add `StructField("_corrupt_record", StringType)` to the schema and set the `columnNameOfCorruptRecord` option.

43. **Coding Task:** Write the PySpark code to read NDJSON files from `s3a://data/`, using an explicit schema that captures corrupt records, and immediately split the DataFrame into `valid_df` and `corrupt_df`.
**Answer:**
```python
from pyspark.sql.types import StructType, StructField, StringType
schema = StructType([
    StructField("id", StringType(), True),
    StructField("_corrupt_record", StringType(), True)
])
df = spark.read.schema(schema).option("columnNameOfCorruptRecord", "_corrupt_record").json("s3a://data/")
df.cache()
corrupt_df = df.filter(df["_corrupt_record"].isNotNull())
valid_df = df.filter(df["_corrupt_record"].isNull()).drop("_corrupt_record")
```

44. **Debugging Scenario:** You are attempting to load a 10GB configuration file formatted as pretty-printed JSON. You set `multiLine=true`. The job consistently dies with `java.lang.OutOfMemoryError: Java heap space` on a single executor, while the other 99 executors sit completely idle. Why?
**Answer:** `multiLine=true` destroys block-level parallelism because Spark cannot use newline characters to safely split the file across executors. A single executor is pinned to read the entire 10GB file. To fix this, the upstream system must produce Newline Delimited JSON (NDJSON), allowing Hadoop `TextInputFormat` to split the file properly.

45. **Coding Task:** You are forced to parse a multi-line JSON file, but you know `multiLine=true` will cause an OOM. Write the Scala code to ingest the file safely using `wholeTextFiles` and Catalyst's `from_json`.
**Answer:**
```scala
val rawRDD = spark.sparkContext.wholeTextFiles("s3a://data/config.json")
val rawDf = rawRDD.toDF("path", "raw_content")
val parsedDf = rawDf.withColumn("data", from_json($"raw_content", mySchema, Map("mode" -> "FAILFAST")))
```

46. **Debugging Scenario:** Your JSON timestamps look like this: `01/15/2024 14:30:00`. You define your schema with `TimestampType`. When you load the data, all timestamp fields are returning `null`. Why?
**Answer:** JSON does not have a native datetime type; they are strings. Spark's default timestamp parser expects the ISO-8601 format (`yyyy-MM-dd'T'HH:mm:ss.SSSXXX`). Because your format differs, parsing fails, and `PERMISSIVE` mode sets it to `null`. Fix this by adding `.option("timestampFormat", "MM/dd/yyyy HH:mm:ss")`.

47. **Debugging Scenario:** You execute `df.filter(col("country") === "USA")` on a JSON dataset. You notice that your disk I/O metrics show that the entire dataset is being read from disk, even though only 10% of the data is from the USA. Why isn't predicate pushdown working?
**Answer:** Predicate pushdown cannot skip disk reads for text-based formats like JSON because there are no row groups or file-level statistics (unlike Parquet). The Jackson parser must still read and tokenize every character to evaluate the filter. 

48. **Coding Task:** Your JSON payload contains 500 fields. You only need `user_id` and `event_type`. Write the exact `StructType` definition to enforce aggressive schema pruning to save memory.
**Answer:**
```scala
val prunedSchema = StructType(Array(
  StructField("user_id", StringType, nullable = false),
  StructField("event_type", StringType, nullable = true)
))
// By explicitly providing this minimal schema to spark.read.schema(), 
// Catalyst will discard the other 498 fields immediately after parsing.
```

49. **Debugging Scenario:** You parse JSON with `timestampFormat` enabled. Your cluster spans AWS regions in us-east-1 and eu-west-1. You notice that the exact same timestamp string in the source JSON is producing different Epoch times in the final Parquet files depending on which executor processed it. Why?
**Answer:** You failed to specify the `timeZone` option. Without it, Spark uses the local JVM timezone of the worker node processing the task. Since the workers are in different timezones, the epoch conversion shifts. Fix this by explicitly setting `.option("timeZone", "UTC")`.

50. **Debugging Scenario:** You have a pipeline running `from_json` inside a `withColumn` transformation. The `mode` is set to `PERMISSIVE`. You notice some nested JSON strings are malformed, but `from_json` is just returning `null` structs. How can you configure `from_json` to immediately crash the job if a malformed payload is detected, ensuring no bad data propagates?
**Answer:** Change the mode option inside the `from_json` map to `FAILFAST`. 
Example: `from_json(col("payload"), schema, Map("mode" -> "FAILFAST"))`. This overrides the permissive behavior and forces the task to throw an exception and fail instantly upon encountering bad JSON.
