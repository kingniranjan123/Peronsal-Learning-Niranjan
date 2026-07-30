## 5. Big Data File Formats

While CSV and JSON are human-readable, they are highly inefficient for distributed computing. Big Data requires formats that support schema evolution, high compression, and specific read patterns.

### Columnar vs. Row-Based Storage
Formats like **Parquet** and **ORC** store data in columns rather than rows. If you have a table with 100 columns but only query 2 of them, a columnar format allows Spark to physically read only the data for those 2 columns from disk, skipping the rest and saving massive amounts of I/O. **Avro** is row-based and is preferred for write-heavy streaming.

```mermaid
graph LR
    subgraph Row-Based (CSV/JSON/Avro)
    R1[Row 1: ID, Name, Age, City]
    R2[Row 2: ID, Name, Age, City]
    end

    subgraph Columnar (Parquet/ORC)
    C1[Column: ID 1, 2, 3...]
    C2[Column: Name A, B, C...]
    C3[Column: Age 22, 24, 26...]
    end
    
    style C1 fill:#dfd
    style C3 fill:#dfd
```

### Practical Examples
1. **Parquet for Analytics:** A Data Scientist running `SELECT AVG(salary) FROM employees` on a Parquet file only reads the salary column from disk.
2. **Avro for Kafka Streams:** A real-time IoT pipeline uses Avro because of its fast row-level write speeds and robust schema evolution (handling new sensor types).
3. **ORC for Hive:** A Data Warehouse team uses ORC because it offers exceptional compression ratios (often 75% smaller than CSV).
4. **Predicate Pushdown:** When Spark queries Parquet files with `WHERE age > 30`, the Parquet metadata allows Spark to completely skip reading file chunks where the maximum age is known to be under 30.

> [!TIP]
> **Library References:**
> *   *Beginning Apache Spark 2* (Parquet/ORC) — Pages 14, 19, 38
> *   *Spark in Action* — Pages 2, 32, 37
