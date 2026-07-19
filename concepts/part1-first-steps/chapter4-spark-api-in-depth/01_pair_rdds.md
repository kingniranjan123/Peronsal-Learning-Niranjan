# Pair RDDs

**Pair RDDs are specialized RDDs that store data as key-value pairs, enabling powerful operations like grouping, aggregating, and joining data based on keys.**

## Why It Matters
In distributed data processing, you rarely work with isolated data points. More often, you need to group data by a specific attribute (e.g., total sales per customer, average temperature per city). Pair RDDs provide the foundational structure `(Key, Value)` that Spark requires to route data correctly across the cluster for these operations. Without Pair RDDs, complex aggregations and relational joins would be nearly impossible to implement efficiently at scale.

## How It Works

Pair RDDs are created simply by mapping an existing RDD into an RDD of tuples with exactly two elements. Spark implicitly provides special operations on any RDD of `(K, V)` tuples through implicit conversions (in Scala) or dynamically (in Python).

When you perform a "By Key" operation on a Pair RDD, Spark uses the Key to determine how data should be partitioned and shuffled across the cluster. All values associated with the same key are guaranteed to be processed together in the final reduction or grouping step.

### Core Operations
- **`reduceByKey(func)`**: Combines values with the same key using a specified associative and commutative function. Performs a map-side combine before shuffling.
- **`groupByKey()`**: Groups all values for a single key into a sequence. Highly discouraged for aggregation due to massive shuffle sizes.
- **`mapValues(func)`**: Applies a function to each value without changing the key. This preserves the original data partitioning, avoiding a shuffle!
- **`flatMapValues(func)`**: Applies a function that returns an iterator to each value, flattening the results while keeping the original key.
- **`keys()` / `values()`**: Returns an RDD of just the keys or just the values.
- **`countByKey()`**: Action that returns a local map of keys to their counts.
- **`sortByKey()`**: Sorts the RDD based on the keys.
- **`join()`, `leftOuterJoin()`, `rightOuterJoin()`**: Relational joins on the keys.
- **`subtractByKey()`**: Removes elements with keys present in another RDD.

## Flow Diagram

```
# Architecture Diagram
# (See MD source for diagram code)
graph TD
    A[Raw Data RDD<br>['a b', 'a c', 'b b']] --> B[FlatMap to Words<br>['a', 'b', 'a', 'c', 'b', 'b']]
    B --> C[Map to Pair RDD<br>'a'->1, 'b'->1, 'a'->1, 'c'->1...]
    C --> D{Operation?...
```

## Data Visualization

### mapValues vs map

| Input Pair RDD | `map(lambda x: (x[0], x[1]*10))` | `mapValues(lambda v: v*10)` | Partitioning Preserved? |
|----------------|----------------------------------|-----------------------------|-------------------------|
| `("apple", 5)` | `("apple", 50)` | `("apple", 50)` | No (for `map`) / Yes (for `mapValues`) |
| `("banana", 2)`| `("banana", 20)`| `("banana", 20)`| No (for `map`) / Yes (for `mapValues`) |
| `("apple", 3)` | `("apple", 30)` | `("apple", 30)` | No (for `map`) / Yes (for `mapValues`) |

Using `mapValues` is vastly superior when only manipulating the value because Spark knows the keys haven't changed, meaning downstream operations might not need to shuffle the data again.

## Code Example

```python
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("PairRDDExample").setMaster("local[*]")
sc = SparkContext(conf=conf)

# 1. Creating a Pair RDD from a list of tuples
data = [("coffee", 2), ("tea", 1), ("coffee", 3), ("water", 5), ("tea", 2)]
pair_rdd = sc.parallelize(data)

# 2. reduceByKey (Best practice for aggregation)
# Computes the total cups of each drink
total_drinks = pair_rdd.reduceByKey(lambda x, y: x + y)
print(f"Total Drinks: {total_drinks.collect()}")
# Output: [('coffee', 5), ('tea', 3), ('water', 5)]

# 3. groupByKey (Avoid if possible, shown for educational purposes)
# Groups all values for each key into an iterable
grouped_drinks = pair_rdd.groupByKey().mapValues(list)
print(f"Grouped Drinks: {grouped_drinks.collect()}")
# Output: [('coffee', [2, 3]), ('tea', [1, 2]), ('water', [5])]

# 4. mapValues (Transforms values while keeping partitioner intact)
# Multiply each order by 10
scaled_orders = pair_rdd.mapValues(lambda v: v * 10)

# 5. Join operations
pricing_data = [("coffee", 2.50), ("tea", 1.50)]
pricing_rdd = sc.parallelize(pricing_data)

# Join the total drinks with pricing to calculate revenue
# Returns (Key, (Value_from_RDD1, Value_from_RDD2))
joined_rdd = total_drinks.join(pricing_rdd)
print(f"Joined RDD: {joined_rdd.collect()}")
# Output: [('coffee', (5, 2.5)), ('tea', (3, 1.5))]

# Calculate final revenue
revenue_rdd = joined_rdd.mapValues(lambda v: v[0] * v[1])
print(f"Revenue per item: {revenue_rdd.collect()}")
# Output: [('coffee', 12.5), ('tea', 4.5)]
```

## Common Pitfalls
* **Using `groupByKey` for aggregation**: This pulls all values for a single key into memory on a single executor, causing massive network shuffles and OutOfMemory errors. Always use `reduceByKey` or `aggregateByKey` instead.
* **Using `map` instead of `mapValues`**: If you transform a Pair RDD using `map(lambda x: (x[0], new_val))`, Spark loses the partitioner information because it assumes the key might have changed. This triggers unnecessary shuffles down the line. Use `mapValues` when keys remain static.
* **Highly Skewed Keys**: If one key (e.g., "null" or "unknown") has 1000x more records than other keys, operations like `reduceByKey` will cause a single task to take exponentially longer than the others (straggler task).
* **Cartesian Joins by Accident**: Performing joins without sufficient filtering or on highly duplicated keys can result in a massive explosion of output records.

## Key Takeaway
**Pair RDDs unlock distributed aggregations and relational joins; always prefer `reduceByKey` over `groupByKey` to minimize network bottlenecks, and use `mapValues` to preserve partitioning.**
