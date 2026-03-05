# Apache Spark 4.1.x Practical Cheat Sheet

A quick reference for **Spark Core (RDD), Spark SQL/DataFrames, and Structured Streaming**.  
Focus: **what it does + when to use it + performance guidance**.

---

# 1. Spark Core (RDD API)

RDD = **Resilient Distributed Dataset**  
Immutable distributed collection processed in parallel across a cluster.

## RDD Transformations (Lazy)

| Operation | Description | When to Use | Avoid When |
|---|---|---|---|
| `map()` | Applies a function to each element. | When transforming records independently. | When expanding one record to many. |
| `flatMap()` | Similar to map but outputs multiple elements per input. | Parsing text or splitting records. | When only one output per input is needed. |
| `filter()` | Keeps elements matching a condition. | Remove unnecessary data early in pipeline. | Rarely avoided; generally safe. |
| `distinct()` | Removes duplicate elements. | Deduplication tasks. | On very large datasets with heavy shuffle cost. |
| `union()` | Combines two RDDs into one. | Merging datasets with same structure. | When schemas differ or deduplication required. |
| `intersection()` | Returns elements present in both RDDs. | Comparing datasets. | Large datasets causing shuffle overhead. |
| `subtract()` | Removes elements present in another RDD. | Filtering reference datasets. | When join logic is more appropriate. |
| `reduceByKey()` | Aggregates values by key using a function. | Distributed counting or summing tasks. | When all values must be preserved. |
| `groupByKey()` | Groups values by key. | When every value must be retained. | Large datasets due to shuffle overhead. |
| `mapValues()` | Applies transformation only to values. | Processing key-value datasets. | When keys must also change. |
| `sortByKey()` | Sorts key-value pairs by key. | Ordered processing or reporting. | Large datasets with expensive shuffles. |
| `join()` | Joins two key-value RDDs. | Enriching datasets. | When one dataset is very large and skewed. |
| `repartition(n)` | Changes partitions using shuffle. | Increase parallelism. | When decreasing partitions. |
| `coalesce(n)` | Reduces partitions without full shuffle. | Before writing output files. | Increasing partitions. |

---

## RDD Actions (Trigger Execution)

| Operation | Description | When to Use |
|---|---|---|
| `collect()` | Returns all elements to driver. | Small datasets only. |
| `count()` | Counts elements in RDD. | Dataset size estimation. |
| `first()` | Returns first element. | Sampling dataset quickly. |
| `take(n)` | Returns first n elements. | Inspecting data safely. |
| `reduce()` | Aggregates dataset using a function. | Global aggregation tasks. |
| `foreach()` | Executes function on each element. | Writing results to external systems. |
| `saveAsTextFile(path)` | Writes RDD to distributed storage. | Exporting results. |

---

# 2. Spark SQL / DataFrame API

DataFrames represent **structured data with schema** and use the Spark SQL engine for optimization.

## DataFrame Transformations (Lazy)

| Operation | Description | When to Use | Avoid When |
|---|---|---|---|
| `select()` | Chooses specific columns. | Reduce dataset size early. | When all columns required. |
| `withColumn()` | Adds or modifies a column. | Feature engineering. | Excessive chained transformations. |
| `drop()` | Removes columns. | Cleaning unnecessary data. | When columns needed later. |
| `filter()`/`where()` | Filters rows based on condition. | Apply early for performance. | Rarely avoided. |
| `groupBy()` | Groups rows for aggregation. | Summaries and analytics. | When no aggregation needed. |
| `agg()` | Performs aggregations. | Metrics calculations. | When row-level processing required. |
| `orderBy()`/`sort()` | Sorts dataset. | Reporting outputs. | Large distributed datasets unnecessarily. |
| `join()` | Combines datasets. | Data enrichment. | When datasets are extremely skewed. |
| `union()` | Combines rows of two DataFrames. | Appending datasets. | Different schemas. |
| `distinct()` | Removes duplicate rows. | Data deduplication. | Massive datasets due to shuffle. |
| `dropDuplicates()` | Removes duplicates based on columns. | Data quality cleanup. | When duplicates are required. |
| `explode()` | Expands arrays/maps into rows. | Processing nested data. | Flat datasets. |
| `repartition(n)` | Redistributes partitions. | Increasing parallelism. | Reducing partitions. |
| `cache()` | Stores dataset in memory. | Reused datasets in pipelines. | One-time use datasets. |
| `persist()` | Stores dataset with configurable storage level. | Large pipelines needing reuse. | Memory constrained environments. |

---

## DataFrame Actions

| Operation | Description | When to Use |
|---|---|---|
| `show()` | Displays rows in console. | Quick debugging. |
| `collect()` | Retrieves all rows to driver. | Small datasets only. |
| `count()` | Returns row count. | Dataset validation. |
| `first()` | Returns first row. | Quick inspection. |
| `take(n)` | Retrieves limited rows. | Sampling safely. |
| `describe()` | Summary statistics. | Quick exploratory analysis. |
| `write()` | Writes data to storage systems. | Persisting datasets. |
| `saveAsTable()` | Stores DataFrame as table. | Data warehouse pipelines. |

---

# 3. Structured Streaming

Structured Streaming processes **continuous data streams as incremental DataFrames**.

---

## Streaming Sources

| Source | Description | When to Use |
|---|---|---|
| `readStream` | Creates streaming DataFrame. | Starting streaming pipeline. |
| Kafka source | Reads data from Kafka topics. | Real-time event ingestion. |
| File source | Monitors directories for new files. | Batch-to-stream pipelines. |
| Socket source | Reads text from TCP socket. | Testing streaming pipelines. |

---

## Streaming Transformations

| Operation | Description | When to Use |
|---|---|---|
| `select()` | Selects columns from streaming data. | Filtering required attributes. |
| `filter()` | Filters incoming events. | Event validation or routing. |
| `groupBy()` | Groups records for aggregation. | Metrics computation. |
| `agg()` | Aggregates streaming records. | Streaming analytics. |
| `join()` | Joins streaming and static datasets. | Enrichment pipelines. |
| `window()` | Groups events into time windows. | Time-based aggregations. |
| `withWatermark()` | Handles late arriving data. | Event-time processing pipelines. |

---

## Streaming Output Operations

| Operation | Description | When to Use |
|---|---|---|
| `writeStream` | Defines streaming sink. | Writing streaming output. |
| `outputMode("append")` | Writes new rows only. | Event streams without updates. |
| `outputMode("update")` | Updates changed rows. | Aggregation pipelines. |
| `outputMode("complete")` | Writes entire result table. | Small aggregated outputs. |
| `trigger()` | Defines processing interval. | Controlling latency vs throughput. |
| `start()` | Starts streaming query. | Beginning execution. |
| `awaitTermination()` | Waits for streaming job completion. | Long-running pipelines. |

---

# 4. Common Aggregation Functions

| Function | Description | Use Case |
|---|---|---|
| `count()` | Counts rows or values. | Event counting. |
| `sum()` | Total of numeric column. | Revenue or metric totals. |
| `avg()` | Average calculation. | KPI analysis. |
| `min()` | Smallest value. | Monitoring thresholds. |
| `max()` | Largest value. | Peak detection. |
| `approx_count_distinct()` | Approximate unique counts. | Large-scale analytics. |

---

# 5. Performance Tips

| Tip | Explanation |
|---|---|
| Filter Early | Reduce data size before expensive transformations. |
| Avoid `groupByKey` | Causes large shuffles; prefer `reduceByKey`. |
| Use Broadcast Joins | Efficient when one dataset is small. |
| Repartition Carefully | Causes expensive shuffles. |
| Cache Only Reused Data | Prevents memory waste. |
| Use Built-in Functions | Faster than Python UDFs. |

---

# 6. Debugging & Development Tools

| Tool | Purpose |
|---|---|
| Spark UI | Visualize stages, tasks, and performance. |
| `explain()` | Displays query execution plan. |
| `printSchema()` | Shows DataFrame schema. |
| `show()` | Inspect data quickly. |

---

# 7. Spark Execution Model

Transformations are **lazy** and build a logical execution plan.

**Execution flow:**

```
Transformations
→ Logical Plan
→ Catalyst Optimizer
→ Physical Plan
→ Tasks on Executors
→ Actions Trigger Execution
```

---

*End of Cheat Sheet*
