# RetailPulse Capstone – Lab Guide
## Days 8, 9 & 10: Foundation → RDD Layer → Structured ETL

---

## Data at a Glance

All files live under `quickstart/jobs/capstone_data/dev/batch/`.

| File | Format | Rows | Key Fields |
|------|--------|------|-----------|
| `orders.csv` | CSV | 10,000 | `order_id`, `customer_id`, `product_id`, `region`, `category`, `total_amount`, `status`, `discount`, `order_date`, `order_month`, `order_year` |
| `customers.csv` | CSV | 1,000 | `customer_id`, `region`, `segment`, `loyalty_tier`, `is_active` |
| `products.json` | JSON-lines | 200 | `product_id`, `category`, `subcategory`, `base_price`, `stock_quantity`, `is_active` |
| `regions.csv` | CSV | 5 | `region`, `target_revenue`, `manager`, `hq_city` |

**Built-in data characteristics you will observe:**
- East region carries ~35% of order volume (data skew)
- ~0.5% of `discount`, `city`, `payment_method` fields are empty strings (dirty data)
- ~1% of orders have `total_amount > 5000` (high-value anomalies)

---

---

# Day 8 – Foundation & Architecture

## Goal
Load the orders dataset, run basic aggregations, and connect what you wrote (Python code) to what Spark actually does (execution plan, shuffle stage, Spark UI).

---

### Task 1 – Load orders with an explicit schema

**What to do:**  
Define a `StructType` for orders.csv and load using `.schema(...)` instead of `inferSchema=True`.

```python
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)

spark = SparkSession.builder.appName("RetailPulse-Day8").getOrCreate()

orders_schema = StructType([
    StructField("order_id",       StringType(),  True),
    StructField("customer_id",    StringType(),  True),
    StructField("product_id",     StringType(),  True),
    StructField("product_name",   StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("subcategory",    StringType(),  True),
    StructField("region",         StringType(),  True),
    StructField("city",           StringType(),  True),
    StructField("segment",        StringType(),  True),
    StructField("order_date",     StringType(),  True),
    StructField("order_year",     IntegerType(), True),
    StructField("order_month",    IntegerType(), True),
    StructField("order_quarter",  IntegerType(), True),
    StructField("quantity",       IntegerType(), True),
    StructField("unit_price",     DoubleType(),  True),
    StructField("discount",       StringType(),  True),  # intentionally String — dirty data
    StructField("total_amount",   DoubleType(),  True),
    StructField("status",         StringType(),  True),
    StructField("payment_method", StringType(),  True),
    StructField("is_high_value",  StringType(),  True),
])

DATA_ROOT = "/opt/spark/jobs/capstone_data/dev/batch"

orders_df = (spark.read
    .option("header", "true")
    .schema(orders_schema)
    .csv(f"{DATA_ROOT}/orders.csv"))

print(f"Row count: {orders_df.count()}")   # expect 10,000
orders_df.printSchema()
```

**Why `discount` is StringType here:**  
The generator deliberately leaves ~0.5% of discount values as empty strings `""`. If you use `DoubleType` with `inferSchema`, those rows silently become `null`. Keeping it `String` first lets you **see and decide** what to do with dirty data before committing to a type cast — which is Task 2.

**Why explicit schema matters:**  
`inferSchema=True` tells Spark to do a full extra pass over the file just to guess types. In production with millions of rows on distributed storage, this doubles read time. Explicit schema = one pass.

---

### Task 2 – Data quality check before any aggregation

**What to do:**  
Three specific checks on the raw loaded data.

```python
from pyspark.sql.functions import col, count, when

total      = orders_df.count()
completed  = orders_df.filter(col("status") == "Completed").count()

# discount is loaded as String — empty string = dirty record
dirty_disc = orders_df.filter(
    col("discount").isNull() | (col("discount") == "")
).count()

print(f"Total orders       : {total}")
print(f"Completed orders   : {completed}  ({100*completed/total:.1f}%)")
print(f"Dirty discount rows: {dirty_disc}  ({100*dirty_disc/total:.2f}%)")
```

**Expected output:**
```
Total orders       : 10000
Completed orders   : ~8000  (80.0%)
Dirty discount rows: ~50    (~0.50%)
```

**Why bother:**  
Those 50 rows with a missing discount would produce wrong `total_amount` averages if you aggregated without handling them first. Finding nulls **before** aggregating is a non-negotiable first step in any real pipeline.

---

### Task 3 – Regional sales summary

**What to do:**  
Filter to `Completed` only, clean `discount`, then aggregate by region.

```python
from pyspark.sql.functions import sum as _sum, avg, count, round as _round, lit

# Fix dirty data: replace empty string discount with "0", then cast to Double
orders_clean = (orders_df
    .filter(col("status") == "Completed")
    .withColumn("discount",
        when(col("discount").isNull() | (col("discount") == ""), "0")
        .otherwise(col("discount")))
    .withColumn("discount", col("discount").cast("double")))

# Compute overall revenue for percentage calculation
overall_revenue = orders_clean.agg(_sum("total_amount").alias("total")).collect()[0]["total"]

regional_summary = (orders_clean
    .groupBy("region")
    .agg(
        count("order_id").alias("total_orders"),
        _round(_sum("total_amount"), 2).alias("total_revenue"),
        _round(avg("total_amount"), 2).alias("avg_order_value"),
    )
    .withColumn("pct_of_total",
        _round(col("total_revenue") / lit(overall_revenue) * 100, 1))
    .orderBy(col("total_revenue").desc()))

regional_summary.show()
```

**Expected shape — East is the outlier:**
```
+-------+------------+-------------+---------------+------------+
|region |total_orders|total_revenue|avg_order_value|pct_of_total|
+-------+------------+-------------+---------------+------------+
|East   |~2800       | ~xxxxxx     | ~xxx          |~35.0       |
|North  |~1450       | ~xxxxxx     | ~xxx          |~18.x       |
...
+-------+------------+-------------+---------------+------------+
```

**Why `pct_of_total`:**  
This single column will be your reference point on Day 11 when you observe data skew. East at 35% is a built-in characteristic of the dataset — it is not an accident.

---

### Task 4 – Category revenue breakdown (top 5)

**What to do:**

```python
category_summary = (orders_clean
    .groupBy("category")
    .agg(
        count("order_id").alias("order_count"),
        _round(_sum("total_amount"), 2).alias("total_revenue"),
    )
    .orderBy(col("total_revenue").desc())
    .limit(5))

category_summary.show()
```

**Why:**  
This is the "category-level metrics" deliverable. It also surfaces which product categories drive the most revenue — that informs whether the `products.json` broadcast join on Day 10 is worth doing (small table, high-value join = classic broadcast candidate).

---

### Task 5 – Read the execution plan for the regional aggregation

**What to do:**  
Before calling `.show()`, call `.explain()` on `regional_summary`.

```python
regional_summary.explain(mode="extended")
```

**What to look for in the output:**

1. **`== Parsed Logical Plan ==`** — what you wrote in Python
2. **`== Analyzed Logical Plan ==`** — types resolved
3. **`== Optimized Logical Plan ==`** — filters pushed down before aggregation
4. **`== Physical Plan ==`** — look for this specific node:
   ```
   Exchange hashpartitioning(region, 200)
   ```
   This is the shuffle. Spark must redistribute all rows across the network so that all records for "East" land on the same executor before it can sum them.

5. Count the number of `Exchange` nodes. For a single `groupBy`, you should see exactly **one**.

**Why this matters:**  
Every slow Spark job has too many `Exchange` nodes. Learning to read this plan on Day 8 — with a simple query you just wrote — makes optimization on Day 11 concrete rather than abstract.

---

### Task 6 – Write regional summary to Parquet

**What to do:**

```python
OUTPUT_ROOT = "/opt/spark/jobs/capstone_data/dev/output"

regional_summary.write.mode("overwrite").parquet(f"{OUTPUT_ROOT}/regional_summary.parquet")

# Verify: read it back and count
spark.read.parquet(f"{OUTPUT_ROOT}/regional_summary.parquet").show()
```

**Why Parquet and not CSV:**  
The regional summary has 5 rows and 4 columns. On this scale it doesn't matter. But the rule is established now: all pipeline outputs are Parquet. When Day 10 reads this back to join with target revenue from regions.csv, it will read only the columns it needs — that is the columnar advantage.

---

### Day 8 Deliverables

| Artifact | What it looks like |
|---|---|
| Execution plan printout | Text showing `Exchange hashpartitioning(region, 200)` |
| Regional summary table | 5 rows, East at ~35% |
| Category top-5 table | 5 rows sorted by revenue |
| Dirty data count | ~50 rows with empty discount |
| Parquet file on disk | `output/regional_summary.parquet` |

---

---

# Day 9 – Distributed Execution & RDD Layer

## Goal
Re-implement a slice of Day 8 using the RDD API to make Spark internals visible: how shuffles happen, what data skew looks like in partitions, how lineage enables fault tolerance, and when caching actually helps.

---

### Task 1 – Re-implement regional revenue with RDD

**What to do:**  
Load `orders.csv` as raw text, parse it manually, compute revenue per region using `reduceByKey`.

```python
sc = spark.sparkContext

raw_rdd = sc.textFile(f"{DATA_ROOT}/orders.csv")

# Remove header
header = raw_rdd.first()
data_rdd = raw_rdd.filter(lambda line: line != header)

# CSV columns by position:
# 0=order_id, 1=customer_id, 2=product_id, 3=product_name, 4=category,
# 5=subcategory, 6=region, 7=city, 8=segment, 9=order_date, 10=order_year,
# 11=order_month, 12=order_quarter, 13=quantity, 14=unit_price, 15=discount,
# 16=total_amount, 17=status, 18=payment_method, 19=is_high_value

def parse_order(line):
    cols = line.split(",")
    try:
        region       = cols[6].strip()
        total_amount = float(cols[16].strip())
        status       = cols[17].strip()
        return (region, total_amount, status)
    except (IndexError, ValueError):
        return None

parsed_rdd = data_rdd.map(parse_order).filter(lambda x: x is not None)

# Keep only Completed, emit (region, total_amount)
completed_rdd = (parsed_rdd
    .filter(lambda x: x[2] == "Completed")
    .map(lambda x: (x[0], x[1])))

region_revenue_rdd = completed_rdd.reduceByKey(lambda a, b: a + b)

print("Revenue by region (RDD):")
for region, revenue in sorted(region_revenue_rdd.collect(), key=lambda x: -x[1]):
    print(f"  {region:<10} {revenue:>12,.2f}")
```

**Cross-check against Day 8:**  
The numbers must match the DataFrame output from Day 8, Task 3 (same filter, same field). If they don't, the parse logic has an off-by-one column index.

**Why `reduceByKey` instead of `groupBy` on RDD:**  
`reduceByKey` performs a partial aggregation on the map side before shuffling — only partial sums travel across the network, not raw rows. `groupByKey` shuffles every raw row. For 10,000 rows this is invisible; for 500,000 rows (full dataset) it is a ~10× difference in shuffle data volume.

---

### Task 2 – Make data skew visible with a custom partitioner

**What to do:**  
Apply a `HashPartitioner` (default) first, then a custom region-based partitioner, and compare partition sizes.

```python
from pyspark import StorageLevel

# Step 1: Default hash partitioning — observe how East records spread
default_partitioned = completed_rdd.partitionBy(5)  # 5 partitions, hash on region key
default_sizes = default_partitioned.glom().map(len).collect()
print("Default HashPartitioner sizes:", default_sizes)

# Step 2: Custom partitioner — pin each region to a specific partition
REGION_MAP = {"North": 0, "South": 1, "East": 2, "West": 3, "Central": 4}

def region_partitioner(region):
    return REGION_MAP.get(region, 0)

custom_partitioned = completed_rdd.partitionBy(5, region_partitioner)
custom_sizes = custom_partitioned.glom().map(len).collect()
print("Custom RegionPartitioner sizes:", custom_sizes)
```

**Expected output pattern:**
```
Default HashPartitioner sizes : [~1600, ~1600, ~1600, ~1600, ~1600]  # roughly even
Custom RegionPartitioner sizes: [~1450, ~1400, ~2800, ~1450, ~900]   # East (index 2) dominates
```

**Why this is the point of the exercise:**  
The custom partitioner makes data skew **physically visible** in partition row counts. East's partition has nearly 2× the work of any other partition. This directly causes one task to run 2× longer than all others — that is what you will see in the Spark UI as a "slow task". In Day 11 you will deal with it using salting.

---

### Task 3 – RDD persistence: memory vs disk, timed comparison

**What to do:**  
Take the `parsed_rdd` (before filter and map), run the same computation twice under three persistence strategies, time the second run each time.

```python
import time
from pyspark import StorageLevel

def run_twice(rdd, label):
    rdd.count()                     # first run — computes from scratch
    t0 = time.time()
    rdd.count()                     # second run — hits cache (or recomputes if no cache)
    elapsed = time.time() - t0
    print(f"  {label:<20}  second run: {elapsed:.3f}s")

# Baseline — no caching
run_twice(parsed_rdd, "No cache")

# MEMORY_ONLY — deserialized Python objects in JVM heap
mem_rdd = parsed_rdd.persist(StorageLevel.MEMORY_ONLY)
run_twice(mem_rdd, "MEMORY_ONLY")
mem_rdd.unpersist()

# DISK_ONLY — serialised to local disk, evicted from memory
disk_rdd = parsed_rdd.persist(StorageLevel.DISK_ONLY)
run_twice(disk_rdd, "DISK_ONLY")
disk_rdd.unpersist()
```

**Expected pattern (dev dataset is small, so differences are modest):**

| Strategy | Second run |
|---|---|
| No cache | ~same as first run |
| MEMORY_ONLY | noticeably faster (sub-50ms) |
| DISK_ONLY | faster than no-cache, slower than memory |

**Why the pattern matters more than the numbers:**  
On the dev dataset the absolute times are tiny. The lesson is the **shape**: cache helps only when the same RDD is reused. If you only use an RDD once, caching wastes memory and adds serialization overhead. The rule: cache when an RDD feeds multiple downstream operations — which is exactly the pattern in the batch pipeline where `orders_clean` feeds both the regional and category aggregations.

---

### Task 4 – Broadcast a lookup table to avoid a shuffle join

**What to do:**  
Load `regions.csv` as a plain Python dict and broadcast it. Use it in a map to tag each order with whether it exceeded its region's revenue target.

```python
import csv

# Load regions.csv as a Python dict — 5 rows, pure Python, negligible size
with open(f"{DATA_ROOT}/regions.csv") as f:
    reader = csv.DictReader(f)
    region_targets = {
        row["region"]: float(row["target_revenue"])
        for row in reader
    }

# Broadcast to all executors — sent once, reused on every partition
bc_targets = sc.broadcast(region_targets)

# Tag each completed order with its region's annual target
def tag_with_target(record):
    region, amount = record
    target = bc_targets.value.get(region, 1.0)
    daily_target = target / 365
    above_daily = amount > daily_target
    return (region, amount, daily_target, above_daily)

tagged_rdd = completed_rdd.map(tag_with_target)

print("Sample tagged records:")
for row in tagged_rdd.take(5):
    print(f"  region={row[0]:<8}  amount={row[1]:>8.2f}  daily_target={row[2]:>8.2f}  above={row[3]}")
```

**Why this is the right use case for broadcast:**  
`regions.csv` is 5 rows — 300 bytes. Without broadcast you would do `completed_rdd.join(regions_rdd)`, which triggers a shuffle of all 10,000 order rows. With broadcast, the 5-row dict is sent once to each executor (zero shuffle). This pattern scales: even with 500,000 orders, the region dict stays 5 rows. Day 10 will use the same pattern via the DataFrame `broadcast()` hint.

---

### Task 5 – Inspect RDD lineage

**What to do:**  
Print the debug string of the `tagged_rdd` to see the full dependency chain.

```python
print(tagged_rdd.toDebugString().decode("utf-8"))
```

**What to look for:**  
```
(N) PythonRDD[...] at RDD at PythonRDD.scala
 |  MapPartitionsRDD[...]   <-- our .map(tag_with_target)
 |  PythonRDD[...]           <-- .filter(Completed)
 |  MapPartitionsRDD[...]    <-- .map(parse_order)
 |  /opt/spark/jobs/.../orders.csv MapPartitionsRDD[...]   <-- sc.textFile
```

Now persist `tagged_rdd` and re-print the lineage:

```python
tagged_rdd.persist(StorageLevel.MEMORY_ONLY)
tagged_rdd.count()  # trigger materialisation
print(tagged_rdd.toDebugString().decode("utf-8"))
```

The lineage above the checkpoint point is still there — Spark doesn't erase it. This is how fault tolerance works: if an executor holding a cached partition dies, Spark re-executes only the lineage steps needed to recompute **that partition**, not the entire dataset.

---

### Day 9 Deliverables

| Artifact | What it looks like |
|---|---|
| RDD revenue output | 5 `(region, revenue)` tuples matching Day 8 numbers exactly |
| Partition size comparison | Default ~even vs custom with East at ~2× others |
| Timing table | 3 rows: no-cache / MEMORY_ONLY / DISK_ONLY, second-run times |
| Broadcast sample | 5 rows showing `region`, `amount`, `daily_target`, `above_daily` |
| `toDebugString` output | Full lineage chain from `sc.textFile` to `tagged_rdd` |

---

---

# Day 10 – DataFrame & Structured ETL Layer

## Goal
Build the full curated dataset by joining all four sources using the DataFrame API, apply advanced aggregations using window functions, handle dirty data properly, and write the final output as Parquet. By the end, you have a clean, enriched, analysis-ready dataset that the Day 11 optimizer and Day 12 streaming pipeline can consume.

---

### Task 1 – Load all four sources in their native formats

**What to do:**

```python
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType, BooleanType
)

# orders.csv — already defined in Day 8, reuse the schema
orders_df = (spark.read
    .option("header", "true")
    .schema(orders_schema)
    .csv(f"{DATA_ROOT}/orders.csv"))

# customers.csv
customers_schema = StructType([
    StructField("customer_id",    StringType(), True),
    StructField("customer_name",  StringType(), True),
    StructField("email",          StringType(), True),
    StructField("phone",          StringType(), True),
    StructField("region",         StringType(), True),
    StructField("city",           StringType(), True),
    StructField("segment",        StringType(), True),
    StructField("loyalty_tier",   StringType(), True),
    StructField("join_date",      StringType(), True),
    StructField("is_active",      StringType(), True),
])
customers_df = (spark.read
    .option("header", "true")
    .schema(customers_schema)
    .csv(f"{DATA_ROOT}/customers.csv"))

# products.json — JSON-lines, Spark infers schema correctly here
# (all fields are consistently typed, no dirty data)
products_df = spark.read.json(f"{DATA_ROOT}/products.json")

# regions.csv — tiny, 5 rows
regions_df = (spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(f"{DATA_ROOT}/regions.csv"))

print("Counts:", orders_df.count(), customers_df.count(),
      products_df.count(), regions_df.count())
# Expect: 10000  1000  200  5
```

**Why `inferSchema=True` is acceptable for `regions.csv` but not `orders.csv`:**  
`regions.csv` is 5 rows with zero dirty data. The schema inference cost is negligible and it correctly detects `target_revenue` as Long. `orders.csv` has 10,000 rows, dirty fields, and you need `discount` as String for the cleaning step — inference would get it wrong.

---

### Task 2 – Clean orders and join all datasets

**What to do:**  
Clean first, then join in a single chain. The join order matters: start with the largest table (orders) and join smaller tables to it.

```python
from pyspark.sql.functions import (
    col, when, broadcast, to_timestamp, coalesce, lit
)

# Step 1: Clean orders — fix dirty discount, cast, drop Cancelled/Returned
orders_clean = (orders_df
    .filter(col("status") == "Completed")
    .withColumn("discount",
        when(col("discount").isNull() | (col("discount") == ""), "0")
        .otherwise(col("discount")))
    .withColumn("discount",   col("discount").cast("double"))
    .withColumn("order_date", to_timestamp(col("order_date"), "yyyy-MM-dd HH:mm:ss")))

# Step 2: Select only what we need from lookup tables
customers_slim = customers_df.select(
    "customer_id", "segment", "loyalty_tier"
)

products_slim = products_df.select(
    "product_id", "base_price", "stock_quantity"
)

# Step 3: Join — broadcast both small tables (products=200 rows, regions=5 rows)
enriched_df = (orders_clean
    .join(broadcast(products_slim), "product_id", "left")
    .join(broadcast(customers_slim), "customer_id", "left")
    .join(broadcast(regions_df.select("region", "target_revenue")), "region", "left"))

enriched_df.printSchema()
print("Enriched row count:", enriched_df.count())
```

**Why broadcast for all three lookup tables:**  
`products_df` = 200 rows, `customers_slim` = 1,000 rows, `regions_df` = 5 rows. Default Spark broadcast threshold is 10 MB; all three are well under 1 MB. Broadcasting means zero shuffle for these joins. The `.explain()` on `enriched_df` should show `BroadcastHashJoin` nodes — not `SortMergeJoin`. This is the correct join strategy for small-large table combinations.

---

### Task 3 – Advanced aggregations with window functions

**What to do:**  
Two aggregations that go beyond a simple `groupBy`.

**A. Monthly revenue trend per region (with month-over-month change):**

```python
from pyspark.sql.functions import sum as _sum, round as _round, lag
from pyspark.sql.window import Window

monthly_region = (enriched_df
    .groupBy("region", "order_year", "order_month")
    .agg(_round(_sum("total_amount"), 2).alias("monthly_revenue"))
    .orderBy("region", "order_year", "order_month"))

# Window: for each region, look at the previous month's revenue
region_window = Window.partitionBy("region").orderBy("order_year", "order_month")

monthly_with_delta = (monthly_region
    .withColumn("prev_month_revenue", lag("monthly_revenue", 1).over(region_window))
    .withColumn("mom_change_pct",
        _round(
            (col("monthly_revenue") - col("prev_month_revenue"))
            / col("prev_month_revenue") * 100,
        1))
)

monthly_with_delta.filter(col("region") == "East").show(15)
```

**B. Category rank within each region (dense_rank):**

```python
from pyspark.sql.functions import dense_rank, count

cat_region = (enriched_df
    .groupBy("region", "category")
    .agg(
        _round(_sum("total_amount"), 2).alias("revenue"),
        count("order_id").alias("orders"),
    ))

rank_window = Window.partitionBy("region").orderBy(col("revenue").desc())

cat_ranked = (cat_region
    .withColumn("rank_in_region", dense_rank().over(rank_window))
    .filter(col("rank_in_region") <= 3)   # top 3 categories per region
    .orderBy("region", "rank_in_region"))

cat_ranked.show(20)
```

**Why window functions instead of multiple groupBy passes:**  
The month-over-month delta requires comparing the current row to the previous row within a group — that is exactly what `lag()` does. Without a window function, you would need a self-join on the aggregated table, which creates an unnecessary shuffle. Window functions compute the delta in a single pass over the already-aggregated data.

**What to observe in Nov/Dec output:**  
The dataset has 3× normal volume in November and December (seasonality built into the generator). `mom_change_pct` for October→November should show a large positive spike for all regions.

---

### Task 4 – Loyalty tier revenue analysis (structured column from a join)

**What to do:**  
The `loyalty_tier` column only exists in `customers.csv`. We brought it in via the join. Now use it.

```python
from pyspark.sql.functions import avg

loyalty_summary = (enriched_df
    .groupBy("loyalty_tier")
    .agg(
        count("order_id").alias("order_count"),
        _round(avg("total_amount"), 2).alias("avg_order_value"),
        _round(_sum("total_amount"), 2).alias("total_revenue"),
    )
    .orderBy(col("avg_order_value").desc()))

loyalty_summary.show()
```

**Expected observation:**  
Platinum customers have the highest `avg_order_value` (they are ~7% of customers but should account for a disproportionate share of high-value orders). This is a real business insight: loyalty tier correlates with transaction value, which informs marketing spend decisions.

**Why this required the join:**  
`orders.csv` does not contain `loyalty_tier`. It only has `customer_id`. The join brought the dimension in. This is the same pattern used in every star-schema data warehouse: fact table (orders) + dimension table (customers) joined at query time.

---

### Task 5 – Write curated output as Parquet

**What to do:**  
Write three output datasets, each partitioned by a meaningful column.

```python
OUTPUT_ROOT = "/opt/spark/jobs/capstone_data/dev/output"

# 1. Full enriched orders — partitioned by region for Day 11 filter pushdown demo
(enriched_df
    .write
    .mode("overwrite")
    .partitionBy("region")
    .parquet(f"{OUTPUT_ROOT}/enriched_orders.parquet"))

# 2. Monthly revenue trend
(monthly_with_delta
    .write
    .mode("overwrite")
    .parquet(f"{OUTPUT_ROOT}/monthly_revenue_trend.parquet"))

# 3. Category ranking per region
(cat_ranked
    .write
    .mode("overwrite")
    .parquet(f"{OUTPUT_ROOT}/category_rank_by_region.parquet"))

# Verify partition layout for enriched_orders
import os
for name in os.listdir(f"{OUTPUT_ROOT}/enriched_orders.parquet"):
    print(name)
# Expect: region=East/  region=North/  region=South/  region=West/  region=Central/
```

**Why partition by `region`:**  
The most common filter in Day 8–11 is `WHERE region = 'East'`. When the Parquet files are partitioned by `region`, a query with that filter reads only the `region=East/` directory — all other partitions are skipped entirely. This is partition pruning. For the dev dataset the gain is modest; for the full 500K-order dataset it becomes a ~5× read reduction for region-specific queries.

---

### Task 6 – DataFrame vs RDD: compare explain output for the same query

**What to do:**  
Run `regional_summary.explain()` (from Day 8, Task 5) and compare it to the RDD lineage printed by `toDebugString()` (from Day 9, Task 5). Both compute revenue by region. Note the differences.

**Discussion points:**

| Aspect | RDD | DataFrame |
|---|---|---|
| **Catalyst optimizer** | No — you wrote exactly what executes | Yes — rewrites the plan for efficiency |
| **Tungsten execution** | Python objects, GC pressure | Off-heap binary format, no Python overhead |
| **Filter pushdown** | Manual — you wrote `.filter()` before `.map()` | Automatic — Catalyst moves filter before scan |
| **Shuffle minimization** | `reduceByKey` does partial aggregation | AQE (Day 11) can dynamically reduce shuffle partitions |
| **When to use RDD** | Custom partitioners, low-level fault tolerance control, arbitrary Python transformations with no SQL equivalent | Avoid for standard ETL — DataFrame is always faster |

The conclusion to draw: RDD is not "lower level and therefore faster". RDD is lower level and **avoids the optimizations** that make DataFrame faster. RDD's one genuine advantage is when you need compute logic that cannot be expressed in expressions — like the custom partitioner in Day 9, Task 2.

---

### Day 10 Deliverables

| Artifact | What it looks like |
|---|---|
| Enriched orders Parquet | Partitioned by region, 5 sub-directories |
| Monthly revenue trend table | Month-over-month % change, Nov spike visible |
| Category rank table | Top 3 categories per region, 15 rows total |
| Loyalty tier table | Platinum highest avg_order_value |
| DataFrame vs RDD comparison notes | Table above, written in your own words |

---

---

## Progression Summary

| Day | What you built | Key concept proven |
|---|---|---|
| 8 | Regional + category aggregation from raw CSV | `Exchange` node = shuffle; execution plan is readable |
| 9 | Same aggregations via RDD API | Partition skew is physically visible; broadcast avoids shuffle join |
| 10 | Enriched multi-source dataset with window functions | Broadcast join strategy; partition pruning; window functions vs self-joins |

Each day builds on the last: Day 8's output Parquet is Day 10's starting point. Day 9's custom partitioner is Day 11's skew problem to solve. Day 10's enriched Parquet is Day 11's optimization target.
