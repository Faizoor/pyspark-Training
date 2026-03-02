# Spark SQL — Core Concepts Demo Guide

> **Spark version:** 4.1.1  
> **Cluster:** standalone, 1 master + 2 workers (2 cores / 2 GB each)  
> **Script:** `quickstart/jobs/spark_sql_demo.py`  
> **Data:** created inline by the script into `/opt/spark/jobs/spark_sql_demo_data/` (shared Docker volume, visible to all workers)

---

## Project Structure

```
quickstart/
└── jobs/
    └── spark_sql_demo.py      ← single demo script (all 16 sections)

labs/
└── spark_sql_demo_guide.md    ← this guide
```

---

## Prerequisites

The Docker cluster must be running:

```bash
cd quickstart
docker compose up -d
```

Verify containers are healthy:

```bash
docker ps --format "table {{.Names}}\t{{.Status}}"
```

Expected output:

```
NAMES           STATUS
spark-master    Up X minutes (healthy)
spark-worker-1  Up X minutes (healthy)
spark-worker-2  Up X minutes (healthy)
```

---

## How to Run

```bash
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark/jobs/spark_sql_demo.py
```

---

## Concepts Covered

| # | Concept |
|---|---------|
| 01 | SparkSession creation |
| 02 | Writing sample JSON + CSV locally |
| 03 | Schema definition — StructType / StructField |
| 04 | Reading JSON with explicit schema |
| 05 | Reading CSV |
| 06 | Column expressions — select, withColumn, alias |
| 07 | Filtering |
| 08 | Aggregations — groupBy, avg, count |
| 09 | Built-in functions — when(), col() |
| 10 | UDF — user-defined function |
| 11 | Nested JSON fields |
| 12 | explode() on arrays |
| 13 | Writing Parquet |
| 14 | JDBC read (code template) |
| 15 | S3 read (code template) |
| 16 | explain(True) — execution plan |

---

## Section-by-Section Walkthrough

---

### Section 01 — SparkSession Creation

**What it demonstrates:**  
The entry point for every Spark application. `SparkSession.builder` configures the session;
`.getOrCreate()` creates a new session or reuses an existing one.

```python
spark = SparkSession.builder \
    .appName("SparkSQLDemo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
```

`setLogLevel("WARN")` suppresses `INFO` log noise so demo output stays clean.

**Expected output:**
```
SparkSession created. Spark version: 4.1.1
```

---

### Section 02 — Writing Sample Data Files Locally

**What it demonstrates:**  
Generating small, self-contained test data without external dependencies.  
The script writes a JSON file (one object per line — Spark's default JSON format)
and a CSV file to `/opt/spark/jobs/spark_sql_demo_data/` using plain Python.

> **Why not `/tmp`?**  
> In a multi-node cluster, each container (master + workers) has its own isolated
> `/tmp`. The driver writes the file on the master, but workers run on separate
> containers and look for the file on *their* local `/tmp` — where it doesn't exist.  
> `/opt/spark/jobs` is a **Docker bind-mount** defined in `docker-compose.yml`
> (`./jobs:/opt/spark/jobs`) that is shared by **all** containers, so every
> worker can read any file written there by the driver.

**Sample JSON record:**
```json
{"id": 1, "name": "Alice", "department": {"name": "Engineering", "floor": 3}, "skills": ["Python", "Spark", "SQL"], "salary": 95000}
```

**Sample CSV:**
```
region,product,units,revenue
North,Laptop,10,12000
North,Phone,25,7500
...
```

**Expected output:**
```
JSON written → /opt/spark/jobs/spark_sql_demo_data/employees.json
CSV  written → /opt/spark/jobs/spark_sql_demo_data/sales.csv
```

---

### Section 03 — Schema Definition with StructType

**What it demonstrates:**  
Defining a schema explicitly instead of relying on `inferSchema`.  
Explicit schemas are **faster** (no inference scan), **safer** (type guarantees),
and handle nested structures and arrays correctly.

```python
dept_schema = StructType([
    StructField("name",  StringType(),  nullable=False),
    StructField("floor", IntegerType(), nullable=True),
])

employee_schema = StructType([
    StructField("id",         IntegerType(),           nullable=False),
    StructField("name",       StringType(),            nullable=False),
    StructField("department", dept_schema,             nullable=True),
    StructField("skills",     ArrayType(StringType()), nullable=True),
    StructField("salary",     DoubleType(),            nullable=True),
])

# StructType has no printTreeString() in Python.
# Create an empty DataFrame with the schema and call printSchema() instead.
print("Schema defined:")
spark.createDataFrame([], employee_schema).printSchema()
```

**Expected output:**
```
Schema defined:
root
 |-- id: integer (nullable = false)
 |-- name: string (nullable = false)
 |-- department: struct (nullable = true)
 |    |-- name: string (nullable = false)
 |    |-- floor: integer (nullable = true)
 |-- skills: array (nullable = true)
 |    |-- element: string (containsNull = true)
 |-- salary: double (nullable = true)
```

---

### Section 04 — Reading JSON

**What it demonstrates:**  
Reading newline-delimited JSON with Spark. The explicit schema (from Section 03)
is passed via `.schema(employee_schema)` — Spark will not re-scan the file for types.

```python
emp_df = spark.read \
    .schema(employee_schema) \
    .json(json_path)

emp_df.printSchema()
emp_df.show(truncate=False)
```

**Expected output:**
```
+---+-----+------------------------+---------------------+-------+
|id |name |department              |skills               |salary |
+---+-----+------------------------+---------------------+-------+
|1  |Alice|{Engineering, 3}        |[Python, Spark, SQL] |95000.0|
|2  |Bob  |{Marketing, 2}          |[Excel, SQL]         |72000.0|
|3  |Carol|{Engineering, 3}        |[Java, Spark]        |105000.|
|4  |Dave |{HR, 1}                 |[Communication]      |65000.0|
|5  |Eve  |{Marketing, 2}          |[SQL, Tableau, Excel]|78000.0|
+---+-----+------------------------+---------------------+-------+
```

---

### Section 05 — Reading CSV

**What it demonstrates:**  
Reading a CSV with `header=true` (treat first row as column names) and
`inferSchema=true` (auto-detect column types).

```python
sales_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_path)
```

**Expected output:**
```
root
 |-- region: string (nullable = true)
 |-- product: string (nullable = true)
 |-- units: integer (nullable = true)
 |-- revenue: integer (nullable = true)

+------+-------+-----+-------+
|region|product|units|revenue|
+------+-------+-----+-------+
|North |Laptop |   10|  12000|
|North |Phone  |   25|   7500|
...
+------+-------+-----+-------+
```

---

### Section 06 — Column Expressions: select / withColumn / alias

**What it demonstrates:**  
Three fundamental column manipulation patterns used in every Spark job.

| Method | Purpose |
|--------|---------|
| `select()` | Choose a subset of columns; can transform inline |
| `alias()` | Rename a column |
| `withColumn()` | Add a new column or replace an existing one |

```python
# alias — rename during select
selected_df = emp_df.select(
    col("id"),
    col("name").alias("employee_name"),
    col("salary"),
)

# withColumn — compute a new bonus column
with_bonus_df = emp_df.withColumn(
    "bonus",
    (col("salary") * 0.10).cast(DoubleType())
)
```

**Expected output — select + alias:**
```
+---+-------------+-------+
| id|employee_name| salary|
+---+-------------+-------+
|  1|        Alice|95000.0|
|  2|          Bob|72000.0|
...
```

**Expected output — withColumn (bonus):**
```
+-----+-------+------+
| name| salary| bonus|
+-----+-------+------+
|Alice|95000.0|9500.0|
|  Bob|72000.0|7200.0|
...
```

---

### Section 07 — Filtering

**What it demonstrates:**  
Row-level filtering using `filter()` (equivalent to SQL `WHERE`).  
Multiple conditions combine with `&` (AND) or `|` (OR) — always wrap each
condition in parentheses.

```python
# Single condition
high_earners = emp_df.filter(col("salary") > 80000)

# Compound condition
north_high = sales_df.filter(
    (col("region") == "North") & (col("revenue") > 8000)
)
```

**Expected output — salary > 80 000:**
```
+-----+--------+
| name|  salary|
+-----+--------+
|Alice| 95000.0|
|Carol|105000.0|
+-----+--------+
```

**Expected output — North + revenue > 8 000:**
```
+------+-------+-----+-------+
|region|product|units|revenue|
+------+-------+-----+-------+
| North| Laptop|   10|  12000|
+------+-------+-----+-------+
```

---

### Section 08 — Aggregations: groupBy / avg / count

**What it demonstrates:**  
`groupBy()` groups rows by one or more columns.  
`agg()` applies multiple aggregate functions at once.  
Common aggregates: `avg()`, `count()`, `sum()`, `min()`, `max()`.

```python
region_agg = sales_df.groupBy("region") \
    .agg(
        avg("revenue").alias("avg_revenue"),
        count("product").alias("num_products"),
    ) \
    .orderBy("region")
```

**Expected output:**
```
+------+-----------+------------+
|region|avg_revenue|num_products|
+------+-----------+------------+
|  East|    10200.0|           2|
| North|     9750.0|           2|
| South|     9300.0|           2|
|  West|     5000.0|           2|
+------+-----------+------------+
```

---

### Section 09 — Built-in Functions: when() and col()

**What it demonstrates:**  
`when()` is Spark's equivalent of SQL `CASE WHEN … THEN … ELSE … END`.  
`col()` is the standard way to reference a column by name inside expressions.

```python
labeled_df = emp_df.withColumn(
    "salary_band",
    when(col("salary") >= 100000, "High")
    .when(col("salary") >= 80000,  "Mid")
    .otherwise("Entry")
)
```

**Expected output:**
```
+-----+--------+-----------+
| name|  salary|salary_band|
+-----+--------+-----------+
|Alice| 95000.0|        Mid|
|  Bob| 72000.0|      Entry|
|Carol|105000.0|       High|
| Dave| 65000.0|      Entry|
|  Eve| 78000.0|        Mid|
+-----+--------+-----------+
```

---

### Section 10 — UDF (User-Defined Function)

**What it demonstrates:**  
A UDF lets you apply any Python function as a Spark column transformation.  
Use UDFs only when no built-in function can do the job — they are slower
than built-ins because each row crosses the JVM ↔ Python boundary.

```python
def salary_category(salary):
    if salary is None:   return "Unknown"
    if salary >= 100000: return "Senior"
    if salary >= 75000:  return "Mid-Level"
    return "Junior"

salary_category_udf = udf(salary_category, StringType())

udf_df = emp_df.withColumn("level", salary_category_udf(col("salary")))
```

**Expected output:**
```
+-----+--------+---------+
| name|  salary|    level|
+-----+--------+---------+
|Alice| 95000.0|Mid-Level|
|  Bob| 72000.0|   Junior|
|Carol|105000.0|   Senior|
| Dave| 65000.0|   Junior|
|  Eve| 78000.0|Mid-Level|
+-----+--------+---------+
```

> **Tip:** Prefer `when()` / `col()` (Section 09) over UDFs for simple
> conditional logic — it stays in the JVM and is significantly faster.

---

### Section 11 — Nested JSON Fields

**What it demonstrates:**  
Spark stores nested JSON objects as `StructType` columns.  
You access sub-fields using **dot notation** inside `col()`.

```python
nested_df = emp_df.select(
    col("name"),
    col("department.name").alias("dept_name"),
    col("department.floor").alias("dept_floor"),
)
```

**Expected output:**
```
+-----+-----------+----------+
| name|  dept_name|dept_floor|
+-----+-----------+----------+
|Alice|Engineering|         3|
|  Bob|  Marketing|         2|
|Carol|Engineering|         3|
| Dave|         HR|         1|
|  Eve|  Marketing|         2|
+-----+-----------+----------+
```

---

### Section 12 — explode() on Array Columns

**What it demonstrates:**  
`explode()` unnests an array column — each element becomes its own row.  
The non-array columns (e.g. `name`) are repeated for each element.

```python
exploded_df = emp_df.select(
    col("name"),
    explode(col("skills")).alias("skill"),
)
```

**Expected output:**
```
+-----+-------------+
| name|        skill|
+-----+-------------+
|Alice|       Python|
|Alice|        Spark|
|Alice|          SQL|
|  Bob|        Excel|
|  Bob|          SQL|
|Carol|         Java|
|Carol|        Spark|
| Dave|Communication|
|  Eve|          SQL|
|  Eve|      Tableau|
|  Eve|        Excel|
+-----+-------------+
```

---

### Section 13 — Writing Parquet

**What it demonstrates:**  
Parquet is the recommended columnar storage format for Spark.  
`mode("overwrite")` replaces any existing data at the target path.  
Spark writes multiple part files in the output directory (one per partition).

```python
parquet_path = "/opt/spark/jobs/spark_sql_demo_data/employees_parquet"

emp_df.write.mode("overwrite").parquet(parquet_path)

# Read back to verify
parquet_df = spark.read.parquet(parquet_path)
parquet_df.select("id", "name", "salary").show()
```

**Expected output:**
```
Parquet written to: /opt/spark/jobs/spark_sql_demo_data/employees_parquet

+---+-----+--------+
| id| name|  salary|
+---+-----+--------+
|  1|Alice| 95000.0|
|  2|  Bob| 72000.0|
|  3|Carol|105000.0|
|  4| Dave| 65000.0|
|  5|  Eve| 78000.0|
+---+-----+--------+
```

> Parquet files are not human-readable. Use `spark.read.parquet()` or
> tools like `parquet-tools` to inspect them.

---

### Section 14 — JDBC Read (Code Template)

**What it demonstrates:**  
How to read a database table directly into a Spark DataFrame over JDBC.  
This section is a **code template only** — no live database is required.

```python
jdbc_df = spark.read \
    .format("jdbc") \
    .option("url",      "jdbc:postgresql://localhost:5432/mydb") \
    .option("dbtable",  "public.employees") \
    .option("user",     "postgres") \
    .option("password", "secret") \
    .option("driver",   "org.postgresql.Driver") \
    .load()
```

Key points:
- The JDBC driver JAR must be available to Spark: `spark-submit --jars postgresql-42.x.jar`
- Replace `url`, `dbtable`, `user`, and `password` with real values
- Use `numPartitions` + `partitionColumn` options to read in parallel (important for large tables)

---

### Section 15 — S3 Read (Code Template)

**What it demonstrates:**  
How to read files from Amazon S3 using the `s3a://` protocol.  
This section is a **code template only** — no AWS credentials are required.

```python
s3_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("s3a://my-bucket/data/sales.csv")
```

Key points:
- Add `hadoop-aws` and `aws-java-sdk` JARs to the classpath
- Configure credentials via SparkConf or IAM role:
  ```
  spark.hadoop.fs.s3a.access.key = <AWS_ACCESS_KEY>
  spark.hadoop.fs.s3a.secret.key = <AWS_SECRET_KEY>
  ```
- On EMR or EC2 with an instance role, no explicit keys are needed

---

### Section 16 — explain(True) — Execution Plan

**What it demonstrates:**  
`explain(True)` prints the full query plan at every stage of Spark's optimizer:

| Plan level | What it shows |
|------------|---------------|
| **Parsed Logical Plan** | Direct translation of your Python code into AST |
| **Analyzed Logical Plan** | Column names and types resolved |
| **Optimized Logical Plan** | After Catalyst optimizer (predicate pushdown, column pruning, etc.) |
| **Physical Plan** | Actual execution strategy chosen by Spark |

```python
region_agg.explain(True)
```

Look for `HashAggregate`, `Exchange` (shuffle), and `FileScan` in the physical plan
to understand where the heavy work happens.

---

## Full Run Checklist

| Step | Command |
|------|---------|
| Pre-create data dir (once) | `mkdir -p quickstart/jobs/spark_sql_demo_data && chmod o+w quickstart/jobs/spark_sql_demo_data` |
| Start cluster | `cd quickstart && docker compose up -d` |
| Run demo | `docker exec spark-master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/jobs/spark_sql_demo.py` |
| Open Spark UI | http://localhost:8080 |
| Inspect output files | `docker exec spark-master ls /opt/spark/jobs/spark_sql_demo_data/` |

---

## Common Errors

| Error | Likely Cause | Fix |
|-------|-------------|-----|
| `PermissionError: [Errno 13] Permission denied` | `spark_sql_demo_data/` exists in bind-mount but `spark` user (uid 185) has no write permission | Run `chmod o+w quickstart/jobs/spark_sql_demo_data` on the **host** |
| `AnalysisException: Path does not exist` | Data dir not pre-created on host before container start | Run `mkdir -p quickstart/jobs/spark_sql_demo_data && chmod o+w quickstart/jobs/spark_sql_demo_data` |
| `WARN: No master set` | Missing `--master` flag | Always pass `--master spark://spark-master:7077` |
| `ClassNotFoundException: org.postgresql.Driver` | JDBC JAR not on classpath | Add `--jars /path/to/postgresql.jar` to spark-submit |
| UDF returns `null` for all rows | Return type mismatch | Ensure the UDF's declared return type matches the Python function's output |

---

## Key Takeaways

- Always define schemas explicitly for production jobs — never rely on `inferSchema` for large datasets.
- Prefer built-in functions (`when`, `col`, `avg`) over UDFs for better performance.
- `explode()` is the standard way to flatten array columns into rows.
- `explain(True)` is your first debugging tool when a query is slow — look for unexpected shuffles (`Exchange`).
- Parquet is the go-to output format: columnar, compressed, and schema-aware.
