"""
Spark SQL Demo — Core Concepts
================================
Demonstrates:
  01. SparkSession creation
  02. Writing sample JSON + CSV data locally
  03. Schema definition using StructType / StructField
  04. Reading JSON with explicit schema
  05. Reading CSV
  06. Column expressions — select, withColumn, alias
  07. Filtering rows
  08. Aggregations — groupBy, avg, count
  09. Built-in functions — when(), col()
  10. UDF — simple salary categorisation
  11. Nested JSON fields
  12. explode() on array columns
  13. Writing Parquet
  14. JDBC read (code-only, no live DB required)
  15. S3 read (code-only example)
  16. explain(True) — execution plan

Run:
  docker exec spark-master /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /opt/spark/jobs/spark_sql_demo.py
"""

import json
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg, col, count, explode, udf, when
)
from pyspark.sql.types import (
    ArrayType, DoubleType, IntegerType, StringType,
    StructField, StructType
)

SEP = "=" * 65

# ─────────────────────────────────────────────────────────────────────
# SECTION 01 — SparkSession
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 01 — SparkSession creation")
print(SEP)

spark = SparkSession.builder \
    .appName("SparkSQLDemo") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("SparkSession created. Spark version:", spark.version)


# ─────────────────────────────────────────────────────────────────────
# SECTION 02 — Write sample data files locally
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 02 — Writing sample JSON and CSV files")
print(SEP)

# /opt/spark/jobs is a Docker bind-mount shared by master AND all workers
# (defined in docker-compose.yml: ./jobs:/opt/spark/jobs).
# Files written here are visible to every executor — unlike /tmp which is
# local to each container and NOT shared across the cluster.
DATA_DIR = "/opt/spark/jobs/spark_sql_demo_data"
os.makedirs(DATA_DIR, exist_ok=True)

# Small JSON dataset — employees with nested dept and skills array
employees = [
    {
        "id": 1, "name": "Alice",
        "department": {"name": "Engineering", "floor": 3},
        "skills": ["Python", "Spark", "SQL"],
        "salary": 95000
    },
    {
        "id": 2, "name": "Bob",
        "department": {"name": "Marketing", "floor": 2},
        "skills": ["Excel", "SQL"],
        "salary": 72000
    },
    {
        "id": 3, "name": "Carol",
        "department": {"name": "Engineering", "floor": 3},
        "skills": ["Java", "Spark"],
        "salary": 105000
    },
    {
        "id": 4, "name": "Dave",
        "department": {"name": "HR", "floor": 1},
        "skills": ["Communication"],
        "salary": 65000
    },
    {
        "id": 5, "name": "Eve",
        "department": {"name": "Marketing", "floor": 2},
        "skills": ["SQL", "Tableau", "Excel"],
        "salary": 78000
    },
]

json_path = os.path.join(DATA_DIR, "employees.json")
with open(json_path, "w") as f:
    for emp in employees:
        f.write(json.dumps(emp) + "\n")   # one JSON object per line

# Small CSV dataset — regional sales figures
csv_path = os.path.join(DATA_DIR, "sales.csv")
csv_content = """region,product,units,revenue
North,Laptop,10,12000
North,Phone,25,7500
South,Laptop,8,9600
South,Phone,30,9000
East,Laptop,12,14400
East,Tablet,15,6000
West,Phone,20,6000
West,Tablet,10,4000
"""
with open(csv_path, "w") as f:
    f.write(csv_content)

print(f"JSON written → {json_path}")
print(f"CSV  written → {csv_path}")


# ─────────────────────────────────────────────────────────────────────
# SECTION 03 — Schema definition using StructType
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 03 — Explicit schema with StructType / StructField")
print(SEP)

# Define the dept sub-struct
dept_schema = StructType([
    StructField("name",  StringType(),  nullable=False),
    StructField("floor", IntegerType(), nullable=True),
])

# Top-level employee schema
employee_schema = StructType([
    StructField("id",         IntegerType(),             nullable=False),
    StructField("name",       StringType(),              nullable=False),
    StructField("department", dept_schema,               nullable=True),
    StructField("skills",     ArrayType(StringType()),   nullable=True),
    StructField("salary",     DoubleType(),              nullable=True),
])

print("Schema defined:")
# StructType has no printTreeString() in Python — create an empty DataFrame
# with the schema and call printSchema() instead (same tree output)
spark.createDataFrame([], employee_schema).printSchema()


# ─────────────────────────────────────────────────────────────────────
# SECTION 04 — Reading JSON with explicit schema
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 04 — Reading JSON")
print(SEP)

# Spark reads JSON natively; we pass our explicit schema for type safety
emp_df = spark.read \
    .schema(employee_schema) \
    .json(json_path)

emp_df.printSchema()
emp_df.show(truncate=False)


# ─────────────────────────────────────────────────────────────────────
# SECTION 05 — Reading CSV
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 05 — Reading CSV")
print(SEP)

# header=True uses first row as column names; inferSchema=True auto-detects types
sales_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_path)

sales_df.printSchema()
sales_df.show()


# ─────────────────────────────────────────────────────────────────────
# SECTION 06 — Column expressions: select, withColumn, alias
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 06 — select / withColumn / alias")
print(SEP)

# select — choose and rename columns
selected_df = emp_df.select(
    col("id"),
    col("name").alias("employee_name"),     # rename with alias
    col("salary"),
)
print("→ select + alias:")
selected_df.show()

# withColumn — add or replace a column
# Add a bonus column = 10 % of salary
with_bonus_df = emp_df.withColumn(
    "bonus",
    (col("salary") * 0.10).cast(DoubleType())
)
print("→ withColumn (bonus = 10% of salary):")
with_bonus_df.select("name", "salary", "bonus").show()


# ─────────────────────────────────────────────────────────────────────
# SECTION 07 — Filtering
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 07 — Filtering rows")
print(SEP)

# Filter employees earning more than 80 000
high_earners = emp_df.filter(col("salary") > 80000)
print("→ Employees with salary > 80 000:")
high_earners.select("name", "salary").show()

# Filter sales from the North region with revenue > 8 000
north_high = sales_df.filter(
    (col("region") == "North") & (col("revenue") > 8000)
)
print("→ North region sales with revenue > 8 000:")
north_high.show()


# ─────────────────────────────────────────────────────────────────────
# SECTION 08 — Aggregations: groupBy, avg, count
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 08 — groupBy / avg / count")
print(SEP)

# Average revenue and number of products sold per region
region_agg = sales_df.groupBy("region") \
    .agg(
        avg("revenue").alias("avg_revenue"),
        count("product").alias("num_products"),
    ) \
    .orderBy("region")

print("→ Sales aggregation by region:")
region_agg.show()


# ─────────────────────────────────────────────────────────────────────
# SECTION 09 — Built-in functions: when() and col()
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 09 — when() / col() built-in functions")
print(SEP)

# Label each employee's salary range using when() (like SQL CASE WHEN)
labeled_df = emp_df.withColumn(
    "salary_band",
    when(col("salary") >= 100000, "High")
    .when(col("salary") >= 80000,  "Mid")
    .otherwise("Entry")
)
print("→ Salary band classification with when():")
labeled_df.select("name", "salary", "salary_band").show()


# ─────────────────────────────────────────────────────────────────────
# SECTION 10 — UDF: user-defined function
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 10 — UDF (salary categorisation)")
print(SEP)

# Define a plain Python function
def salary_category(salary):
    """Returns a human-readable salary tier string."""
    if salary is None:
        return "Unknown"
    if salary >= 100000:
        return "Senior"
    if salary >= 75000:
        return "Mid-Level"
    return "Junior"

# Wrap it as a Spark UDF
salary_category_udf = udf(salary_category, StringType())

# Apply the UDF as a new column
udf_df = emp_df.withColumn("level", salary_category_udf(col("salary")))
print("→ Salary level assigned by UDF:")
udf_df.select("name", "salary", "level").show()


# ─────────────────────────────────────────────────────────────────────
# SECTION 11 — Nested JSON fields
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 11 — Accessing nested JSON fields (dot notation)")
print(SEP)

# Access nested struct fields with dot notation
nested_df = emp_df.select(
    col("name"),
    col("department.name").alias("dept_name"),   # nested field
    col("department.floor").alias("dept_floor"),
)
print("→ Nested department fields extracted:")
nested_df.show()


# ─────────────────────────────────────────────────────────────────────
# SECTION 12 — explode() on array columns
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 12 — explode() — one row per array element")
print(SEP)

# explode() turns each element of the array into its own row
exploded_df = emp_df.select(
    col("name"),
    explode(col("skills")).alias("skill"),   # one row per skill
)
print("→ Each skill as a separate row after explode():")
exploded_df.show()


# ─────────────────────────────────────────────────────────────────────
# SECTION 13 — Writing Parquet
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 13 — Writing Parquet")
print(SEP)

parquet_path = os.path.join(DATA_DIR, "employees_parquet")

# mode("overwrite") replaces any existing data at that path
emp_df.write.mode("overwrite").parquet(parquet_path)
print(f"→ Parquet written to: {parquet_path}")

# Read it back to confirm the round-trip
parquet_df = spark.read.parquet(parquet_path)
print("→ Parquet read back:")
parquet_df.select("id", "name", "salary").show()


# ─────────────────────────────────────────────────────────────────────
# SECTION 14 — JDBC read (code-only, no live database required)
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 14 — JDBC read (code template — no live DB needed)")
print(SEP)

print("""
  # To read from a PostgreSQL table, you would write:
  #
  # jdbc_df = spark.read \\
  #     .format("jdbc") \\
  #     .option("url",      "jdbc:postgresql://localhost:5432/mydb") \\
  #     .option("dbtable",  "public.employees") \\
  #     .option("user",     "postgres") \\
  #     .option("password", "secret") \\
  #     .option("driver",   "org.postgresql.Driver") \\
  #     .load()
  #
  # jdbc_df.show()
  #
  # The JDBC JAR must be on the classpath:
  #   spark-submit --jars /path/to/postgresql-42.x.x.jar spark_sql_demo.py
""")


# ─────────────────────────────────────────────────────────────────────
# SECTION 15 — Reading from S3 (code-only example)
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 15 — S3 read (code template — no AWS credentials needed)")
print(SEP)

print("""
  # To read a CSV from S3, you would write:
  #
  # s3_df = spark.read \\
  #     .option("header", "true") \\
  #     .option("inferSchema", "true") \\
  #     .csv("s3a://my-bucket/data/sales.csv")
  #
  # Prerequisites:
  #   - spark-submit with hadoop-aws and aws-java-sdk JARs on classpath
  #   - SparkConf keys:
  #       spark.hadoop.fs.s3a.access.key  = <AWS_ACCESS_KEY>
  #       spark.hadoop.fs.s3a.secret.key  = <AWS_SECRET_KEY>
  #   OR use an IAM instance role when running on EC2 / EMR.
""")


# ─────────────────────────────────────────────────────────────────────
# SECTION 16 — explain(True) — execution plan
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  SECTION 16 — explain(True) — Spark execution plan")
print(SEP)

# explain(True) prints the full plan: parsed → analysed → optimised → physical
print("→ Execution plan for the region aggregation query:")
region_agg.explain(True)


# ─────────────────────────────────────────────────────────────────────
# Done
# ─────────────────────────────────────────────────────────────────────
print(f"\n{SEP}")
print("  All sections complete.")
print(SEP)

spark.stop()
