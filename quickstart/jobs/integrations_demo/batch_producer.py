"""
batch_producer.py  –  Spark + HDFS + Kafka  |  Batch: Enrich & Publish
=======================================================================

What this script does
---------------------
  1. Create a raw products DataFrame in memory
  2. Enrich the data with two new columns (transformations):
       - discount_pct   : category-based discount percentage
       - discounted_price: price after discount (price * (1 - discount_pct/100))
  3. Write the enriched DataFrame to HDFS as Parquet
       (Acts as a durable raw-zone landing area)
  4. Publish each enriched row as a JSON message to a Kafka topic
       (Downstream consumers can read from here independently)

Data flow
---------
  [in-memory DataFrame]
        │
        ▼  transform: add discount_pct + discounted_price
  [enriched DataFrame]
        │
        ├──► HDFS  :  hdfs://hdfs-namenode:8020/demo/raw/products  (Parquet)
        │
        └──► Kafka :  topic = demo-products  (JSON messages)

HOW TO RUN
----------
From the quickstart/ directory:

    docker compose exec spark-master /opt/spark/bin/spark-submit \\
        --master local[2] \
        --conf spark.jars.ivy=/tmp/.ivy2 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \\
        /opt/spark/jobs/integrations_demo/batch_producer.py

Run batch_consumer_transform.py afterwards to read, transform, and aggregate.
"""

import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
HDFS_RAW_PATH = "hdfs://hdfs-namenode:8020/demo/raw/products"
KAFKA_BROKER  = "kafka-broker:29092"
KAFKA_TOPIC   = "demo-products"

# ---------------------------------------------------------------------------
# Spark session
# (HDFS address comes from spark-defaults.conf — no override needed)
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("batch_producer")
    .master("local[2]")     # local mode — no worker allocation needed for demo
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*60)
print("  PRODUCER  —  Enrich, store to HDFS, publish to Kafka")
print("="*60)

# ---------------------------------------------------------------------------
# Step 1 – Raw data
# ---------------------------------------------------------------------------
schema = StructType([
    StructField("product_id", IntegerType(), False),
    StructField("name",       StringType(),  False),
    StructField("category",   StringType(),  False),
    StructField("price",      DoubleType(),  False),
])

rows = [
    (1, "Laptop Pro 15",   "Electronics", 1299.99),
    (2, "Wireless Mouse",  "Electronics",   29.99),
    (3, "Standing Desk",   "Furniture",    499.00),
    (4, "Coffee Maker",    "Appliances",    89.95),
    (5, "Python Cookbook", "Books",         39.99),
    (6, "4K Monitor",      "Electronics",  649.00),
    (7, "Ergonomic Chair", "Furniture",    349.00),
    (8, "Blender",         "Appliances",    59.99),
]

raw_df = spark.createDataFrame(rows, schema)

print("\n[Step 1] Raw input data:")
raw_df.show(truncate=False)

# ---------------------------------------------------------------------------
# Step 2 – Transformation: add discount columns
#
# Business rule:
#   Electronics  → 15% discount  (high-margin category)
#   Furniture    → 10% discount
#   All others   → 5%  discount
#
# F.when() is Spark's equivalent of a CASE WHEN expression in SQL.
# F.round() avoids floating-point noise in the output.
# ---------------------------------------------------------------------------
enriched_df = raw_df.withColumn(
    "discount_pct",
    F.when(F.col("category") == "Electronics", 15.0)
     .when(F.col("category") == "Furniture",   10.0)
     .otherwise(5.0)
).withColumn(
    "discounted_price",
    F.round(F.col("price") * (1 - F.col("discount_pct") / 100), 2)
).withColumn(
    # ISO-8601 timestamp so downstream consumers know when this was produced
    "produced_at",
    F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")
)

print("[Step 2] After enrichment (discount_pct + discounted_price added):")
enriched_df.show(truncate=False)

# ---------------------------------------------------------------------------
# Step 3 – Write enriched data to HDFS (raw zone)
#
# Parquet is columnar and compressed — ideal for analytical workloads.
# Partition layout on HDFS:  /demo/raw/products/*.parquet
# ---------------------------------------------------------------------------
print(f"[Step 3] Writing enriched data to HDFS  →  {HDFS_RAW_PATH}")

jvm  = spark._jvm
Path = jvm.org.apache.hadoop.fs.Path
fs   = Path(HDFS_RAW_PATH).getFileSystem(spark._jsc.sc().hadoopConfiguration())
if fs.exists(Path(HDFS_RAW_PATH)):
    fs.delete(Path(HDFS_RAW_PATH), True)

enriched_df.write.mode("overwrite").parquet(HDFS_RAW_PATH)
print(f"         {enriched_df.count()} rows written  ✓")

# ---------------------------------------------------------------------------
# Step 4 – Publish to Kafka
#
# Kafka expects a "value" column.  We serialise the entire enriched row
# as a JSON string so consumers get the full enriched payload.
# The topic is recreated fresh on every run so message counts stay exact.
# ---------------------------------------------------------------------------
print(f"[Step 4] Publishing to Kafka topic '{KAFKA_TOPIC}'")

try:
    Properties  = jvm.java.util.Properties
    AdminClient = jvm.org.apache.kafka.clients.admin.AdminClient
    props = Properties()
    props.put("bootstrap.servers", KAFKA_BROKER)
    admin = AdminClient.create(props)
    if admin.listTopics().names().get().contains(KAFKA_TOPIC):
        admin.deleteTopics(
            jvm.java.util.Collections.singletonList(KAFKA_TOPIC)
        ).all().get()
        time.sleep(2)
        print("         (topic reset for clean run)")
    admin.close()
except Exception as e:
    print(f"         [note] topic reset skipped: {e}")

(
    enriched_df
    .select(F.to_json(F.struct("*")).alias("value"))
    .write
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("topic", KAFKA_TOPIC)
    .save()
)
print(f"         {len(rows)} messages published  ✓")

print("\n" + "="*60)
print("  Producer done — run demo_consumer_transform.py next")
print("="*60 + "\n")

spark.stop()
