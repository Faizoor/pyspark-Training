"""
batch_consumer_transform.py  –  Spark + Kafka + HDFS  |  Batch: Consume & Transform
=====================================================================================

What this script does
---------------------
  1. Consume enriched product messages from the Kafka topic (batch read)
  2. Parse the JSON payload and apply further transformations:
       - price_tier  : classify each product as budget / mid-range / premium
       - savings      : how much money is saved per unit (price - discounted_price)
  3. Aggregate by category:
       - product_count  : number of products in this category
       - avg_price      : average original price
       - avg_discount   : average discount percentage
       - total_savings  : total savings across all products in category
  4. Write the transformed rows  to HDFS  →  /demo/transformed/products
  5. Write the aggregation results to HDFS  →  /demo/aggregated/by_category
  6. Print both results to the console

Data flow
---------
  Kafka : topic = demo-products
        │
        ▼  parse JSON + add price_tier + savings
  [transformed DataFrame]
        │
        ├──► HDFS  :  /demo/transformed/products   (row-level output)
        │
        ▼  groupBy category
  [aggregated DataFrame]
        │
        └──► HDFS  :  /demo/aggregated/by_category  (summary output)

HOW TO RUN
----------
From the quickstart/ directory (run demo_producer.py first):

    docker compose exec spark-master /opt/spark/bin/spark-submit \\
        --master local[2] \
        --conf spark.jars.ivy=/tmp/.ivy2 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \\
        /opt/spark/jobs/integrations_demo/batch_consumer_transform.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BROKER       = "kafka-broker:29092"
KAFKA_TOPIC        = "demo-products"
HDFS_TRANSFORMED   = "hdfs://hdfs-namenode:8020/demo/transformed/products"
HDFS_AGGREGATED    = "hdfs://hdfs-namenode:8020/demo/aggregated/by_category"

# Schema must match what the producer serialised into JSON
producer_schema = StructType([
    StructField("product_id",       IntegerType(), True),
    StructField("name",             StringType(),  True),
    StructField("category",         StringType(),  True),
    StructField("price",            DoubleType(),  True),
    StructField("discount_pct",     DoubleType(),  True),
    StructField("discounted_price", DoubleType(),  True),
    StructField("produced_at",      StringType(),  True),
])

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("batch_consumer_transform")
    .master("local[2]")     # local mode — no worker allocation needed for demo
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*60)
print("  CONSUMER  —  Read from Kafka, Transform, Store to HDFS")
print("="*60)

# ---------------------------------------------------------------------------
# Step 1 – Consume from Kafka (batch read)
#
# startingOffsets=earliest reads all messages from the beginning of the topic.
# In a production streaming pipeline, use readStream instead of read.
# The raw Kafka "value" column arrives as binary — we cast it to string first.
# ---------------------------------------------------------------------------
print(f"\n[Step 1] Reading messages from Kafka topic '{KAFKA_TOPIC}'")

raw_kafka = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

# Parse the JSON value column using the producer's schema
parsed_df = (
    raw_kafka
    .select(
        F.col("offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), producer_schema).alias("d")
    )
    .select("offset", "kafka_timestamp", "d.*")
)

msg_count = parsed_df.count()
print(f"         {msg_count} messages received from Kafka:")
parsed_df.select("product_id", "name", "category", "price",
                 "discount_pct", "discounted_price").show(truncate=False)

# ---------------------------------------------------------------------------
# Step 2 – Transform: add price_tier and savings
#
# price_tier classifies each product so analysts can filter easily:
#   budget     → price < 50
#   mid-range  → 50 ≤ price < 500
#   premium    → price ≥ 500
#
# savings shows exactly how much a customer saves per unit — useful for
# marketing ("Save $195 on this laptop!") or inventory decisions.
# ---------------------------------------------------------------------------
print("[Step 2] Applying transformations: price_tier + savings")

transformed_df = parsed_df.withColumn(
    "price_tier",
    F.when(F.col("price") < 50,   "budget")
     .when(F.col("price") < 500,  "mid-range")
     .otherwise("premium")
).withColumn(
    "savings",
    F.round(F.col("price") - F.col("discounted_price"), 2)
).withColumn(
    "consumed_at",
    F.date_format(F.current_timestamp(), "yyyy-MM-dd HH:mm:ss")
)

transformed_df.select(
    "product_id", "name", "price", "discounted_price",
    "savings", "price_tier"
).show(truncate=False)

# ---------------------------------------------------------------------------
# Step 3 – Write transformed rows to HDFS
#
# This becomes the "curated zone" — enriched, clean, ready for BI tools.
# ---------------------------------------------------------------------------
print(f"[Step 3] Writing transformed rows to HDFS  →  {HDFS_TRANSFORMED}")

jvm  = spark._jvm
Path = jvm.org.apache.hadoop.fs.Path
fs   = Path(HDFS_TRANSFORMED).getFileSystem(spark._jsc.sc().hadoopConfiguration())
if fs.exists(Path(HDFS_TRANSFORMED)):
    fs.delete(Path(HDFS_TRANSFORMED), True)

transformed_df.write.mode("overwrite").parquet(HDFS_TRANSFORMED)
print(f"         {transformed_df.count()} rows written  ✓")

# ---------------------------------------------------------------------------
# Step 4 – Aggregate by category
#
# GroupBy + agg is a classic wide transformation — it triggers a shuffle
# (data moves between partitions) so you can see a Stage boundary in the
# Spark UI at http://localhost:4040.
#
# Metrics per category:
#   product_count : how many products fall in this category
#   avg_price     : mean original price
#   avg_discount  : mean discount percentage
#   total_savings : sum of (price - discounted_price) across the category
# ---------------------------------------------------------------------------
print("[Step 4] Aggregating by category  (triggers shuffle — check Spark UI)")

summary_df = (
    transformed_df
    .groupBy("category")
    .agg(
        F.count("product_id")              .alias("product_count"),
        F.round(F.avg("price"),         2) .alias("avg_price"),
        F.round(F.avg("discount_pct"),  1) .alias("avg_discount_pct"),
        F.round(F.sum("savings"),       2) .alias("total_savings"),
    )
    .orderBy(F.col("avg_price").desc())
)

print("\n  Category summary:")
summary_df.show(truncate=False)

# ---------------------------------------------------------------------------
# Step 5 – Write aggregation results to HDFS
#
# Stored separately from row-level data so dashboards can read a tiny file
# instead of scanning all rows.
# ---------------------------------------------------------------------------
print(f"[Step 5] Writing aggregation to HDFS  →  {HDFS_AGGREGATED}")

fs2 = Path(HDFS_AGGREGATED).getFileSystem(spark._jsc.sc().hadoopConfiguration())
if fs2.exists(Path(HDFS_AGGREGATED)):
    fs2.delete(Path(HDFS_AGGREGATED), True)

summary_df.write.mode("overwrite").parquet(HDFS_AGGREGATED)
print(f"         {summary_df.count()} category rows written  ✓")

# ---------------------------------------------------------------------------
# Done
# ---------------------------------------------------------------------------
print("\n  HDFS paths written:")
print(f"    transformed  →  {HDFS_TRANSFORMED}")
print(f"    aggregated   →  {HDFS_AGGREGATED}")
print(f"    browse at    →  http://localhost:9870")
print("\n" + "="*60)
print("  Consumer done — pipeline complete!")
print("="*60 + "\n")

spark.stop()
