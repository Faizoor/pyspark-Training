"""
streaming_producer.py  –  Spark + Kafka  |  Streaming: Continuous Order Producer
==================================================================================

What this script does
---------------------
This script simulates a long-running order ingestion service that continuously
publishes new order events to a Kafka topic.

  Phase 1 – Seed bulk (instant)
    Publishes 20 initial orders covering all product categories.
    These give the streaming consumer something to process right away.

  Phase 2 – Continuous stream (loops for ~90 seconds)
    Every 4 seconds, generates 3–5 random new orders and publishes them.
    This mimics a real e-commerce order stream arriving over time.

Data model — each Kafka message carries one order as JSON:
  {
    "order_id"     : int,    unique order identifier
    "customer_id"  : str,    e.g. "CUST-042"
    "product_id"   : int,    references the product catalog
    "product_name" : str,
    "category"     : str,    Electronics | Furniture | Appliances | Books
    "quantity"     : int,    1–5 units
    "unit_price"   : float,  original list price
    "order_ts"     : str,    ISO-8601 event timestamp
  }

Data flow
---------
  [Python random data generator]
        │  createDataFrame() per micro-batch
        ▼
  [Spark DataFrame  (in-memory)]
        │  to_json → "value" column
        ▼
  Kafka topic: stream-orders

HOW TO RUN
----------
From the quickstart/ directory:

  Terminal 1 – start the consumer FIRST (so it catches all messages):

    docker compose exec spark-master /opt/spark/bin/spark-submit \\
        --master local[2] \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
        /opt/spark/jobs/integrations_demo/streaming_consumer.py

  Terminal 2 – then start the producer:

    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master local[2] \
        --conf spark.jars.ivy=/tmp/.ivy2 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \\
        /opt/spark/jobs/integrations_demo/streaming_producer.py
"""

import time
import random
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BROKER  = "kafka-broker:29092"
KAFKA_TOPIC   = "stream-orders"

LOOP_INTERVAL_SECS  = 4     # pause between micro-batches
LOOP_ITERATIONS     = 20    # total streaming phases  (~80 seconds)
ORDERS_PER_LOOP     = 4     # orders published each iteration

# ---------------------------------------------------------------------------
# Product catalog (used to generate realistic order events)
# ---------------------------------------------------------------------------
CATALOG = [
    (1,  "Laptop Pro 15",    "Electronics", 1299.99),
    (2,  "Wireless Mouse",   "Electronics",   29.99),
    (3,  "4K Monitor",       "Electronics",  649.00),
    (4,  "Mechanical KB",    "Electronics",  129.99),
    (5,  "USB-C Hub",        "Electronics",   49.99),
    (6,  "Standing Desk",    "Furniture",    499.00),
    (7,  "Ergonomic Chair",  "Furniture",    349.00),
    (8,  "Monitor Arm",      "Furniture",     89.99),
    (9,  "Bookshelf",        "Furniture",    199.00),
    (10, "Coffee Maker",     "Appliances",    89.95),
    (11, "Air Purifier",     "Appliances",   149.00),
    (12, "Desk Fan",         "Appliances",    39.99),
    (13, "Blender",          "Appliances",    59.99),
    (14, "Python Cookbook",  "Books",         39.99),
    (15, "Clean Code",       "Books",         34.99),
    (16, "Designing Data",   "Books",         49.99),
    (17, "Spark: TDG",       "Books",         44.99),
    (18, "Noise Cancelling", "Electronics",  299.00),
    (19, "Laptop Stand",     "Furniture",     79.99),
    (20, "Smart Kettle",     "Appliances",    69.99),
]

order_schema = StructType([
    StructField("order_id",     IntegerType(), False),
    StructField("customer_id",  StringType(),  False),
    StructField("product_id",   IntegerType(), False),
    StructField("product_name", StringType(),  False),
    StructField("category",     StringType(),  False),
    StructField("quantity",     IntegerType(), False),
    StructField("unit_price",   DoubleType(),  False),
    StructField("order_ts",     StringType(),  False),
])

def make_orders(start_order_id: int, count: int) -> list:
    """Generate `count` random order rows starting from start_order_id."""
    orders = []
    for i in range(count):
        pid, pname, cat, price = random.choice(CATALOG)
        orders.append((
            start_order_id + i,
            f"CUST-{random.randint(1, 50):03d}",
            pid,
            pname,
            cat,
            random.randint(1, 5),
            price,
            datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
        ))
    return orders

def publish(spark, orders: list, label: str) -> None:
    """Serialize orders to JSON and publish to Kafka."""
    df = spark.createDataFrame(orders, order_schema)
    (
        df.select(F.to_json(F.struct("*")).alias("value"))
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", KAFKA_TOPIC)
        .save()
    )
    print(f"  {label}  →  {len(orders)} orders published  "
          f"[order_ids {orders[0][0]}–{orders[-1][0]}]")

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("streaming_producer")
    .master("local[2]")     # local mode — no worker allocation needed for demo
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*60)
print("  STREAMING PRODUCER  —  Continuous Order Publisher")
print("="*60)

# ---------------------------------------------------------------------------
# Reset Kafka topic for a clean run
# ---------------------------------------------------------------------------
jvm = spark._jvm
try:
    props = jvm.java.util.Properties()
    props.put("bootstrap.servers", KAFKA_BROKER)
    admin = jvm.org.apache.kafka.clients.admin.AdminClient.create(props)
    if admin.listTopics().names().get().contains(KAFKA_TOPIC):
        admin.deleteTopics(
            jvm.java.util.Collections.singletonList(KAFKA_TOPIC)
        ).all().get()
        time.sleep(3)
        print(f"\n  Topic '{KAFKA_TOPIC}' reset for clean run")
    admin.close()
except Exception as e:
    print(f"  [note] topic reset skipped: {e}")

# ---------------------------------------------------------------------------
# Phase 1 – Seed bulk: publish all 20 catalog products as initial orders
# ---------------------------------------------------------------------------
print("\n── Phase 1: Seed bulk (20 orders) ──────────────────────")

seed_orders = [(
    i + 1,
    f"CUST-{random.randint(1, 50):03d}",
    pid, pname, cat, random.randint(1, 3), price,
    datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
) for i, (pid, pname, cat, price) in enumerate(CATALOG)]

publish(spark, seed_orders, "Seed bulk")
next_order_id = len(seed_orders) + 1

# ---------------------------------------------------------------------------
# Phase 2 – Continuous stream: publish small batches in a loop
# ---------------------------------------------------------------------------
print(f"\n── Phase 2: Streaming loop "
      f"({LOOP_ITERATIONS} × {ORDERS_PER_LOOP} orders, "
      f"every {LOOP_INTERVAL_SECS}s) ─────────")
print(f"  Total runtime: ~{LOOP_ITERATIONS * LOOP_INTERVAL_SECS}s\n")

for i in range(1, LOOP_ITERATIONS + 1):
    orders = make_orders(next_order_id, ORDERS_PER_LOOP)
    publish(spark, orders, f"Batch {i:02d}/{LOOP_ITERATIONS}")
    next_order_id += ORDERS_PER_LOOP
    if i < LOOP_ITERATIONS:
        time.sleep(LOOP_INTERVAL_SECS)

total = len(seed_orders) + LOOP_ITERATIONS * ORDERS_PER_LOOP
print(f"\n  Total orders published: {total}")
print("\n" + "="*60)
print("  Producer done.")
print("="*60 + "\n")

spark.stop()
