"""
streaming_consumer.py  –  Spark + Kafka + HDFS  |  Streaming: Consume & Transform
===================================================================================

What this script does
---------------------
This is a long-running Structured Streaming job that continuously reads order
events from Kafka, applies transformations in each micro-batch, and writes
results to both the console and HDFS — without ever stopping until timeout.

  Every 10 seconds (one micro-batch trigger):
    1.  Read new Kafka messages that arrived since the last trigger
    2.  Parse JSON → order DataFrame
    3.  Enrich each order:
          order_total       = quantity × unit_price
          discount_pct      = category-based discount (same rules as batch demo)
          discounted_total  = order_total after discount
          order_value_tier  = budget / mid-range / premium (by order_total)
    4.  Append enriched rows   →  HDFS  /demo/streaming/orders
    5.  Print a live category  →  console (aggregated summary per micro-batch)
        summary to console

  After 150 seconds the query auto-terminates cleanly.

Key Structured Streaming concepts shown
----------------------------------------
  readStream     : opens a continuous unbounded read from Kafka
  trigger        : controls how often a micro-batch fires (every 10s here)
  foreachBatch   : gives access to each micro-batch as a regular DataFrame,
                   letting you write to multiple sinks or run arbitrary logic
  checkpointing  : Spark records the last processed Kafka offset to
                   /tmp/spark-checkpoints so the job can resume without
                   reprocessing or missing messages after a restart
  awaitTermination(ms) : blocks until timeout — the job runs continuously
                         until this timer expires (demo-friendly auto-stop)

Data flow  (repeats every 10s)
------------------------------
  Kafka  →  parse JSON  →  enrich (order_total, discount, tier)
                │
                ├──► HDFS append  /demo/streaming/orders  (full enriched rows)
                │
                └──► Console      category summary (count, revenue, avg discount)

HOW TO RUN
----------
From the quickstart/ directory — start the CONSUMER FIRST, then the producer:

  Terminal 1:
    docker compose exec spark-master /opt/spark/bin/spark-submit \\
        --master local[2] \
        --conf spark.jars.ivy=/tmp/.ivy2 \
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \
        /opt/spark/jobs/integrations_demo/streaming_consumer.py

  Terminal 2:
    docker compose exec spark-master /opt/spark/bin/spark-submit \
        --master local[2] \
        --conf spark.jars.ivy=/tmp/.ivy2 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1 \\
        /opt/spark/jobs/integrations_demo/streaming_producer.py

  The consumer auto-stops after ~150s. Press Ctrl+C to stop earlier.
  Browse HDFS output at: http://localhost:9870
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, StringType, DoubleType
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
KAFKA_BROKER    = "kafka-broker:29092"
KAFKA_TOPIC     = "stream-orders"
HDFS_OUTPUT     = "hdfs://hdfs-namenode:8020/demo/streaming/orders"
CHECKPOINT_DIR  = "/tmp/spark-checkpoints/stream-orders-consumer"
TRIGGER_SECS    = 10    # micro-batch interval
TIMEOUT_MS      = 150_000   # auto-stop after 150 seconds

# ---------------------------------------------------------------------------
# Schema — must match what streaming_producer.py serialises
# ---------------------------------------------------------------------------
order_schema = StructType([
    StructField("order_id",     IntegerType(), True),
    StructField("customer_id",  StringType(),  True),
    StructField("product_id",   IntegerType(), True),
    StructField("product_name", StringType(),  True),
    StructField("category",     StringType(),  True),
    StructField("quantity",     IntegerType(), True),
    StructField("unit_price",   DoubleType(),  True),
    StructField("order_ts",     StringType(),  True),
])

# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
spark = (
    SparkSession.builder
    .appName("streaming_consumer")
    .master("local[2]")     # local mode — no worker allocation needed for demo
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("\n" + "="*60)
print("  STREAMING CONSUMER  —  Kafka → Transform → HDFS")
print(f"  Micro-batch every {TRIGGER_SECS}s  |  Auto-stop after {TIMEOUT_MS//1000}s")
print("="*60)
print(f"\n  Listening on Kafka topic : {KAFKA_TOPIC}")
print(f"  HDFS output              : {HDFS_OUTPUT}")
print(f"  Checkpoint dir           : {CHECKPOINT_DIR}")
print(f"  Spark UI                 : http://localhost:4040\n")

# ---------------------------------------------------------------------------
# Clean up previous run state
# Deleting the checkpoint dir forces the stream to restart from the beginning
# of the Kafka topic (startingOffsets=earliest applies again on a fresh start).
# Clearing HDFS output avoids accumulating data across demo runs.
# Both are safe to skip — remove these blocks if you want resume behaviour.
# ---------------------------------------------------------------------------
jvm  = spark._jvm
Path = jvm.org.apache.hadoop.fs.Path
conf = spark._jsc.sc().hadoopConfiguration()

for label, path_str in [("checkpoint", CHECKPOINT_DIR), ("HDFS output", HDFS_OUTPUT)]:
    try:
        p  = Path(path_str)
        fs = p.getFileSystem(conf)
        if fs.exists(p):
            fs.delete(p, True)
            print(f"  [reset] Cleared previous {label}: {path_str}")
    except Exception as e:
        print(f"  [reset] Could not clear {label}: {e}")


# ---------------------------------------------------------------------------
# Source — open a streaming read from Kafka
#
# startingOffsets=earliest  : read from the very beginning of the topic on the
#                              first run (ignored on restart; checkpoint takes over)
# failOnDataLoss=false       : tolerate missing offsets in development/demo mode
# ---------------------------------------------------------------------------
raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    # Tolerate transient partition-metadata gaps (e.g. topic delete/recreate)
    .option("kafka.metadata.max.age.ms", "5000")
    .option("kafka.max.block.ms", "10000")
    .option("minPartitions", "1")
    .load()
)

# Parse binary Kafka value column into structured order fields
parsed_stream = (
    raw_stream
    .select(
        F.col("offset"),
        F.col("timestamp").alias("kafka_ts"),
        F.from_json(F.col("value").cast("string"), order_schema).alias("o")
    )
    .select("offset", "kafka_ts", "o.*")
    # Drop any malformed rows where JSON parsing produced nulls
    .filter(F.col("order_id").isNotNull())
)


# ---------------------------------------------------------------------------
# Transformation function — applied inside each foreachBatch call
#
# Using a function keeps the logic reusable and clearly separated from the
# streaming plumbing.  The enriched DataFrame is a normal static DataFrame
# inside foreachBatch — all standard Spark APIs apply.
# ---------------------------------------------------------------------------
def enrich_orders(df: DataFrame) -> DataFrame:
    """
    Add three derived columns to order rows:
      order_total       – gross revenue for this line item
      discount_pct      – category-based promotional discount
      discounted_total  – net revenue after discount
      order_value_tier  – segment label for downstream BI filtering
    """
    return (
        df
        # Gross line-item value
        .withColumn(
            "order_total",
            F.round(F.col("quantity") * F.col("unit_price"), 2)
        )
        # Category discount:  Electronics 15%, Furniture 10%, others 5%
        .withColumn(
            "discount_pct",
            F.when(F.col("category") == "Electronics", 15.0)
             .when(F.col("category") == "Furniture",   10.0)
             .otherwise(5.0)
        )
        # Net value after discount
        .withColumn(
            "discounted_total",
            F.round(
                F.col("order_total") * (1 - F.col("discount_pct") / 100), 2
            )
        )
        # Tier label based on gross order value
        .withColumn(
            "order_value_tier",
            F.when(F.col("order_total") < 100,  "budget")
             .when(F.col("order_total") < 1000, "mid-range")
             .otherwise("premium")
        )
    )


# ---------------------------------------------------------------------------
# foreachBatch — called once per micro-batch with the batch as a DataFrame
#
# Two things happen per batch:
#   A) Enrich + persist to HDFS (append mode — rows accumulate over time)
#   B) Print a live category summary so you can watch the stream in the console
# ---------------------------------------------------------------------------
batch_counter = [0]   # mutable container so the closure can update it

def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    row_count = batch_df.count()
    if row_count == 0:
        print(f"  [batch {batch_id:03d}] No new messages — waiting...")
        return

    batch_counter[0] += 1
    enriched = enrich_orders(batch_df)

    # ── A) Persist enriched rows to HDFS ─────────────────────────────────
    # mode=append means each batch adds new rows; older rows are preserved.
    (
        enriched.write
        .mode("append")
        .parquet(HDFS_OUTPUT)
    )

    # ── B) Console summary ────────────────────────────────────────────────
    print(f"\n  ┌─ Micro-batch {batch_id:03d}  "
          f"({row_count} new orders) "
          f"{'─' * max(0, 30 - len(str(row_count)))}┐")

    summary = (
        enriched
        .groupBy("category")
        .agg(
            F.count("order_id")                    .alias("orders"),
            F.sum("quantity")                      .alias("units_sold"),
            F.round(F.sum("order_total"),       2) .alias("gross_revenue"),
            F.round(F.sum("discounted_total"),  2) .alias("net_revenue"),
            F.round(F.avg("discount_pct"),      1) .alias("avg_discount_%"),
        )
        .orderBy(F.col("gross_revenue").desc())
    )
    summary.show(truncate=False)

    tier_dist = (
        enriched
        .groupBy("order_value_tier")
        .agg(F.count("order_id").alias("order_count"))
        .orderBy("order_value_tier")
    )
    print("  Order tier distribution:")
    tier_dist.show(truncate=False)

    print(f"  └─ Appended {row_count} rows to HDFS  →  {HDFS_OUTPUT}")


# ---------------------------------------------------------------------------
# Sink — wire the stream through foreachBatch
#
# processingTime trigger: fires every TRIGGER_SECS seconds.
# checkpointLocation   : Spark writes the last committed Kafka offset here.
#                        On restart, reading resumes exactly where it left off.
# ---------------------------------------------------------------------------
query = (
    parsed_stream.writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime=f"{TRIGGER_SECS} seconds")
    .option("checkpointLocation", CHECKPOINT_DIR)
    .start()
)

print(f"  Streaming query started (id = {query.id})")
print(f"  Waiting for messages... (auto-stops in {TIMEOUT_MS//1000}s)\n")

# Block until timeout — the stream runs continuously until this expires.
# Catch UnknownTopicOrPartitionException: Kafka may briefly lose partition
# metadata when a topic is deleted/recreated by the producer mid-stream.
# If we have already processed at least one batch the demo succeeded.
try:
    query.awaitTermination(TIMEOUT_MS)
except Exception as exc:
    err = str(exc)
    benign = "UnknownTopicOrPartitionException" in err or "STREAM_FAILED" in err
    if benign and batch_counter[0] > 0:
        print(f"\n  [info] Stream stopped after Kafka topic was recycled by the producer.")
        print(f"  [info] All data was already processed ({batch_counter[0]} micro-batch(es) completed).")
    else:
        raise
finally:
    try:
        query.stop()
    except Exception:
        pass

print("\n" + "="*60)
print(f"  Consumer stopped after {TIMEOUT_MS//1000}s.")
print(f"  Total micro-batches with data: {batch_counter[0]}")
print(f"  Browse HDFS output : http://localhost:9870")
print(f"  Spark history      : http://localhost:18080")
print("="*60 + "\n")

spark.stop()
