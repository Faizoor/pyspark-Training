"""
spark_jobs/transform_sales.py
─────────────────────────────────────────────────────────────────────────────
Spark transformation: aggregate sales data by product.

TEACHING NOTE — SPARK DATAFRAME API vs PYTHON LOOPS:
  This module contains ONLY Spark DataFrame API calls. No Python for-loops
  or list comprehensions are used to process rows.

  WHY?
  ┌─────────────────────────────────┬──────────────────────────────────────┐
  │ Python loop over rows           │ Spark DataFrame API                  │
  ├─────────────────────────────────┼──────────────────────────────────────┤
  │ Runs on the driver (1 machine)  │ Runs on all executors (N machines)   │
  │ Sequential                      │ Parallel across partitions           │
  │ Breaks fault tolerance          │ Lineage tracked for re-computation   │
  │ O(n) driver memory              │ O(partition) per executor memory     │
  └─────────────────────────────────┴──────────────────────────────────────┘

  The Python code in this project (OOP, decorators, generators, config) runs
  on the DRIVER to orchestrate the pipeline. Spark code runs on EXECUTORS
  to process data. Understanding this boundary is key to writing correct,
  performant PySpark.

  Pipeline steps:
    1. Cast price and quantity to numeric types (avoids silent nulls)
    2. Compute revenue per row  (price × quantity)
    3. Group by product
    4. Aggregate: total_revenue, total_units_sold, num_transactions
    5. Sort by total_revenue descending
    6. Write result as Parquet
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from utils.decorators import log_step, time_step
from utils.logger import get_logger

logger = get_logger(__name__)


@log_step
@time_step
def run_sales_transformation(df: DataFrame, output_path: str, output_mode: str = "overwrite") -> DataFrame:
    """
    Transforms raw sales data into a product-level revenue summary.

    TEACHING NOTE — DECORATOR STACKING:
      @log_step wraps the outer layer  → logs START / END
      @time_step wraps the inner layer → measures elapsed time
      Python applies decorators bottom-up, so execution order is:
          log_step.start → time_step.start → function body
                         → time_step.end  → log_step.end

    Args:
        df:          Raw sales Spark DataFrame (id, product, price, quantity).
        output_path: Destination directory for Parquet output.
        output_mode: Spark write mode ("overwrite", "append", "ignore").

    Returns:
        Aggregated DataFrame (for inspection / testing).
    """
    logger.info(f"Input row count (before transform): {df.count()}")

    # ── Step 1: Cast columns to correct types ─────────────────────────────────
    # TEACHING NOTE:
    #   inferSchema works well for demos, but explicit casting is safer in
    #   production because schema drift in the source file won't silently
    #   produce nulls. We use F.col() from pyspark.sql.functions — never
    #   Python string operations on column values.
    df_typed = df.withColumn("price",    F.col("price").cast("double")) \
                 .withColumn("quantity", F.col("quantity").cast("integer"))

    # ── Step 2: Derive revenue column ─────────────────────────────────────────
    # TEACHING NOTE:
    #   F.col("price") * F.col("quantity") is a Spark column expression.
    #   Spark computes this in parallel across all partitions. This is NOT
    #   a Python multiplication — it builds a DSL plan that Catalyst optimises.
    df_with_revenue = df_typed.withColumn(
        "revenue", F.col("price") * F.col("quantity")
    )

    # ── Step 3: Group and aggregate ───────────────────────────────────────────
    # TEACHING NOTE:
    #   groupBy + agg is the Spark equivalent of SQL GROUP BY + aggregate
    #   functions. F.sum, F.count, F.round are all Spark built-in functions
    #   that run on executors — never Python UDFs which are slow and opaque
    #   to the Catalyst query optimiser.
    df_summary = (
        df_with_revenue
        .groupBy("product")
        .agg(
            F.round(F.sum("revenue"),  2).alias("total_revenue"),
            F.sum("quantity")           .alias("total_units_sold"),
            F.count("id")               .alias("num_transactions"),
        )
        .orderBy(F.col("total_revenue").desc())
    )

    # ── Step 4: Write output ──────────────────────────────────────────────────
    # TEACHING NOTE:
    #   Parquet is a columnar format — it compresses well and supports
    #   predicate pushdown (Spark skips columns you don't need). It is the
    #   default format for data lakes. CSV would be human-readable but
    #   unsuitable for large-scale querying.
    logger.info(f"Writing Parquet output to: {output_path}  (mode={output_mode})")
    df_summary.write.mode(output_mode).parquet(output_path)

    # ── Step 5: Preview result ────────────────────────────────────────────────
    logger.info("=== Sales Summary (aggregated) ===")
    df_summary.show(truncate=False)

    logger.info(f"Output row count: {df_summary.count()}")
    return df_summary
