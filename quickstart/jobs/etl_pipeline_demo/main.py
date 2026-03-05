"""
main.py — ETL Pipeline Demo Orchestrator
─────────────────────────────────────────────────────────────────────────────
Entry point for the modular PySpark ETL pipeline.

This file demonstrates how all advanced Python concepts combine to form a
clean, observable, and extensible pipeline framework.

Advanced Python concepts used in this project
─────────────────────────────────────────────
  Concept                  │ Where used
  ─────────────────────────┼──────────────────────────────────────────────
  OOP / Abstract classes   │ pipeline/base_datasource.py, csv_source.py,
                           │ api_source.py
  Decorators               │ utils/decorators.py → @log_step, @time_step,
                           │   @retry, applied in transform_sales.py and here
  Generators               │ utils/generator_reader.py → read_large_file()
  YAML configuration       │ config.yaml + yaml.safe_load()
  concurrent.futures       │ utils/parallel_loader.py → ThreadPoolExecutor
  ─────────────────────────┴──────────────────────────────────────────────

Run:
    cd quickstart/jobs/etl_pipeline_demo
    python main.py
"""

import os
import sys
import yaml

from pyspark.sql import SparkSession

from pipeline.csv_source import CSVSource
from pipeline.api_source import APISource
from spark_jobs.transform_sales import run_sales_transformation
from utils.decorators import log_step, time_step
from utils.generator_reader import demo_generator_reading
from utils.logger import get_logger
from utils.parallel_loader import load_sources_parallel

logger = get_logger(__name__)

# ── Source type registry ───────────────────────────────────────────────────────
# TEACHING NOTE:
#   Instead of a long if/elif chain, we use a dict as a dispatch table.
#   Adding a new source type (e.g. "jdbc") only requires adding one entry here
#   and creating the corresponding class — no changes to the pipeline logic.
SOURCE_REGISTRY = {
    "csv": CSVSource,
    "api": APISource,
}


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline steps (decorated for observability)
# ─────────────────────────────────────────────────────────────────────────────

@log_step
@time_step
def load_config(config_path: str) -> dict:
    """
    Loads and returns the pipeline configuration from a YAML file.

    TEACHING NOTE:
      yaml.safe_load is preferred over yaml.load because it refuses to
      deserialise arbitrary Python objects — preventing code injection if
      the config file comes from an untrusted source.
    """
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    logger.info(f"Config loaded from: {config_path}  environment={config.get('environment')}")
    return config


@log_step
@time_step
def build_spark_session(spark_config: dict) -> SparkSession:
    """
    Creates and returns the SparkSession.

    TEACHING NOTE:
      There is only ever ONE SparkSession per application (it is a singleton).
      getOrCreate() returns the existing session if one already exists, making
      it safe to call from multiple modules.

      master=local[*] means: use all available CPU cores on the local machine.
      In production this would be yarn, k8s, or spark://<host>:7077.
    """
    spark = (
        SparkSession.builder
        .appName(spark_config.get("app_name", "ETLPipelineDemo"))
        .master(spark_config.get("master", "local[*]"))
        # Silence noisy INFO logs from Spark internals during the demo
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"SparkSession created: {spark.sparkContext.appName}")
    return spark


@log_step
@time_step
def build_sources(sources_config: dict, spark: SparkSession) -> dict:
    """
    Dynamically constructs DataSource objects from the YAML config.

    TEACHING NOTE:
      SOURCE_REGISTRY[source_type] is a class lookup at runtime.
      We call the class (e.g. CSVSource) with (name, config, spark) to
      instantiate it. This is *dynamic dispatch* — the pipeline doesn't
      know or care which concrete class it is creating as long as it
      implements the DataSource interface (read() method).

      This is the Factory pattern: a central place that knows how to create
      objects, so callers don't need to import every concrete class.
    """
    sources = {}
    for name, cfg in sources_config.items():
        source_type = cfg.get("type")
        klass = SOURCE_REGISTRY.get(source_type)
        if klass is None:
            logger.warning(f"Unknown source type '{source_type}' for '{name}'. Skipping.")
            continue
        sources[name] = klass(name=name, config=cfg, spark=spark)
        logger.info(f"Built source: {name}  → {klass.__name__}")
    return sources


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline
# ─────────────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("  ETL Pipeline Demo — Starting")
    logger.info("=" * 60)

    # ── Resolve paths relative to THIS file's location ───────────────────────
    # So the pipeline can be run from any working directory.
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config.yaml")

    # ── Step 1: Load YAML config ─────────────────────────────────────────────
    config = load_config(config_path)

    # Resolve data paths relative to the project root
    for source_cfg in config.get("sources", {}).values():
        if source_cfg.get("type") == "csv":
            source_cfg["path"] = os.path.join(base_dir, source_cfg["path"])

    output_path = config["output"]["path"]

    # ── Step 2: Create SparkSession ──────────────────────────────────────────
    spark = build_spark_session(config.get("spark", {}))

    # ── Step 3 (bonus): Generator demo ───────────────────────────────────────
    # TEACHING NOTE:
    #   This step is pure Python — no Spark involvement. It demonstrates that
    #   generators can safely process large files before Spark even starts,
    #   e.g. to validate/filter raw log files before ingestion.
    demo_generator_reading()

    # ── Step 4: Build DataSource objects dynamically ─────────────────────────
    sources = build_sources(config.get("sources", {}), spark)

    # ── Step 5: Load sources in PARALLEL ─────────────────────────────────────
    # TEACHING NOTE:
    #   ThreadPoolExecutor fires all source.read() calls simultaneously.
    #   order of completion depends on which source responds fastest.
    loaded_data = load_sources_parallel(sources, max_workers=4)

    # ── Step 6: Run Spark transformation on sales CSV ────────────────────────
    # TEACHING NOTE:
    #   We only run the Spark transformation on the CSV source (the one that
    #   produced a real sales DataFrame). The API source data could be joined
    #   or enriched here as a further exercise.
    sales_df = loaded_data.get("sales_csv")
    if sales_df is None:
        logger.error("sales_csv source not found in loaded data. Aborting.")
        sys.exit(1)

    run_sales_transformation(
        df=sales_df,
        output_path=output_path,
        output_mode=config["output"].get("mode", "overwrite"),
    )

    # ── Step 7: Done ─────────────────────────────────────────────────────────
    logger.info("=" * 60)
    logger.info(f"  Pipeline complete. Output written to: {output_path}")
    logger.info("=" * 60)

    spark.stop()


if __name__ == "__main__":
    main()
