"""
pipeline/csv_source.py
─────────────────────────────────────────────────────────────────────────────
Concrete DataSource implementation for CSV files.

TEACHING NOTE — INHERITANCE & SPARK READ:
  CSVSource extends DataSource and provides the read() implementation.
  Notice that the loading uses Spark's built-in CSV reader, not Python's
  `csv` module or pandas.read_csv(). This matters because:

  - spark.read.csv() is distributed: each Spark executor reads a partition
    of the file in parallel across the cluster.
  - Python's csv module is single-threaded and runs only on the driver node.
  - For small files the difference is negligible, but the habit of using
    Spark APIs scales correctly to production data sizes.

  The SparkSession is injected via the constructor (Dependency Injection):
  the source does not create its own SparkSession — the pipeline creates one
  and passes it in. This makes the source easy to test by passing a mock.
"""

from pyspark.sql import SparkSession, DataFrame

from pipeline.base_datasource import DataSource
from utils.logger import get_logger

logger = get_logger(__name__)


class CSVSource(DataSource):
    """
    Reads a CSV file into a Spark DataFrame.

    Config keys expected under this source in config.yaml:
        path    : relative or absolute path to the CSV file
        options : dict of spark.read.csv options (header, inferSchema, etc.)
    """

    def __init__(self, name: str, config: dict, spark: SparkSession):
        """
        Args:
            name:   Logical source name.
            config: Source-level config dict from config.yaml.
            spark:  Active SparkSession — injected by the pipeline.
        """
        super().__init__(name, config)
        self.spark = spark

    def read(self) -> DataFrame:
        """
        Loads the CSV file using Spark's native CSV reader.

        TEACHING NOTE:
          spark.read is the DataFrameReader entry point.
          .options(**dict) applies all key-value pairs from config at once.
          .csv(path) triggers the actual read.

          We do NOT iterate over rows in Python here. Spark reads the file
          in a distributed, lazy fashion — actual I/O only happens when an
          action (show, count, write) is called.
        """
        path = self.config.get("path")
        options = self.config.get("options", {})

        logger.info(f"[CSVSource] Reading file: {path}  options={options}")

        df: DataFrame = (
            self.spark.read
            .options(**options)
            .csv(path)
        )

        logger.info(f"[CSVSource] Schema inferred: {df.columns}")
        return df
