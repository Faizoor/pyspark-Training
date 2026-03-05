"""
pipeline/api_source.py
─────────────────────────────────────────────────────────────────────────────
Concrete DataSource implementation for a REST API endpoint.

TEACHING NOTE — WHEN TO USE PYTHON (NOT SPARK) FOR INGESTION:
  Some data comes from APIs, not files. Spark has no native HTTP client, so
  we use Python's urllib (standard library, no extra deps) to fetch the data
  on the driver and then hand it to Spark via spark.createDataFrame().

  Flow:
      API  →  Python list of dicts (driver)  →  spark.createDataFrame()
                                                         ↓
                                                  Spark DataFrame
                                                  (now distributed)

  This is a legitimate pattern when the API response is small enough to fit
  in driver memory. For large paginated APIs you would fetch in chunks and
  append each chunk, or write to a file first and use CSVSource.

  The @retry decorator from utils/decorators.py wraps the fetch call:
  network calls can fail transiently, and retrying automatically is better
  than crashing the whole pipeline.
"""

import json
import urllib.request
from typing import List, Dict

from pyspark.sql import SparkSession, DataFrame

from pipeline.base_datasource import DataSource
from utils.decorators import retry
from utils.logger import get_logger

logger = get_logger(__name__)


class APISource(DataSource):
    """
    Fetches JSON records from a REST endpoint and returns a Spark DataFrame.

    Config keys expected under this source in config.yaml:
        endpoint     : Full URL of the API.
        record_limit : Maximum number of records to keep (default: all).
    """

    def __init__(self, name: str, config: dict, spark: SparkSession):
        super().__init__(name, config)
        self.spark = spark

    @retry(max_attempts=3, delay_seconds=1.0)
    def _fetch_json(self, url: str) -> List[Dict]:
        """
        Fetches JSON from `url` and returns a list of record dicts.

        TEACHING NOTE:
          The @retry decorator wraps this method transparently. If urllib
          throws a URLError (timeout, DNS failure), the decorator waits 1s
          and tries again, up to 3 times. The method itself has no retry
          logic — decoration handles it cleanly.
        """
        logger.info(f"[APISource] GET {url}")
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode("utf-8"))

        # jsonplaceholder returns a list; normalise to list
        if isinstance(data, dict):
            data = [data]

        return data

    def read(self) -> DataFrame:
        """
        Fetches records from the API and converts them to a Spark DataFrame.

        TEACHING NOTE:
          spark.createDataFrame(list_of_dicts) infers the schema from the
          Python dicts — no schema file needed for this demo. In production
          you would define an explicit StructType schema to catch schema drift.
        """
        endpoint = self.config.get("endpoint")
        limit = self.config.get("record_limit")

        records = self._fetch_json(endpoint)

        if limit:
            records = records[:limit]

        logger.info(f"[APISource] {len(records)} records fetched from {endpoint}")

        # Flatten nested dicts to one level so Spark can infer the schema
        flat_records = [_flatten(r) for r in records]

        df: DataFrame = self.spark.createDataFrame(flat_records)
        return df


# ── Helper ────────────────────────────────────────────────────────────────────

def _flatten(record: dict, prefix: str = "") -> dict:
    """
    Flattens one level of nested dicts.
    e.g. {"a": {"b": 1}} → {"a_b": 1}

    TEACHING NOTE:
      This is plain Python — it runs on the driver, not on executors.
      It is appropriate here because the record count is small (from the API).
      If records came from Spark, we would use spark functions like
      col("struct_col.field") or schema_of_json() instead.
    """
    flat = {}
    for key, value in record.items():
        full_key = f"{prefix}{key}" if not prefix else f"{prefix}_{key}"
        if isinstance(value, dict):
            flat.update(_flatten(value, prefix=full_key))
        else:
            flat[full_key] = value
    return flat
