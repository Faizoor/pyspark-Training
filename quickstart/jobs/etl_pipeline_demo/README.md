# ETL Pipeline Demo — Advanced Python + PySpark

A teaching project that shows how **advanced Python concepts** are used to
build a **modular, observable ETL framework** that orchestrates Apache Spark jobs.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   Python Pipeline Framework                      │
│                                                                  │
│  config.yaml ──► main.py                                         │
│                      │                                           │
│                      ├── load_config()        @log_step          │
│                      │                        @time_step         │
│                      ├── build_spark_session()                   │
│                      │                                           │
│                      ├── demo_generator_reading()  ← generator   │
│                      │                                           │
│                      ├── build_sources()       ← OOP / ABC       │
│                      │       CSVSource                           │
│                      │       APISource  (@retry)                 │
│                      │                                           │
│                      └── load_sources_parallel()  ← ThreadPool   │
│                                   │                              │
└───────────────────────────────────┼──────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────┐
│                     Spark Processing                             │
│                                                                  │
│  run_sales_transformation()   @log_step  @time_step             │
│      │                                                           │
│      ├── cast types          (withColumn + .cast)               │
│      ├── derive revenue      (price × quantity)                  │
│      ├── groupBy product     (groupBy + agg)                     │
│      ├── aggregate           (sum, count, round)                 │
│      └── write Parquet       (df.write.parquet)                  │
│                                                                  │
└──────────────────────────────────┬──────────────────────────────┘
                                   │
                                   ▼
                         /tmp/etl_pipeline_demo/
                         output/sales_summary/
                         (Parquet files)
```

---

## Project Layout

```
etl_pipeline_demo/
├── main.py                        # Pipeline orchestrator
├── config.yaml                    # YAML-driven configuration
├── data/
│   └── sales.csv                  # Sample dataset (20 rows)
├── pipeline/
│   ├── base_datasource.py         # Abstract Base Class (DataSource)
│   ├── csv_source.py              # CSVSource  — extends DataSource
│   └── api_source.py              # APISource  — extends DataSource
├── spark_jobs/
│   └── transform_sales.py         # Spark transformation (DataFrame API)
└── utils/
    ├── decorators.py              # @log_step, @time_step, @retry
    ├── generator_reader.py        # Lazy large-file reading with generators
    ├── parallel_loader.py         # ThreadPoolExecutor parallel ingestion
    └── logger.py                  # Centralised logging setup
```

---

## Advanced Python Concepts — Where and Why

| Concept | File(s) | Why it is used here |
|---|---|---|
| **OOP + ABC** | `pipeline/base_datasource.py` | Enforces a common `read()` interface across all sources. New source types require no changes to pipeline logic. |
| **Decorators** | `utils/decorators.py` | Add logging, timing, and retry behaviour to any function without modifying the function itself. Applied to pipeline steps via `@log_step`, `@time_step`, `@retry`. |
| **Generators** | `utils/generator_reader.py` | Read arbitrarily large log files one line at a time — constant memory usage regardless of file size. |
| **YAML config** | `config.yaml` + `main.py` | Externalise all environment-specific settings. Change source paths or output locations without editing Python code. |
| **concurrent.futures** | `utils/parallel_loader.py` | Load multiple independent data sources simultaneously. Reduces wall-clock time from sum of load times to max of load times. |

---

## Spark vs Python — The Key Boundary

```
┌─────────────────────────────────┬──────────────────────────────────────┐
│ Python (driver)                 │ Spark (executors)                    │
├─────────────────────────────────┼──────────────────────────────────────┤
│ Read YAML config                │ spark.read.csv()                     │
│ Build DataSource objects (OOP)  │ withColumn, groupBy, agg             │
│ Coordinate parallel loads       │ sum, count, round (built-in funcs)   │
│ Generate/filter log lines       │ df.write.parquet()                   │
│ Decorate pipeline steps         │                                      │
└─────────────────────────────────┴──────────────────────────────────────┘
```

> **Rule of thumb:** Python orchestrates *when* and *how* Spark runs.
> Spark decides *where* and *how fast* data is processed.
> Never iterate over Spark rows in Python — that defeats the purpose of a
> distributed engine.

---

## Setup and Run

### 1 — Install dependencies (local machine)

```bash
pip install pyspark pyyaml
```

### 2 — Run the pipeline

```bash
cd quickstart/jobs/etl_pipeline_demo
python main.py
```

### 3 — Run inside the existing Docker Spark environment

The `quickstart/` folder already has a `docker-compose.yml` that mounts
`jobs/` into the Spark container. If the cluster is running:

```bash
docker exec -it spark-master bash
cd /opt/spark/work-dir/etl_pipeline_demo
python main.py
```

Or submit as a Spark application:

```bash
spark-submit \
  --master local[*] \
  /opt/spark/work-dir/etl_pipeline_demo/main.py
```

---

## Expected Output

Console logs (trimmed):

```
2026-03-05 12:00:00 | INFO     | __main__ | [START] Step: load_config
2026-03-05 12:00:00 | INFO     | __main__ | Config loaded: environment=dev
2026-03-05 12:00:00 | INFO     | __main__ | [TIMER] load_config completed in 0.002s
...
2026-03-05 12:00:01 | INFO     | __main__ | Starting parallel ingestion of 2 source(s)
2026-03-05 12:00:01 | INFO     | __main__ | [OK]  Source 'sales_csv' loaded successfully.
2026-03-05 12:00:02 | INFO     | __main__ | [OK]  Source 'api_sales' loaded successfully.
...
=== Sales Summary (aggregated) ===
+-----------+-------------+----------------+----------------+
|product    |total_revenue|total_units_sold|num_transactions|
+-----------+-------------+----------------+----------------+
|Laptop     |12000.0      |10              |4               |
|Monitor    |4500.0       |10              |3               |
|Headphones |2400.0       |16              |3               |
|Keyboard   |1275.0       |17              |3               |
|Webcam     |900.0        |10              |2               |
|USB Hub    |735.0        |21              |2               |
|Mouse      |1125.0       |45              |3               |
+-----------+-------------+----------------+----------------+
```

Parquet output is written to `/tmp/etl_pipeline_demo/output/sales_summary/`.

---

## Key Learning Points

1. **Python OOP** structures the pipeline — each source type is a class,
   all sharing the same `read()` contract via the ABC.

2. **Decorators** provide cross-cutting concerns (logging, timing, retries)
   without polluting business logic.

3. **Generators** allow safe, memory-constant processing of large files on
   the driver before data enters Spark.

4. **YAML config** decouples environment-specific settings from code,
   enabling dev/staging/prod environments from a single codebase.

5. **ThreadPoolExecutor** overlaps I/O-bound source loads for a direct
   reduction in pipeline latency.

6. **Spark DataFrame API** (not Python UDFs) is used for all transformations
   so Catalyst can optimise the query plan and executors run in parallel.
