"""
utils/parallel_loader.py
─────────────────────────────────────────────────────────────────────────────
Parallel data ingestion using concurrent.futures.ThreadPoolExecutor.

TEACHING NOTE — WHY PARALLEL INGESTION?
  In a real ETL pipeline you often have many independent data sources
  (a CSV file, an API call, a database table). Loading them one-by-one is
  wasteful: while you wait for the API response, the CPU is idle and the CSV
  could already be loading.

  concurrent.futures gives you high-level parallelism:
    - ThreadPoolExecutor  → good for I/O-bound tasks (API calls, file reads)
    - ProcessPoolExecutor → good for CPU-bound tasks

  Source loading is I/O-bound (waiting for network / disk), so threads are the
  right choice here.

  With 3 sources and each load taking 2s:
    Sequential : 3 × 2s = 6 seconds
    Parallel   : max(2s, 2s, 2s) ≈ 2 seconds   ← 3× faster
"""

from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

from utils.logger import get_logger

logger = get_logger(__name__)


def load_source(name: str, source) -> tuple:
    """
    Calls source.read() and returns (name, DataFrame-or-data).

    This thin wrapper exists so ThreadPoolExecutor can call it; the real
    loading logic lives inside each DataSource subclass.

    Args:
        name:   Logical source name from config (e.g. "sales_csv").
        source: A DataSource instance (CSVSource, APISource, etc.).

    Returns:
        (name, data) tuple — data is whatever source.read() returns.
    """
    logger.info(f"[LOAD ]  Starting source: {name}")
    data = source.read()
    logger.info(f"[LOAD ]  Finished source: {name}")
    return name, data


def load_sources_parallel(
    sources: Dict,
    max_workers: int = 4,
) -> Dict:
    """
    Loads multiple DataSource objects concurrently using threads.

    TEACHING NOTE:
      as_completed() is an iterator that yields futures as they FINISH,
      not in the order they were submitted. This means if source B finishes
      before source A, you process B first — no unnecessary waiting.

    Args:
        sources:     Dict mapping name → DataSource instance.
        max_workers: Thread-pool size. Tune to the number of sources.

    Returns:
        Dict mapping name → loaded data (DataFrame or list of records).
    """
    results: Dict = {}
    failed: Dict = {}

    logger.info(
        f"Starting parallel ingestion of {len(sources)} source(s) "
        f"with {max_workers} worker thread(s)."
    )

    # TEACHING NOTE:
    #   The `with` block ensures the thread pool is properly shut down
    #   (threads cleaned up) even if an exception is raised mid-load.
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all source loads to the thread pool simultaneously
        future_to_name = {
            executor.submit(load_source, name, source): name
            for name, source in sources.items()
        }

        # Collect results as each future completes (order = fastest first)
        for future in as_completed(future_to_name):
            name = future_to_name[future]
            try:
                _, data = future.result()
                results[name] = data
                logger.info(f"[OK]  Source '{name}' loaded successfully.")
            except Exception as exc:
                logger.error(f"[ERR] Source '{name}' failed: {exc}")
                failed[name] = str(exc)

    if failed:
        logger.warning(f"Parallel load finished with {len(failed)} failure(s): {failed}")
    else:
        logger.info("All sources loaded successfully.")

    return results
