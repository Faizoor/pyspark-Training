"""
utils/decorators.py
─────────────────────────────────────────────────────────────────────────────
Reusable decorators for ETL pipeline observability.

TEACHING NOTE:
  Decorators are a powerful Python feature that let you wrap any function
  with extra behaviour (logging, timing, retries) WITHOUT modifying the
  function's own code. This is the Open/Closed Principle in action:
  pipeline steps are open for extension (decorators) but closed for
  modification (you don't touch the step's logic).

  In ETL pipelines, decorators are particularly valuable because:
  - Every step needs the same observability (timing, logging)
  - Writing that boilerplate inside each step would create repetition
  - Decorators centralise it and make steps easy to read

Usage:
    @log_step
    @time_step
    def my_pipeline_step(df):
        ...
"""

import functools
import time

from utils.logger import get_logger

logger = get_logger(__name__)


def log_step(func):
    """
    Decorator: logs the start and end of any pipeline function.

    TEACHING NOTE:
      functools.wraps preserves the original function's __name__ and __doc__
      so introspection tools (and humans reading logs) see the real name,
      not the wrapper's name.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger.info(f"[START] Step: {func.__name__}")
        result = func(*args, **kwargs)
        logger.info(f"[END]   Step: {func.__name__}")
        return result
    return wrapper


def time_step(func):
    """
    Decorator: measures and logs wall-clock execution time of a function.

    TEACHING NOTE:
      This decorator is stacked below @log_step in typical usage, so the
      order of decorator application (bottom-up) means time_step wraps the
      raw function first, then log_step wraps that. The timing therefore
      measures exactly the function's own work.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        elapsed = time.perf_counter() - start
        logger.info(f"[TIMER] {func.__name__} completed in {elapsed:.3f}s")
        return result
    return wrapper


def retry(max_attempts: int = 3, delay_seconds: float = 1.0):
    """
    Parametrised decorator factory: retries a function up to max_attempts
    times before re-raising the last exception.

    TEACHING NOTE:
      This is a *decorator factory* — a function that returns a decorator.
      You call it with arguments: @retry(max_attempts=3).
      This pattern is common when decorators need configuration.

    Args:
        max_attempts: Maximum number of tries before giving up.
        delay_seconds: Seconds to wait between retries.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    last_exception = exc
                    logger.warning(
                        f"[RETRY] {func.__name__} attempt {attempt}/{max_attempts} "
                        f"failed: {exc}"
                    )
                    if attempt < max_attempts:
                        time.sleep(delay_seconds)
            logger.error(f"[RETRY] {func.__name__} exhausted all {max_attempts} attempts.")
            raise last_exception
        return wrapper
    return decorator
