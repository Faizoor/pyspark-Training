"""
utils/logger.py
─────────────────────────────────────────────────────────────────────────────
Centralised logging configuration for the ETL pipeline.

TEACHING NOTE:
  Using a single logger factory means every module shares the same format and
  output destination. This is standard Python practice for multi-module projects.
"""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Returns a named logger with a consistent format.

    Args:
        name: Typically passed as __name__ from the calling module
              so log lines show exactly which module they came from.
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers if this function is called multiple times
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)

    return logger
