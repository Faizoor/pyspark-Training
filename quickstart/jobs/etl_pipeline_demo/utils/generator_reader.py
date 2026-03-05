"""
utils/generator_reader.py
─────────────────────────────────────────────────────────────────────────────
Demonstrates Python generators for memory-efficient file processing.

TEACHING NOTE — WHY GENERATORS?
  When you process large files with a regular list, Python loads EVERY line
  into memory at once. For a 1 GB log file, that means 1 GB of RAM used
  before you even look at the first line.

  A generator uses the `yield` keyword to produce one item at a time and
  pause execution. Memory usage stays constant regardless of file size because
  only one line is ever in memory at a time.

  Key difference:
      list_based  = [line for line in open(path)]   # loads ALL lines
      generator   = read_large_file(path)            # loads ONE line at a time

  In ETL pipelines, generators are useful for:
  - Reading large flat log files before ingesting into Spark
  - Streaming records from an API page-by-page
  - Pre-filtering rows before handing data to Spark (avoids loading junk)
"""

import os
import tempfile
from typing import Generator

from utils.logger import get_logger

logger = get_logger(__name__)


def read_large_file(path: str) -> Generator[str, None, None]:
    """
    Generator that yields one line at a time from a text file.

    TEACHING NOTE:
      `yield` suspends the function and returns control to the caller.
      The next call to next() (which a for-loop does automatically) resumes
      from right after the yield. No list is ever built.

    Args:
        path: Absolute or relative path to the file.

    Yields:
        One stripped line string at a time.
    """
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            yield line.rstrip("\n")


def count_lines_with_generator(path: str) -> int:
    """
    Counts lines in a file without loading it into memory.

    Uses the read_large_file generator — demonstrates that generators
    compose naturally with standard Python iteration.
    """
    return sum(1 for _ in read_large_file(path))


def demo_generator_reading():
    """
    Educational demo:
      1. Creates a temporary file simulating a large log file (1 000 lines).
      2. Reads it line-by-line with the generator.
      3. Counts lines containing the word ERROR.
      4. Cleans up.

    TEACHING NOTE:
      We use 1 000 lines here to keep the demo fast, but the memory behaviour
      is identical for 1 000 000 lines because generators are lazy — they
      never hold more than one line in memory.
    """
    logger.info("=== Generator Demo: Lazy Large File Reading ===")

    # ── Step 1: create a simulated log file ──────────────────────────────────
    tmp = tempfile.NamedTemporaryFile(
        mode="w", suffix=".log", delete=False, encoding="utf-8"
    )
    line_count = 1_000
    error_count = 0
    for i in range(line_count):
        level = "ERROR" if i % 100 == 0 else "INFO"
        if level == "ERROR":
            error_count += 1
        tmp.write(f"2026-03-05 12:00:{i:05d} {level} Record {i}\n")
    tmp.close()
    logger.info(f"  Simulated log file created: {line_count} lines at {tmp.name}")

    # ── Step 2: read lazily with the generator ────────────────────────────────
    # TEACHING NOTE:
    #   At no point do we call list() or store all lines. The for-loop pulls
    #   one line from the generator, processes it, then asks for the next.
    errors_found = 0
    for line in read_large_file(tmp.name):
        if "ERROR" in line:
            errors_found += 1

    logger.info(f"  Lines scanned lazily : {line_count}")
    logger.info(f"  ERROR lines detected : {errors_found}  (expected {error_count})")
    logger.info("  Memory used at peak  : ~1 line (generator, not a list)")

    # ── Step 3: cleanup ───────────────────────────────────────────────────────
    os.unlink(tmp.name)
    logger.info("=== Generator Demo complete ===")
