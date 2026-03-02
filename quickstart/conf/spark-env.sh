#!/usr/bin/env bash
# =============================================================================
# spark-env.sh  –  Shell environment sourced by every Spark process at startup
# Mounted into every container at /opt/spark/conf/spark-env.sh
# =============================================================================

# -----------------------------------------------------------------------------
# Worker resource defaults
# Each worker advertises these resources to the Master.
# Override per-container by setting SPARK_WORKER_CORES / SPARK_WORKER_MEMORY
# environment variables directly in docker-compose.yml if you need
# different sizes per worker.
# -----------------------------------------------------------------------------
export SPARK_WORKER_CORES=2
export SPARK_WORKER_MEMORY=2g
