"""
DE2 — Landing-to-Raw Ingestion Flow, as an Airflow DAG.

Wraps the already-tested functions in de2_ingestion.py. This file
contains ZERO business logic of its own — schema validation, quality
checks and routing all live in de2_ingestion.py so they stay testable
and reusable outside Airflow (per DE2/DE3 trainer guidance: "Airflow
orchestrates. Validation logic stays in reusable Python functions
rather than being buried in DAG code").

Tasks: detect_files >> validate_and_route >> log_summary

detect_files      -> list of candidate filepaths in data/landing/
validate_and_route -> for each file: validate_schema, validate_minimum_quality,
                       route_file, write_ingestion_log (all via process_one_file)
log_summary       -> human-readable run summary in the Airflow task log
"""

from __future__ import annotations

import logging
from datetime import datetime

from airflow.sdk import dag, task

# de2_ingestion.py lives alongside this file in the dags/ folder,
# so it imports directly with no path manipulation needed.
import de2_ingestion as de2

logger = logging.getLogger("airflow.task")


@dag(
    dag_id="de2_landing_to_raw",
    schedule=None,          # manually triggered for this lab
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["de2", "ingestion", "network-intelligence"],
)
def de2_landing_to_raw():

    @task
    def detect_files_task() -> list[str]:
        files = de2.detect_files()
        if not files:
            logger.warning("No candidate files found in %s.", de2.LANDING_DIR)
        return files

    @task
    def validate_and_route_task(files: list[str]) -> list[dict]:
        results = []
        for filepath in files:
            logger.info("--- Processing %s ---", filepath)
            result = de2.process_one_file(filepath)
            results.append(result)
        return results

    @task
    def log_summary_task(results: list[dict]) -> None:
        logger.info("=" * 60)
        logger.info("DE2 INGESTION RUN SUMMARY")
        if not results:
            logger.info("  (no files were processed)")
        for r in results:
            suffix = f" ({r['reason']})" if r.get("reason") else ""
            logger.info("  %s -> %s%s", r["filename"], r["status"], suffix)
        logger.info("=" * 60)

    files = detect_files_task()
    results = validate_and_route_task(files)
    log_summary_task(results)


de2_landing_to_raw()
