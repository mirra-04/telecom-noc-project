"""
DE3 — Orchestrate the Spark Processing Job.

Extends DE2 with a Spark processing step and a status-publish step.
Airflow orchestrates; it contains no cleaning/aggregation logic of its
own — that all lives in spark/telecom_pipeline.py (SP7), reused
unchanged here.

Tasks: detect_files_task >> validate_and_route_task >> run_spark_task >> publish_status_task

run_spark_task invokeas spark/telecom_pipeline.py as a subprocess
(python3 <script> --input-dir data/raw --reference milano-grid.geojson
--output-dir data) and fails the task (non-zero exit) if the Spark job
fails for any reason, including "no raw files present" (SP7's
fail-fast behaviour). Because Airflow's default trigger rule is
all_success, publish_status_task will NOT run if run_spark_task fails
-- this is what "downstream tasks depend on Spark success" means in
practice, not a manual check.

publish_status_task confirms the analytics outputs actually exist
after a successful Spark run and reports basic stats. It does not
recompute anything -- it only reads what Spark already wrote.
"""

from __future__ import annotations

import logging
import os
import subprocess
import sys
from datetime import datetime

from airflow.sdk import dag, task

import de2_ingestion as de2

logger = logging.getLogger("airflow.task")

PROJECT_ROOT = "/mnt/d/Training/Project - 2"
SPARK_SCRIPT = os.path.join(PROJECT_ROOT, "spark", "telecom_pipeline.py")
REFERENCE_PATH = os.path.join(PROJECT_ROOT, "data", "reference", "milano-grid.geojson")
DATA_DIR = os.path.join(PROJECT_ROOT, "data")
SPARK_LOG_PATH = os.path.join(PROJECT_ROOT, "logs", "spark_pipeline.log")

PROCESSED_PATH = os.path.join(DATA_DIR, "processed", "activity")
ANALYTICS_PATH = os.path.join(DATA_DIR, "analytics", "hourly_grid_summary", "part-0.parquet")
DASHBOARD_PATH = os.path.join(DATA_DIR, "dashboard_summary.csv")


@dag(
    dag_id="de3_spark_orchestration",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["de3", "spark", "network-intelligence"],
)
def de3_spark_orchestration():

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
    def run_spark_task(ingestion_results: list[dict]) -> dict:
        """Launch spark/telecom_pipeline.py as a subprocess using the
        SAME Python interpreter running this Airflow task (so it sees
        the pyspark/pandas/pyarrow installed in this venv). Raises
        (failing the task) on any non-zero exit, which stops
        publish_status_task from running."""
        job_start = datetime.now()
        logger.info("run_spark_task(): launching Spark job at %s", job_start.isoformat())
        logger.info("run_spark_task(): input-dir=%s reference=%s output-dir=%s",
                    de2.RAW_DIR, REFERENCE_PATH, DATA_DIR)

        cmd = [
            sys.executable, SPARK_SCRIPT,
            "--input-dir", de2.RAW_DIR,
            "--reference", REFERENCE_PATH,
            "--output-dir", DATA_DIR,
            "--log-file", SPARK_LOG_PATH,
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)

        # Surface the Spark job's own stdout/stderr into the Airflow task log
        # regardless of outcome, so failures are diagnosable from the UI.
        logger.info("---- telecom_pipeline.py stdout ----\n%s", result.stdout[-5000:])
        logger.info("---- telecom_pipeline.py stderr ----\n%s", result.stderr[-5000:])

        job_end = datetime.now()
        elapsed = (job_end - job_start).total_seconds()

        if result.returncode != 0:
            logger.error("run_spark_task(): Spark job FAILED (exit=%d) after %.1fs",
                        result.returncode, elapsed)
            raise RuntimeError(
                f"Spark job failed with exit code {result.returncode}. "
                f"See task logs above and {SPARK_LOG_PATH} for detail."
            )

        logger.info("run_spark_task(): Spark job SUCCEEDED after %.1fs", elapsed)
        return {"status": "SUCCESS", "elapsed_seconds": elapsed, "start": job_start.isoformat(), "end": job_end.isoformat()}

    @task
    def publish_status_task(spark_result: dict) -> dict:
        """Confirm the expected analytics outputs exist and are
        non-empty. Reads only -- never recomputes. If this runs at
        all, run_spark_task already succeeded (default trigger rule),
        so a missing file here means write_outputs() silently failed
        to write what it claimed to."""
        checks = {
            "processed_activity_dir_exists": os.path.isdir(PROCESSED_PATH),
            "analytics_parquet_exists": os.path.isfile(ANALYTICS_PATH),
            "dashboard_summary_exists": os.path.isfile(DASHBOARD_PATH),
        }

        date_partitions = []
        if checks["processed_activity_dir_exists"]:
            date_partitions = sorted(
                d for d in os.listdir(PROCESSED_PATH)
                if os.path.isdir(os.path.join(PROCESSED_PATH, d))
            )
        checks["date_partition_count"] = len(date_partitions)

        analytics_size_bytes = (
            os.path.getsize(ANALYTICS_PATH) if checks["analytics_parquet_exists"] else 0
        )
        checks["analytics_size_bytes"] = analytics_size_bytes

        all_ok = (
            checks["processed_activity_dir_exists"]
            and checks["analytics_parquet_exists"]
            and checks["dashboard_summary_exists"]
            and checks["date_partition_count"] > 0
            and analytics_size_bytes > 0
        )
        checks["all_ok"] = all_ok

        logger.info("publish_status_task(): %s", checks)
        logger.info("publish_status_task(): date partitions found: %s", date_partitions)

        if not all_ok:
            raise RuntimeError(
                f"Analytics outputs missing or empty after a reportedly successful "
                f"Spark run: {checks}"
            )

        logger.info("publish_status_task(): all analytics outputs verified present. "
                    "Spark job elapsed=%.1fs", spark_result["elapsed_seconds"])
        return checks

    files = detect_files_task()
    ingestion_results = validate_and_route_task(files)
    spark_result = run_spark_task(ingestion_results)
    publish_status_task(spark_result)


de3_spark_orchestration()
