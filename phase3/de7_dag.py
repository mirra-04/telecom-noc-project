"""
DE7 — End-to-End Airflow Orchestration

Runs the complete batch flow from one trigger:
    ingest -> validate -> spark_process -> load_warehouse -> quality_check -> notify

This DAG contains NO cleaning/aggregation/warehouse-modelling logic of
its own. Every task calls into an existing, already-validated component:

    ingest / validate  -> de2_ingestion.py   (DE2, unchanged)
    spark_process       -> spark/telecom_pipeline.py (SP7, unchanged logic;
                            DE7 added one small block that persists the
                            stats it already computes as JSON -- see that
                            file's module docstring)
    load_warehouse       -> phase3/de6_warehouse_load.py (DE6, unchanged)
    ml_features / ml_score -> Phase 6 feature generation and batch scoring
                              after warehouse publication
    quality_check        -> NEW in DE7: reads the outputs of the tasks
                            above and writes ONE machine-readable pipeline
                            status record. This is the file API6, C3, C12
                            and C14 will read later -- it must never be
                            just a log line.
    notify                -> NEW in DE7: reads quality_check's own output
                            and logs a clear SUCCESS/FAILURE line.

Failure behaviour: default Airflow trigger rule (all_success) is used
throughout, so a failure at any task stops everything downstream of it.
Concretely:
    - run_spark_task fails            -> load_warehouse_task, quality_check_task,
                                          notify_task never run
    - load_warehouse_task fails        -> quality_check_task, notify_task never run
    - quality_check_task fails         -> notify_task never runs
This is what "quality_check/notify depend on load_warehouse success"
means in practice -- there is no manual if-checking in this file.

See docs/DE7_troubleshooting.md for the module-by-module failure map
(Student Activity 7).
"""

from __future__ import annotations

import glob
import json
import logging
import os
import sqlite3
import subprocess
import sys
from datetime import datetime, timedelta

from airflow.sdk import dag, task

# Airflow's DAG file processor imports this file via importlib, which
# does NOT add this file's own directory to sys.path (unlike running
# `python de7_dag.py` directly, where Python does that automatically).
# Without this, `import de2_ingestion` fails with ModuleNotFoundError
# the moment Airflow -- rather than a human -- loads this file.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import de2_ingestion as de2

logger = logging.getLogger("airflow.task")

# ---------------------------------------------------------------------
# Paths -- identical conventions to de3_dag.py and de6_warehouse_load.py,
# so this DAG drops into the same repo without inventing new layout rules.
# ---------------------------------------------------------------------
PROJECT_ROOT = "/mnt/d/Training/Project - 2"
SPARK_SCRIPT = os.path.join(PROJECT_ROOT, "spark", "telecom_pipeline.py")
WAREHOUSE_SCRIPT = os.path.join(PROJECT_ROOT, "phase3", "de6_warehouse_load.py")
FEATURE_SCRIPT = os.path.join(PROJECT_ROOT, "phase6", "build_features.py")
SCORE_SCRIPT = os.path.join(PROJECT_ROOT, "phase6", "batch_score.py")

# DATA_ROOT is deliberately NOT under PROJECT_ROOT. PROJECT_ROOT (on
# /mnt/d/, a Windows-drive mount via DrvFs) is fine for reading small
# .py source files, but is unreliable for this pipeline's heavy,
# repeated data I/O -- confirmed via intermittent "Cannot allocate
# memory" crashes inside both Python's plain file reads and Spark's
# Hadoop-backed local file reads. Only .py source stays on D:\ (still
# edited from Windows as normal); all data/logs live on native WSL
# storage instead.
DATA_ROOT = "/home/mirrag/project_data"
REFERENCE_PATH = os.path.join(DATA_ROOT, "data", "reference", "milano-grid.geojson")
DATA_DIR = os.path.join(DATA_ROOT, "data")
LOGS_DIR = os.path.join(DATA_ROOT, "logs")
SPARK_LOG_PATH = os.path.join(LOGS_DIR, "spark_pipeline.log")
SPARK_STATS_PATH = os.path.join(DATA_DIR, "spark_job_stats.json")

WAREHOUSE_DB_PATH = "/home/mirrag/warehouse/network_intelligence.db"
# NOTE: deliberately NOT under PROJECT_ROOT (/mnt/d/...). DrvFs (WSL's
# bridge to Windows drives) makes SQLite's WAL-mode writes catastrophically
# slow for a 1.68M-row fact table -- a manual timing test went from 25+
# minutes stuck on /mnt/d to 20.8s on native WSL storage. de6_warehouse_load.py
# was updated the same way (its WAREHOUSE_DIR points here too) -- this
# constant just has to agree with that script's own path.

PIPELINE_STATUS_DIR = os.path.join(DATA_DIR, "pipeline_status")
PIPELINE_STATUS_LATEST = os.path.join(PIPELINE_STATUS_DIR, "latest_status.json")
FEATURES_PATH = os.path.join(DATA_DIR, "analytics", "grid_features.parquet")
RISK_SCORES_PATH = os.path.join(DATA_DIR, "analytics", "network_risk_scores.csv")
ATTENTION_REPORT_PATH = os.path.join(DATA_DIR, "analytics", "top20_operational_attention.csv")
MODEL_PATH = os.path.join(PROJECT_ROOT, "data", "models", "network_risk_model.joblib")

NOTIFICATIONS_LOG = os.path.join(LOGS_DIR, "notifications.log")


def _run_id() -> str:
    """Prefer Airflow's own dag_run_id (traceable in the UI). Falls back
    to a timestamp-based id if called outside a live task context (e.g.
    manual local testing), so this never hard-fails just to get an id."""
    try:
        from airflow.sdk import get_current_context
        ctx = get_current_context()
        run_id = ctx["dag_run"].run_id
        if run_id:
            return run_id
    except Exception:
        pass
    return "manual_" + datetime.now().strftime("%Y%m%dT%H%M%S")


def _check_missing_expected_file(files_seen: list[str]) -> dict:
    """DE8 control #3: WARN if the most recent expected daily file is
    absent from landing/, rather than saying nothing. This does not
    fail the DAG -- the pipeline still safely reruns against whatever
    is already in raw/ -- it only flags the gap for a human to check,
    since a missing file could be legitimate (no traffic that day) or
    a genuine upstream delivery failure."""
    landing_files = sorted(glob.glob(os.path.join(de2.LANDING_DIR, de2.FILE_PATTERN)))
    if not landing_files:
        return {"missing_file_warning": True,
                "detail": "No files at all found in landing/ -- nothing to check against."}

    # Expect daily files to be contiguous by date (encoded in filename,
    # e.g. sms-call-internet-mi-2013-11-07.csv). If the most recent
    # file's date is more than 1 day older than "today" in the dataset's
    # own timeline (the latest date seen so far), flag a gap.
    import re
    dates_seen = []
    for f in landing_files:
        base = os.path.basename(f)
        match = re.search(r"(\d{4}-\d{2}-\d{2})", base)
        if match:
            try:
                dates_seen.append(datetime.strptime(match.group(1), "%Y-%m-%d").date())
            except ValueError:
                continue
    if not dates_seen:
        return {"missing_file_warning": False, "detail": "Could not parse dates from filenames."}

    dates_seen.sort()
    gaps = [
        (dates_seen[i], dates_seen[i + 1])
        for i in range(len(dates_seen) - 1)
        if (dates_seen[i + 1] - dates_seen[i]).days > 1
    ]
    if gaps:
        return {"missing_file_warning": True,
                "detail": f"Gap(s) detected in daily file sequence: {gaps}"}
    return {"missing_file_warning": False, "detail": "No gaps detected in daily file sequence."}


@dag(
    dag_id="de7_end_to_end_pipeline",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["de7", "de8", "network-intelligence", "production"],
)
def de7_end_to_end_pipeline():

    # -------------------------------------------------------------
    # ingest
    # -------------------------------------------------------------
    @task
    def detect_files_task() -> list[str]:
        files = de2.detect_files()
        if not files:
            logger.warning("No candidate files found in %s.", de2.LANDING_DIR)
        return files

    # -------------------------------------------------------------
    # validate
    # -------------------------------------------------------------
    @task
    def validate_and_route_task(files: list[str]) -> list[dict]:
        results = []
        for filepath in files:
            logger.info("--- Processing %s ---", filepath)
            result = de2.process_one_file(filepath)
            results.append(result)

        accepted = sum(1 for r in results if r["status"] == "ACCEPTED")
        rejected = sum(1 for r in results if r["status"] == "REJECTED")
        logger.info("validate_and_route_task(): %d file(s) accepted, %d rejected",
                    accepted, rejected)
        return results

    # -------------------------------------------------------------
    # spark_process
    # -------------------------------------------------------------
    @task(retries=1, retry_delay=timedelta(minutes=1))
    def run_spark_task(ingestion_results: list[dict]) -> dict:
        """Launch spark/telecom_pipeline.py as a subprocess using the
        SAME Python interpreter running this Airflow task, so it sees
        the pyspark/pandas/pyarrow installed in this venv. Raises
        (failing the task) on any non-zero exit -- this is what stops
        load_warehouse_task/quality_check_task/notify_task from
        running (default trigger rule)."""
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

        # telecom_pipeline.py just wrote this on success -- read it back
        # rather than recomputing or re-parsing logs.
        if not os.path.isfile(SPARK_STATS_PATH):
            raise RuntimeError(
                f"Spark job reported success but {SPARK_STATS_PATH} is missing -- "
                f"cannot proceed without machine-readable job stats."
            )
        with open(SPARK_STATS_PATH, "r", encoding="utf-8") as f:
            spark_stats = json.load(f)

        logger.info("run_spark_task(): Spark job SUCCEEDED after %.1fs, stats=%s",
                    elapsed, spark_stats)
        return spark_stats

    # -------------------------------------------------------------
    # load_warehouse
    # -------------------------------------------------------------
    @task
    def load_warehouse_task(spark_stats: dict) -> dict:
        """Launch phase3/de6_warehouse_load.py as a subprocess -- reuses
        the already-validated DE6 star schema build unchanged. That
        script runs its own acceptance-criteria assertions (no geometry
        in fact table, dim_grid row count, fact row count, aggregate
        match, index exists) and raises/exits non-zero if any fail, so
        a bad load surfaces here as a task failure, not silently."""
        job_start = datetime.now()
        logger.info("load_warehouse_task(): launching DE6 warehouse load at %s",
                    job_start.isoformat())

        cmd = [sys.executable, WAREHOUSE_SCRIPT]
        result = subprocess.run(cmd, capture_output=True, text=True)

        logger.info("---- de6_warehouse_load.py stdout ----\n%s", result.stdout[-5000:])
        logger.info("---- de6_warehouse_load.py stderr ----\n%s", result.stderr[-5000:])

        elapsed = (datetime.now() - job_start).total_seconds()

        if result.returncode != 0:
            logger.error("load_warehouse_task(): warehouse load FAILED (exit=%d) after %.1fs",
                        result.returncode, elapsed)
            raise RuntimeError(
                f"Warehouse load failed with exit code {result.returncode}. "
                f"See task logs above."
            )

        if not os.path.isfile(WAREHOUSE_DB_PATH):
            raise RuntimeError(
                f"Warehouse load reported success but {WAREHOUSE_DB_PATH} does not exist."
            )

        # Read the two numbers quality_check needs directly from the
        # warehouse itself -- this is the "published" layer, so this is
        # the most accurate source for rows_published and AS_OF, more
        # accurate than trusting Spark's row count a second time.
        conn = sqlite3.connect(WAREHOUSE_DB_PATH)
        try:
            fact_rows = conn.execute(
                "SELECT COUNT(*) FROM fact_network_activity"
            ).fetchone()[0]
            as_of = conn.execute(
                "SELECT MAX(timestamp) FROM fact_network_activity"
            ).fetchone()[0]
        finally:
            conn.close()

        logger.info("load_warehouse_task(): warehouse load SUCCEEDED after %.1fs, "
                    "fact_rows=%d, AS_OF=%s", elapsed, fact_rows, as_of)

        return {
            "status": "SUCCESS",
            "elapsed_seconds": round(elapsed, 2),
            "fact_rows": fact_rows,
            "as_of": as_of,
        }

    # -------------------------------------------------------------
    # ml_features
    # -------------------------------------------------------------
    @task
    def generate_features_task(warehouse_result: dict) -> dict:
        """Generate leakage-safe ML2 features after the warehouse is published."""
        environment = os.environ.copy()
        environment.update(
            {
                "NETWORK_INTELLIGENCE_DB": WAREHOUSE_DB_PATH,
                "NETWORK_FEATURES_FILE": FEATURES_PATH,
            }
        )
        result = subprocess.run(
            [sys.executable, FEATURE_SCRIPT],
            capture_output=True,
            text=True,
            env=environment,
        )
        logger.info("---- build_features.py stdout ----\n%s", result.stdout[-5000:])
        logger.info("---- build_features.py stderr ----\n%s", result.stderr[-5000:])
        if result.returncode != 0 or not os.path.isfile(FEATURES_PATH):
            raise RuntimeError("ML2 feature generation failed; risk scoring will not run.")
        return {"status": "SUCCESS", "features_path": FEATURES_PATH}

    # -------------------------------------------------------------
    # ml_score
    # -------------------------------------------------------------
    @task
    def batch_score_task(feature_result: dict) -> dict:
        """Score persisted features before quality_check and notify."""
        environment = os.environ.copy()
        environment.update(
            {
                "NETWORK_FEATURES_FILE": FEATURES_PATH,
                "NETWORK_MODEL_FILE": MODEL_PATH,
                "NETWORK_RISK_SCORES_FILE": RISK_SCORES_PATH,
                "NETWORK_ATTENTION_REPORT_FILE": ATTENTION_REPORT_PATH,
            }
        )
        result = subprocess.run(
            [sys.executable, SCORE_SCRIPT],
            capture_output=True,
            text=True,
            env=environment,
        )
        logger.info("---- batch_score.py stdout ----\n%s", result.stdout[-5000:])
        logger.info("---- batch_score.py stderr ----\n%s", result.stderr[-5000:])
        if result.returncode != 0 or not os.path.isfile(RISK_SCORES_PATH):
            raise RuntimeError("ML6 batch scoring failed; quality_check will not run.")
        return {"status": "SUCCESS", "risk_scores_path": RISK_SCORES_PATH}

    # -------------------------------------------------------------
    # quality_check
    # -------------------------------------------------------------
    @task
    def quality_check_task(
        files: list[str],
        ingestion_results: list[dict],
        spark_stats: dict,
        warehouse_result: dict,
        feature_result: dict,
        score_result: dict,
    ) -> dict:
        """Writes the ONE machine-readable pipeline status record that
        API6, C3, C12 and C14 consume later. This task performs no
        recomputation -- every number here was already produced by an
        upstream task; this only assembles and persists them.

        DE8 addition: the record now distinguishes DUPLICATE ingestion
        attempts from ACCEPTED/REJECTED, and includes a missing-expected-
        file WARN check -- so each injected fault type produces a
        genuinely distinguishable status record, not just a pass/fail."""
        run_id = _run_id()
        run_timestamp = datetime.now().isoformat()

        files_accepted = sum(1 for r in ingestion_results if r["status"] == "ACCEPTED")
        files_rejected = sum(1 for r in ingestion_results if r["status"] == "REJECTED")
        files_duplicate = sum(1 for r in ingestion_results if r["status"] == "DUPLICATE")

        missing_file_check = _check_missing_expected_file(files)

        status_record = {
            "run_id": run_id,
            "run_timestamp": run_timestamp,
            "pipeline_status": "SUCCESS",
            "task_status": {
                "ingest": "SUCCESS",
                "validate": "SUCCESS",
                "spark_process": "SUCCESS",
                "load_warehouse": "SUCCESS",
                "ml_features": feature_result["status"],
                "ml_score": score_result["status"],
            },
            # File-level ingestion audit (DE2 grain)
            "files_accepted": files_accepted,
            "files_rejected": files_rejected,
            "files_duplicate": files_duplicate,
            "file_results": ingestion_results,
            # DE8: fault-type visibility -- each of these can be checked
            # independently after an injected fault, per fault type,
            # rather than reading one opaque SUCCESS/FAILURE flag.
            "faults_detected": {
                "duplicate_ingestion_attempts": files_duplicate,
                "file_level_rejections": files_rejected,
                "missing_expected_file_warning": missing_file_check["missing_file_warning"],
                "missing_expected_file_detail": missing_file_check["detail"],
                "row_level_quarantine_count": spark_stats["rejected_count"],
                "nulls_handled_total": spark_stats["nulls_handled_total"],
            },
            # Row-level stats (DE7 required fields, Spark/SP7 grain)
            "rows_in": spark_stats["input_row_count"],
            "rows_rejected": spark_stats["rejected_count"],
            "nulls_handled": spark_stats["nulls_handled_total"],
            # Published layer (warehouse grain) -- what actually reached
            # the warehouse consumers, per learner decision on this field
            "rows_published": warehouse_result["fact_rows"],
            "as_of": warehouse_result["as_of"],
        }

        os.makedirs(PIPELINE_STATUS_DIR, exist_ok=True)

        # Timestamped archive copy (one per run, never overwritten) ...
        archive_path = os.path.join(PIPELINE_STATUS_DIR, f"status_{run_id}.json")
        with open(archive_path, "w", encoding="utf-8") as f:
            json.dump(status_record, f, indent=2)

        # ... plus a stable "latest" pointer for consumers that just want
        # the current pipeline health without knowing run ids.
        with open(PIPELINE_STATUS_LATEST, "w", encoding="utf-8") as f:
            json.dump(status_record, f, indent=2)

        logger.info("quality_check_task(): wrote %s and %s", archive_path, PIPELINE_STATUS_LATEST)
        logger.info("quality_check_task(): rows_in=%d rows_rejected=%d nulls_handled=%d "
                    "rows_published=%d AS_OF=%s",
                    status_record["rows_in"], status_record["rows_rejected"],
                    status_record["nulls_handled"], status_record["rows_published"],
                    status_record["as_of"])

        return status_record

    # -------------------------------------------------------------
    # notify
    # -------------------------------------------------------------
    @task
    def notify_task(status_record: dict) -> None:
        """Reads quality_check's own output (never recomputes) and logs
        a single clear line to logs/notifications.log plus the task log.
        This is intentionally simple -- a lightweight log-based notifier
        satisfies DE7's requirement without inventing an email/Slack
        integration this lab doesn't need."""
        os.makedirs(LOGS_DIR, exist_ok=True)

        line = (
            f"{status_record['run_timestamp']} | run_id={status_record['run_id']} | "
            f"pipeline_status={status_record['pipeline_status']} | "
            f"rows_in={status_record['rows_in']} | "
            f"rows_rejected={status_record['rows_rejected']} | "
            f"rows_published={status_record['rows_published']} | "
            f"AS_OF={status_record['as_of']}\n"
        )

        with open(NOTIFICATIONS_LOG, "a", encoding="utf-8") as f:
            f.write(line)

        logger.info("notify_task(): %s", line.strip())
        logger.info("notify_task(): PIPELINE RUN COMPLETE -- %s",
                    status_record["pipeline_status"])

    # -------------------------------------------------------------
    # Wiring: ingest -> validate -> spark_process -> load_warehouse
    #         -> ml_features -> ml_score -> quality_check -> notify
    # -------------------------------------------------------------
    files = detect_files_task()
    ingestion_results = validate_and_route_task(files)
    spark_stats = run_spark_task(ingestion_results)
    warehouse_result = load_warehouse_task(spark_stats)
    feature_result = generate_features_task(warehouse_result)
    score_result = batch_score_task(feature_result)
    status_record = quality_check_task(
        files, ingestion_results, spark_stats, warehouse_result, feature_result, score_result
    )
    notify_task(status_record)


de7_end_to_end_pipeline()