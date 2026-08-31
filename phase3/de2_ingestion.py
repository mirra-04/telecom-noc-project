"""
DE2 — Landing-to-Raw Ingestion Flow

Detects daily activity files in data/landing/, validates schema and
minimum quality, and routes them to data/raw/ (valid) or
data/rejected/ (invalid, with a stated reason). Writes an audit log
entry for every file seen, regardless of outcome.

milano-grid.geojson is explicitly excluded — it lives in
data/reference/ and never passes through this daily ingestion flow.
"""

import csv
import glob
import json
import logging
import os
import shutil
from datetime import datetime, date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("ingestion")

LANDING_DIR = "../data/landing"
RAW_DIR = "../data/raw"
REJECTED_DIR = "../data/rejected"
LOG_DIR = "../logs"
INGESTION_LOG_PATH = os.path.join(LOG_DIR, "ingestion_log.csv")

EXPECTED_COLUMNS = [
    "datetime", "CellID", "countrycode",
    "smsin", "smsout", "callin", "callout", "internet",
]

FILE_PATTERN = "sms-call-internet-mi-*.csv"  # deliberately excludes *.geojson


# =====================================================================
def detect_files(landing_dir=LANDING_DIR):
    """Find files matching the daily activity CSV naming pattern.
    Does NOT match milano-grid.geojson or anything else."""
    pattern = os.path.join(landing_dir, FILE_PATTERN)
    files = sorted(glob.glob(pattern))
    logger.info("detect_files(): found %d candidate file(s) in %s", len(files), landing_dir)
    return files


# =====================================================================
def validate_schema(filepath):
    """Check the file has exactly the expected 8 columns, in a
    case-sensitive match. Returns (is_valid, reason_or_none)."""
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
    except StopIteration:
        return False, "empty_file_no_header"
    except Exception as e:
        return False, f"unreadable_file: {e}"

    missing = [c for c in EXPECTED_COLUMNS if c not in header]
    extra = [c for c in header if c not in EXPECTED_COLUMNS]

    if missing:
        return False, f"missing_columns: {missing}"
    if extra:
        return False, f"unexpected_columns: {extra}"
    if header != EXPECTED_COLUMNS:
        return False, f"column_order_mismatch: got {header}"

    return True, None


# =====================================================================
def validate_minimum_quality(filepath):
    """Row-level spot checks: malformed timestamps, negative activity
    values. Does not require every row to be checked exhaustively —
    reads the file once and flags the first disqualifying issue found,
    plus a row count. Returns (is_valid, reason_or_none, row_count)."""
    row_count = 0
    activity_cols = ["smsin", "smsout", "callin", "callout", "internet"]

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for line_num, row in enumerate(reader, start=2):  # start=2: line 1 is header
            row_count += 1

            ts_raw = row.get("datetime", "")
            try:
                datetime.strptime(ts_raw, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return False, f"malformed_timestamp at line {line_num}: '{ts_raw}'", row_count

            for col in activity_cols:
                val = row.get(col, "")
                if val == "" or val is None:
                    continue  # nulls are expected/valid, per NP1
                try:
                    num = float(val)
                except ValueError:
                    return False, f"non_numeric_value in {col} at line {line_num}: '{val}'", row_count
                if num < 0:
                    return False, f"negative_value in {col} at line {line_num}: {num}", row_count

    return True, None, row_count


# =====================================================================
def route_file(filepath, is_valid, reason, row_count):
    """Copy (not move-then-verify-later) the file to raw/ or rejected/,
    preserving the original filename. The landing/ copy is left in
    place — landing/ is defined as 'never modified, never deleted'
    per the DE1 architecture design."""
    filename = os.path.basename(filepath)
    dest_dir = RAW_DIR if is_valid else REJECTED_DIR
    os.makedirs(dest_dir, exist_ok=True)
    dest_path = os.path.join(dest_dir, filename)

    shutil.copy2(filepath, dest_path)  # copy2 preserves metadata/timestamps

    # Verify byte-for-byte the destination matches the source —
    # required per Learner Validation: "raw files preserved unchanged"
    with open(filepath, "rb") as f1, open(dest_path, "rb") as f2:
        identical = f1.read() == f2.read()

    logger.info(
        "route_file(): %s -> %s (identical_bytes=%s)",
        filename, dest_dir, identical,
    )
    if not identical:
        raise IOError(f"Byte-for-byte mismatch after copying {filename} to {dest_dir}!")

    return dest_path


# =====================================================================
def write_ingestion_log(filename, status, row_count, reason, processed_at):
    """Append one row per file to the ingestion audit log CSV.
    Every file gets a log entry, valid or rejected."""
    os.makedirs(LOG_DIR, exist_ok=True)
    file_exists = os.path.exists(INGESTION_LOG_PATH)

    with open(INGESTION_LOG_PATH, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["filename", "status", "row_count", "reason", "processed_at"])
        writer.writerow([filename, status, row_count, reason or "", processed_at])

    logger.info("write_ingestion_log(): logged %s as %s", filename, status)


# =====================================================================
def process_one_file(filepath):
    """Full pipeline for a single file: validate schema, validate
    quality, route, log. Returns a summary dict."""
    filename = os.path.basename(filepath)
    processed_at = datetime.now().isoformat()

    schema_ok, schema_reason = validate_schema(filepath)
    if not schema_ok:
        route_file(filepath, is_valid=False, reason=schema_reason, row_count=0)
        write_ingestion_log(filename, "REJECTED", 0, schema_reason, processed_at)
        return {"filename": filename, "status": "REJECTED", "reason": schema_reason}

    quality_ok, quality_reason, row_count = validate_minimum_quality(filepath)
    if not quality_ok:
        route_file(filepath, is_valid=False, reason=quality_reason, row_count=row_count)
        write_ingestion_log(filename, "REJECTED", row_count, quality_reason, processed_at)
        return {"filename": filename, "status": "REJECTED", "reason": quality_reason}

    route_file(filepath, is_valid=True, reason=None, row_count=row_count)
    write_ingestion_log(filename, "ACCEPTED", row_count, None, processed_at)
    return {"filename": filename, "status": "ACCEPTED", "reason": None}


# =====================================================================
def main():
    logger.info("=" * 60)
    logger.info("DE2 INGESTION RUN START")
    logger.info("=" * 60)

    files = detect_files()
    if not files:
        logger.warning("No candidate files found in %s. Nothing to do.", LANDING_DIR)
        return

    results = []
    for filepath in files:
        logger.info("--- Processing %s ---", os.path.basename(filepath))
        result = process_one_file(filepath)
        results.append(result)

    logger.info("=" * 60)
    logger.info("DE2 INGESTION RUN SUMMARY")
    for r in results:
        logger.info("  %s -> %s%s", r["filename"], r["status"],
                    f" ({r['reason']})" if r["reason"] else "")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()