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

LANDING_DIR = "/home/mirrag/project_data/data/landing"
RAW_DIR = "/home/mirrag/project_data/data/raw"
REJECTED_DIR = "/home/mirrag/project_data/data/rejected"
LOG_DIR = "/home/mirrag/project_data/logs"
INGESTION_LOG_PATH = os.path.join(LOG_DIR, "ingestion_log.csv")

# NOTE: these are absolute paths into native WSL storage, not relative
# paths under this script's own directory. DrvFs (WSL's bridge to
# Windows drives, e.g. /mnt/d/...) is unreliable for the sustained,
# repeated I/O this pipeline does -- confirmed twice via intermittent
# "Cannot allocate memory" crashes deep inside both Python's own file
# reads and Spark/Hadoop's local file reads. Source .py files stay on
# D:\ (edited from Windows as normal); only the data/logs directories
# that take heavy I/O moved to native storage.

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
    values, and truncated/malformed rows (wrong field count). Does not
    require every row to be checked exhaustively — reads the file once
    and flags the first disqualifying issue found, plus a row count.
    Returns (is_valid, reason_or_none, row_count)."""
    row_count = 0
    activity_cols = ["smsin", "smsout", "callin", "callout", "internet"]
    expected_field_count = len(EXPECTED_COLUMNS)

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)  # skip header, already validated by validate_schema()
        for line_num, raw_row in enumerate(reader, start=2):  # start=2: line 1 is header
            row_count += 1

            # DE8 fix: csv.DictReader silently pads short rows with
            # None for missing trailing fields, which then looked
            # indistinguishable from a legitimately null activity
            # value -- this let a truncated/corrupt row through as
            # ACCEPTED. Checking the raw field count explicitly catches
            # truncation before it can be mistaken for a null.
            if len(raw_row) != expected_field_count:
                return (False,
                        f"truncated_or_malformed_row at line {line_num}: "
                        f"expected {expected_field_count} fields, got {len(raw_row)}",
                        row_count)

            row = dict(zip(EXPECTED_COLUMNS, raw_row))

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
def is_duplicate_ingestion(filename):
    """Check ingestion_log.csv for a prior ACCEPTED entry for this exact
    filename. Returns True if this file has already been successfully
    ingested before -- a re-dropped file under the same name is a
    duplicate-ingestion attempt (WARN), not a fresh arrival."""
    if not os.path.exists(INGESTION_LOG_PATH):
        return False
    with open(INGESTION_LOG_PATH, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("filename") == filename and row.get("status") == "ACCEPTED":
                return True
    return False


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
    #
    # Compare in chunks rather than f1.read() == f2.read(). Reading an
    # entire ~240MB file in one call is exactly the I/O pattern that
    # triggers DrvFs's intermittent "Cannot allocate memory" error when
    # source/dest live under a Windows-drive mount (/mnt/d/...); chunked
    # reads verify the same bytes without ever holding a huge buffer or
    # issuing one giant read() syscall.
    CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
    identical = True
    with open(filepath, "rb") as f1, open(dest_path, "rb") as f2:
        while True:
            chunk1 = f1.read(CHUNK_SIZE)
            chunk2 = f2.read(CHUNK_SIZE)
            if chunk1 != chunk2:
                identical = False
                break
            if not chunk1:  # both exhausted at the same point
                break

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
    """Full pipeline for a single file: check for duplicate ingestion,
    validate schema, validate quality, route, log. Returns a summary dict."""
    filename = os.path.basename(filepath)
    processed_at = datetime.now().isoformat()

    # DE8 control #1: duplicate ingestion detection (WARN, not
    # REJECT/FAIL). A file we've already successfully accepted, showing
    # up again under the same name, isn't dangerous to reprocess (the
    # copy is idempotent) but IS worth flagging -- it usually means an
    # operator mistake (wrong file re-dropped) rather than a fresh
    # day's data. We skip re-copying/re-logging as a fresh ACCEPTED
    # entry and instead log a distinguishable DUPLICATE status.
    if is_duplicate_ingestion(filename):
        logger.warning("process_one_file(): %s already ACCEPTED previously -- "
                        "DUPLICATE ingestion attempt, skipping reprocessing", filename)
        write_ingestion_log(filename, "DUPLICATE", 0,
                             "already_accepted_previously", processed_at)
        return {"filename": filename, "status": "DUPLICATE",
                "reason": "already_accepted_previously"}

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