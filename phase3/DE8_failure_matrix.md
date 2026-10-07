# DE8 — Failure Handling Matrix

For each fault: what's OBSERVED (evidence from code/logs) vs INFERRED (the
design judgment call), the assigned operational action, and where it's
enforced.

| # | Fault | Observed (evidence) | Assigned Action | Enforced where | Inferred rationale |
|---|---|---|---|---|---|
| 1 | Missing daily file | `detect_files_task` returns an empty/short list; no exception is raised anywhere in the current code for "expected file absent" | **WARN** | NEW: `check_missing_expected_file()` in `de7_dag.py`, surfaced in `quality_check_task`'s status record | A missing file isn't necessarily an error — it could be a legitimately quiet day upstream. Failing the whole DAG for this would be too aggressive; the pipeline should still run against whatever's already in `raw/` (safe, idempotent) while flagging the gap for a human to check. |
| 2 | Duplicate file / duplicate ingestion attempt | `de2_ingestion.py`'s `route_file()` unconditionally `shutil.copy2()`s and logs every call, with no check for "have I already accepted this filename" | **WARN** (skip reprocessing, log as DUPLICATE) | NEW: duplicate check added to `process_one_file()` against `ingestion_log.csv` | Reprocessing a re-dropped file isn't dangerous by itself (copy2 overwrites, Spark re-reads the same content) — but it wastes a full pipeline run and could mask an operator mistake (wrong file re-uploaded under the same name). WARN + skip is safer than silent silent reprocessing, and less disruptive than REJECT/FAIL. |
| 3 | Malformed timestamps | `de2_ingestion.py`'s `validate_minimum_quality()` already calls `datetime.strptime()` per row and returns `False` on the first failure | **REJECT** (whole file, at ingestion) | EXISTING: `validate_minimum_quality()` → `route_file()` to `data/rejected/` | A malformed timestamp breaks the grain key (`grid_id`, `timestamp`) the entire warehouse is built on — this can't be partially trusted, so the file is rejected before it ever reaches Spark. |
| 4 | Negative activity values | Two layers already implement this: (a) `de2_ingestion.py` rejects the *file* if any row has a negative value; (b) `telecom_pipeline.py`'s `clean()` additionally quarantines *individual rows* with negative values into `rejected_df` with `reject_reason="negative_activity_value"`, without failing the job | **REJECT** (file, at ingestion) **+ QUARANTINE** (row, at Spark) | EXISTING: both layers | Defense in depth: if a bad row somehow got past ingestion, Spark's `clean()` still won't silently aggregate physically-impossible negative traffic into the warehouse — it quarantines that row and continues (`accepted_count + rejected_count == input_row_count` reconciliation proves nothing was silently dropped). |
| 5 | Unexpected/missing column | `de2_ingestion.py`'s `validate_schema()` checks exact column match (`missing`, `extra`, and order) before any row is read | **REJECT** (whole file, at ingestion) | EXISTING: `validate_schema()` → `route_file()` to `data/rejected/` | A schema mismatch is unrecoverable without human judgment about what the columns actually mean now — rejecting immediately (before spending time on quality checks or Spark) is the cheapest and safest failure point. |
| 6 | Partially corrupt file (e.g. truncated mid-row) | `csv.DictReader` in `validate_minimum_quality()` will either raise on the malformed row or produce a row with missing/misaligned fields, which then fails the `datetime.strptime()` or numeric-cast checks already in place | **REJECT** (whole file, at ingestion) | EXISTING: same path as #3 — corruption almost always surfaces as a malformed timestamp or non-numeric value | A partially corrupt file is not safe to partially accept — line-level corruption can silently shift columns, which the schema/type checks unequal to a truncation-aware parser can't always catch. Reject the whole file and require a human to investigate/redeliver rather than guess which rows are trustworthy. |
| 7 | Spark job failure (e.g. reference file missing, executor crash, row-reconciliation mismatch) | `telecom_pipeline.py` already fails fast and exits non-zero on: zero input files, missing `--reference`, reconciliation mismatch, or grain violation — all raise before any output is written | **FAIL**, then **RETRY** once (new) | EXISTING: `telecom_pipeline.py` raises/exits 1; NEW: `run_spark_task` now has `retries=1` in `de7_dag.py` | Most Spark failures in this environment have been transient/environmental (WSL/DrvFs I/O flakiness — see project history), not data problems. One automatic retry recovers from transient infra issues for free; if it fails twice, that's a strong signal it's a real data/config problem worth failing the whole DAG for (`all_success` trigger rule stops `load_warehouse_task` etc.) rather than retrying forever. |

## Controls implemented and demonstrated (≥3 required)

1. **Duplicate ingestion detection (WARN)** — `de2_ingestion.py`
2. **Retry on Spark failure (RETRY)** — `de7_dag.py`, `run_spark_task`
3. **Missing-expected-file detection (WARN)** — `de7_dag.py`, new check surfaced in `quality_check_task`
4. *(pre-existing, demonstrated not built)* **Schema/quality REJECT** — `de2_ingestion.py`
5. *(pre-existing, demonstrated not built)* **Row-level QUARANTINE + CONTINUE** — `telecom_pipeline.py` `clean()`

## Safe rerun guarantee

Both `telecom_pipeline.py` (idempotent: overwrites at the same `--output-dir`)
and `de6_warehouse_load.py` (idempotent: `DROP TABLE IF EXISTS` + full rebuild
each run) are safe to rerun from scratch at any point. The rerun demo in
Student Activity 9 confirms the `(grid_id, timestamp)` primary key on
`fact_network_activity` still holds — SQLite's own `PRIMARY KEY` constraint
would raise `IntegrityError` on a genuine duplicate insert, and DE6's own
acceptance-criteria query (`fact row count == source row count`) independently
proves no duplication occurred.
