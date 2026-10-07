# DE5 — Storage Strategy & Data Zones

## 1. Zone Map

| Zone | Path | Contents (confirmed from repo) | Role |
|---|---|---|---|
| **Landing** | `data/landing/` | Incoming daily CSVs, as delivered (currently 8 files: 7 valid days + 1 deliberately-invalid test file) | Arrival buffer. Untouched by anything except DE2's `route_file()`, which *copies* out of it — never moves or deletes. |
| **Raw** | `data/raw/` | Byte-identical copies of files that passed DE2 validation (currently 7 files) | The immutable source of truth for everything Spark reads. |
| **Rejected** | `data/rejected/` | Files that failed DE2 validation, with the reason recorded in `logs/ingestion_log.csv` | Quarantine — evidence for debugging, not a processing input. |
| **Reference** | `data/reference/` | `milano-grid.geojson` (3.2 MB, static) | Geographic lookup, joined on `properties.cellId`. Never dated, never re-ingested. |
| **Processed** | `data/processed/activity/` | Date-partitioned Parquet, cleaned/canonicalized activity data (7 partitions, ~34-40 MB each) | Cleaned country-code-grain data, one partition per `date`. |
| **Analytics** | `data/analytics/hourly_grid_summary/` | Single Parquet file, ~92 MB, 1,679,994 rows | The grid/hour grain everything downstream (API, ML, dashboard) actually reads. |
| **Logs** | `logs/` | `ingestion_log.csv` (DE2 audit trail), `spark_pipeline.log` (DE3 Spark job logs) | Audit and run history — not a data product, but a first-class zone in its own right (see below). |

## 2. Directory structure

**Processed — date-partitioned** (already implemented in SP6/SP7, confirmed on disk):

```
data/processed/activity/
    date=2013-11-01/part-0.parquet
    date=2013-11-02/part-0.parquet
    date=2013-11-03/part-0.parquet
    date=2013-11-04/part-0.parquet
    date=2013-11-05/part-0.parquet
    date=2013-11-06/part-0.parquet
    date=2013-11-07/part-0.parquet
```

Partitioning by `date` earns its keep here: it lets a Spark reader or a downstream job filter to specific days without scanning the whole processed layer, and it's exactly how new arrivals get added — one new partition per day, with no rewrite of existing partitions.

**Reference — flat, not partitioned:**

```
data/reference/
    milano-grid.geojson
```

No `date=` folder, no versioning by date. The grid geometry does not change day to day — partitioning it would imply a temporal dimension that doesn't exist and would break the SP4/RE4 join logic, which expects one static file at one static path.

**Analytics** is currently a single unpartitioned file (`hourly_grid_summary/part-0.parquet`) rather than partitioned by date, and that is a deliberate choice, not an oversight: `write_outputs()` in `telecom_pipeline.py` re-reads the *entire* `data/raw/` directory on every run and rewrites the whole analytics table, because the grid/hour grain (Core Dataset Contract §2) has to reflect the full accumulated history for hotspot ranking and cross-day comparisons to work. Partitioning it by date would fragment queries that need "top grids across all history" into a multi-partition scan for no benefit at this data volume (92 MB total).

## 3. Why raw is retained unchanged

If `data/raw/` were ever edited or partially rewritten, three things become impossible:

1. **Reproducibility** — DE3's Spark job could no longer be re-run to reproduce the exact same `hourly_grid_summary` it produced before, because the input would have silently changed. We proved this table is reproducible (1,679,994 rows, grid 5161 busiest) precisely *because* raw never changes between runs.
2. **The DE2 byte-identity guarantee** — `route_file()` explicitly re-reads both the source and destination files and raises `IOError` if they don't match byte-for-byte. That check is worthless if raw is later allowed to drift from what was actually validated.
3. **Debugging a bad downstream number** — if a KPI looks wrong, the only way to know whether the bug is in Spark's aggregation logic or in the source data is to have an unmodified copy of exactly what was ingested. A mutable raw zone destroys that audit trail permanently — you cannot un-corrupt evidence after the fact.

## 4. Append vs overwrite, per layer

| Zone | Write mode | Why |
|---|---|---|
| Landing | **Append** (new files arrive; existing files are never edited) | Files are named by date, so a new day's file cannot collide with an existing one. |
| Raw | **Append-only, write-once** | Each file is written to raw exactly once, by DE2. `route_file()` uses `shutil.copy2`, which would overwrite if re-run on the same file — this is the one place in the pipeline where idempotency on rerun is a known open question (flagged in the DE2 acceptance criteria as "the behaviour is defined and logged" — currently it re-copies and re-logs identically, which is safe but not explicitly deduplicated). |
| Rejected | **Append** | Same reasoning as landing — each rejected file is quarantined once, under its own name. |
| Reference | **Overwrite, manual only** | The GeoJSON is replaced only if Milan's grid definition itself changes (in practice: never, in this project). It is never written by any automated task. |
| Processed | **Append by partition, overwrite within a partition** | A new day adds a new `date=` folder (append at the partition level). Re-running the pipeline for a day that's already been processed overwrites *that partition only* — this is what makes reprocessing a single bad day safe without touching the other six. |
| Analytics | **Full overwrite on every run** | Confirmed behavior: `write_outputs()` rewrites `hourly_grid_summary/part-0.parquet` in its entirety every time, because the grid/hour grain requires the complete accumulated history to be correct (this is exactly what we saw happen in DE3's failure test — and why running Spark against a partial `data/raw/` would have silently shrunk the whole table if we hadn't caught it). |
| Logs | **Append-only** | `ingestion_log.csv` and `spark_pipeline.log` both grow by adding rows/lines per run — confirmed on disk (the ingestion log already has duplicate filenames from separate DAG runs, which is correct: it's a run history, not a current-state table). |

## 5. Retention

- **Raw**: retain indefinitely for this project's scope (a 7-day training archive is small — ~600 MB total). In a production system, raw retention would be driven by how far back reprocessing needs to reach (matches the ML phase's minimum-history requirements: 10-14 days), plus any regulatory requirement on source-data retention. Raw is never retained "forever by default" without that justification — it's retained as long as something might legitimately need to reprocess from it.
- **Logs**: retain longer than raw, not shorter — `ingestion_log.csv` is the only historical record of *what was rejected and why*, which remains useful evidence long after the underlying rejected file itself might be cleaned up. Log retention should be measured in reporting cycles (e.g., "keep audit logs for the life of the training programme"), not tied to the retention of the data zones it describes.

## 6. Storage contract (summary table — full detail above)

| Zone | Format | Write mode | Retention |
|---|---|---|---|
| Landing | CSV, as delivered | Append (new files only) | Until routed to raw/rejected; not a long-term store |
| Raw | CSV, byte-identical to landing | Write-once (append at file level) | Retain for the life of the project / as long as reprocessing might be needed |
| Rejected | CSV, byte-identical to landing | Append | Retained alongside its log entry for debugging |
| Reference | GeoJSON | Overwrite, manual only | Indefinite — static reference data |
| Processed | Parquet, partitioned by `date` | Append new partitions; overwrite within a reprocessed partition | Tied to raw retention (it's a deterministic function of raw) |
| Analytics | Parquet, single file (grid/hour grain) | Full overwrite every run | Tied to raw retention (fully reproducible from raw + reference) |
| Logs | CSV / plain text | Append-only | Outlives the data zones it audits |

## Acceptance criteria check

- [x] Every zone has a stated format, write mode, and retention position (table above).
- [x] Reference is explicitly **not** date-partitioned (§2), and the rationale is stated (no temporal dimension exists for a static grid definition).
- [x] `logs/` appears as a first-class zone in the strategy (§1, §4, §5) — not just listed as a folder that happens to exist.
