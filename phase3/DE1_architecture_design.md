# DE1 — Telecom Network Data Architecture (Design Document)

## 1. Architecture Diagram (text form)

```
SOURCES
├── sms-call-internet-mi-*.csv   (daily file, arrives in landing/)
└── milano-grid.geojson          (static, lives directly in reference/, never in landing/)

                    │
                    ▼
┌─────────────────────────────────────────────────────────────┐
│ landing/      exactly what arrives, never modified/deleted   │
└─────────────────────────────────────────────────────────────┘
                    │  (schema + column-count check)
        ┌───────────┴───────────┐
        ▼                       ▼
┌───────────────┐      ┌────────────────────┐
│ raw/           │      │ quarantine/         │
│ passed files   │      │ failed files, kept   │
│                │      │ for inspection       │
└───────────────┘      └────────────────────┘
        │
        ▼  (Spark: clean + aggregate — telecom_pipeline.py)
┌─────────────────────────────────────────────────────────────┐
│ processed/     cleaned, typed, Parquet, partitioned by date  │
└─────────────────────────────────────────────────────────────┘
        │
        ▼  (Spark: aggregate to grid+hour, enrich with reference/)
┌─────────────────────────────────────────────────────────────┐
│ analytics/     hourly_grid_summary, daily_grid_summary,       │
│                hotspots, alerts, risk table                   │
└─────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────────┐
│ FastAPI   →   React (dashboard)                              │
│           →   Claude (natural-language explanation)          │
│           ↔   ML (reads analytics/ for training,              │
│                    writes predictions back to analytics/)     │
└───────────────────────────────────────────────────────────┘

reference/ (milano-grid.geojson) is joined in only at the analytics
enrichment step (broadcast join) — it is never duplicated per-row
and never treated as a daily ingestion file.
```

## 2. Layer → Format/Tool Mapping

| Layer | Format | Tool |
|---|---|---|
| `landing/` | CSV, as delivered | filesystem only |
| `raw/` | CSV, validated | filesystem only |
| `quarantine/` | CSV, rejected + reason | filesystem only |
| `reference/` | GeoJSON, static | filesystem only |
| `processed/` | Parquet, partitioned by date | Spark |
| `analytics/` | Parquet / SQL tables | Spark writes, SQL/Warehouse serves |

## 3. Tool Responsibilities (one sentence each, no overlap)

| Tool | Responsibility |
|---|---|
| **Spark** | Computes analytics tables from raw data and writes them to storage. |
| **SQL / Warehouse** | Answers queries against already-computed analytics tables; it does no computation of its own. |
| **Airflow** | Triggers and sequences the daily pipeline, and records whether each run succeeded. |
| **FastAPI** | Serves analytics data to consumers over HTTP, and nothing else. |
| **React** | Displays what FastAPI returns, as a dashboard. |
| **ML** | Learns patterns from historical analytics data to produce risk predictions. |
| **Claude** | Explains analytics/pipeline-status data in natural language, using FastAPI as its only source of truth. |

## 4. Quality Gates

**Before raw acceptance (landing → raw):**
- File matches expected schema (8 raw columns, correct types)
- `grid_id` / `timestamp` not null (else → quarantine)
- No negative activity values (else → quarantine)
- Row-count reconciliation: accepted + quarantined = input

**Before analytics publication (processed → analytics):**
- Grain check: exactly one row per `grid_id + timestamp`, no duplicates
- Round-trip validation: rows written = rows read back
- Pipeline status record written (success/failure, row counts, timestamp)
- **Known-gap tolerance check:** if row count ≠ `grid_count × hour_count`, do not auto-fail — but the gap must be logged and explained (e.g. grid 5239's 6 missing hours in the training data), never silently accepted as "close enough"

## 5. Analytics Outputs

| Table | Grain | Source Lab |
|---|---|---|
| `hourly_grid_summary` | one row per grid + hour | SP3 / SP6 |
| `daily_grid_summary` | one row per grid + day | new rollup of `hourly_grid_summary` |
| `hotspots` | top-N grids by activity, per day | SP3 |
| alerts | one row per flagged grid+hour | NP3, at production scale |
| risk table | grid_id + timestamp + risk_score | future ML phase — shape defined now |

## 6. Assumptions
- One landing file arrives per day, matching the `sms-call-internet-mi-*.csv` naming pattern.
- `milano-grid.geojson` does not change; if it ever does, it is a manual, versioned reference update, not a daily ingestion event.
- Local/single-machine Spark is sufficient for this project's data volume; a real production deployment would size a cluster separately.
- Consumers (React, Claude, ML) never read raw files directly — FastAPI is the only interface into `analytics/`.

## 7. Non-Goals
- This architecture does not handle real-time/streaming ingestion — it is strictly daily batch.
- It does not define network capacity or congestion metrics — no such data exists in the source (per the Core Dataset Contract).
- It does not specify Airflow's retry/alerting configuration in detail — that is DE2/DE3's scope, not DE1's.
- It does not cover multi-region or multi-city support — Milan only.
