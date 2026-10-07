# DE4 — Batch vs Streaming Decision Workshop

## Decision Matrix

| Scenario | Source | Arrival Pattern | Required Latency | Decision | Why |
|---|---|---|---|---|---|
| **Daily activity summary** (`hourly_grid_summary`, `dashboard_summary.csv`) | `sms-call-internet-mi-*.csv` landing files | One file per day, arrives once every 24h | 24h (next-day reporting is acceptable) | **Batch** | The source itself only refreshes once a day. Streaming infrastructure would sit idle 23h59m out of every 24h — pure cost with zero latency benefit. This is the case defended below. |
| **Hypothetical live activity events** (per-event SMS/call/internet pings, not the hourly-aggregated file we actually have) | A theoretical per-event telecom feed (does not exist in this dataset — the supplied files are already hourly aggregates) | Continuous, sub-second | Seconds | **Streaming** | If Telecom Italia exposed raw per-event pings instead of pre-aggregated hourly files, waiting a full hour to batch-process would throw away the only thing streaming buys you: reacting while the spike is still happening. This is the scenario that motivates a Kafka entry point (see below) — but it requires a source we don't have. |
| **Billing report** | Aggregated warehouse (`fact_network_activity`) | Monthly/periodic batch cycle | Days (billing cycles are not real-time by nature) | **Batch** | Billing reconciliation needs completeness and auditability (every row present, every country-code category summed correctly) far more than it needs speed. A streaming billing pipeline actively works against the "wait until the batch is fully closed" requirement that billing correctness depends on. |
| **Hotspot alerts** (`network_alerts` — NP3 rule-based, later ML4/ML6 anomaly-scored) | `hourly_grid_summary`, computed from the daily batch | Currently: recomputed once per daily batch run. Would ideally react per-interval if the source were live. | Minutes, if the source supported it | **Batch today, streaming-shaped candidate later** | The alert *logic* (baseline comparison, threshold check) is naturally event-driven, but it is starved by its source: the input data itself only arrives once a day. Streaming the alert computation without a streaming source would just mean streaming a batch file — no actual latency win. This is why the current implementation (NP3/API3/RE4) is batch, and the "optional path" below shows where this would change if the source changed. |
| **Executive dashboard refresh** (`AS_OF`-driven React views) | FastAPI reading the analytics warehouse | On-demand, whenever a user loads the page | Seconds (page load), but the *underlying data* only changes once a day | **Batch backend, "live-feeling" frontend** | The dashboard *reads* fast (API response in milliseconds) but the *data behind it* is only as fresh as the last batch run — that's exactly what the `AS_OF` convention (Core Dataset Contract) exists to make honest: the UI never pretends the data is more current than the last completed batch. |
| **Model training and scoring** (ML3 classifier, ML4 anomaly baseline, ML6 batch scoring) | `network_feature_table`, built from the batch warehouse | Training: once per accumulated-history cycle. Scoring (ML6): once per daily batch run, same cadence as ingestion. | Training: hours acceptable. Scoring: same 24h cycle as the data it scores. | **Batch** | Training fundamentally requires historical windows (chronological train/test split per ML3) — there is no streaming equivalent of "look at 14 days of history." Batch scoring is scored at the same cadence the *underlying activity data* refreshes, so real-time scoring would again be streaming a batch source for no latency gain. |

## The case for batch, defended

**Daily activity summary is the clearest case.** The single fact that settles it: the source file arrives once every 24 hours, full stop. No amount of downstream infrastructure — Kafka, Spark Structured Streaming, a message queue — can make data more current than the file that hasn't arrived yet. Building a streaming pipeline here would mean paying for always-on infrastructure (a running consumer, a running stream processor, state management for windowing) to process a burst of ~2M rows once a day and then sit idle. Batch with a daily Airflow trigger is not a compromise — it is the correct match between architecture and how the data actually behaves.

## Why these files are processed as batch even though real network activity is continuous

Real mobile network activity is, by nature, a continuous stream of events — every SMS, call, and internet session happens at a specific instant. But the **data we were given** is not that stream: it is an **hourly-aggregated republication** of the original ten-minute-slot Telecom Italia release (see Core Dataset Contract, §3). Someone upstream already did the batching — one hourly bucket per grid per country-code, delivered as one CSV per day. We are two aggregation steps removed from the live event stream by the time the file reaches `data/landing/`. Treating this project's ingestion as batch is not a design choice we're making against the data's nature — it's the only choice that matches the shape of the data we actually have.

## Where Kafka could conceptually enter (without changing this training dataset)

If a future phase of this platform connected to the *actual* live network (rather than this fixed 2013 archive), Kafka would sit at exactly one point: **between the raw per-event telecom feed and the point where hourly aggregation happens.** Concretely:

```
Live per-event telecom feed (hypothetical)
        ↓
   Kafka topic (raw events)
        ↓
Spark Structured Streaming — windowed hourly aggregation
        ↓
   Same hourly_grid_summary schema we already have
        ↓
   Same warehouse, same API, same dashboard, same Claude assistant
```

This is the important part: **everything downstream of the aggregation step is unaffected.** The warehouse schema, the FastAPI contracts, the React dashboard, and the Claude tools were all built against `hourly_grid_summary` — a streaming source would just be a different way of *producing* that same table, on a shorter cycle. This is precisely why AS_OF was designed as a single configuration value rather than hardcoded anywhere: swapping batch for streaming underneath it would not require touching a single downstream consumer.

We do not build this. It requires a data source (live per-event pings) that does not exist in this project. It is drawn here as the answer to "where would this go," not as a task.

## Batch → model training, Streaming → potential real-time scoring

- **Batch is structurally required for training** (ML1–ML3): a chronological train/test split needs historical windows to exist before training starts. There is no meaningful "streaming" version of "train on the first 10 days, test on the last 4."
- **Streaming is a plausible future upgrade for scoring only** (not training): once a model exists, scoring a single grid's feature vector as its window closes is a small, fast, per-record operation — the kind of thing Kafka + a lightweight consumer calling `ml/predict.py` could do per-interval, if the input pipeline were also streaming. Today (ML6), scoring runs in batch immediately after each daily Spark run, because that's the cadence the feature table itself refreshes at — scoring faster than your input data changes buys nothing.

## Revised Architecture — streaming path drawn, not built

```
                         ┌─────────────────────────────────┐
                         │   CURRENT — BATCH (built)        │
                         │                                   │
Daily CSV (data/landing/)│                                   │
        ↓                │                                   │
   Airflow ingest/validate (DE2)                              │
        ↓                │                                   │
   data/raw/             │                                   │
        ↓                │                                   │
   Spark telecom_pipeline.py (SP1-SP7, DE3)                   │
        ↓                │                                   │
   data/processed/ + data/analytics/ (hourly_grid_summary)    │
        ↓                │                                   │
   Warehouse (fact_network_activity, dim_grid, dim_time)      │
        ↓                │                                   │
   FastAPI → React → Claude                                   │
                         └─────────────────────────────────┘

                         ┌─────────────────────────────────┐
                         │  OPTIONAL FUTURE — STREAMING     │
                         │  (NOT BUILT — requires a live    │
                         │   per-event source we don't have)│
                         │                                   │
   Live per-event feed (hypothetical)                         │
        ↓                │                                   │
   Kafka topic            │                                   │
        ↓                │                                   │
   Spark Structured Streaming (windowed hourly agg)            │
        ↓ ─────────────── joins back into the SAME            │
                           hourly_grid_summary schema          │
                         └─────────────────────────────────┘
```

The streaming branch feeds into the *same* schema the batch branch produces. This is the point of drawing it separately rather than redesigning the warehouse: streaming is an alternative **producer** of `hourly_grid_summary`, not a different downstream architecture.

## Non-goals

- We are not building Kafka, Spark Structured Streaming, or any streaming consumer in this project. This document exists to show *where* it would go and *why it isn't needed yet*, not as an implementation task.
- We are not pretending the 2013 archive is a live feed. Every "streaming" scenario above is explicitly hypothetical and labeled as such.
