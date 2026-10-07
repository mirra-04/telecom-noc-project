# Project Report: Network Operations & Predictive Intelligence

## 1. Executive Summary

This project implements a full end-to-end telecom network intelligence solution spanning data ingestion, canonical analytics, warehouse modeling, FastAPI service exposure, a React dashboard, and machine learning risk analysis.

The work is organized in six completed phases:

1. Phase 1 — Data profiling and alert generation
2. Phase 2 — Spark processing and geospatial enrichment
3. Phase 3 — Data engineering and warehouse orchestration
4. Phase 4 — FastAPI network services
5. Phase 5 — React operational dashboard
6. Phase 6 — ML feature engineering, training, anomaly detection, and risk scoring

The solution preserves project guardrails from the trainer guide:

- Canonical hourly grid grain at one `grid_id` per `timestamp`
- No raw country-code detail leakage into analytics/API/ML/dashboard layers
- Activity fields treated as proportional activity measures, not count metrics
- High activity and risk are framed as investigation signals instead of confirmed faults
- GeoJSON joins use `properties.cellId` only
- `AS_OF` is defined as the maximum analytics timestamp
- Claude/AI layers consume curated evidence only, not raw warehouse extracts

## 2. Overall Architecture

The project follows a layered architecture:

- Raw telecom activity data is processed into canonical grid-hour analytics
- Spark + warehouse store the curated operational data model
- FastAPI exposes curated endpoints for network, feature, prediction, and status queries
- React dashboard consumes API data and the static Milan GeoJSON reference
- ML model and scoring artifacts produce operational risk and anomaly signals

Core operational data flows:

- daily source activity data
- Spark transformations and enrichment
- canonical fact tables in SQLite warehouse
- persisted feature table for machine learning
- model/artifact outputs for risk and anomaly scoring
- API availability for dashboard and integration clients

## 3. Phase 1 — Source Data Profiling and Alerting

### Objective
Understand the raw telecom activity inputs and prepare the data model that the rest of the stack depends on.

### Implemented work
- Source profiling and schema inspection
- Cleaning and normalization of telecom activity data
- Timestamp derivation and aggregation to hourly grid-level operations
- KPI generation and activity feature preparation
- NP3-style rule-based alert generation

### Key files
- `phase1/p1_profile.py`
- `phase1/usage_processor.py`
- `phase1/np3_alerts.py`

### Outcomes
- Established the base activity representation and KPI structure
- Produced operational alerts based on meaningful deviation logic
- Learned the data quality constraints and activity semantics required by subsequent phases

## 4. Phase 2 — Spark Processing and Geospatial Enrichment

### Objective
Transform source activity into canonical analytics and enrich them with spatial context.

### Implemented work
- Spark ingestion and baseline cleaning
- Aggregation to the canonical `grid_id` + `timestamp` grain
- Geospatial enrichment for grid-cell alignment and cell references
- Performance-oriented processing updates to keep large-scale data handling efficient
- Data persistence to processed parquet/warehouse outputs

### Key files
- `phase2/sp1_ingestion.py`
- `phase2/sp2_cleaning.py`
- `phase2/sp3_aggregations.py`
- `phase2/sp4_geospatial.py`
- `phase2/sp5_performance.py`
- `phase2/sp6_write_data.py`

### Outcomes
- Built the canonical analytics layer used by downstream ML/API/dashboard components
- Ensured geospatial joins align with `properties.cellId`
- Produced a valid processed data model for warehouse loading and operational queries

## 5. Phase 3 — Data Engineering and Warehouse Orchestration

### Objective
Create the operational pipeline, publish analytics into the warehouse, and establish status monitoring.

### Implemented work
- Warehouse loading and validation
- Airflow/DAG-oriented orchestration design and task sequencing
- Pipeline status and health reporting
- Quality checks and failure handling patterns
- Integration of ML2 feature generation and ML6 scoring in the orchestration flow

### Key files
- `phase3/de2_ingestion.py`
- `phase3/de2_dag.py`
- `phase3/de3_dag.py`
- `phase3/de6_warehouse_load.py`
- `phase3/de7_dag.py`
- `phase3/DE1_architecture_design.md`
- `phase3/DE4_batch_vs_streaming.md`
- `phase3/DE5_storage_strategy.md`
- `phase3/DE8_failure_matrix.md`

### Outcomes
- Produced the warehouse with canonical operational records
- Established reproducible `AS_OF` semantics and status tracking
- Connected upstream pipelines to later ML and API layers

## 6. Phase 4 — FastAPI Network Services

### Objective
Expose curated operational intelligence over stable REST APIs for the dashboard and ML consumers.

### Implemented APIs
- API1 — `/network/summary`
- API2 — `/network/grid/{grid_id}`
- API3 — `/network/hotspots` and `/network/alerts`
- API4 — `/network/grid/{grid_id}/features`
- API5 — `/network/predict-risk`
- API6 — `/pipeline/status` and `/network/grid/{grid_id}/location`

### Key files
- `phase4/app/main.py`
- `phase4/app/api1.py` / `api1_service.py`
- `phase4/app/api2.py` / `api2_service.py`
- `phase4/app/api3.py` / `api3_service.py`
- `phase4/app/api4.py` / `api4_service.py`
- `phase4/app/api5.py` / `api5_service.py`
- `phase4/app/api6.py` / `api6_service.py`
- `phase4/tests/`

### Outcomes
- Built a stable, contract-driven API layer
- Verified endpoints for network summary, hotspot and alert views, feature service, prediction service, and operational status
- Added validation and regression coverage for expected behavior

## 7. Phase 5 — React Network Operations Dashboard

### Objective
Deliver a lightweight NOC dashboard that consumes the APIs and highlights operational signals visually.

### Implemented features
- App shell and navigation
- Network overview metrics
- Grid explorer
- Hotspots and alerts section
- Milan map rendering with grid overlay and hotspot highlighting
- Predictive risk page

### Key files
- `phase5/src/App.jsx`
- `phase5/src/api.js`
- `phase5/src/re1_shell.jsx`
- `phase5/src/re2_overview.jsx`
- `phase5/src/re3_grid_explorer.jsx`
- `phase5/src/re4_hotspots.jsx`
- `phase5/src/re5_risk.jsx`
- `phase5/public/reference/milano-grid.geojson`

### Outcomes
- Completed the dashboard UI for the operational workflow
- Connected the user interface to API responses without reading warehouse data directly
- Added map rendering using the static GeoJSON and selected hotspot overlays

## 8. Phase 6 — Machine Learning and Risk Scoring

### Objective
Generate ML-ready features, train a model, add anomaly logic, and score all grids for operational attention.

### Implemented work
- ML1 — problem framing and objective definition
- ML2 — engineered feature rows with leakage-safe logic and persisted output
- ML3 — chronological logistic regression classification training
- ML4 — anomaly baseline scoring for historical deviations
- ML5 — model serving through FastAPI
- ML6 — batch risk scoring across all persisted features and top-priority report generation

### Key files
- `phase6/ml1_problem_statement.md`
- `phase6/ml/features.py`
- `phase6/build_features.py`
- `phase6/ml/ml3_train.py`
- `phase6/train_ml3.py`
- `phase6/ml/anomaly.py`
- `phase6/score_anomalies.py`
- `phase6/ml/batch_score.py`
- `phase6/batch_score.py`
- `phase6/tests/`

### Outcomes
- Produced persisted ML feature table
- Trained and served a real logistic-risk model
- Generated anomaly and risk score artifacts
- Produced `network_risk_scores.csv` and `top20_operational_attention.csv`
- Established a real risk signal that is framed as investigation evidence

## 9. Generated Artifacts and Data Assets

Key generated outputs include:

- `data/warehouse/network_intelligence.db`
- `data/pipeline_status_latest.json`
- `data/analytics/grid_features.parquet`
- `data/analytics/ml3_evaluation.json`
- `data/analytics/network_anomaly_scores.csv`
- `data/analytics/network_risk_scores.csv`
- `data/analytics/top20_operational_attention.csv`
- `data/models/network_risk_model.joblib`

## 10. Validation Status

The implemented phases were validated through focused tests and endpoint checks:

- Phase 4 API regression tests passed
- Phase 6 ML tests passed
- Combined phase4 + phase6 validation suite passed
- Frontend build for the dashboard passed
- Swagger/API responses were verified
- Model-serving and prediction operations were tested against real persisted assets

Representative validation result:

- 22 Phase 4/6 tests passed in the final combined project pass
- Phase 5 React build succeeded
- Phase 7 offline C1–C3 tests passed in the local foundation build

## 11. Final Assessment

The project successfully implements the six-phase trainer-guide path from raw telecom activity ingestion to operational intelligence, risk scoring, and a working dashboard. The implementation follows the intended architecture and preserves the project’s safety and evidence boundaries.

The system is now ready for broader integration work and for later Phase 7 Claude tooling built on top of curated evidence instead of raw warehouse access.

## 12. Project Summary

This is a complete operational telecom intelligence solution built across the following layers:

- data profiling and alerting
- Spark processing and geometry enrichment
- warehouse orchestration and pipeline health
- API-driven analytics services
- dashboard visualization
- predictive ML and batch scoring

The result is a structured, reproducible, and operationally useful network intelligence platform that matches the project’s architecture and trainer-guide intent.
