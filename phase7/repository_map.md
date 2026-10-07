# C4 Repository Map

## Top-level responsibilities

| Directory | Responsibility |
|---|---|
| `phase1/` | Source usage processing and NP3 rule-based alerts |
| `phase2/` | Spark performance, geospatial, and processing exercises |
| `phase3/` | Windows handoff, warehouse loading, Airflow-style pipeline and quality status |
| `spark/` | Telecom activity Spark transformation and persisted analytics outputs |
| `phase4/` | FastAPI API1–API6 service, contracts, and regression tests |
| `phase5/` | React/Vite dashboard consuming FastAPI and static Milan GeoJSON |
| `phase6/` | ML1–ML6 problem definition, features, classifier, anomaly scores, API serving, and batch scoring |
| `data/warehouse/` | SQLite canonical analytics warehouse |
| `data/analytics/` | Persisted feature, anomaly, risk, and report artifacts |
| `data/reference/` | Static Milan grid GeoJSON reference |

## End-to-end flow

```text
landing/source activity
  -> canonical processing and country-code aggregation
  -> hourly grid analytics
  -> SQLite warehouse
  -> ML2 features
  -> ML3 model and ML4 anomaly scores
  -> ML6 batch risk scores
  -> FastAPI API1–API6
  -> React dashboard
  -> future Claude explanations and tool-driven investigations
```

The Phase 7 Claude layer must use curated API/ML evidence and pipeline status,
not raw warehouse rows.

## Verified integration points

- Spark transformation: `spark/telecom_pipeline.py`
- Pipeline and status artifacts: `phase3/de7_dag.py`,
  `data/pipeline_status_latest.json`
- API entry point: `phase4/app/main.py`
- API contracts and routes: `phase4/app/api1.py` through `api6.py`
- ML features: `phase6/ml/features.py`
- ML model and evaluation: `phase6/ml/ml3_train.py`
- Anomaly scoring: `phase6/ml/anomaly.py`
- Batch scoring: `phase6/ml/batch_score.py`
- React shell and pages: `phase5/src/`
