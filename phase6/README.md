# Phase 6 — Machine Learning

Phase 6 starts with the trainer guide's ML1 and ML2 foundations. The selected
problem is next-hour high-activity risk using a documented percentile proxy;
the result is an investigation signal, not a congestion diagnosis.

## Build the feature table

From the repository root:

```powershell
& ".\.venv\Scripts\python.exe" phase6\build_features.py
```

The default output is `data/analytics/grid_features.parquet`. Set
`NETWORK_FEATURES_FILE` to use CSV instead. Features use a 24-hour trailing
window and a preceding 24-hour baseline, and every row records `feature_timestamp`.

## Validation

```powershell
& ".\.venv\Scripts\python.exe" -m pytest phase6\tests -q
```

## ML3 classifier

After generating ML2 features, train the chronological Logistic Regression
baseline:

```powershell
& ".\.venv\Scripts\python.exe" phase6\train_ml3.py
```

This writes the model artifact to `data/models/network_risk_model.joblib` and
the evaluation report to `data/analytics/ml3_evaluation.json`.

## ML4 anomaly baseline

Score each grid against a chronological historical hour-of-day baseline. Each
row uses only earlier observations for the same grid and hour; the current row
and future observations are excluded, with at least two prior observations
required by default:

```powershell
& ".\.venv\Scripts\python.exe" phase6\score_anomalies.py
```

This writes `data/analytics/network_anomaly_scores.csv` with current value,
baseline value, percentage deviation, high/low direction, and an operational
reason. A positive anomaly is an investigation signal, not a diagnosis.

## ML6 batch scoring

Score every persisted feature row and produce the top-20 operational report:

```powershell
& ".\.venv\Scripts\python.exe" phase6\batch_score.py
```

Outputs:

- `data/analytics/network_risk_scores.csv`
- `data/analytics/top20_operational_attention.csv`

Scores are deduplicated by `grid_id` and `feature_timestamp`, and every row
contains the model version and investigation-oriented reason.

The DE7 pipeline runs ML2 feature generation and ML6 scoring after the
warehouse load and before `quality_check`. API3 reads the persisted
`network_risk_scores.csv` artifact and adds risk metadata to hotspot and alert
responses without changing their existing fields.
