# Phase 4 — FastAPI

## Run the API

From the repository root:

```powershell
& ".\.venv\Scripts\python.exe" -m uvicorn phase4.app.main:app --reload
```

Open Swagger at <http://127.0.0.1:8000/docs>.

The Windows warehouse path defaults to
`data/warehouse/network_intelligence.db`. Override it with the
`NETWORK_INTELLIGENCE_DB` environment variable when needed.

## API1

`GET /network/summary` returns the metrics for the requested hourly
`as_of` timestamp, or the latest warehouse timestamp when omitted:

- `total_activity`
- `active_grids`
- `peak_hour` for the selected calendar date
- `top_grid` for the selected hourly interval
- effective `as_of`

## API2

`GET /network/grid/{grid_id}` returns one grid's hourly activity series.
Without filters it returns the trailing 24 hourly intervals ending at the
latest warehouse timestamp. Optional `date`, `hour`, and `as_of` query
parameters narrow the warehouse query.

## API3

`GET /network/hotspots` serves deterministically ordered high-activity
grids from the warehouse. `GET /network/alerts` serves the existing NP3
rule-based alert output. Both support `limit`, `severity`, and `as_of`.
Risk fields are nullable and reserved for later ML integration.

## API4

`GET /network/grid/{grid_id}/features` serves the persisted ML2 feature
vector. It intentionally returns an explicit server error until the stored
feature table is produced; the API does not calculate feature values.

## API5

`POST /network/predict-risk` is the contract-first prediction endpoint. The
current implementation returns a clearly marked `stub-v1` response.

## API6

`GET /pipeline/status` reads the DE7 machine-readable status record.
`GET /network/grid/{grid_id}/location` reads `dim_grid` and returns only
centroid coordinates plus `geometry_ref`, not Polygon geometry.