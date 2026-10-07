# Phase 5 — Lightweight NOC Dashboard

## Run

From this directory:

```powershell
npm install
npm run dev
```

The dashboard runs at `http://127.0.0.1:5173`. The API base defaults to
`http://127.0.0.1:8000` and can be changed with a Vite environment variable:

```powershell
$env:VITE_API_BASE_URL = "http://127.0.0.1:8000"
```

The dashboard consumes FastAPI only. The sole static-data exception is
`public/reference/milano-grid.geojson`, loaded once by RE4. RE4 uses an
OpenStreetMap basemap with the static grid overlay, joins with
`properties.cellId`, and highlights API-selected hotspots.

## Pages

- RE1/RE2: API-connected shell and network overview
- RE3: grid explorer for the trailing hourly series
- RE4: hotspots, alerts, and selected Milan polygons
- RE5: contract-first predictive-risk view with a disabled future AI action
