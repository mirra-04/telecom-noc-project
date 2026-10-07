# Network Operations & Predictive Intelligence

## Architecture and data flow

The project is layered:

1. Raw and processed source data are handled by Phase 1–3 pipelines.
2. Spark and the warehouse publish canonical hourly grid analytics.
3. FastAPI in `phase4/app/` serves curated network, feature, risk, location,
   and pipeline-status data.
4. ML artifacts and batch scores are produced by `phase6/`.
5. The React dashboard in `phase5/` consumes the API and the static GeoJSON
   reference file.
6. Phase 7 Claude integrations may reason over API and ML outputs, but must not
   bypass those layers to read raw warehouse rows.

## Non-negotiable project rules

- The canonical analytics grain is exactly one `grid_id` per hourly
  `timestamp`, after country-code rows have been aggregated.
- Raw country-code detail must not leak into the operational analytics path,
  API contracts, ML features, or dashboard.
- Activity fields are proportional activity measures. They are not message
  counts, call counts, subscribers, megabytes, throughput, or capacity.
- High activity, risk, or anomaly is an investigation signal. Never describe it
  as confirmed congestion or a confirmed network fault.
- Geographic joins must use `feature.properties.cellId`. Never use the
  top-level GeoJSON feature `id`, which is zero-based.
- `AS_OF` is the maximum timestamp in the analytics layer and defines the
  reproducible reporting "now". Optional `as_of` parameters must preserve this
  convention.
- Claude receives curated API responses, feature records, scores, and pipeline
  status only. Do not send raw warehouse extracts to Claude.
- Evidence and interpretation must be separated. Never invent missing values;
  report insufficient evidence instead.

## Validation expectations

- Preserve backward-compatible API contracts unless an additive change is
  explicitly reviewed.
- Test grain uniqueness, feature leakage boundaries, API responses, and
  terminology-sensitive explanations.
- Run focused tests after changes, then the complete relevant regression suite.
- Human approval remains required for merges, deployment, destructive changes,
  secret handling, and production configuration.
