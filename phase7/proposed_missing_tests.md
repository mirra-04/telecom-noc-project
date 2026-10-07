# C4 Proposed Follow-up Tests

- Verify duplicate `(grid_id, timestamp)` rows are rejected before API/ML use.
- Verify all ML feature rows have `feature_timestamp` and do not read future
  intervals.
- Verify every GeoJSON join uses `properties.cellId`, not the top-level
  feature identifier.
- Verify API5 fails clearly when the model artifact is missing.
- Verify Claude-facing evidence builders reject missing required fields rather
  than inventing values.
- Verify tool-driven answers cite their source endpoint and report failed
  endpoints explicitly.
- Verify explanations never assert congestion or convert activity measures
  into counts or megabytes.
