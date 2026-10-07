# C3 Context-Engineering Checklist

- Define the reporting `AS_OF` before collecting evidence.
- Collect current grid activity, not raw warehouse rows.
- Summarize older activity before placing it in the active prompt.
- Include prior alerts, persisted features, model/anomaly output, location, and
  pipeline health only when they are available through curated services.
- Separate CURRENT EVIDENCE, HISTORICAL EVIDENCE, and UNCERTAINTY.
- Record failed evidence sources; never replace them with estimates.
- Keep activity and risk terminology proportional and investigation-oriented.
- Never state confirmed congestion or a confirmed network fault from these
  signals alone.
- Compare a curated prompt with a raw/dump-style prompt only as a controlled
  context experiment; do not send raw warehouse extracts to Claude.
- Trace every factual statement in the report to a collected evidence field.
