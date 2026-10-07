from __future__ import annotations

from pathlib import Path

from ml.anomaly import load_activity, score_anomalies, write_anomaly_scores

ROOT = Path(__file__).resolve().parents[1]
source = ROOT / "data" / "analytics" / "hourly_grid_summary"
output = ROOT / "data" / "analytics" / "network_anomaly_scores.csv"

if __name__ == "__main__":
    scores = score_anomalies(load_activity(source))
    write_anomaly_scores(scores, output)
    print(f"Wrote {len(scores):,} anomaly scores to {output}")
    print("Top operational attention cases:")
    print(scores.head(5).to_string(index=False))
