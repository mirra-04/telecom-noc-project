from __future__ import annotations

import os
from pathlib import Path

from ml.batch_score import batch_score

ROOT = Path(__file__).resolve().parents[1]

if __name__ == "__main__":
    features_path = Path(
        os.getenv("NETWORK_FEATURES_FILE", ROOT / "data" / "analytics" / "grid_features.parquet")
    )
    model_path = Path(
        os.getenv("NETWORK_MODEL_FILE", ROOT / "data" / "models" / "network_risk_model.joblib")
    )
    scores_path = Path(
        os.getenv("NETWORK_RISK_SCORES_FILE", ROOT / "data" / "analytics" / "network_risk_scores.csv")
    )
    report_path = Path(
        os.getenv(
            "NETWORK_ATTENTION_REPORT_FILE",
            ROOT / "data" / "analytics" / "top20_operational_attention.csv",
        )
    )
    scores = batch_score(
        features_path,
        model_path,
        scores_path,
        report_path,
    )
    print(f"Wrote {len(scores):,} deduplicated risk scores")
    print("Top-20 report: data\\analytics\\top20_operational_attention.csv")
