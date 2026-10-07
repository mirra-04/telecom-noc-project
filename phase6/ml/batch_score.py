from __future__ import annotations

from pathlib import Path

import joblib
import pandas as pd

from .features import FEATURE_COLUMNS


def batch_score(
    features_path: Path,
    model_path: Path,
    output_path: Path,
    report_path: Path,
) -> pd.DataFrame:
    features = pd.read_parquet(features_path) if features_path.suffix == ".parquet" else pd.read_csv(features_path)
    artifact = joblib.load(model_path)
    if not isinstance(artifact, dict) or not {"model", "model_version"} <= artifact.keys():
        raise ValueError(f"Invalid model artifact: {model_path}")
    model = artifact["model"]
    scored = features[["grid_id", "feature_timestamp", *FEATURE_COLUMNS]].copy()
    scored["risk_score"] = model.predict_proba(scored[FEATURE_COLUMNS])[:, 1]
    scored["risk_level"] = pd.cut(
        scored["risk_score"],
        bins=[-float("inf"), 0.4, 0.7, float("inf")],
        labels=["LOW", "ATTENTION", "HIGH"],
    ).astype(str)
    scored["model_version"] = str(artifact["model_version"])
    scored["reason"] = (
        "Investigation signal from next-hour high-activity risk model; "
        "not a congestion diagnosis."
    )
    scored = scored[
        [
            "grid_id",
            "feature_timestamp",
            "risk_score",
            "risk_level",
            "model_version",
            "reason",
        ]
    ].drop_duplicates(["grid_id", "feature_timestamp"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    scored.to_csv(output_path, index=False)
    scored.sort_values(["risk_score", "grid_id"], ascending=[False, True]).head(20).to_csv(
        report_path, index=False
    )
    return scored
