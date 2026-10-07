from __future__ import annotations

import os
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATABASE_PATH = Path(
    os.getenv(
        "NETWORK_INTELLIGENCE_DB",
        PROJECT_ROOT / "data" / "warehouse" / "network_intelligence.db",
    )
)
ALERTS_PATH = Path(
    os.getenv("NETWORK_ALERTS_CSV", PROJECT_ROOT / "phase1" / "network_alerts.csv")
)
FEATURES_PATH = Path(
    os.getenv(
        "NETWORK_FEATURES_FILE",
        PROJECT_ROOT / "data" / "analytics" / "grid_features.parquet",
    )
)
MODEL_PATH = Path(
    os.getenv(
        "NETWORK_MODEL_FILE",
        PROJECT_ROOT / "data" / "models" / "network_risk_model.joblib",
    )
)
RISK_SCORES_PATH = Path(
    os.getenv(
        "NETWORK_RISK_SCORES_FILE",
        PROJECT_ROOT / "data" / "analytics" / "network_risk_scores.csv",
    )
)
PIPELINE_STATUS_PATH = Path(
    os.getenv(
        "PIPELINE_STATUS_FILE",
        PROJECT_ROOT / "data" / "pipeline_status_latest.json",
    )
)