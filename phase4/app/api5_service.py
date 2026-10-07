from __future__ import annotations

from pathlib import Path
from typing import Any

import joblib
import pandas as pd

from .api1_service import AnalyticsDataError
from .api5_models import RiskPredictionRequest
from .config import FEATURES_PATH, MODEL_PATH

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


def _read_features() -> pd.DataFrame:
    if not FEATURES_PATH.is_file():
        raise FileNotFoundError(
            f"Stored feature table not found: {FEATURES_PATH}. "
            "Run phase6\\build_features.py before serving API5."
        )
    if FEATURES_PATH.suffix.lower() == ".parquet":
        return pd.read_parquet(FEATURES_PATH)
    if FEATURES_PATH.suffix.lower() == ".csv":
        return pd.read_csv(FEATURES_PATH)
    raise AnalyticsDataError(f"Unsupported stored feature table format: {FEATURES_PATH.suffix}")


def _read_model() -> dict[str, Any]:
    if not MODEL_PATH.is_file():
        raise FileNotFoundError(
            f"Trained model artifact not found: {MODEL_PATH}. "
            "Run phase6\\train_ml3.py before serving API5."
        )
    artifact = joblib.load(MODEL_PATH)
    if not isinstance(artifact, dict) or not {"model", "model_version"} <= artifact.keys():
        raise AnalyticsDataError(f"Invalid trained model artifact: {MODEL_PATH}")
    return artifact


def _select_features(features: pd.DataFrame, request: RiskPredictionRequest) -> pd.Series:
    missing = [column for column in ["grid_id", "feature_timestamp", *FEATURE_COLUMNS] if column not in features]
    if missing:
        raise AnalyticsDataError(
            f"Stored feature table is missing required fields: {', '.join(missing)}"
        )
    matches = features.loc[features["grid_id"] == request.grid_id].copy()
    if request.as_of is not None:
        matches = matches[matches["feature_timestamp"] <= request.as_of]
    if matches.empty:
        raise AnalyticsDataError(f"No stored features exist for grid {request.grid_id}")
    matches["feature_timestamp"] = matches["feature_timestamp"].astype(str)
    return matches.sort_values("feature_timestamp").iloc[-1]


def predict_risk(request: RiskPredictionRequest) -> dict[str, float | str]:
    artifact = _read_model()
    row = _select_features(_read_features(), request)
    model = artifact["model"]
    probability = float(model.predict_proba(pd.DataFrame([[row[column] for column in FEATURE_COLUMNS]], columns=FEATURE_COLUMNS))[0][1])
    risk_level = "HIGH" if probability >= 0.7 else "ATTENTION" if probability >= 0.4 else "LOW"
    return {
        "risk_score": round(probability, 6),
        "risk_level": risk_level,
        "model_version": str(artifact["model_version"]),
        "feature_timestamp": str(row["feature_timestamp"]),
        "explanation_note": (
            "Model output is a high-activity investigation signal for the next hour; "
            "it is not a congestion diagnosis."
        ),
    }
