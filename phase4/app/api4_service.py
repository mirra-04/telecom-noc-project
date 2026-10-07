from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from .api1_service import AnalyticsDataError
from .config import FEATURES_PATH


FEATURE_COLUMNS = (
    "grid_id",
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
    "feature_timestamp",
)


def get_grid_features(grid_id: int) -> dict[str, int | float | str]:
    if not 1 <= grid_id <= 10000:
        raise AnalyticsDataError(f"Grid {grid_id} was not found")
    if not FEATURES_PATH.is_file():
        raise FileNotFoundError(
            f"Stored feature table not found: {FEATURES_PATH}. "
            "Run the ML feature-generation step before serving API4."
        )

    if FEATURES_PATH.suffix.lower() == ".parquet":
        features = pd.read_parquet(FEATURES_PATH)
    elif FEATURES_PATH.suffix.lower() == ".csv":
        features = pd.read_csv(FEATURES_PATH)
    else:
        raise AnalyticsDataError(
            f"Unsupported stored feature table format: {FEATURES_PATH.suffix}"
        )

    missing = [column for column in FEATURE_COLUMNS if column not in features.columns]
    if "data_quality" not in features.columns:
        missing.append("data_quality")
    if missing:
        raise AnalyticsDataError(
            f"Stored feature table is missing required fields: {', '.join(missing)}"
        )

    matches = features.loc[features["grid_id"] == grid_id]
    if matches.empty:
        raise AnalyticsDataError(f"No stored features exist for grid {grid_id}")
    row = matches.iloc[0]

    feature_timestamp = str(row["feature_timestamp"])
    try:
        feature_time = datetime.fromisoformat(feature_timestamp)
    except ValueError as exc:
        raise AnalyticsDataError(
            "Stored feature table contains an invalid feature_timestamp"
        ) from exc
    if feature_time.tzinfo is not None:
        feature_time = feature_time.astimezone(timezone.utc).replace(tzinfo=None)
    freshness_hours = round(
        max(0.0, (datetime.now(timezone.utc).replace(tzinfo=None) - feature_time).total_seconds() / 3600),
        2,
    )

    return {
        "grid_id": int(row["grid_id"]),
        "avg_activity": float(row["avg_activity"]),
        "activity_growth": float(row["activity_growth"]),
        "active_hours": int(row["active_hours"]),
        "peak_ratio": float(row["peak_ratio"]),
        "variability": float(row["variability"]),
        "internet_share": float(row["internet_share"]),
        "feature_timestamp": feature_timestamp,
        "data_quality": str(row["data_quality"]),
        "freshness_hours": freshness_hours,
    }
