from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd

FEATURE_COLUMNS = [
    "avg_activity",
    "activity_growth",
    "active_hours",
    "peak_ratio",
    "variability",
    "internet_share",
]


def build_features(
    activity: pd.DataFrame,
    lookback_hours: int = 24,
    baseline_hours: int = 24,
) -> pd.DataFrame:
    """Build features at t using only rows at or before t for each grid."""
    required = {"grid_id", "timestamp", "internet_activity", "total_activity"}
    missing = required - set(activity.columns)
    if missing:
        raise ValueError(f"Missing activity columns: {', '.join(sorted(missing))}")
    if lookback_hours < 1 or baseline_hours < 1:
        raise ValueError("lookback_hours and baseline_hours must be positive")

    frame = activity.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="raise")
    frame = frame.sort_values(["grid_id", "timestamp"]).reset_index(drop=True)
    grouped = frame.groupby("grid_id", sort=False)
    rolling_activity = grouped["total_activity"]
    rolling_internet = grouped["internet_activity"]
    average = rolling_activity.transform(lambda series: series.rolling(lookback_hours).mean())
    baseline_average = rolling_activity.transform(
        lambda series: series.shift(lookback_hours).rolling(baseline_hours).mean()
    )
    total = rolling_activity.transform(lambda series: series.rolling(lookback_hours).sum())
    peak = rolling_activity.transform(lambda series: series.rolling(lookback_hours).max())
    deviation = rolling_activity.transform(
        lambda series: series.rolling(lookback_hours).std(ddof=0)
    )
    active_hours = grouped["total_activity"].transform(
        lambda series: series.gt(0).rolling(lookback_hours).sum()
    )
    internet_total = rolling_internet.transform(
        lambda series: series.rolling(lookback_hours).sum()
    )
    valid = average.notna() & baseline_average.notna()
    result = frame.loc[valid, ["grid_id", "timestamp"]].copy()
    result = result.rename(columns={"timestamp": "feature_timestamp"})
    result["feature_timestamp"] = result["feature_timestamp"].dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    result["avg_activity"] = average.loc[valid].to_numpy()
    result["activity_growth"] = (
        (average.loc[valid] - baseline_average.loc[valid])
        .div(baseline_average.loc[valid].replace(0, float("nan")))
        .fillna(0.0)
        .to_numpy()
    )
    result["active_hours"] = active_hours.loc[valid].astype(int).to_numpy()
    safe_average = average.loc[valid].replace(0, float("nan"))
    safe_total = total.loc[valid].replace(0, float("nan"))
    result["peak_ratio"] = peak.loc[valid].div(safe_average).fillna(0.0).to_numpy()
    result["variability"] = deviation.loc[valid].div(safe_average).fillna(0.0).to_numpy()
    result["internet_share"] = internet_total.loc[valid].div(safe_total).fillna(0.0).to_numpy()
    result["data_quality"] = "OK"
    result = result.reset_index(drop=True)
    if result.empty:
        return pd.DataFrame(columns=["grid_id", "feature_timestamp", *FEATURE_COLUMNS, "data_quality"])
    return result


def load_activity(database_path: Path) -> pd.DataFrame:
    if not database_path.is_file():
        raise FileNotFoundError(f"Warehouse database not found: {database_path}")
    with sqlite3.connect(database_path) as connection:
        return pd.read_sql_query(
            "SELECT grid_id, timestamp, internet_activity, total_activity "
            "FROM fact_network_activity ORDER BY grid_id, timestamp",
            connection,
        )


def write_features(features: pd.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() == ".parquet":
        features.to_parquet(output_path, index=False)
    else:
        features.to_csv(output_path, index=False)
    return output_path


def build_features_from_database(
    database_path: Path,
    output_path: Path,
    lookback_hours: int = 24,
    baseline_hours: int = 24,
) -> pd.DataFrame:
    features = build_features(load_activity(database_path), lookback_hours, baseline_hours)
    write_features(features, output_path)
    return features
