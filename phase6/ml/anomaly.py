from __future__ import annotations

from pathlib import Path

import pandas as pd


def load_activity(path: Path) -> pd.DataFrame:
    files = sorted(path.rglob("*.parquet")) if path.is_dir() else [path]
    if not files:
        raise FileNotFoundError(f"No activity files found in {path}")
    return pd.concat([pd.read_parquet(file) for file in files], ignore_index=True)


def score_anomalies(activity: pd.DataFrame, min_history: int = 2) -> pd.DataFrame:
    required = {"grid_id", "timestamp", "total_activity"}
    missing = required - set(activity.columns)
    if missing:
        raise ValueError(f"Missing activity columns: {', '.join(sorted(missing))}")
    if min_history < 1:
        raise ValueError("min_history must be positive")
    frame = activity.copy()
    frame["timestamp"] = pd.to_datetime(frame["timestamp"], errors="raise")
    frame = frame.sort_values(["grid_id", "timestamp"]).reset_index(drop=True)
    if frame.duplicated(["grid_id", "timestamp"]).any():
        raise ValueError("Activity must contain one row per grid_id and timestamp")
    frame["hour"] = frame["timestamp"].dt.hour
    buckets = frame.groupby(["grid_id", "hour"])["total_activity"]
    historical = buckets.transform(lambda series: series.shift(1).expanding().mean())
    history_count = buckets.transform(lambda series: series.shift(1).expanding().count())
    frame["baseline_count"] = history_count
    frame["baseline_activity"] = historical
    frame["current_activity"] = frame["total_activity"].astype(float)
    usable = (frame["baseline_count"] >= min_history) & frame["baseline_activity"].ne(0)
    frame = frame.loc[usable].copy()
    frame["anomaly_score"] = (
        (frame["current_activity"] - frame["baseline_activity"])
        .abs()
        .div(frame["baseline_activity"].abs())
    )
    frame["direction"] = frame.apply(
        lambda row: "high"
        if row["current_activity"] > row["baseline_activity"]
        else "low",
        axis=1,
    )
    frame["reason"] = frame.apply(
        lambda row: (
            f"Activity is {row['anomaly_score']:.2f}x away from the "
            f"historical hour-of-day baseline"
        ),
        axis=1,
    )
    return frame[
        [
            "grid_id",
            "timestamp",
            "current_activity",
            "baseline_activity",
            "anomaly_score",
            "direction",
            "reason",
        ]
    ].sort_values(["anomaly_score", "grid_id"], ascending=[False, True])


def write_anomaly_scores(scores: pd.DataFrame, output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    scores.to_csv(output_path, index=False)
    return output_path
