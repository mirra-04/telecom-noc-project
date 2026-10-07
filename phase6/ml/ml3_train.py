from __future__ import annotations

import json
from pathlib import Path

import joblib
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, precision_score, recall_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

from .features import FEATURE_COLUMNS

MODEL_VERSION = "logistic-risk-v1"


def _read_activity(path: Path) -> pd.DataFrame:
    if path.is_dir():
        files = sorted(path.rglob("*.parquet")) + sorted(path.rglob("*.csv"))
        if not files:
            raise FileNotFoundError(f"No parquet or CSV activity files found in {path}")
        frames = [_read_activity(file) for file in files]
        return pd.concat(frames, ignore_index=True)
    if path.suffix.lower() == ".parquet":
        return pd.read_parquet(path)
    if path.suffix.lower() == ".csv":
        return pd.read_csv(path)
    raise ValueError(f"Unsupported activity source: {path}")


def load_training_frame(features_path: Path, activity_path: Path) -> pd.DataFrame:
    features = pd.read_parquet(features_path)
    activity = _read_activity(activity_path)
    activity["timestamp"] = pd.to_datetime(activity["timestamp"])
    activity = activity.sort_values(["grid_id", "timestamp"])
    activity["next_timestamp"] = activity.groupby("grid_id")["timestamp"].shift(-1)
    activity["next_activity"] = activity.groupby("grid_id")["total_activity"].shift(-1)
    frame = features.copy()
    frame["feature_timestamp"] = pd.to_datetime(frame["feature_timestamp"])
    frame = frame.merge(
        activity[["grid_id", "timestamp", "next_timestamp", "next_activity"]],
        left_on=["grid_id", "feature_timestamp"],
        right_on=["grid_id", "timestamp"],
        how="inner",
    )
    frame = frame[frame["next_timestamp"] == frame["feature_timestamp"] + pd.Timedelta(hours=1)]
    return frame.sort_values("feature_timestamp").reset_index(drop=True)


def train_model(frame: pd.DataFrame, artifact_path: Path, report_path: Path) -> dict:
    if frame.empty:
        raise ValueError("No leakage-safe feature/next-hour pairs are available")
    split_timestamp = frame["feature_timestamp"].quantile(0.8)
    train = frame[frame["feature_timestamp"] <= split_timestamp].copy()
    test = frame[frame["feature_timestamp"] > split_timestamp].copy()
    threshold = float(train["next_activity"].quantile(0.9))
    train["label"] = (train["next_activity"] >= threshold).astype(int)
    test["label"] = (test["next_activity"] >= threshold).astype(int)
    if train["label"].nunique() < 2:
        raise ValueError("Training label contains one class; adjust the proxy threshold")

    model = Pipeline(
        [
            ("scale", StandardScaler()),
            ("classifier", LogisticRegression(max_iter=200, class_weight="balanced")),
        ]
    )
    model.fit(train[FEATURE_COLUMNS], train["label"])
    predictions = model.predict(test[FEATURE_COLUMNS])
    report = {
        "model_version": MODEL_VERSION,
        "label_definition": "next-hour total_activity >= training-history 90th percentile",
        "threshold": threshold,
        "train_start": train["feature_timestamp"].min().strftime("%Y-%m-%d %H:%M:%S"),
        "train_end": train["feature_timestamp"].max().strftime("%Y-%m-%d %H:%M:%S"),
        "test_start": test["feature_timestamp"].min().strftime("%Y-%m-%d %H:%M:%S"),
        "test_end": test["feature_timestamp"].max().strftime("%Y-%m-%d %H:%M:%S"),
        "train_rows": len(train),
        "test_rows": len(test),
        "base_rate": float(test["label"].mean()),
        "accuracy": float(accuracy_score(test["label"], predictions)),
        "precision": float(precision_score(test["label"], predictions, zero_division=0)),
        "recall": float(recall_score(test["label"], predictions, zero_division=0)),
        "observations": [
            "The chronological split prevents later intervals from influencing training.",
            "Precision and recall are reported with the positive-class base rate.",
            "A positive result requests investigation and is not evidence of congestion.",
        ],
    }
    artifact_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump({"model": model, "threshold": threshold, "model_version": MODEL_VERSION}, artifact_path)
    report_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
