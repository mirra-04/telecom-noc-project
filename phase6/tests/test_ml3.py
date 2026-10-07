from __future__ import annotations

import pandas as pd

from phase6.ml.ml3_train import load_training_frame


def test_training_frame_keeps_only_adjacent_future_labels(tmp_path) -> None:
    features = pd.DataFrame(
        {
            "grid_id": [1, 1],
            "feature_timestamp": ["2024-01-01 00:00:00", "2024-01-01 01:00:00"],
            "avg_activity": [1.0, 2.0],
        }
    )
    activity = pd.DataFrame(
        {
            "grid_id": [1, 1, 1],
            "timestamp": pd.date_range("2024-01-01", periods=3, freq="h"),
            "total_activity": [1.0, 2.0, 3.0],
        }
    )
    feature_path = tmp_path / "features.parquet"
    activity_path = tmp_path / "activity.csv"
    features.to_parquet(feature_path)
    activity.to_csv(activity_path, index=False)
    result = load_training_frame(feature_path, activity_path)
    assert result["next_activity"].tolist() == [2.0, 3.0]
    assert all(result["next_timestamp"] > result["feature_timestamp"])
