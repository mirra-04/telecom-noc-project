from __future__ import annotations

import pandas as pd

from phase6.ml.features import FEATURE_COLUMNS, build_features


def _activity(hours: int = 72) -> pd.DataFrame:
    timestamps = pd.date_range("2024-01-01", periods=hours, freq="h")
    return pd.DataFrame(
        {
            "grid_id": 1,
            "timestamp": timestamps,
            "internet_activity": [10.0] * hours,
            "total_activity": [20.0] * hours,
        }
    )


def test_feature_schema_and_timestamp_boundary() -> None:
    result = build_features(_activity(), lookback_hours=24, baseline_hours=24)
    assert list(result.columns) == [
        "grid_id",
        "feature_timestamp",
        *FEATURE_COLUMNS,
        "data_quality",
    ]
    assert result["feature_timestamp"].min() == "2024-01-02 23:00:00"
    assert result["feature_timestamp"].max() == "2024-01-03 23:00:00"


def test_features_do_not_change_when_future_rows_are_appended() -> None:
    source = _activity()
    before = build_features(source.iloc[:60], 24, 24)
    after = build_features(source, 24, 24)
    timestamp = "2024-01-03 11:00:00"
    left = before[before["feature_timestamp"] == timestamp].iloc[0]
    right = after[after["feature_timestamp"] == timestamp].iloc[0]
    pd.testing.assert_series_equal(
        left[FEATURE_COLUMNS].reset_index(drop=True),
        right[FEATURE_COLUMNS].reset_index(drop=True),
        check_names=False,
    )


def test_zero_activity_has_finite_safe_values() -> None:
    source = _activity()
    source["total_activity"] = 0.0
    source["internet_activity"] = 0.0
    result = build_features(source, 24, 24)
    assert (result[FEATURE_COLUMNS].fillna(0).abs() < float("inf")).all().all()
    assert (result[["activity_growth", "peak_ratio", "variability", "internet_share"]] == 0).all().all()
