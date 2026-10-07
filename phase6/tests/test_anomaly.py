from __future__ import annotations

import pandas as pd
import pytest

from phase6.ml.anomaly import score_anomalies


def test_hour_of_day_anomaly_score_reports_both_directions() -> None:
    timestamps = pd.date_range("2024-01-01", periods=72, freq="h")
    activity = pd.DataFrame(
        {
            "grid_id": 1,
            "timestamp": timestamps,
            "total_activity": [10.0] * 24 + [20.0] * 24 + [10.0] * 24,
        }
    )
    result = score_anomalies(activity, min_history=1)
    assert {"high", "low"} == set(result["direction"])
    assert (result["anomaly_score"] >= 0).all()
    assert {"grid_id", "timestamp", "baseline_activity", "reason"} <= set(result.columns)


def test_baseline_uses_only_prior_same_hour_observations() -> None:
    activity = pd.DataFrame(
        {
            "grid_id": [7, 7, 7],
            "timestamp": pd.to_datetime(
                ["2024-01-01 10:00", "2024-01-02 10:00", "2024-01-03 10:00"]
            ),
            "total_activity": [10.0, 10.0, 100.0],
        }
    )

    result = score_anomalies(activity)

    latest = result.loc[result["timestamp"].eq(pd.Timestamp("2024-01-03 10:00"))].iloc[0]
    assert latest["baseline_activity"] == 10.0
    assert latest["anomaly_score"] == 9.0
    assert latest["direction"] == "high"


def test_rows_without_enough_history_are_excluded() -> None:
    activity = pd.DataFrame(
        {
            "grid_id": [7, 7, 7],
            "timestamp": pd.to_datetime(
                ["2024-01-01 10:00", "2024-01-02 10:00", "2024-01-03 10:00"]
            ),
            "total_activity": [10.0, 20.0, 30.0],
        }
    )

    result = score_anomalies(activity, min_history=2)

    assert list(result["timestamp"]) == [pd.Timestamp("2024-01-03 10:00")]
    assert result.iloc[0]["baseline_activity"] == 15.0


def test_duplicate_grid_timestamps_are_rejected() -> None:
    activity = pd.DataFrame(
        {
            "grid_id": [7, 7],
            "timestamp": pd.to_datetime(["2024-01-01 10:00", "2024-01-01 10:00"]),
            "total_activity": [10.0, 20.0],
        }
    )

    with pytest.raises(ValueError, match="one row per grid_id and timestamp"):
        score_anomalies(activity)
