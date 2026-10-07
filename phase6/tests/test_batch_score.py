from __future__ import annotations

import joblib
import pandas as pd
from sklearn.dummy import DummyClassifier

from phase6.ml.batch_score import batch_score


def test_batch_scores_are_deduplicated_and_include_metadata(tmp_path) -> None:
    features = pd.DataFrame(
        {
            "grid_id": [1, 1, 2],
            "feature_timestamp": ["2024-01-01 00:00:00"] * 3,
            "avg_activity": [1.0, 1.0, 2.0],
            "activity_growth": [0.0] * 3,
            "active_hours": [24] * 3,
            "peak_ratio": [1.0] * 3,
            "variability": [0.0] * 3,
            "internet_share": [0.5] * 3,
        }
    )
    feature_path = tmp_path / "features.csv"
    model_path = tmp_path / "model.joblib"
    output_path = tmp_path / "scores.csv"
    report_path = tmp_path / "top20.csv"
    features.to_csv(feature_path, index=False)
    model = DummyClassifier(strategy="prior").fit(
        features[["avg_activity", "activity_growth", "active_hours", "peak_ratio", "variability", "internet_share"]],
        [0, 1, 1],
    )
    joblib.dump({"model": model, "model_version": "test-v1"}, model_path)
    result = batch_score(feature_path, model_path, output_path, report_path)
    assert len(result) == 2
    assert result["model_version"].eq("test-v1").all()
    assert output_path.is_file()
    assert report_path.is_file()
