from __future__ import annotations

from pathlib import Path

from ml.ml3_train import load_training_frame, train_model

ROOT = Path(__file__).resolve().parents[1]
features_path = ROOT / "data" / "analytics" / "grid_features.parquet"
activity_path = ROOT / "data" / "analytics" / "hourly_grid_summary"
artifact_path = ROOT / "data" / "models" / "network_risk_model.joblib"
report_path = ROOT / "data" / "analytics" / "ml3_evaluation.json"

if __name__ == "__main__":
    report = train_model(load_training_frame(features_path, activity_path), artifact_path, report_path)
    print(f"Trained {report['model_version']}")
    print(f"Chronological test accuracy: {report['accuracy']:.4f}")
    print(f"Precision: {report['precision']:.4f}; recall: {report['recall']:.4f}")
