from __future__ import annotations

from fastapi.testclient import TestClient

from phase4.app.main import app


client = TestClient(app)


def test_api4_returns_persisted_features() -> None:
    response = client.get("/network/grid/4821/features")

    assert response.status_code == 200
    payload = response.json()
    assert payload["grid_id"] == 4821
    assert payload["data_quality"] == "OK"
    assert payload["feature_timestamp"]


def test_api5_returns_trained_model_prediction() -> None:
    response = client.post(
        "/network/predict-risk",
        json={"grid_id": 4821, "as_of": "2013-11-07 23:00:00"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["model_version"] == "logistic-risk-v1"
    assert 0.0 <= payload["risk_score"] <= 1.0
    assert payload["feature_timestamp"]
    assert "congestion diagnosis" in payload["explanation_note"]


def test_api5_validates_grid_id() -> None:
    response = client.post("/network/predict-risk", json={"grid_id": 10001})
    assert response.status_code == 422


def test_api6_pipeline_status_reads_de7_record() -> None:
    response = client.get("/pipeline/status")

    assert response.status_code == 200
    payload = response.json()
    assert payload["healthy"] is True
    assert payload["as_of"] == "2013-11-07 23:00:00"
    assert payload["rows_published"] == 1679994
    assert payload["reasons"] == []


def test_api6_location_returns_centroid_without_geometry() -> None:
    response = client.get("/network/grid/4821/location")

    assert response.status_code == 200
    payload = response.json()
    assert payload["grid_id"] == 4821
    assert 45.35 < payload["centroid_lat"] < 45.57
    assert 9.01 < payload["centroid_lon"] < 9.32
    assert "geometry" not in payload
    assert payload["geometry_ref"].endswith("cellId=4821")


def test_api6_location_rejects_unknown_grid() -> None:
    assert client.get("/network/grid/10001/location").status_code == 404
