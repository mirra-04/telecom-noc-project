from __future__ import annotations

from fastapi.testclient import TestClient

from phase4.app.main import app


client = TestClient(app)


def test_hotspots_respect_limit_and_are_deterministic() -> None:
    response = client.get(
        "/network/hotspots",
        params={"limit": 3, "as_of": "2013-11-07 23:00:00"},
    )

    assert response.status_code == 200
    payload = response.json()
    assert len(payload["items"]) == 3
    assert [item["grid_id"] for item in payload["items"]] == [4857, 4856, 5458]
    assert all(item["status"] == "HOTSPOT" for item in payload["items"])
    assert all(item["model_version"] == "logistic-risk-v1" for item in payload["items"])
    assert all(item["risk_level"] in {"LOW", "ATTENTION", "HIGH"} for item in payload["items"])
    assert all("congestion" not in str(item).lower() for item in payload["items"])


def test_alerts_respect_limit_and_include_future_safe_fields() -> None:
    response = client.get(
        "/network/alerts",
        params={"limit": 2, "as_of": "2013-11-01 12:00:00"},
    )

    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 2
    assert all("risk_score" in item for item in items)
    assert all(item["model_version"] in {None, "logistic-risk-v1"} for item in items)
    assert all(item["severity"] in {"HIGH", "ATTENTION"} for item in items)


def test_alert_severity_filter_is_applied() -> None:
    response = client.get(
        "/network/alerts",
        params={"limit": 10, "severity": "HIGH", "as_of": "2013-11-01 12:00:00"},
    )

    assert response.status_code == 200
    assert response.json()["items"]
    assert all(item["severity"] == "HIGH" for item in response.json()["items"])


def test_hotspot_attention_filter_returns_no_hotspots() -> None:
    response = client.get(
        "/network/hotspots",
        params={"severity": "ATTENTION", "as_of": "2013-11-07 23:00:00"},
    )

    assert response.status_code == 200
    assert response.json()["items"] == []
