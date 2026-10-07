from __future__ import annotations

import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from phase4.app.main import app


DATABASE_PATH = (
    Path(__file__).resolve().parents[2]
    / "data"
    / "warehouse"
    / "network_intelligence.db"
)
client = TestClient(app)


def test_grid_4821_defaults_to_trailing_24_points() -> None:
    response = client.get("/network/grid/4821")

    assert response.status_code == 200
    payload = response.json()
    assert payload["grid_id"] == 4821
    assert payload["as_of"] == "2013-11-07 23:00:00"
    assert len(payload["points"]) == 24
    assert len({point["timestamp"] for point in payload["points"]}) == 24
    assert payload["points"][0]["timestamp"] == "2013-11-07 00:00:00"
    assert payload["points"][-1]["timestamp"] == "2013-11-07 23:00:00"


def test_grid_activity_matches_warehouse() -> None:
    timestamp = "2013-11-07 14:00:00"
    with sqlite3.connect(DATABASE_PATH) as connection:
        expected = connection.execute(
            """
            SELECT sms_in, sms_out, total_sms, call_in, call_out,
                   total_calls, internet_activity, total_activity
            FROM fact_network_activity
            WHERE grid_id = ? AND timestamp = ?
            """,
            (4821, timestamp),
        ).fetchone()

    response = client.get(
        "/network/grid/4821",
        params={"as_of": "2013-11-07 23:00:00"},
    )

    assert response.status_code == 200
    point = next(
        item for item in response.json()["points"] if item["timestamp"] == timestamp
    )
    assert tuple(point[field] for field in (
        "sms_in",
        "sms_out",
        "total_sms",
        "call_in",
        "call_out",
        "total_calls",
        "internet_activity",
        "total_activity",
    )) == expected


def test_grid_zero_and_10001_return_404() -> None:
    assert client.get("/network/grid/0").status_code == 404
    assert client.get("/network/grid/10001").status_code == 404
