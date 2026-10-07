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


def test_summary_defaults_to_latest_as_of() -> None:
    response = client.get("/network/summary")

    assert response.status_code == 200
    payload = response.json()
    assert payload["as_of"] == "2013-11-07 23:00:00"
    assert payload["active_grids"] == 10000
    assert payload["top_grid"] == 4857


def test_summary_matches_warehouse_for_explicit_as_of() -> None:
    as_of = "2013-11-01 00:00:00"
    with sqlite3.connect(DATABASE_PATH) as connection:
        expected = connection.execute(
            """
            SELECT SUM(total_activity), COUNT(DISTINCT grid_id)
            FROM fact_network_activity
            WHERE timestamp = ?
            """,
            (as_of,),
        ).fetchone()

    response = client.get("/network/summary", params={"as_of": as_of})

    assert response.status_code == 200
    payload = response.json()
    assert payload["as_of"] == as_of
    assert payload["total_activity"] == expected[0]
    assert payload["active_grids"] == expected[1]


def test_summary_rejects_unknown_timestamp() -> None:
    response = client.get(
        "/network/summary",
        params={"as_of": "2013-11-08 00:00:00"},
    )

    assert response.status_code == 500
    assert "No analytics data exists" in response.json()["detail"]
