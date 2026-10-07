from __future__ import annotations

import csv
import sqlite3
from threading import Lock
from pathlib import Path
from functools import lru_cache

from .api1_service import AnalyticsDataError
from .config import ALERTS_PATH, RISK_SCORES_PATH
from .database import get_connection


VALID_SEVERITIES = {"HIGH", "ATTENTION"}
_RISK_CACHE_LOCK = Lock()


@lru_cache(maxsize=4)
def _risk_scores(path_string: str, modified_ns: int) -> dict[tuple[int, str], dict[str, float | str]]:
    path = Path(path_string)
    if not path.is_file():
        return {}
    with path.open(newline="", encoding="utf-8") as file:
        return {
            (int(row["grid_id"]), row["feature_timestamp"]): {
                "risk_score": float(row["risk_score"]),
                "risk_level": row["risk_level"],
                "model_version": row["model_version"],
            }
            for row in csv.DictReader(file)
        }


def _risk_for(grid_id: int, timestamp: str) -> dict[str, float | str | None]:
    modified_ns = RISK_SCORES_PATH.stat().st_mtime_ns if RISK_SCORES_PATH.is_file() else 0
    # lru_cache does not hold its lock while computing a missing value. The
    # dashboard requests hotspots and alerts concurrently, so serialize the
    # first large risk-table load to avoid parsing the 190 MB CSV twice.
    with _RISK_CACHE_LOCK:
        values = _risk_scores(str(RISK_SCORES_PATH), modified_ns).get((grid_id, timestamp))
    return values or {"risk_score": None, "risk_level": None, "model_version": None}


def _latest_as_of(connection: sqlite3.Connection) -> str:
    row = connection.execute(
        "SELECT MAX(timestamp) AS as_of FROM fact_network_activity"
    ).fetchone()
    if row is None or row["as_of"] is None:
        raise AnalyticsDataError("No analytics data exists in the warehouse")
    return str(row["as_of"])


def _validate_severity(severity: str | None) -> str | None:
    if severity is None:
        return None
    normalized = severity.upper()
    if normalized not in VALID_SEVERITIES:
        raise AnalyticsDataError(
            f"severity must be one of: {', '.join(sorted(VALID_SEVERITIES))}"
        )
    return normalized


def get_hotspots(
    limit: int,
    severity: str | None = None,
    as_of: str | None = None,
) -> dict[str, str | list[dict[str, int | float | str | None]]]:
    requested_severity = _validate_severity(severity)
    with get_connection() as connection:
        effective_as_of = as_of or _latest_as_of(connection)
        rows = connection.execute(
            """
            SELECT grid_id, timestamp, total_activity
            FROM fact_network_activity
            WHERE timestamp = ?
            ORDER BY total_activity DESC, grid_id ASC
            LIMIT ?
            """,
            (effective_as_of, limit),
        ).fetchall()

    if not rows:
        raise AnalyticsDataError(f"No analytics data exists for as_of={effective_as_of}")

    # Hotspots are the prioritized high-activity view, so every returned
    # item has HIGH severity. The field remains additive-safe for ML scores.
    if requested_severity == "ATTENTION":
        rows = []

    return {
        "as_of": effective_as_of,
        "items": [
            {
                "grid_id": int(row["grid_id"]),
                "timestamp": str(row["timestamp"]),
                "total_activity": float(row["total_activity"]),
                "status": "HOTSPOT",
                "severity": "HIGH",
                "reason": "Highest total activity in the selected reporting interval",
                **_risk_for(int(row["grid_id"]), str(row["timestamp"])),
            }
            for row in rows
        ],
    }


def _alert_severity(alert_type: str) -> str:
    return "HIGH" if alert_type in {"HIGH_ACTIVITY", "ACTIVITY_SPIKE"} else "ATTENTION"


def get_alerts(
    limit: int,
    severity: str | None = None,
    as_of: str | None = None,
) -> dict[str, str | list[dict[str, int | float | str | None]]]:
    requested_severity = _validate_severity(severity)
    effective_as_of = as_of

    if effective_as_of is None:
        with get_connection() as connection:
            effective_as_of = _latest_as_of(connection)

    if not ALERTS_PATH.is_file():
        raise FileNotFoundError(f"Alert output not found: {ALERTS_PATH}")

    items: list[dict[str, int | float | str | None]] = []
    with ALERTS_PATH.open(newline="", encoding="utf-8") as file:
        for row in csv.DictReader(file):
            row_severity = _alert_severity(row["alert_type"])
            if row["timestamp"] > effective_as_of:
                continue
            if requested_severity is not None and row_severity != requested_severity:
                continue
            items.append(
                {
                    "grid_id": int(row["grid_id"]),
                    "timestamp": row["timestamp"],
                    "alert_type": row["alert_type"],
                    "current_activity": float(row["current_activity"]),
                    "baseline_activity": float(row["baseline_activity"]),
                    "severity": row_severity,
                    "reason": row["reason"],
                    **_risk_for(int(row["grid_id"]), row["timestamp"]),
                }
            )

    items.sort(key=lambda item: (str(item["timestamp"]), int(item["grid_id"]), str(item["alert_type"])))
    return {"as_of": effective_as_of, "items": items[:limit]}
