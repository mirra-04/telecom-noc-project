from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta

from .api1_service import AnalyticsDataError
from .database import get_connection


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def _parse_timestamp(value: str, field_name: str) -> datetime:
    try:
        return datetime.strptime(value, TIMESTAMP_FORMAT)
    except ValueError as exc:
        raise AnalyticsDataError(
            f"{field_name} must use format YYYY-MM-DD HH:MM:SS"
        ) from exc


def _latest_as_of(connection: sqlite3.Connection) -> str:
    row = connection.execute(
        "SELECT MAX(timestamp) AS as_of FROM fact_network_activity"
    ).fetchone()
    if row is None or row["as_of"] is None:
        raise AnalyticsDataError("No analytics data exists in the warehouse")
    return str(row["as_of"])


def get_grid_activity(
    grid_id: int,
    date: str | None = None,
    hour: int | None = None,
    as_of: str | None = None,
) -> dict[str, int | str | list[dict[str, float | str]]]:
    with get_connection() as connection:
        if not 1 <= grid_id <= 10000:
            raise AnalyticsDataError(f"Grid {grid_id} was not found")

        grid_exists = connection.execute(
            "SELECT 1 FROM dim_grid WHERE grid_id = ?",
            (grid_id,),
        ).fetchone()
        if grid_exists is None:
            raise AnalyticsDataError(f"Grid {grid_id} was not found")

        effective_as_of = as_of or _latest_as_of(connection)
        as_of_datetime = _parse_timestamp(effective_as_of, "as_of")

        conditions = ["grid_id = ?", "timestamp <= ?"]
        parameters: list[int | str] = [grid_id, effective_as_of]

        if date is None and hour is None:
            conditions.append("timestamp >= ?")
            parameters.append(
                (as_of_datetime - timedelta(hours=23)).strftime(TIMESTAMP_FORMAT)
            )
        else:
            if date is not None:
                try:
                    datetime.strptime(date, "%Y-%m-%d")
                except ValueError as exc:
                    raise AnalyticsDataError(
                        "date must use format YYYY-MM-DD"
                    ) from exc
                conditions.append("DATE(timestamp) = DATE(?)")
                parameters.append(date)
            if hour is not None:
                conditions.append("CAST(SUBSTR(timestamp, 12, 2) AS INTEGER) = ?")
                parameters.append(hour)

        rows = connection.execute(
            f"""
            SELECT
                timestamp,
                COALESCE(sms_in, 0.0) AS sms_in,
                COALESCE(sms_out, 0.0) AS sms_out,
                COALESCE(total_sms, 0.0) AS total_sms,
                COALESCE(call_in, 0.0) AS call_in,
                COALESCE(call_out, 0.0) AS call_out,
                COALESCE(total_calls, 0.0) AS total_calls,
                COALESCE(internet_activity, 0.0) AS internet_activity,
                COALESCE(total_activity, 0.0) AS total_activity
            FROM fact_network_activity
            WHERE {" AND ".join(conditions)}
            ORDER BY timestamp ASC
            """,
            parameters,
        ).fetchall()

        if not rows:
            raise AnalyticsDataError(
                f"No analytics data exists for grid {grid_id} and the requested filters"
            )

        return {
            "grid_id": grid_id,
            "as_of": effective_as_of,
            "points": [dict(row) for row in rows],
        }
