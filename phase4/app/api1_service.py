from __future__ import annotations

import sqlite3

from .database import get_connection


class AnalyticsDataError(RuntimeError):
    """Raised when the warehouse cannot satisfy an API request."""


def _latest_as_of(connection: sqlite3.Connection) -> str:
    row = connection.execute(
        "SELECT MAX(timestamp) AS as_of FROM fact_network_activity"
    ).fetchone()
    if row is None or row["as_of"] is None:
        raise AnalyticsDataError("No analytics data exists in the warehouse")
    return str(row["as_of"])


def get_network_summary(as_of: str | None = None) -> dict[str, int | float | str]:
    with get_connection() as connection:
        effective_as_of = as_of or _latest_as_of(connection)
        metrics = connection.execute(
            """
            SELECT
                SUM(total_activity) AS total_activity,
                COUNT(DISTINCT grid_id) AS active_grids
            FROM fact_network_activity
            WHERE timestamp = ?
            """,
            (effective_as_of,),
        ).fetchone()

        if metrics is None or metrics["total_activity"] is None:
            raise AnalyticsDataError(
                f"No analytics data exists for as_of={effective_as_of}"
            )

        peak = connection.execute(
            """
            SELECT CAST(SUBSTR(timestamp, 12, 2) AS INTEGER) AS peak_hour
            FROM fact_network_activity
            WHERE DATE(timestamp) = DATE(?)
            GROUP BY peak_hour
            ORDER BY SUM(total_activity) DESC, peak_hour ASC
            LIMIT 1
            """,
            (effective_as_of,),
        ).fetchone()
        top_grid = connection.execute(
            """
            SELECT grid_id
            FROM fact_network_activity
            WHERE timestamp = ?
            ORDER BY total_activity DESC, grid_id ASC
            LIMIT 1
            """,
            (effective_as_of,),
        ).fetchone()

        if peak is None or top_grid is None:
            raise AnalyticsDataError(
                f"No summary metrics could be derived for as_of={effective_as_of}"
            )

        return {
            "total_activity": float(metrics["total_activity"]),
            "active_grids": int(metrics["active_grids"]),
            "peak_hour": int(peak["peak_hour"]),
            "top_grid": int(top_grid["grid_id"]),
            "as_of": effective_as_of,
        }
