from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone

from .api1_service import AnalyticsDataError
from .config import PIPELINE_STATUS_PATH
from .database import get_connection


TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"


def _freshness_hours(as_of: str) -> float:
    analytics_time = datetime.strptime(as_of, TIMESTAMP_FORMAT)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    return round(max(0.0, (now - analytics_time).total_seconds() / 3600), 2)


def get_pipeline_status() -> dict[str, object]:
    if not PIPELINE_STATUS_PATH.is_file():
        raise FileNotFoundError(f"Pipeline status record not found: {PIPELINE_STATUS_PATH}")

    with PIPELINE_STATUS_PATH.open(encoding="utf-8") as file:
        record = json.load(file)

    required = (
        "run_id",
        "run_timestamp",
        "pipeline_status",
        "task_status",
        "rows_in",
        "rows_rejected",
        "nulls_handled",
        "rows_published",
        "as_of",
    )
    missing = [field for field in required if field not in record]
    if missing:
        raise AnalyticsDataError(
            f"Pipeline status record is missing required fields: {', '.join(missing)}"
        )

    reasons: list[str] = []
    if record["pipeline_status"] != "SUCCESS":
        reasons.append(f"pipeline_status={record['pipeline_status']}")
    failed_tasks = [
        name for name, status in record["task_status"].items() if status != "SUCCESS"
    ]
    if failed_tasks:
        reasons.append(f"failed_tasks={','.join(failed_tasks)}")
    if int(record["rows_published"]) <= 0:
        reasons.append("rows_published must be greater than zero")

    return {
        **record,
        "healthy": not reasons,
        "reasons": reasons,
        "freshness_hours": _freshness_hours(str(record["as_of"])),
    }


def get_grid_location(grid_id: int) -> dict[str, int | float | str]:
    if not 1 <= grid_id <= 10000:
        raise AnalyticsDataError(f"Grid {grid_id} was not found")
    with get_connection() as connection:
        row = connection.execute(
            """
            SELECT grid_id, centroid_lat, centroid_lon, geometry_ref
            FROM dim_grid
            WHERE grid_id = ?
            """,
            (grid_id,),
        ).fetchone()
    if row is None:
        raise AnalyticsDataError(f"Grid {grid_id} was not found")
    return dict(row)
