from __future__ import annotations

from pydantic import BaseModel


class PipelineStatusResponse(BaseModel):
    healthy: bool
    reasons: list[str]
    run_id: str
    run_timestamp: str
    pipeline_status: str
    task_status: dict[str, str]
    rows_in: int
    rows_rejected: int
    nulls_handled: int
    rows_published: int
    as_of: str
    freshness_hours: float


class GridLocationResponse(BaseModel):
    grid_id: int
    centroid_lat: float
    centroid_lon: float
    geometry_ref: str
