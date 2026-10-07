from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class NetworkSummaryResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "total_activity": 4550702.6679,
                "active_grids": 10000,
                "peak_hour": 14,
                "top_grid": 4857,
                "as_of": "2013-11-07 23:00:00",
            }
        }
    )

    total_activity: float
    active_grids: int
    peak_hour: int
    top_grid: int
    as_of: str