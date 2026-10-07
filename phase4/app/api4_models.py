from __future__ import annotations

from pydantic import BaseModel


class GridFeaturesResponse(BaseModel):
    grid_id: int
    avg_activity: float
    activity_growth: float
    active_hours: int
    peak_ratio: float
    variability: float
    internet_share: float
    feature_timestamp: str
    data_quality: str
    freshness_hours: float
