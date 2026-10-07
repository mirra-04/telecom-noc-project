from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class GridActivityPoint(BaseModel):
    timestamp: str
    sms_in: float
    sms_out: float
    total_sms: float
    call_in: float
    call_out: float
    total_calls: float
    internet_activity: float
    total_activity: float


class GridActivityResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "grid_id": 4821,
                "as_of": "2013-11-07 23:00:00",
                "points": [
                    {
                        "timestamp": "2013-11-07 23:00:00",
                        "sms_in": 0.0,
                        "sms_out": 0.0,
                        "total_sms": 0.0,
                        "call_in": 0.0,
                        "call_out": 0.0,
                        "total_calls": 0.0,
                        "internet_activity": 254.9675,
                        "total_activity": 254.9675,
                    }
                ],
            }
        }
    )

    grid_id: int
    as_of: str
    points: list[GridActivityPoint]
