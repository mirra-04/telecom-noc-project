from __future__ import annotations

from pydantic import BaseModel, ConfigDict


class OperationalItem(BaseModel):
    grid_id: int
    timestamp: str
    severity: str
    reason: str
    risk_score: float | None = None
    risk_level: str | None = None
    model_version: str | None = None


class HotspotItem(OperationalItem):
    total_activity: float
    status: str


class AlertItem(OperationalItem):
    alert_type: str
    current_activity: float
    baseline_activity: float


class HotspotsResponse(BaseModel):
    model_config = ConfigDict(
        json_schema_extra={
            "example": {
                "as_of": "2013-11-07 23:00:00",
                "items": [
                    {
                        "grid_id": 4857,
                        "timestamp": "2013-11-07 23:00:00",
                        "total_activity": 8344.6583,
                        "status": "HOTSPOT",
                        "severity": "HIGH",
                        "reason": "Highest total activity in the selected reporting interval",
                        "risk_score": None,
                        "risk_level": None,
                        "model_version": None,
                    }
                ],
            }
        }
    )

    as_of: str
    items: list[HotspotItem]


class AlertsResponse(BaseModel):
    as_of: str
    items: list[AlertItem]
