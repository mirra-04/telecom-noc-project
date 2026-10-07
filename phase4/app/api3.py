from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from .api1_service import AnalyticsDataError
from .api3_models import AlertsResponse, HotspotsResponse
from .api3_service import get_alerts, get_hotspots


router = APIRouter(prefix="/network", tags=["API3 - Hotspots and Alerts"])


@router.get("/hotspots", response_model=HotspotsResponse)
def hotspots(
    limit: int = Query(default=10, ge=1, le=100),
    severity: str | None = Query(default=None),
    as_of: str | None = Query(default=None),
) -> HotspotsResponse:
    try:
        return HotspotsResponse.model_validate(get_hotspots(limit, severity, as_of))
    except (AnalyticsDataError, FileNotFoundError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/alerts", response_model=AlertsResponse)
def alerts(
    limit: int = Query(default=100, ge=1, le=1000),
    severity: str | None = Query(default=None),
    as_of: str | None = Query(default=None),
) -> AlertsResponse:
    try:
        return AlertsResponse.model_validate(get_alerts(limit, severity, as_of))
    except (AnalyticsDataError, FileNotFoundError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
