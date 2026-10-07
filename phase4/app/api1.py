from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from .api1_service import AnalyticsDataError, get_network_summary
from .models import NetworkSummaryResponse


router = APIRouter(prefix="/network", tags=["API1 - Network Summary"])


@router.get("/summary", response_model=NetworkSummaryResponse)
def network_summary(
    as_of: str | None = Query(
        default=None,
        description="Hourly warehouse timestamp. Defaults to the latest available timestamp.",
    ),
) -> NetworkSummaryResponse:
    try:
        return NetworkSummaryResponse.model_validate(get_network_summary(as_of))
    except (AnalyticsDataError, FileNotFoundError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
