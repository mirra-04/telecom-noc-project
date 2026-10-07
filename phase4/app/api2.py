from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from .api1_service import AnalyticsDataError
from .api2_models import GridActivityResponse
from .api2_service import get_grid_activity


router = APIRouter(prefix="/network", tags=["API2 - Grid Activity"])


@router.get("/grid/{grid_id}", response_model=GridActivityResponse)
def grid_activity(
    grid_id: int,
    date: str | None = Query(
        default=None,
        description="Optional calendar date in YYYY-MM-DD format.",
    ),
    hour: int | None = Query(
        default=None,
        ge=0,
        le=23,
        description="Optional hour-of-day filter from 0 through 23.",
    ),
    as_of: str | None = Query(
        default=None,
        description="Optional ending timestamp in YYYY-MM-DD HH:MM:SS format.",
    ),
) -> GridActivityResponse:
    try:
        return GridActivityResponse.model_validate(
            get_grid_activity(grid_id, date, hour, as_of)
        )
    except AnalyticsDataError as exc:
        status_code = 404 if "was not found" in str(exc) else 500
        raise HTTPException(status_code=status_code, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
