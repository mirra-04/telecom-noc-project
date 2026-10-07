from __future__ import annotations

from fastapi import APIRouter, HTTPException

from .api1_service import AnalyticsDataError
from .api4_models import GridFeaturesResponse
from .api4_service import get_grid_features


router = APIRouter(prefix="/network", tags=["API4 - Grid Features"])


@router.get("/grid/{grid_id}/features", response_model=GridFeaturesResponse)
def grid_features(grid_id: int) -> GridFeaturesResponse:
    try:
        return GridFeaturesResponse.model_validate(get_grid_features(grid_id))
    except AnalyticsDataError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
