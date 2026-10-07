from __future__ import annotations

from fastapi import APIRouter, HTTPException

from .api1_service import AnalyticsDataError
from .api6_models import GridLocationResponse, PipelineStatusResponse
from .api6_service import get_grid_location, get_pipeline_status


router = APIRouter(tags=["API6 - Operational Support"])


@router.get("/pipeline/status", response_model=PipelineStatusResponse)
def pipeline_status() -> PipelineStatusResponse:
    try:
        return PipelineStatusResponse.model_validate(get_pipeline_status())
    except AnalyticsDataError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except (FileNotFoundError, ValueError, KeyError, TypeError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc


@router.get("/network/grid/{grid_id}/location", response_model=GridLocationResponse)
def grid_location(grid_id: int) -> GridLocationResponse:
    try:
        return GridLocationResponse.model_validate(get_grid_location(grid_id))
    except AnalyticsDataError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
