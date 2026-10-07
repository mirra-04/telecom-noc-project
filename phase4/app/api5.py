from __future__ import annotations

from fastapi import APIRouter, HTTPException

from .api5_models import RiskPredictionRequest, RiskPredictionResponse
from .api1_service import AnalyticsDataError
from .api5_service import predict_risk as predict_risk_model


router = APIRouter(prefix="/network", tags=["API5 - Risk Prediction"])


@router.post("/predict-risk", response_model=RiskPredictionResponse)
def predict_risk(request: RiskPredictionRequest) -> RiskPredictionResponse:
    try:
        return RiskPredictionResponse.model_validate(predict_risk_model(request))
    except (AnalyticsDataError, FileNotFoundError) as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
