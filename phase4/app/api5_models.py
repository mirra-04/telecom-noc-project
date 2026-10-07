from __future__ import annotations

from pydantic import BaseModel, Field


class RiskPredictionRequest(BaseModel):
    grid_id: int = Field(..., ge=1, le=10000)
    as_of: str | None = Field(
        default=None,
        description="Optional warehouse timestamp for the future model implementation.",
    )


class RiskPredictionResponse(BaseModel):
    risk_score: float
    risk_level: str
    model_version: str
    explanation_note: str
    feature_timestamp: str
