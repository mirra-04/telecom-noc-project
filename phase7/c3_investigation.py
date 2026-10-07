from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from phase4.app.api2_service import get_grid_activity
from phase4.app.api3_service import get_alerts
from phase4.app.api4_service import get_grid_features
from phase4.app.api6_service import get_grid_location, get_pipeline_status
from phase7.c2_assistant import _get_anomaly_score


@dataclass(frozen=True)
class InvestigationContext:
    grid_id: int
    as_of: str
    current_evidence: dict[str, object] | None
    historical_evidence: dict[str, object] | None
    prior_alerts: list[dict[str, object]]
    features: dict[str, object] | None
    model_score: dict[str, object] | None
    location: dict[str, object] | None
    pipeline_status: dict[str, object] | None
    failures: list[str]


def collect_context(
    grid_id: int,
    as_of: str | None = None,
    tools: dict[str, Callable[..., dict[str, object]]] | None = None,
) -> InvestigationContext:
    """Collect bounded API evidence and summarize history before prompting."""

    selected = tools or {
        "activity": get_grid_activity,
        "alerts": get_alerts,
        "features": get_grid_features,
        "anomaly": _get_anomaly_score,
        "location": get_grid_location,
        "pipeline": get_pipeline_status,
    }
    failures: list[str] = []

    def call(name: str, **arguments: object) -> dict[str, object] | None:
        try:
            return selected[name](**arguments)
        except Exception as exc:
            failures.append(f"{name}: {exc}")
            return None

    pipeline = call("pipeline")
    activity = call("activity", grid_id=grid_id, as_of=as_of)
    effective_as_of = as_of or (
        str(activity.get("as_of")) if activity is not None else "unknown"
    )
    features = call("features", grid_id=grid_id)
    anomaly = call("anomaly", grid_id=grid_id, as_of=as_of)
    location = call("location", grid_id=grid_id)
    alert_payload = call("alerts", limit=1000, as_of=effective_as_of)
    alerts = (
        [
            item
            for item in alert_payload.get("items", [])
            if int(item["grid_id"]) == grid_id
        ]
        if alert_payload is not None
        else []
    )

    points = activity.get("points", []) if activity is not None else []
    historical = _summarize_history(points[:-1]) if points else None
    current = points[-1] if points else None
    return InvestigationContext(
        grid_id=grid_id,
        as_of=effective_as_of,
        current_evidence=current,
        historical_evidence=historical,
        prior_alerts=alerts,
        features=features,
        model_score=anomaly,
        location=location,
        pipeline_status=pipeline,
        failures=failures,
    )


def _summarize_history(points: list[dict[str, object]]) -> dict[str, object]:
    activity = [float(point["total_activity"]) for point in points]
    if not activity:
        return {"intervals": 0}
    return {
        "intervals": len(activity),
        "first_timestamp": points[0]["timestamp"],
        "last_timestamp": points[-1]["timestamp"],
        "min_total_activity": min(activity),
        "max_total_activity": max(activity),
        "average_total_activity": sum(activity) / len(activity),
    }


def build_investigation_prompt(context: InvestigationContext) -> str:
    """Create a small structured context instead of dumping historical rows."""

    return (
        "Investigate a possible unusual activity pattern. Use only this curated "
        "evidence. Separate CURRENT EVIDENCE, HISTORICAL EVIDENCE and "
        "UNCERTAINTY. Distinguish observed metrics from inference. Activity, "
        "anomaly and risk are investigation signals, not proof of congestion "
        "or a confirmed network fault. Report failed evidence sources.\n\n"
        f"GRID: {context.grid_id}\nAS_OF: {context.as_of}\n"
        f"CURRENT EVIDENCE: {context.current_evidence}\n"
        f"HISTORICAL EVIDENCE: {context.historical_evidence}\n"
        f"PRIOR ALERTS: {context.prior_alerts}\n"
        f"FEATURES: {context.features}\n"
        f"MODEL SCORE: {context.model_score}\n"
        f"LOCATION: {context.location}\n"
        f"PIPELINE STATUS: {context.pipeline_status}\n"
        f"UNCERTAINTIES / FAILED SOURCES: {context.failures}\n"
    )


def offline_investigation_report(context: InvestigationContext) -> dict[str, object]:
    """Produce a deterministic report for validation without an API key."""

    return {
        "current_evidence": context.current_evidence,
        "historical_evidence": context.historical_evidence,
        "uncertainty": context.failures
        or ["No collection failures; interpretation remains limited to curated evidence."],
        "interpretation": (
            "The evidence may justify investigation, but does not establish "
            "congestion or a confirmed network fault."
        ),
        "prompt": build_investigation_prompt(context),
    }
