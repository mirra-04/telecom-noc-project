from __future__ import annotations

import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from phase4.app.api1_service import get_network_summary
from phase4.app.api2_service import get_grid_activity
from phase4.app.api3_service import get_alerts, get_hotspots
from phase4.app.api4_service import get_grid_features
from phase4.app.api6_service import get_grid_location, get_pipeline_status
from phase4.app.config import RISK_SCORES_PATH


ToolFunction = Callable[..., dict[str, object]]


@dataclass(frozen=True)
class ToolCall:
    name: str
    arguments: dict[str, object]
    result: dict[str, object] | None = None
    error: str | None = None


def _get_anomaly_score(grid_id: int, as_of: str | None = None) -> dict[str, object]:
    if not RISK_SCORES_PATH.is_file():
        raise FileNotFoundError(f"Risk score artifact not found: {RISK_SCORES_PATH}")
    matches: list[dict[str, object]] = []
    with RISK_SCORES_PATH.open(newline="", encoding="utf-8") as file:
        for row in csv.DictReader(file):
            if int(row["grid_id"]) != grid_id:
                continue
            if as_of is not None and row["feature_timestamp"] > as_of:
                continue
            matches.append(
                {
                    "grid_id": grid_id,
                    "feature_timestamp": row["feature_timestamp"],
                    "risk_score": float(row["risk_score"]),
                    "risk_level": row["risk_level"],
                    "model_version": row["model_version"],
                    "reason": row["reason"],
                }
            )
    if not matches:
        raise ValueError(f"No anomaly or risk score exists for grid {grid_id}")
    return max(matches, key=lambda row: str(row["feature_timestamp"]))


def build_local_tools() -> dict[str, ToolFunction]:
    """Expose only curated service/artifact outputs to the assistant."""

    return {
        "get_network_summary": get_network_summary,
        "get_grid_activity": get_grid_activity,
        "get_hotspots": get_hotspots,
        "get_grid_features": get_grid_features,
        "get_anomaly_score": _get_anomaly_score,
        "get_grid_location": get_grid_location,
        "get_pipeline_status": get_pipeline_status,
    }


class NetworkOperationsAssistant:
    def __init__(self, tools: dict[str, ToolFunction] | None = None) -> None:
        self.tools = tools or build_local_tools()
        self.tool_log: list[ToolCall] = []

    def call_tool(self, name: str, **arguments: object) -> dict[str, object] | None:
        if name not in self.tools:
            call = ToolCall(name, arguments, error="Tool is not available")
            self.tool_log.append(call)
            return None
        try:
            result = self.tools[name](**arguments)
        except Exception as exc:
            self.tool_log.append(ToolCall(name, arguments, error=str(exc)))
            return None
        self.tool_log.append(ToolCall(name, arguments, result=result))
        return result

    def answer(self, question: str) -> dict[str, object]:
        normalized = question.lower()
        if "attention" in normalized or "areas" in normalized or "hotspot" in normalized:
            return self._situation_answer()
        if "grid" in normalized:
            grid_id = _extract_grid_id(question)
            if grid_id is None:
                return {"answer": "Please provide a numeric grid ID.", "tool_log": []}
            return self._grid_answer(grid_id)
        return {
            "answer": "I can answer attention-area questions or explain a specific grid.",
            "tool_log": [],
        }

    def _situation_answer(self) -> dict[str, object]:
        status = self.call_tool("get_pipeline_status")
        summary = self.call_tool("get_network_summary")
        hotspots = self.call_tool("get_hotspots", limit=10)
        alerts = self.call_tool("get_alerts", limit=10)
        return {
            "answer": _situation_text(status, summary, hotspots, alerts),
            "tool_log": [call.__dict__ for call in self.tool_log],
        }

    def _grid_answer(self, grid_id: int) -> dict[str, object]:
        status = self.call_tool("get_pipeline_status")
        activity = self.call_tool("get_grid_activity", grid_id=grid_id)
        features = self.call_tool("get_grid_features", grid_id=grid_id)
        anomaly = self.call_tool("get_anomaly_score", grid_id=grid_id)
        location = self.call_tool("get_grid_location", grid_id=grid_id)
        return {
            "answer": _grid_text(status, activity, features, anomaly, location, grid_id),
            "tool_log": [call.__dict__ for call in self.tool_log],
        }


def _extract_grid_id(question: str) -> int | None:
    tokens = question.replace("?", " ").split()
    for index, token in enumerate(tokens):
        if token.lower() == "grid" and index + 1 < len(tokens):
            try:
                return int(tokens[index + 1].strip(".,!?"))
            except ValueError:
                return None
    return None


def _sources(calls: list[tuple[str, object | None]]) -> str:
    return "; ".join(
        f"{name} ({'available' if value is not None else 'failed'})" for name, value in calls
    )


def _situation_text(
    status: dict[str, object] | None,
    summary: dict[str, object] | None,
    hotspots: dict[str, object] | None,
    alerts: dict[str, object] | None,
) -> str:
    health = "unknown because get_pipeline_status failed"
    if status is not None:
        health = "healthy" if status.get("healthy") else f"not healthy: {status.get('reasons')}"
    return (
        f"Observed evidence: pipeline status is {health}. "
        f"Summary={summary}; hotspots={hotspots}; alerts={alerts}. "
        "Interpretation: these are investigation signals, not a confirmed "
        "congestion diagnosis or network fault. "
        f"Sources: {_sources([('get_pipeline_status', status), ('get_network_summary', summary), ('get_hotspots', hotspots), ('get_alerts', alerts)])}."
    )


def _grid_text(
    status: dict[str, object] | None,
    activity: dict[str, object] | None,
    features: dict[str, object] | None,
    anomaly: dict[str, object] | None,
    location: dict[str, object] | None,
    grid_id: int,
) -> str:
    return (
        f"Grid {grid_id}. Observed evidence: location={location}; activity={activity}; "
        f"features={features}; anomaly/risk={anomaly}; pipeline_status={status}. "
        "Interpretation: review the evidence as an investigation signal; it "
        "does not establish congestion or a confirmed network fault. "
        f"Sources: {_sources([('get_pipeline_status', status), ('get_grid_activity', activity), ('get_grid_features', features), ('get_anomaly_score', anomaly), ('get_grid_location', location)])}."
    )
