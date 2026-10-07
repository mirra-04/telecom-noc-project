from __future__ import annotations

from phase7.c2_assistant import NetworkOperationsAssistant


def test_situation_answer_calls_pipeline_before_network_evidence() -> None:
    calls: list[str] = []

    def tool(name: str, value: dict[str, object]):
        def run(**_: object) -> dict[str, object]:
            calls.append(name)
            return value

        return run

    assistant = NetworkOperationsAssistant(
        {
            "get_pipeline_status": tool("status", {"healthy": True}),
            "get_network_summary": tool("summary", {"as_of": "t"}),
            "get_hotspots": tool("hotspots", {"items": []}),
            "get_alerts": tool("alerts", {"items": []}),
        }
    )

    result = assistant.answer("Which areas need attention right now?")

    assert calls == ["status", "summary", "hotspots", "alerts"]
    assert "Sources:" in result["answer"]
    assert "congestion diagnosis" in result["answer"]


def test_failed_tool_is_reported_instead_of_hidden() -> None:
    assistant = NetworkOperationsAssistant(
        {
            "get_pipeline_status": lambda **_: (_ for _ in ()).throw(RuntimeError("service down")),
        }
    )

    result = assistant.answer("Which areas need attention?")

    assert "failed" in result["answer"]
    assert result["tool_log"][0]["error"] == "service down"


def test_grid_follow_up_calls_required_tools() -> None:
    assistant = NetworkOperationsAssistant(
        {
            name: (lambda name=name, **_: {"tool": name})
            for name in (
                "get_pipeline_status",
                "get_grid_activity",
                "get_grid_features",
                "get_anomaly_score",
                "get_grid_location",
            )
        }
    )

    result = assistant.answer("Explain Grid 4821.")

    assert "Grid 4821" in result["answer"]
    assert {call["name"] for call in result["tool_log"]} == {
        "get_pipeline_status",
        "get_grid_activity",
        "get_grid_features",
        "get_anomaly_score",
        "get_grid_location",
    }
