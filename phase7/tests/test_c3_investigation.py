from __future__ import annotations

from phase7.c3_investigation import (
    InvestigationContext,
    _summarize_history,
    build_investigation_prompt,
    offline_investigation_report,
)


def test_history_is_summarized_without_raw_rows() -> None:
    summary = _summarize_history(
        [
            {"timestamp": "t1", "total_activity": 10.0},
            {"timestamp": "t2", "total_activity": 30.0},
        ]
    )

    assert summary == {
        "intervals": 2,
        "first_timestamp": "t1",
        "last_timestamp": "t2",
        "min_total_activity": 10.0,
        "max_total_activity": 30.0,
        "average_total_activity": 20.0,
    }


def test_prompt_has_required_sections_and_safety_language() -> None:
    context = InvestigationContext(
        grid_id=4821,
        as_of="2013-11-07 23:00:00",
        current_evidence={"timestamp": "t", "total_activity": 10.0},
        historical_evidence={"intervals": 23},
        prior_alerts=[],
        features={"avg_activity": 10.0},
        model_score={"risk_level": "LOW"},
        location={"centroid_lat": 45.4},
        pipeline_status={"healthy": True},
        failures=["alerts: unavailable"],
    )

    prompt = build_investigation_prompt(context)

    assert "CURRENT EVIDENCE" in prompt
    assert "HISTORICAL EVIDENCE" in prompt
    assert "UNCERTAINTIES" in prompt
    assert "not proof of congestion" in prompt
    assert "alerts: unavailable" in prompt


def test_offline_report_preserves_uncertainty() -> None:
    context = InvestigationContext(
        grid_id=4821,
        as_of="t",
        current_evidence=None,
        historical_evidence=None,
        prior_alerts=[],
        features=None,
        model_score=None,
        location=None,
        pipeline_status=None,
        failures=["activity: service down"],
    )

    report = offline_investigation_report(context)

    assert report["uncertainty"] == ["activity: service down"]
    assert "confirmed network fault" in report["interpretation"]
