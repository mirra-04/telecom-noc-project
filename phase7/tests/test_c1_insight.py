from __future__ import annotations

import pytest

from phase7.c1_insight import CuratedEvidence, generate_insight


def _evidence() -> CuratedEvidence:
    return CuratedEvidence(
        as_of="2013-11-07 23:00:00",
        summary={"active_grids": 10000},
        hotspots=[{"grid_id": 4857, "severity": "HIGH"}],
        alerts=[{"grid_id": 896, "severity": "ATTENTION"}],
        risk=[{"grid_id": 4857, "risk_level": "HIGH"}],
        pipeline_status={"healthy": True},
    )


def test_offline_provider_generates_investigation_safe_insight() -> None:
    result = generate_insight(_evidence())

    assert "investigation signals" in result
    assert "congestion diagnosis" in result
    assert "confirmed network fault" in result


def test_missing_as_of_is_rejected() -> None:
    with pytest.raises(ValueError, match="as_of is required"):
        CuratedEvidence(
            as_of="",
            summary={},
            hotspots=[],
            alerts=[],
            risk=[],
            pipeline_status={},
        )
