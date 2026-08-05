import argparse

import pytest

from scripts.record_experiment_verdict import build_verdict


def _args(**overrides):
    base = {
        "experiment_id": "e7-spot-open-paper-lane",
        "flow_stage": "LEARN/SELF-DEV",
        "verdict": "E7_FALSIFIED",
        "decision": "UNLOAD_AND_ARCHIVE",
        "primary_metric": "resolved_paper_roi_pct",
        "metric_value": -46.270609,
        "threshold": "close if ROI <= -20%",
        "evidence": "357 resolved paper fills",
        "artifact": ["data/research/e7_spot_open_paper_lane_state.json"],
        "owner": "Codex/Fable",
    }
    base.update(overrides)
    return argparse.Namespace(**base)


def test_build_verdict_accepts_complete_record() -> None:
    payload = build_verdict(_args(), generated_at="2026-07-09T08:00:00Z")

    assert payload["kind"] == "experiment_verdict"
    assert payload["experiment_id"] == "e7-spot-open-paper-lane"
    assert payload["verdict"] == "E7_FALSIFIED"
    assert payload["metric_value"] == -46.270609


def test_build_verdict_rejects_missing_artifacts() -> None:
    with pytest.raises(ValueError, match="missing_result_artifacts"):
        build_verdict(_args(artifact=[]), generated_at="2026-07-09T08:00:00Z")
