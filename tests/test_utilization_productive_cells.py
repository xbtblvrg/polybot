from datetime import UTC, datetime

from scripts import build_utilization_meter as meter


def test_strategy_lane_summary_counts_isolated_cells_in_one_resident(monkeypatch, tmp_path):
    cell_state = tmp_path / "matrix.json"
    cell_state.write_text('{"productive_cell_count": 8}')
    evidence_state = tmp_path / "matrix_evidence.json"
    evidence_state.write_text('{"generated_at":"2026-07-25T05:30:00Z"}')
    monkeypatch.setattr(meter, "_launchd_pid", lambda _label: 123)
    monkeypatch.setattr(meter, "_process_usage", lambda _pid: {"rss_mb": 80.0, "cpu_pct": 1.0})
    monkeypatch.setattr(
        meter,
        "_load_json",
        lambda path, default: {"productive_cell_count": 8}
        if str(path).endswith("matrix.json")
        else {"generated_at": "2026-07-25T05:30:00Z"}
        if str(path).endswith("matrix_evidence.json")
        else default,
    )
    result = meter._strategy_lane_summary(
        {
            "rows": [
                {
                    "mechanism_id": "matrix",
                    "status": "PAPER",
                    "evidence_generated_at": "2026-07-25T05:30:00Z",
                    "runner_binding": {
                        "label": "matrix-label",
                        "freshness_slo_s": 60,
                        "evidence_artifact": str(evidence_state),
                        "productive_cell_artifact": str(cell_state),
                        "productive_cell_field": "productive_cell_count",
                    },
                }
            ]
        },
        now=datetime(2026, 7, 25, 5, 30, 30, tzinfo=UTC),
    )
    assert result["productive_lane_count"] == 8
    assert result["productive_services"]["matrix-label"]["productive_lane_count"] == 8
