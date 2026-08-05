import argparse
import json
from pathlib import Path

from scripts import build_utilization_meter


def _args(tmp_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        strategy_map=str(tmp_path / "data/research/strategy_map_latest.json"),
        scorecard=str(tmp_path / "data/research/wallet_copy_daily_scorecard_current.json"),
        output=str(tmp_path / "data/research/resource_utilization_latest.json"),
        lane_memory_budget_gb=2.0,
        min_idle_cpu_headroom_pct=25.0,
        min_idle_memory_headroom_pct=20.0,
    )


def _write_inputs(tmp_path: Path, rows: list[dict], *, day_pnl: float = 12.5) -> None:
    data = tmp_path / "data/research"
    data.mkdir(parents=True, exist_ok=True)
    (data / "strategy_map_latest.json").write_text(json.dumps({"rows": rows}), encoding="utf-8")
    (data / "wallet_copy_daily_scorecard_current.json").write_text(
        json.dumps(
            {
                "generated_at": "2999-01-01T00:00:00Z",
                "day_pnl_basis": {"day_pnl_response_basis": day_pnl},
                "volume_kpi": {
                    "canonical_daily": {"windows_filled": 58, "denominator_windows": 288}
                },
            }
        ),
        encoding="utf-8",
    )


def _write_evidence(tmp_path: Path, name: str, generated_at: str) -> str:
    relative = f"data/research/{name}"
    (tmp_path / relative).parent.mkdir(parents=True, exist_ok=True)
    (tmp_path / relative).write_text(
        json.dumps({"generated_at": generated_at}), encoding="utf-8"
    )
    return relative


def _patch_resources(monkeypatch) -> None:
    monkeypatch.setattr(
        build_utilization_meter,
        "_cpu_snapshot",
        lambda: {"status": "OK", "cpu_count": 8, "headroom_pct": 75.0, "load1": 2.0},
    )
    monkeypatch.setattr(
        build_utilization_meter,
        "_memory_snapshot",
        lambda: {
            "status": "OK",
            "available_gb": 10.0,
            "total_gb": 32.0,
            "headroom_pct": 31.25,
            "source": "unit",
        },
    )


def test_stale_and_unwired_gated_rows_count_zero_productive_lanes(monkeypatch, tmp_path: Path):
    stale_artifact = _write_evidence(tmp_path, "stale.json", "2020-01-01T00:00:00Z")
    _write_inputs(
        tmp_path,
        [
            {"id": "unwired", "status": "GATED", "evidence_generated_at": "2026-07-23T17:00:00Z"},
            {
                "id": "stale",
                "status": "GATED",
                "evidence_generated_at": "2020-01-01T00:00:00Z",
                "runner_binding": {
                    "label": "stale.service",
                    "freshness_slo_s": 60,
                    "evidence_artifact": stale_artifact,
                },
            },
        ],
    )
    _patch_resources(monkeypatch)
    monkeypatch.setattr(build_utilization_meter, "_launchd_pid", lambda label: 999)
    monkeypatch.setattr(
        build_utilization_meter, "_process_usage", lambda pid: {"cpu_pct": 1.0, "rss_mb": 100.0}
    )

    payload = build_utilization_meter.build_meter(tmp_path, _args(tmp_path))

    assert payload["registry_active_or_gated_lane_count"] == 2
    assert payload["registry_status_occupancy_pct"] == 100.0
    assert payload["productive_lane_count"] == 0
    assert payload["productive_utilization_pct"] is None
    assert payload["verdict"] == "INSUFFICIENT_PRODUCTIVE_BINDINGS"
    assert payload["defect"]["open"] is True


def test_running_fresh_service_drives_productive_resource_capacity(monkeypatch, tmp_path: Path):
    fresh_artifact = _write_evidence(tmp_path, "productive.json", "2999-01-01T00:00:00Z")
    _write_inputs(
        tmp_path,
        [
            {
                "id": "productive",
                "status": "PAPER",
                "evidence_generated_at": "2999-01-01T00:00:00Z",
                "runner_binding": {
                    "label": "paper.service",
                    "freshness_slo_s": 180,
                    "evidence_artifact": fresh_artifact,
                },
            },
            {"id": "metadata-only", "status": "GATED"},
        ],
    )
    _patch_resources(monkeypatch)
    monkeypatch.setattr(build_utilization_meter, "_launchd_pid", lambda label: 321)
    monkeypatch.setattr(
        build_utilization_meter, "_process_usage", lambda pid: {"cpu_pct": 5.0, "rss_mb": 1024.0}
    )

    payload = build_utilization_meter.build_meter(tmp_path, _args(tmp_path))

    assert payload["productive_lane_count"] == 1
    assert payload["lanes"]["additional_lanes_by_memory"] == 10
    assert payload["lanes"]["additional_lanes_by_cpu"] == 15
    assert payload["measured_max_lane_count"] == 11
    assert payload["idle_lane_capacity"] == 10
    assert payload["productive_utilization_pct"] == 9.090909
    assert payload["verdict"] == "IDLE_CAPACITY"


def test_any_measured_idle_capacity_is_a_defect_while_goal_unmet(monkeypatch, tmp_path: Path):
    fresh_artifact = _write_evidence(tmp_path, "productive.json", "2999-01-01T00:00:00Z")
    _write_inputs(
        tmp_path,
        [
            {
                "id": "productive",
                "status": "PAPER",
                "evidence_generated_at": "2999-01-01T00:00:00Z",
                "runner_binding": {
                    "label": "paper.service",
                    "freshness_slo_s": 180,
                    "evidence_artifact": fresh_artifact,
                },
            }
        ],
    )
    monkeypatch.setattr(
        build_utilization_meter,
        "_cpu_snapshot",
        lambda: {"status": "OK", "cpu_count": 8, "headroom_pct": 1.0, "load1": 7.9},
    )
    monkeypatch.setattr(
        build_utilization_meter,
        "_memory_snapshot",
        lambda: {
            "status": "OK",
            "available_gb": 2.0,
            "total_gb": 32.0,
            "headroom_pct": 1.0,
            "source": "unit",
        },
    )
    monkeypatch.setattr(build_utilization_meter, "_launchd_pid", lambda label: 321)
    monkeypatch.setattr(
        build_utilization_meter,
        "_process_usage",
        lambda pid: {"cpu_pct": 1.0, "rss_mb": 1024.0},
    )

    payload = build_utilization_meter.build_meter(tmp_path, _args(tmp_path))

    assert payload["idle_lane_capacity"] == 1
    assert payload["verdict"] == "IDLE_CAPACITY"
    assert payload["defect"]["open"] is True


def test_duplicate_rows_for_one_service_count_once(monkeypatch, tmp_path: Path):
    fresh_artifact = _write_evidence(tmp_path, "shared.json", "2999-01-01T00:00:00Z")
    binding = {
        "label": "shared.service",
        "freshness_slo_s": 180,
        "evidence_artifact": fresh_artifact,
    }
    _write_inputs(
        tmp_path,
        [
            {"id": "a", "status": "LIVE", "evidence_generated_at": "2999-01-01T00:00:00Z", "runner_binding": binding},
            {"id": "b", "status": "GATED", "evidence_generated_at": "2999-01-01T00:00:00Z", "runner_binding": binding},
        ],
        day_pnl=125.0,
    )
    _patch_resources(monkeypatch)
    monkeypatch.setattr(build_utilization_meter, "_launchd_pid", lambda label: 321)
    monkeypatch.setattr(
        build_utilization_meter, "_process_usage", lambda pid: {"cpu_pct": 5.0, "rss_mb": 1024.0}
    )
    payload = build_utilization_meter.build_meter(tmp_path, _args(tmp_path))
    assert payload["productive_lane_count"] == 1
    assert payload["registry_active_or_gated_lane_count"] == 2
    assert len(payload["lanes"]["productive_services"]) == 1
