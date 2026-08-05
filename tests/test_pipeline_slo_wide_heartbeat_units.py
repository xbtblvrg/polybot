from datetime import UTC, datetime
import json
import os
from pathlib import Path

from scripts.report_pipeline_slo import grade_wide_supervisor_heartbeat


def _fixture(tmp_path: Path, *, run_id: str, start_s: float) -> tuple[Path, Path, Path, Path]:
    root = tmp_path / "repo"
    data = root / "data" / "research"
    agents = tmp_path / "LaunchAgents"
    data.mkdir(parents=True)
    agents.mkdir()
    (agents / "com.belavarga.polymarket.wide-prospective-supervisor.plist").write_text(
        "plist"
    )
    (data / "wide_prospective_supervisor.lock").write_text("123")
    (data / "wide_supervisor_heartbeat_state.json").write_text("{}")
    (data / "wide_exact_policy_manifest_active.json").write_text(
        json.dumps(
            {
                "manifest_path": "data/research/manifest.json",
                "published_at_s": start_s,
            }
        )
    )
    (data / "manifest.json").write_text(json.dumps({"score_run_id": run_id}))
    marker = data / f"wide_candidate_standings_{run_id}.json"
    marker.write_text("{}")
    os.utime(marker, (start_s, start_s))
    capture = data / f"polygon_orderfilled_ws_capture_alpha_decay_{run_id}.jsonl"
    capture.write_bytes(b"start\n")
    (data / "wide_prospective_supervisor_state.json").write_text(
        json.dumps(
            {
                "latest_cycles": [
                    {"started_at_s": start_s + offset * 490.0}
                    for offset in range(12)
                ]
            }
        )
    )
    return root, agents, marker, capture


def _grade(root: Path, agents: Path, now_s: float) -> dict:
    return grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(now_s, tz=UTC),
        process_alive=lambda pid: pid == 123,
        launchd_last_exit_code=0,
    )


def test_healthy_slow_scorer_uses_advancing_capture_bytes_for_liveness(
    tmp_path: Path,
) -> None:
    root, agents, marker, capture = _fixture(tmp_path, run_id="wide_healthy", start_s=1000.0)

    assert _grade(root, agents, 1000.0)["status"] == "PASS"
    for now_s in range(1030, 1301, 30):
        capture.write_bytes(capture.read_bytes() + b"tick\n")
        heartbeat = _grade(root, agents, float(now_s))
        assert heartbeat["status"] == "PASS"

    assert heartbeat["last_cut_run_id"] == "wide_healthy"
    assert heartbeat["capture_advanced"] is True
    assert heartbeat["progress_span_s"] == 0.0
    assert marker.stat().st_mtime == 1000.0
    assert heartbeat["scorer_cycle_period"]["status"] == "BREACH"
    assert heartbeat["scorer_cycle_period"]["achieved_period_s"] == 490.0
    assert heartbeat["scorer_cycle_period"]["sample_count"] == 11


def test_genuinely_dead_capture_fires_on_frozen_bytes(
    tmp_path: Path,
) -> None:
    root, agents, _marker, capture = _fixture(tmp_path, run_id="wide_wedged", start_s=1000.0)

    first = _grade(root, agents, 1000.0)
    assert first["status"] == "PASS"
    assert first["capture_size_bytes"] == capture.stat().st_size

    heartbeat = _grade(root, agents, 1200.0)

    assert heartbeat["status"] == "PRODUCER_NO_FORWARD_PROGRESS"
    assert heartbeat["last_cut_run_id"] == "wide_wedged"
    assert heartbeat["capture_advanced"] is False
    assert heartbeat["progress_span_s"] == 200.0
    assert heartbeat["stall_threshold_s"] == 90.0


def test_heartbeat_publish_period_compares_achieved_gap_to_declared_interval(
    tmp_path: Path,
) -> None:
    root, agents, _marker, capture = _fixture(
        tmp_path, run_id="wide_first", start_s=1000.0
    )
    first = _grade(root, agents, 1000.0)
    assert first["heartbeat_publish_period"]["declared_interval_s"] == 60.0

    data = root / "data" / "research"
    (data / "wide_exact_policy_manifest_active.json").write_text(
        json.dumps(
            {
                "manifest_path": "data/research/manifest.json",
                "published_at_s": 1120.0,
            }
        )
    )
    (data / "manifest.json").write_text(
        json.dumps({"score_run_id": "wide_second"})
    )
    capture = data / "polygon_orderfilled_ws_capture_alpha_decay_wide_second.jsonl"
    capture.write_bytes(b"second\n")

    second = _grade(root, agents, 1120.0)
    period = second["heartbeat_publish_period"]
    assert period["status"] == "BREACH"
    assert period["achieved_period_s"] == 120.0
    assert period["completed_gap_median_s"] == 120.0
    assert period["achieved_to_declared_ratio"] == 2.0
    assert period["sample_count"] == 1


def test_stale_heartbeat_open_interval_is_measured_as_achieved_lower_bound(
    tmp_path: Path,
) -> None:
    root, agents, _marker, capture = _fixture(
        tmp_path, run_id="wide_stale", start_s=1000.0
    )
    assert _grade(root, agents, 1000.0)["heartbeat_publish_period"]["status"] == "PASS"
    capture.write_bytes(capture.read_bytes() + b"producer-still-alive\n")

    stale = _grade(root, agents, 1120.0)["heartbeat_publish_period"]
    assert stale["status"] == "BREACH"
    assert stale["achieved_period_s"] == 120.0
    assert stale["current_interval_elapsed_s"] == 120.0
    assert stale["sample_count"] == 0


def test_run_roll_resets_capture_observation_and_passes(
    tmp_path: Path,
) -> None:
    root, agents, _marker, _capture = _fixture(tmp_path, run_id="wide_old", start_s=1000.0)

    assert _grade(root, agents, 1000.0)["status"] == "PASS"
    data = root / "data/research"
    (data / "manifest.json").write_text(json.dumps({"score_run_id": "wide_new"}))
    new_capture = data / "polygon_orderfilled_ws_capture_alpha_decay_wide_new.jsonl"
    new_capture.write_bytes(b"new-run\n")
    heartbeat = _grade(root, agents, 1200.0)

    assert heartbeat["status"] == "PASS"
    assert heartbeat["last_cut_run_id"] == "wide_new"
    assert heartbeat["capture_advanced"] is True
    assert heartbeat["progress_span_s"] == 0.0
