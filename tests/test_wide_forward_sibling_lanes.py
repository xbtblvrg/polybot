import json

from scripts import run_wide_forward_sibling_lanes as sibling_runner
from scripts.report_bac25_forward_writer_scope import build_report
from scripts.run_wide_forward_sibling_lanes import SIBLING_LANES, run_cycle


def test_writer_scope_proves_forward_and_park_artifacts_are_disjoint():
    binding_id = "widebind_b968c71aae449a34c27330fc"
    terminal = {
        "status": "PARK_SEAT_UNFED_CLOCK",
        "stop_writer": True,
        "deadline_at": "2026-07-31T06:44:50.331867Z",
    }
    report = build_report(
        supervisor_state={
            "status": "CAPTURE_AND_SCORER_RESIDENT",
            "managed_run_id": "wide_test",
            "capture_pid": 22,
        },
        binding_artifact={
            "binding": {
                "source_binding_id": binding_id,
                "terminal_outcome_on_deadline": terminal,
            }
        },
        ready_shadow_state={
            "lanes": [
                {
                    "source_binding_id": binding_id,
                    "terminal_outcome_on_deadline": terminal,
                }
            ]
        },
        process_label="com.polymarket.wide-prospective-supervisor",
        process_pid=11,
        process_command=["python", "scripts/run_wide_prospective_supervisor.py"],
        sibling_process_pid=33,
        sibling_process_command=[
            "python",
            "scripts/run_wide_forward_sibling_lanes.py",
        ],
        generated_at="2026-07-30T22:00:00+00:00",
    )

    assert report["status"] == "DISJOINT_WRITER_SCOPES_PROVEN"
    assert report["artifact_intersection"] == []
    assert all(report["checks"].values())
    assert len(report["forward_lanes"]) == 3
    assert {row["wide_policy_fingerprint"][:8] for row in report["forward_lanes"]} == {
        "bac25bed",
        "8bb70201",
        "fdd8af33",
    }
    assert report["sibling_writer"]["pid"] == 33
    assert len(report["sibling_writer"]["fingerprints"]) == 2


def test_writer_scope_fails_without_resident_sibling_writer():
    report = build_report(
        supervisor_state={"status": "CAPTURE_AND_SCORER_RESIDENT"},
        binding_artifact={"binding": {"source_binding_id": ""}},
        ready_shadow_state={},
        process_label="test",
        process_pid=11,
        process_command=["scripts/run_wide_prospective_supervisor.py"],
        sibling_process_pid=0,
        sibling_process_command=[],
        generated_at="2026-07-30T22:00:00+00:00",
    )

    assert report["status"] == "WRITER_SCOPE_PROOF_FAILED"
    assert report["checks"]["sibling_process_is_resident"] is False


def test_main_exits_zero_when_singleton_lock_is_held(monkeypatch):
    monkeypatch.setattr(
        sibling_runner,
        "parse_args",
        lambda: type(
            "Args",
            (),
            {
                "lock_file": "held.lock",
                "iterations": 1,
                "supervisor_state": "unused.json",
                "resolutions": "unused.jsonl",
                "state": "unused-state.json",
                "sleep_s": 0.0,
            },
        )(),
    )
    monkeypatch.setattr(
        sibling_runner,
        "acquire_lock",
        lambda _path: (_ for _ in ()).throw(
            RuntimeError("WIDE_FORWARD_SIBLING_LANES_LOCK_HELD")
        ),
    )

    assert sibling_runner.main() == 0


def test_sibling_cycle_scores_both_exact_fingerprints(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    seed_alpha = tmp_path / "data/research/alpha_decay_report_seed.json"
    polygon = (
        tmp_path
        / "data/research/polygon_orderfilled_ws_capture_alpha_decay_managed.jsonl"
    )
    seed_alpha.parent.mkdir(parents=True)
    seed_alpha.write_text(json.dumps({"status": "PASS_CURRENT_SOURCE"}))
    polygon.write_text("")
    calls = []

    def scorer(**kwargs):
        calls.append(kwargs)
        return [{"ok": True}]

    cycle = run_cycle(
        supervisor_state={
            "status": "CAPTURE_AND_SCORER_RESIDENT",
            "managed_run_id": "managed",
            "seed_run_id": "seed",
        },
        resolution_path="resolutions.jsonl",
        scorer=scorer,
    )

    assert cycle["status"] == "SIBLING_FORWARD_LANES_SCORED"
    assert [call["spec"] for call in calls] == list(SIBLING_LANES)
    assert all(call["direct_event"] is None for call in calls)
    assert len({lane.fingerprint for lane in SIBLING_LANES}) == 2
    assert all("forward_only" in lane.run_id for lane in SIBLING_LANES)
    assert len({lane.atomic_output for lane in SIBLING_LANES}) == 2
    assert all("atomic_move_slice_rescore" in lane.atomic_output for lane in SIBLING_LANES)


def test_sibling_cycle_records_scorer_exception_without_exiting(
    tmp_path, monkeypatch
):
    monkeypatch.chdir(tmp_path)
    seed_alpha = tmp_path / "data/research/alpha_decay_report_seed.json"
    polygon = (
        tmp_path
        / "data/research/polygon_orderfilled_ws_capture_alpha_decay_managed.jsonl"
    )
    seed_alpha.parent.mkdir(parents=True)
    seed_alpha.write_text("{}")
    polygon.write_text("")

    def scorer(**kwargs):
        raise TimeoutError(kwargs["spec"].run_id)

    cycle = run_cycle(
        supervisor_state={
            "status": "CAPTURE_AND_SCORER_RESIDENT",
            "managed_run_id": "managed",
            "seed_run_id": "seed",
        },
        resolution_path="resolutions.jsonl",
        scorer=scorer,
    )

    assert cycle["status"] == "SIBLING_FORWARD_LANES_CYCLE_FAILED"
    assert len(cycle["failed_results"]) == 2
    assert all("TimeoutError" in row["error"] for row in cycle["failed_results"])
    assert all(lane.observation_window_s == 172_800 for lane in SIBLING_LANES)
