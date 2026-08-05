from datetime import UTC, datetime
import os
from pathlib import Path

import scripts.report_pipeline_slo as pipeline_slo
from scripts.report_pipeline_slo import (
    A689,
    VOLUME,
    build_report,
    grade_wide_supervisor_heartbeat,
    refresh_binding_from_ready_shadow,
)


def test_refresh_binding_copies_matching_nonterminal_lane_measurements() -> None:
    wallet = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    artifact, result = refresh_binding_from_ready_shadow(
        {
            "execution_status": "BOUND",
            "binding": {
                "wallet": wallet,
                "source_binding_id": "binding-1",
                "standby_evidence_started_at": "2026-07-31T00:00:00Z",
            },
        },
        {"lanes": [{
            "wallet": wallet,
            "source_binding_id": "binding-1",
            "standby_evidence_started_at": "2026-07-31T00:00:00Z",
            "standby_evidence_elapsed_h": 1.5,
            "resolved_paper_fills": 7,
            "in_lane_fresh_resolved_signals": 7,
            "in_lane_post_fee_pnl_usd": 2.25,
        }]},
        now=datetime(2026, 7, 31, 1, 30, tzinfo=UTC),
    )

    assert result["status"] == "APPLIED"
    assert artifact["binding"]["standby_evidence_elapsed_h"] == 1.5
    assert artifact["binding"]["resolved_paper_fills"] == 7
    assert artifact["binding"]["in_lane_post_fee_pnl_usd"] == 2.25


def test_refresh_binding_leaves_committed_park_byte_identical() -> None:
    source = {
        "execution_status": "PARK_COMMITTED",
        "binding": {
            "wallet": "0x82c857cb4d18e919c1b7d3c6865be4debe50da77",
            "source_binding_id": "binding-1",
            "standby_evidence_started_at": "2026-07-31T00:00:00Z",
            "standby_evidence_elapsed_h": 0.0,
        },
    }
    artifact, result = refresh_binding_from_ready_shadow(
        source,
        {"lanes": []},
    )

    assert result["status"] == "REFUSED_TERMINAL_BINDING"
    assert artifact == source


def test_pipeline_slo_reports_six_budgets_standby_clocks_and_counter_reconciliation() -> None:
    report = build_report(
        ready_shadow={
            "generated_at": "2026-07-21T18:20:00Z",
            "summary": {"gate_crossed": 1, "all_measurement_hot_standby_ready": 0},
            "lanes": [
                {"wallet": A689, "source_binding_status": "WIRED", "standby_evidence_started_at": "2026-07-21T18:20:00Z", "resolved_paper_fills": 0},
                {"wallet": VOLUME, "paper_canary_enrolled_at": "2026-07-21T01:00:00Z", "copyable_buy_events": 25, "paper_pnl_usd": 3.0, "ready_shadow_full_utc_day": False},
            ],
        },
        full_pool_queue={"generated_at": "2026-07-21T18:19:00Z", "summary": {"hot_standby_ready": 1}},
        structural_scalp={"seeded_at": "2026-07-20T02:00:00Z", "summary": {"forward_gate_passed": False, "forward_fills": 219, "forward_pnl_usd": -0.7}},
        now=datetime(2026, 7, 21, 19, 0, tzinfo=UTC),
    )
    assert len(report["pipeline_slo"]["stages"]) == 9
    scorer = next(
        row
        for row in report["pipeline_slo"]["stages"]
        if row["stage"] == "wide_scorer_cycle_period"
    )
    assert scorer["status"] == "INSUFFICIENT_CYCLES"
    assert scorer["breached"] is False
    assert report["standby_ready"]["seat"]["source_binding_status"] == "WIRED"
    assert report["standby_ready"]["volume"]["copyable_buys"] == 25
    assert report["standby_ready"]["volume"]["elapsed_h"] == 18.0
    assert report["standby_ready"]["volume"]["status"] == "ACCRUING_RED"
    assert report["hot_standby_counter_reconciliation"]["queue_supply_ready_and_alive"] == 1
    assert report["hot_standby_counter_reconciliation"]["canonical_op_standby_ready"] == 0


def test_wide_supervisor_heartbeat_uses_installed_plist_and_live_lock_pid(tmp_path) -> None:
    root = tmp_path / "repo"
    data = root / "data" / "research"
    agents = tmp_path / "LaunchAgents"
    data.mkdir(parents=True)
    agents.mkdir()
    (agents / "com.belavarga.polymarket.wide-prospective-supervisor.plist").write_text("plist")
    (data / "wide_prospective_supervisor.lock").write_text("123")
    (data / "wide_supervisor_heartbeat_state.json").write_text("{}")
    (data / "wide_exact_policy_manifest_active.json").write_text(
        '{"manifest_path":"data/research/manifest.json","published_at_s":1785542400}'
    )
    (data / "manifest.json").write_text('{"score_run_id":"wide_new"}')

    heartbeat = grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime(2026, 8, 1, 0, 30, tzinfo=UTC),
        process_alive=lambda pid: pid == 123,
        launchd_last_exit_code=0,
    )

    assert heartbeat["status"] == "PASS"
    assert heartbeat["last_cut_run_id"] == "wide_new"
    assert heartbeat["scheduler_installed"] is True
    assert heartbeat["process_alive"] is True


def test_wide_supervisor_heartbeat_fails_closed_on_post_cut_traceback(tmp_path) -> None:
    root = tmp_path / "repo"
    data = root / "data" / "research"
    agents = tmp_path / "LaunchAgents"
    data.mkdir(parents=True)
    agents.mkdir()
    (agents / "com.belavarga.polymarket.wide-prospective-supervisor.plist").write_text("plist")
    (data / "wide_prospective_supervisor.lock").write_text("123")
    (data / "wide_exact_policy_manifest_active.json").write_text(
        '{"manifest_path":"data/research/manifest.json","published_at_s":1000}'
    )
    (data / "manifest.json").write_text('{"score_run_id":"wide_crashed"}')
    error_log = data / "wide_prospective_supervisor.launchd.err"
    error_log.write_text("Traceback (most recent call last):\nOSError: argv")
    os.utime(error_log, (1001, 1001))

    heartbeat = grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(1010, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=1,
        error_log_path=error_log,
    )

    assert heartbeat["status"] == "PRODUCER_CRASH_RESTART"
    assert heartbeat["process_alive"] is True
    assert heartbeat["post_cut_traceback_count"] == 1


def test_wide_supervisor_heartbeat_retains_pre_cut_traceback_after_pointer_advances(
    tmp_path,
) -> None:
    root = tmp_path / "repo"
    data = root / "data" / "research"
    agents = tmp_path / "LaunchAgents"
    data.mkdir(parents=True)
    agents.mkdir()
    (agents / "com.belavarga.polymarket.wide-prospective-supervisor.plist").write_text("plist")
    (data / "wide_prospective_supervisor.lock").write_text("123")
    (data / "wide_exact_policy_manifest_active.json").write_text(
        '{"manifest_path":"data/research/manifest.json","published_at_s":2000}'
    )
    (data / "manifest.json").write_text('{"score_run_id":"wide_after_restart"}')
    error_log = data / "wide_prospective_supervisor.launchd.err"
    error_log.write_text("Traceback (most recent call last):\nOSError: argv")
    os.utime(error_log, (1000, 1000))

    heartbeat = grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(2010, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=0,
        error_log_path=error_log,
    )

    assert heartbeat["status"] == "PRODUCER_CRASH_RESTART"
    assert heartbeat["new_traceback_count"] == 1
    assert heartbeat["crash_open"]["offending_run_id"] == "wide_after_restart"


def test_wide_supervisor_sticky_exit_clears_only_on_evidenced_progress(tmp_path) -> None:
    root = tmp_path / "repo"
    data = root / "data" / "research"
    agents = tmp_path / "LaunchAgents"
    data.mkdir(parents=True)
    agents.mkdir()
    (agents / "com.belavarga.polymarket.wide-prospective-supervisor.plist").write_text("plist")
    (data / "wide_prospective_supervisor.lock").write_text("123")
    pointer = data / "wide_exact_policy_manifest_active.json"
    manifest = data / "manifest.json"
    validity = data / "wide_alpha_metric_validity_latest.json"
    pointer.write_text('{"manifest_path":"data/research/manifest.json","published_at_s":1000}')
    manifest.write_text('{"score_run_id":"wide_crashed"}')
    validity.write_text('{"summary":{"total_fill_sample":10}}')

    crashed = grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(1010, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=1,
    )
    assert crashed["status"] == "PRODUCER_CRASH_RESTART"

    pointer.write_text('{"manifest_path":"data/research/manifest.json","published_at_s":1020}')
    unchanged = grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(1025, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=1,
    )
    assert unchanged["status"] == "PRODUCER_CRASH_RESTART"
    assert unchanged["crash_clearance"] is None

    pointer.write_text('{"manifest_path":"data/research/manifest.json","published_at_s":1040}')
    manifest.write_text('{"score_run_id":"wide_recovered"}')
    recovered = grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(1045, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=1,
    )
    assert recovered["status"] == "PASS"
    assert recovered["crash_open"] is None
    assert recovered["crash_clearance"]["cleared_by_run_id"] == "wide_recovered"


def test_wide_supervisor_unparseable_exit_status_fails_closed(tmp_path) -> None:
    root = tmp_path / "repo"
    data = root / "data" / "research"
    agents = tmp_path / "LaunchAgents"
    data.mkdir(parents=True)
    agents.mkdir()
    (agents / "com.belavarga.polymarket.wide-prospective-supervisor.plist").write_text("plist")
    (data / "wide_prospective_supervisor.lock").write_text("123")
    (data / "wide_exact_policy_manifest_active.json").write_text(
        '{"manifest_path":"data/research/manifest.json","published_at_s":1000}'
    )
    (data / "manifest.json").write_text('{"score_run_id":"wide_unknown_exit"}')

    heartbeat = grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(1010, tz=UTC),
        process_alive=lambda pid: True,
        launchd_status_text="last exit code = mystery",
    )

    assert heartbeat["status"] == "PRODUCER_EXIT_STATUS_UNREADABLE"
    assert heartbeat["launchd_exit_status"] == "UNREADABLE"


def test_wide_supervisor_unchanged_capture_across_three_score_intervals_fails_progress(tmp_path) -> None:
    root = tmp_path / "repo"
    data = root / "data" / "research"
    agents = tmp_path / "LaunchAgents"
    data.mkdir(parents=True)
    agents.mkdir()
    (agents / "com.belavarga.polymarket.wide-prospective-supervisor.plist").write_text("plist")
    (data / "wide_prospective_supervisor.lock").write_text("123")
    (data / "wide_supervisor_heartbeat_state.json").write_text("{}")
    (data / "wide_exact_policy_manifest_active.json").write_text(
        '{"manifest_path":"data/research/manifest.json","published_at_s":1000}'
    )
    (data / "manifest.json").write_text('{"score_run_id":"wide_static"}')
    capture = data / "polygon_orderfilled_ws_capture_alpha_decay_wide_static.jsonl"
    capture.write_text("static\n")

    first = grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(1010, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=0,
    )
    assert first["status"] == "PASS"
    second = grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(1101, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=0,
    )
    assert second["status"] == "PRODUCER_NO_FORWARD_PROGRESS"


def test_wide_supervisor_heartbeat_names_stale_cut_even_when_lock_file_exists(tmp_path) -> None:
    root = tmp_path / "repo"
    data = root / "data" / "research"
    data.mkdir(parents=True)
    (data / "wide_prospective_supervisor.lock").write_text("999")
    (data / "wide_exact_policy_manifest_active.json").write_text(
        '{"manifest_path":"data/research/manifest.json","published_at_s":1785542400}'
    )
    (data / "manifest.json").write_text('{"score_run_id":"wide_old"}')

    heartbeat = grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=tmp_path / "missing-agents",
        now=datetime(2026, 8, 1, 4, 0, 1, tzinfo=UTC),
        process_alive=lambda pid: False,
        launchd_last_exit_code=0,
    )

    assert heartbeat["status"] == "BLOCKED_NO_CUT_PRODUCER"
    assert heartbeat["process_alive"] is False


def test_wide_supervisor_grades_once_then_two_consumers_read_identical_verdict(
    tmp_path,
) -> None:
    root = tmp_path / "repo"
    data = root / "data" / "research"
    agents = tmp_path / "LaunchAgents"
    data.mkdir(parents=True)
    agents.mkdir()
    (agents / "com.belavarga.polymarket.wide-prospective-supervisor.plist").write_text("plist")
    (data / "wide_prospective_supervisor.lock").write_text("123")
    (data / "wide_exact_policy_manifest_active.json").write_text(
        '{"manifest_path":"data/research/manifest.json","published_at_s":2000}'
    )
    (data / "manifest.json").write_text('{"score_run_id":"wide_writer"}')
    error_log = data / "wide_prospective_supervisor.launchd.err"
    error_log.write_text("Traceback (most recent call last):\nOSError: argv")

    graded = pipeline_slo.grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(2010, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=0,
        error_log_path=error_log,
    )
    state_path = data / "wide_supervisor_heartbeat_state.json"
    state_after_grade = state_path.read_bytes()
    first_reader = pipeline_slo.read_wide_supervisor_heartbeat(root=root)
    second_reader = pipeline_slo.read_wide_supervisor_heartbeat(root=root)

    assert graded["status"] == first_reader["status"] == second_reader["status"]
    assert first_reader["status"] == "PRODUCER_CRASH_RESTART"
    assert first_reader["tracebacks_graded"] == 1
    assert state_path.read_bytes() == state_after_grade


def test_wide_supervisor_rotation_is_sticky_for_second_consumer(tmp_path) -> None:
    root = tmp_path / "repo"
    data = root / "data" / "research"
    agents = tmp_path / "LaunchAgents"
    data.mkdir(parents=True)
    agents.mkdir()
    (agents / "com.belavarga.polymarket.wide-prospective-supervisor.plist").write_text("plist")
    (data / "wide_prospective_supervisor.lock").write_text("123")
    pointer = data / "wide_exact_policy_manifest_active.json"
    pointer.write_text('{"manifest_path":"data/research/manifest.json","published_at_s":1000}')
    (data / "manifest.json").write_text('{"score_run_id":"wide_before_rotation"}')
    error_log = data / "wide_prospective_supervisor.launchd.err"
    error_log.write_text("old log")
    pipeline_slo.grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(1010, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=0,
        error_log_path=error_log,
    )
    error_log.unlink()
    error_log.write_text("rotated log")
    rotated = pipeline_slo.grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(1020, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=0,
        error_log_path=error_log,
    )
    reader = pipeline_slo.read_wide_supervisor_heartbeat(root=root)
    assert rotated["status"] == "PRODUCER_LOG_ROTATED"
    assert reader["status"] == "PRODUCER_LOG_ROTATED"
    assert reader["rotation_open"]["offending_run_id"] == "wide_before_rotation"

    pointer.write_text('{"manifest_path":"data/research/manifest.json","published_at_s":1025}')
    still_open = pipeline_slo.grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(1030, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=0,
        error_log_path=error_log,
    )
    assert still_open["status"] == "PRODUCER_LOG_ROTATED"
    assert still_open["rotation_open"] is not None


def test_wide_supervisor_missing_ledger_is_explicitly_non_pass(tmp_path) -> None:
    root = tmp_path / "repo"
    data = root / "data" / "research"
    agents = tmp_path / "LaunchAgents"
    data.mkdir(parents=True)
    agents.mkdir()
    (agents / "com.belavarga.polymarket.wide-prospective-supervisor.plist").write_text("plist")
    (data / "wide_prospective_supervisor.lock").write_text("123")
    (data / "wide_exact_policy_manifest_active.json").write_text(
        '{"manifest_path":"data/research/manifest.json","published_at_s":1000}'
    )
    (data / "manifest.json").write_text('{"score_run_id":"wide_no_ledger"}')
    error_log = data / "wide_prospective_supervisor.launchd.err"
    error_log.write_text("Traceback (most recent call last):\nRuntimeError: crash")

    heartbeat = pipeline_slo.grade_wide_supervisor_heartbeat(
        root=root,
        launch_agents_dir=agents,
        now=datetime.fromtimestamp(1010, tz=UTC),
        process_alive=lambda pid: True,
        launchd_last_exit_code=0,
        error_log_path=error_log,
    )
    assert heartbeat["status"] != "PASS"
    assert heartbeat["heartbeat_state_missing"] is True


def test_pipeline_slo_closes_due_structural_scalp_park_clock() -> None:
    report = build_report(
        ready_shadow={"summary": {}, "lanes": []},
        full_pool_queue={"summary": {}},
        structural_scalp={
            "seeded_at": "2026-07-20T02:00:00Z",
            "summary": {"forward_gate_passed": False, "forward_fills": 264, "forward_pnl_usd": -3.523437},
        },
        structural_scalp_promotion={
            "decision": "PARK_METHOD_LANE_PAPER_ONLY",
            "decision_clock": {"due": True, "decision_at": "2026-07-23T02:00:00Z"},
        },
        now=datetime(2026, 7, 23, 2, 1, tzinfo=UTC),
    )

    method = report["standby_ready"]["method"]
    assert method["status"] == "PARKED"
    assert method["terminal_decision"] == "PARK_METHOD_LANE_PAPER_ONLY"
    assert method["decision_due"] is True


def test_pipeline_slo_terminal_volume_park_is_monotone_over_elapsed_readiness() -> None:
    report = build_report(
        ready_shadow={
            "summary": {},
            "lanes": [
                {
                    "wallet": VOLUME,
                    "paper_canary_enrolled_at": "2026-07-20T00:00:00Z",
                    "copyable_buy_events": 276,
                    "paper_pnl_usd": 177.0,
                }
            ],
        },
        full_pool_queue={"summary": {}},
        structural_scalp={},
        volume_standby_promotion={
            "decision": "FRESH_PRECONDITION_INPUTS_REQUIRED",
            "terminal_decision": "PARK_VOLUME_STANDBY_PAPER_ONLY",
            "prederived_decision_branches": {
                "current_branch": "DEFER_VOLUME_DECISION_FRESH_INPUT_REQUIRED"
            },
        },
        now=datetime(2026, 7, 24, 0, 0, tzinfo=UTC),
    )

    volume = report["standby_ready"]["volume"]
    assert volume["status"] == "PARKED"
    assert volume["terminal_decision"] == "PARK_VOLUME_STANDBY_PAPER_ONLY"
    assert volume["terminal_basis"] == "terminal PARK decision outranks elapsed/sample readiness"


def test_daily_scorecard_passes_volume_terminal_decision_to_pipeline_slo() -> None:
    source = (Path(__file__).resolve().parents[1] / "scripts" / "daily_scorecard.py").read_text()

    call_start = source.index('scorecard["pipeline_slo_and_standby_readiness"] = build_pipeline_slo_report(')
    call_end = source.index("\n    )", call_start)
    call = source[call_start:call_end]

    assert "volume_standby_promotion=load_json(" in call
    assert "13e0_exact_policy_promotion_packet_latest.json" in call


def test_pipeline_slo_a689_seat_fails_closed_without_clock_or_source_binding() -> None:
    report = build_report(
        ready_shadow={
            "summary": {},
            "lanes": [
                {
                    "wallet": A689,
                    "hot_standby_ready": True,
                    "resolved_paper_fills": 28,
                    "standby_evidence_started_at": None,
                    "source_binding_status": None,
                }
            ],
        },
        full_pool_queue={"summary": {}},
        structural_scalp={},
        now=datetime(2026, 7, 24, 0, 0, tzinfo=UTC),
    )

    seat = report["standby_ready"]["seat"]
    assert seat["status"] == "CLOCK_OR_SOURCE_BINDING_MISSING"
    assert seat["clock_or_source_binding_missing"] is True
    assert "non-backdated" in seat["next_action"]


def test_pipeline_slo_seat_tracks_successor_when_cut_terminalized() -> None:
    successor = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    report = build_report(
        ready_shadow={
            "summary": {},
            "a689_82c8_cut": {"status": "EXECUTED_ATOMIC_STATE_REBIND", "executed_at": "2026-07-23T18:32:33Z"},
            "lanes": [
                {
                    "wallet": successor,
                    "source_binding": "FABLE_20260723_82C8_READY_SHADOW",
                    "source_binding_status": "WIRED",
                    "standby_evidence_started_at": "2026-07-23T18:32:33Z",
                    "resolved_paper_fills": 5,
                    "hot_standby_ready": False,
                }
            ],
        },
        full_pool_queue={"summary": {}},
        structural_scalp={},
        now=datetime(2026, 7, 24, 0, 0, tzinfo=UTC),
    )

    seat = report["standby_ready"]["seat"]
    assert seat["wallet"] == successor
    assert seat["status"] == "ACCRUING_RED"
    assert seat["clock_or_source_binding_missing"] is False
    assert seat["resolved"] == 5
    assert "continue evidence" in seat["next_action"]


def test_pipeline_slo_retires_successor_standby_clock_when_t2_is_live() -> None:
    successor = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    report = build_report(
        ready_shadow={
            "summary": {},
            "a689_82c8_cut": {
                "status": "EXECUTED_ATOMIC_STATE_REBIND",
                "executed_at": "2026-07-23T18:32:33Z",
            },
            "lanes": [{"wallet": successor}],
        },
        full_pool_queue={"summary": {}},
        structural_scalp={},
        t2_cell_admission={
            "status": "PASS",
            "member": {
                "source_wallet": successor,
                "cell_scoped_admission": {"status": "ACTIVE"},
            },
        },
        now=datetime(2026, 7, 28, 19, 0, tzinfo=UTC),
    )

    seat = report["standby_ready"]["seat"]
    assert seat["status"] == "SUPERSEDED_BY_T2_LIVE"
    assert seat["clock_or_source_binding_missing"] is False
    assert seat["next_action"].startswith("none;")


def test_pipeline_slo_new_wide_receipt_clock_supersedes_old_t2_hygiene_mark() -> None:
    successor = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    report = build_report(
        ready_shadow={
            "summary": {},
            "a689_82c8_cut": {
                "status": "EXECUTED_ATOMIC_STATE_REBIND",
                "executed_at": "2026-07-23T18:32:33Z",
            },
            "lanes": [
                {
                    "wallet": successor,
                    "source_binding": "FABLE_20260729_82C8_WIDE_RECEIPT_STANDBY",
                    "source_binding_status": "WIRED",
                    "standby_evidence_started_at": "2026-07-29T06:44:50Z",
                    "resolved_paper_fills": 0,
                    "hot_standby_ready": False,
                }
            ],
        },
        full_pool_queue={"summary": {}},
        structural_scalp={},
        t2_cell_admission={
            "status": "PASS",
            "member": {
                "source_wallet": successor,
                "cell_scoped_admission": {"status": "ACTIVE"},
            },
        },
        now=datetime(2026, 7, 29, 7, 0, tzinfo=UTC),
    )

    seat = report["standby_ready"]["seat"]
    assert seat["status"] == "ACCRUING_RED"
    assert seat["clock_or_source_binding_missing"] is False
    assert seat["next_action"] == "continue evidence accrual"


def test_pipeline_slo_binds_seat_readiness_to_authoritative_measured_elapsed() -> None:
    successor = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    report = build_report(
        ready_shadow={
            "summary": {},
            "a689_82c8_cut": {
                "status": "EXECUTED_ATOMIC_STATE_REBIND",
                "executed_at": "2026-07-23T18:32:33Z",
            },
            "lanes": [
                {
                    "wallet": successor,
                    "source_binding": "FABLE_20260729_82C8_WIDE_RECEIPT_STANDBY",
                    "source_binding_status": "WIRED",
                    "standby_evidence_started_at": "2026-07-29T06:44:50Z",
                    "standby_evidence_elapsed_h": 24.0,
                    "standby_evidence_minimum_h": 48.0,
                    "resolved_paper_fills": 30,
                    "hot_standby_ready": True,
                }
            ],
        },
        full_pool_queue={"summary": {}},
        structural_scalp={},
        wide_standby_binding={
            "execution_status": "EXECUTED",
            "binding": {
                "wallet": successor,
                "source_binding_status": "WIRED",
                "standby_evidence_started_at": "2026-07-29T06:44:50Z",
                "standby_evidence_elapsed_h": 0.0,
                "standby_evidence_minimum_h": 48.0,
                "terminal_outcome_on_deadline": {
                    "status": "PARK_SEAT_UNFED_CLOCK",
                    "terminal": True,
                },
            },
        },
        now=datetime(2026, 7, 30, 6, 44, 50, tzinfo=UTC),
    )

    seat = report["standby_ready"]["seat"]
    assert seat["status"] == "UNFED_CLOCK_CANNOT_MATURE"
    assert seat["elapsed_h"] == 0.0
    assert seat["wall_clock_elapsed_h"] == 24.0
    assert seat["elapsed_basis"] == "authoritative_binding_measured_elapsed"
    assert seat["projected_resolved_at_48h"] == 0.0
    assert seat["admission_forecast"] is False


def test_pipeline_slo_reports_committed_terminal_park() -> None:
    successor = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    report = build_report(
        ready_shadow={
            "a689_82c8_cut": {
                "status": "EXECUTED_ATOMIC_STATE_REBIND",
                "executed_at": "2026-07-23T18:32:33Z",
            },
            "lanes": [
                {
                    "wallet": successor,
                    "source_binding_status": "WIRED",
                    "standby_evidence_started_at": "2026-07-29T06:44:50Z",
                }
            ],
        },
        full_pool_queue={"summary": {}},
        structural_scalp={},
        wide_standby_binding={
            "execution_status": "PARK_COMMITTED",
            "binding": {
                "wallet": successor,
                "source_binding_status": "WIRED",
                "standby_evidence_started_at": "2026-07-29T06:44:50Z",
                "standby_evidence_elapsed_h": 0.0,
                "standby_evidence_minimum_h": 48.0,
                "terminal_executed_at": "2026-07-31T06:45:02.613320Z",
                "terminal_outcome_on_deadline": {
                    "status": "PARK_SEAT_UNFED_CLOCK",
                    "terminal": True,
                    "stop_writer": True,
                },
            },
        },
        wide_exact_state={
            "attempt_terminals": [
                {
                    "attempt_id": "inside-refused",
                    "wallet": successor,
                    "recorded_at": "2026-07-30T06:44:50Z",
                    "f1_f4_terminal": {"terminal": "REFUSED_ALPHA_PROFILE_FILTER"},
                },
                {
                    "attempt_id": "outside-after-terminal",
                    "wallet": successor,
                    "recorded_at": "2026-07-31T07:00:00Z",
                    "f1_f4_terminal": {"terminal": "COPYABLE_EXACT_POLICY_PAPER_FILL"},
                },
            ]
        },
        now=datetime(2026, 7, 31, 8, 45, tzinfo=UTC),
    )

    seat = report["standby_ready"]["seat"]
    assert seat["status"] == "PARK_SEAT_UNFED_CLOCK_COMMITTED"
    assert seat["terminal_executed_at"] == "2026-07-31T06:45:02.613320Z"
    assert seat["resolutions_attempted"] == 1
    assert seat["resolution_attempt_taxonomy"] == {
        "REFUSED_ALPHA_PROFILE_FILTER": 1
    }
    assert seat["attempt_log_retention"]["status"] == "WINDOW_RETAINED"
    assert seat["next_action"] == (
        "none; terminal park committed at 2026-07-31T06:45:02.613320Z"
    )


def test_pipeline_slo_does_not_turn_unretained_window_into_zero_attempts() -> None:
    result = pipeline_slo._binding_resolution_attempts(
        {
            "wallet": "0x82c8",
            "standby_evidence_started_at": "2026-07-29T06:44:50Z",
            "terminal_executed_at": "2026-07-31T06:45:02Z",
        },
        {
            "attempt_terminals": [
                {
                    "attempt_id": "current-cohort",
                    "wallet": "0x82c8",
                    "recorded_at": "2026-08-01T03:50:06Z",
                    "f1_f4_terminal": {"terminal": "REFUSED_ALPHA_PROFILE_FILTER"},
                }
            ]
        },
    )

    assert result["resolutions_attempted"] is None
    assert result["attempt_log_retention"] == {
        "status": "NO_RETAINED_ATTEMPT_LOG_FOR_WINDOW",
        "retained_from": "2026-08-01T03:50:06Z",
        "retained_to": "2026-08-01T03:50:06Z",
        "cohort_scoped": True,
    }


def test_pipeline_slo_counts_nonterminated_passes_over_budget_as_breaches() -> None:
    successor = "0x82c857cb4d18e919c1b7d3c6865be4debe50da77"
    report = build_report(
        ready_shadow={
            "generated_at": "2026-07-30T10:00:00Z",
            "summary": {},
            "a689_82c8_cut": {
                "status": "EXECUTED_ATOMIC_STATE_REBIND",
                "executed_at": "2026-07-23T18:32:33Z",
            },
            "lanes": [
                {
                    "wallet": successor,
                    "source_binding_status": "WIRED",
                    "standby_evidence_started_at": "2026-07-29T06:44:50Z",
                },
                {
                    "wallet": VOLUME,
                    "paper_canary_enrolled_at": "2026-07-21T01:10:12Z",
                    "ready_shadow_full_utc_day": True,
                },
            ],
        },
        full_pool_queue={"summary": {}},
        structural_scalp={},
        now=datetime(2026, 7, 30, 10, 0, tzinfo=UTC),
    )

    stages = {row["stage"]: row for row in report["pipeline_slo"]["stages"]}
    assert stages["shadow_to_ready"]["time_in_stage_h"] == 224.83
    assert stages["shadow_to_ready"]["status"] == "PASS_OVER_BUDGET"
    assert stages["shadow_to_ready"]["breached"] is True
    assert stages["standby_wiring_repair"]["time_in_stage_h"] > 48.37
    assert stages["standby_wiring_repair"]["status"] == "PASS_OVER_BUDGET"
    assert stages["standby_wiring_repair"]["breached"] is True
    assert stages["wide_scorer_cycle_period"]["status"] == "INSUFFICIENT_CYCLES"
    assert stages["wide_scorer_cycle_period"]["breached"] is False
    assert stages["wide_supervisor_heartbeat_publish_period"]["status"] == (
        "INSUFFICIENT_PUBLICATIONS"
    )
    assert stages["wide_supervisor_heartbeat_publish_period"]["breached"] is False
    assert report["pipeline_slo"]["breach_count"] == 2
