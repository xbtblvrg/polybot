import argparse
import json

from scripts.run_alpha_decay_simultaneous_capture import (
    CommandResult,
    _alpha_command,
    _alpha_summary,
    _annotate_command,
    _capture_paths,
    _classify_state,
    _clob_command,
    _polygon_command,
)


def _args(**overrides: object) -> argparse.Namespace:
    values = {
        "run_id": "20260704T040000Z",
        "duration_s": 12.0,
        "python": "/usr/bin/python3",
        "polygon_jsonl": "",
        "clob_jsonl": "",
        "alpha_report": "data/research/alpha_decay_report.json",
        "state": "",
        "event_log": "data/research/alpha_decay_simultaneous_capture_events.jsonl",
        "detection_report": "data/research/detection_latency_report.json",
        "asset_ids_output": "data/research/alpha_decay_target_asset_ids.json",
        "log_dir": "data/research/alpha_decay_capture_logs",
        "polygon_rpc_url": "https://rpc.example",
        "polygon_wss_url": "wss://rpc.example",
        "polygon_wss_fallback_url": ["wss://fallback.example"],
        "polygon_lookback_blocks": 7,
        "polygon_timeout_s": 1.5,
        "polygon_ws_retry_s": 0.25,
        "registry": ["configs/wallet_copy/wallets.json"],
        "active_registry": "data/research/wallet_copy_active_hotlane_registry.json",
        "clob_base_url": "https://clob.polymarket.com",
        "clob_timeout_s": 0.75,
        "clob_retries": 1,
        "asset_ids_file": "data/research/alpha_decay_target_asset_ids.json",
        "snapshot_interval_s": 0.5,
        "asset_refresh_s": 1.0,
        "max_assets": 5,
        "polygon_scan_limit": 123,
        "polygon_max_age_s": 30.0,
        "polygon_disable_default_registry": False,
        "polygon_registry_only": True,
        "http_backfill_fallback": True,
        "disable_source_base_overrides": True,
        "alpha_sample_limit": 250,
        "history_state": "data/research/wallet_copy_live_guard_hot_history_state.json",
        "profile_min_fills": 3,
        "profile_horizon_s": 2.0,
        "startup_delay_s": 0.0,
        "process_timeout_buffer_s": 5.0,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def test_simultaneous_capture_commands_pair_outputs() -> None:
    args = _args()
    paths = _capture_paths(args)

    polygon = _polygon_command(args, paths)
    clob = _clob_command(args, paths)
    alpha = _alpha_command(args, paths)

    assert paths.run_id == "20260704T040000Z"
    assert paths.polygon_jsonl.endswith("polygon_orderfilled_ws_capture_alpha_decay_20260704T040000Z.jsonl")
    assert paths.clob_jsonl.endswith("clob_book_snapshots_alpha_decay_20260704T040000Z.jsonl")
    assert polygon[:2] == ["/usr/bin/python3", "scripts/probe_polygon_orderfilled_ws.py"]
    assert polygon[polygon.index("--output") + 1] == paths.polygon_jsonl
    assert polygon[polygon.index("--report") + 1] == paths.detection_report
    assert polygon[polygon.index("--polygon-rpc-url") + 1] == "https://rpc.example"
    assert polygon[polygon.index("--polygon-wss-url") + 1] == "wss://rpc.example"
    assert polygon[polygon.index("--polygon-wss-fallback-url") + 1] == "wss://fallback.example"
    assert clob[:2] == ["/usr/bin/python3", "scripts/capture_clob_book_snapshots.py"]
    assert clob[clob.index("--asset-ids-file") + 1] == "data/research/alpha_decay_target_asset_ids.json"
    assert clob[clob.index("--polygon-jsonl") + 1] == paths.polygon_jsonl
    assert clob[clob.index("--output") + 1] == paths.clob_jsonl
    assert clob[clob.index("--max-assets") + 1] == "5"
    assert clob.count("--polygon-source") == 2
    assert "polygon_http_getLogs_tail" in clob
    assert "--disable-source-base-overrides" in clob
    assert alpha[:2] == ["/usr/bin/python3", "scripts/report_alpha_decay.py"]
    assert alpha[alpha.index("--polygon-jsonl") + 1] == paths.polygon_jsonl
    assert alpha[alpha.index("--clob-jsonl") + 1] == paths.clob_jsonl
    assert alpha[alpha.index("--profile-min-fills") + 1] == "3"
    assert alpha[alpha.index("--history-state") + 1] == "data/research/wallet_copy_live_guard_hot_history_state.json"
    assert alpha.count("--fill-source") == 2
    assert "polygon_http_getLogs_tail" in alpha


def test_simultaneous_capture_defaults_to_run_scoped_alpha_report() -> None:
    paths = _capture_paths(_args(alpha_report=""))

    assert paths.alpha_report.endswith("alpha_decay_report_20260704T040000Z.json")


def test_simultaneous_capture_can_disable_http_backfill_fallback() -> None:
    args = _args(http_backfill_fallback=False)
    paths = _capture_paths(args)

    clob = _clob_command(args, paths)
    alpha = _alpha_command(args, paths)

    assert clob.count("--polygon-source") == 1
    assert clob[clob.index("--polygon-source") + 1] == "polygon_ws"
    assert alpha.count("--fill-source") == 1
    assert alpha[alpha.index("--fill-source") + 1] == "polygon_ws"


def test_simultaneous_capture_can_disable_polygon_registry_only() -> None:
    args = _args(polygon_registry_only=False)
    paths = _capture_paths(args)

    clob = _clob_command(args, paths)

    assert "--no-polygon-registry-only" in clob


def test_simultaneous_capture_can_disable_default_polygon_registry() -> None:
    args = _args(polygon_disable_default_registry=True)
    paths = _capture_paths(args)

    polygon = _polygon_command(args, paths)

    assert "--disable-default-registry" in polygon


def test_simultaneous_capture_classifies_non_green_measurement_evidence() -> None:
    clob_no_books = _annotate_command(CommandResult("clob_books", ["cmd"], 2, 1.0))
    polygon_timeout = _annotate_command(CommandResult("polygon_fills", ["cmd"], 3, 1.0))
    alpha_fail = _annotate_command(CommandResult("alpha_decay_report", ["cmd"], 2, 1.0))

    assert clob_no_books["ok"] is True
    assert clob_no_books["accepted_non_green_reason"] == "clob_snapshot_no_available_books_evidence"
    assert polygon_timeout["ok"] is True
    assert polygon_timeout["accepted_non_green_reason"] == "polygon_ws_no_rows_or_timeout_evidence"
    assert alpha_fail["ok"] is False
    assert _classify_state([clob_no_books, polygon_timeout], {"execution_profile_status": "PASS"}) == "PASS"
    assert _classify_state([clob_no_books], {"alpha_status": "PASS", "execution_profile_status": "WATCH"}) == "ANALYZE"
    assert _classify_state([clob_no_books], {"alpha_status": "INSUFFICIENT_BOOK_COVERAGE"}) == "WATCH"
    assert _classify_state([alpha_fail], {"execution_profile_status": "PASS"}) == "CORRECTION"


def test_polygon_sigterm_is_accepted_only_after_full_capture_timebox() -> None:
    completed = _annotate_command(
        CommandResult("polygon_fills", ["cmd"], -15, 1800.1),
        expected_duration_s=1800.0,
    )
    early = _annotate_command(
        CommandResult("polygon_fills", ["cmd"], -15, 1799.9),
        expected_duration_s=1800.0,
    )

    assert completed["ok"] is True
    assert completed["accepted_non_green_reason"] == "polygon_timebox_complete_sigterm"
    assert early["ok"] is False


def test_alpha_summary_extracts_profile_gate_fields(tmp_path) -> None:
    report = tmp_path / "alpha_decay_report.json"
    report.write_text(
        json.dumps(
            {
                "updated_at": "2026-07-04T04:00:00+00:00",
                "alpha_decay": {
                    "status": "INSUFFICIENT_BOOK_COVERAGE",
                    "fills_total": 12,
                    "fill_source_counts": {"polygon_http_getLogs": 2, "polygon_ws": 10},
                    "fills_with_any_book_coverage": 2,
                    "overlapping_fill_book_assets": 1,
                    "fills_on_book_assets": 2,
                    "blockers": ["alpha_decay_profile_coverage_missing"],
                    "next_action": "capture more paired evidence",
                },
                "execution_profiles": {
                    "status": "WATCH",
                    "eligible_profile_count": 0,
                    "profile_count": 1,
                    "blockers": ["no_execution_profile_positive_at_latency"],
                },
            }
        ),
        encoding="utf-8",
    )

    summary = _alpha_summary(str(report))

    assert summary["alpha_status"] == "INSUFFICIENT_BOOK_COVERAGE"
    assert summary["fills_total"] == 12
    assert summary["fill_source_counts"] == {"polygon_http_getLogs": 2, "polygon_ws": 10}
    assert summary["overlapping_fill_book_assets"] == 1
    assert summary["execution_profile_status"] == "WATCH"
    assert summary["eligible_profile_count"] == 0
    assert summary["next_action"] == "capture more paired evidence"
