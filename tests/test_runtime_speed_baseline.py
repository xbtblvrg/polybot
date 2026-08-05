import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from scripts.report_runtime_speed_baseline import build_report, main


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload) + "\n")


def _seed_speed_inputs(
    root: Path,
    *,
    guard_total_s: float = 10.0,
    guard_event_totals: tuple[float, ...] = (8.0, 10.0, 12.0),
) -> None:
    data = root / "data" / "research"
    _write_json(
        data / "wallet_copy_live_guard_state.json",
        {
            "pid": 123,
            "status": "LIVE_GUARD_RUNNING",
            "guard_loop_profile": {
                "total_s_before_state_write": guard_total_s,
                "target_median_iteration_lt_s": 15.0,
            },
        },
    )
    (data / "wallet_copy_live_guard_events.jsonl").write_text(
        "\n".join(
            json.dumps({"guard_loop_profile": {"total_s_before_state_write": value}})
            for value in guard_event_totals
        )
        + "\n"
    )
    _write_json(
        data / "order_flow_deadman_state.json",
        {
            "member_signal_age": {
                "0xaaa": {"signal_age_p50_s": 1.0, "signal_age_p90_s": 2.0},
                "0xbbb": {"signal_age_p50_s": 3.0, "signal_age_p90_s": 4.0},
            }
        },
    )
    _write_json(
        data / "brainless_ops_latest.json",
        {"status": "OK", "started_at": "2026-07-10T00:00:00Z", "finished_at": "2026-07-10T00:00:30Z"},
    )
    _write_json(
        data / "state_digest.json",
        {"generated_at": "2026-07-10T00:00:00Z", "line_count": 100, "generation_duration_s": 4.0},
    )
    _write_json(
        data / "wallet_copy_live_execution_state.json",
        {
            "orders": [
                {
                    "submitted_at": "2026-07-10T00:00:03Z",
                    "status": "FILLED",
                    "source_intent": {"observed_ts": 1783641601.0, "source_wallet": "0xaaa"},
                },
                {
                    "submitted_at": "2026-07-10T00:00:06Z",
                    "status": "FILLED",
                    "source_intent": {"observed_ts": 1783641602.0, "source_wallet": "0xaaa"},
                },
            ]
        },
    )
    handoff = root / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True, exist_ok=True)
    handoff.write_text(
        "\n".join(
            [
                "## 2026-07-10T00:00Z codex STATUS [LIVE/SELF-DEV]",
                "- one",
                "## 2026-07-10T00:10Z codex STATUS [LIVE/SELF-DEV]",
                "- two",
            ]
        )
    )


def _write_handoff_statuses(root: Path, timestamps: list[str]) -> None:
    handoff = root / "docs" / "agents" / "HANDOFF.md"
    handoff.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = []
    for index, timestamp in enumerate(timestamps):
        lines.extend([f"## {timestamp} codex STATUS [SELF-DEV]", f"- status {index}"])
    handoff.write_text("\n".join(lines) + "\n")


def _write_serving_markers(root: Path, timestamps: list[str]) -> None:
    path = root / "data" / "research" / "codex_serving_heartbeat.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(
            json.dumps(
                {
                    "kind": "codex_serving_heartbeat",
                    "generated_at": timestamp,
                    "task": "test",
                    "live_orders_allowed": False,
                    "paper_only": True,
                }
            )
            for timestamp in timestamps
        )
        + "\n"
    )


def _write_brainless_log(root: Path, rows: list[dict]) -> None:
    path = root / "data" / "research" / "brainless_ops.launchd.out"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(json.dumps({"kind": "brainless_ops_state", **row}) for row in rows) + "\n")


def _set_mtime(path: Path, timestamp: datetime) -> None:
    epoch = timestamp.timestamp()
    os.utime(path, (epoch, epoch))


def _write_ask_fable_log(root: Path, stamp: str, *, rc: int, wall_s: float) -> None:
    path = root / "data" / "research" / "ask_fable_provider_logs" / f"{stamp}_claude__123_rc{rc}.log"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("ok\n")
    started = datetime.strptime(stamp, "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)
    _set_mtime(path, started + timedelta(seconds=wall_s))


def test_runtime_speed_baseline_pins_once_and_preserves_original(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path, guard_total_s=10.0)

    first = build_report(tmp_path)
    assert first["pin_written"] is True
    assert first["status"] == "PASS"
    assert first["metrics"]["guard_cycle_total_s"]["value"] == 10.0

    _seed_speed_inputs(tmp_path, guard_total_s=11.0)
    second = build_report(tmp_path)
    pinned = json.loads((tmp_path / "data/research/runtime_speed_baseline_pinned.json").read_text())

    assert second["pin_written"] is False
    assert pinned["metrics"]["guard_cycle_total_s"]["value"] == 10.0
    assert second["comparison"]["status"] == "PASS"


def test_runtime_speed_baseline_flags_twenty_percent_regression(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path, guard_total_s=10.0)
    build_report(tmp_path)

    _seed_speed_inputs(tmp_path, guard_total_s=13.0)
    report = build_report(tmp_path)
    regressions = {row["metric"]: row for row in report["comparison"]["regressions"]}

    assert report["status"] == "REGRESSION"
    assert regressions["guard_cycle_total_s"]["baseline"] == 10.0
    assert regressions["guard_cycle_total_s"]["current"] == 13.0
    assert regressions["guard_cycle_total_s"]["ratio"] == 1.3


def test_runtime_speed_baseline_ignores_subsecond_ratio_regressions(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:00:00Z",
            "finished_at": "2026-07-10T00:00:01Z",
            "step_durations": {"tiny_step": 0.1},
        },
    )
    build_report(tmp_path)
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:10:00Z",
            "finished_at": "2026-07-10T00:10:01Z",
            "step_durations": {"tiny_step": 0.15},
        },
    )

    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "PASS"
    assert rows["brainless_step:tiny_step"]["status"] == "PASS_ABS_DELTA_LT_1S"
    assert rows["brainless_step:tiny_step"]["absolute_delta_s"] == 0.05
    assert rows["brainless_run_duration_s"]["status"] == "PASS_ABS_DELTA_LT_1S"


def test_runtime_speed_baseline_brainless_aggregate_compares_shared_steps(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:00:00Z",
            "finished_at": "2026-07-10T00:01:40Z",
            "step_durations": {"existing": 100.0},
        },
    )
    build_report(tmp_path)

    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:10:00Z",
            "finished_at": "2026-07-10T00:13:40Z",
            "step_durations": {"existing": 105.0, "new_step": 115.0},
        },
    )
    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "PASS"
    assert rows["brainless_run_duration_s"]["comparison_rule"] == "composition_aware_shared_step_sum"
    assert rows["brainless_run_duration_s"]["current"] == 105.0
    assert rows["brainless_run_duration_s"]["baseline"] == 100.0
    assert rows["brainless_step:new_step"]["status"] == "NEW_STEP"


def test_runtime_speed_baseline_brainless_shared_step_regression_counts(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:00:00Z",
            "finished_at": "2026-07-10T00:01:40Z",
            "step_durations": {"existing": 100.0},
        },
    )
    build_report(tmp_path)

    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:10:00Z",
            "finished_at": "2026-07-10T00:12:10Z",
            "step_durations": {"existing": 130.0},
        },
    )
    report = build_report(tmp_path)
    regressions = {row["metric"]: row for row in report["comparison"]["regressions"]}
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "REGRESSION"
    assert rows["brainless_run_duration_s"]["comparison_rule"] == "composition_aware_shared_step_sum"
    assert rows["brainless_run_duration_s"]["current"] == 130.0
    assert rows["brainless_run_duration_s"]["baseline"] == 100.0
    assert regressions["brainless_step:existing"]["ratio"] == 1.3


def test_runtime_speed_baseline_brainless_steps_use_rolling_median_after_five_samples(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:00:00Z",
            "finished_at": "2026-07-10T00:00:01Z",
            "step_durations": {"existing": 1.0},
        },
    )
    build_report(tmp_path)
    _write_brainless_log(
        tmp_path,
        [
            {
                "status": "OK",
                "started_at": f"2026-07-10T00:0{index}:00Z",
                "finished_at": f"2026-07-10T00:0{index}:10Z",
                "step_durations": {"existing": value},
            }
            for index, value in enumerate([9.0, 10.0, 10.0, 10.0, 11.0], start=1)
        ],
    )
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:06:00Z",
            "finished_at": "2026-07-10T00:06:11Z",
            "step_durations": {"existing": 11.0},
        },
    )

    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "PASS"
    assert rows["brainless_run_duration_s"]["comparison_rule"] == "composition_aware_rolling_step_median_sum"
    assert rows["brainless_run_duration_s"]["baseline"] == 10.0
    assert rows["brainless_run_duration_s"]["pinned_baseline_shared_step_sum_s"] == 1.0
    assert rows["brainless_step:existing"]["comparison_rule"] == "per_step_rolling_median_ratio"
    assert rows["brainless_step:existing"]["baseline"] == 10.0
    assert rows["brainless_step:existing"]["pinned_baseline"] == 1.0
    assert rows["brainless_step:existing"]["rolling_baseline_sample_count"] == 5


def test_runtime_speed_baseline_brainless_rolling_median_still_flags_regression(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:00:00Z",
            "finished_at": "2026-07-10T00:00:10Z",
            "step_durations": {"existing": 10.0},
        },
    )
    build_report(tmp_path)
    _write_brainless_log(
        tmp_path,
        [
            {
                "status": "OK",
                "started_at": f"2026-07-10T00:0{index}:00Z",
                "finished_at": f"2026-07-10T00:0{index}:10Z",
                "step_durations": {"existing": value},
            }
            for index, value in enumerate([9.0, 10.0, 10.0, 10.0, 11.0], start=1)
        ],
    )
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:06:00Z",
            "finished_at": "2026-07-10T00:06:13Z",
            "step_durations": {"existing": 13.0},
        },
    )

    report = build_report(tmp_path)
    regressions = {row["metric"]: row for row in report["comparison"]["regressions"]}

    assert report["status"] == "REGRESSION"
    assert regressions["brainless_step:existing"]["comparison_rule"] == "per_step_rolling_median_ratio"
    assert regressions["brainless_step:existing"]["baseline"] == 10.0
    assert regressions["brainless_step:existing"]["ratio"] == 1.3


def test_runtime_speed_baseline_excludes_workload_variable_steps_from_aggregate_regression(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:00:00Z",
            "finished_at": "2026-07-10T00:00:11Z",
            "step_durations": {
                "market_mining_cadence": 1.0,
                "member_queue": 1.0,
                "state_digest": 1.0,
                "stable": 10.0,
            },
        },
    )
    build_report(tmp_path)
    _write_brainless_log(
        tmp_path,
        [
            {
                "status": "OK",
                "started_at": f"2026-07-10T00:0{index}:00Z",
                "finished_at": f"2026-07-10T00:0{index}:11Z",
                "step_durations": {
                    "market_mining_cadence": 1.0,
                    "member_queue": 1.0,
                    "state_digest": 1.0,
                    "stable": 10.0,
                },
            }
            for index in range(1, 6)
        ],
    )
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {
            "status": "OK",
            "started_at": "2026-07-10T00:06:00Z",
            "finished_at": "2026-07-10T00:08:10Z",
            "step_durations": {
                "market_mining_cadence": 120.0,
                "member_queue": 120.0,
                "state_digest": 120.0,
                "stable": 10.0,
            },
        },
    )

    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "PASS"
    assert rows["brainless_run_duration_s"]["status"] == "PASS"
    assert rows["brainless_run_duration_s"]["aggregate_excludes_workload_variable_steps"] is True
    assert rows["brainless_run_duration_s"]["current"] == 10.0
    assert rows["brainless_step:market_mining_cadence"]["status"] == "WORKLOAD_VARIABLE"
    assert rows["brainless_step:market_mining_cadence"]["workload_variable"] is True
    assert rows["brainless_step:member_queue"]["status"] == "WORKLOAD_VARIABLE"
    assert rows["brainless_step:member_queue"]["workload_variable"] is True
    assert rows["brainless_step:state_digest"]["status"] == "WORKLOAD_VARIABLE"
    assert rows["brainless_step:state_digest"]["workload_variable"] is True
    assert report["comparison"]["regression_count"] == 0


def test_runtime_speed_baseline_marks_low_n_signal_age_as_pass_low_n(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_json(
        tmp_path / "data/research/order_flow_deadman_state.json",
        {
            "member_signal_age": {
                "0xaaa": {"signal_age_count": 20, "signal_age_p50_s": 1.0, "signal_age_p90_s": 1.0},
                "0xbbb": {"signal_age_count": 1, "signal_age_p50_s": 1.0, "signal_age_p90_s": 2.0},
            }
        },
    )
    build_report(tmp_path)

    _write_json(
        tmp_path / "data/research/order_flow_deadman_state.json",
        {
            "member_signal_age": {
                "0xaaa": {"signal_age_count": 20, "signal_age_p50_s": 1.0, "signal_age_p90_s": 1.0},
                "0xbbb": {"signal_age_count": 1, "signal_age_p50_s": 1.0, "signal_age_p90_s": 5.0},
            }
        },
    )
    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "PASS"
    assert report["metrics"]["signal_age_p90_s"]["value"] == 1.0
    assert report["metrics"]["signal_age_p90_s"]["worst_member"] == "0xaaa"
    assert report["metrics"]["signal_age_p90_s"]["sample_count"] == 20
    assert report["metrics"]["signal_age_p90_s"]["status"] == "MEASURED"
    assert report["metrics"]["signal_age_p90_s"]["eligible_member_signal_age_count"] == 1
    assert rows["signal_age_p90_s"]["status"] == "PASS"


def test_runtime_speed_baseline_regresses_signal_age_when_worst_member_has_enough_samples(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_json(
        tmp_path / "data/research/order_flow_deadman_state.json",
        {"member_signal_age": {"0xaaa": {"signal_age_count": 20, "signal_age_p50_s": 1.0, "signal_age_p90_s": 2.0}}},
    )
    build_report(tmp_path)

    _write_json(
        tmp_path / "data/research/order_flow_deadman_state.json",
        {"member_signal_age": {"0xaaa": {"signal_age_count": 20, "signal_age_p50_s": 1.0, "signal_age_p90_s": 3.0}}},
    )
    report = build_report(tmp_path)
    regressions = {row["metric"]: row for row in report["comparison"]["regressions"]}

    assert report["status"] == "REGRESSION"
    assert regressions["signal_age_p90_s"]["current"] == 3.0
    assert regressions["signal_age_p90_s"]["baseline"] == 2.0


def test_runtime_speed_baseline_pass_low_n_when_all_signal_members_are_low_n(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_json(
        tmp_path / "data/research/order_flow_deadman_state.json",
        {"member_signal_age": {"0xaaa": {"signal_age_count": 1, "signal_age_p50_s": 1.0, "signal_age_p90_s": 2.0}}},
    )
    build_report(tmp_path)

    _write_json(
        tmp_path / "data/research/order_flow_deadman_state.json",
        {"member_signal_age": {"0xaaa": {"signal_age_count": 1, "signal_age_p50_s": 1.0, "signal_age_p90_s": 5.0}}},
    )
    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "PASS"
    assert report["metrics"]["signal_age_p90_s"]["status"] == "PASS_LOW_N"
    assert report["metrics"]["signal_age_p90_s"]["sample_count"] == 1
    assert rows["signal_age_p90_s"]["status"] == "PASS_LOW_N"
    assert rows["signal_age_p90_s"]["worst_member"] == "0xaaa"
    assert rows["signal_age_p90_s"]["worst_member_signal_age_count"] == 1
    assert rows["signal_age_p90_s"]["sample_count"] == 1


def test_runtime_speed_baseline_flags_absolute_signal_age_staleness_at_any_sample_count(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_json(
        tmp_path / "data/research/order_flow_deadman_state.json",
        {
            "member_signal_age": {
                "0xaaa": {"signal_age_count": 20, "signal_age_p50_s": 1.0, "signal_age_p90_s": 1.0},
                "0xbbb": {"signal_age_count": 1, "signal_age_p50_s": 1.0, "signal_age_p90_s": 2.0},
            }
        },
    )
    build_report(tmp_path)

    _write_json(
        tmp_path / "data/research/order_flow_deadman_state.json",
        {
            "member_signal_age": {
                "0xaaa": {"signal_age_count": 20, "signal_age_p50_s": 1.0, "signal_age_p90_s": 1.0},
                "0xbbb": {"signal_age_count": 1, "signal_age_p50_s": 1.0, "signal_age_p90_s": 15.0},
            }
        },
    )
    report = build_report(tmp_path)
    regressions = {row["metric"]: row for row in report["comparison"]["regressions"]}

    assert report["status"] == "REGRESSION"
    assert regressions["signal_age_p90_s"]["status"] == "REGRESSION"
    assert regressions["signal_age_p90_s"]["regression_reason"] == "absolute_staleness"
    assert regressions["signal_age_p90_s"]["worst_member"] == "0xbbb"
    assert regressions["signal_age_p90_s"]["sample_count"] == 1


def test_runtime_speed_baseline_absolute_signal_age_staleness_overrides_missing_baseline_metric(
    tmp_path: Path,
) -> None:
    _seed_speed_inputs(tmp_path)
    build_report(tmp_path)
    pinned_path = tmp_path / "data/research/runtime_speed_baseline_pinned.json"
    pinned = json.loads(pinned_path.read_text())
    pinned["metrics"].pop("signal_age_p90_s")
    _write_json(pinned_path, pinned)

    _write_json(
        tmp_path / "data/research/order_flow_deadman_state.json",
        {
            "member_signal_age": {
                "0xaaa": {"signal_age_count": 20, "signal_age_p50_s": 1.0, "signal_age_p90_s": 1.0},
                "0xbbb": {"signal_age_count": 1, "signal_age_p50_s": 1.0, "signal_age_p90_s": 15.0},
            }
        },
    )
    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}
    regressions = {row["metric"]: row for row in report["comparison"]["regressions"]}

    assert report["status"] == "REGRESSION"
    assert "signal_age_p90_s" not in report["comparison"]["missing_comparable_metrics"]
    assert rows["signal_age_p90_s"]["status"] == "REGRESSION"
    assert rows["signal_age_p90_s"]["baseline"] is None
    assert rows["signal_age_p90_s"]["ratio"] is None
    assert regressions["signal_age_p90_s"]["regression_reason"] == "absolute_staleness"
    assert regressions["signal_age_p90_s"]["worst_member"] == "0xbbb"
    assert regressions["signal_age_p90_s"]["sample_count"] == 1


def test_runtime_speed_baseline_ignores_latest_guard_tail_when_distribution_passes(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path, guard_total_s=10.0)
    build_report(tmp_path)

    _seed_speed_inputs(tmp_path, guard_total_s=13.0, guard_event_totals=(10.0, 10.0, 14.0))
    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "PASS"
    assert rows["guard_cycle_total_s"]["status"] == "PASS"
    assert rows["guard_cycle_total_s"]["tail_sample_grace"] == "guard_latest_within_recent_p90_and_distribution_passes"


def test_runtime_speed_baseline_ignores_latest_guard_tail_inside_pinned_p90_envelope(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path, guard_total_s=20.0, guard_event_totals=(20.0, 22.0, 30.0))
    build_report(tmp_path)

    _seed_speed_inputs(tmp_path, guard_total_s=25.0, guard_event_totals=(20.0, 21.0, 26.0))
    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "PASS"
    assert rows["guard_cycle_total_s"]["status"] == "PASS"
    assert rows["guard_cycle_total_s"]["tail_sample_grace"] == "guard_latest_within_recent_p90_and_distribution_passes"


def test_runtime_speed_baseline_flags_unannotated_status_gap(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    build_report(tmp_path)

    _write_handoff_statuses(tmp_path, ["2026-07-10T00:00Z", "2026-07-10T00:31Z"])
    report = build_report(tmp_path)
    regressions = {row["metric"]: row for row in report["comparison"]["regressions"]}

    assert report["status"] == "REGRESSION"
    assert report["metrics"]["heartbeat_cadence_latest_s"]["value"] == 1860.0
    assert regressions["heartbeat_cadence_latest_s"]["threshold_s"] == 1200.0


def test_runtime_speed_baseline_accepts_marker_segmented_work_burst(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    build_report(tmp_path)

    _write_handoff_statuses(tmp_path, ["2026-07-10T00:00Z", "2026-07-10T00:31Z"])
    _write_serving_markers(tmp_path, ["2026-07-10T00:15Z", "2026-07-10T00:30Z"])
    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "PASS"
    assert report["metrics"]["heartbeat_cadence_latest_s"]["value"] == 900.0
    assert rows["heartbeat_cadence_latest_s"]["status"] == "PASS"
    assert rows["heartbeat_cadence_latest_s"]["ratio"] == 1.5
    assert report["evidence"]["heartbeat_latest_interval"]["marker_count"] == 2


def test_runtime_speed_baseline_marks_stale_brainless_sample_not_regression(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    build_report(tmp_path)
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {"status": "OK", "started_at": "2026-07-10T00:00:00Z", "finished_at": "2026-07-10T00:10:00Z"},
    )
    _set_mtime(tmp_path / "data/research/brainless_ops_latest.json", datetime(2026, 7, 10, tzinfo=timezone.utc))

    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "STALE_SAMPLE"
    assert report["comparison"]["regression_count"] == 0
    assert rows["brainless_run_duration_s"]["status"] == "STALE_SAMPLE"
    assert rows["brainless_run_duration_s"]["sample_age_s"] > 1200.0
    assert "producer" in report["next_action"]


def test_runtime_speed_baseline_marks_first_post_reclaim_run_warm_up(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    build_report(tmp_path)
    started = "2026-07-18T12:02:23Z"
    finished = "2026-07-18T12:12:55Z"
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {"kind": "brainless_ops_state", "status": "DEGRADED", "started_at": started, "finished_at": finished},
    )
    _set_mtime(tmp_path / "data/research/brainless_ops_latest.json", datetime.now(timezone.utc))
    (tmp_path / "data/research" / "brainless_ops.launchd.out").write_text(
        '{"status":"STALE_LOCK_RECLAIMED"}\n'
        + json.dumps({"kind": "brainless_ops_state", "started_at": started, "finished_at": finished})
        + "\n"
    )

    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "WARM_UP"
    assert report["comparison"]["regression_count"] == 0
    assert rows["brainless_run_duration_s"]["status"] == "WARM_UP"
    assert rows["brainless_run_duration_s"]["post_stale_lock_reclaim_run_ordinal"] == 1


def test_runtime_speed_baseline_marks_brainless_regression_pending_until_timed_samples(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    build_report(tmp_path)
    _write_json(
        tmp_path / "data/research/brainless_ops_latest.json",
        {"status": "OK", "started_at": "2026-07-10T00:00:00Z", "finished_at": "2026-07-10T00:10:00Z"},
    )
    _set_mtime(tmp_path / "data/research/brainless_ops_latest.json", datetime.now(timezone.utc))

    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "KNOWN_PENDING_ATTRIBUTION"
    assert report["comparison"]["regression_count"] == 0
    assert rows["brainless_run_duration_s"]["status"] == "KNOWN_PENDING_ATTRIBUTION"
    assert rows["brainless_run_duration_s"]["timed_sample_count"] == 0


def test_runtime_speed_baseline_returns_zero_for_written_regression_report(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path, guard_total_s=10.0)
    build_report(tmp_path)
    _seed_speed_inputs(tmp_path, guard_total_s=13.0)

    rc = main(["--root", str(tmp_path), "--output", "data/research/runtime_speed_baseline_latest.json"])
    report = json.loads((tmp_path / "data/research/runtime_speed_baseline_latest.json").read_text())

    assert rc == 0
    assert report["status"] == "REGRESSION"
    assert {row["metric"] for row in report["comparison"]["regressions"]} == {"guard_cycle_total_s"}


def test_runtime_speed_baseline_reclassifies_ask_fable_wall_as_info(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_ask_fable_log(tmp_path, "20260710T000000Z", rc=0, wall_s=30.0)
    build_report(tmp_path)
    _write_ask_fable_log(tmp_path, "20260710T001000Z", rc=0, wall_s=300.0)

    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "PASS"
    assert rows["ask_fable_latest_wall_s"]["status"] == "INFO"
    assert rows["ask_fable_latest_wall_s"]["ratio"] == 10.0
    assert report["comparison"]["regression_count"] == 0


def test_runtime_speed_baseline_flags_ask_fable_slow_without_runtime_regression(tmp_path: Path) -> None:
    _seed_speed_inputs(tmp_path)
    _write_ask_fable_log(tmp_path, "20260710T000000Z", rc=0, wall_s=30.0)
    build_report(tmp_path)
    _write_ask_fable_log(tmp_path, "20260710T001000Z", rc=0, wall_s=901.0)

    report = build_report(tmp_path)
    rows = {row["metric"]: row for row in report["comparison"]["rows"]}

    assert report["status"] == "PASS"
    assert rows["ask_fable_latest_wall_s"]["status"] == "ASK_FABLE_SLOW"
    assert rows["ask_fable_latest_wall_s"]["slow_threshold_s"] == 900.0
    assert report["comparison"]["regression_count"] == 0
