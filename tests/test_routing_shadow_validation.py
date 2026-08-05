from pathlib import Path
from types import SimpleNamespace

import scripts.report_routing_shadow_validation as routing_shadow
import scripts.run_wallet_copy_live_guard as live_guard
from scripts.report_routing_shadow_validation import (
    _load_resolution_map,
    _validation_clock_started_at,
    build_routing_shadow_validation,
    r7_dated_snapshot_path,
    render_fee_gate_calibration_table,
    write_r7_dated_snapshot,
)


WALLET_A = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
WALLET_B = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
WALLET_C = "0xcccccccccccccccccccccccccccccccccccccccc"


def _state(wallet: str, *, observed_ts: float, intent_id: str, window: int = 1_800_000_000) -> dict:
    return {
        "source_wallet": wallet,
        "candidate_id": f"candidate_{wallet[2:6]}",
        "policy_id": "policy_a",
        "candidate_intent_summary": {
            "fresh_candidate_intents": 1,
            "fresh_candidate_intents_after_toxicity_protection": 1,
            "fresh_candidate_intents_after_expected_fee_gate": 1,
            "sample_intents": [
                {
                    "intent_id": intent_id,
                    "source_wallet": wallet,
                    "window_start_s": window,
                    "market_slug": f"btc-updown-5m-{window}",
                    "observed_ts": observed_ts,
                    "side": "BUY",
                    "outcome": "YES",
                    "limit_price": 0.51,
                }
            ],
        },
    }


def test_routing_shadow_validation_writes_r7_dated_snapshot(monkeypatch, tmp_path: Path) -> None:
    latest = tmp_path / "routing_shadow_validation_latest.json"
    payload = {"kind": "routing_shadow_validation", "generated_at": "2026-07-15T12:14:57Z"}
    writes: list[tuple[Path, dict]] = []

    def record_write(path: Path, data: dict, **_kwargs) -> None:
        writes.append((Path(path), data))

    monkeypatch.setattr(routing_shadow, "atomic_write_json", record_write)

    snapshot = write_r7_dated_snapshot(latest, payload)

    assert snapshot == tmp_path / "routing_shadow_validation_latest_r7_20260715T1214Z.json"
    assert len(writes) == 1
    assert writes[0][0] == snapshot
    assert writes[0][1]["r7_dated_snapshot"]["source_latest_path"] == str(latest)
    assert writes[0][1]["r7_dated_snapshot"]["snapshot_path"] == str(snapshot)


def test_routing_shadow_validation_r7_snapshot_path_uses_generated_at() -> None:
    assert r7_dated_snapshot_path(
        Path("data/research/routing_shadow_validation_latest.json"),
        generated_at="2026-07-15T12:14:57Z",
    ) == Path("data/research/routing_shadow_validation_latest_r7_20260715T1214Z.json")


def test_routing_shadow_validation_r7_snapshot_same_minute_never_overwrites(tmp_path: Path) -> None:
    latest = tmp_path / "routing_shadow_validation_latest.json"
    first = write_r7_dated_snapshot(
        latest,
        {"kind": "routing_shadow_validation", "generated_at": "2026-07-15T12:14:01Z", "sequence": 1},
    )
    first_bytes = first.read_bytes()

    second = write_r7_dated_snapshot(
        latest,
        {"kind": "routing_shadow_validation", "generated_at": "2026-07-15T12:14:57Z", "sequence": 2},
    )

    assert first == tmp_path / "routing_shadow_validation_latest_r7_20260715T1214Z.json"
    assert second == tmp_path / "routing_shadow_validation_latest_r7_20260715T1214Z_2.json"
    assert first.read_bytes() == first_bytes
    assert second.exists()


def test_load_resolution_map_streams_jsonl_without_read_text(monkeypatch, tmp_path: Path) -> None:
    resolutions = tmp_path / "btc_resolutions.jsonl"
    resolutions.write_text(
        '{"market_slug":"btc-updown-5m-1800000000","direction":"UP"}\n'
        "not-json\n"
        '{"market_slug":"btc-updown-5m-1800000300","direction":"DOWN"}\n',
        encoding="utf-8",
    )

    def fail_read_text(*_args, **_kwargs):
        raise AssertionError("resolution feed must be streamed")

    monkeypatch.setattr(Path, "read_text", fail_read_text)

    loaded = _load_resolution_map(str(resolutions))

    assert loaded["btc-updown-5m-1800000000"]["direction"] == "UP"
    assert loaded["btc-updown-5m-1800000300"]["direction"] == "DOWN"


def test_routing_shadow_validation_main_writes_r7_snapshot_before_latest(monkeypatch, tmp_path: Path) -> None:
    latest = tmp_path / "routing_shadow_validation_latest.json"
    report = {
        "kind": "routing_shadow_validation",
        "generated_at": "2026-07-15T12:14:57Z",
        "summary": {"status": "ACCUMULATING"},
    }
    writes: list[tuple[Path, dict]] = []

    monkeypatch.setattr(
        routing_shadow,
        "parse_args",
        lambda: SimpleNamespace(
            guard_state=str(tmp_path / "guard.json"),
            out=str(latest),
            previous_report="",
            fee_cal_table_out="",
            shadow_candidate_seats=str(tmp_path / "seats.json"),
            min_validation_hours=6.0,
            retain_rows=100,
            retain_cycles=100,
            r7_dated_snapshot=True,
        ),
    )
    monkeypatch.setattr(routing_shadow, "load_json", lambda _path, default=None: default or {})
    monkeypatch.setattr(routing_shadow, "load_shadow_candidate_seats", lambda _path: [])
    monkeypatch.setattr(routing_shadow, "build_routing_shadow_validation_from_guard", lambda *_args, **_kwargs: report)
    monkeypatch.setattr(routing_shadow, "atomic_write_json", lambda path, payload: writes.append((Path(path), payload)))

    assert routing_shadow.main() == 0

    assert [path for path, _payload in writes] == [
        tmp_path / "routing_shadow_validation_latest_r7_20260715T1214Z.json",
        latest,
    ]


def test_live_guard_routing_shadow_cadence_does_not_write_r7_dated_names(monkeypatch, tmp_path: Path) -> None:
    writes: list[Path] = []
    output = tmp_path / "routing_shadow_validation_latest.json"

    monkeypatch.setattr(live_guard, "load_json", lambda _path, default=None: default if default is not None else {})
    monkeypatch.setattr(live_guard, "atomic_write_json", lambda path, _payload: writes.append(Path(path)))
    monkeypatch.setattr(
        live_guard,
        "build_routing_shadow_validation_from_guard",
        lambda *_args, **_kwargs: {"kind": "routing_shadow_validation", "summary": {}},
    )
    monkeypatch.setattr(live_guard, "_latest_successful_nondenied_wallet", lambda *_args, **_kwargs: {})

    live_guard._run_routing_shadow_validation(
        SimpleNamespace(
            routing_router_mode="shadow",
            routing_shadow_validation_state=str(output),
            routing_shadow_candidate_seats=str(tmp_path / "seats.json"),
            routing_shadow_validation_min_hours=6.0,
            routing_shadow_validation_retain_rows=100,
        ),
        active_set_runtime={"members": []},
        live_stdout={},
        live_probe_result={},
        live_probe_promotion_result={},
        dataapi_poll_result={},
        generated_at="2026-07-15T12:14:57Z",
    )

    assert writes == [output]
    assert all("_r7_" not in path.name for path in writes)


def test_live_guard_shadow_lanes_row_profile_counts_compacted_payload() -> None:
    profile = live_guard._shadow_lanes_row_profile(
        {
            "rows_count": 203,
            "lanes_count": 2,
            "parked_lanes_count": 1,
            "summary": {
                "intents_built": 203,
                "fresh_intents": 3,
                "guard_filter_passed": 3,
                "parity_passed": 3,
            },
        }
    )

    assert profile["rows_count"] == 203
    assert profile["lanes_count"] == 2
    assert profile["parked_lanes_count"] == 1
    assert profile["lane_intents_built"] == 203
    assert profile["total_rows_held_across_shadow_lane_specs"] == 203


def test_live_guard_stdout_summary_reports_guard_tail_markers() -> None:
    summary = live_guard._stdout_cycle_summary(
        {
            "status": "LIVE_GUARD_RUNNING",
            "live_orders_allowed": True,
            "guard_cycle_tail_attribution": {
                "state_payload_serialized_mib": 6.1,
                "status": "MEASURED_PREVIOUS_WRITE_STAT",
            },
            "guard_loop_profile": {
                "stage_timers": [
                    {"name": "guard_cycle_scheduler", "duration_s": 0.1},
                    {"name": "guard_state_payload_measurement", "duration_s": 0.2},
                ],
                "stage_timers_after_persist": [
                    {"name": "guard_state_payload_measurement", "duration_s": 0.2},
                    {"name": "guard_state_persist", "duration_s": 0.01},
                ],
            },
        }
    )

    markers = summary["guard_loop_stage_markers"]
    assert summary["guard_cycle_tail_attribution"]["state_payload_serialized_mib"] == 6.1
    assert markers["has_guard_cycle_scheduler"] is True
    assert markers["has_guard_state_persist_post_write_stat"] is True
    assert markers["has_guard_state_persist"] is True


def test_routing_shadow_validation_routes_earliest_signal_and_suppresses_later_member() -> None:
    probe_states = {
        "wallet_b.json": _state(WALLET_B, observed_ts=10.0, intent_id="b1"),
    }

    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
                {"source_wallet": WALLET_B, "candidate_id": "candidate_b", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=_state(WALLET_A, observed_ts=20.0, intent_id="a1"),
        live_probe_result={"rows": [{"source_wallet": WALLET_B, "path": "wallet_b.json"}]},
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
        load_json_func=lambda path, default: probe_states.get(Path(path).name, default),
    )

    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False
    assert report["summary"]["status"] == "ACCUMULATING"
    assert report["summary"]["copyintent_parity_status"] == "PASS"
    assert report["summary"]["would_submit_windows"] == 1
    assert report["summary"]["extra_would_submit_windows"] == 0
    assert report["summary"]["routing_suppressed_signals"] == 1
    assert report["summary"]["runtime_selected_wallet"] == WALLET_A
    assert report["summary"]["runtime_selected_wallet_source"] == "selected_member"
    assert report["summary"]["shadow_selected_wallet"] == WALLET_B
    assert report["summary"]["selection_changes"] == 0
    assert report["rows"][0]["runtime_selected_wallet"] == WALLET_A
    assert report["rows"][0]["runtime_selected_wallet_source"] == "selected_member"
    assert report["rows"][0]["shadow_selected_wallet"] == WALLET_B
    assert report["rows"][0]["winning_source_wallet"] == WALLET_B
    assert report["routing_suppressed_rows"][0]["suppressed_source_wallet"] == WALLET_A


def test_routing_shadow_validation_flags_parity_conflict_and_keeps_live_flip_off() -> None:
    conflicted_state = _state(WALLET_A, observed_ts=10.0, intent_id="bad")
    conflicted_state["candidate_intent_summary"]["sample_intents"][0]["source_wallet"] = WALLET_C

    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=conflicted_state,
        live_probe_result={"rows": []},
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
    )

    assert report["summary"]["status"] == "CONFLICT"
    assert report["summary"]["copyintent_parity_conflicts"] == 1
    assert report["summary"]["live_flip_allowed"] is False
    assert report["rows"] == []
    assert report["copyintent_parity_conflicts"][0]["copyintent_source_wallet"] == WALLET_C


def test_routing_shadow_validation_keeps_priority_frozen_member_out_of_route() -> None:
    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
                {"source_wallet": WALLET_B, "candidate_id": "candidate_b", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=_state(WALLET_A, observed_ts=20.0, intent_id="a1"),
        live_probe_result={"rows": [{"source_wallet": WALLET_B, "path": "wallet_b.json"}]},
        selection_priority_freeze={"wallets": [WALLET_B]},
        generated_at="2026-07-10T08:00:00Z",
        load_json_func=lambda path, default: _state(WALLET_B, observed_ts=10.0, intent_id="b1"),
    )

    assert report["rows"][0]["winning_source_wallet"] == WALLET_A
    assert report["summary"]["denied_signal_count_latest_cycle"] == 1
    assert report["denied_signal_rows"][0]["source_wallet"] == WALLET_B


def test_routing_shadow_validation_seats_shadow_candidate_for_fee_accrual() -> None:
    shadow_state = _state(WALLET_B, observed_ts=10.0, intent_id="shadow-fee", window=1_800_000_300)
    shadow_state["candidate_intent_summary"]["fresh_candidate_intents_after_expected_fee_gate"] = 0
    intent = shadow_state["candidate_intent_summary"]["sample_intents"][0]
    intent.update(
        {
            "side": "YES",
            "outcome": "UP",
            "limit_price": 0.40,
            "shares": 10.0,
            "metadata": {"expected_fee_gate": {"expected_fee_usd": 0.168}},
        }
    )

    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=_state(WALLET_A, observed_ts=20.0, intent_id="a1"),
        live_probe_result={"rows": []},
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
        shadow_candidate_members=[
            {
                "source_wallet": WALLET_B,
                "candidate_id": "shadow_b",
                "policy_id": "paper_shadow_policy",
                "enabled": True,
                "shadow_only": True,
            }
        ],
        load_json_func=lambda path, default: shadow_state
        if Path(path).name == "wallet_copy_live_execution_probe_shadow_b.json"
        else default,
        resolution_map={
            "btc-updown-5m-1800000300": {
                "market_slug": "btc-updown-5m-1800000300",
                "direction": "UP",
                "uma_resolution_status": "RESOLVED",
                "source": "test",
            }
        },
    )

    retained = report["summary"]["fee_gate_calibration_retained"]
    assert report["summary"]["runtime_selected_wallet"] == WALLET_A
    assert report["summary"]["shadow_candidate_seat_count"] == 1
    assert report["summary"]["shadow_candidate_seat_wallets"] == [WALLET_B]
    assert retained["by_member"][WALLET_B]["fee_gated_intents"] == 1
    assert retained["by_member"][WALLET_B]["measured_unique_windows"] == 1
    assert retained["by_member"][WALLET_B]["post_fee_pnl_usd"] == 5.832
    evidence = {row["source_wallet"]: row for row in report["member_evidence"]}
    assert evidence[WALLET_B]["probe_status"] == "PASS"


def test_routing_shadow_validation_accounts_no_signal_member_and_reports_attrition() -> None:
    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
                {"source_wallet": WALLET_B, "candidate_id": "candidate_b", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=_state(WALLET_A, observed_ts=20.0, intent_id="a1"),
        live_probe_result={"rows": []},
        active_set_dataapi_poller={
            "fetch_meta": {
                WALLET_B: {
                    "fresh_buy_rows_le_10s_by_source": {"trade:user": 0},
                    "policy_feedback": {"policy_compatible_fresh_buy_rows_le_30s": 0},
                }
            }
        },
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
    )

    assert report["summary"]["coverage_accounted_member_count"] == 2
    assert report["summary"]["all_non_denied_members_accounted_this_cycle"] is True
    assert report["summary"]["missing_non_denied_members"] == []
    assert report["summary"]["filter_attrition_totals_latest_cycle"] == {
        "fresh_candidate_intents": 1,
        "fresh_candidate_intents_after_expected_fee_gate": 1,
        "fresh_candidate_intents_after_toxicity_protection": 1,
        "routeable_signals": 1,
        "would_submit": 1,
    }
    member_b = [row for row in report["member_evidence"] if row["source_wallet"] == WALLET_B][0]
    assert member_b["probe_status"] == "STRUCTURAL_EXCLUSION"
    assert member_b["structural_exclusion_reason"] == "no_current_fresh_or_policy_compatible_signal_in_dataapi_meta"


def test_routing_shadow_validation_counts_runtime_selection_changes() -> None:
    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_B},
            "members": [
                {"source_wallet": WALLET_B, "candidate_id": "candidate_b", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=_state(WALLET_B, observed_ts=20.0, intent_id="b1"),
        live_probe_result={"rows": []},
        previous={
            "cycle_samples": [
                {"generated_at": "2026-07-10T07:50:00Z", "runtime_selected_wallet": WALLET_A},
                {"generated_at": "2026-07-10T07:55:00Z", "runtime_selected_wallet": WALLET_A},
            ]
        },
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
    )

    assert report["summary"]["runtime_selected_wallet"] == WALLET_B
    assert report["summary"]["selection_changes"] == 1
    assert report["cycle_samples"][-1]["runtime_selected_wallet"] == WALLET_B


def test_routing_shadow_validation_reports_runtime_selection_source_fallbacks() -> None:
    fresh_runtime = build_routing_shadow_validation(
        active_set_runtime={
            "fresh_runtime_member_selection": {"selected_wallet": WALLET_B},
            "members": [
                {"source_wallet": WALLET_B, "candidate_id": "candidate_b", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=_state(WALLET_A, observed_ts=20.0, intent_id="a1"),
        live_probe_result={"rows": []},
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
    )

    assert fresh_runtime["summary"]["runtime_selected_wallet"] == WALLET_B
    assert fresh_runtime["summary"]["runtime_selected_wallet_source"] == "fresh_runtime_member_selection"
    assert fresh_runtime["cycle_samples"][-1]["runtime_selected_wallet_source"] == "fresh_runtime_member_selection"

    live_fallback = build_routing_shadow_validation(
        active_set_runtime={
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=_state(WALLET_A, observed_ts=20.0, intent_id="a1"),
        live_probe_result={"rows": []},
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
    )

    assert live_fallback["summary"]["runtime_selected_wallet"] == WALLET_A
    assert live_fallback["summary"]["runtime_selected_wallet_source"] == "live_execution_state_fallback"
    assert live_fallback["cycle_samples"][-1]["runtime_selected_wallet_source"] == "live_execution_state_fallback"
    assert live_fallback["rows"][0]["runtime_selected_wallet_source"] == "live_execution_state_fallback"


def test_validation_clock_uses_handoff_start_when_cycle_buffer_evicted(tmp_path: Path) -> None:
    handoff = tmp_path / "HANDOFF.md"
    handoff.write_text(
        "\n".join(
            [
                "## 2026-07-13T00:25Z codex STATUS [PROMOTE]",
                "- routing_shadow ACCRUING elapsed_h=43.644042 would=425",
                "## 2026-07-13T00:45Z codex STATUS [PROMOTE]",
                "- routing_shadow validation_elapsed_hours=43.627817 would=425",
                "## 2026-07-13T01:12Z fable DIRECTION [SELF-DEV]",
                "- metric is decreasing: 43.644042@00:25 -> 43.627@00:45; use the explicit pre-cap anchor.",
            ]
        )
    )

    started_at, elapsed = _validation_clock_started_at(
        cycles=[
            {"generated_at": "2026-07-13T00:55:00Z"},
            {"generated_at": "2026-07-13T01:05:00Z"},
        ],
        previous={},
        generated_at="2026-07-13T01:05:00Z",
        handoff_path=handoff,
    )

    assert started_at == "2026-07-11T04:46:21.448800Z"
    assert elapsed == 44.310709


def test_validation_clock_elapsed_is_monotone_after_cycle_eviction() -> None:
    previous = {
        "generated_at": "2026-07-13T01:00:00Z",
        "summary": {
            "validation_clock_started_at": "2026-07-11T04:46:21.448800Z",
            "validation_elapsed_hours": 44.227375,
        },
    }

    started_at, elapsed = _validation_clock_started_at(
        cycles=[
            {"generated_at": "2026-07-13T01:05:00Z"},
            {"generated_at": "2026-07-13T01:10:00Z"},
        ],
        previous=previous,
        generated_at="2026-07-13T01:10:00Z",
        handoff_path=Path("/does/not/exist"),
    )

    assert started_at == "2026-07-11T04:46:21.448800Z"
    assert elapsed > previous["summary"]["validation_elapsed_hours"]
    assert elapsed == 44.394042


def test_routing_shadow_validation_ties_attribution_by_wallet_not_last_successful() -> None:
    probe_states = {
        "wallet_a.json": _state(WALLET_A, observed_ts=10.0, intent_id="a1"),
    }

    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_B},
            "members": [
                {"source_wallet": WALLET_B, "candidate_id": "candidate_b", "policy_id": "policy_a", "enabled": True},
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=_state(WALLET_B, observed_ts=10.0, intent_id="b1"),
        live_probe_result={"rows": [{"source_wallet": WALLET_A, "path": "wallet_a.json"}]},
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
        load_json_func=lambda path, default: probe_states.get(Path(path).name, default),
        last_successful_wallet=WALLET_B,
    )

    assert report["rows"][0]["winning_source_wallet"] == WALLET_A
    assert report["rows"][0]["tiebreak_rule"] == "earliest_observed_ts_then_lexicographic_wallet"
    assert report["summary"]["attribution_rule"] == "earliest_observed_ts_then_lexicographic_wallet"


def test_routing_shadow_validation_persists_fee_gate_calibration_rows() -> None:
    state = _state(WALLET_A, observed_ts=10.0, intent_id="pass")
    state["candidate_intent_summary"] = {
        "fresh_candidate_intents": 2,
        "fresh_candidate_intents_after_toxicity_protection": 1,
        "fresh_candidate_intents_after_expected_fee_gate": 1,
        "expected_fee_capture_gate": {"fee_rate": 0.07},
        "sample_intents": [
            {
                "intent_id": "pass",
                "source_wallet": WALLET_A,
                "window_start_s": 1_800_000_000,
                "market_slug": "btc-updown-5m-1800000000",
                "observed_ts": 10.0,
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.51,
                "shares": 10.0,
            },
            {
                "intent_id": "fee-gated",
                "source_wallet": WALLET_A,
                "window_start_s": 1_800_000_300,
                "market_slug": "btc-updown-5m-1800000300",
                "observed_ts": 11.0,
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.40,
                "shares": 10.0,
                "metadata": {"drift_buffer": {"expected_edge_after_buffer": 0.02}},
            },
        ],
    }
    previous_fee_row = {
        "cycle_generated_at": "2026-07-10T07:55:00Z",
        "source_wallet": WALLET_A,
        "market_slug": "btc-updown-5m-1799999700",
        "window_start_s": 1_799_999_700,
        "intent_id": "previous-fee",
        "expected_fee_usd": 0.1,
        "expected_edge": 0.01,
        "realized_paper_outcome": {"status": "UNRESOLVED"},
    }

    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=state,
        live_probe_result={"rows": []},
        previous={"fee_gated_measurement_rows": [previous_fee_row]},
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
        resolution_map={
            "btc-updown-5m-1800000300": {
                "market_slug": "btc-updown-5m-1800000300",
                "direction": "UP",
                "uma_resolution_status": "RESOLVED",
                "source": "test",
            }
        },
    )

    latest = report["summary"]["fee_gate_calibration_latest_cycle"]
    retained = report["summary"]["fee_gate_calibration_retained"]
    row = [item for item in report["fee_gated_measurement_rows"] if item["intent_id"] == "fee-gated"][0]
    assert row["cycle_generated_at"] == "2026-07-10T08:00:00Z"
    assert row["expected_fee_usd"] == 0.168
    assert row["expected_edge"] == 0.02
    assert row["expected_edge_source"] == "metadata.drift_buffer.expected_edge_after_buffer"
    assert row["expected_edge_status"] == "AVAILABLE"
    assert row["expected_edge_absence_reason"] is None
    assert row["regime"] == "weekday"
    assert row["day_of_week_utc"] == "fri"
    assert row["utc_hour"] == 8
    assert row["realized_paper_outcome"]["wins"] is True
    assert row["realized_paper_outcome"]["paper_pnl_usd"] == 6.0
    assert latest["fee_gated_intents"] == 1
    assert latest["resolved_intents"] == 1
    assert latest["paper_pnl_usd"] == 6.0
    assert latest["pre_fee_pnl_usd"] == 6.0
    assert latest["expected_fee_usd_sum"] == 0.168
    assert latest["expected_fee_usd_all_rows_sum"] == 0.168
    assert latest["post_fee_pnl_usd"] == 5.832
    assert latest["measurable_resolved_intents"] == 1
    assert latest["unmeasured_resolved_intents"] == 0
    assert latest["regime_counts"] == {"weekday": 1}
    assert latest["expected_edge_status"] == "AVAILABLE"
    assert latest["expected_edge_count"] == 1
    assert retained["fee_gated_intents"] == 2
    assert retained["by_member"][WALLET_A]["fee_gated_intents"] == 2
    assert retained["regime_counts"] == {"weekday": 2}
    assert retained["by_member"][WALLET_A]["regime_counts"] == {"weekday": 2}
    assert retained["expected_fee_usd_sum"] == 0.168
    assert retained["expected_fee_usd_all_rows_sum"] == 0.268
    assert retained["expected_fee_usd_unresolved_sum"] == 0.1
    assert retained["post_fee_pnl_usd"] == 5.832
    assert retained["expected_edge_status"] == "AVAILABLE"
    table = render_fee_gate_calibration_table(
        report,
        source_artifact="data/research/routing_shadow_validation_latest.json",
        table_generated_at="2026-07-10T08:01:00Z",
    )
    assert "pre_fee_pnl_usd: +6.000000" in table
    assert "post_fee_pnl_usd: +5.832000" in table
    assert "regime_counts: weekday:2" in table
    assert "## Extra Would-Submit Cohort" in table
    assert "| 0xaaaa...aaaa | weekday:2 | 2 | 1 | 1 | 0 | 1 | 1-0 | +0.168000 | +6.000000 | +5.832000 | +0.000000 | +0.100000 | missing_market_resolution_in_resolution_feed:1 |" in table


def test_routing_shadow_validation_measures_extra_would_submit_cohort() -> None:
    state_a = _state(WALLET_A, observed_ts=20.0, intent_id="a1")
    state_b = _state(WALLET_B, observed_ts=10.0, intent_id="b1")
    state_a["candidate_intent_summary"]["fresh_candidate_intents_after_expected_fee_gate"] = 0
    for state, wallet, intent_id in ((state_a, WALLET_A, "a1"), (state_b, WALLET_B, "b1")):
        state["candidate_intent_summary"]["expected_fee_capture_gate"] = {"fee_rate": 0.07}
        intent = state["candidate_intent_summary"]["sample_intents"][0]
        intent.update(
            {
                "intent_id": intent_id,
                "source_wallet": wallet,
                "window_start_s": 1_800_000_300,
                "market_slug": "btc-updown-5m-1800000300",
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.40,
                "shares": 10.0,
            }
        )

    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
                {"source_wallet": WALLET_B, "candidate_id": "candidate_b", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=state_a,
        live_probe_result={"rows": [{"source_wallet": WALLET_B, "path": "wallet_b.json"}]},
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
        load_json_func=lambda path, default: state_b if Path(path).name == "wallet_b.json" else default,
        resolution_map={
            "btc-updown-5m-1800000300": {
                "market_slug": "btc-updown-5m-1800000300",
                "direction": "UP",
                "uma_resolution_status": "RESOLVED",
                "source": "test",
            }
        },
    )

    extra = report["summary"]["extra_would_submit_post_fee_measurement"]
    assert report["rows"][0]["extra_would_submit_window"] is True
    assert report["rows"][0]["winning_source_wallet"] == WALLET_B
    assert extra["fee_gated_intents"] == 1
    assert extra["unique_windows"] == 1
    assert extra["resolved_intents"] == 1
    assert extra["measurable_resolved_intents"] == 1
    assert extra["measured_unique_windows"] == 1
    assert extra["expected_fee_usd_sum"] == 0.168
    assert extra["pre_fee_pnl_usd"] == 6.0
    assert extra["post_fee_pnl_usd"] == 5.832
    table = render_fee_gate_calibration_table(
        report,
        source_artifact="data/research/routing_shadow_validation_latest.json",
        table_generated_at="2026-07-10T08:01:00Z",
    )
    assert "gate_result: EXTEND_SHADOW" in table
    assert "unique_windows: 1" in table
    assert "measured_unique_windows: 1" in table
    assert "promote_if: post_fee_pnl_usd > 0 and measured_unique_windows >= 30" in table
    assert "| 0xbbbb...bbbb | weekday:1 | 1 | 1 | 1 | 0 | 1-0 | +0.168000 | +6.000000 | +5.832000 |" in table


def test_routing_shadow_validation_marks_missing_expected_edge_as_fee_vs_fixed_basis() -> None:
    state = _state(WALLET_A, observed_ts=10.0, intent_id="pass")
    state["candidate_intent_summary"] = {
        "fresh_candidate_intents": 2,
        "fresh_candidate_intents_after_toxicity_protection": 2,
        "fresh_candidate_intents_after_expected_fee_gate": 1,
        "expected_fee_capture_gate": {"fee_rate": 0.07},
        "sample_intents": [
            {
                "intent_id": "pass",
                "source_wallet": WALLET_A,
                "window_start_s": 1_800_000_000,
                "market_slug": "btc-updown-5m-1800000000",
                "observed_ts": 10.0,
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.51,
                "shares": 10.0,
            },
            {
                "intent_id": "fee-gated",
                "source_wallet": WALLET_A,
                "window_start_s": 1_800_000_300,
                "market_slug": "btc-updown-5m-1800000300",
                "observed_ts": 11.0,
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.40,
                "shares": 10.0,
                "metadata": {"inventory_v3_drip": {"tranche_limit_price": 0.40}},
            },
        ],
    }

    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=state,
        live_probe_result={"rows": []},
        previous={},
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
    )

    latest = report["summary"]["fee_gate_calibration_latest_cycle"]
    row = report["fee_gated_measurement_rows"][0]
    assert row["expected_edge"] is None
    assert row["expected_edge_source"] == "not_available_fee_gate_operates_on_fee_vs_fixed_assumption"
    assert row["expected_edge_status"] == "NOT_AVAILABLE_NO_UPSTREAM_INPUT"
    assert row["expected_edge_absence_reason"] == (
        "no upstream expected_edge or expected_edge_after_buffer field on intent, metadata, or drift_buffer"
    )
    assert row["expected_edge_calibration_basis"] == "realized_paper_pnl_vs_expected_fee_per_member"
    assert latest["expected_edge_status"] == "NOT_AVAILABLE_NO_UPSTREAM_INPUT"
    assert latest["expected_edge_count"] == 0
    assert latest["expected_edge_missing_count"] == 1
    assert latest["expected_edge_calibration_basis"] == "realized_paper_pnl_vs_expected_fee_per_member"


def test_routing_shadow_validation_reports_unmeasured_resolved_fee_gated_rows() -> None:
    state = _state(WALLET_A, observed_ts=10.0, intent_id="pass")
    state["candidate_intent_summary"] = {
        "fresh_candidate_intents": 2,
        "fresh_candidate_intents_after_toxicity_protection": 2,
        "fresh_candidate_intents_after_expected_fee_gate": 1,
        "expected_fee_capture_gate": {"fee_rate": 0.07},
        "sample_intents": [
            {
                "intent_id": "pass",
                "source_wallet": WALLET_A,
                "window_start_s": 1_800_000_000,
                "market_slug": "btc-updown-5m-1800000000",
                "observed_ts": 10.0,
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.51,
                "shares": 10.0,
            },
            {
                "intent_id": "fee-gated-missing-shares",
                "source_wallet": WALLET_A,
                "window_start_s": 1_800_000_300,
                "market_slug": "btc-updown-5m-1800000300",
                "observed_ts": 11.0,
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.40,
                "metadata": {"expected_fee_gate": {"expected_fee_usd": 0.168}},
            },
        ],
    }

    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=state,
        live_probe_result={"rows": []},
        previous={},
        selection_priority_freeze={},
        generated_at="2026-07-10T08:00:00Z",
        resolution_map={
            "btc-updown-5m-1800000300": {
                "market_slug": "btc-updown-5m-1800000300",
                "direction": "UP",
                "uma_resolution_status": "RESOLVED",
                "source": "test",
            }
        },
    )

    latest = report["summary"]["fee_gate_calibration_latest_cycle"]
    assert latest["resolved_intents"] == 1
    assert latest["measurable_resolved_intents"] == 0
    assert latest["unmeasured_resolved_intents"] == 1
    assert latest["expected_fee_usd_sum"] == 0.0
    assert latest["expected_fee_usd_unmeasured_resolved_sum"] == 0.168
    assert latest["pnl_measurement_gap_reasons"] == {
        "missing_shares_or_size_on_fee_gated_measurement_row": 1
    }
    table = render_fee_gate_calibration_table(
        report,
        source_artifact="data/research/routing_shadow_validation_latest.json",
        table_generated_at="2026-07-10T08:01:00Z",
    )
    assert "unmeasured_resolved_intents: 1" in table
    assert "missing_shares_or_size_on_fee_gated_measurement_row:1" in table


def test_routing_shadow_validation_rejoins_retained_extra_rows_against_fresh_resolutions() -> None:
    state_a = _state(WALLET_A, observed_ts=20.0, intent_id="a2", window=1_800_000_600)
    previous = {
        "rows": [
            {
                "cycle_generated_at": "2026-07-10T07:00:00Z",
                "window_start_s": 1_800_000_300,
                "market_slug": "btc-updown-5m-1800000300",
                "winning_intent_id": "b1",
                "winning_source_wallet": WALLET_B,
                "winning_source_wallet_short": "0xbbbb...bbbb",
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.40,
                "shares": 10.0,
                "expected_fee_usd": 0.168,
                "extra_would_submit_window": True,
                "realized_paper_outcome": {"status": "UNRESOLVED", "market_slug": "btc-updown-5m-1800000300"},
            },
            {
                "cycle_generated_at": "2026-07-10T07:00:00Z",
                "window_start_s": 1_800_000_000,
                "market_slug": "btc-updown-5m-1800000000",
                "winning_intent_id": "b0",
                "winning_source_wallet": WALLET_B,
                "winning_source_wallet_short": "0xbbbb...bbbb",
                "limit_price": 0.40,
                "shares": 10.0,
                "extra_would_submit_window": True,
            },
        ]
    }

    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=state_a,
        live_probe_result={"rows": []},
        selection_priority_freeze={},
        previous=previous,
        generated_at="2026-07-10T08:00:00Z",
        resolution_map={
            "btc-updown-5m-1800000300": {
                "market_slug": "btc-updown-5m-1800000300",
                "direction": "UP",
                "uma_resolution_status": "RESOLVED",
                "source": "test",
            },
            "btc-updown-5m-1800000000": {
                "market_slug": "btc-updown-5m-1800000000",
                "direction": "DOWN",
                "uma_resolution_status": "RESOLVED",
                "source": "test",
            },
        },
    )

    rows_by_intent = {row["winning_intent_id"]: row for row in report["rows"]}
    rejoined = rows_by_intent["b1"]["realized_paper_outcome"]
    assert rejoined["status"] == "RESOLVED"
    assert rejoined["wins"] is True
    assert rejoined["paper_pnl_usd"] == 6.0
    directionless = rows_by_intent["b0"]["realized_paper_outcome"]
    assert directionless["status"] == "RESOLVED"
    assert directionless["wins"] is None
    assert directionless["paper_pnl_usd"] is None
    assert directionless["measurement_gap_reason"] == "missing_side_or_outcome_on_measurement_row"

    extra = report["summary"]["extra_would_submit_post_fee_measurement"]
    assert extra["unique_windows"] == 2
    assert extra["measured_unique_windows"] == 1
    assert extra["resolved_intents"] == 2
    assert extra["measurable_resolved_intents"] == 1
    assert extra["unmeasured_resolved_intents"] == 1
    assert extra["pre_fee_pnl_usd"] == 6.0
    assert extra["post_fee_pnl_usd"] == 5.832
    assert extra["unresolved_intents"] == 0
    assert extra["pnl_measurement_gap_reasons"] == {"missing_side_or_outcome_on_measurement_row": 1}


def test_routing_shadow_validation_extra_dedupe_prefers_direction_carrying_row_over_legacy() -> None:
    state_a = _state(WALLET_A, observed_ts=20.0, intent_id="a2", window=1_800_000_600)
    previous = {
        "rows": [
            {
                "cycle_generated_at": "2026-07-10T06:55:00Z",
                "window_start_s": 1_800_000_300,
                "market_slug": "btc-updown-5m-1800000300",
                "winning_intent_id": "legacy1",
                "winning_source_wallet": WALLET_B,
                "winning_source_wallet_short": "0xbbbb...bbbb",
                "winning_observed_ts": 1_800_000_100.0,
                "limit_price": 0.40,
                "shares": 10.0,
                "extra_would_submit_window": True,
            },
            {
                "cycle_generated_at": "2026-07-10T07:00:00Z",
                "window_start_s": 1_800_000_300,
                "market_slug": "btc-updown-5m-1800000300",
                "winning_intent_id": "b1",
                "winning_source_wallet": WALLET_B,
                "winning_source_wallet_short": "0xbbbb...bbbb",
                "winning_observed_ts": 1_800_000_302.0,
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.40,
                "shares": 10.0,
                "expected_fee_usd": 0.168,
                "extra_would_submit_window": True,
                "realized_paper_outcome": {"status": "UNRESOLVED", "market_slug": "btc-updown-5m-1800000300"},
            },
        ]
    }

    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=state_a,
        live_probe_result={"rows": []},
        selection_priority_freeze={},
        previous=previous,
        generated_at="2026-07-10T08:00:00Z",
        resolution_map={
            "btc-updown-5m-1800000300": {
                "market_slug": "btc-updown-5m-1800000300",
                "direction": "UP",
                "uma_resolution_status": "RESOLVED",
                "source": "test",
            },
        },
    )

    extra = report["summary"]["extra_would_submit_post_fee_measurement"]
    assert extra["unique_windows"] == 1
    assert extra["measured_unique_windows"] == 1
    assert extra["measurable_resolved_intents"] == 1
    assert extra["pre_fee_pnl_usd"] == 6.0
    assert extra["post_fee_pnl_usd"] == 5.832


def test_routing_shadow_validation_pins_extra_attribution_and_dedupes_literal_rows() -> None:
    state_a = _state(WALLET_A, observed_ts=20.0, intent_id="a2", window=1_800_000_600)
    previous = {
        "summary": {
            "attribution_stability": {
                "snapshots": [
                    {
                        "generated_at": "2026-07-10T07:49:00Z",
                        "measured_unique_windows": 1,
                        "measured_member_split": {WALLET_B: 1},
                        "measured_window_attribution": {"btc-updown-5m-1800000300": WALLET_B},
                    }
                ]
            }
        },
        "rows": [
            {
                "cycle_generated_at": "2026-07-10T07:50:00Z",
                "window_start_s": 1_800_000_300,
                "market_slug": "btc-updown-5m-1800000300",
                "winning_intent_id": "a1",
                "winning_source_wallet": WALLET_A,
                "winning_source_wallet_short": "0xaaaa...aaaa",
                "winning_observed_ts": 1_800_000_100.0004,
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.40,
                "shares": 10.0,
                "expected_fee_usd": 0.168,
                "extra_would_submit_window": True,
            },
            {
                "cycle_generated_at": "2026-07-10T07:51:00Z",
                "window_start_s": 1_800_000_300,
                "market_slug": "btc-updown-5m-1800000300",
                "winning_intent_id": "b1",
                "winning_source_wallet": WALLET_B,
                "winning_source_wallet_short": "0xbbbb...bbbb",
                "winning_observed_ts": 1_800_000_100.0,
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.40,
                "shares": 10.0,
                "expected_fee_usd": 0.168,
                "extra_would_submit_window": True,
            },
            {
                "cycle_generated_at": "2026-07-10T07:52:00Z",
                "window_start_s": 1_800_000_300,
                "market_slug": "btc-updown-5m-1800000300",
                "winning_intent_id": "b1",
                "winning_source_wallet": WALLET_B,
                "winning_source_wallet_short": "0xbbbb...bbbb",
                "winning_observed_ts": 1_800_000_100.0,
                "side": "YES",
                "outcome": "UP",
                "limit_price": 0.40,
                "shares": 10.0,
                "expected_fee_usd": 0.168,
                "extra_would_submit_window": True,
            },
        ],
    }

    report = build_routing_shadow_validation(
        active_set_runtime={
            "selected_member": {"source_wallet": WALLET_A},
            "members": [
                {"source_wallet": WALLET_A, "candidate_id": "candidate_a", "policy_id": "policy_a", "enabled": True},
            ],
        },
        live_execution_state=state_a,
        live_probe_result={"rows": []},
        selection_priority_freeze={},
        previous=previous,
        generated_at="2026-07-10T08:00:00Z",
        resolution_map={
            "btc-updown-5m-1800000300": {
                "market_slug": "btc-updown-5m-1800000300",
                "direction": "UP",
                "uma_resolution_status": "RESOLVED",
                "source": "test",
            },
        },
    )

    assert [
        row
        for row in report["rows"]
        if row["market_slug"] == "btc-updown-5m-1800000300" and row["winning_source_wallet"] == WALLET_B
    ][0]["winning_intent_id"] == "b1"
    assert len(
        [
            row
            for row in report["rows"]
            if row["market_slug"] == "btc-updown-5m-1800000300" and row["winning_source_wallet"] == WALLET_B
        ]
    ) == 1
    assert report["summary"]["attribution_literal_dedupe"]["duplicate_rows_dropped"] == 1
    extra = report["summary"]["extra_would_submit_post_fee_measurement"]
    assert extra["measured_unique_windows"] == 1
    assert extra["by_member"][WALLET_B]["measured_unique_windows"] == 1
    assert WALLET_A not in extra["by_member"]
    assert extra["post_fee_pnl_usd"] == 5.832
    stability = report["summary"]["attribution_stability"]
    assert stability["status"] == "PASS"
    assert stability["common_measured_windows"] == 1
    assert stability["changed_window_count"] == 0
