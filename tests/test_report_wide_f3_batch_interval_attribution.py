import json

from scripts.report_wide_f3_batch_interval_attribution import (
    build_report,
    load_instrumentation_events,
)


WALLET = "0x0000000000000000000000000000000000000001"


def _row(
    *,
    terminal: str,
    cycle: str,
    event_id: str,
    start: float,
    received: float,
    first_received: float,
    ordinal: int = 0,
    run_id: str = "run-1",
    cohort_id: str = "cohort-1",
    manifest_id: str = "manifest-1",
    provenance: str = "capture_prefetched",
    recorded_at: str = "2026-07-30T00:00:00Z",
) -> dict:
    return {
        "attempt_id": f"{run_id}:{event_id}",
        "source_event_id": event_id,
        "run_id": run_id,
        "cohort_id": cohort_id,
        "manifest_id": manifest_id,
        "wallet": WALLET,
        "token_id": "token",
        "recorded_at": recorded_at,
        "fetch_instrumentation_schema_version": 2,
        "fetch_provenance": provenance,
        "fetch_cycle_id": cycle,
        "fetch_started_monotonic_s": start,
        "fetch_started_monotonic_observed": True,
        "recv_monotonic_s": received,
        "token_cycle_first_recv_monotonic_s": first_received,
        "token_event_ordinal_in_cycle": ordinal,
        "f1_f4_terminal": {
            "F2_alpha_profile": "PASS",
            "terminal": terminal,
        },
    }


def _reachability() -> dict:
    return {
        "summary": {
            "dual_gate_winners_after": 0,
            "winner_wallets_after": [],
            "input_equals_terminal": True,
            "input_rows": 3,
            "terminal_rows": 3,
        }
    }


def test_instrumentation_window_accrues_without_early_decision() -> None:
    rows = [
        _row(
            terminal="REFUSED_STALE_RECEIPT_TO_FETCH",
            cycle="multi",
            event_id="one",
            ordinal=0,
            start=20.0,
            received=10.0,
            first_received=10.0,
        ),
        _row(
            terminal="COPYABLE_EXACT_POLICY_PAPER_FILL",
            cycle="multi",
            event_id="two",
            ordinal=1,
            start=20.0,
            received=19.8,
            first_received=10.0,
        ),
    ]
    report = build_report(
        rows,
        _reachability(),
        now_s=1785369600.0 + 3600.0,
    )

    assert report["summary"]["batch_interval_exposed_f3_share_pct"] == 100.0
    assert report["summary"]["decision_branch"] == "ACCRUING_STEP1B_CONTROL_WINDOW"
    assert report["summary"]["decision_is_binding"] is False
    assert report["raw_intervals"]["event_recv_to_fetch_start"]["attempts"] == 2
    assert (
        report["raw_intervals"]["cycle_first_token_event_to_fetch_start"]["max_ms"]
        == 10000.0
    )
    assert report["copyable_rate_threshold_pct_unchanged"] == 70.0
    assert report["f3_lag_limit_s_unchanged"] == 5.0
    assert report["paper_only"] is True
    assert report["live_orders_allowed"] is False


def test_complete_window_falsifies_q_when_control_rates_match_despite_gate() -> None:
    rows = [
        _row(
            terminal=(
                "REFUSED_STALE_RECEIPT_TO_FETCH"
                if index < 120
                else "COPYABLE_EXACT_POLICY_PAPER_FILL"
            ),
            cycle=f"single-{index}",
            event_id=f"single-{index}",
            start=20.0 if index < 120 else 10.2,
            received=10.0,
            first_received=10.0,
        )
        for index in range(150)
    ]
    rows.extend(
        _row(
            terminal=(
                "REFUSED_STALE_RECEIPT_TO_FETCH"
                if index < 1080
                else "COPYABLE_EXACT_POLICY_PAPER_FILL"
            ),
            cycle=f"multi-{index // 2}",
            event_id=f"multi-{index}",
            ordinal=index % 2,
            start=20.0 if index < 1080 else 10.2,
            received=10.0 + (index % 2),
            first_received=10.0,
        )
        for index in range(1350)
    )
    report = build_report(
        rows,
        _reachability(),
        now_s=1785369600.0 + 90000.0,
    )

    assert report["instrumentation_completeness"]["window_complete"] is True
    assert report["summary"]["batch_interval_exposed_f3_share_pct"] == 90.0
    assert report["summary"]["batch_attribution_gate_pass"] is True
    assert report["single_event_control"]["test_eligible"] is True
    assert report["single_event_control"]["statistically_indistinguishable"] is True
    assert report["summary"]["falsifier_hit"] is True
    assert report["summary"]["decision_branch"] == (
        "FALSIFY_DEFECT_Q_SINGLE_EVENT_CONTROL"
    )


def test_complete_window_opens_q_when_control_rates_differ() -> None:
    rows = [
        _row(
            terminal="COPYABLE_EXACT_POLICY_PAPER_FILL",
            cycle=f"single-{index}",
            event_id=f"single-{index}",
            start=10.2,
            received=10.0,
            first_received=10.0,
        )
        for index in range(150)
    ]
    rows.extend(
        _row(
            terminal="REFUSED_STALE_RECEIPT_TO_FETCH",
            cycle=f"multi-{index // 2}",
            event_id=f"multi-{index}",
            ordinal=index % 2,
            start=20.0,
            received=10.0 + (index % 2),
            first_received=10.0,
        )
        for index in range(1350)
    )
    report = build_report(
        rows,
        _reachability(),
        now_s=1785369600.0 + 90000.0,
    )

    assert report["summary"]["batch_interval_exposed_f3_share_pct"] == 100.0
    assert report["single_event_control"]["statistically_indistinguishable"] is False
    assert report["summary"]["falsifier_hit"] is False
    assert report["summary"]["decision_branch"] == (
        "OPEN_DEFECT_Q_ARCHITECTURE_INDUCED_STALENESS"
    )
    assert report["summary"]["dual_gate_winners_current"] == 0
    assert report["summary"]["winner_wallets_current"] == []


def test_run_id_rotation_retains_elapsed_window() -> None:
    old = _row(
        terminal="COPYABLE_EXACT_POLICY_PAPER_FILL",
        cycle="old-cycle",
        event_id="old",
        start=10.2,
        received=10.0,
        first_received=10.0,
        run_id="old-run",
        cohort_id="old-cohort",
        manifest_id="old-manifest",
        recorded_at="2026-07-30T00:00:00Z",
    )
    new = _row(
        terminal="COPYABLE_EXACT_POLICY_PAPER_FILL",
        cycle="new-cycle",
        event_id="new",
        start=20.2,
        received=20.0,
        first_received=20.0,
        run_id="new-run",
        cohort_id="new-cohort",
        manifest_id="new-manifest",
        recorded_at="2026-07-30T01:00:00Z",
    )
    report = build_report(
        [old, new],
        _reachability(),
        now_s=1785369600.0 + 7200.0,
    )

    assert report["instrumentation_completeness"]["run_ids_retained"] == [
        "new-run",
        "old-run",
    ]
    assert report["instrumentation_completeness"]["cohort_ids_retained"] == [
        "new-cohort",
        "old-cohort",
    ]
    assert report["instrumentation_completeness"]["manifest_ids_retained"] == [
        "new-manifest",
        "old-manifest",
    ]
    assert report["instrumentation_completeness"]["elapsed_s"] == 7200.0


def test_defaulted_or_nonpositive_timestamps_are_excluded() -> None:
    invalid = _row(
        terminal="REFUSED_STALE_RECEIPT_TO_FETCH",
        cycle="cycle",
        event_id="event",
        start=20.0,
        received=0.0,
        first_received=0.0,
    )
    invalid["fetch_started_monotonic_observed"] = False
    report = build_report(
        [invalid],
        _reachability(),
        now_s=1785369600.0,
    )

    assert report["instrumentation_completeness"]["f2_pass_rows_total"] == 1
    assert report["instrumentation_completeness"]["instrumented_f2_pass_rows"] == 0
    assert report["summary"]["decision_branch"] == "ACCRUING_STEP1B_CONTROL_WINDOW"


def test_completed_window_without_power_is_not_binding() -> None:
    rows = [
        _row(
            terminal="REFUSED_STALE_RECEIPT_TO_FETCH",
            cycle=f"multi-{index // 2}",
            event_id=f"event-{index}",
            ordinal=index % 2,
            start=20.0,
            received=10.0,
            first_received=10.0,
        )
        for index in range(1500)
    ]
    report = build_report(
        rows,
        _reachability(),
        now_s=1785369600.0 + 90000.0,
    )

    assert report["instrumentation_completeness"]["window_complete"] is True
    assert report["single_event_control"]["test_eligible"] is False
    assert report["summary"]["decision_branch"] == "ACCRUING_STEP1B_CONTROL_POWER"
    assert report["summary"]["decision_is_binding"] is False


def test_append_only_reader_dedupes_attempt_id_across_rotations(tmp_path) -> None:
    old = _row(
        terminal="REFUSED_STALE_RECEIPT_TO_FETCH",
        cycle="old",
        event_id="old",
        start=20.0,
        received=10.0,
        first_received=10.0,
        run_id="old-run",
        cohort_id="old-cohort",
        manifest_id="old-manifest",
    )
    duplicate = dict(old, recorded_at="2026-07-30T00:01:00Z")
    new = _row(
        terminal="COPYABLE_EXACT_POLICY_PAPER_FILL",
        cycle="new",
        event_id="new",
        start=20.2,
        received=20.0,
        first_received=20.0,
        run_id="new-run",
        cohort_id="new-cohort",
        manifest_id="new-manifest",
    )
    path = tmp_path / "events.jsonl"
    path.write_text(
        "".join(json.dumps(row) + "\n" for row in [old, duplicate, new])
    )

    loaded = load_instrumentation_events(path)

    assert len(loaded) == 2
    retained_old = next(row for row in loaded if row["attempt_id"] == old["attempt_id"])
    assert retained_old["recorded_at"] == "2026-07-30T00:01:00Z"
