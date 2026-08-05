from scripts.report_copy_event_triggered_cycle_scheduler_verdict import build_verdict_packet


def _state(*, pnl: float = 2.0, dirty_flag: bool = False, summary_pnl: float | None = None) -> dict:
    summary_pnl = pnl if summary_pnl is None else summary_pnl
    return {
        "kind": "copy_event_triggered_cycle_scheduler_paper_lane",
        "generated_at": "2026-07-17T12:31:00Z",
        "status": "PAPER_CLOCK_POSITIVE_ACCRUING",
        "paper_only": True,
        "live_orders_allowed": False,
        "guard_code_touched": dirty_flag,
        "single_submitter_change": False,
        "copyintent_parity_change": False,
        "clock_start_utc": "2026-07-15T01:45:00Z",
        "clock_end_utc": "2026-07-17T01:45:00Z",
        "paper_clock_accumulation_started_at": "2026-07-15T15:45:00Z",
        "summary": {
            "clock_complete": True,
            "paper_clock_recovered_candidates": 2,
            "paper_clock_rows_landed": 2,
            "paper_clock_rows_resolved": 2,
            "paper_clock_post_fee_would_pnl_usd": summary_pnl,
            "paper_clock_resolved_positive_rows": 1 if pnl > 0 else 0,
            "paper_clock_resolved_windows": 2,
            "paper_clock_resolved_positive_windows": 1 if pnl > 0 else 0,
            "aggregate_post_fee_would_pnl_usd": -100.0,
            "rolling_scan_basis": "context only",
        },
        "paper_clock_accumulator": {
            "win": {
                "row_type": "paper_clock_recovered_candidate",
                "event_id": "we_win",
                "market_slug": "btc-updown-5m-1784073600",
                "post_fee_would_pnl_status": "RESOLVED_POST_FEE_MEASURED",
                "post_fee_would_pnl_usd": 3.0,
            },
            "loss": {
                "row_type": "paper_clock_recovered_candidate",
                "event_id": "we_loss",
                "market_slug": "btc-updown-5m-1784073900",
                "post_fee_would_pnl_status": "RESOLVED_POST_FEE_MEASURED",
                "post_fee_would_pnl_usd": pnl - 3.0,
            },
        },
    }


def test_scheduler_verdict_passes_when_accumulator_positive_and_invariants_clean() -> None:
    packet = build_verdict_packet(_state(pnl=2.0), generated_at="2026-07-17T12:40:00Z")

    assert packet["summary"]["verdict"] == "PASS_PREAUTHORIZED_LIVE_PROMOTION"
    assert packet["summary"]["gate_pass"] is True
    assert packet["gate"]["paper_clock_post_fee_would_pnl_usd"] == 2.0
    assert packet["gate"]["summary_matches_accumulator"] is True
    assert packet["gate"]["recovered_candidates_context"] == 2
    assert packet["context_not_gate"]["aggregate_post_fee_would_pnl_usd"] == -100.0
    assert packet["invariants"]["clean"] is True
    assert packet["promotion"]["pre_authorized_by_ruling21b"] is True
    assert packet["clock"]["accumulation_late_start_h"] == 14.0
    assert packet["clock"]["accumulation_observed_hours_until_clock_end"] == 34.0


def test_scheduler_verdict_fails_on_summary_accumulator_mismatch() -> None:
    packet = build_verdict_packet(
        _state(pnl=2.0, summary_pnl=99.0),
        generated_at="2026-07-17T12:40:00Z",
    )

    assert packet["summary"]["gate_pass"] is False
    assert packet["summary"]["verdict"] == "FAIL_NO_LIVE_PROMOTION"
    assert packet["gate"]["summary_matches_accumulator"] is False
    assert packet["gate"]["summary_mismatches"]["paper_clock_post_fee_would_pnl_usd"] == {
        "summary": 99.0,
        "accumulator": 2.0,
    }


def test_scheduler_verdict_fails_on_dirty_invariant() -> None:
    packet = build_verdict_packet(_state(pnl=2.0, dirty_flag=True), generated_at="2026-07-17T12:40:00Z")

    assert packet["summary"]["gate_pass"] is False
    assert packet["invariants"]["clean"] is False
    assert packet["promotion"]["pre_authorized_by_ruling21b"] is False
