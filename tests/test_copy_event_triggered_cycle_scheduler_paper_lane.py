from datetime import datetime, timezone

from scripts.run_copy_event_triggered_cycle_scheduler_paper_lane import build_state


def test_paper_lane_carries_replay_seed_and_preserves_invariants():
    replay = {
        "status": "PASS_PAPER_SEED_NEXT",
        "summary": {"recovered_source_active_but_no_fresh_cycle_overlap_windows": 1},
        "rows": [
            {
                "counterfactual_fresh_overlap_recovered": True,
                "candidate_id": "candidate_a",
                "source_wallet": "0xabc",
                "market_slug": "btc-updown-5m-1784073600",
                "first_policy_received_iso": "2026-07-15T00:01:00Z",
                "counterfactual_trigger_at_iso": "2026-07-15T00:01:00Z",
                "source_rows": 2,
            }
        ],
    }

    state = build_state(
        replay=replay,
        history={"events": []},
        guard_state={"active_set_runtime": {"members": []}},
        guard_cycles=[],
        previous_state={},
        clock_start=datetime(2026, 7, 15, 1, 45, tzinfo=timezone.utc),
        clock_hours=48.0,
        fresh_horizon_s=30.0,
        max_price=0.5,
        sample_limit=10,
        generated_at=datetime(2026, 7, 15, 2, 0, tzinfo=timezone.utc),
    )

    assert state["paper_only"] is True
    assert state["live_orders_allowed"] is False
    assert state["copyintent_parity_change"] is False
    assert state["single_submitter_change"] is False
    assert state["guard_code_touched"] is False
    assert state["summary"]["replay_recovered_windows"] == 1
    assert state["replay_seed_rows"][0]["row_type"] == "replay_seed_recovered_window"


def test_paper_lane_marks_no_fresh_cycle_event_as_recovered_candidate():
    history = {
        "events": [
            {
                "action": "BUY",
                "asset": "BTC",
                "duration": "5m",
                "row_type": "trade",
                "event_id": "we_1",
                "event_ts": 1784073610.0,
                "observed_ts": 1784073610.0,
                "market_slug": "btc-updown-5m-1784073600",
                "outcome": "Up",
                "price": 0.45,
                "source_wallet": "0xabc",
                "transaction_hash": "0x1",
            }
        ]
    }
    guard_state = {
        "active_set_runtime": {
            "members": [{"source_wallet": "0xabc", "candidate_id": "candidate_a"}]
        }
    }
    guard_cycles = [
        {
            "cycle_started_at": datetime(2026, 7, 15, 0, 2, 0, tzinfo=timezone.utc),
            "source_wallet": "0xabc",
            "candidate_id": "candidate_a",
            "cycle": 1,
            "pid": 1,
        }
    ]

    state = build_state(
        replay={"status": "PASS_PAPER_SEED_NEXT", "summary": {}, "rows": []},
        history=history,
        guard_state=guard_state,
        guard_cycles=guard_cycles,
        previous_state={},
        clock_start=datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc),
        clock_hours=48.0,
        fresh_horizon_s=30.0,
        max_price=0.5,
        sample_limit=10,
        generated_at=datetime(2026, 7, 15, 0, 3, tzinfo=timezone.utc),
    )

    assert state["summary"]["paper_clock_recovered_candidates"] == 1
    assert state["summary"]["paper_clock_rows_landed"] == 1
    assert state["summary"]["paper_clock_rows_resolved"] == 0
    assert state["summary"]["paper_clock_post_fee_would_pnl_usd"] == 0.0
    assert state["rows"][0]["candidate_id"] == "candidate_a"
    assert state["rows"][0]["next_actual_cycle_lag_s"] == 110.0
    assert state["rows"][0]["post_fee_would_pnl_status"] == "PENDING_RESOLUTION_OR_JOIN"


def test_paper_lane_scores_resolved_recovered_candidate():
    history = {
        "events": [
            {
                "action": "BUY",
                "asset": "BTC",
                "duration": "5m",
                "row_type": "trade",
                "event_id": "we_2",
                "event_ts": 1784073610.0,
                "observed_ts": 1784073610.0,
                "market_slug": "btc-updown-5m-1784073600",
                "outcome": "Up",
                "price": 0.5,
                "source_wallet": "0xabc",
                "transaction_hash": "0x2",
            }
        ]
    }
    guard_state = {
        "active_set_runtime": {
            "members": [{"source_wallet": "0xabc", "candidate_id": "candidate_a"}]
        }
    }
    resolutions = {
        "slug_start:1784073600": {
            "direction": "UP",
            "expiry_unix_ts": 1784073900,
            "source": "polymarket_gamma_resolved_outcome",
            "research_only": False,
            "window_type": "5m",
        }
    }

    state = build_state(
        replay={"status": "PASS_PAPER_SEED_NEXT", "summary": {}, "rows": []},
        history=history,
        guard_state=guard_state,
        guard_cycles=[],
        resolutions=resolutions,
        previous_state={},
        clock_start=datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc),
        clock_hours=48.0,
        fresh_horizon_s=30.0,
        max_price=0.5,
        paper_order_size_usd=1.0,
        sample_limit=10,
        generated_at=datetime(2026, 7, 15, 0, 8, tzinfo=timezone.utc),
    )

    assert state["status"] == "PAPER_CLOCK_POSITIVE_ACCRUING"
    assert state["summary"]["resolved_measured_rows"] == 1
    assert state["summary"]["resolved_measured_windows"] == 1
    assert state["summary"]["resolved_positive_rows"] == 1
    assert state["summary"]["resolved_positive_windows"] == 1
    assert state["summary"]["aggregate_post_fee_would_pnl_usd"] == 1.0
    assert state["summary"]["paper_clock_rows_landed"] == 1
    assert state["summary"]["paper_clock_rows_resolved"] == 1
    assert state["summary"]["paper_clock_post_fee_would_pnl_usd"] == 1.0
    assert state["rows"][0]["post_fee_would_pnl_status"] == "RESOLVED_POST_FEE_MEASURED"
    assert state["rows"][0]["post_fee_would_pnl_usd"] == 1.0
    assert state["resolved_sample_rows"][0]["post_fee_would_pnl_usd"] == 1.0


def _history_event(event_id: str, tx: str) -> dict:
    return {
        "action": "BUY",
        "asset": "BTC",
        "duration": "5m",
        "row_type": "trade",
        "event_id": event_id,
        "event_ts": 1784073610.0,
        "observed_ts": 1784073610.0,
        "market_slug": "btc-updown-5m-1784073600",
        "outcome": "Up",
        "price": 0.5,
        "source_wallet": "0xabc",
        "transaction_hash": tx,
    }


_GUARD_STATE = {
    "active_set_runtime": {
        "members": [{"source_wallet": "0xabc", "candidate_id": "candidate_a"}]
    }
}

_RESOLUTIONS = {
    "slug_start:1784073600": {
        "direction": "UP",
        "expiry_unix_ts": 1784073900,
        "source": "polymarket_gamma_resolved_outcome",
        "research_only": False,
        "window_type": "5m",
    }
}


def _build(history_events, previous_state, resolutions, generated_at):
    return build_state(
        replay={"status": "PASS_PAPER_SEED_NEXT", "summary": {}, "rows": []},
        history={"events": history_events},
        guard_state=_GUARD_STATE,
        guard_cycles=[],
        resolutions=resolutions,
        previous_state=previous_state,
        clock_start=datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc),
        clock_hours=48.0,
        fresh_horizon_s=30.0,
        max_price=0.5,
        paper_order_size_usd=1.0,
        sample_limit=10,
        generated_at=generated_at,
    )


def test_paper_clock_accumulator_survives_history_rollout():
    first = _build(
        [_history_event("we_a", "0xa")],
        {},
        {},
        datetime(2026, 7, 15, 0, 3, tzinfo=timezone.utc),
    )
    assert first["summary"]["paper_clock_rows_landed"] == 1
    assert first["summary"]["paper_clock_rows_resolved"] == 0
    assert "we_a" in first["paper_clock_accumulator"]

    # Event ages out of the rolling history; landed count must not shrink.
    second = _build(
        [],
        first,
        {},
        datetime(2026, 7, 15, 0, 30, tzinfo=timezone.utc),
    )
    assert second["summary"]["paper_clock_rows_landed"] == 1
    assert second["summary"]["paper_clock_recovered_candidates"] == 0
    assert second["summary"]["paper_clock_accumulation_started_at"] == "2026-07-15T00:03:00Z"


def test_paper_clock_accumulator_scores_aged_out_pending_row():
    first = _build(
        [_history_event("we_b", "0xb")],
        {},
        {},
        datetime(2026, 7, 15, 0, 3, tzinfo=timezone.utc),
    )
    assert first["summary"]["paper_clock_rows_resolved"] == 0
    assert "paper_order" in first["paper_clock_accumulator"]["we_b"]

    # Resolution arrives only after the event left the rolling history.
    second = _build(
        [],
        first,
        _RESOLUTIONS,
        datetime(2026, 7, 15, 1, 0, tzinfo=timezone.utc),
    )
    entry = second["paper_clock_accumulator"]["we_b"]
    assert entry["post_fee_would_pnl_status"] == "RESOLVED_POST_FEE_MEASURED"
    assert entry["post_fee_would_pnl_usd"] == 1.0
    assert "paper_order" not in entry
    assert second["summary"]["paper_clock_rows_resolved"] == 1
    assert second["summary"]["paper_clock_post_fee_would_pnl_usd"] == 1.0
    assert second["summary"]["paper_clock_resolved_positive_windows"] == 1
    assert second["status"] == "PAPER_CLOCK_POSITIVE_ACCRUING"


def test_paper_clock_accumulator_keeps_resolved_rows_stable():
    first = _build(
        [_history_event("we_c", "0xc")],
        {},
        _RESOLUTIONS,
        datetime(2026, 7, 15, 0, 8, tzinfo=timezone.utc),
    )
    assert first["summary"]["paper_clock_rows_resolved"] == 1

    # Re-scan of the same event without resolutions must not un-resolve it.
    second = _build(
        [_history_event("we_c", "0xc")],
        first,
        {},
        datetime(2026, 7, 15, 0, 20, tzinfo=timezone.utc),
    )
    assert second["summary"]["paper_clock_rows_landed"] == 1
    assert second["summary"]["paper_clock_rows_resolved"] == 1
    assert second["summary"]["paper_clock_post_fee_would_pnl_usd"] == 1.0
