from __future__ import annotations

from scripts.run_wallet_copy_live_guard import _stdout_cycle_summary


def test_stdout_cycle_summary_keeps_verbose_rows_out_of_stdout() -> None:
    payload = {
        "_state_path": "state.json",
        "generated_at": "2026-07-07T00:00:00+00:00",
        "status": "LIVE_GUARD_RUNNING",
        "cycle_outcome": "LIVE_GUARD_RUNNING",
        "cycle": 7,
        "pid": 123,
        "candidate_id": "candidate",
        "source_wallet": "0xabc",
        "policy_id": "policy",
        "live_orders_allowed": True,
        "paper_only": False,
        "blockers": [],
        "recent_cycle_counts": {"submitted_cycles": 0},
        "window_participation": {
            "active_windows": 12,
            "missed_active_windows": 11,
            "consecutive_missed_active_windows": 11,
            "incident_triggered": True,
            "rows": [{"large": "row"}],
            "dominant_skip_reason_counts": {"window_time_gte_180s": 1},
        },
        "live_execution": {"status": "LIVE_ARMED_NO_FRESH_INTENTS", "orders_submitted": 0},
        "drought_funnel": {"diagnosis": "signal_or_intent_build_drought"},
    }

    summary = _stdout_cycle_summary(payload)

    assert summary["kind"] == "wallet_copy_live_guard_stdout_summary"
    assert summary["full_state_path"] == "state.json"
    assert "rows" not in summary["window_participation"]
    assert summary["window_participation"]["incident_triggered"] is True
