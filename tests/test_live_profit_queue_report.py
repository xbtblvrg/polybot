from pathlib import Path

from scripts.report_live_profit_queue import build_reject_taxonomy, build_report, build_scaling_gate


def _scorecard() -> dict:
    return {
        "generated_at": "2026-07-09T07:04:58Z",
        "day_utc": "2026-07-09",
        "late_window_cohort": {
            "rows": [
                {
                    "market_slug": "btc-updown-5m-1",
                    "submitted_at": "2026-07-09T00:07:58Z",
                    "seconds_to_close_s": 121.0,
                    "resolved": True,
                    "pnl_usd": -2.0,
                },
                {
                    "market_slug": "btc-updown-5m-2",
                    "submitted_at": "2026-07-09T01:55:16Z",
                    "seconds_to_close_s": 283.0,
                    "resolved": True,
                    "pnl_usd": 3.0,
                },
                {
                    "market_slug": "btc-updown-5m-3",
                    "submitted_at": "2026-07-09T02:05:25Z",
                    "seconds_to_close_s": 274.0,
                    "resolved": True,
                    "pnl_usd": -2.45,
                },
            ]
        },
        "canonical_pnl_truth": {
            "events": [
                {
                    "market_slug": "btc-updown-5m-1",
                    "submitted_at": "2026-07-09T00:07:58Z",
                    "status": "FILLED",
                    "cost_usd": 2.0,
                    "pnl_usd": -2.0,
                },
                {
                    "market_slug": "btc-updown-5m-2",
                    "submitted_at": "2026-07-09T01:55:16Z",
                    "status": "FILLED",
                    "cost_usd": 2.0,
                    "pnl_usd": 3.0,
                },
                {
                    "market_slug": "btc-updown-5m-3",
                    "submitted_at": "2026-07-09T02:05:25Z",
                    "status": "FILLED",
                    "cost_usd": 2.45,
                    "pnl_usd": -2.45,
                },
            ]
        },
        "volume_kpi": {
            "rows": [
                {
                    "market_slug": "btc-updown-5m-old",
                    "window_start_s": 1783574100.0,
                    "missed_window_attribution": "guard_reject",
                    "our_submits": 0,
                    "our_fills": 0,
                    "wallet_eligible_orders": 15,
                    "skip_reasons": {"inventory_best_ask_missing": 1},
                },
                {
                    "market_slug": "btc-updown-5m-1783577700",
                    "window_start_s": 1783577700.0,
                    "missed_window_attribution": "guard_reject",
                    "our_submits": 0,
                    "our_fills": 0,
                    "wallet_eligible_orders": 4,
                    "skip_reasons": {"window_time_gte_180s": 1},
                },
                {
                    "market_slug": "btc-updown-5m-1783578900",
                    "window_start_s": 1783578900.0,
                    "missed_window_attribution": "guard_reject",
                    "our_submits": 0,
                    "our_fills": 0,
                    "wallet_eligible_orders": 17,
                    "skip_reasons": {"window_time_gte_180s": 1},
                },
            ]
        },
    }


def test_scaling_gate_uses_canonical_early_entry_bucket() -> None:
    gate = build_scaling_gate(_scorecard())

    assert gate["sample_n"] == 2
    assert gate["pnl_usd"] == 0.55
    assert gate["cost_usd"] == 4.45
    assert gate["verdict"] == "NO_RAISE_SAMPLE_FLOOR_NOT_MET"
    assert gate["action"] == "DO_NOT_RAISE"
    assert gate["live_guard_reload_required"] is False


def test_reject_taxonomy_scopes_after_latest_order_and_classifies_freshness() -> None:
    deadman = {
        "latest_order_ts": "2026-07-09T06:10:02Z",
        "fresh_stale_signal_rows": 21,
        "approved_suppression_events": 21,
        "approved_suppression_tags": ["window_time_gte_180s"],
    }

    taxonomy = build_reject_taxonomy(_scorecard(), deadman, {})

    assert taxonomy["window_count"] == 2
    assert taxonomy["wallet_eligible_orders"] == 21
    assert taxonomy["eligible_order_weighted_gate_counts"] == {"freshness": 21.0}
    assert taxonomy["freshness_weighted_pct"] == 100.0
    assert taxonomy["verdict"] == "EXPECTED_THIN_FLOW"
    assert taxonomy["gate_action"] == "NO_GATE_CHANGE"


def test_build_report_marks_no_live_path_change_when_sample_floor_unmet() -> None:
    report = build_report(_scorecard(), {}, {}, scorecard_path=Path("scorecard.json"))

    assert report["kind"] == "wallet_copy_live_profit_queue_report"
    assert report["p1d_scaling"]["action"] == "DO_NOT_RAISE"
    assert report["live_path_change"] is False
