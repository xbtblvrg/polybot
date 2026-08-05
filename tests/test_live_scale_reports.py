from scripts.report_early_window_drip_bucket import build_report as build_early_window_report
from scripts.report_reject_taxonomy import build_report as build_reject_taxonomy, classify_reason


def test_early_window_bucket_requires_ten_resolved_fills_before_raise() -> None:
    ledger = {
        "orders": [
            {
                "order_id": "a",
                "status": "FILLED",
                "final_status": "FILLED",
                "submitted_at": "2026-07-09T01:55:15+00:00",
                "market_slug": "btc-updown-5m-1783562100",
                "side": "NO",
                "filled_size_usd": 2.0,
                "filled_shares": 5.0,
            },
            {
                "order_id": "b",
                "status": "FILLED",
                "final_status": "FILLED",
                "submitted_at": "2026-07-09T01:56:05+00:00",
                "market_slug": "btc-updown-5m-1783562100",
                "side": "NO",
                "filled_size_usd": 2.0,
                "filled_shares": 5.0,
            },
        ]
    }
    resolutions = {"slug_start:1783562100": {"direction": "DOWN"}}

    report = build_early_window_report(ledger, resolutions)

    assert report["metrics"]["resolved_fills"] == 1
    assert report["metrics"]["pnl_usd"] == 3.0
    assert report["metrics"]["raise_gate_pass"] is False
    assert report["metrics"]["verdict"] == "NO_RAISE_WAIT_FOR_N"


def test_reject_taxonomy_classifies_freshness_vs_envelope() -> None:
    guard_state = {
        "window_participation": {
            "window_rollups": [
                {
                    "market_slug": "btc-updown-5m-1",
                    "source_wallet": "0xabc",
                    "wallet_eligible_orders": 3,
                    "our_attempts": 0,
                    "our_submits": 0,
                    "our_fills": 0,
                    "missed_active_window": True,
                    "dominant_skip_reason_counts": {"window_time_gte_180s": 3},
                },
                {
                    "market_slug": "btc-updown-5m-2",
                    "source_wallet": "0xdef",
                    "wallet_eligible_orders": 1,
                    "our_attempts": 1,
                    "our_submits": 0,
                    "our_fills": 0,
                    "dominant_skip_reason_counts": {"inventory_best_ask_missing": 1},
                    "outcomes": ["Down"],
                    "first_seen_at": "2026-07-09T00:01:00+00:00",
                    "last_seen_at": "2026-07-09T00:01:30+00:00",
                },
            ]
        }
    }
    ledger = {"orders": [{"status": "REJECTED", "submitted_at": "x", "market_slug": "m"}]}
    resolutions = {"btc-updown-5m-2": {"direction": "DOWN"}}
    history_state = {
        "events": [
            {
                "market_slug": "btc-updown-5m-2",
                "source_wallet": "0xdef",
                "outcome": "Down",
                "price": 0.4,
                "observed_ts": 1783555270,
            }
        ]
    }

    report = build_reject_taxonomy(
        guard_state,
        ledger,
        resolutions=resolutions,
        history_state=history_state,
    )

    assert classify_reason("window_time_gte_180s") == "freshness"
    assert classify_reason("inventory_best_ask_missing") == "envelope"
    assert report["summary"]["all_reject_or_skip_windows"] == 2
    assert report["summary"]["missed_active_windows"] == 1
    assert report["summary"]["ledger_rejects_total"] == 1
    assert report["summary"]["freshness_taxonomy_pct"] == 75.0
    counterfactuals = report["summary"]["best_ask_missing_counterfactuals"]
    assert counterfactuals["sample_n"] == 1
    assert counterfactuals["resolved_n"] == 1
    assert counterfactuals["would_have_won_n"] == 1
    assert counterfactuals["counterfactual_pnl_usd_per_1usd"] == 1.5
    assert counterfactuals["reopen_bar_pass"] is False
    assert report["rows"][1]["best_ask_missing_counterfactual"]["limit_price"] == 0.4
