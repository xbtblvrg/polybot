from __future__ import annotations

from scripts.run_inventory_convergence_skip_paper_lane import build_state


def test_inventory_convergence_skip_scales_positive_post_fee_to_probe_cap() -> None:
    state = build_state(
        guard_state={
            "window_participation": {
                "rows": [
                    {
                        "current_set_generation": True,
                        "dominant_skip_reason": "inventory_target_already_met",
                        "market_slug": "btc-updown-5m-1",
                        "window_start_s": 1,
                        "source_wallet": "0xe6db",
                        "wallet_eligible_orders": 2,
                        "our_fills": 1,
                        "our_submits": 1,
                        "outcome": "Up",
                    },
                    {
                        "current_set_generation": True,
                        "dominant_skip_reason": "inventory_target_already_met",
                        "market_slug": "btc-updown-5m-2",
                        "window_start_s": 2,
                        "source_wallet": "0xe6db",
                        "wallet_eligible_orders": 1,
                        "our_fills": 0,
                        "our_submits": 0,
                        "outcome": "Down",
                    },
                    {
                        "current_set_generation": False,
                        "dominant_skip_reason": "inventory_target_already_met",
                        "market_slug": "btc-updown-5m-old",
                        "window_start_s": 3,
                    },
                ]
            }
        },
        routing_shadow={
            "rows": [
                {
                    "market_slug": "btc-updown-5m-1",
                    "window_start_s": 1,
                    "winning_intent_id": "ci_positive",
                    "winning_policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "limit_price": 0.5,
                    "shares": 5.0,
                    "expected_fee_usd": 0.1,
                    "realized_paper_outcome": {
                        "status": "RESOLVED",
                        "paper_pnl_usd": 2.5,
                        "wins": True,
                    },
                },
                {
                    "market_slug": "btc-updown-5m-2",
                    "window_start_s": 2,
                    "winning_intent_id": "ci_negative",
                    "winning_policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "limit_price": 0.5,
                    "shares": 5.0,
                    "expected_fee_usd": 0.1,
                    "realized_paper_outcome": {
                        "status": "RESOLVED",
                        "paper_pnl_usd": -2.5,
                        "wins": False,
                    },
                },
            ]
        },
        probe_cap_usd=1.0,
    )

    summary = state["summary"]
    assert summary["inventory_target_already_met_windows"] == 2
    assert summary["routing_shadow_joined_windows"] == 2
    assert summary["would_submit_windows"] == 2
    assert summary["positive_post_fee_windows"] == 1
    assert summary["recoverable_positive_windows"] == 1
    assert summary["source_order_gap_positive_units"] == 1
    assert summary["positive_probe_cap_post_fee_pnl_usd"] == 0.96
    assert summary["recoverable_probe_cap_post_fee_pnl_usd"] == 0.96
    assert state["rows"][1]["post_fee_measurement"]["probe_cap_scale"] == 0.4
    assert state["paper_only"] is True
    assert state["live_orders_allowed"] is False
