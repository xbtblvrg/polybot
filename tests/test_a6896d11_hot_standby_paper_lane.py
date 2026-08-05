from __future__ import annotations

from scripts.run_a6896d11_hot_standby_paper_lane import DEFAULT_WALLET, build_state


def test_a6896d11_hot_standby_lane_preserves_paper_only_readiness() -> None:
    state = build_state(
        ready_shadow_state={
            "lanes": [
                {
                    "wallet": DEFAULT_WALLET,
                    "shadow_status": "WATCH_TIER_READMITTED_POST_FEE_PENDING",
                    "paper_policy_id": "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window",
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "hot_standby_ready": True,
                    "succession_eligible": True,
                    "readiness_verdict": "HOT_STANDBY_READY",
                    "in_lane_post_fee_pnl_usd": 28.082886,
                    "resolved_paper_fills": 235,
                    "resolved_fill_gap": 0,
                    "paper_orders": 559,
                    "source_liveness": {
                        "status": "PASS",
                        "last_trade_age_h": 0.1,
                    },
                }
            ],
            "hot_standby_ranked_candidates": [
                {
                    "wallet": DEFAULT_WALLET,
                    "hot_standby_ready": True,
                    "in_lane_post_fee_pnl_usd": 28.082886,
                    "resolved_count": 235,
                    "readiness_verdict": "HOT_STANDBY_READY",
                }
            ],
        },
        watch_tier_shadow_ev={
            "wallets": [
                {
                    "source_wallet": DEFAULT_WALLET,
                    "eligible_signals": 1150,
                    "resolved_signals": 556,
                    "roi_pct": 22.299394,
                }
            ]
        },
        readmission_rulings={
            "rulings": [
                {
                    "source_wallet": DEFAULT_WALLET,
                    "ruling": "HOT_STANDBY_PENDING_LIVENESS",
                    "ruling_id": "2026-07-13T15:12Z-fable-hot-standby-pending-liveness",
                }
            ]
        },
        hot_standby_liveness={
            "rows": [
                {
                    "wallet": DEFAULT_WALLET,
                    "address_selection": {
                        "last_trade_age_h": 0.1,
                    },
                }
            ]
        },
    )

    assert state["status"] == "HOT_STANDBY_READY_PAPER_LANE"
    assert state["paper_only"] is True
    assert state["live_orders_allowed"] is False
    assert state["summary"]["hot_standby_ready"] is True
    assert state["summary"]["succession_eligible"] is True
    assert state["summary"]["in_lane_post_fee_pnl_usd"] == 28.082886
    assert state["summary"]["source_liveness_status"] == "PASS"
    assert state["summary"]["next"] == "keep paper-only hot standby armed; use only on recorded succession trigger"
