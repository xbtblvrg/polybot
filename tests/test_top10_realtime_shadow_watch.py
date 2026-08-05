from scripts.apply_top10_realtime_shadow_watch import WATCH_STATUS, apply_reclassification


def test_reclassification_arms_four_primary_shadow_wallets_and_keeps_passive_no_sample() -> None:
    wallets = [
        ("0x1111111111111111111111111111111111111111", "ANALYZE_PARTIAL_SAMPLE"),
        ("0x2222222222222222222222222222222222222222", "FAIL_LANE_COVERAGE"),
        ("0x3333333333333333333333333333333333333333", "ANALYZE_NO_RECENT_BUY_SAMPLE"),
    ]
    clearance, shadow, lane = apply_reclassification(
        clearance_summary={
            "summary": {"candidate_count": 3},
            "rows": [
                {"wallet": wallet, "candidate_id": f"candidate_{idx}", "clearance_status": status}
                for idx, (wallet, status) in enumerate(wallets)
            ],
        },
        parity_rescore={
            "rows": [
                {
                    "wallet": wallets[0][0],
                    "parity_taker_fillable_orders": 7,
                    "parity_maker_maybe_fill_orders": 3,
                    "copyable_rate_pct_parity_taker": 25.0,
                    "reject_category_counts": {"deep_slippage": 1},
                },
                {
                    "wallet": wallets[1][0],
                    "parity_taker_fillable_orders": 0,
                    "parity_maker_maybe_fill_orders": 9,
                    "copyable_rate_pct_parity_taker": 0.0,
                    "reject_category_counts": {"maker_fallback_fillable": 9},
                },
            ]
        },
    )

    assert clearance["summary"]["watch_realtime_shadow_required_count"] == 2
    assert clearance["summary"]["ready_for_live"] == 0
    assert [row["clearance_status"] for row in clearance["rows"][:2]] == [WATCH_STATUS, WATCH_STATUS]
    assert clearance["rows"][2]["shadow_coverage_mode"] == "passive_if_wallet_trades"
    assert shadow["status"] == "ARMED"
    assert shadow["paper_only"] is True
    assert shadow["live_orders_allowed"] is False
    assert shadow["summary"] == {"registered_wallets": 2, "passive_wallets": 1, "ready_for_live": 0}
    assert shadow["scoring_contract"]["maker_maybe_counts_as_admission_fill"] is False
    assert lane["status"] == "ARMED"
    assert lane["summary"]["primary_shadow_wallets"] == 2
    assert lane["summary"]["passive_wallets"] == 1
    assert len(lane["ranked_wallets"]) == 3
    assert lane["ranked_wallets"][0]["promotion_arithmetic"] == "realtime_shadow_taker_fills_only"

    second_clearance, second_shadow, second_lane = apply_reclassification(
        clearance_summary=clearance,
        parity_rescore={"rows": []},
    )
    assert second_clearance["summary"]["watch_realtime_shadow_required_count"] == 2
    assert second_shadow["summary"]["registered_wallets"] == 2
    assert second_lane["summary"]["primary_shadow_wallets"] == 2


def test_reclassification_adds_explicit_member_queue_shadow_watch_without_promoting() -> None:
    watch_wallet = "0xad825954d08beba32f74b594821f4251460c3df1"

    clearance, shadow, lane = apply_reclassification(
        clearance_summary={"summary": {"candidate_count": 0}, "rows": []},
        parity_rescore={"rows": []},
        extra_watch_rows=[
            {
                "wallet": watch_wallet,
                "candidate_id": "leaderboard_crypto_ad825954d0",
                "clearance_reason": "fable_shadow_watch_until_20_copyable_clob_backed_buys;current_copyable_buy_events=8",
                "parity_prior": {
                    "copyable_buy_events": 8,
                    "candidate_clob_backed_orders": 8,
                    "promotion_min_copyable_buy_events": 20,
                },
            }
        ],
    )

    assert clearance["summary"]["ready_for_live"] == 0
    assert shadow["summary"]["registered_wallets"] == 1
    assert shadow["wallets"][0]["wallet"] == watch_wallet
    assert shadow["wallets"][0]["status"] == WATCH_STATUS
    assert shadow["live_orders_allowed"] is False
    assert lane["ranked_wallets"][0]["wallet"] == watch_wallet
    assert lane["ranked_wallets"][0]["promotion_arithmetic"] == "realtime_shadow_taker_fills_only"
