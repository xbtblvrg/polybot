from scripts.report_ranked_queue_clearance_packets import (
    build_converged_packets,
    build_packets,
    select_fresh_flow_wallets,
)


def test_clearance_packet_names_risk_and_exact_policy_gaps() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    payload = build_packets(
        wallets=[wallet],
        queue={
            "summary": {
                "clearance_ready": 0,
                "hot_standby_ready": 0,
                "fill_backed_candidates": 2,
                "recruitment_vintage_rule_pass": False,
                "queue_depth": 1,
                "ready_for_live": 0,
            },
            "ranked_members": [
                {
                    "wallet": wallet,
                    "queue_rank": 1,
                    "copyability_profile": {"copyability_score": 12.5},
                    "full_universe_copyability": {"universe_rank": 1},
                    "fresh_flow_rank": {"latest_trade_age_h": 0.5, "remote_dataapi_btc5m_buys_24h": 25},
                    "bench_liveness": {"status": "BENCH_ALIVE_NOT_READY"},
                    "replay": {"policy_id": "exact", "max_buy_price": 0.5},
                }
            ],
        },
        replay={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "failure_reasons": ["candidate_rejected_fill_ratio_above_maximum"],
                        "paper_orders": 10,
                        "policy_buy_events": 2,
                        "copyable_buy_events": 2,
                        "resolved_orders": 2,
                        "paper_pnl_usd": 4.0,
                        "candidate_clob_backed_orders": 2,
                        "replay_orders": [
                            {"final_status": "FILLED", "condition_id": "c"},
                            {"final_status": "FILLED", "condition_id": "c"},
                            *[
                                {
                                    "final_status": "REJECTED",
                                    "condition_id": "c",
                                    "fill_estimate": {"reject_details": {"blocking_reason": "no_ask_liquidity"}},
                                }
                                for _ in range(8)
                            ],
                        ],
                    },
                }
            ]
        },
        resolutions={"c": {"winning_outcome": "YES"}},
        toxicity={
            "rows": [
                {
                    "source_wallet": wallet,
                    "price_bucket": "01_25_50",
                    "deny_rule": "signals_100_roi_le_0",
                    "all_signals": {"roi_pct": -3.0},
                }
            ]
        },
        degrade={"latest_wallet_demotion": {"source_wallet": wallet, "reason": "demoted on loss"}},
    )

    packet = payload["packets"][0]
    assert packet["paper_only"] is True
    assert packet["live_orders_allowed"] is False
    assert packet["exact_policy"]["policy_id"] == "exact"
    assert packet["expected_edge"]["usd_per_resolved_order"] == 2.0
    assert packet["replay_fill_backed"]["prospective_reject_taxonomy"]["counts"]["no_ask"] == 8
    assert packet["replay_fill_backed"]["prospective_reject_taxonomy"]["dominant_attributable_reject_category"] == "no_ask"
    assert packet["exact_policy_post_fee_shadow"]["resolved_orders"] == 2
    assert packet["exact_policy_post_fee_shadow"]["gate_pass"] is False
    assert packet["clearance"]["paper_disposition"] == "PARK_STRUCTURAL_NO_ASK_DOMINANT"
    assert packet["clearance"]["named_cause"] == "no_recoverable_ask_dominates_attributable_rejects"
    assert packet["clearance"]["evidence_feed_active"] is True
    assert packet["clearance"]["accrual_clock_active"] is False
    assert packet["clearance"]["readmission_rule"] == "future attributable no-ask share <= 0.5"
    assert packet["risk_flags"]["toxicity_deny_cells"][0]["price_bucket"] == "01_25_50"
    assert packet["risk_flags"]["prior_demotion"] is True
    assert "attributable_reject_ratio_above_maximum" in packet["clearance"]["failed_gates"]
    assert packet["clearance"]["ready_for_live"] is False


def test_clearance_packet_derives_hot_standby_from_queue_truth() -> None:
    wallet = "0x2222222222222222222222222222222222222222"
    payload = build_packets(
        wallets=[wallet],
        queue={
            "summary": {"hot_standby_ready": 1},
            "ranked_members": [
                {
                    "wallet": wallet,
                    "ready_for_live": True,
                    "bench_liveness": {"status": "READY_AND_ALIVE"},
                    "replay": {"policy_id": "exact"},
                }
            ],
        },
        replay={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_orders": 0,
                        "resolved_orders": 0,
                        "paper_pnl_usd": 0.0,
                        "replay_orders": [],
                    },
                }
            ]
        },
        resolutions={},
        toxicity={},
        degrade={},
    )

    packet = payload["packets"][0]
    assert packet["clearance"]["ready_for_live"] is True
    assert packet["clearance"]["hot_standby_ready"] is True
    assert "hot_standby_ready_not_proven" not in packet["clearance"]["named_missing_gates"]
    assert payload["summary"]["packet_hot_standby_ready"] == 1


def test_ready_for_live_requires_fill_backed_replay_pass() -> None:
    wallet = "0x9999999999999999999999999999999999999999"
    payload = build_packets(
        wallets=[wallet],
        queue={
            "summary": {"clearance_ready": 1, "hot_standby_ready": 1},
            "ranked_members": [
                {
                    "wallet": wallet,
                    "ready_for_live": True,
                    "bench_liveness": {"status": "READY_AND_ALIVE"},
                    "replay": {"status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY"},
                }
            ],
        },
        replay={},
        resolutions={},
        toxicity={},
        degrade={},
    )

    packet = payload["packets"][0]
    assert packet["replay_fill_backed"]["gate_pass"] is False
    assert packet["expected_edge"]["not_live_promotable"] is True
    assert packet["clearance"]["ready_for_live"] is False
    assert packet["clearance"]["hot_standby_ready"] is False
    assert payload["summary"]["clearance_ready"] == 0
    assert payload["summary"]["hot_standby_ready"] == 0
    assert payload["summary"]["queue_reported_clearance_ready"] == 1


def test_empty_sample_is_named_no_recent_buy_not_reject_ratio() -> None:
    wallet = "0x3333333333333333333333333333333333333333"
    payload = build_packets(
        wallets=[wallet],
        queue={"ranked_members": [{"wallet": wallet}]},
        replay={},
        resolutions={},
        toxicity={},
        degrade={},
    )

    packet = payload["packets"][0]
    assert packet["clearance"]["named_cause"] == "no_recent_buy_sample"
    assert packet["clearance"]["paper_disposition"] == "ACCRUE_RECENT_BUY_SAMPLE"
    assert payload["summary"]["controlling_gate"] == "no_recent_buy_sample"


def test_fresh_flow_screened_packet_keeps_shadow_accrual_active() -> None:
    wallet = "0x8888888888888888888888888888888888888888"
    payload = build_packets(
        wallets=[wallet],
        queue={"ranked_members": [{"wallet": wallet}]},
        replay={
            "candidates": [
                {
                    "wallet": wallet,
                    "paper_replay": {
                        "paper_orders": 10,
                        "policy_buy_events": 2,
                        "copyable_buy_events": 2,
                        "replay_orders": [
                            {
                                "final_status": "REJECTED",
                                "fill_estimate": {"reject_details": {"blocking_reason": "no_ask_liquidity"}},
                            }
                            for _ in range(10)
                        ],
                    },
                }
            ]
        },
        resolutions={},
        toxicity={},
        degrade={},
        fresh_flow_screened=True,
    )

    clearance = payload["packets"][0]["clearance"]
    assert clearance["paper_disposition"] == "ACCRUE_EXACT_POLICY_SHADOW"
    assert clearance["accrual_clock_active"] is True
    assert clearance["named_cause"] is None


def test_packet_selection_parks_dormant_names_and_walks_fresh_queue() -> None:
    dormant = "0x4444444444444444444444444444444444444444"
    fresh_a = "0x5555555555555555555555555555555555555555"
    stale = "0x6666666666666666666666666666666666666666"
    fresh_b = "0x7777777777777777777777777777777777777777"
    selected, parked = select_fresh_flow_wallets(
        queue={
            "ranked_members": [
                {
                    "wallet": fresh_a,
                    "fresh_flow_rank": {"latest_trade_age_h": 1.0, "remote_dataapi_btc5m_buys_24h": 3},
                },
                {
                    "wallet": stale,
                    "fresh_flow_rank": {"latest_trade_age_h": 25.0, "remote_dataapi_btc5m_buys_24h": 99},
                },
                {
                    "wallet": fresh_b,
                    "fresh_flow_rank": {"latest_trade_age_h": 2.0, "remote_dataapi_btc5m_buys_24h": 1},
                },
            ]
        },
        preferred_wallets=[dormant],
        count=2,
    )

    assert selected == [fresh_a, fresh_b]
    assert parked[0]["wallet"] == dormant
    assert parked[0]["status"] == "PARKED_DORMANT"
    assert parked[0]["recheck_after_h"] == 24


def test_convergence_parks_high_reject_packet_and_backfills_to_three() -> None:
    wallets = [f"0x{index:040x}" for index in range(1, 5)]
    queue_rows = [
        {
            "wallet": wallet,
            "queue_rank": index,
            "fresh_flow_rank": {"latest_trade_age_h": 1.0, "remote_dataapi_btc5m_buys_24h": 5},
        }
        for index, wallet in enumerate(wallets, start=1)
    ]
    payload = build_converged_packets(
        count=3,
        queue={"ranked_members": queue_rows},
        replay={
            "candidates": [
                {
                    "wallet": wallets[0],
                    "paper_replay": {
                        "paper_orders": 200,
                        "policy_buy_events": 200,
                        "copyable_buy_events": 200,
                        "replay_orders": [
                            {
                                "final_status": "REJECTED",
                                "fill_estimate": {"reject_details": {"blocking_reason": "price_above_limit"}},
                            }
                            for _ in range(200)
                        ],
                    },
                }
            ]
        },
        resolutions={},
        toxicity={},
        degrade={},
        parked_dormant=[],
    )

    assert [row["wallet"] for row in payload["packets"]] == wallets[1:]
    parked = payload["parked_reject_ratio"][0]
    assert parked["wallet"] == wallets[0]
    assert parked["status"] == "PARK_REJECT_RATIO"
    assert parked["paper_orders"] == 200
    assert parked["attributable_reject_ratio"] == 1.0
    assert payload["summary"]["accruing_packet_count"] == 3


def test_convergence_parks_mathematically_doomed_packet_before_minimum_n() -> None:
    wallets = [f"0x{index:040x}" for index in range(1, 5)]
    queue_rows = [
        {
            "wallet": wallet,
            "queue_rank": index,
            "fresh_flow_rank": {"latest_trade_age_h": 1.0, "remote_dataapi_btc5m_buys_24h": 5},
        }
        for index, wallet in enumerate(wallets, start=1)
    ]
    payload = build_converged_packets(
        count=3,
        queue={"ranked_members": queue_rows},
        replay={
            "candidates": [
                {
                    "wallet": wallets[0],
                    "paper_replay": {
                        "paper_orders": 129,
                        "policy_buy_events": 129,
                        "copyable_buy_events": 129,
                        "replay_orders": [
                            {
                                "final_status": "REJECTED",
                                "fill_estimate": {"reject_details": {"blocking_reason": "price_above_limit"}},
                            }
                            for _ in range(125)
                        ],
                    },
                }
            ]
        },
        resolutions={},
        toxicity={},
        degrade={},
        parked_dormant=[],
    )

    assert [row["wallet"] for row in payload["packets"]] == wallets[1:]
    parked = payload["parked_reject_ratio"][0]
    assert parked["wallet"] == wallets[0]
    assert parked["status"] == "PARK_REJECT_RATIO"
    assert parked["paper_orders"] == 129
    assert parked["attributable_rejects"] == 125
    assert parked["mathematically_doomed"] is True
    assert parked["doom_floor_at_minimum"] == 0.625
    assert parked["maximum_rejects_at_minimum"] == 120.0


def test_convergence_preserves_prior_ratio_park_across_refresh() -> None:
    wallets = [f"0x{index:040x}" for index in range(1, 5)]
    queue_rows = [
        {
            "wallet": wallet,
            "queue_rank": index,
            "fresh_flow_rank": {"latest_trade_age_h": 1.0, "remote_dataapi_btc5m_buys_24h": 5},
        }
        for index, wallet in enumerate(wallets, start=1)
    ]
    prior_park = {
        "wallet": wallets[0],
        "status": "PARK_REJECT_RATIO",
        "paper_orders": 276,
        "attributable_reject_ratio": 0.630631,
    }

    payload = build_converged_packets(
        count=3,
        queue={"ranked_members": queue_rows},
        replay={},
        resolutions={},
        toxicity={},
        degrade={},
        parked_dormant=[],
        prior_parked_reject_ratio=[prior_park],
    )

    assert [row["wallet"] for row in payload["packets"]] == wallets[1:]
    assert payload["parked_reject_ratio"] == [prior_park]
    assert payload["summary"]["parked_reject_ratio_count"] == 1


def test_convergence_keeps_mature_packet_with_ratio_below_threshold() -> None:
    wallets = [f"0x{index:040x}" for index in range(1, 4)]
    queue_rows = [
        {
            "wallet": wallet,
            "queue_rank": index,
            "fresh_flow_rank": {"latest_trade_age_h": 1.0, "remote_dataapi_btc5m_buys_24h": 5},
        }
        for index, wallet in enumerate(wallets, start=1)
    ]
    payload = build_converged_packets(
        count=3,
        queue={"ranked_members": queue_rows},
        replay={
            "candidates": [
                {
                    "wallet": wallets[0],
                    "paper_replay": {
                        "paper_orders": 400,
                        "policy_buy_events": 400,
                        "copyable_buy_events": 400,
                        "replay_orders": [
                            *[{"final_status": "FILLED", "condition_id": "c"} for _ in range(270)],
                            *[
                                {
                                    "final_status": "REJECTED",
                                    "fill_estimate": {"reject_details": {"blocking_reason": "price_above_limit"}},
                                }
                                for _ in range(130)
                            ],
                        ],
                    },
                }
            ]
        },
        resolutions={},
        toxicity={},
        degrade={},
        parked_dormant=[],
    )

    assert [row["wallet"] for row in payload["packets"]] == wallets
    assert payload["parked_reject_ratio"] == []


def test_convergence_purges_prior_park_no_longer_justified() -> None:
    wallets = [f"0x{index:040x}" for index in range(1, 5)]
    queue_rows = [
        {
            "wallet": wallet,
            "queue_rank": index,
            "fresh_flow_rank": {"latest_trade_age_h": 1.0, "remote_dataapi_btc5m_buys_24h": 5},
        }
        for index, wallet in enumerate(wallets, start=1)
    ]
    stale_park = {
        "wallet": wallets[0],
        "status": "PARK_REJECT_RATIO",
        "paper_orders": 674,
        "attributable_rejects": 163,
        "attributable_reject_ratio": 0.326,
        "mathematically_doomed": True,
    }
    kept_park = {
        "wallet": wallets[3],
        "status": "PARK_REJECT_RATIO",
        "paper_orders": 276,
        "attributable_reject_ratio": 0.630631,
    }

    payload = build_converged_packets(
        count=3,
        queue={"ranked_members": queue_rows},
        replay={},
        resolutions={},
        toxicity={},
        degrade={},
        parked_dormant=[],
        prior_parked_reject_ratio=[stale_park, kept_park],
    )

    assert [row["wallet"] for row in payload["packets"]] == wallets[:3]
    assert payload["parked_reject_ratio"] == [kept_park]
    assert payload["summary"]["parked_reject_ratio_count"] == 1
