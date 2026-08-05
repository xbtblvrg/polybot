from scripts.build_top10_watch_clearance_summary import build_summary, classify_clearance
from scripts.prepare_top10_watch_clearance_replay import prepare_replay_payload


def test_clearance_taxonomy_keeps_resolution_blind_spot_unmeasurable() -> None:
    status, reason = classify_clearance(
        replay={
            "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
            "paper_pnl_usd": 0.0,
            "resolved_orders": 0,
            "copyable_buy_events": 5,
            "unresolved_ratio": 1.0,
        },
        measurement={
            "buy_events": 6,
            "copyable_buy_events": 6,
            "paper_pnl_usd": -2.0,
            "reject_reasons": {"price_above_slippage_cap": 1},
        },
    )

    assert status == "UNMEASURABLE_RESOLUTION_BLIND_SPOT"
    assert reason == "all_replay_orders_unresolved"


def test_clearance_taxonomy_allows_resolved_negative_edge_fail() -> None:
    status, reason = classify_clearance(
        replay={
            "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
            "paper_pnl_usd": -1.25,
            "resolved_orders": 3,
            "copyable_buy_events": 5,
            "unresolved_ratio": 0.4,
        },
        measurement={
            "buy_events": 6,
            "copyable_buy_events": 5,
            "paper_pnl_usd": -1.25,
            "reject_reasons": {"price_above_slippage_cap": 1},
        },
    )

    assert status == "DEFINITIVE_FAIL_NEGATIVE_EDGE"
    assert reason == "negative_resolved_replay"


def test_summary_reports_per_wallet_resolution_window_coverage() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    payload = build_summary(
        candidates_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "candidate_a",
                    "source_queue_rank": 7,
                    "paper_replay_seed": {"paper_pnl_usd": 2.0},
                }
            ]
        },
        lane_state={"ranked_wallets": [{"wallet": wallet, "name": "lane_a"}]},
        replay_payload={
            "replay_summary": {"candidate_count": 1},
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "candidate_a",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 1.5,
                        "paper_orders": 1,
                        "resolved_orders": 1,
                        "copyable_buy_events": 1,
                        "candidate_clob_backed_orders": 1,
                        "unresolved_ratio": 0.0,
                        "replay_orders": [
                            {
                                "final_status": "FILLED",
                                "market_slug": "btc-updown-5m-1783376100",
                            }
                        ],
                    },
                }
            ],
        },
        measurement_state={
            "summary": {"wallets": 1},
            "wallets": {
                wallet: {
                    "wallet": wallet,
                    "buy_events": 1,
                    "copyable_buy_events": 1,
                    "paper_pnl_usd": 1.5,
                }
            },
        },
        resolution_rows=[
            {
                "market_slug": "btc-updown-5m-1783376100",
                "expiry_unix_ts": 1783376400,
                "window_type": "5m",
                "source": "polymarket_gamma_resolved_outcome",
            }
        ],
        resolution_summary={"requested_windows": 1},
        resolutions_path="resolutions.jsonl",
    )

    assert payload["summary"]["definitive_pass_count"] == 1
    assert payload["summary"]["resolution_covered_replay_windows"] == 1
    coverage = payload["rows"][0]["resolution_window_coverage"]
    assert coverage["status"] == "COVERED"
    assert coverage["replay_window_start_min"] == 1783376100


def test_prepare_top10_replay_filters_full_pool_to_same_wallets() -> None:
    wanted = "0x1111111111111111111111111111111111111111"
    extra = "0x2222222222222222222222222222222222222222"
    payload = prepare_replay_payload(
        candidates_payload={
            "candidates": [
                {
                    "wallet": wanted,
                    "candidate_id": "seed_candidate",
                    "source_queue_rank": 4,
                    "paper_replay_seed": {"paper_pnl_usd": 2.0},
                }
            ]
        },
        full_pool_replay_payload={
            "candidates": [
                {
                    "wallet": wanted,
                    "candidate_id": "full_candidate",
                    "paper_replay": {
                        "paper_orders": 2,
                        "resolved_orders": 1,
                    },
                },
                {
                    "wallet": extra,
                    "candidate_id": "extra_candidate",
                    "paper_replay": {
                        "paper_orders": 10,
                        "resolved_orders": 10,
                    },
                },
            ]
        },
    )

    assert payload["candidate_count"] == 1
    assert payload["candidates"][0]["wallet"] == wanted
    assert payload["candidates"][0]["candidate_id"] == "seed_candidate"
    assert payload["replay_summary"]["total_paper_orders"] == 2
    assert payload["replay_summary"]["candidates_with_resolved_orders"] == 1


def test_prepare_replay_payload_restores_full_pool_replay_orders_for_same_wallets() -> None:
    wallet = "0x2222222222222222222222222222222222222222"
    payload = prepare_replay_payload(
        candidates_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "top10_candidate",
                    "source_queue_rank": 3,
                    "paper_replay_seed": {"paper_pnl_usd": 4.0},
                }
            ]
        },
        full_pool_replay_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "full_pool_candidate",
                    "paper_replay": {
                        "paper_orders": 2,
                        "resolved_orders": 1,
                        "replay_orders": [{"order_id": "a"}, {"order_id": "b"}],
                    },
                }
            ]
        },
    )

    assert payload["candidate_count"] == 1
    assert payload["replay_summary"]["total_paper_orders"] == 2
    assert payload["replay_summary"]["candidates_with_resolved_orders"] == 1
    row = payload["candidates"][0]
    assert row["candidate_id"] == "top10_candidate"
    assert row["paper_replay"]["replay_orders"] == [{"order_id": "a"}, {"order_id": "b"}]
