from scripts.build_full_pool_member_queue import build_queue


def _fresh_flow_probe(wallet: str, *, age_h: float = 1.0) -> dict:
    return {
        "kind": "queue_remote_dataapi_fresh_flow_probe",
        "generated_at": "2026-07-13T23:52:53Z",
        "rows": [
            {
                "wallet": wallet,
                "btc5m_buys_24h": 3,
                "btc5m_trades_24h": 3,
                "btc5m_buy_rows_24h_by_price_subband": {"01a_25_32": 2},
                "latest_trade_age_h": age_h,
            }
        ],
    }


def test_full_pool_member_queue_marks_unresolved_next_action() -> None:
    wallet = "0x1111111111111111111111111111111111111111"
    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 1, "top_count": 1},
            "top_candidates": [
                {
                    "wallet": wallet,
                    "resolved_pnl": 10.0,
                    "complementary_fills": 3,
                    "eligible_profile": {
                        "status": "PASS",
                        "best_eligible_move_slice": {
                            "move_slice_key": "000-060|<=0.25",
                            "entry_price_band": "<=0.25",
                            "status": "PASS",
                        },
                    },
                }
            ],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 1, "promotable_replays": 0},
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "candidate_a",
                    "paper_replay": {
                        "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "paper_pnl_usd": 5.0,
                        "candidate_clob_backed_orders": 2,
                        "failure_reasons": ["candidate_unresolved_ratio_above_maximum"],
                    },
                }
            ],
        },
        rotation_state={"decision": {"action": "RETAIN_NO_ELIGIBLE_CANDIDATE"}},
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["ready_for_live"] is False
    assert row["next_action"] == "refresh/attach market resolutions, then rescore stored replay orders"
    assert payload["defects"][0]["defect"] == "rotation_triggered_without_promotable_candidate"


def test_full_pool_member_queue_marks_pass_ready() -> None:
    wallet = "0x2222222222222222222222222222222222222222"
    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 1, "top_count": 1},
            "top_candidates": [{"wallet": wallet, "resolved_pnl": 1.0, "complementary_fills": 1}],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 1, "promotable_replays": 1},
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "candidate_b",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 1.5,
                        "copyable_buy_events": 20,
                        "candidate_clob_backed_orders": 20,
                    },
                }
            ],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        fresh_flow_probe=_fresh_flow_probe(wallet),
        limit=10,
    )

    assert payload["ranked_members"][0]["ready_for_live"] is True
    assert payload["ranked_members"][0]["fresh_flow_rank"][
        "remote_dataapi_btc5m_buy_rows_24h_by_price_subband"
    ] == {"01a_25_32": 2}
    assert payload["summary"]["ready_for_live"] == 1


def test_full_pool_member_queue_uses_fresh_copyability_ranked_queue_as_authority() -> None:
    wallet = "0x" + "3" * 40
    payload = build_queue(
        shortlist={"top_candidates": [{"wallet": "0x" + "4" * 40}]},
        replay_payload={"candidates": []},
        rotation_state={"decision": {"action": "WATCH"}},
        copyability_payload={
            "status": "PASS_CURRENT_SOURCE",
            "promotion_grade": True,
            "inputs": {
                "source_freshness_pass": True,
                "followability_freshness_pass": True,
                "replay_freshness_pass": True,
            },
            "ranked_queue": [
                {
                    "wallet": wallet,
                    "queue_eligible": True,
                    "admission_status": "READY_QUEUE",
                    "copyability_score": 12.5,
                    "universe_rank": 1,
                    "copy_replay": {
                        "candidate_id": "fresh_candidate",
                        "paper_pnl_usd": 4.0,
                        "copyable_buy_events": 30,
                        "candidate_clob_backed_orders": 30,
                        "unique_windows": 4,
                    },
                }
            ],
        },
        limit=10,
    )

    assert [row["wallet"] for row in payload["ranked_members"]] == [wallet]
    assert payload["ranked_members"][0]["queue_source"] == "fresh_full_universe_copyability"
    assert payload["ranked_members"][0]["ready_for_live"] is False
    assert payload["summary"]["copyability_authoritative"] is True


def test_full_pool_member_queue_fails_closed_on_stale_copyability() -> None:
    payload = build_queue(
        shortlist={"top_candidates": [{"wallet": "0x" + "4" * 40}]},
        replay_payload={"candidates": []},
        rotation_state={"decision": {"action": "WATCH"}},
        copyability_payload={
            "status": "DEPENDENCY_FRESHNESS_FAIL_CLOSED",
            "promotion_grade": False,
            "inputs": {"source_freshness_pass": False},
            "ranked_queue": [{"wallet": "0x" + "3" * 40, "queue_eligible": True}],
        },
        limit=10,
    )

    assert payload["ranked_members"] == []
    assert payload["summary"]["copyability_authoritative"] is True


def test_authoritative_copyability_applies_formal_packet_clearance_to_hot_standby() -> None:
    wallet = "0x1313131313131313131313131313131313131313"
    payload = build_queue(
        shortlist={},
        replay_payload={"candidates": []},
        rotation_state={"decision": {"action": "WATCH"}},
        clearance_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "classification": "CLEAR",
                    "metrics": {
                        "paper_pnl_usd": 4.0,
                        "copyable_buy_events": 60,
                        "candidate_clob_backed_orders": 60,
                        "resolved_orders": 55,
                        "unresolved_filled_order_count": 0,
                    },
                    "window_coverage": {"missing_resolution_market_count": 0},
                }
            ]
        },
        clearance_packets_payload={
            "packets": [
                {
                    "wallet": wallet,
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "clearance": {"paper_disposition": "ACCRUE_EXACT_POLICY_SHADOW", "failed_gates": []},
                    "exact_policy_post_fee_shadow": {"gate_pass": True},
                }
            ]
        },
        copyability_payload={
            "status": "PASS_CURRENT_SOURCE",
            "promotion_grade": True,
            "inputs": {
                "source_freshness_pass": True,
                "followability_freshness_pass": True,
                "replay_freshness_pass": True,
            },
            "ranked_queue": [
                {
                    "wallet": wallet,
                    "queue_eligible": True,
                    "copyability_score": 12.5,
                    "copy_replay": {
                        "paper_pnl_usd": 4.0,
                        "resolved_orders": 55,
                        "copyable_buy_events": 60,
                        "candidate_clob_backed_orders": 60,
                    },
                }
            ],
        },
        fresh_flow_probe=_fresh_flow_probe(wallet),
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["ready_for_live"] is True
    assert row["clearance_ready"] is True
    assert row["bench_liveness"]["status"] == "READY_AND_ALIVE"
    assert row["clearance_packet"]["live_mutation"] is False
    assert payload["summary"]["packet_clearance_applied"] == 1
    assert payload["summary"]["hot_standby_ready"] == 1


def test_full_pool_member_queue_ranks_complementary_hours_before_fills() -> None:
    wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa1"
    wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb2"
    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 2, "top_count": 2},
            "top_candidates": [
                {
                    "wallet": wallet_a,
                    "resolved_pnl": 10.0,
                    "complementary_fills": 20,
                    "complementary_hours_utc": [1],
                },
                {
                    "wallet": wallet_b,
                    "resolved_pnl": 5.0,
                    "complementary_fills": 1,
                    "complementary_hours_utc": [2, 3, 4],
                },
            ],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 2, "promotable_replays": 2},
            "candidates": [
                {
                    "wallet": wallet_a,
                    "candidate_id": "candidate_a",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 10.0,
                        "copyable_buy_events": 20,
                        "candidate_clob_backed_orders": 20,
                    },
                },
                {
                    "wallet": wallet_b,
                    "candidate_id": "candidate_b",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 5.0,
                        "copyable_buy_events": 20,
                        "candidate_clob_backed_orders": 20,
                    },
                },
            ],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        limit=10,
    )

    assert payload["ranking"]["primary"].startswith("ready_for_live_then_policy_compatible_fresh_flow")
    assert payload["ranked_members"][0]["wallet"] == wallet_b
    assert payload["ranked_members"][0]["complementary_hours_score"] == 3


def test_full_pool_member_queue_ranks_fresh_flow_before_complementary_hours() -> None:
    fresh_wallet = "0xabababababababababababababababababababab"
    stale_wallet = "0xcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd"
    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 2, "top_count": 2},
            "top_candidates": [
                {
                    "wallet": stale_wallet,
                    "resolved_pnl": 10.0,
                    "complementary_fills": 20,
                    "complementary_hours_utc": [1, 2, 3],
                },
                {
                    "wallet": fresh_wallet,
                    "resolved_pnl": 2.0,
                    "complementary_fills": 1,
                    "complementary_hours_utc": [4],
                },
            ],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 2, "promotable_replays": 2},
            "candidates": [
                {
                    "wallet": fresh_wallet,
                    "candidate_id": "candidate_fresh",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 2.0,
                        "copyable_buy_events": 20,
                        "candidate_clob_backed_orders": 20,
                    },
                },
                {
                    "wallet": stale_wallet,
                    "candidate_id": "candidate_stale",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 10.0,
                        "copyable_buy_events": 20,
                        "candidate_clob_backed_orders": 20,
                    },
                },
            ],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        fresh_flow_probe={
            "ranked_candidates": [
                {
                    "wallet": fresh_wallet,
                    "fresh_flow": True,
                    "p1_promotion_eligible": True,
                    "btc5m_buys": 3,
                    "latest_trade_age_h": 0.1,
                },
                {
                    "wallet": stale_wallet,
                    "fresh_flow": False,
                    "btc5m_buys": 0,
                    "latest_trade_age_h": 24.0,
                },
            ]
        },
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["wallet"] == fresh_wallet
    assert row["fresh_flow_rank"]["fresh_flow"] is True
    assert payload["summary"]["fresh_flow_ready"] == 1


def test_full_pool_member_queue_ranks_policy_fresh_flow_before_dataapi_flow() -> None:
    policy_wallet = "0x9191919191919191919191919191919191919191"
    dataapi_wallet = "0x9292929292929292929292929292929292929292"
    base_replay = {
        "eligibility_status": "PASS",
        "paper_pnl_usd": 3.0,
        "copyable_buy_events": 20,
        "candidate_clob_backed_orders": 20,
    }

    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 2, "top_count": 2},
            "top_candidates": [
                {"wallet": dataapi_wallet, "resolved_pnl": 9.0, "complementary_hours_utc": [1, 2, 3]},
                {"wallet": policy_wallet, "resolved_pnl": 3.0, "complementary_hours_utc": [4]},
            ],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 2, "promotable_replays": 2},
            "candidates": [
                {"wallet": dataapi_wallet, "candidate_id": "dataapi", "paper_replay": base_replay},
                {"wallet": policy_wallet, "candidate_id": "policy", "paper_replay": base_replay},
            ],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        active_set_poller={
            "fetch_meta": {
                policy_wallet: {
                    "policy_feedback": {
                        "policy_compatible_fresh_buy_rows_le_30s": 1,
                        "freshest_policy_compatible_buy_lag_s": 4.5,
                    }
                }
            }
        },
        dataapi_first_seen_rows=[
            {
                "event": "dataapi_first_seen",
                "wallet": dataapi_wallet,
                "captured_at_s": 9_999_999_999.0,
            }
            for _ in range(20)
        ],
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["wallet"] == policy_wallet
    assert row["fresh_flow_rank"]["policy_compatible_fresh_buy_rows_le_30s"] == 1
    assert row["fresh_flow_rank"]["rank_source"] == "policy_feedback_le_30s"


def test_full_pool_member_queue_uses_remote_dataapi_24h_probe_evidence() -> None:
    fresh_wallet = "0xefefefefefefefefefefefefefefefefefefefef"
    stale_wallet = "0x1212121212121212121212121212121212121212"
    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 2, "top_count": 2},
            "top_candidates": [
                {"wallet": stale_wallet, "resolved_pnl": 20.0, "complementary_fills": 20},
                {"wallet": fresh_wallet, "resolved_pnl": 1.0, "complementary_fills": 1},
            ],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 2, "promotable_replays": 2},
            "candidates": [
                {
                    "wallet": fresh_wallet,
                    "candidate_id": "candidate_remote_fresh",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 1.0,
                        "copyable_buy_events": 20,
                        "candidate_clob_backed_orders": 20,
                    },
                },
                {
                    "wallet": stale_wallet,
                    "candidate_id": "candidate_remote_stale",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 20.0,
                        "copyable_buy_events": 20,
                        "candidate_clob_backed_orders": 20,
                    },
                },
            ],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        fresh_flow_probe={
            "ranked_candidates": [
                {
                    "wallet": fresh_wallet,
                    "evidence": {
                        "remote_dataapi_24h": {
                            "btc5m_trades_24h": 5,
                            "btc5m_buys_24h": 4,
                            "policy_compatible_inband_buy_rows_24h": 3,
                            "latest_trade_age_h": 1.5,
                        }
                    },
                },
                {
                    "wallet": stale_wallet,
                    "evidence": {
                        "remote_dataapi_24h": {
                            "btc5m_trades_24h": 0,
                            "btc5m_buys_24h": 0,
                            "policy_compatible_inband_buy_rows_24h": 0,
                            "latest_trade_age_h": None,
                        }
                    },
                },
            ]
        },
        external_liveness_probe={
            "generated_at": "2023-11-14T22:13:20Z",
            "rows": [
                {
                    "wallet": fresh_wallet,
                    "status": "PASS",
                    "fetched_at": "2023-11-14T22:13:20Z",
                    "latest_btc5m_trade_ts": 1_699_996_400.0,
                    "btc5m_trades_24h": 5,
                }
            ],
        },
        now_ts=1_700_000_000.0,
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["wallet"] == fresh_wallet
    assert row["external_liveness_status"] == "PASS"
    assert row["external_liveness_reason"] == "external_liveness_pass"
    assert row["external_latest_trade_age_h"] == 1.0
    assert row["external_liveness_probe"]["probe_row_status"] == "PASS"
    assert row["fresh_flow_rank"]["rank_source"] == "remote_dataapi_24h"
    assert row["fresh_flow_rank"]["remote_dataapi_btc5m_buys_24h"] == 4
    assert payload["summary"]["clearance_ready_with_remote_dataapi_fresh"] == 0


def test_full_pool_member_queue_demotes_stale_bench_to_dormant() -> None:
    live_wallet = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    stale_wallet = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    replay = {
        "eligibility_status": "PASS",
        "paper_pnl_usd": 2.0,
        "copyable_buy_events": 20,
        "candidate_clob_backed_orders": 20,
    }

    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 2, "top_count": 2},
            "top_candidates": [
                {"wallet": stale_wallet, "resolved_pnl": 3.0, "complementary_fills": 20},
                {"wallet": live_wallet, "resolved_pnl": 2.0, "complementary_fills": 20},
            ],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 2, "promotable_replays": 2},
            "candidates": [
                {"wallet": stale_wallet, "candidate_id": "stale", "paper_replay": replay},
                {"wallet": live_wallet, "candidate_id": "live", "paper_replay": replay},
            ],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        fresh_flow_probe={
            "ranked_candidates": [
                {
                    "wallet": stale_wallet,
                    "evidence": {"remote_dataapi_24h": {"btc5m_buys_24h": 3, "latest_trade_age_h": 60.0}},
                },
                {
                    "wallet": live_wallet,
                    "evidence": {"remote_dataapi_24h": {"btc5m_buys_24h": 3, "latest_trade_age_h": 2.0}},
                },
            ]
        },
        limit=10,
    )

    rows = {row["wallet"]: row for row in payload["ranked_members"]}
    assert rows[live_wallet]["bench_liveness"]["status"] == "READY_AND_ALIVE"
    assert rows[live_wallet]["bench_liveness"]["last_trade_age_hours"] == 2.0
    assert rows[live_wallet]["last_trade_age_hours"] == 2.0
    assert rows[stale_wallet]["bench_liveness"]["status"] == "DORMANT_STALE_GT_48H"
    assert rows[stale_wallet]["ready_for_live"] is False
    assert rows[stale_wallet]["bench_tier"] == "dormant"
    assert payload["summary"]["ready_alive"] == 1
    assert payload["summary"]["hot_standby_ready"] == 1
    assert payload["summary"]["hot_standby_gap"] == 1
    assert payload["summary"]["dormant_stale_gt_48h"] == 1


def test_full_pool_member_queue_recomputes_nested_remote_age_from_trade_timestamp() -> None:
    now_ts = 1_800_000_000.0
    fresh_wallet = "0x3434343434343434343434343434343434343434"
    stale_wallet = "0x5656565656565656565656565656565656565656"
    base_replay = {
        "eligibility_status": "PASS",
        "paper_pnl_usd": 2.0,
        "copyable_buy_events": 20,
        "candidate_clob_backed_orders": 20,
    }

    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 2, "top_count": 2},
            "top_candidates": [
                {"wallet": stale_wallet, "resolved_pnl": 20.0, "complementary_fills": 20},
                {"wallet": fresh_wallet, "resolved_pnl": 1.0, "complementary_fills": 1},
            ],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 2, "promotable_replays": 2},
            "candidates": [
                {"wallet": stale_wallet, "candidate_id": "stale", "paper_replay": base_replay},
                {"wallet": fresh_wallet, "candidate_id": "fresh", "paper_replay": base_replay},
            ],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        fresh_flow_probe={
            "ranked_candidates": [
                {
                    "wallet": stale_wallet,
                    "evidence": {
                        "remote_dataapi_24h": {
                            "btc5m_trades_24h": 5,
                            "btc5m_buys_24h": 4,
                            "policy_compatible_inband_buy_rows_24h": 3,
                            "latest_btc5m_trade_ts": now_ts - 7 * 3600,
                            "latest_trade_age_h": 0.01,
                            "pass_admission_threshold": True,
                        }
                    },
                },
                {
                    "wallet": fresh_wallet,
                    "evidence": {
                        "remote_dataapi_24h": {
                            "btc5m_trades_24h": 4,
                            "btc5m_buys_24h": 3,
                            "policy_compatible_inband_buy_rows_24h": 2,
                            "latest_btc5m_trade_ts": now_ts - 1800,
                            "latest_trade_age_h": 9.0,
                            "pass_admission_threshold": False,
                        }
                    },
                },
            ]
        },
        now_ts=now_ts,
        limit=10,
    )

    rows = {row["wallet"]: row for row in payload["ranked_members"]}
    assert payload["ranked_members"][0]["wallet"] == fresh_wallet
    assert rows[stale_wallet]["fresh_flow_rank"]["latest_trade_age_h"] == 7.0
    assert rows[stale_wallet]["fresh_flow_rank"]["remote_dataapi_latest_trade_age_h"] == 7.0
    assert rows[stale_wallet]["fresh_flow_rank"]["source_reported_latest_trade_age_h"] == 0.01
    assert rows[stale_wallet]["fresh_flow_rank"]["latest_trade_age_source"] == "computed_from_latest_btc5m_trade_ts"
    assert rows[stale_wallet]["fresh_flow_rank"]["p1_promotion_eligible"] is False
    assert rows[fresh_wallet]["fresh_flow_rank"]["latest_trade_age_h"] == 0.5
    assert rows[fresh_wallet]["fresh_flow_rank"]["p1_promotion_eligible"] is True


def test_full_pool_member_queue_staleness_uses_nested_remote_dataapi_timestamp() -> None:
    wallet = "0x8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a8a"
    replay = {
        "eligibility_status": "PASS",
        "paper_pnl_usd": 2.0,
        "copyable_buy_events": 20,
        "candidate_clob_backed_orders": 20,
    }

    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 1, "top_count": 1},
            "top_candidates": [{"wallet": wallet, "resolved_pnl": 2.0, "complementary_hours_utc": [4]}],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 1, "promotable_replays": 1},
            "candidates": [{"wallet": wallet, "candidate_id": "fresh", "paper_replay": replay}],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        fresh_flow_probe={
            "generated_at": "2000-01-01T00:00:00Z",
            "remote_dataapi_24h": {
                "generated_at": "2099-01-01T00:00:00Z",
                "rows": [
                    {
                        "wallet": wallet,
                        "btc5m_buys_24h": 3,
                        "btc5m_trades_24h": 4,
                        "policy_compatible_inband_buy_rows_24h": 3,
                        "latest_trade_age_h": 0.5,
                        "pass_admission_threshold": True,
                    }
                ],
            },
        },
        limit=10,
    )

    assert payload["summary"]["fresh_flow_probe_generated_at_source"] == "remote_dataapi_24h"
    assert payload["summary"]["fresh_flow_probe_stale_gt_6h"] is False


def test_full_pool_member_queue_keeps_below_floor_pass_paper_only() -> None:
    wallet = "0x2424242424242424242424242424242424242424"
    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 1, "top_count": 1},
            "top_candidates": [{"wallet": wallet, "resolved_pnl": 1.0, "complementary_fills": 8}],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 1, "promotable_replays": 1},
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "candidate_below_floor",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 1.5,
                        "copyable_buy_events": 8,
                        "candidate_clob_backed_orders": 8,
                    },
                }
            ],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["ready_for_live"] is False
    assert ">=20 copyable/CLOB-backed BUYs" in row["next_action"]
    assert payload["summary"]["ready_for_live"] == 0


def test_full_pool_member_queue_marks_below_floor_clearance_ready() -> None:
    wallet = "0x2525252525252525252525252525252525252525"
    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 1, "top_count": 1},
            "top_candidates": [{"wallet": wallet, "resolved_pnl": 1.0, "complementary_fills": 8}],
        },
        replay_payload={
            "replay_summary": {"candidate_count": 1, "promotable_replays": 0},
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "candidate_clear_below_floor",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 1.5,
                        "copyable_buy_events": 8,
                        "candidate_clob_backed_orders": 8,
                    },
                }
            ],
        },
        rotation_state={"decision": {"action": "WATCH"}},
        clearance_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "classification": "CLEAR",
                    "queue_rank": 29,
                    "metrics": {
                        "paper_pnl_usd": 1.5,
                        "copyable_buy_events": 8,
                        "candidate_clob_backed_orders": 8,
                        "resolved_orders": 8,
                        "reject_ratio": 0.27,
                        "unresolved_filled_order_count": 0,
                    },
                    "window_coverage": {"missing_resolution_market_count": 0},
                }
            ]
        },
        fresh_flow_probe=_fresh_flow_probe(wallet),
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["ready_for_live"] is True
    assert row["clearance_ready"] is True
    assert row["clearance"]["queue_rank"] == 29
    assert row["next_action"] == "eligible_for_half_size_fable_pin_from_clearance"
    assert payload["summary"]["ready_for_live"] == 1
    assert payload["summary"]["clearance_ready"] == 1


def test_full_pool_member_queue_adds_strict_replay_pass_outside_shortlist() -> None:
    wallet = "0x3333333333333333333333333333333333333333"
    payload = build_queue(
        shortlist={"summary": {"pool_after_active_set_exclusion": 0, "top_count": 0}, "top_candidates": []},
        replay_payload={
            "replay_summary": {"candidate_count": 1, "promotable_replays": 1},
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "strict_pass",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 2.0,
                        "paper_orders": 24,
                        "resolved_orders": 20,
                        "copyable_buy_events": 20,
                        "candidate_clob_backed_orders": 20,
                        "policy_id": "policy_a",
                    },
                }
            ],
        },
        rotation_state={"decision": {"action": "FABLE_ROTATION_DECISION_READY"}},
        fresh_flow_probe=_fresh_flow_probe(wallet),
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["wallet"] == wallet
    assert row["queue_source"] == "strict_replay_pass"
    assert row["ready_for_live"] is True
    assert payload["summary"]["ready_for_live"] == 1


def test_full_pool_member_queue_keeps_below_floor_strict_replay_visible() -> None:
    wallet = "0x4444444444444444444444444444444444444444"
    payload = build_queue(
        shortlist={"summary": {"pool_after_active_set_exclusion": 0, "top_count": 0}, "top_candidates": []},
        replay_payload={
            "replay_summary": {"candidate_count": 1, "promotable_replays": 1},
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "strict_pass_below_floor",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 2.0,
                        "paper_orders": 11,
                        "resolved_orders": 8,
                        "copyable_buy_events": 8,
                        "candidate_clob_backed_orders": 8,
                        "policy_id": "policy_a",
                    },
                }
            ],
        },
        rotation_state={"decision": {"action": "FABLE_ROTATION_DECISION_READY"}},
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["wallet"] == wallet
    assert row["queue_source"] == "strict_replay_pass"
    assert row["ready_for_live"] is False
    assert ">=20 copyable/CLOB-backed BUYs" in row["next_action"]
    assert payload["summary"]["ready_for_live"] == 0


def test_full_pool_member_queue_adds_clearance_ready_outside_shortlist() -> None:
    wallet = "0x4545454545454545454545454545454545454545"
    payload = build_queue(
        shortlist={"summary": {"pool_after_active_set_exclusion": 0, "top_count": 0}, "top_candidates": []},
        replay_payload={
            "replay_summary": {"candidate_count": 1, "promotable_replays": 0},
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "clearance_only",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 2.0,
                        "paper_orders": 11,
                        "resolved_orders": 4,
                        "copyable_buy_events": 4,
                        "candidate_clob_backed_orders": 4,
                        "policy_id": "policy_a",
                    },
                }
            ],
        },
        rotation_state={"decision": {"action": "FABLE_ROTATION_DECISION_READY"}},
        clearance_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "classification": "CLEAR",
                    "queue_rank": 54,
                    "metrics": {
                        "paper_pnl_usd": 2.0,
                        "copyable_buy_events": 4,
                        "candidate_clob_backed_orders": 4,
                        "resolved_orders": 4,
                        "reject_ratio": 0.33,
                        "unresolved_filled_order_count": 0,
                    },
                    "window_coverage": {"missing_resolution_market_count": 0},
                }
            ]
        },
        fresh_flow_probe=_fresh_flow_probe(wallet),
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["wallet"] == wallet
    assert row["queue_source"] == "strict_replay_pass"
    assert row["ready_for_live"] is True
    assert row["clearance_ready"] is True
    assert payload["summary"]["ready_for_live"] == 1


def test_full_pool_member_queue_clears_unknown_liveness_ready_rows() -> None:
    wallet = "0x4646464646464646464646464646464646464646"
    payload = build_queue(
        shortlist={"summary": {"pool_after_active_set_exclusion": 0, "top_count": 0}, "top_candidates": []},
        replay_payload={
            "replay_summary": {"candidate_count": 1, "promotable_replays": 0},
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "clearance_unknown_liveness",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 2.0,
                        "paper_orders": 11,
                        "resolved_orders": 4,
                        "copyable_buy_events": 4,
                        "candidate_clob_backed_orders": 4,
                    },
                }
            ],
        },
        rotation_state={"decision": {"action": "FABLE_ROTATION_DECISION_READY"}},
        clearance_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "classification": "CLEAR",
                    "metrics": {
                        "paper_pnl_usd": 2.0,
                        "copyable_buy_events": 4,
                        "candidate_clob_backed_orders": 4,
                        "resolved_orders": 4,
                    },
                    "window_coverage": {"missing_resolution_market_count": 0},
                }
            ]
        },
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["ready_for_live_before_bench_liveness_purge"] is True
    assert row["ready_for_live"] is False
    assert row["bench_liveness"]["status"] == "UNKNOWN_NO_REMOTE_LIVENESS"
    assert row["bench_tier"] == "unknown_remote_liveness"
    assert "liveness refresh" in row["next_action"]
    assert payload["summary"]["ready_for_live"] == 0
    assert payload["summary"]["unknown_remote_liveness"] == 1


def test_full_pool_member_queue_marks_adjudicated_zero_btc5m_not_fresh() -> None:
    wallet = "0x4747474747474747474747474747474747474747"
    payload = build_queue(
        shortlist={"summary": {"pool_after_active_set_exclusion": 0, "top_count": 0}, "top_candidates": []},
        replay_payload={
            "replay_summary": {"candidate_count": 1, "promotable_replays": 0},
            "candidates": [
                {
                    "wallet": wallet,
                    "candidate_id": "clearance_zero_btc5m",
                    "paper_replay": {
                        "eligibility_status": "PASS",
                        "paper_pnl_usd": 2.0,
                        "copyable_buy_events": 4,
                        "candidate_clob_backed_orders": 4,
                    },
                }
            ],
        },
        rotation_state={"decision": {"action": "FABLE_ROTATION_DECISION_READY"}},
        clearance_payload={
            "candidates": [
                {
                    "wallet": wallet,
                    "classification": "CLEAR",
                    "metrics": {
                        "paper_pnl_usd": 2.0,
                        "copyable_buy_events": 4,
                        "candidate_clob_backed_orders": 4,
                        "resolved_orders": 4,
                    },
                    "window_coverage": {"missing_resolution_market_count": 0},
                }
            ]
        },
        fresh_flow_probe={
            "kind": "queue_remote_dataapi_fresh_flow_probe",
            "generated_at": "2026-07-14T00:16:28Z",
            "rows": [
                {
                    "wallet": wallet,
                    "status": "PASS",
                    "btc5m_buys_24h": 0,
                    "btc5m_trades_24h": 0,
                    "latest_trade_age_h": None,
                    "pass_admission_threshold": False,
                    "coverage_complete_24h": True,
                    "remote_rows_saturated": False,
                }
            ],
        },
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["ready_for_live"] is False
    assert row["bench_liveness"]["status"] == "DORMANT_NOT_FRESH"
    assert row["bench_liveness"]["zero_btc5m_adjudication"]["coverage_complete_24h"] is True
    assert row["bench_tier"] == "dormant_not_fresh"
    assert payload["summary"]["dormant_not_fresh"] == 1
    assert payload["summary"]["unknown_remote_liveness"] == 0


def test_full_pool_member_queue_applies_breadth_dispositions_to_ready_rows() -> None:
    denied_wallet = "0x4545454545454545454545454545454545454545"
    measure_wallet = "0x5656565656565656565656565656565656565656"
    stale_wallet = "0x6767676767676767676767676767676767676767"
    payload = build_queue(
        shortlist={"summary": {"pool_after_active_set_exclusion": 0, "top_count": 0}, "top_candidates": []},
        replay_payload={
            "replay_summary": {"candidate_count": 3, "promotable_replays": 0},
            "candidates": [
                {
                    "wallet": denied_wallet,
                    "candidate_id": "denied_clearance",
                    "paper_replay": {"policy_id": "policy_a"},
                },
                {
                    "wallet": measure_wallet,
                    "candidate_id": "measurement_clearance",
                    "paper_replay": {"policy_id": "policy_a"},
                },
                {
                    "wallet": stale_wallet,
                    "candidate_id": "stale_clearance",
                    "paper_replay": {"policy_id": "policy_a"},
                },
            ],
        },
        rotation_state={"decision": {"action": "FABLE_ROTATION_DECISION_READY"}},
        clearance_payload={
            "candidates": [
                {
                    "wallet": denied_wallet,
                    "classification": "CLEAR",
                    "metrics": {
                        "paper_pnl_usd": 2.0,
                        "copyable_buy_events": 10,
                        "candidate_clob_backed_orders": 10,
                        "resolved_orders": 10,
                    },
                    "window_coverage": {"missing_resolution_market_count": 0},
                },
                {
                    "wallet": measure_wallet,
                    "classification": "CLEAR",
                    "metrics": {
                        "paper_pnl_usd": 3.0,
                        "copyable_buy_events": 12,
                        "candidate_clob_backed_orders": 12,
                        "resolved_orders": 12,
                    },
                    "window_coverage": {"missing_resolution_market_count": 0},
                },
                {
                    "wallet": stale_wallet,
                    "classification": "CLEAR",
                    "metrics": {
                        "paper_pnl_usd": 4.0,
                        "copyable_buy_events": 13,
                        "candidate_clob_backed_orders": 13,
                        "resolved_orders": 13,
                    },
                    "window_coverage": {"missing_resolution_market_count": 0},
                },
            ]
        },
        breadth_dispositions={
            "dispositions": [
                {
                    "wallet": denied_wallet,
                    "status": "DENIED_READMISSION_TODAY",
                    "reason": "weekday PROVEN-NEGATIVE",
                    "expires_at": "2026-07-14T00:00:00Z",
                },
                {
                    "wallet": measure_wallet,
                    "status": "MEASUREMENT_ONLY_TEMPORAL_CLOSURE",
                    "reason": "active weekday UNPROVEN",
                },
                {
                    "wallet": stale_wallet,
                    "status": "DENIED_STALE_AFTER_ADDRESS_FORM_REPROBE",
                    "reason": "no BTC-5m trades inside 24h fresh-flow window",
                },
            ]
        },
        now_ts=1_783_936_800.0,
        limit=10,
    )

    rows = {row["wallet"]: row for row in payload["ranked_members"]}
    assert payload["summary"]["ready_for_live"] == 0
    assert payload["summary"]["breadth_disposition_counts"] == {
        "DENIED_READMISSION_TODAY": 1,
        "DENIED_STALE_AFTER_ADDRESS_FORM_REPROBE": 1,
        "MEASUREMENT_ONLY_TEMPORAL_CLOSURE": 1,
    }
    assert rows[denied_wallet]["ready_for_live_before_breadth_disposition"] is True
    assert rows[denied_wallet]["ready_for_live"] is False
    assert rows[measure_wallet]["breadth_disposition"]["status"] == "MEASUREMENT_ONLY_TEMPORAL_CLOSURE"
    assert "temporal measurement" in rows[measure_wallet]["next_action"]
    assert rows[stale_wallet]["ready_for_live"] is False
    assert rows[stale_wallet]["breadth_disposition"]["status"] == "DENIED_STALE_AFTER_ADDRESS_FORM_REPROBE"
    assert "fresh-flow" in rows[stale_wallet]["next_action"]


def test_full_pool_member_queue_adds_positive_replay_watch_rows_without_promoting() -> None:
    watch_wallet = "0x5555555555555555555555555555555555555555"
    active_wallet = "0x6666666666666666666666666666666666666666"
    demoted_wallet = "0x7777777777777777777777777777777777777777"
    payload = build_queue(
        shortlist={"summary": {"pool_after_active_set_exclusion": 0, "top_count": 0}, "top_candidates": []},
        replay_payload={
            "replay_summary": {"candidate_count": 3, "promotable_replays": 0},
            "candidates": [
                {
                    "wallet": watch_wallet,
                    "candidate_id": "positive_watch",
                    "paper_replay": {
                        "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "paper_pnl_usd": 3.25,
                        "paper_orders": 12,
                        "resolved_orders": 6,
                        "copyable_buy_events": 6,
                        "candidate_clob_backed_orders": 6,
                        "failure_reasons": ["candidate_unresolved_ratio_above_maximum"],
                    },
                },
                {
                    "wallet": active_wallet,
                    "candidate_id": "active_positive_watch",
                    "paper_replay": {
                        "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "paper_pnl_usd": 10.0,
                        "copyable_buy_events": 10,
                        "candidate_clob_backed_orders": 10,
                    },
                },
                {
                    "wallet": demoted_wallet,
                    "candidate_id": "demoted_positive_watch",
                    "paper_replay": {
                        "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                        "paper_pnl_usd": 9.0,
                        "copyable_buy_events": 9,
                        "candidate_clob_backed_orders": 9,
                    },
                },
            ],
        },
        rotation_state={"decision": {"action": "RETAIN_NO_ELIGIBLE_CANDIDATE"}},
        excluded_wallets={active_wallet, demoted_wallet},
        limit=10,
    )

    assert [row["wallet"] for row in payload["ranked_members"]] == [watch_wallet]
    row = payload["ranked_members"][0]
    assert row["queue_source"] == "positive_replay_watch"
    assert row["ready_for_live"] is False
    assert row["next_action"] == "refresh/attach market resolutions, then rescore stored replay orders"
    assert payload["summary"]["queue_depth"] == 1
    assert payload["summary"]["ready_for_live"] == 0


def test_full_pool_member_queue_bridges_market_cohort_replay_as_bench_only() -> None:
    wallet = "0x8888888888888888888888888888888888888888"
    payload = build_queue(
        shortlist={"summary": {"pool_after_active_set_exclusion": 0, "top_count": 0}, "top_candidates": []},
        replay_payload={"replay_summary": {"candidate_count": 0, "promotable_replays": 0}, "candidates": []},
        rotation_state={"decision": {"action": "CONTINUE_BUILD_MEASUREMENT"}},
        market_cohort_replay={
            "generated_at": "2026-07-13T19:00:00Z",
            "live_ready_picks": [
                {
                    "wallet": wallet,
                    "live_ready": True,
                    "status": "LIVE_READY_SHADOW_PICK",
                    "paper_pnl_usd": 1040.9026,
                    "roi_pct": 4.196462,
                    "resolved_copyable_events": 1475,
                    "copyable_buy_events": 1681,
                    "unresolved_copyable_events": 206,
                    "win_rate_pct": 52.40678,
                    "unique_markets": 39,
                    "first_trade_ts": "2026-07-13T16:05:25Z",
                    "latest_trade_ts": "2026-07-13T18:41:13Z",
                    "stake_usd": 24804.2874,
                    "history_rows_seen": 2000,
                    "pagination_cap_reached": False,
                }
            ],
        },
        fresh_flow_probe={
            "rows": [
                {
                    "wallet": wallet,
                    "btc5m_buys_24h": 3,
                    "latest_trade_age_h": 1.0,
                    "pass_admission_threshold": True,
                }
            ]
        },
        limit=10,
    )

    row = payload["ranked_members"][0]
    assert row["wallet"] == wallet
    assert row["queue_source"] == "market_cohort_replay"
    assert row["ready_for_live"] is False
    assert row["bench_tier"] == "shadow_clock_held"
    assert row["shadow_clock_held"] is True
    assert row["bench_liveness"]["status"] == "SHADOW_CLOCK_HELD"
    assert row["bench_liveness"]["shadow_clock"]["ready_for_live_semantics_changed"] is False
    assert row["market_cohort_replay"]["paper_pnl_usd"] == 1040.9026
    assert row["market_cohort_replay"]["replay_window_hours"] == 2.596667
    assert row["market_cohort_replay"]["resolved_copyable_events"] == 1475
    assert row["next_action"].startswith("wait for routing-shadow clock re-adjudication")
    assert payload["summary"]["shadow_clock_held"] == 1
    assert payload["summary"]["bench_alive_not_ready"] == 0
    assert payload["summary"]["market_cohort_bridge_candidates"] == 1
    assert payload["summary"]["market_cohort_bridge_ranked"] == 1
    assert payload["summary"]["market_cohort_bridge_defects"] == 0
    assert payload["summary"]["market_cohort_bridge_source_live_ready_picks"] == 1
    assert payload["summary"]["market_cohort_bridge_bridged"] == 1
    assert payload["summary"]["market_cohort_bridge_excluded"] == 0
    assert payload["summary"]["market_cohort_bridge_excluded_reason_counts"] == {}


def test_full_pool_member_queue_accounts_market_cohort_bridge_exclusions() -> None:
    existing_wallet = "0x7777777777777777777777777777777777777777"
    bridged_wallet = "0x8888888888888888888888888888888888888888"
    denylisted_wallet = "0x9999999999999999999999999999999999999999"
    pick = {
        "live_ready": True,
        "status": "LIVE_READY_SHADOW_PICK",
        "paper_pnl_usd": 1040.9026,
        "roi_pct": 4.196462,
        "resolved_copyable_events": 1475,
        "copyable_buy_events": 1681,
        "unresolved_copyable_events": 206,
        "win_rate_pct": 52.40678,
        "unique_markets": 39,
        "first_trade_ts": "2026-07-13T16:05:25Z",
        "latest_trade_ts": "2026-07-13T18:41:13Z",
        "stake_usd": 24804.2874,
        "history_rows_seen": 2000,
        "pagination_cap_reached": False,
    }
    payload = build_queue(
        shortlist={
            "summary": {"pool_after_active_set_exclusion": 0, "top_count": 1},
            "top_candidates": [{"wallet": existing_wallet, "resolved_pnl": 1.0}],
        },
        replay_payload={"replay_summary": {"candidate_count": 0, "promotable_replays": 0}, "candidates": []},
        rotation_state={"decision": {"action": "CONTINUE_BUILD_MEASUREMENT"}},
        market_cohort_replay={
            "generated_at": "2026-07-13T19:00:00Z",
            "live_ready_picks": [
                {**pick, "wallet": existing_wallet},
                {**pick, "wallet": bridged_wallet},
                {**pick, "wallet": denylisted_wallet},
            ],
        },
        excluded_wallets={denylisted_wallet},
        limit=10,
    )

    assert payload["summary"]["market_cohort_bridge_source_live_ready_picks"] == 3
    assert payload["summary"]["market_cohort_bridge_bridged"] == 1
    assert payload["summary"]["market_cohort_bridge_excluded"] == 2
    assert payload["summary"]["market_cohort_bridge_excluded_reason_counts"] == {
        "already_in_queue": 1,
        "excluded_wallet_denylist": 1,
    }
    assert payload["summary"]["market_cohort_bridge_excluded_reason_wallets"] == {
        "already_in_queue": [existing_wallet],
        "excluded_wallet_denylist": [denylisted_wallet],
    }


def test_full_pool_member_queue_limit_zero_writes_all_candidates() -> None:
    candidates = []
    for idx in range(3):
        wallet = f"0x{idx + 1:040x}"
        candidates.append(
            {
                "wallet": wallet,
                "candidate_id": f"positive_watch_{idx}",
                "paper_replay": {
                    "eligibility_status": "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY",
                    "paper_pnl_usd": 1.0 + idx,
                    "paper_orders": 12,
                    "resolved_orders": 6,
                    "copyable_buy_events": 6,
                    "candidate_clob_backed_orders": 6,
                    "failure_reasons": ["candidate_unresolved_ratio_above_maximum"],
                },
            }
        )

    payload = build_queue(
        shortlist={"summary": {"pool_after_active_set_exclusion": 0, "top_count": 0}, "top_candidates": []},
        replay_payload={"replay_summary": {"candidate_count": 3, "promotable_replays": 0}, "candidates": candidates},
        rotation_state={"decision": {"action": "CONTINUE_BUILD_MEASUREMENT"}},
        limit=0,
    )

    assert payload["summary"]["queue_depth"] == 3
    assert len(payload["ranked_members"]) == 3


def test_full_pool_member_queue_admits_only_explicit_registry_observation_wallets() -> None:
    admitted = "0x00000000000000000000000000000000000000a1"
    not_admitted = "0x00000000000000000000000000000000000000a2"

    def packet(wallet: str) -> dict:
        return {
            "wallet": wallet,
            "candidate_id": f"registry_{wallet[-4:]}",
            "paper_policy_id": "registry_weekday_f1",
            "fresh_own_source_buy_rows_30m": 12,
            "temporal_evidence": {
                "classification": "WEEKDAY-ONLY",
                "matched_slice": {"resolved_trades": 250, "pnl_usd": 25.0, "roi_pct": 2.0},
            },
        }

    payload = build_queue(
        shortlist={"summary": {}, "top_candidates": []},
        replay_payload={"replay_summary": {}, "candidates": []},
        rotation_state={"decision": {"action": "CONTINUE_BUILD_MEASUREMENT"}},
        cohort_admission={"packets": [packet(admitted), packet(not_admitted)]},
        observation_admissions={"wallets": [admitted]},
        limit=0,
    )

    observations = [row for row in payload["ranked_members"] if row.get("observation_member")]
    assert [row["wallet"] for row in observations] == [admitted]
    assert observations[0]["paper_only"] is True
    assert observations[0]["live_orders_allowed"] is False
    assert observations[0]["ready_for_live"] is False
    assert payload["summary"]["registry_admission_observation_members"] == 1
