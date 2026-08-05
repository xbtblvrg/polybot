from __future__ import annotations

import json

from scripts.report_rotation_evidence_pack import build_pack


def test_rotation_evidence_pack_marks_stale_suppressed_member_for_review() -> None:
    guard = {
        "candidate_id": "member-a",
        "source_wallet": "0xaaa",
        "policy_id": "policy",
        "active_set": {
            "generated_at": "2026-07-07T00:00:00+00:00",
            "members": [
                {"candidate_id": "member-a", "source_wallet": "0xaaa", "policy_id": "policy"},
            ],
        },
    }
    deadman = {
        "status": "OK",
        "eligible_drought_s": 7600.0,
        "eligible_drought_status": "ROTATION_EVIDENCE_DUE",
        "member_signal_age": {
            "0xaaa": {
                "eligible_intents": 0,
                "suppressed_intents": 2,
                "signal_age_count": 2,
                "signal_age_p90_s": 140.0,
            }
        },
    }
    queue = {
        "generated_at": "2026-07-07T00:00:00+00:00",
        "summary": {"ready_for_live": 1},
        "ranked_members": [
            {"queue_rank": 1, "wallet": "0xaaa", "ready_for_live": True},
            {"queue_rank": 2, "wallet": "0xbbb", "ready_for_live": True, "resolved_pnl": 1.2},
        ],
    }
    digest = {
        "active_set": {"live_members_today": [{"wallet": "0xaaa", "pnl_usd": -1.0}]},
        "order_flow_deadman": {
            "eligible_drought_s": 7601.0,
            "eligible_drought_status": "ROTATION_EVIDENCE_DUE",
            "latest_order_ts": None,
        },
    }

    pack = build_pack(
        guard_state=guard,
        deadman_state=deadman,
        queue_state=queue,
        leaderboard_state={"summary": {"unique_wallets": 10}},
        state_digest=digest,
        top_n=5,
    )

    assert pack["no_rotation_executed"] is True
    assert pack["artifact_places_no_orders"] is True
    assert pack["guard_live_orders_allowed"] is False
    assert pack["flow_truth_consistency"]["status"] == "PASS"
    assert pack["member_signal_age_table"][0]["recommendation"] == "ROTATE_REVIEW_STALE_SUPPRESSED_FLOW_AT_0139Z_RULING"
    assert pack["candidate_ranking_delta"]["top_non_active_candidates"][0]["wallet"] == "0xbbb"


def test_rotation_evidence_pack_holds_member_with_eligible_flow() -> None:
    guard = {"active_set": {"members": [{"candidate_id": "member-a", "source_wallet": "0xaaa"}]}}
    deadman = {
        "eligible_drought_s": 1.0,
        "latest_order_ts": "2026-07-07T21:35:24+00:00",
        "member_signal_age": {"0xaaa": {"eligible_intents": 1, "suppressed_intents": 0}},
    }

    pack = build_pack(
        guard_state=guard,
        deadman_state=deadman,
        queue_state={"ranked_members": []},
        leaderboard_state={},
        state_digest={
            "order_flow_deadman": {
                "eligible_drought_s": 1.0,
                "latest_order_ts": "2026-07-07T21:35:24+00:00",
            }
        },
        top_n=5,
    )

    assert pack["member_signal_age_table"][0]["recommendation"] == "HOLD_HAS_ELIGIBLE_FLOW_WAIT_FOR_FAST_FEED_RULING"
    assert pack["recommendation_summary"]["rotate_review_members"] == 0


def test_rotation_evidence_pack_marks_active_live_flow_without_fresh_snapshot() -> None:
    guard = {"active_set": {"members": [{"candidate_id": "member-a", "source_wallet": "0xaaa"}]}}
    deadman = {
        "eligible_drought_s": 1.0,
        "latest_order_ts": "2026-07-07T21:35:24+00:00",
        "member_signal_age": {"0xaaa": {"eligible_intents": 0, "suppressed_intents": 0}},
    }

    pack = build_pack(
        guard_state=guard,
        deadman_state=deadman,
        queue_state={"ranked_members": []},
        leaderboard_state={},
        state_digest={
            "active_set": {
                "live_members_today": [
                    {"wallet": "0xaaa", "orders": 4, "fills": 2, "rejects": 1, "pnl_usd": -2.1}
                ]
            },
            "order_flow_deadman": {
                "eligible_drought_s": 1.0,
                "latest_order_ts": "2026-07-07T21:35:24+00:00",
            },
        },
        top_n=5,
    )

    row = pack["member_signal_age_table"][0]
    assert row["today_orders"] == 4
    assert row["recommendation"] == "HOLD_ACTIVE_LIVE_FLOW_WAIT_FOR_RAMP_RULING"


def test_rotation_evidence_pack_adds_dormancy_lookback() -> None:
    guard = {
        "active_set": {"members": [{"candidate_id": "member-a", "source_wallet": "0xaaa"}]},
        "window_participation": {
            "rows": [
                {
                    "source_wallet": "0xaaa",
                    "effective_latest_observed_ts": 1_700_000_000.0,
                }
            ]
        },
    }

    pack = build_pack(
        guard_state=guard,
        deadman_state={"member_signal_age": {}, "eligible_drought_s": 10.0},
        queue_state={"ranked_members": []},
        leaderboard_state={},
        state_digest={"order_flow_deadman": {"eligible_drought_s": 10.0}},
        top_n=5,
    )

    assert pack["member_signal_age_table"][0]["hours_since_last_observed_source_trade"] is not None


def test_rotation_evidence_pack_flags_stale_deadman_flow_truth_mismatch() -> None:
    pack = build_pack(
        guard_state={"active_set": {"members": []}},
        deadman_state={
            "eligible_drought_s": 307.5,
            "eligible_drought_status": "OK",
            "latest_order_ts": "2026-07-07T21:35:24+00:00",
        },
        queue_state={"ranked_members": []},
        leaderboard_state={},
        state_digest={
            "order_flow_deadman": {
                "eligible_drought_s": 8292.0,
                "eligible_drought_status": "ROTATION_EVIDENCE_DUE",
                "latest_order_ts": "2026-07-07T21:35:24+00:00",
            }
        },
        top_n=5,
    )

    consistency = pack["flow_truth_consistency"]
    assert consistency["status"] == "FAIL"
    assert consistency["eligible_drought_status_match"] is False
    assert consistency["eligible_drought_delta_s"] > 5.0


def test_rotation_evidence_pack_attributes_m6_eligible_but_unsubmitted_cycle(tmp_path) -> None:
    target_ts = "2026-07-08T00:01:34.038125+00:00"
    guard_events = tmp_path / "guard_events.jsonl"
    previous = {
        "event": "wallet_copy_live_guard_cycle",
        "generated_at": "2026-07-08T00:00:37.048973+00:00",
        "live_execution": {
            "drought_funnel": {
                "fresh_candidate_intents": 0,
                "orders_submitted": 0,
                "reject_taxonomy_counts": {"window:toxicity_protection": 1},
            },
            "profit_latency_suppression": {"input_intents": 0, "filtered_intents": 0, "output_intents": 0},
            "toxicity_protection": {"input_intents": 0, "blocked_intents": 0, "output_intents": 0},
        },
    }
    current = {
        "event": "wallet_copy_live_guard_cycle",
        "generated_at": target_ts,
        "live_execution": {
            "selected_candidate": {
                "candidate_id": "bucket_conc_251c1a2541f",
                "source_wallet": "0x251c1a283703beed41590b0875a8dcb8ddd1541f",
            },
            "drought_funnel": {
                "fresh_candidate_intents": 2,
                "orders_submitted": 0,
                "reject_taxonomy_counts": {
                    "toxicity_protection": 2,
                    "window:toxicity_protection": 3,
                },
            },
            "profit_latency_suppression": {"input_intents": 2, "filtered_intents": 0, "output_intents": 2},
            "toxicity_protection": {
                "input_intents": 2,
                "blocked_intents": 2,
                "output_intents": 0,
                "taxonomy_counts": {"toxicity_protection": 2},
                "sample_filtered_intents": [
                    {
                        "intent_id": "ci_down",
                        "market_slug": "btc-updown-5m-1783468800",
                        "price_bucket": "01_25_50",
                    },
                    {
                        "intent_id": "ci_up",
                        "market_slug": "btc-updown-5m-1783468800",
                        "price_bucket": "01_25_50",
                    },
                ],
            },
        },
    }
    guard_events.write_text("\n".join(json.dumps(row) for row in [previous, current]) + "\n")

    pack = build_pack(
        guard_state={"active_set": {"members": []}},
        deadman_state={"eligible_drought_s": 1.0},
        queue_state={"ranked_members": []},
        leaderboard_state={},
        state_digest={"order_flow_deadman": {"eligible_drought_s": 1.0}},
        top_n=5,
        guard_event_log_path=guard_events,
        eligible_target_ts=target_ts,
        guard_event_tail_bytes=4096,
    )

    details = pack["eligible_but_unsubmitted_attribution_details"]
    assert details["status"] == "PASS"
    assert details["post_filter_intents"] == 2
    assert details["orders_submitted"] == 0
    assert details["consuming_gate"] == "toxicity_protection"
    assert details["reject_delta_vs_prior"]["toxicity_protection"] == 2
    assert details["reject_delta_vs_prior"]["window:toxicity_protection"] == 2
    assert "consumed_by=toxicity_protection(blocked=2/2)" in pack["eligible_but_unsubmitted_attribution"]
