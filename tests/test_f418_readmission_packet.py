from __future__ import annotations

from scripts.report_f418_readmission_packet import WALLET, build_packet


def test_event_trigger_without_positive_counterfactual_denies_activation() -> None:
    packet = build_packet(
        generated_at="2026-07-15T03:00:00Z",
        since_midnight={
            "generated_at": "2026-07-15T02:48:27Z",
            "summary": {
                "btc5m_buy_rows_since": 40,
                "btc5m_buy_windows_since": 2,
                "source_active_rows": 2,
                "source_active_windows": 1,
                "policy_eligible_rows": 0,
                "policy_eligible_windows": 0,
                "required_source_active_windows": 1,
                "source_active_tally_status": "PASS",
                "policy_eligible_tally_status": "PENDING",
            },
            "latest_rows": [{"event_iso": "2026-07-15T02:47:56Z"}],
        },
        last_24h={"generated_at": "2026-07-15T02:48:27Z", "summary": {"btc5m_buy_rows_since": 40}, "latest_rows": [{"event_iso": "2026-07-15T02:47:56Z"}]},
        member_since_midnight={"generated_at": "2026-07-15T02:48:27Z", "source_active_windows": 1},
        probe={
            "generated_at": "2026-07-15T02:51:41Z",
            "status": "CORRECTION",
            "selected_candidate": {"candidate_id": "cohort_alive_admit_f418d3a1", "source_wallet": WALLET},
            "candidate_intent_summary": {
                "candidate_intents": 2,
                "fresh_candidate_intents": 2,
                "fresh_candidate_intents_after_expected_fee_gate": 2,
                "fresh_candidate_intents_after_window_fill_cap": 0,
                "expected_fee_capture_gate": {"expected_fee_sum_usd": 0.17, "sample_intents": []},
            },
            "window_participation": {
                "rows": [
                    {
                        "market_slug": "btc-updown-5m-1784083800",
                        "miss_pending_market_lifecycle": True,
                        "dominant_skip_reason": "window_time_gte_180s",
                    }
                ]
            },
        },
        routing_shadow={
            "summary": {
                "extra_would_submit_post_fee_measurement": {
                    "by_member": {
                        WALLET: {
                            "measurable_resolved_intents": 0,
                            "post_fee_pnl_usd": 0.0,
                        }
                    }
                }
            }
        },
        scorecard={
            "lifetime_pnl_truth": {
                "by_member": {
                    WALLET: {
                        "orders": 13,
                        "fills": 9,
                        "rejects": 4,
                        "pnl_usd": -9.733318,
                    }
                }
            },
            "active_set_roster": {"members": [{"source_wallet": WALLET, "candidate_id": "cohort_alive_admit_f418d3a1"}]},
        },
        temporal={"classification": "FADING", "all": {"roi_pct": 1.932881}, "recent": {"roi_pct": -1.408665}},
        previous_packet={},
        paths={
            "since_midnight": "since.json",
            "last_24h": "last24.json",
            "member_since_midnight": "member.json",
            "probe": "probe.json",
            "routing_shadow": "routing_shadow.json",
            "scorecard": "scorecard.json",
            "temporal": "temporal.json",
            "previous_packet": "previous.json",
        },
    )

    assert packet["fresh_liveness"]["trigger_pass"] is True
    assert packet["status"] == "DENIED_BY_RULE_COUNTERFACTUAL_NOT_POSITIVE"
    gate = packet["counterfactual_gate_since_non_admissible_boundary"]
    assert gate["gate_pass"] is False
    assert "zero_resolved_post_fee_counterfactual_rows" in gate["live_probe"]["denial_reasons"]
    assert "current_probe_after_window_cap_zero" in gate["live_probe"]["denial_reasons"]
    assert "routing_shadow_resolved_intents_below_10" in gate["denial_reasons"]
    assert packet["invariants"]["guard_code_touched"] is False


def test_positive_routing_shadow_counterfactual_pre_rules_activation() -> None:
    packet = build_packet(
        generated_at="2026-07-16T05:40:00Z",
        since_midnight={
            "generated_at": "2026-07-16T05:30:00Z",
            "summary": {
                "source_active_windows": 1,
                "required_source_active_windows": 1,
                "source_active_tally_status": "PASS",
            },
            "latest_rows": [{"event_iso": "2026-07-15T11:46:48Z"}],
        },
        last_24h={"generated_at": "2026-07-16T05:30:00Z", "summary": {"source_active_windows": 1}, "latest_rows": [{"event_iso": "2026-07-15T11:46:48Z"}]},
        member_since_midnight={"generated_at": "2026-07-16T05:30:00Z", "source_active_windows": 1},
        probe={
            "generated_at": "2026-07-15T02:51:41Z",
            "candidate_intent_summary": {
                "candidate_intents": 2,
                "fresh_candidate_intents": 2,
                "fresh_candidate_intents_after_expected_fee_gate": 2,
                "fresh_candidate_intents_after_window_fill_cap": 0,
            },
            "window_participation": {"rows": []},
        },
        routing_shadow={
            "generated_at": "2026-07-16T05:30:59Z",
            "summary": {
                "extra_would_submit_post_fee_measurement": {
                    "by_member": {
                        WALLET: {
                            "fee_gated_intents": 16,
                            "resolved_intents": 15,
                            "measurable_resolved_intents": 15,
                            "unresolved_intents": 1,
                            "unmeasured_resolved_intents": 0,
                            "wins": 8,
                            "losses": 7,
                            "pre_fee_pnl_usd": 5.83403,
                            "expected_fee_usd_sum": 1.289624,
                            "post_fee_pnl_usd": 4.544406,
                        }
                    }
                }
            },
        },
        scorecard={
            "lifetime_pnl_truth": {"by_member": {WALLET: {"orders": 13, "fills": 9, "pnl_usd": -9.733318}}},
            "active_set_roster": {"members": []},
        },
        temporal={"classification": "FADING", "all": {"roi_pct": 1.932881}, "recent": {"roi_pct": -1.408665}},
        previous_packet={},
        paths={
            "since_midnight": "since.json",
            "last_24h": "last24.json",
            "member_since_midnight": "member.json",
            "probe": "probe.json",
            "routing_shadow": "routing_shadow.json",
            "scorecard": "scorecard.json",
            "temporal": "temporal.json",
            "previous_packet": "previous.json",
        },
    )

    gate = packet["counterfactual_gate_since_non_admissible_boundary"]
    assert packet["status"] == "PRE_RULED_ADMIT_F418_ACTIVATION"
    assert packet["decision"].startswith("ADMIT_UNDER_FABLE_20260716T0528_BRANCH_A")
    assert gate["gate_pass"] is True
    assert gate["basis"] == "routing_shadow_member_attribution"
    assert gate["routing_shadow_member"]["measurable_resolved_intents"] == 15
    assert gate["routing_shadow_member"]["post_fee_pnl_usd"] == 4.544406
    assert packet["sizing_if_fable_allows_activation"]["min_order_usd"] == 1.0
    assert packet["next_action"].startswith("Apply the pre-ruled branch")


def test_stale_liveness_can_never_admit() -> None:
    base = {
        "generated_at": "2026-07-18T00:00:00Z",
        "summary": {"source_active_windows": 1},
    }
    packet = build_packet(
        generated_at="2026-07-20T00:00:01Z",
        since_midnight=base,
        last_24h=base,
        member_since_midnight={"generated_at": "2026-07-18T00:00:00Z", "source_active_windows": 1},
        probe={"candidate_intent_summary": {}, "window_participation": {"rows": []}},
        routing_shadow={"summary": {"extra_would_submit_post_fee_measurement": {"by_member": {WALLET: {"measurable_resolved_intents": 15, "post_fee_pnl_usd": 4.0}}}}},
        scorecard={}, temporal={}, previous_packet={},
        paths={key: f"{key}.json" for key in ("since_midnight", "last_24h", "member_since_midnight", "probe", "routing_shadow", "scorecard", "temporal", "previous_packet")},
    )

    assert packet["status"] == "DORMANT_SOURCE_EXPIRED"
    assert packet["fresh_liveness"]["stale_input"] is True
    assert packet["decision"].startswith("DO_NOT_ACTIVATE")
