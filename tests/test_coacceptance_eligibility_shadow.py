from scripts.report_coacceptance_eligibility_shadow import TARGETS, build_packet


def test_shadow_attributes_zero_conversion_without_live_mutation() -> None:
    ee = TARGETS["ee888f"]["wallet"]
    a3 = TARGETS["a3e0"]["wallet"]
    guard = {
        "active_set_runtime": {
            "selected_member": {"source_wallet": "0x" + "a" * 40},
            "members": [
                {"source_wallet": ee, "enabled": True, "policy_id": "ee-policy"},
                {"source_wallet": a3, "enabled": True, "policy_id": "a3-policy"},
            ],
        }
    }
    deadman = {
        "status": "INCIDENT_POLICY_CHOKE",
        "can_trade": True,
        "policy_choke": {
            "status": "INCIDENT_POLICY_CHOKE",
            "rung_a_seat_read": {
                "rows": [
                    {"wallet": ee, "raw_own_source_buy_rows": 22, "policy_eligible_intents": 0, "accepted_live_orders": 0},
                    {"wallet": a3, "raw_own_source_buy_rows": 21, "policy_eligible_intents": 0, "accepted_live_orders": 0},
                ]
            },
        },
    }
    probes = {
        "ee888f": {"candidate_intent_summary": {"source_events": 2, "live_event_prefilter": {"skip_counts": {"inventory_window_state_stale": 1}}}},
        "a3e0": {"candidate_intent_summary": {"source_events": 21, "live_event_prefilter": {"policy_reject_counts": {"price_outside_policy": 7}, "skip_counts": {"market_closed_now": 14}}}},
    }
    packet = build_packet(
        deadman=deadman,
        guard=guard,
        evidence_by_name={"ee888f": {"status": "ACTIVATED_SELECTABLE"}, "a3e0": {"status": "ACTIVATED_SELECTABLE"}},
        probe_by_name=probes,
        generated_at="2026-07-21T00:25:00Z",
    )
    assert packet["status"] == "ACCRUING_ZERO_TARGET_CONVERSION"
    assert packet["live_mutation"] is False
    rows = {row["name"]: row for row in packet["rows"]}
    assert rows["ee888f"]["probe_funnel"]["dominant_observed_gate"] == "inventory_window_state_stale"
    assert rows["a3e0"]["probe_funnel"]["observed_gate_counts"] == {"market_closed_now": 14, "price_outside_policy": 7}


def test_shadow_flags_converting_target_for_op_no_wait_review() -> None:
    ee = TARGETS["ee888f"]["wallet"]
    packet = build_packet(
        deadman={
            "can_trade": True,
            "policy_choke": {"rung_a_seat_read": {"rows": [{"wallet": ee, "raw_own_source_buy_rows": 10, "policy_eligible_intents": 2, "accepted_live_orders": 1}]}},
        },
        guard={"active_set_runtime": {"members": [{"source_wallet": ee, "enabled": True}]}},
        evidence_by_name={"ee888f": {"status": "ACTIVATED_SELECTABLE"}},
        probe_by_name={},
        generated_at="2026-07-21T00:25:00Z",
    )
    assert packet["status"] == "CONVERTING_TARGET_PRESENT"
    assert packet["seat_action"] == "OP_NO_WAIT_REEVALUATE"
