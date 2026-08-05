from scripts.report_f418_acceptance_funnel import _latest_selected_wallet, build_report


WALLET = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"


def test_latest_selected_wallet_follows_guard_cycle_order():
    assert _latest_selected_wallet(
        [
            {"event": "wallet_copy_live_guard_cycle", "source_wallet": WALLET},
            {"event": "unrelated", "source_wallet": "0xignored"},
            {"event": "wallet_copy_live_guard_cycle", "source_wallet": "0xLATEST"},
        ]
    ) == "0xlatest"


def _routing(intent_id: str, observed_ts: float):
    return {
        "source_wallet": WALLET,
        "intent_id": intent_id,
        "observed_ts": observed_ts,
        "dominant_skip_reason": "eligible",
        "market_slug": f"btc-updown-5m-{int(observed_ts // 300) * 300}",
    }


def _cycle(intent_id: str):
    return {
        "event": "wallet_copy_live_guard_cycle",
        "generated_at": "2026-07-21T10:00:01Z",
        "source_wallet": WALLET,
        "cycle": 7,
        "live_execution": {
            "candidate_intent_summary": {
                "latest_candidate_intent_runtime": {"intent_id": intent_id},
                "profit_latency_suppression": {
                    "sample_filtered_intents": [{"intent_id": intent_id}]
                },
            }
        },
    }


def test_report_attributes_gate_and_reports_ledger_acceptance_separately():
    observed = 1_784_628_000.0
    report = build_report(
        routing_shadow={"fee_gated_measurement_rows": [_routing("ci_gate", observed)]},
        guard_cycles=[_cycle("ci_gate")],
        execution_events=[],
        ledger={
            "orders": [
                {
                    "source_wallet": WALLET,
                    "intent_id": "ci_fill",
                    "submitted_at": "2026-07-21T10:00:02Z",
                    "status": "FILLED",
                    "order_id": "0x1",
                }
            ]
        },
        source_wallet=WALLET,
        day="2026-07-21",
        generated_at="2026-07-21T10:01:00Z",
    )
    assert report["policy_eligible_unique_intents"] == 1
    assert report["accepted_live_orders"] == 0
    assert report["terminal_stage_counts"] == {"profit_latency_suppression": 1}
    assert report["canonical_live_ledger"]["accepted_orders_today"] == 1
    assert report["canonical_live_ledger"]["routing_to_live_intent_join_gap"] == 1
    assert report["status"] == "PASS_WITH_JOIN_GAP"


def test_report_names_exchange_rejection_error():
    observed = 1_784_628_000.0
    report = build_report(
        routing_shadow={"fee_gated_measurement_rows": [_routing("ci_reject", observed)]},
        guard_cycles=[],
        execution_events=[],
        ledger={
            "orders": [
                {
                    "source_wallet": WALLET,
                    "intent_id": "ci_reject",
                    "submitted_at": "2026-07-21T10:00:02Z",
                    "status": "REJECTED",
                    "trade_result": {"error": "precision cap exceeded"},
                }
            ]
        },
        source_wallet=WALLET,
        day="2026-07-21",
        generated_at="2026-07-21T10:01:00Z",
    )
    assert report["terminal_stage_counts"] == {"exchange_rejected": 1}
    assert report["rows"][0]["error_class"] == "precision cap exceeded"


def test_report_classifies_routing_only_intent_as_not_selected_live_seat():
    observed = 1_784_628_000.0
    row = _routing("ci_shadow_only", observed)
    row["market_slug"] = "btc-updown-5m-1784628000"
    report = build_report(
        routing_shadow={"fee_gated_measurement_rows": [row]},
        guard_cycles=[],
        execution_events=[],
        ledger={"orders": []},
        source_wallet=WALLET,
        day="2026-07-21",
        generated_at="2026-07-21T10:01:00Z",
    )
    assert report["status"] == "PASS"
    assert report["terminal_stage_counts"] == {"not_selected_live_seat": 1}


def test_report_classifies_routing_eligibility_observed_after_market_close():
    observed = 1_784_628_301.0
    row = _routing("ci_stale", observed)
    row["market_slug"] = "btc-updown-5m-1784628000"
    report = build_report(
        routing_shadow={"fee_gated_measurement_rows": [row]},
        guard_cycles=[],
        execution_events=[],
        ledger={"orders": []},
        source_wallet=WALLET,
        day="2026-07-21",
        generated_at="2026-07-21T10:01:00Z",
    )
    assert report["terminal_stage_counts"] == {
        "routing_shadow_eligible_after_market_close": 1
    }


def test_selected_intent_with_terminal_gate_is_fully_attributed():
    observed = 1_784_628_000.0
    report = build_report(
        routing_shadow={"fee_gated_measurement_rows": [_routing("ci_gate", observed)]},
        guard_cycles=[_cycle("ci_gate")],
        execution_events=[],
        ledger={"orders": []},
        source_wallet=WALLET,
        day="2026-07-21",
        generated_at="2026-07-21T10:01:00Z",
        since_at="2026-07-21T09:59:00Z",
    )
    assert report["selected_policy_eligible_unique_intents"] == 1
    assert report["selected_member_attribution"]["status"] == "PASS"
    assert report["rows"][0]["attribution_class"] == "guard_terminal"


def test_entry_band_shadow_annotation_falls_through_to_downstream_terminal():
    observed = 1_784_628_000.0
    intent_id = "ci_shadow_then_latency"
    report = build_report(
        routing_shadow={"fee_gated_measurement_rows": [_routing(intent_id, observed)]},
        guard_cycles=[_cycle(intent_id)],
        execution_events=[
            {
                "event": "wallet_copy_live_entry_price_band_gate_counterfactual",
                "intent_id": intent_id,
                "event_type": "COUNTERFACTUAL_SHADOW_NOT_APPLIED",
                "approved_suppression": False,
                "live_gate_applied": False,
            },
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "intent_id": intent_id,
                "approved_suppression": True,
            },
        ],
        ledger={"orders": []},
        source_wallet=WALLET,
        day="2026-07-21",
        generated_at="2026-07-21T10:01:00Z",
    )
    assert report["selected_policy_eligible_unique_intents"] == 1
    assert report["terminal_stage_counts"] == {"profit_latency_suppression": 1}
    assert report["selected_member_attribution"]["status"] == "PASS"


def test_legacy_live_entry_band_block_remains_terminal_and_preserves_parity():
    observed = 1_784_628_000.0
    intents = ["ci_live_band", "ci_shadow_then_latency"]
    report = build_report(
        routing_shadow={
            "fee_gated_measurement_rows": [_routing(intent_id, observed + offset) for offset, intent_id in enumerate(intents)]
        },
        guard_cycles=[_cycle(intent_id) for intent_id in intents],
        execution_events=[
            {
                "event": "wallet_copy_live_entry_price_band_gate_counterfactual",
                "intent_id": "ci_live_band",
                "approved_suppression": True,
                "live_gate_applied": True,
            },
            {
                "event": "wallet_copy_live_entry_price_band_gate_counterfactual",
                "intent_id": "ci_shadow_then_latency",
                "approved_suppression": False,
            },
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "intent_id": "ci_shadow_then_latency",
            },
        ],
        ledger={"orders": []},
        source_wallet=WALLET,
        day="2026-07-21",
        generated_at="2026-07-21T10:01:00Z",
    )
    assert report["policy_eligible_unique_intents"] == 2
    assert report["selected_policy_eligible_unique_intents"] == 2
    assert report["terminal_stage_counts"] == {
        "entry_price_band_gate": 1,
        "profit_latency_suppression": 1,
    }


def test_selected_intent_passing_all_gates_without_submit_is_wiring_defect():
    observed = 1_784_628_000.0
    cycle = _cycle("ci_wiring")
    summary = cycle["live_execution"]["candidate_intent_summary"]
    summary["profit_latency_suppression"] = {"input_intents": 1, "output_intents": 1}
    summary["fresh_candidate_intents_after_window_fill_cap"] = 1
    report = build_report(
        routing_shadow={"fee_gated_measurement_rows": [_routing("ci_wiring", observed)]},
        guard_cycles=[cycle],
        execution_events=[],
        ledger={"orders": []},
        source_wallet=WALLET,
        day="2026-07-21",
        generated_at="2026-07-21T10:01:00Z",
    )
    assert report["selected_member_attribution"]["defect_classification"] == "WIRING_DEFECT"
    assert report["rows"][0]["terminal_stage"] == "eligible_passed_all_gates_no_guard_submit"


def test_selected_intent_without_terminal_detail_is_telemetry_defect():
    observed = 1_784_628_000.0
    cycle = _cycle("ci_telemetry")
    cycle["live_execution"]["candidate_intent_summary"]["profit_latency_suppression"] = {
        "input_intents": 1,
        "output_intents": 0,
    }
    report = build_report(
        routing_shadow={"fee_gated_measurement_rows": [_routing("ci_telemetry", observed)]},
        guard_cycles=[cycle],
        execution_events=[],
        ledger={"orders": []},
        source_wallet=WALLET,
        day="2026-07-21",
        generated_at="2026-07-21T10:01:00Z",
    )
    assert report["selected_member_attribution"]["defect_classification"] == "TELEMETRY_DEFECT"
    assert report["rows"][0]["terminal_stage"] == "selected_terminal_taxonomy_missing"


def test_inventory_blocked_sample_decision_is_exact_terminal_taxonomy():
    observed = 1_784_628_000.0
    cycle = _cycle("ci_book")
    summary = cycle["live_execution"]["candidate_intent_summary"]
    summary["profit_latency_suppression"] = {"input_intents": 1, "output_intents": 1}
    summary["inventory_best_ask_gate"] = {
        "sample_decisions": [
            {
                "intent_id": "ci_book",
                "status": "BLOCKED",
                "reason": "inventory_best_ask_above_vwap_plus_buffer",
            }
        ]
    }
    report = build_report(
        routing_shadow={"fee_gated_measurement_rows": [_routing("ci_book", observed)]},
        guard_cycles=[cycle],
        execution_events=[],
        ledger={"orders": []},
        source_wallet=WALLET,
        day="2026-07-21",
        generated_at="2026-07-21T10:01:00Z",
    )
    assert report["selected_member_attribution"]["defect_classification"] == "NONE"
    assert report["rows"][0]["terminal_stage"] == "inventory_best_ask_gate"
