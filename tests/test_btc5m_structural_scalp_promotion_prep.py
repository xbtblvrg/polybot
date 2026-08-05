from scripts.report_btc5m_structural_scalp_promotion_prep import build_packet


def test_promotion_prep_uses_measured_span_and_two_sided_fees() -> None:
    state = {
        "forward_gate": {"forward_floor_window_start_s": 1000},
        "inputs": {"freshness_pass": True, "newest_source_event_age_s": 10.0, "freshness_limit_s": 86400.0},
        "summary": {"study_ev_per_day_usd": 6.0},
        "adapter_contract": {"current_intents_field": "current_intents", "direct_submitter": False},
        "single_guard_contract": {"direct_submitter": False},
    }
    fills = [
        {"window_start_s": 1000 + i * 300, "pnl_usd": 0.1, "cost_usd": 1.0,
         "matched_shares": 2.0, "entry_price": 0.5, "exit_price": 0.55}
        for i in range(30)
    ]

    packet = build_packet(state, fills, generated_at="2026-07-20T00:00:00Z")

    economics = packet["fee_aware_economics"]
    assert economics["expected_entry_and_exit_fee_usd"] == 0.0
    assert economics["post_fee_pnl_usd"] == economics["raw_pnl_usd"]
    assert economics["post_fee_ev_per_measured_day_usd"] != economics["post_fee_pnl_usd"]
    assert packet["proposed_initial_live_sizing"]["order_usd"] == 1.0
    assert packet["copyintent_parity"]["status"] == "PASS"
    assert packet["single_guard_path"]["status"] == "PASS"
    assert packet["forward_window"]["freshness_pass"] is True
    assert economics["fill_pnl_bucket_counts"]["win"] == 30
    assert packet["evidence_gate_pass"] is False
    assert packet["decision"] == "FRESH_FORWARD_EVIDENCE_REQUIRED"


def test_promotion_prep_fails_closed_when_age_missing() -> None:
    state = {
        "forward_gate": {"forward_floor_window_start_s": 1000},
        "inputs": {"freshness_pass": True},
        "summary": {"study_ev_per_day_usd": 1.0},
        "adapter_contract": {"current_intents_field": "current_intents", "direct_submitter": False},
        "single_guard_contract": {"direct_submitter": False},
    }
    fills = [
        {"window_start_s": 1000 + i * 9000, "pnl_usd": 0.1, "cost_usd": 1.0,
         "matched_shares": 2.0, "entry_price": 0.5, "exit_price": 0.55}
        for i in range(30)
    ]

    packet = build_packet(state, fills, generated_at="2026-07-20T00:00:00Z")

    assert packet["forward_window"]["freshness_pass"] is False
    assert packet["evidence_gate_pass"] is False


def test_promotion_packet_reconciles_rolling_snapshot_and_prederives_deadline_branch() -> None:
    state = {
        "seeded_at": "2026-07-20T01:56:47.585110Z",
        "forward_gate": {"forward_floor_window_start_s": 1_784_512_800},
        "inputs": {
            "freshness_pass": True,
            "newest_source_event_age_s": 10.0,
            "freshness_limit_s": 86400.0,
            "gate_window_hours": 24.0,
        },
        "metrics": {"gate_24h": {"fills": 76, "pnl_usd": -1.0676}},
        "summary": {"study_ev_per_day_usd": 1.0},
        "adapter_contract": {"current_intents_field": "current_intents", "direct_submitter": False},
        "single_guard_contract": {"direct_submitter": False},
    }
    reference_floor = 1_784_627_100
    aged_out = [
        {
            "window_start_s": reference_floor + (i % 11) * 300,
            "event_ts": reference_floor + (i % 11) * 300 + 10 + i,
            "pnl_usd": 2.552626 / 47,
            "cost_usd": 1.0,
            "matched_shares": 2.0,
            "entry_price": 0.5,
            "exit_price": 0.55,
        }
        for i in range(47)
    ]
    later = [
        {
            "window_start_s": 1_784_630_400 + (i % 76) * 300,
            "event_ts": 1_784_630_400 + (i % 76) * 300 + 10,
            "pnl_usd": -1.0676 / 76,
            "cost_usd": 1.0,
            "matched_shares": 2.0,
            "entry_price": 0.5,
            "exit_price": 0.55,
        }
        for i in range(76)
    ]
    fills = [*aged_out, *later]

    packet = build_packet(state, fills, generated_at="2026-07-23T02:00:00Z")

    assert packet["basis_reconciliation"]["status"] == "WINDOW_BASIS_DIFFERENCE_NOT_REBUILD_DRIFT"
    assert packet["decision_clock"]["due"] is True
    assert packet["prederived_decision_branches"]["current_branch"] in {
        "PROMOTE_PACKET_READY_FOR_FABLE_LIVE_DECISION",
        "PARK_METHOD_LANE_PAPER_ONLY",
    }
