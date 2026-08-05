from scripts.report_order148_seated_fill_dispositions import build_report


def test_order148_recovers_price_and_floor_reasons_and_goal_bound() -> None:
    ids = [
        {"transaction_hash": "0xhigh1", "event_ts": 100.0, "price": 0.75, "size": 13.62},
        {"transaction_hash": "0xhigh2", "event_ts": 100.0, "price": 0.75, "size": 36.378},
        {"transaction_hash": "0xlow", "event_ts": 200.0, "price": 0.086914, "size": 50.0},
    ]
    history = {"events": [
        {"transaction_hash": row["transaction_hash"], "event_id": f"event-{index}"}
        for index, row in enumerate(ids)
    ]}
    price_cycle = {
        "cycle": 1,
        "generated_at": "2026-08-02T02:54:00Z",
        # Production persists only the latest event id plus an aggregate reject count.
        "event_id": "event-0",
        "live_execution": {"candidate_intent_summary": {
            "fresh_candidate_intents": 0,
            "live_event_prefilter": {"policy_reject_counts": {"price_outside_policy": 2}},
            "candidate_build_events_filtered_reasons": {"policy_price_outside_policy": 2},
        }},
    }
    floor_cycle = {
        "cycle": 2,
        "generated_at": "2026-08-02T03:25:00Z",
        "event_id": "event-2",
        "live_execution": {"candidate_intent_summary": {
            "fresh_candidate_intents": 1,
            "fresh_candidate_intents_after_hard_entry_floor": 0,
            "fresh_candidate_intents_after_hard_entry_cap": 1,
        }},
    }
    report = build_report(
        order147={"paper_accumulator": {"identities": ids}},
        history=history,
        cycles=[price_cycle, floor_cycle],
    )
    assert report["status"] == "E1_ALL_REASONS_PERSISTED"
    assert report["reason_counts"] == {
        "hard_entry_floor_filtered_candidate_copy_intents": 1,
        "policy_price_outside_policy": 2,
    }
    assert report["price_band_hypothesis"] == {
        "status": "CONFIRMED",
        "price_policy_rejections": 3,
        "non_price_policy_rejections": 0,
        "effective_min_buy_price": 0.25,
        "effective_max_buy_price": 0.5,
        "taxonomy_resolution": {
            "policy_price_outside_policy": "price_band_upper_rail",
            "hard_entry_floor_filtered_candidate_copy_intents": "price_band_lower_rail",
        },
    }
    assert report["rows"][1]["reason_evidence"]["association"] == (
        "same_source_event_timestamp_and_aggregate_reject_count"
    )
    assert report["rows"][2]["reason_evidence"]["resolved_taxonomy"] == "price_band_lower_rail"
    assert report["supply_goal_arithmetic"]["in_band_fills"] == 0
    assert report["supply_goal_arithmetic"]["perfect_all_wins_daily_profit_upper_bound_usd"] == 0
    assert report["supply_goal_arithmetic"]["goal_status"] == "ROTATION_NECESSARY_NOT_SUFFICIENT"
