from scripts.report_market_buy_precision_counterfactual import build_report


def test_precision_cells_require_positive_development_and_holdout():
    events = []
    resolutions = []
    for index in range(70):
        slug = f"btc-updown-5m-{index * 300}"
        events.append(
            {
                "event": "wallet_copy_live_market_buy_precision_infeasible_reject",
                "intent_id": f"i{index}",
                "market_slug": slug,
                "condition_id": f"c{index}",
                "outcome": "Up",
                "nearest_executable_tick": 0.5,
                "nearest_executable_amount_usd": 1.0,
                "nearest_tick_price_delta": 0.005,
                "policy_cap_usd": 1.0,
                "event_age_s": 5.0,
                "ts": f"1970-01-01T{index // 12:02d}:{(index % 12) * 5:02d}:10+00:00",
            }
        )
        resolutions.append({"market_slug": slug, "direction": "UP"})
    report = build_report(event_rows=events, resolution_rows=resolutions, generated_at="now")
    assert report["status"] == "REQUEST_CELL_RULING"
    cell = next(iter(report["cell_summaries"].values()))
    assert cell["development"]["resolved"] == 50
    assert cell["chronological_holdout"]["resolved"] == 20
    assert cell["gate"] == "REQUEST_CELL_RULING"


def test_precision_decomposition_names_cap_manufactured_constraint() -> None:
    event = {
        "event": "wallet_copy_live_market_buy_precision_infeasible_reject",
        "intent_id": "ci_cap",
        "market_slug": "btc-updown-5m-1",
        "outcome": "Up",
        "copy_size_usd": 1.0,
        "effective_chase_price": 0.49,
        "precision_safe_amount_usd": 1.47,
        "min_order_usd": 1.0,
        "policy_cap_usd": 1.0,
        "precision_cap_limit_usd": 1.1,
        "nearest_executable_tick": 0.48,
        "nearest_executable_amount_usd": 1.02,
    }
    report = build_report(event_rows=[event], resolution_rows=[], generated_at="now")
    row = report["precision_decomposition"]["rows"][0]
    assert row["exact_constraint_violated"] == "valid_precision_amount_exceeds_policy_cap_plus_allowance"
    assert row["one_dollar_cap_manufactures_infeasibility"] is True
    assert row["cap_shortfall_usd"] == 0.37
    assert report["precision_decomposition"]["verdict"] == "ONE_DOLLAR_CAP_DELETES_ALL_OBSERVED_PRECISION_SUPPLY"
    assert report["precision_decomposition"]["historical_unique_suppression_intents"] == 1
    assert report["precision_decomposition"]["focus_recent_13"]["one_dollar_cap_manufactured_count"] == 1
