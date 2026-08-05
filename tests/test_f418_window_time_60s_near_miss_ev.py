from scripts.report_f418_window_time_60s_near_miss_ev import F418, build_report
def test_report_is_paper_only_and_fee_aware():
    events=[
      {"event":"wallet_copy_live_profit_latency_suppression_reject","reject_reason":"window_time_gte_60s","source_wallet":F418,"market_slug":"btc-updown-5m-100","outcome":"Up","window_time_s":65,"limit_price":.5,"copy_size_usd":1},
      {"event":"wallet_copy_live_order","final_status":"FILLED","source_wallet":F418,"market_slug":"btc-updown-5m-400","outcome":"Down","window_time_s":50,"limit_price":.4,"copy_size_usd":1}]
    resolutions=[{"market_slug":"btc-updown-5m-100","winning_outcome":"Up"},{"market_slug":"btc-updown-5m-400","winning_outcome":"Up"}]
    report=build_report(events,resolutions,generated_at="2026-07-23T23:00:00Z")
    assert report["paper_only"] and not report["live_mutation"] and report["copy_intent_parity"]
    assert report["cohorts"]["45_59"]["resolved_windows"]==1
    assert report["cohorts"]["gte_60_suppressed"]["resolved_windows"]==1
    assert all(row["expected_fee_usd"]>0 for row in report["cells"])
    assert report["policy"]["price_cell_edges"] == [0.25, 0.32, 0.40, 0.50, 0.70]
    assert {row["price_cell"] for row in report["cells"]} == {"040_050", "050_070"}
    assert all(row["move_magnitude_usd"] == 1.0 for row in report["cells"])
    assert all(
        row["move_magnitude_basis"] == "copy_intent_requested_size_usd"
        for row in report["cells"]
    )


def test_report_preserves_wallet_identity_and_publishes_per_wallet_bins():
    other = "0x13e0d447520ebe7f8eeaf7817211201b2c585204"
    events = [
        {
            "event": "wallet_copy_live_profit_latency_suppression_reject",
            "reject_reason": "window_time_gte_60s",
            "source_wallet": wallet,
            "market_slug": "btc-updown-5m-100",
            "outcome": "Up",
            "window_time_s": 65,
            "limit_price": 0.5,
            "copy_size_usd": 1,
        }
        for wallet in (F418, other)
    ]
    report = build_report(
        events,
        [{"market_slug": "btc-updown-5m-100", "winning_outcome": "Up"}],
        generated_at="2026-08-05T00:00:00Z",
        wallets=[F418, other],
    )
    assert report["cohort_wallets"] == sorted([F418, other])
    assert len(report["cells"]) == 2
    assert report["per_wallet"][F418]["cohorts"]["gte_60_suppressed"]["resolved_windows"] == 1
    assert report["per_wallet"][other]["cohorts"]["gte_60_suppressed"]["resolved_windows"] == 1


def test_report_separates_live_price_contract_subbands():
    prices = (0.31, 0.35, 0.45)
    events = [
        {
            "event": "wallet_copy_live_profit_latency_suppression_reject",
            "reject_reason": "window_time_gte_60s",
            "source_wallet": F418,
            "market_slug": f"btc-updown-5m-{100 + index * 300}",
            "outcome": "Up",
            "window_time_s": 65,
            "limit_price": price,
            "copy_size_usd": index + 1,
        }
        for index, price in enumerate(prices)
    ]
    resolutions = [
        {"market_slug": row["market_slug"], "winning_outcome": "Up"}
        for row in events
    ]

    report = build_report(events, resolutions, generated_at="2026-08-05T02:00:00Z")

    assert [row["price_cell"] for row in report["cells"]] == [
        "025_032",
        "032_040",
        "040_050",
    ]
    assert [row["move_magnitude_usd"] for row in report["cells"]] == [1.0, 2.0, 3.0]
