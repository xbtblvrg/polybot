from scripts.report_probe_fill_quality_shadows import A689, build_packet


def test_build_packet_is_paper_only_and_uses_fixed_cells() -> None:
    ledger = {
        "orders": [{
            "source_wallet": A689,
            "final_status": "FILLED",
            "market_slug": "btc-updown-5m-1784592000",
            "submitted_at": "2026-07-21T00:10:00Z",
            "limit_price": 0.4,
            "side": "UP",
            "trade_result": {"response_filled_size_usd": 1.0, "response_fill_size_shares": 2.5},
            "source_intent": {"metadata": {"wallet_copy_policy": {"effective_live_cap_usd": 1.0}, "inventory_v2": {"observed_slug_epoch_delta_s": 130}}},
        }],
    }
    guard = {"window_participation": {"rows": [{
        "source_wallet": A689,
        "dominant_skip_reason": "window_time_gte_180s",
        "observed_slug_epoch_delta_s": 190,
        "market_slug": "btc-updown-5m-1784592300",
        "outcome": "UP",
        "source_inventory_vwap": 0.4,
        "would_floor_min_order_usd": 1.0,
    }]}}
    resolutions = {
        "slug_start:1784592000": {"market_slug": "btc-updown-5m-1784592000", "direction": "DOWN"},
        "slug_start:1784592300": {"market_slug": "btc-updown-5m-1784592300", "direction": "UP"},
    }
    packet = build_packet(ledger=ledger, guard=guard, resolutions=resolutions, generated_at="2026-07-21T02:30:00Z")
    assert packet["paper_only"] is True
    assert packet["live_orders_allowed"] is False
    assert packet["live_mutation"] is False
    assert packet["weekday_micro_loss"]["n"] == 1
    assert packet["weekday_micro_loss"]["cells"]["01_25_50|120_179"]["post_fee_pnl_usd"] < 0
    assert packet["early_utc_probe_fill_quality"]["early_utc"]["n"] == 1
    assert packet["window_time_near_miss"]["n"] == 1
    assert packet["decision"] == "REPORT_ONLY_NO_LIVE_DENY_OR_BAR_CHANGE"
