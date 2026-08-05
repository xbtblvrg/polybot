from scripts.report_f418_window_time_book_crossed import build_report
from scripts.report_f418_window_time_60s_near_miss_ev import F418


def test_book_crossed_matrix_uses_ask_and_day_bounded_gate() -> None:
    events = []
    books = []
    resolutions = []
    for day in range(1, 7):
        start = 1782864000 + day * 86400
        for index in range(20):
            slug = f"btc-updown-5m-{start + index * 300}"
            asset = f"asset-{day}-{index}"
            ts = start + index * 300 + 65
            events.extend(
                [
                    {
                        "event": "wallet_copy_live_profit_latency_suppression_reject",
                        "reject_reason": "window_time_gte_60s",
                        "source_wallet": F418,
                        "market_slug": slug,
                        "condition_id": slug,
                        "outcome": "up",
                        "window_time_s": 65,
                        "limit_price": 0.45,
                        "copy_size_usd": 1.0,
                        "ts": ts,
                    },
                    {
                        "event": "wallet_copy_live_order",
                        "market_slug": slug,
                        "outcome": "up",
                        "trade_result": {"market_id": asset},
                    },
                ]
            )
            books.append({"event_type": "best_bid_ask", "asset_id": asset, "captured_at_s": ts, "best_bid": 0.39, "best_ask": 0.40})
            resolutions.append({"market_slug": slug, "winning_outcome": "up"})

    report = build_report(events, books, resolutions, generated_at="2026-08-02T00:00:00Z")

    cell = report["matrix"]["040_050"]["gte_60_suppressed"]
    assert report["paper_only"] is True
    assert report["live_mutation"] is False
    assert report["sample_ready_cells"] == ["040_050/gte_60_suppressed"]
    assert report["verdict"] == "BOOK_CROSSED_POSITIVE_CELL_SURVIVES"
    assert report["coverage"]["book_crossed_rows"] == 120
    assert cell["sample_gate"]["status"] == "PASS"
    assert cell["sample_gate"]["split_integrity"] == "DAY_BOUNDED"
    assert cell["aggregate"]["post_fee_pnl_usd"] > 0
    assert cell["survives_book_crossing"] is True


def test_cohort_before_book_capture_is_terminal_unpriceable() -> None:
    slug = "btc-updown-5m-1782864000"
    events = [
        {
            "event": "wallet_copy_live_profit_latency_suppression_reject",
            "reject_reason": "window_time_gte_60s",
            "source_wallet": F418,
            "market_slug": slug,
            "condition_id": slug,
            "outcome": "up",
            "window_time_s": 65,
            "limit_price": 0.45,
            "ts": 1782864065,
        },
        {
            "event": "wallet_copy_live_order",
            "market_slug": slug,
            "outcome": "up",
            "trade_result": {"market_id": "asset"},
        },
    ]
    books = [{
        "event_type": "best_bid_ask",
        "asset_id": "asset",
        "captured_at_s": 1782950465,
        "best_bid": 0.39,
        "best_ask": 0.40,
    }]
    report = build_report(
        events,
        books,
        [{"market_slug": slug, "winning_outcome": "up"}],
        generated_at="2026-08-02T00:00:00Z",
    )
    assert report["verdict"] == "TERMINAL_UNPRICEABLE_COHORT_NO_CONTEMPORANEOUS_BOOK"
    assert report["temporal_coverage_proof"]["candidate_rows_before_book_log"] == 1
    assert report["temporal_coverage_proof"]["candidate_distinct_days_in_book_window"] == 0
