from scripts.report_e5_paper_live_divergence import build_report


def test_build_report_names_queue_fill_optimism_and_splits_dimensions():
    paper = {
        "contract": {"sizing_policy_id": "fixed_shares_5"},
        "executions": [
            {
                "quote_price": 0.49,
                "quote_ts": 130.0,
                "window_end_s": 300.0,
                "outcome": "Up",
                "execution_status": "FULL_FILL",
                "filled_shares": 5,
                "filled_size_usd": 2.45,
                "post_fee_pnl_usd": 2.40,
                "resolved": True,
                "fill_events": [{"event_ts": 132.0}],
            }
        ],
    }
    ledger = {
        "orders": [
            {
                "maker_cancel": {"execution_lane": "e5_maker_first_btc5m_v1"},
                "lifecycle": [
                    {"status": "LIVE_SUBMITTED", "ts": "1970-01-01T00:02:10+00:00"},
                    {"status": "LIVE_MAKER_FILLED", "ts": "1970-01-01T00:02:15+00:00"},
                ],
                "order_id": "0xabc",
                "submitted_at": "1970-01-01T00:02:10+00:00",
                "market_slug": "btc-updown-5m-0",
                "outcome": "Down",
                "limit_price": 0.49,
                "fill_size_shares": 5,
                "requested_shares": 5,
                "filled_size_usd": 2.45,
            }
        ]
    }
    result = build_report(paper, ledger, [{"market_slug": "btc-updown-5m-0", "direction": "UP"}])
    assert result["paper"]["by_entry_price"]["03_45_50"]["resolved_fills"] == 1
    assert result["live"]["by_side"]["DOWN"]["pnl_usd"] == -2.45
    assert result["optimism_components"]["realized_roi_gap_percentage_points"] > 190
    assert result["optimism_components"]["named_primary_component"] == "POST_QUOTE_TRADE_THROUGH_IS_NOT_QUEUE_FILL"
