from scripts import run_btc5m_cross_exchange_passive_conversion_shadow as lane


def test_quote_decision_is_strict_post_only_and_positive_edge():
    decision = lane._quote_decision(
        {
            "window_start_s": 1784953800,
            "market_slug": "btc-updown-5m-1784953800",
            "condition_id": "condition",
            "outcome": "Up",
            "token_id": "token",
            "calibrated_probability": 0.675,
            "book": {"best_bid": 0.44, "best_ask": 0.45},
        },
        now_ts=1784953835,
    )
    assert decision["eligible"] is True
    assert decision["quote_price"] == 0.44
    assert decision["strict_post_only"] is True
    assert decision["net_edge_per_share"] > 0


def test_quote_decision_refuses_crossing_or_nonpositive_edge():
    decision = lane._quote_decision(
        {
            "window_start_s": 1784953800,
            "market_slug": "btc-updown-5m-1784953800",
            "condition_id": "condition",
            "outcome": "Down",
            "token_id": "token",
            "calibrated_probability": 0.40,
            "book": {"best_bid": 0.50, "best_ask": 0.50},
        },
        now_ts=1784953835,
    )
    assert decision["eligible"] is False
    assert "not_strict_post_only" in decision["reasons"]
    assert "net_edge_nonpositive" in decision["reasons"]


def test_queue_ahead_counts_only_same_price_bids():
    assert lane._queue_ahead_at_price(
        {
            "bids": [
                {"price": "0.44", "size": "3"},
                {"price": "0.44", "size": "2"},
                {"price": "0.43", "size": "99"},
            ]
        },
        0.44,
    ) == 5.0
