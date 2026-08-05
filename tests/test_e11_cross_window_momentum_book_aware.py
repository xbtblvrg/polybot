from __future__ import annotations

import scripts.run_e11_cross_window_momentum_book_aware_paper_lane as runner


def _signal() -> dict:
    return {
        "quote_id": "e11q-test",
        "market_slug": "btc-updown-5m-1783413600",
        "condition_id": "cond",
        "outcome": "Up",
        "side": "YES",
        "quote_price": 0.45,
        "quote_ts": 1_783_413_601.25,
        "window_end_s": 1_783_413_900.0,
        "cancel_before_close_s": 30.0,
        "order_usd": 1.0,
        "source_event_ts": 1_783_413_601.0,
        "token_id": "up-token",
        "top_of_book": {
            "status": "OK",
            "book_hash": "book",
            "route_report": {"route_class": "DIRECT_PASS"},
        },
        "enforced_no_fallback_book": True,
        "book_evidence_mode": "enforced_no_fallback",
    }


def test_e11_intent_and_order_use_separate_lane_identity():
    signal = _signal()

    intent = runner.e11_signal_to_intent(signal)
    order = runner._e11_order_from_signal(signal, [], now_ts=1_783_413_602.0)

    assert intent.source_wallet == runner.SOURCE_WALLET
    assert intent.wallet_name == runner.LANE_ID
    assert intent.strategy_family == runner.LANE_ID
    assert intent.live_orders_allowed is False
    assert intent.metadata["copy_model"] == "e11_cross_window_momentum_book_aware"
    assert order["source_wallet"] == runner.SOURCE_WALLET
    assert order["wallet_name"] == runner.LANE_ID
    assert order["fill_model"] == "e11_cross_window_momentum_book_aware_crossing_rtds_v1"


def test_e11_gate_uses_terminal_fill_rate_not_raw_open_quote_rate():
    gate = runner._e11_promotion_gate(
        {
            "prospective_no_fallback_summary": {
                "resolved_paper_fills": 50,
                "resolved_paper_pnl_usd": 1.0,
                "maker_fill_rate_pct": 45.0,
                "terminal_maker_fill_rate_pct": 90.0,
            }
        },
        copyintent_parity_violations=0,
    )

    assert gate["maker_fill_rate_denominator"] == "terminal_quotes_filled_plus_cancelled"
    assert gate["promotion_50_terminal_no_fallback_positive"] == "PASS"


def test_e11_dedupes_market_outcome_across_launchd_runs():
    signals, skipped = runner._dedupe_window_signals(
        [
            {"market_slug": "btc-updown-5m-1783414200", "outcome": "Down"},
            {"market_slug": "btc-updown-5m-1783414500", "outcome": "Up"},
        ],
        [
            {
                "market_slug": "btc-updown-5m-1783414200",
                "outcome": "Down",
                "maker_quote": {"market_slug": "btc-updown-5m-1783414200", "outcome": "Down"},
            }
        ],
    )

    assert skipped == 1
    assert signals == [{"market_slug": "btc-updown-5m-1783414500", "outcome": "Up"}]


def test_e11_dedupes_prior_orders_before_gate_scoring():
    orders, skipped = runner._dedupe_prior_orders(
        [
            {"order_id": "first", "market_slug": "btc-updown-5m-1783414200", "outcome": "Down"},
            {"order_id": "duplicate", "market_slug": "btc-updown-5m-1783414200", "outcome": "Down"},
            {"order_id": "next", "maker_quote": {"market_slug": "btc-updown-5m-1783414500", "outcome": "Up"}},
        ]
    )

    assert skipped == 1
    assert [order["order_id"] for order in orders] == ["first", "next"]


def test_e11_cancel_reason_counts_separate_book_move_from_window_expiry():
    window_expiry = runner._e11_order_from_signal(_signal(), [], now_ts=1_783_413_871.0)
    book_moved_signal = _signal()
    book_moved_signal["quote_id"] = "e11q-book-moved"
    book_moved_signal["top_of_book"]["blocking_reason"] = "price_above_slippage_cap"
    book_moved = runner._e11_order_from_signal(book_moved_signal, [], now_ts=1_783_413_871.0)
    re_quote = {
        "final_status": "CANCELLED",
        "maker_quote": _signal(),
        "lifecycle": [{"message": "paper E11 maker quote cancelled for re-quote"}],
    }

    assert window_expiry["final_status"] == "CANCELLED"
    assert window_expiry["cancel_reason"] == "window_expiry"
    assert book_moved["cancel_reason"] == "book_moved"
    assert runner._e11_order_cancel_reason(re_quote) == "re_quote"
    assert runner._e11_cancel_reason_counts([window_expiry, book_moved, re_quote]) == {
        "book_moved": 1,
        "re_quote": 1,
        "window_expiry": 1,
    }
