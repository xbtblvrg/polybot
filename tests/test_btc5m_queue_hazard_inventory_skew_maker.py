from scripts import run_btc5m_queue_hazard_inventory_skew_maker as lane


def book(bid=0.40, ask=0.45, bid_n=100, ask_n=100, micro=0.425):
    return {
        "status": "PASS", "best_bid": bid, "best_ask": ask,
        "best_bid_size": bid_n, "best_ask_size": ask_n, "microprice": micro,
    }


def test_quote_requires_spread_net_of_fee_and_adverse_reserve():
    quote, blockers = lane.choose_quote(book())
    assert blockers == []
    assert quote["net_edge_per_share"] > 0
    refused, reasons = lane.choose_quote(book(ask=0.41, micro=0.405))
    assert refused is None
    assert "spread_below_fee_adverse_reserve" in reasons


def test_contra_volume_is_trade_identity_and_price_bounded():
    rows = [
        {"transactionHash": "a", "timestamp": 11, "asset": "t", "side": "SELL", "price": 0.40, "size": 3},
        {"transactionHash": "a", "timestamp": 11, "asset": "t", "side": "SELL", "price": 0.40, "size": 3},
        {"transactionHash": "b", "timestamp": 12, "asset": "t", "side": "SELL", "price": 0.41, "size": 9},
    ]
    assert lane.contra_volume(rows, token_id="t", price=0.40, after_ts=10) == (3.0, ["a"])


def test_intent_is_exact_one_dollar_post_only_paper():
    quote, _ = lane.choose_quote(book())
    intent = lane.build_intent(
        outcome="Up", condition_id="c", slug="btc-updown-5m-300",
        token_id="t", observed_ts=333, quote=quote,
    ).asdict()
    assert intent["copy_size_usd"] == 1.0
    assert intent["order_type"] == "GTC_POST_ONLY_STRICT"
    assert intent["live_orders_allowed"] is False
    assert intent["metadata"]["parity_disagreement"] == 0


def terminal(window, intent=None):
    return {
        "window_start_s": window, "market_slug": f"btc-updown-5m-{window}",
        "raw_clock_complete": True, "intent": intent, "blockers": ["no_edge"] if intent is None else [],
    }


def test_two_zero_intent_windows_park_with_exact_reconciliation():
    state = lane.reduce_generation([terminal(300), terminal(600)], [], {})
    assert state["status"] == "PARK_ZERO_INTENT_GENERATION"
    assert state["gate_checks"]["raw_input_equals_terminal"] is True
    assert state["stop_writer"] is True


def test_six_windows_without_genuine_fills_park():
    intent = {"metadata": {"parity_disagreement": 0}}
    state = lane.reduce_generation([terminal(300 * (i + 1), intent) for i in range(6)], [], {})
    assert state["status"] == "PARK_FAILED_GATE_BY_SIX_WINDOWS"


def test_selector_is_fail_closed_before_all_gates():
    payload = {
        "generated_at": "2026-07-25T16:00:00Z", "resolved_orders": 0,
        "genuine_queue_fills": 0, "post_cost_pnl_usd": 0,
        "first_half_post_cost_pnl_usd": 0, "second_half_post_cost_pnl_usd": 0,
        "gate_checks": {"raw_input_equals_terminal": True, "no_synthetic_touch_fills": True, "exact_parity": True},
    }
    selector = lane._selector(payload, {"checksum": "p"})
    assert selector["status"] == "NO_GATE_COMPLETE_CELL"
    assert selector["selected"] is None
