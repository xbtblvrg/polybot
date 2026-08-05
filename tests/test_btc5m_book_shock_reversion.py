from scripts import run_btc5m_book_shock_reversion as lane


def test_summarize_l2_accepts_live_client_dict_shape():
    summary = lane.summarize_l2(
        {
            "bids": [{"price": "0.44", "size": "12"}],
            "asks": [{"price": "0.45", "size": "8"}],
            "timestamp": "123",
        },
        token_id="t",
        observed_at_s=1.0,
    )
    assert summary["status"] == "PASS"
    assert summary["best_bid"] == 0.44
    assert summary["best_ask"] == 0.45
    assert summary["book_timestamp"] == "123"


def _book(*, bid=0.40, bid_size=100.0, ask=0.42, ask_size=100.0):
    return {
        "status": "PASS",
        "best_bid": bid,
        "best_bid_size": bid_size,
        "best_ask": ask,
        "best_ask_size": ask_size,
        "microprice": (ask * bid_size + bid * ask_size) / (bid_size + ask_size),
        "executable_depth_usd": ask * ask_size,
    }


def test_detect_shock_requires_price_depth_and_post_cost_edge():
    signal, blockers = lane.detect_shock(
        anchor=_book(bid=0.50, ask=0.52, ask_size=100),
        current=_book(bid=0.44, ask=0.45, ask_size=10),
    )
    assert blockers == []
    assert signal is not None
    assert signal["actual_depth_verified"] is True
    refused, reasons = lane.detect_shock(
        anchor=_book(bid=0.50, ask=0.52, ask_size=100),
        current=_book(bid=0.47, ask=0.48, ask_size=100),
    )
    assert refused is None
    assert "current_depth_not_vacuum" in reasons


def test_copyintent_is_exact_one_dollar_paper_only():
    intent = lane.build_intent(
        outcome="Up",
        condition_id="c",
        market_slug="btc-updown-5m-300",
        token_id="t",
        observed_at_s=333,
        signal={
            "entry_price": 0.40,
            "shares": 2.5,
            "actual_depth_verified": True,
        },
    ).asdict()
    assert intent["copy_size_usd"] == 1.0
    assert intent["mode"] == "paper"
    assert intent["live_orders_allowed"] is False
    assert intent["metadata"]["parity_disagreement"] == 0


def _terminal(window, *, intent=None, blockers=None):
    return {
        "window_start_s": window,
        "market_slug": f"btc-updown-5m-{window}",
        "raw_clock_complete": True,
        "intent": intent,
        "signal": {"expected_fee_usd": 0.01, "actual_depth_verified": True} if intent else None,
        "blockers": blockers or [],
    }


def test_two_complete_zero_intent_windows_park_monotonically():
    state = lane.reduce_state(
        [_terminal(300, blockers=["no_shock"]), _terminal(600, blockers=["no_shock"])],
        {},
    )
    assert state["status"] == "PARK_ZERO_INTENT_GENERATION"
    assert state["stop_writer"] is True
    assert state["blocker_taxonomy"] == {"no_shock": 2}


def test_all_pass_requires_resolved_positive_both_halves_and_parity():
    terminals = []
    resolutions = {}
    for index in range(10):
        window = 300 * (index + 1)
        outcome = "Up"
        intent = {
            "outcome": outcome,
            "limit_price": 0.40,
            "shares": 2.5,
            "metadata": {"parity_disagreement": 0},
        }
        terminals.append(_terminal(window, intent=intent))
        resolutions[f"btc-updown-5m-{window}"] = outcome
    state = lane.reduce_state(terminals, resolutions)
    assert state["status"] == "PROMOTION_HANDOFF_READY"
    assert all(state["gate_checks"].values())


def test_six_windows_with_insufficient_resolutions_park():
    intent = {
        "outcome": "Up",
        "limit_price": 0.40,
        "shares": 2.5,
        "metadata": {"parity_disagreement": 0},
    }
    state = lane.reduce_state([_terminal(300 * (i + 1), intent=intent) for i in range(6)], {})
    assert state["status"] == "PARK_INSUFFICIENT_EXECUTABLE_FILL_RATE"
    assert state["stop_writer"] is True
