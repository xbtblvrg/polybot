from scripts import run_btc5m_native_aggressor_sweep_continuation as lane


def book(*, ask=0.40, ask_size=100, micro=0.41):
    return {
        "status": "PASS",
        "best_bid": ask - 0.02,
        "best_ask": ask,
        "best_bid_size": 100,
        "best_ask_size": ask_size,
        "microprice": micro,
        "token_id": "up",
    }


def sweep_rows():
    return [
        {
            "transactionHash": "a",
            "timestamp": 100,
            "asset": "up",
            "side": "BUY",
            "price": 0.38,
            "size": 200,
        },
        {
            "transactionHash": "b",
            "timestamp": 101,
            "asset": "up",
            "side": "BUY",
            "price": 0.40,
            "size": 200,
        },
    ]


def test_trade_identity_dedupe_fails_closed_on_conflict():
    rows = sweep_rows()
    rows.append({**rows[0], "price": 0.39})
    clean, conflicts = lane.dedupe_trades(rows)
    assert len(clean) == 2
    assert conflicts == ["conflicting_trade_identity:a"]


def test_native_sweep_requires_sequence_l2_and_all_frozen_features():
    features, blockers = lane.sweep_features(
        sweep_rows(),
        outcome="Up",
        token_id="up",
        now=102,
        prior_book=book(ask_size=1000, micro=0.38),
        current_book=book(ask_size=500, micro=0.41),
    )
    assert blockers == []
    assert features is not None
    assert features["sweep_levels"] == 2
    assert features["sweep_notional_usd"] == 156.0
    assert features["sweep_depth_ratio"] == 0.4
    missing, reasons = lane.sweep_features(
        sweep_rows(),
        outcome="Up",
        token_id="up",
        now=102,
        prior_book={},
        current_book=book(),
    )
    assert missing is None
    assert "sequence_consistent_l2_missing" in reasons


def test_signal_is_costed_and_executable():
    features, _ = lane.sweep_features(
        sweep_rows(),
        outcome="Up",
        token_id="up",
        now=102,
        prior_book=book(ask_size=1000, micro=0.38),
        current_book=book(ask=0.30, ask_size=500, micro=0.41),
    )
    signal, blockers = lane.choose_signal(
        outcome="Up", features=features, book=book(ask=0.30, ask_size=500, micro=0.41)
    )
    assert blockers == []
    assert signal is not None
    assert signal["net_edge_per_share"] > 0
    refused, reasons = lane.choose_signal(
        outcome="Up", features=features, book=book(ask=0.60, ask_size=500, micro=0.61)
    )
    assert refused is None
    assert "executable_ask_outside_bounds" in reasons


def test_intent_is_exact_one_dollar_taker_paper():
    features, _ = lane.sweep_features(
        sweep_rows(),
        outcome="Up",
        token_id="up",
        now=102,
        prior_book=book(ask_size=1000, micro=0.38),
        current_book=book(ask=0.30, ask_size=500, micro=0.41),
    )
    signal, _ = lane.choose_signal(
        outcome="Up", features=features, book=book(ask=0.30, ask_size=500, micro=0.41)
    )
    intent = lane.build_intent(
        outcome="Up",
        condition_id="c",
        slug="btc-updown-5m-300",
        token_id="up",
        observed_ts=333,
        signal=signal,
    ).asdict()
    assert intent["copy_size_usd"] == 1.0
    assert intent["order_type"] == "FAK"
    assert intent["live_orders_allowed"] is False
    assert intent["metadata"]["model_checksum"] == lane.MODEL_CHECKSUM


def terminal(window, intent=None):
    return {
        "generation_checksum": lane.CHECKSUM,
        "window_start_s": window,
        "raw_clock_complete": True,
        "intent": intent,
        "blockers": ["no_sweep"] if intent is None else [],
    }


def test_two_complete_zero_intent_windows_park():
    state = lane.reduce_generation(
        [
            terminal(lane.FORWARD_START_S),
            terminal(lane.FORWARD_START_S + 300),
        ],
        [],
        {},
    )
    assert state["status"] == "PARK_ZERO_INTENT_GENERATION"
    assert state["gate_checks"]["raw_input_equals_terminal"] is True
    assert state["stop_writer"] is True


def test_six_windows_without_resolved_gate_park():
    intent = {
        "metadata": {
            "parity_disagreement": 0,
            "lookahead_violations": 0,
            "identity_disagreements": 0,
        }
    }
    state = lane.reduce_generation(
        [
            terminal(lane.FORWARD_START_S + 300 * index, intent)
            for index in range(6)
        ],
        [],
        {},
    )
    assert state["status"] == "PARK_FAILED_GATE_BY_SIX_WINDOWS"


def test_selector_is_fail_closed_before_all_gates():
    payload = {
        "generated_at": "2026-07-25T16:45:00Z",
        "frozen_model": {"checksum": lane.MODEL_CHECKSUM},
        "resolved_orders": 0,
        "post_cost_pnl_usd": 0,
        "first_half_post_cost_pnl_usd": 0,
        "second_half_post_cost_pnl_usd": 0,
        "gate_checks": {
            "resolved_gte_10": False,
            "post_cost_positive": False,
            "first_half_positive": False,
            "second_half_positive": False,
            "incremental_vs_no_trade_positive": False,
            "raw_input_equals_terminal": True,
            "exact_copyintent_parity": True,
            "zero_identity_disagreement": True,
        },
    }
    selector = lane._selector(payload, {"checksum": "p"})
    assert selector["status"] == "NO_GATE_COMPLETE_CELL"
    assert selector["selected"] is None
