from scripts import run_btc5m_polymarket_first_leader_cross_asset_lag as lane


def _book(microprice: float, *, ask: float = 0.40, depth: float = 100.0) -> dict:
    return {
        "status": "PASS",
        "best_bid": max(0.01, ask - 0.02),
        "best_bid_size": depth,
        "best_ask": ask,
        "best_ask_size": depth,
        "microprice": microprice,
    }


def _trade(identity: str, token: str, price: float, size: float, ts: float) -> dict:
    return {
        "id": identity,
        "asset": token,
        "side": "BUY",
        "price": price,
        "size": size,
        "timestamp": ts,
    }


def test_generation_checksum_is_frozen_and_distinct():
    assert lane.CHECKSUM == "1241ecaa95d9975c6a7ee148a8f6fff265a2025d807f7833aa646556f56b8a0a"
    assert lane.CONFIG["paper_only"] is True
    assert lane.CONFIG["live_orders_allowed"] is False
    assert lane.CONFIG["entry_bounds"] == [0.25, 0.50]


def test_leader_sweep_requires_measured_notional_levels_and_book_move():
    now = 100.0
    snapshots = [
        {
            "asset_name": "ETH",
            "outcome": "Up",
            "receipt_timestamp_s": 98.0,
            "book": _book(0.50),
        }
    ]
    rows = [
        _trade("a", "eth-up", 0.51, 30, 99.0),
        _trade("b", "eth-up", 0.52, 30, 99.5),
    ]
    feature, blockers = lane.leader_sweep(
        asset_name="ETH",
        outcome="Up",
        token_id="eth-up",
        rows=rows,
        snapshots=snapshots,
        current_book=_book(0.53),
        now=now,
    )
    assert blockers == []
    assert feature is not None
    assert feature["price_levels"] == 2
    assert feature["buy_notional_usd"] > lane.MIN_SWEEP_NOTIONAL_USD
    assert feature["microprice_displacement"] == 0.03


def test_first_valid_leader_is_sufficient_without_joint_confirmation():
    now = 100.0
    markets = {
        "ETH": {"outcomes": '["Up","Down"]', "clobTokenIds": '["eth-up","eth-down"]'},
        "SOL": {"outcomes": '["Up","Down"]', "clobTokenIds": '["sol-up","sol-down"]'},
    }
    snapshots = [
        {"asset_name": "ETH", "outcome": "Up", "receipt_timestamp_s": 98.0, "book": _book(0.50)},
        {"asset_name": "BTC", "outcome": "Up", "receipt_timestamp_s": 98.0, "book": _book(0.52)},
    ]
    signal, blockers = lane.choose_signal(
        markets=markets,
        trades_by_asset={
            "ETH": [
                _trade("a", "eth-up", 0.51, 30, 99.0),
                _trade("b", "eth-up", 0.52, 30, 99.5),
            ],
            "SOL": [],
        },
        snapshots=snapshots,
        books={"ETH": {"Up": _book(0.53)}, "SOL": {}, "BTC": {"Up": _book(0.525)}},
        now=now,
        elapsed=60,
    )
    assert blockers == []
    assert signal is not None
    assert signal["outcome"] == "Up"
    assert [row["asset_name"] for row in signal["leaders"]] == ["ETH"]


def test_opposite_first_leaders_within_lag_window_abstain():
    now = 100.0
    markets = {
        "ETH": {"outcomes": '["Up","Down"]', "clobTokenIds": '["eth-up","eth-down"]'},
        "SOL": {"outcomes": '["Up","Down"]', "clobTokenIds": '["sol-up","sol-down"]'},
    }
    snapshots = [
        {"asset_name": "ETH", "outcome": "Up", "receipt_timestamp_s": 98.0, "book": _book(0.50)},
        {"asset_name": "SOL", "outcome": "Down", "receipt_timestamp_s": 98.0, "book": _book(0.50)},
        {"asset_name": "BTC", "outcome": "Up", "receipt_timestamp_s": 98.0, "book": _book(0.52)},
        {"asset_name": "BTC", "outcome": "Down", "receipt_timestamp_s": 98.0, "book": _book(0.52)},
    ]
    signal, blockers = lane.choose_signal(
        markets=markets,
        trades_by_asset={
            "ETH": [
                _trade("eu1", "eth-up", 0.51, 30, 99.0),
                _trade("eu2", "eth-up", 0.52, 30, 99.5),
            ],
            "SOL": [
                _trade("sd1", "sol-down", 0.51, 30, 99.2),
                _trade("sd2", "sol-down", 0.52, 30, 99.7),
            ],
        },
        snapshots=snapshots,
        books={
            "ETH": {"Up": _book(0.53)},
            "SOL": {"Down": _book(0.53)},
            "BTC": {"Up": _book(0.525), "Down": _book(0.525)},
        },
        now=now,
        elapsed=60,
    )
    assert signal is None
    assert blockers == ["opposite_leader_direction_conflict_within_2s"]


def test_build_intent_preserves_paper_only_copyintent():
    signal = {
        "outcome": "Up",
        "leaders": [
            {"asset_name": "ETH", "event_ts": 99.0},
            {"asset_name": "SOL", "event_ts": 99.5},
        ],
        "executable_ask": 0.40,
        "executable_ask_depth": 100.0,
        "shares": 2.5,
    }
    market = {
        "slug": "btc-updown-5m-0",
        "conditionId": "condition",
        "outcomes": '["Up","Down"]',
        "clobTokenIds": '["up-token","down-token"]',
    }
    intent = lane.build_intent(signal, market, 100.0)
    assert intent["mode"] == "paper"
    assert intent["live_orders_allowed"] is False
    assert intent["copy_size_usd"] == 1.0
    assert intent["limit_price"] == 0.40
    assert intent["token_id"] == "up-token"
    assert intent["metadata"]["generation_checksum"] == lane.CHECKSUM


def test_two_complete_zero_intent_windows_park_monotonically():
    terminals = [
        {
            "generation_checksum": lane.CHECKSUM,
            "window_start_s": lane.FORWARD_START_S + offset,
            "raw_clock_complete": True,
            "intent": None,
            "trade_receipts": [],
            "book_receipts": [],
            "raw_evidence_checksum": lane.canonical_checksum(
                {"trade_receipts": [], "book_receipts": []}
            ),
            "raw_integrity": {},
            "blockers": ["first_leader_missing"],
        }
        for offset in (0, 300)
    ]
    reduced = lane.reduce_generation(terminals, [], {})
    assert reduced["status"] == "PARK_ZERO_INTENT_GENERATION"
    assert reduced["stop_writer"] is True
    assert reduced["completed_windows"] == 2
    assert reduced["positive_edge_intents"] == 0


def test_terminal_selector_is_not_actionable():
    payload = {
        "status": "PARK_ZERO_INTENT_GENERATION",
        "generated_at": "2026-07-25T18:29:30Z",
    }
    result = lane.selector(payload, {"checksum": "p"})
    assert result["selected"] is None
    assert result["cells"] == []
    assert result["active_capacity"] is False
    assert result["due"] is False
    assert result["historical_arbiter_only"] is True
