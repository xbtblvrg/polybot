from scripts import run_btc5m_native_l2_cross_outcome_parity_stale_ask as lane


def _book(microprice: float, *, ask: float, ask_size: float = 100.0) -> dict:
    return {
        "status": "PASS",
        "best_bid": max(0.01, ask - 0.02),
        "best_bid_size": 100.0,
        "best_ask": ask,
        "best_ask_size": ask_size,
        "microprice": microprice,
    }


def test_generation_is_frozen_paper_only():
    assert lane.CHECKSUM == lane.canonical_checksum(lane.CONFIG)
    assert lane.CONFIG["method"] == "btc5m_native_l2_cross_outcome_parity_stale_ask_v1"
    assert lane.CONFIG["forward_start_s"] == 1_785_009_900
    assert lane.CONFIG["training_cutoff_s"] == 1_785_009_300
    assert lane.CONFIG["features"]["min_net_cross_edge_per_share"] == 0.0
    assert lane.CONFIG["paper_only"] is True


def test_cross_outcome_parity_emits_on_ordinary_dual_books():
    signal, blockers = lane.choose_signal(
        markets={"BTC": {}},
        trades_by_asset={},
        snapshots=[],
        books={
            "BTC": {
                "Up": _book(0.45, ask=0.40),
                "Down": _book(0.35, ask=0.35),
            }
        },
        now=100.0,
        elapsed=60.0,
    )
    assert blockers == []
    assert signal is not None
    assert signal["outcome"] == "Up"
    assert signal["complement"] == "Down"
    assert signal["p_fair"] == 0.65
    assert signal["net_edge_per_share"] > 0


def test_dual_l2_receipt_cycle_is_integrity_complete():
    books = [
        {
            "asset_name": "BTC",
            "outcome": outcome,
            "receipt_sequence": 1,
            "receipt_timestamp_s": 100.0,
            "book_hash": outcome,
        }
        for outcome in ("Up", "Down")
    ]
    result = lane.raw_integrity([], books, 101.0)
    assert result["dual_book_receipt_cycles"] == 1
    assert result["continuity_disagreements"] == 0


def test_two_complete_zero_intent_windows_park():
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
            "blockers": ["btc_post_cost_edge_nonpositive"],
        }
        for offset in (0, 300)
    ]
    reduced = lane.reduce_generation(terminals, [], {})
    assert reduced["status"] == "PARK_ZERO_INTENT_GENERATION"
    assert reduced["stop_writer"] is True


def test_negative_resolved_pnl_parks_immediately():
    intent = {
        "market_slug": f"btc-updown-5m-{lane.FORWARD_START_S}",
        "outcome": "Down",
        "shares": 2.0,
        "limit_price": 0.5,
    }
    terminal = {
        "generation_checksum": lane.CHECKSUM,
        "window_start_s": lane.FORWARD_START_S,
        "raw_clock_complete": True,
        "intent": intent,
        "trade_receipts": [],
        "book_receipts": [],
        "raw_evidence_checksum": lane.canonical_checksum(
            {"trade_receipts": [], "book_receipts": []}
        ),
        "raw_integrity": {},
        "signal": {"executable_ask_depth": 2.0},
        "execution": {"price": 0.5, "shares": 2.0},
    }
    event = {"generation_checksum": lane.CHECKSUM, "intent": intent}
    losing_outcome = "Up" if terminal["intent"]["outcome"] == "Down" else "Down"
    reduced = lane.reduce_generation(
        [terminal],
        [event],
        {terminal["intent"]["market_slug"]: losing_outcome},
    )
    assert reduced["resolved_orders"] == 1
    assert reduced["post_cost_pnl_usd"] < 0
    assert reduced["status"] == "PARK_NEGATIVE_ROLLING_PAPER_PNL"
    assert reduced["stop_writer"] is True
    assert reduced["completed_windows"] == 1


def test_integrity_disagreement_parks_immediately():
    terminal = {
        "generation_checksum": lane.CHECKSUM,
        "window_start_s": lane.FORWARD_START_S,
        "raw_clock_complete": True,
        "intent": None,
        "trade_receipts": [],
        "book_receipts": [],
        "raw_evidence_checksum": lane.canonical_checksum(
            {"trade_receipts": [], "book_receipts": []}
        ),
        "raw_integrity": {"continuity_disagreements": 1},
    }
    reduced = lane.reduce_generation([terminal], [], {})
    assert reduced["status"] == "PARK_IRRECOVERABLE_INTEGRITY_DISAGREEMENT"
    assert reduced["stop_writer"] is True


def test_two_consecutive_later_zero_intent_windows_park_after_signal():
    def terminal(window: int, intent: dict | None) -> dict:
        return {
            "generation_checksum": lane.CHECKSUM,
            "window_start_s": window,
            "raw_clock_complete": True,
            "intent": intent,
            "trade_receipts": [],
            "book_receipts": [],
            "raw_evidence_checksum": lane.canonical_checksum(
                {"trade_receipts": [], "book_receipts": []}
            ),
            "raw_integrity": {},
        }

    signal = {
        "market_slug": "m",
        "outcome": "Up",
        "shares": 2,
        "limit_price": 0.5,
        "mode": "paper",
        "action": "BUY",
        "order_type": "FAK",
        "copy_size_usd": lane.ORDER_USD,
        "live_orders_allowed": False,
        "metadata": {"generation_checksum": lane.CHECKSUM},
    }
    rows = [
        terminal(lane.FORWARD_START_S, signal),
        terminal(lane.FORWARD_START_S + 300, None),
    ]
    rows[0]["execution"] = {"price": 0.5, "shares": 2}
    rows[0]["signal"] = {"executable_ask_depth": 2}
    assert lane.reduce_generation(rows, [], {})["status"] == "PAPER_CELL_ACTIVE"
    rows.append(terminal(lane.FORWARD_START_S + 600, None))
    reduced = lane.reduce_generation(rows, [], {})
    assert reduced["status"] == "PARK_ZERO_INTENT_GENERATION"
    assert reduced["stop_writer"] is True
