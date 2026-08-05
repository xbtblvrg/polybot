from scripts import run_btc5m_native_l2_tob_pressure_imbalance as lane


def _book(*, bid_size: float, ask_size: float, microprice: float = 0.49, ask: float = 0.40) -> dict:
    return {
        "status": "PASS",
        "best_bid": ask - 0.02,
        "best_bid_size": bid_size,
        "best_ask": ask,
        "best_ask_size": ask_size,
        "microprice": microprice,
    }


def test_generation_is_frozen_paper_only():
    assert lane.CHECKSUM == lane.canonical_checksum(lane.CONFIG)
    assert lane.CONFIG["method"] == "btc5m_native_l2_tob_pressure_imbalance_v1"
    assert lane.CONFIG["forward_start_s"] == 1_785_008_700
    assert lane.CONFIG["features"]["min_abs_pressure"] == 0.45
    assert lane.CONFIG["paper_only"] is True
    assert lane.CONFIG["live_orders_allowed"] is False


def test_pressure_acceleration_emits_without_trade_tape():
    now = 100.0
    snapshots = [
        {
            "asset_name": "BTC",
            "outcome": "Up",
            "receipt_timestamp_s": 97.0,
            "book": _book(bid_size=60.0, ask_size=40.0),
        }
    ]
    signal, blockers = lane.choose_signal(
        markets={"BTC": {}},
        trades_by_asset={},
        snapshots=snapshots,
        books={"BTC": {"Up": _book(bid_size=80.0, ask_size=20.0)}},
        now=now,
        elapsed=60.0,
    )
    assert blockers == []
    assert signal is not None
    assert signal["outcome"] == "Up"
    assert signal["pressure"] == 0.6
    assert signal["pressure_delta"] == 0.4


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
            "blockers": ["tob_pressure_below_0p45"],
        }
        for offset in (0, 300)
    ]
    reduced = lane.reduce_generation(terminals, [], {})
    assert reduced["status"] == "PARK_ZERO_INTENT_GENERATION"
    assert reduced["stop_writer"] is True
    assert reduced["completed_windows"] == 2
