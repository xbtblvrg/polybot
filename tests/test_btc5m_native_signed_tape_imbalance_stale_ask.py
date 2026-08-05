from scripts import run_btc5m_native_signed_tape_imbalance_stale_ask as lane


def _book(microprice: float, *, ask: float = 0.405, depth: float = 100.0) -> dict:
    return {
        "status": "PASS",
        "best_bid": ask - 0.02,
        "best_bid_size": depth,
        "best_ask": ask,
        "best_ask_size": depth,
        "microprice": microprice,
    }


def _trade(identity: str, token: str, side: str, price: float, size: float, ts: float) -> dict:
    return {
        "id": identity,
        "asset": token,
        "side": side,
        "price": price,
        "size": size,
        "timestamp": ts,
    }


def test_generation_is_frozen_paper_only():
    assert lane.CHECKSUM == lane.canonical_checksum(lane.CONFIG)
    assert lane.CONFIG["method"] == "btc5m_native_signed_tape_imbalance_stale_ask_v1"
    assert lane.CONFIG["forward_start_s"] == 1_785_006_600
    assert lane.CONFIG["paper_only"] is True
    assert lane.CONFIG["live_orders_allowed"] is False


def test_signed_tape_event_needs_no_multilevel_sweep():
    now = 100.0
    markets = {
        "BTC": {
            "outcomes": '["Up","Down"]',
            "clobTokenIds": '["btc-up","btc-down"]',
        }
    }
    snapshots = [
        {
            "asset_name": "BTC",
            "outcome": "Up",
            "receipt_timestamp_s": 95.0,
            "book": _book(0.45, ask=0.40),
        }
    ]
    signal, blockers = lane.choose_signal(
        markets=markets,
        trades_by_asset={
            "BTC": [_trade("one-print", "btc-up", "BUY", 0.46, 50.0, 99.0)]
        },
        snapshots=snapshots,
        books={"BTC": {"Up": _book(0.47)}},
        now=now,
        elapsed=60.0,
    )
    assert blockers == []
    assert signal is not None
    assert signal["outcome"] == "Up"
    assert signal["signed_notional_usd"] == 23.0
    assert signal["trade_identities"] == ["one-print"]


def test_raw_integrity_requires_only_dual_btc_books():
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
            "blockers": ["signed_tape_missing"],
        }
        for offset in (0, 300)
    ]
    reduced = lane.reduce_generation(terminals, [], {})
    assert reduced["status"] == "PARK_ZERO_INTENT_GENERATION"
    assert reduced["stop_writer"] is True
    assert reduced["completed_windows"] == 2
