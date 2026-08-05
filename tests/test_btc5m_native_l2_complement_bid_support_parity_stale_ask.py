from scripts import run_btc5m_native_l2_complement_bid_support_parity_stale_ask as lane


def _book(*, bid: float, ask: float, microprice: float = 0.5) -> dict:
    return {
        "status": "PASS",
        "best_bid": bid,
        "best_bid_size": 100.0,
        "best_ask": ask,
        "best_ask_size": 100.0,
        "microprice": microprice,
    }


def test_generation_is_unique_frozen_and_paper_only():
    assert lane.CHECKSUM == lane.base.canonical_checksum(lane.CONFIG)
    assert lane.CONFIG["forward_start_s"] == 1_785_012_600
    assert lane.CONFIG["training_cutoff_s"] == 1_785_012_000
    assert lane.CONFIG["features"]["max_receipt_gap_s"] == 5.0
    assert lane.CONFIG["paper_only"] is True
    assert lane.CONFIG["live_orders_allowed"] is False


def test_signal_uses_complement_best_bid_not_microprice():
    signal, blockers = lane.choose_signal(
        markets={"BTC": {}},
        trades_by_asset={},
        snapshots=[],
        books={
            "BTC": {
                "Up": _book(bid=0.38, ask=0.40, microprice=0.90),
                "Down": _book(bid=0.34, ask=0.50, microprice=0.10),
            }
        },
        now=60.0,
        elapsed=60.0,
    )
    assert blockers == []
    assert signal is not None
    assert signal["outcome"] == "Up"
    assert signal["fair_basis"] == "one_minus_complement_best_bid"
    assert signal["p_fair"] == 0.66


def test_integrity_disagreement_parks_immediately():
    terminal = {
        "generation_checksum": lane.CHECKSUM,
        "window_start_s": lane.FORWARD_START_S,
        "raw_clock_complete": True,
        "intent": None,
        "trade_receipts": [],
        "book_receipts": [],
        "raw_evidence_checksum": lane.base.canonical_checksum(
            {"trade_receipts": [], "book_receipts": []}
        ),
        "raw_integrity": {"continuity_disagreements": 1},
        "blockers": [],
    }
    reduced = lane.reduce_generation([terminal], [], {})
    assert reduced["status"] == "PARK_IRRECOVERABLE_INTEGRITY_DISAGREEMENT"
    assert reduced["stop_writer"] is True
