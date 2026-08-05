from scripts import run_btc5m_native_l2_complement_ask_cap_parity_stale_ask as lane


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
    assert lane.CONFIG["forward_start_s"] == 1_785_014_100
    assert lane.CONFIG["training_cutoff_s"] == 1_785_013_500
    assert lane.CONFIG["features"]["max_receipt_gap_s"] == 5.0
    assert lane.CONFIG["terminal_clock"]["integrity_disagreement_park"] is True
    assert lane.CONFIG["paper_only"] is True


def test_signal_refuses_dual_outcome_equal_realized_edge_without_fee_tiebreak():
    signal, blockers = lane.choose_signal(
        markets={"BTC": {}},
        trades_by_asset={},
        snapshots=[],
        books={
            "BTC": {
                "Up": _book(bid=0.37, ask=0.40, microprice=0.90),
                "Down": _book(bid=0.30, ask=0.35, microprice=0.10),
            }
        },
        now=60.0,
        elapsed=60.0,
    )
    assert blockers == ["dual_outcome_equal_edge"]
    assert signal is None


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
    }
    reduced = lane.reduce_generation([terminal], [], {})
    assert reduced["status"] == "PARK_IRRECOVERABLE_INTEGRITY_DISAGREEMENT"
    assert reduced["stop_writer"] is True
