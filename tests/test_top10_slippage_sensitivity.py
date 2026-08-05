from scripts.analyze_top10_fresh_slippage_sensitivity import build_report


def _event(wallet: str, source: float, ask: float, bid: float, size: float = 1.0, depth: float = 10.0):
    return {
        "copy_size_usd": size,
        "receipt_to_fetch_latency_ms": 12_000,
        "result": {
            "book": {
                "best_ask": ask,
                "best_bid": bid,
                "book_timestamp": "123",
                "source_price": source,
                "top_of_book": {
                    "best_ask": ask,
                    "best_ask_depth_usd": depth,
                    "best_bid": bid,
                    "book_timestamp": "123",
                },
            }
        },
        "source_price": source,
        "transaction_hash": f"tx-{wallet}-{ask}",
        "wallet": wallet,
    }


def test_slippage_sweep_applies_current_and_wider_caps(tmp_path):
    rows = [
        _event("a", 0.50, 0.51, 0.50),
        _event("a", 0.50, 0.524, 0.514),
        _event("b", 0.50, 0.60, 0.59),
    ]

    report = build_report(rows, base_slippage_bps=250.0, min_fill_ratio=0.999, event_log=tmp_path / "events.jsonl")

    by_label = {rung["label"]: rung for rung in report["rungs"]}
    assert by_label["current"]["copyable_events"] == 1
    assert by_label["1.5x"]["copyable_events"] == 1
    assert by_label["2x"]["copyable_events"] == 2
    assert by_label["uncapped"]["copyable_events"] == 3
    assert by_label["current"]["reject_reasons"] == {"price_above_slippage_cap": 2}


def test_park_rule_requires_positive_pnl_and_three_copyable(tmp_path):
    rows = [
        _event("a", 0.50, 0.49, 0.48),
        _event("b", 0.50, 0.49, 0.48),
        _event("c", 0.50, 0.49, 0.48),
    ]

    report = build_report(rows, base_slippage_bps=250.0, min_fill_ratio=0.999, event_log=tmp_path / "events.jsonl")

    assert report["rungs"][0]["copyable_events"] == 3
    assert report["rungs"][0]["paper_pnl_usd"] < 0
    assert report["park_rule"]["status"] == "PARK_NO_POSITIVE_FRESH_BOOK_POLICY"
