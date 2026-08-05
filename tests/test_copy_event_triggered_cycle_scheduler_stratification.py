from scripts.report_copy_event_triggered_cycle_scheduler_stratification import build_report


def _row(
    key: str,
    *,
    wallet: str = "0xaaa",
    price: float = 0.5,
    pnl: float = 1.0,
    edge_pct: float | None = 60.0,
    event_at: str = "2026-07-15T12:00:00Z",
    market: str | None = None,
) -> tuple[str, dict]:
    row = {
        "event_id": key,
        "event_at": event_at,
        "market_slug": market or f"btc-updown-5m-{key}",
        "paper_order_size_usd": 1.0,
        "post_fee_would_pnl_status": "RESOLVED_POST_FEE_MEASURED",
        "post_fee_would_pnl_usd": pnl,
        "price": price,
        "source_wallet": wallet,
    }
    if edge_pct is not None:
        row["pre_trade_edge_pct"] = edge_pct
    return key, row


def _state(rows: dict[str, dict]) -> dict:
    return {
        "kind": "copy_event_triggered_cycle_scheduler_paper_lane",
        "generated_at": "2026-07-17T12:00:00Z",
        "status": "PAPER_CLOCK_MATURE",
        "clock_start_utc": "2026-07-15T00:00:00Z",
        "clock_end_utc": "2026-07-17T00:00:00Z",
        "paper_clock_accumulation_started_at": "2026-07-15T00:00:00Z",
        "paper_clock_accumulator": rows,
    }


def test_scheduler_stratification_finds_source_wallet_survivor() -> None:
    rows = {}
    for idx in range(30):
        key, row = _row(f"first-{idx}", pnl=1.0, event_at="2026-07-15T06:00:00Z")
        rows[key] = row
    for idx in range(30):
        key, row = _row(f"second-{idx}", pnl=1.0, event_at="2026-07-16T18:00:00Z")
        rows[key] = row

    report = build_report(_state(rows), generated_at="2026-07-18T13:30:00Z")

    assert report["summary"]["status"] == "SURVIVING_STRATUM_FOUND_22C_SPEC_REQUIRED"
    survivor = next(
        row for row in report["survivors"] if row["family"] == "source_wallet" and row["key"] == "0xaaa"
    )
    assert survivor["resolved_windows"] == 60
    assert survivor["halves"]["first"]["post_fee_pnl_usd"] > 0
    assert survivor["halves"]["second"]["post_fee_pnl_usd"] > 0


def test_scheduler_stratification_retires_without_positive_both_halves() -> None:
    rows = {}
    for idx in range(60):
        key, row = _row(f"first-{idx}", pnl=1.0, event_at="2026-07-15T06:00:00Z")
        rows[key] = row
    for idx in range(60):
        key, row = _row(f"second-{idx}", pnl=-2.0, event_at="2026-07-16T18:00:00Z")
        rows[key] = row

    report = build_report(_state(rows), generated_at="2026-07-18T13:30:00Z")

    assert report["summary"]["status"] == "NO_SURVIVING_STRATUM_RETIRE"
    assert report["summary"]["survivors"] == 0
    assert report["summary"]["next"] == "retire scheduler paper lane and unload producer"
