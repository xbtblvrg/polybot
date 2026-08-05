from scripts.report_f418_spread_elasticity_shadow import build_report
from scripts.launch_f418_spread_elasticity_shadow_service import (
    LABEL,
    build_launchd_payload,
)


def _event(intent_id: str, ts: str, asset: str, slug: str, outcome: str, price: float) -> dict:
    shares = 1.0 / price
    return {
        "event": "wallet_copy_live_lifecycle",
        "status": "LIVE_FILLED",
        "intent_id": intent_id,
        "ts": ts,
        "payload": {
            "market_id": asset,
            "response_fill_price": price,
            "response_fill_size_shares": shares,
            "response_filled_size_usd": 1.0,
            "wallet_copy_execute_live_profile": {
                "market_slug": slug,
                "condition_id": slug,
                "outcome": outcome,
            },
        },
    }


def test_spread_shadow_classifies_fresh_books_and_fails_closed_on_stale_books():
    events = [
        _event("i1", "2026-07-24T08:20:10Z", "a1", "btc-updown-5m-100", "up", 0.5),
        _event("i2", "2026-07-24T08:25:10Z", "a2", "btc-updown-5m-200", "down", 0.4),
        _event("i3", "2026-07-24T08:30:10Z", "a3", "btc-updown-5m-300", "up", 0.5),
    ]
    books = [
        {"event_type": "best_bid_ask", "asset_id": "a1", "captured_at_s": 1784881211, "best_bid": 0.49, "best_ask": 0.50},
        {"event_type": "best_bid_ask", "asset_id": "a2", "captured_at_s": 1784881512, "best_bid": 0.36, "best_ask": 0.40},
        {"event_type": "best_bid_ask", "asset_id": "a3", "captured_at_s": 1784881900, "best_bid": 0.40, "best_ask": 0.50},
    ]
    resolutions = [
        {"market_slug": "btc-updown-5m-100", "winning_outcome": "up"},
        {"market_slug": "btc-updown-5m-200", "winning_outcome": "down"},
        {"market_slug": "btc-updown-5m-300", "winning_outcome": "up"},
    ]
    report = build_report(
        event_rows=events,
        book_rows=books,
        resolution_rows=resolutions,
        generated_at="2026-07-24T09:00:00Z",
        min_resolved_windows=50,
    )
    assert report["paper_only"] is True
    assert report["live_mutation"] is False
    assert report["coverage"]["resolved_f418_fills"] == 3
    assert report["coverage"]["spread_classified_rows"] == 2
    assert report["coverage"]["resolved_without_fresh_book"] == 1
    assert [row["spread_bin"] for row in report["rows"]] == [
        "tight_le_0.02",
        "medium_0.02_0.05",
    ]
    assert report["status"] == "ACCRUING"


def test_spread_shadow_requires_positive_holdout_cell_after_sample_gate():
    events = [
        _event(f"i{index}", f"2026-07-24T08:{20 + index:02d}:10Z", f"a{index}", f"btc-updown-5m-{index}", "up", 0.5)
        for index in range(3)
    ]
    books = [
        {
            "event_type": "best_bid_ask",
            "asset_id": f"a{index}",
            "captured_at_s": 1784881210 + index * 60,
            "best_bid": 0.49,
            "best_ask": 0.50,
        }
        for index in range(3)
    ]
    resolutions = [
        {"market_slug": f"btc-updown-5m-{index}", "winning_outcome": "up"}
        for index in range(3)
    ]
    report = build_report(
        event_rows=events,
        book_rows=books,
        resolution_rows=resolutions,
        generated_at="2026-07-24T09:00:00Z",
        min_resolved_windows=2,
    )
    assert report["status"] == "GATE_READY_POSITIVE_CELL"
    assert report["gate"]["qualified_positive_holdout_cells"] == ["tight_le_0.02"]
    assert report["gate"]["live_change_allowed"] is False


def test_spread_shadow_launchd_payload_is_paper_reporter_only():
    payload = build_launchd_payload(
        python="/usr/bin/python3",
        stdout="/tmp/spread.out",
        stderr="/tmp/spread.err",
    )
    assert payload["Label"] == LABEL
    assert payload["KeepAlive"] is True
    arguments = payload["ProgramArguments"]
    assert arguments[0] == "/usr/bin/python3"
    assert arguments[1].endswith("run_f418_spread_elasticity_shadow_service.py")
    assert "--execute-live" not in arguments
