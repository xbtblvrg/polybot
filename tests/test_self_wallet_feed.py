import argparse
import json
from datetime import UTC, datetime
from pathlib import Path

from scripts import reconcile_self_wallet_feed as self_feed


def _args(tmp_path: Path, *, grace_s: float = 0.0) -> argparse.Namespace:
    return argparse.Namespace(
        ledger=str(tmp_path / "ledger.json"),
        scorecard=str(tmp_path / "scorecard.json"),
        resolutions=str(tmp_path / "resolutions.jsonl"),
        output=str(tmp_path / "report.json"),
        self_feed_log=str(tmp_path / "self_feed.jsonl"),
        user="0xee888fa7b96007f7fa270988e92bddb0ae19ed10",
        start_iso="",
        end_iso="1970-01-01T00:33:20Z",
        day="",
        limit=100,
        max_pages=1,
        timeout_s=1.0,
        ledger_missing_grace_s=grace_s,
        data_api_base_url="https://data-api.polymarket.com",
        polygon_rpc_url="https://polygon.invalid",
        polygon_lookback_blocks=10,
        enable_polygon=False,
    )


def _ledger_order(tx: str, *, order_id: str, ts: str, price: float, size: float) -> dict:
    return {
        "order_id": order_id,
        "status": "FILLED",
        "final_status": "FILLED",
        "submitted_at": ts,
        "condition_id": "0xcond",
        "market_slug": "btc-updown-5m-1000",
        "outcome": "Up",
        "trade_result": {
            "side": "BUY",
            "response_fill_price": price,
            "response_fill_size_shares": size,
            "response_filled_size_usd": round(price * size, 6),
            "tx_hashes": [tx],
        },
    }


def _data_api_trade(tx: str, *, ts: int, price: float, size: float) -> dict:
    return {
        "transactionHash": tx,
        "timestamp": ts,
        "asset": "123",
        "conditionId": "0xcond",
        "slug": "btc-updown-5m-1000",
        "outcome": "Up",
        "side": "BUY",
        "price": price,
        "size": size,
    }


def test_self_feed_reports_critical_row_level_diffs(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path)
    ledger = {
        "orders": [
            _ledger_order("0xmatch", order_id="0xorder1", ts="1970-01-01T00:16:40Z", price=0.4, size=5.0),
            _ledger_order("0xmissing", order_id="0xorder2", ts="1970-01-01T00:18:20Z", price=0.5, size=4.0),
        ]
    }
    Path(args.ledger).write_text(json.dumps(ledger), encoding="utf-8")
    Path(args.scorecard).write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "chain_reconciliation": {"reconciliation_start_iso": "1970-01-01T00:15:00Z"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        self_feed,
        "_fetch_data_api_trades",
        lambda **_: (
            [
                _data_api_trade("0xmatch", ts=1000, price=0.4, size=5.0),
                _data_api_trade("0xextra", ts=1200, price=0.3, size=2.0),
            ],
            {"status": "OK", "pages": 1, "truncated": False},
        ),
    )

    report = self_feed.build_report(args)

    summary = report["summary"]
    assert report["status"] == "CRITICAL"
    assert summary["matched_ledger_tx_groups"] == 1
    assert summary["ledger_missing_self_feed_critical"] == 1
    assert summary["self_feed_missing_ledger_critical"] == 1
    assert summary["amount_mismatch_tx_groups"] == 0


def test_self_feed_dedupes_exact_data_api_duplicate_events(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path)
    ledger = {
        "orders": [
            _ledger_order("0xdup", order_id="0xorder1", ts="1970-01-01T00:16:40Z", price=0.4, size=5.0),
        ]
    }
    Path(args.ledger).write_text(json.dumps(ledger), encoding="utf-8")
    Path(args.scorecard).write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "chain_reconciliation": {"reconciliation_start_iso": "1970-01-01T00:15:00Z"},
            }
        ),
        encoding="utf-8",
    )
    duplicate = _data_api_trade("0xdup", ts=1000, price=0.4, size=5.0)
    monkeypatch.setattr(
        self_feed,
        "_fetch_data_api_trades",
        lambda **_: ([duplicate, dict(duplicate)], {"status": "OK", "pages": 1, "truncated": False}),
    )

    report = self_feed.build_report(args)

    summary = report["summary"]
    assert report["status"] == "PASS"
    assert summary["data_api_raw_trade_rows"] == 2
    assert summary["data_api_trade_rows"] == 1
    assert summary["data_api_duplicate_event_rows"] == 1
    assert summary["self_feed_event_rows"] == 1
    assert summary["amount_mismatch_tx_groups"] == 0


def test_self_feed_keeps_fresh_ledger_missing_trade_in_grace(monkeypatch, tmp_path: Path) -> None:
    args = _args(tmp_path, grace_s=300.0)
    args.end_iso = "1970-01-01T00:16:50Z"
    ledger = {
        "orders": [
            _ledger_order("0xfresh", order_id="0xorder1", ts="1970-01-01T00:16:40Z", price=0.4, size=5.0),
        ]
    }
    Path(args.ledger).write_text(json.dumps(ledger), encoding="utf-8")
    Path(args.scorecard).write_text(
        json.dumps(
            {
                "generated_at": datetime.now(UTC).isoformat(),
                "chain_reconciliation": {"reconciliation_start_iso": "1970-01-01T00:15:00Z"},
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(self_feed, "_fetch_data_api_trades", lambda **_: ([], {"status": "OK"}))
    monkeypatch.setattr(self_feed.time, "time", lambda: 1010.0)

    report = self_feed.build_report(args)

    summary = report["summary"]
    assert report["status"] == "PENDING_GRACE"
    assert summary["ledger_missing_self_feed_critical"] == 0
    assert summary["ledger_missing_self_feed_within_grace"] == 1
