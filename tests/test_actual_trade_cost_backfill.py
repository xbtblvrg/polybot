from __future__ import annotations

from scripts import backfill_today_actual_trade_costs as backfill


def test_annotate_actual_trade_costs_exact_single_order_tx_group() -> None:
    ledger = {
        "orders": [
            {
                "order_id": "0xorder",
                "final_status": "FILLED",
                "submitted_at": "2026-07-07T10:00:00+00:00",
                "market_slug": "btc-updown-5m-1783418400",
                "trade_result": {
                    "response_filled_size_usd": 4.0,
                    "response_fill_size_shares": 10.0,
                    "tx_hashes": ["0xtx"],
                },
            },
            {
                "order_id": "missing",
                "final_status": "FILLED",
                "submitted_at": "2026-07-07T10:05:00+00:00",
                "trade_result": {"response_filled_size_usd": 2.0},
            },
        ]
    }

    report = backfill.annotate_actual_trade_costs(
        ledger,
        {"0xtx": {"actual_cost_usd": 3.2, "actual_size_shares": 10.0}},
        start_ts=1783382400,
        end_ts=1783468800,
        updated_at="2026-07-07T10:30:00Z",
    )

    order = ledger["orders"][0]
    assert order["actual_trade_cost_usd"] == 3.2
    assert order["trade_result"]["actual_trade_cost_usd"] == 3.2
    assert order["price_improvement_usd"] == 0.8
    assert report["summary"]["fills_total"] == 2
    assert report["summary"]["fills_with_tx"] == 1
    assert report["summary"]["fills_with_tx_before_backfill"] == 1
    assert report["summary"]["fills_missing_tx"] == 1
    assert report["summary"]["actual_cost_annotated_fills"] == 1


def test_annotate_actual_trade_costs_skips_ambiguous_multi_order_tx_group() -> None:
    ledger = {
        "orders": [
            {
                "order_id": f"order-{idx}",
                "final_status": "FILLED",
                "submitted_at": "2026-07-07T10:00:00+00:00",
                "trade_result": {"response_filled_size_usd": 2.0, "tx_hashes": ["0xsame"]},
            }
            for idx in range(2)
        ]
    }

    report = backfill.annotate_actual_trade_costs(
        ledger,
        {"0xsame": {"actual_cost_usd": 3.2}},
        start_ts=1783382400,
        end_ts=1783468800,
        updated_at="2026-07-07T10:30:00Z",
    )

    assert "actual_trade_cost_usd" not in ledger["orders"][0]
    assert report["summary"]["ambiguous_tx_groups"] == 1
    assert report["summary"]["actual_cost_annotated_fills"] == 0


def test_annotate_actual_trade_costs_uses_clob_order_id_backfill_for_missing_tx() -> None:
    ledger = {
        "orders": [
            {
                "order_id": "0xorder",
                "final_status": "FILLED",
                "submitted_at": "2026-07-07T10:00:00+00:00",
                "market_slug": "btc-updown-5m-1783418400",
                "trade_result": {
                    "response_filled_size_usd": 4.0,
                    "response_fill_size_shares": 10.0,
                },
            }
        ]
    }

    report = backfill.annotate_actual_trade_costs(
        ledger,
        {},
        start_ts=1783382400,
        end_ts=1783468800,
        updated_at="2026-07-07T10:30:00Z",
        actual_by_order_id={
            "0xorder": {
                "actual_cost_usd": 3.6,
                "actual_size_shares": 10.0,
                "tx": "0xmaker",
                "trade_id": "trade-1",
                "source": "clob_associate_trade_by_order_id",
            }
        },
    )

    order = ledger["orders"][0]
    assert order["transaction_hash"] == "0xmaker"
    assert order["actual_trade_cost_usd"] == 3.6
    assert order["trade_result"]["tx_hashes"] == ["0xmaker"]
    assert order["actual_trade_cost_source"] == "clob_associate_trade_by_order_id"
    assert order["price_improvement_usd"] == 0.4
    assert report["summary"]["fills_with_tx_before_backfill"] == 0
    assert report["summary"]["fills_with_tx"] == 1
    assert report["summary"]["fills_missing_tx"] == 0
    assert report["summary"]["clob_order_id_annotated_fills"] == 1


def test_annotate_actual_trade_costs_scales_partial_trade_record_to_fill_size() -> None:
    ledger = {
        "orders": [
            {
                "order_id": "0xpartial",
                "final_status": "FILLED",
                "submitted_at": "2026-07-07T10:00:00+00:00",
                "trade_result": {
                    "response_filled_size_usd": 4.0,
                    "response_fill_size_shares": 10.0,
                    "tx_hashes": ["0xpartialtx"],
                },
            }
        ]
    }

    report = backfill.annotate_actual_trade_costs(
        ledger,
        {"0xpartialtx": {"actual_cost_usd": 2.0, "actual_size_shares": 5.0}},
        start_ts=1783382400,
        end_ts=1783468800,
        updated_at="2026-07-07T10:30:00Z",
    )

    order = ledger["orders"][0]
    assert order["actual_trade_cost_usd"] == 4.0
    assert order["actual_trade_cost_normalization"] == "scaled_partial_trade_price_to_fill_size"
    assert report["summary"]["partial_scaled_annotations"] == 1
    assert report["summary"]["annotation_rejected_suspect"] == 0


def test_annotate_actual_trade_costs_rejects_suspect_improvement_to_intended() -> None:
    ledger = {
        "orders": [
            {
                "order_id": "0xsuspect",
                "final_status": "FILLED",
                "submitted_at": "2026-07-07T10:00:00+00:00",
                "trade_result": {
                    "response_filled_size_usd": 4.0,
                    "response_fill_size_shares": 10.0,
                    "tx_hashes": ["0xsuspecttx"],
                },
            }
        ]
    }

    report = backfill.annotate_actual_trade_costs(
        ledger,
        {"0xsuspecttx": {"actual_cost_usd": 0.1, "actual_size_shares": 10.0}},
        start_ts=1783382400,
        end_ts=1783468800,
        updated_at="2026-07-07T10:30:00Z",
    )

    order = ledger["orders"][0]
    assert "actual_trade_cost_usd" not in order
    assert order["actual_trade_cost_rejected_reason"] == "suspect_actual_improvement_or_missing_size"
    assert order["intended_cost_usd"] == 4.0
    assert report["summary"]["actual_cost_annotated_fills"] == 0
    assert report["summary"]["annotation_rejected_suspect"] == 1
