from scripts import report_realized_fee_receipts as fees


def _receipt(amount_micro: int) -> dict:
    return {
        "status": "0x1",
        "logs": [{
            "address": fees.PUSD_TOKEN,
            "topics": [fees.TRANSFER_TOPIC, "0x" + "0" * 24 + "a" * 40, "0x" + "0" * 64],
            "data": hex(amount_micro),
        }],
    }


def test_receipt_cost_selects_matching_pusd_debit() -> None:
    amount, sender = fees._receipt_cost(_receipt(1_261_839), expected_total_usd=1.261844)
    assert amount == 1.261839
    assert sender == "0x" + "a" * 40


def test_reconcile_pending_fee_uses_receipt_minus_response_cost() -> None:
    ledger = {"orders": [{
        "order_id": "0xorder",
        "market_slug": "btc-updown-5m-1",
        "submitted_at": "2026-07-20T03:25:33Z",
        "expected_vs_realized_fee": {
            "status": "PENDING_RECEIPT",
            "realized_fee_usd": None,
            "response_cost_usd": 1.219999,
            "response_expected_fee_usd": 0.041845,
        },
        "trade_result": {"tx_hashes": ["0x" + "1" * 64]},
    }]}

    rows = fees.reconcile_pending_fees(ledger, receipt_loader=lambda _tx: _receipt(1_261_839))

    assert rows[0]["realized_fee_usd"] == 0.04184
    assert rows[0]["realized_minus_expected_fee_usd"] == -0.000005
    assert rows[0]["status"] == "PASS_RECEIPT_RECONCILED"


def test_merge_cost_rows_is_idempotent_by_transaction() -> None:
    row = {
        "tx": "0xabc", "order_id": "0xorder", "response_cost_usd": 1.0,
        "receipt_total_cost_usd": 1.04, "realized_fee_usd": 0.04,
        "market_slug": "btc", "submitted_at": "2026-07-20T00:00:00Z",
    }
    first = fees._merge_cost_rows({"rows": []}, [row])
    second = fees._merge_cost_rows(first, [row])
    assert len(second["rows"]) == 1
    assert second["summary"]["pUSD_out_sum"] == 1.04


def test_fee_coverage_measures_receipt_and_names_missing_hash_gap() -> None:
    tx = "0x" + "1" * 64
    ledger = {
        "orders": [
            {
                "order_id": "measured",
                "final_status": "FILLED",
                "submitted_at": "2026-07-06T00:00:00Z",
                "limit_price": 0.30,
                "trade_result": {
                    "response_filled_size_usd": 1.0,
                    "response_fill_size_shares": 3.333333,
                    "tx_hashes": [tx],
                },
            },
            {
                "order_id": "gap",
                "final_status": "FILLED",
                "submitted_at": "2026-07-06T00:05:00Z",
                "limit_price": 0.30,
                "trade_result": {
                    "response_filled_size_usd": 1.0,
                    "response_fill_size_shares": 3.333333,
                },
            },
        ]
    }

    rows = fees.reconcile_fee_coverage(
        ledger,
        receipts={tx: _receipt(1_049_000)},
        receipt_errors={},
        start_iso="2026-07-05T12:55:00Z",
    )

    assert rows[0]["status"] == "PASS_RECEIPT_MEASURED"
    assert rows[0]["realized_fee_usd"] == 0.049
    assert abs(rows[0]["receipt_measured_fee_rate"] - 0.07) < 0.000001
    assert rows[1]["status"] == "COVERAGE_GAP_TX_HASH_COUNT"
