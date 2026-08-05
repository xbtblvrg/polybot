from scripts.summarize_order109_usdc_transfer_audit import build_summary


def test_order109_summary_is_bounded_and_names_unenumerable_class():
    report = build_summary(
        {
            "status": "PASS",
            "classification_summary": {
                "fill_settlement": {"rows": 2, "net_usd": -12.0},
                "redemption_payout": {"rows": 1, "net_usd": 3.0},
            },
            "residual_reconciliation": {"canonical_residual_usd": 10.328578},
            "fetch": {"transfers": {"rows_in_window": 3}},
            "other_counterparty_rows": [],
        }
    )

    assert report["ledger_row_count"] == 6
    assert report["acceptance"]["within_20_rows"] is True
    assert report["acceptance"]["signed_sum_matches_residual_within_one_cent"] is True
    assert report["ledger_signed_sum_usd"] == 10.328578
    assert report["acceptance"]["unique_residual_tx_hash"] is None
    assert report["acceptance"]["unmapped_class"] == "unknown_account_value_identity_residual"
    assert report["ledger_rows"][-1]["enumeration"] == "UNENUMERABLE_AS_USDC_TRANSFER"
    assert report["acceptance"]["decision"] == "RETIRED_PERMANENT_NAMED_CONSTANT_NO_FURTHER_RETRACE"
