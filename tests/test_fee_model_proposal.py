from __future__ import annotations

from pathlib import Path

from scripts import build_wallet_copy_fee_model_proposal as proposal
from src.wallet_copy.store import atomic_write_json


def test_product_price_inverse_fee_fit_beats_flat_cost_markup(tmp_path: Path) -> None:
    rows = [
        {
            "order_id": "o1",
            "market_slug": "btc-updown-5m-1",
            "response_shares": 20.0,
            "response_fill_price": 0.20,
            "response_cost_usd": 4.0,
            "excess_over_response_usd": 0.224,
        },
        {
            "order_id": "o2",
            "market_slug": "btc-updown-5m-2",
            "response_shares": 10.0,
            "response_fill_price": 0.40,
            "response_cost_usd": 4.0,
            "excess_over_response_usd": 0.168,
        },
    ]
    leak = tmp_path / "leak.json"
    scorecard = tmp_path / "scorecard.json"
    ledger = tmp_path / "ledger.json"
    atomic_write_json(
        leak,
        {
            "summary": {
                "named_cause": "embedded_exchange_fee_excluded_from_response_filled_size_usd",
                "inferred_our_embedded_fee_usd": 0.392,
                "response_cost_usd": 8.0,
                "rows_where_fee_equals_our_excess": 2,
                "rows_where_fee_transfer_covers_our_excess": 2,
            },
            "rows": rows,
        },
    )
    atomic_write_json(
        scorecard,
        {
            "generated_at": "2026-07-06T19:21:00Z",
            "cost_basis_source": "tx_receipt_pusd_debit",
            "canonical_pnl_truth": {
                "total": {"pnl_usd": 1.0, "roi_pct": 12.5},
                "by_lane": {"lane": {"cost_usd": 8.0, "pnl_usd": 1.0, "resolved_fills": 2, "fills": 2}},
            },
            "since_topup_truth": {
                "primary_verdict": "NOT_PRODUCING",
                "canonical_pnl_usd": -1.0,
                "actual_delta_vs_baseline_usd": -2.0,
            },
            "volume_kpi": {"canonical_daily": {"windows_filled": 2, "windows_submitted": 2, "denominator_windows": 288}},
        },
    )
    atomic_write_json(
        ledger,
        {
            "orders": [
                {
                    "status": "FILLED",
                    "trade_result": {
                        "response_filled_size_usd": 4.0,
                        "response_fill_size_shares": 20.0,
                        "details": {"makingAmount": "4", "takingAmount": "20"},
                    },
                }
            ]
        },
    )

    packet = proposal.build_proposal(leak_artifact=leak, scorecard_path=scorecard, ledger_path=ledger)

    product = packet["fits"]["product_price_inverse"]
    flat = packet["fits"]["flat_cost_markup"]
    assert product["rate_pct"] == 7.0
    assert product["sum_abs_residual_usd"] == 0.0
    assert flat["sum_abs_residual_usd"] > product["sum_abs_residual_usd"]
    assert packet["submit_response_fee_field_check"]["explicit_fee_field_found"] is False
    assert packet["recommendation"]["no_unilateral_changes_made"] is True
