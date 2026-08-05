import json
from pathlib import Path

from scripts.report_h2_account_value_residual import _canonical_residual_class, build_report


def _write_scorecard(path: Path, *, generated_at: str, canonical: float, actual: float, residual: float) -> None:
    path.write_text(
        json.dumps(
            {
                "kind": "wallet_copy_daily_scorecard",
                "generated_at": generated_at,
                "day": "2026-07-08",
                "chain_reconciliation": {
                    "delta_vs_expected_usd": round(actual - canonical, 6),
                    "status": "MISMATCH",
                },
                "since_topup_truth": {
                    "baseline_usd": 335.0,
                    "account_value_usd": round(335.0 + actual, 6),
                    "live_cash_balance_usd": round(335.0 + actual, 6),
                    "actual_account_delta_vs_baseline_usd": actual,
                    "actual_cash_delta_vs_baseline_usd": actual,
                    "canonical_pnl_usd": canonical,
                    "expected_account_value_usd": round(335.0 + canonical, 6),
                    "expected_cash_identity_usd": round(335.0 + canonical, 6),
                    "unresolved_open_cost_usd": 0.0,
                    "balance_status": "OK",
                    "actual_value_basis": "account_value",
                    "cash_diff_reconciliation_residual": {
                        "status": "NAMED_RESIDUAL",
                        "residual_usd": residual,
                        "residual_classification": "unaccounted_one_time_cash_movement",
                        "fill_cost_payout_explained_usd": 0.0,
                        "joined_tx_groups": 147,
                        "unjoined_tx_groups": 0,
                        "ledger_fills_missing_tx": 0,
                    },
                },
            }
        ),
        encoding="utf-8",
    )


def test_h2_account_value_residual_classifies_unaccounted_cash_movement(tmp_path: Path) -> None:
    _write_scorecard(
        tmp_path / "wallet_copy_daily_scorecard_1.json",
        generated_at="2026-07-08T01:00:00Z",
        canonical=-30.0,
        actual=-39.0,
        residual=-7.590385,
    )
    _write_scorecard(
        tmp_path / "wallet_copy_daily_scorecard_2.json",
        generated_at="2026-07-08T02:00:00Z",
        canonical=-35.0,
        actual=-44.0,
        residual=-7.590385,
    )
    _write_scorecard(
        tmp_path / "wallet_copy_daily_scorecard_3.json",
        generated_at="2026-07-08T03:00:00Z",
        canonical=-54.0,
        actual=-60.0,
        residual=-7.590385,
    )
    (tmp_path / "h2_external_redemption_ingestion_latest.json").write_text(
        json.dumps(
            {
                "status": "PASS",
                "acceptance": {
                    "residual_explained_by_external_redeems_usd": 0.0,
                    "residual_unexplained_after_external_redeems_usd": -7.590385,
                },
            }
        ),
        encoding="utf-8",
    )

    report = build_report(tmp_path)

    assert report["summary"]["status"] == "PASS"
    assert report["summary"]["snapshot_count"] == 3
    assert report["summary"]["residual_class"] == "unaccounted_one_time_cash_movement"
    assert report["summary"]["open_position_mark_timing_ruled_out"] is True
    assert report["summary"]["fee_or_dust_ruled_out"] is True
    assert len(report["intervals"]) == 2


def test_h2_normalizes_legacy_unaccounted_residual_classes() -> None:
    assert (
        _canonical_residual_class("account_value_residual_not_explained_by_joined_fill_cost_or_payout")
        == "unaccounted_one_time_cash_movement"
    )
    assert (
        _canonical_residual_class("unaccounted_cash_movement_or_balance_sampling_residual")
        == "unaccounted_one_time_cash_movement"
    )
    assert _canonical_residual_class("fee_or_rounding_dust") == "fee_or_rounding_dust"
