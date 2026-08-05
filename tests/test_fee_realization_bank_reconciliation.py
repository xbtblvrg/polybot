import pytest

from scripts.report_fee_realization_bank_reconciliation import (
    assert_bank_identity,
    assert_fee_receipt_count,
    build_report,
)


def _scorecard(*, pnl: float = -34.578093, actual: float | None = None) -> dict:
    out = {
        "generated_at": "2026-07-19T03:01:29Z",
        "lifetime_pnl_truth": {
            "scope": {"latest_order_ts": "2026-07-19T03:00:17Z"},
            "total": {"pnl_usd": pnl, "cost_usd": 100.0, "resolved_fills": 2},
        },
    }
    if actual is not None:
        out["since_topup_truth"] = {
            "baseline_usd": 100.0,
            "baseline_iso": "2026-07-19T02:00:00Z",
            "account_value_usd": 101.5,
            "canonical_pnl_usd": 1.5,
            "unresolved_open_cost_usd": 0.0,
            "actual_delta_vs_baseline_usd": actual,
            "cash_diff_reconciliation_residual": {"residual_usd": 10.328578},
        }
    return out


def test_bank_identity_and_population_fork_are_named() -> None:
    truth = {
        "total": {"pnl_usd": -33.0, "cost_usd": 101.5, "resolved_fills": 3},
        "events": [
            {
                "status": "FILLED",
                "resolved": True,
                "submitted_at": "2026-07-19T02:00:00Z",
                "cost_usd": 99.5,
                "pnl_usd": -34.0,
            },
            {
                "status": "FILLED",
                "resolved": True,
                "submitted_at": "2026-07-19T02:30:00Z",
                "cost_usd": 1.0,
                "pnl_usd": 0.0,
            },
            {
                "status": "FILLED",
                "resolved": True,
                "submitted_at": "2026-07-19T03:05:00Z",
                "cost_usd": 1.0,
                "pnl_usd": 1.0,
            },
        ],
    }
    report = build_report(
        baseline_scorecard=_scorecard(),
        current_scorecard=_scorecard(actual=-24.779465),
        current_truth=truth,
        response_basis_truth=truth,
        ledger={"orders": []},
        generated_at="now",
    )

    assert report["bank_identity"]["residual_usd"] == 0.0
    assert report["bank_identity"]["status"] == "PASS_WITHIN_1.00"
    assert report["bank_identity"]["named_cash_movements_usd"] == 0.0
    assert report["bank_identity"]["excluded_stale_gap_snapshot_usd"] == 10.328578
    assert report["response_basis_counterfactual"]["bank_residual_usd"] == 34.5
    assert report["response_basis_counterfactual"]["collapses_residual_by_more_than_1_usd"] is False
    assert report["population_fork"]["resolved_fill_delta"] == 1
    assert report["population_fork"]["appended_resolved_rows"] == 1
    assert report["population_fork"]["status"] == "STALE_BASELINE_ARTEFACT_NOT_A_FORK"
    assert report["fee_authority"]["active_fee_rate"] == 0.0
    assert_bank_identity(report)


def test_bank_identity_fails_loudly_above_one_dollar() -> None:
    report = build_report(
        baseline_scorecard=_scorecard(pnl=-10.0),
        current_scorecard={
            **_scorecard(actual=10.0),
            "since_topup_truth": {
                "baseline_usd": 100.0,
                "baseline_iso": "2026-07-19T02:00:00Z",
                "account_value_usd": 90.0,
                "canonical_pnl_usd": 0.0,
                "unresolved_open_cost_usd": 0.0,
            },
        },
        current_truth={"total": {}, "events": []},
        response_basis_truth={"total": {}, "events": []},
        ledger={"orders": []},
        generated_at="now",
    )

    with pytest.raises(AssertionError, match="exceeds \\$1.00"):
        assert_bank_identity(report)


def test_observed_fee_count_comes_from_embedded_receipt_block() -> None:
    report = build_report(
        baseline_scorecard=_scorecard(),
        current_scorecard=_scorecard(actual=-24.0),
        current_truth={"total": {}, "events": []},
        response_basis_truth={"total": {}, "events": []},
        ledger={"orders": []},
        generated_at="now",
        receipt_report={"summary": {"receipt_measured_rows": 3, "realized_fee_usd_measured": 0.2}},
    )
    assert report["fee_authority"]["orders_with_observed_distinct_realized_fee"] == 3
    assert_fee_receipt_count(report)


def test_zero_observed_count_is_rejected_when_receipt_block_has_pass_rows() -> None:
    report = {
        "fee_authority": {
            "orders_with_observed_distinct_realized_fee": 0,
            "receipt_backfill": {"receipt_measured_rows": 1},
        }
    }
    with pytest.raises(AssertionError, match="count is zero"):
        assert_fee_receipt_count(report)
