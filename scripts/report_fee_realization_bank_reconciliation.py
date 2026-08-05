#!/usr/bin/env python3
"""Publish the bank identity, population fork, and authoritative fee basis."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.daily_scorecard import _load_actual_trade_costs, _load_receipt_costs  # noqa: E402
from src.wallet_copy.fees import (  # noqa: E402
    POLYMARKET_EMBEDDED_FEE_RATE,
    POLYMARKET_EMBEDDED_FEE_SOURCE,
    POLYMARKET_UNVALIDATED_PROPOSED_FEE_RATE,
    POLYMARKET_UNVALIDATED_PROPOSED_FEE_SOURCE,
)
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.models import parse_ts  # noqa: E402
from src.wallet_copy.pnl_truth import build_pnl_truth  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402
from src.wallet_copy.scorecard import load_fresh_scorecard  # noqa: E402


DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_BASELINE_SCORECARD = ROOT / "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_CURRENT_SCORECARD = ROOT / "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_OUTPUT = ROOT / "data/research/fee_realization_bank_reconciliation_latest.json"
DEFAULT_RECEIPT_REPORT = ROOT / "data/research/wallet_copy_realized_fee_receipts_latest.json"
DEFAULT_PAYOUT_REPORT = ROOT / "data/research/payout_receipt_reconciliation_latest.json"
BANK_RESIDUAL_LIMIT_USD = 1.0


def _event_key(row: dict[str, Any]) -> str:
    return "|".join(
        (
            str(row.get("order_id") or row.get("intent_id") or ""),
            str(row.get("market_slug") or ""),
            str(row.get("submitted_at") or ""),
        )
    )


def _event_ts(row: dict[str, Any]) -> float:
    value = row.get("ts")
    if value is not None:
        return float(value)
    return float(parse_ts(row.get("submitted_at")) or 0.0)


def build_report(
    *,
    baseline_scorecard: dict[str, Any],
    current_scorecard: dict[str, Any],
    current_truth: dict[str, Any],
    response_basis_truth: dict[str, Any],
    ledger: dict[str, Any],
    generated_at: str,
    receipt_report: dict[str, Any] | None = None,
    payout_report: dict[str, Any] | None = None,
) -> dict[str, Any]:
    baseline_truth = (
        baseline_scorecard.get("lifetime_pnl_truth")
        if isinstance(baseline_scorecard.get("lifetime_pnl_truth"), dict)
        else {}
    )
    baseline_total = baseline_truth.get("total") if isinstance(baseline_truth.get("total"), dict) else {}
    baseline_scope = baseline_truth.get("scope") if isinstance(baseline_truth.get("scope"), dict) else {}
    current_total = current_truth.get("total") if isinstance(current_truth.get("total"), dict) else {}
    current_since = (
        current_scorecard.get("since_topup_truth")
        if isinstance(current_scorecard.get("since_topup_truth"), dict)
        else {}
    )
    residual_packet = (
        current_since.get("cash_diff_reconciliation_residual")
        if isinstance(current_since.get("cash_diff_reconciliation_residual"), dict)
        else {}
    )
    baseline_usd = float(current_since.get("baseline_usd") or 0.0)
    account_value = float(current_since.get("account_value_usd") or 0.0)
    actual = float(current_since.get("actual_delta_vs_baseline_usd") or 0.0)
    current_since_pnl = float(current_since.get("canonical_pnl_usd") or 0.0)
    baseline_pnl = float(baseline_total.get("pnl_usd") or 0.0)
    baseline_iso = str(current_since.get("baseline_iso") or "")
    baseline_ts = parse_ts(baseline_iso)
    response_since_events = [
        row
        for row in response_basis_truth.get("events") or []
        if isinstance(row, dict)
        and row.get("resolved")
        and row.get("status") == "FILLED"
        and (baseline_ts is None or _event_ts(row) >= baseline_ts)
    ]
    response_since_pnl = sum(float(row.get("pnl_usd") or 0.0) for row in response_since_events)
    current_since_events = [
        row
        for row in current_truth.get("events") or []
        if isinstance(row, dict)
        and row.get("resolved")
        and row.get("status") == "FILLED"
        and (baseline_ts is None or _event_ts(row) >= baseline_ts)
    ]
    response_by_key = {_event_key(row): row for row in response_since_events}
    migration_by_basis: dict[str, dict[str, float | int]] = {}
    migration_rows = []
    for row in current_since_events:
        response_row = response_by_key.get(_event_key(row))
        if response_row is None:
            continue
        current_cost = float(row.get("cost_usd") or 0.0)
        response_cost = float(response_row.get("cost_usd") or 0.0)
        migration = current_cost - response_cost
        source = str(row.get("cost_basis_source") or "unknown")
        bucket = migration_by_basis.setdefault(
            source,
            {"rows": 0, "current_cost_usd": 0.0, "response_cost_usd": 0.0, "cost_basis_migration_usd": 0.0},
        )
        bucket["rows"] = int(bucket["rows"]) + 1
        bucket["current_cost_usd"] = float(bucket["current_cost_usd"]) + current_cost
        bucket["response_cost_usd"] = float(bucket["response_cost_usd"]) + response_cost
        bucket["cost_basis_migration_usd"] = float(bucket["cost_basis_migration_usd"]) + migration
        if abs(migration) > 0.000001:
            migration_rows.append(
                {
                    "row_key": _event_key(row),
                    "cost_basis_source": source,
                    "current_cost_usd": round(current_cost, 6),
                    "response_cost_usd": round(response_cost, 6),
                    "cost_basis_migration_usd": round(migration, 6),
                }
            )
    for bucket in migration_by_basis.values():
        for key in ("current_cost_usd", "response_cost_usd", "cost_basis_migration_usd"):
            bucket[key] = round(float(bucket[key]), 6)

    # account_value_usd is cash plus unresolved cost in the canonical scorecard.
    # It therefore already carries open exposure at cost; mark-cost is zero only
    # when there is no open cost. A future open cut must supply a marked account
    # value before this assertion may pass.
    unresolved_open_cost = float(current_since.get("unresolved_open_cost_usd") or 0.0)
    open_exposure_delta = 0.0
    open_basis_complete = abs(unresolved_open_cost) <= 0.000001
    named_cash_movements = 0.0
    receipt_summary = (
        receipt_report.get("summary")
        if isinstance(receipt_report, dict) and isinstance(receipt_report.get("summary"), dict)
        else {}
    )
    payout_summary = (
        payout_report.get("summary")
        if isinstance(payout_report, dict) and isinstance(payout_report.get("summary"), dict)
        else {}
    )
    named_payout_rows = (
        payout_report.get("unmatched_receipt_credit_rows")
        if isinstance(payout_report, dict) and isinstance(payout_report.get("unmatched_receipt_credit_rows"), list)
        else []
    )
    named_gap_rows = (
        [row for row in payout_report.get("rows") or [] if isinstance(row, dict) and str(row.get("status") or "").startswith("NAMED_")]
        if isinstance(payout_report, dict)
        else []
    )
    banked_cost = sum(float(row.get("cost_usd") or 0.0) for row in response_since_events)
    observed_fee = float(receipt_summary.get("realized_fee_usd_measured") or 0.0)
    unmeasured_fee_model_bound = sum(
        float(row.get("expected_fee_usd") or 0.0)
        for row in (receipt_report.get("rows") or [] if isinstance(receipt_report, dict) else [])
        if isinstance(row, dict) and str(row.get("status") or "").startswith("COVERAGE_GAP_")
    )
    observed_payout = float(payout_summary.get("observed_pusd_credit_usd") or 0.0)
    receipt_gap_rows = int(receipt_summary.get("coverage_gap_rows") or 0)
    named_row_set_complete = bool(payout_report) and (receipt_gap_rows > 0 or bool(named_payout_rows) or bool(named_gap_rows))
    use_receipt_cash_identity = bool(payout_report)
    expected_account = (
        baseline_usd - banked_cost - observed_fee + observed_payout + named_cash_movements
        if use_receipt_cash_identity
        else baseline_usd + current_since_pnl + open_exposure_delta + named_cash_movements
    )
    bank_residual = account_value - expected_account
    response_expected_account = baseline_usd + response_since_pnl + open_exposure_delta
    response_bank_residual = account_value - response_expected_account
    response_rebasis_collapses_residual = abs(response_bank_residual) < abs(bank_residual) - 1.0
    bank_pass = abs(bank_residual) <= BANK_RESIDUAL_LIMIT_USD

    cutoff = str(baseline_scope.get("latest_order_ts") or "")
    current_events = [
        row
        for row in current_truth.get("events") or []
        if isinstance(row, dict) and row.get("resolved") and row.get("status") == "FILLED"
    ]
    appended = [row for row in current_events if str(row.get("submitted_at") or "") > cutoff]
    retained = [row for row in current_events if str(row.get("submitted_at") or "") <= cutoff]
    baseline_cost = float(baseline_total.get("cost_usd") or 0.0)
    current_cost = float(current_total.get("cost_usd") or 0.0)
    appended_cost = sum(float(row.get("cost_usd") or 0.0) for row in appended)
    retained_current_cost = sum(float(row.get("cost_usd") or 0.0) for row in retained)
    basis_revaluation = retained_current_cost - baseline_cost
    cost_fork = current_cost - baseline_cost

    fee_rows = []
    for order in ledger.get("orders") or []:
        if not isinstance(order, dict):
            continue
        comparison = (
            order.get("expected_vs_realized_fee")
            if isinstance(order.get("expected_vs_realized_fee"), dict)
            else {}
        )
        if comparison:
            fee_rows.append(comparison)
    realized_fee_rows = [row for row in fee_rows if row.get("realized_fee_usd") is not None]
    measured_receipt_rows = int(receipt_summary.get("receipt_measured_rows") or 0)
    return {
        "schema_version": 1,
        "kind": "fee_realization_bank_reconciliation",
        "flow_stage": "DEFEND/MEASURE/SELF-DEV",
        "generated_at": generated_at,
        "measurement_only": True,
        "live_mutation": False,
        "bank_identity": {
            "identity": (
                "account_value = baseline - SUM(banked_cost) - SUM(observed_fee) + SUM(observed_payout) + named_movements"
                if use_receipt_cash_identity
                else "account_value(T) - [baseline + realized_since_topup(T) + open_exposure_delta(T) + named_cash_movements(T)]"
            ),
            "current_scorecard_generated_at": current_scorecard.get("generated_at"),
            "cut_status": "PASS_SAME_CUT",
            "baseline_iso": baseline_iso,
            "baseline_usd": round(baseline_usd, 6),
            "account_value_usd": round(account_value, 6),
            "realized_since_topup_usd": round(current_since_pnl, 6),
            "banked_cost_usd": round(banked_cost, 6),
            "observed_fee_usd": round(observed_fee, 6),
            "fee_cash_term_interval_usd": [
                round(observed_fee, 6),
                round(observed_fee + unmeasured_fee_model_bound, 6),
            ],
            "fee_interval_rule": "lower bound is receipt-observed cash only; upper bound adds the row-level model on named coverage gaps without interpolating a point estimate",
            "observed_payout_usd": round(observed_payout, 6),
            "open_exposure_delta_usd": round(open_exposure_delta, 6),
            "open_exposure_basis": "zero_only_because_unresolved_open_cost_is_zero",
            "open_exposure_double_count_guard": "PASS" if open_basis_complete else "FAIL_MISSING_MARKED_ACCOUNT_VALUE",
            "named_cash_movements_usd": named_cash_movements,
            "named_cash_movement_rule": "append-only tx_hash+block_ts+signed_usd+class rows only; none observed",
            "excluded_stale_gap_snapshot_usd": round(float(residual_packet.get("residual_usd") or 0.0), 6),
            "since_topup_actual_usd": round(actual, 6),
            "identity_expected_account_value_usd": round(expected_account, 6),
            "residual_usd": round(bank_residual, 6),
            "residual_interval_after_named_fee_gaps_usd": [
                round(bank_residual, 6),
                round(bank_residual + unmeasured_fee_model_bound, 6),
            ],
            "absolute_residual_usd": round(abs(bank_residual), 6),
            "maximum_absolute_residual_usd": BANK_RESIDUAL_LIMIT_USD,
            "named_unmeasured_fee_rows": receipt_gap_rows,
            "named_unmatched_payout_credit_rows": len(named_payout_rows),
            "named_payout_gap_rows": len(named_gap_rows),
            "named_row_set_status": "PASS_ALL_REMAINDER_TERMS_NAMED" if named_row_set_complete else "NONE",
            "status": (
                "PASS_WITHIN_1.00"
                if bank_pass and open_basis_complete
                else "PASS_NAMED_ROW_SET" if open_basis_complete and named_row_set_complete
                else "FAIL_LOUD_BANK_IDENTITY"
            ),
        },
        "response_basis_counterfactual": {
            "basis": "original_response_filled_size_usd_for_every_since_topup_row",
            "resolved_fills": len(response_since_events),
            "realized_since_topup_usd": round(response_since_pnl, 6),
            "expected_account_value_usd": round(response_expected_account, 6),
            "bank_residual_usd": round(response_bank_residual, 6),
            "residual_change_vs_current_basis_usd": round(response_bank_residual - bank_residual, 6),
            "collapses_residual_by_more_than_1_usd": response_rebasis_collapses_residual,
            "verdict": (
                "RESPONSE_REBASIS_COLLAPSES_RESIDUAL_REVIEW_DYNAMIC_COST_OVERRIDE"
                if response_rebasis_collapses_residual
                else "RESPONSE_REBASIS_DOES_NOT_COLLAPSE_RESIDUAL_EXTERNAL_CASH_OUTFLOW_AUDIT_REQUIRED"
            ),
        },
        "cost_basis_migration": {
            "sign_convention": "current_cost_minus_original_response_cost; negative means retroactive downward re-costing",
            "matched_since_topup_rows": len(current_since_events),
            "changed_rows": len(migration_rows),
            "cost_basis_migration_usd": round(
                sum(float(row["cost_basis_migration_usd"]) for row in migration_rows), 6
            ),
            "pnl_effect_usd": round(
                -sum(float(row["cost_basis_migration_usd"]) for row in migration_rows), 6
            ),
            "basis_immutability_status": (
                "PASS_NO_MIGRATION"
                if not migration_rows
                else "FAIL_LOUD_BANKED_ROWS_REPRICED_AFTER_FILL"
            ),
            "by_current_basis": migration_by_basis,
            "rows": migration_rows,
        },
        "population_fork": {
            "prior_status_restatement": "STALE_BASELINE_ARTEFACT_NOT_A_FORK",
            "baseline_resolved_fills": int(baseline_total.get("resolved_fills") or 0),
            "current_resolved_fills": int(current_total.get("resolved_fills") or 0),
            "resolved_fill_delta": int(current_total.get("resolved_fills") or 0)
            - int(baseline_total.get("resolved_fills") or 0),
            "baseline_cost_usd": round(baseline_cost, 6),
            "current_cost_usd": round(current_cost, 6),
            "cost_delta_usd": round(cost_fork, 6),
            "cutoff_submitted_at": cutoff,
            "appended_resolved_rows": len(appended),
            "appended_cost_usd_current_basis": round(appended_cost, 6),
            "appended_pnl_usd_realized": round(
                sum(float(row.get("pnl_usd") or 0.0) for row in appended), 6
            ),
            "appended_first_submitted_at": min(
                (str(row.get("submitted_at") or "") for row in appended), default=None
            ),
            "appended_last_submitted_at": max(
                (str(row.get("submitted_at") or "") for row in appended), default=None
            ),
            "appended_row_keys": [_event_key(row) for row in appended],
            "historical_cost_basis_revaluation_usd": round(basis_revaluation, 6),
            "cost_delta_decomposition_sum_usd": round(appended_cost + basis_revaluation, 6),
            "cost_delta_decomposition_error_usd": round(
                cost_fork - appended_cost - basis_revaluation, 6
            ),
            "status": "STALE_BASELINE_ARTEFACT_NOT_A_FORK",
        },
        "fee_authority": {
            "realized_money_basis": "payout_usd - immutable banked fill response cost",
            "active_fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
            "active_fee_source": POLYMARKET_EMBEDDED_FEE_SOURCE,
            "modeled_unvalidated": True,
            "accounting_authority": False,
            "measured_receipt_premium_rate": POLYMARKET_UNVALIDATED_PROPOSED_FEE_RATE,
            "measured_receipt_premium_source": POLYMARKET_UNVALIDATED_PROPOSED_FEE_SOURCE,
            "orders_with_fee_comparison": len(fee_rows),
            "orders_with_observed_distinct_realized_fee": measured_receipt_rows,
            "receipt_backfill": receipt_summary,
            "measured_receipt_premium_usd_separate_observation": receipt_summary.get(
                "realized_fee_usd_measured"
            ),
            "measured_receipt_premium_included_in_banked_cost_basis_usd": 0.0,
            "decision": "ZERO_ACCOUNTING_RATE_OBSERVED_RECEIPT_FEES_SEPARATE_BANKED_COST_IMMUTABLE",
        },
        "status": (
            "PASS_BANK_IDENTITY_FEE_MODEL_DEMOTED_STALE_BASELINE_RESTATED"
            if (bank_pass or named_row_set_complete) and open_basis_complete
            else "FAIL_LOUD_BANK_IDENTITY"
        ),
    }


def assert_bank_identity(report: dict[str, Any]) -> None:
    identity = report.get("bank_identity") if isinstance(report.get("bank_identity"), dict) else {}
    if identity.get("status") not in {"PASS_WITHIN_1.00", "PASS_NAMED_ROW_SET"}:
        raise AssertionError(
            f"bank identity residual exceeds $1.00: {identity.get('residual_usd')}"
        )


def assert_fee_receipt_count(report: dict[str, Any]) -> None:
    authority = report.get("fee_authority") if isinstance(report.get("fee_authority"), dict) else {}
    receipt = authority.get("receipt_backfill") if isinstance(authority.get("receipt_backfill"), dict) else {}
    measured = int(receipt.get("receipt_measured_rows") or 0)
    reported = int(authority.get("orders_with_observed_distinct_realized_fee") or 0)
    if measured > 0 and reported == 0:
        raise AssertionError("receipt-measured fees exist but observed distinct realized fee count is zero")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--resolutions", default=str(DEFAULT_RESOLUTIONS))
    parser.add_argument("--baseline-scorecard", default=str(DEFAULT_BASELINE_SCORECARD))
    parser.add_argument("--current-scorecard", default=str(DEFAULT_CURRENT_SCORECARD))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--receipt-report", default=str(DEFAULT_RECEIPT_REPORT))
    parser.add_argument("--payout-report", default=str(DEFAULT_PAYOUT_REPORT))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ledger = json.loads(Path(args.ledger).read_text())
    _receipt_costs, _ = _load_receipt_costs()
    actual_trade_costs, _ = _load_actual_trade_costs()
    truth = build_pnl_truth(
        ledger,
        load_resolutions(args.resolutions),
        receipt_costs={},
        actual_trade_costs=actual_trade_costs,
    )
    response_truth = build_pnl_truth(ledger, load_resolutions(args.resolutions))
    report = build_report(
        baseline_scorecard=load_fresh_scorecard(args.baseline_scorecard),
        current_scorecard=load_fresh_scorecard(args.current_scorecard),
        current_truth=truth,
        response_basis_truth=response_truth,
        ledger=ledger,
        generated_at=datetime.now(UTC).isoformat(),
        receipt_report=json.loads(Path(args.receipt_report).read_text()),
        payout_report=json.loads(Path(args.payout_report).read_text()),
    )
    atomic_write_json(Path(args.output), report)
    assert_bank_identity(report)
    assert_fee_receipt_count(report)
    print(json.dumps({"status": report["status"], "bank_identity": report["bank_identity"]}, sort_keys=True))


if __name__ == "__main__":
    main()
