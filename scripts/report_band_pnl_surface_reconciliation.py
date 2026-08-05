#!/usr/bin/env python3
"""Reconcile canonical realized PnL with the taker measurement surface."""

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
from scripts.report_passive_at_source_holdout import day_bounded_split  # noqa: E402
from scripts.report_taker_subband_holdout import (  # noqa: E402
    _resolution_index,
    _resolved_taker_rows,
)
from src.wallet_copy.fees import (  # noqa: E402
    POLYMARKET_EMBEDDED_FEE_FORMULA,
    POLYMARKET_EMBEDDED_FEE_RATE,
    POLYMARKET_EMBEDDED_FEE_SOURCE,
    expected_polymarket_buy_fee_usd,
    modeled_unvalidated_polymarket_buy_fee_usd,
)
from src.wallet_copy.performance import load_resolutions  # noqa: E402
from src.wallet_copy.pnl_truth import build_pnl_truth  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = ROOT / "data/research/band_pnl_surface_reconciliation_latest.json"
FOCUS_BANDS = ("01a_25_32", "01b_32_40", "01c_40_50")


def _key(row: dict[str, Any]) -> str:
    return f"{row.get('market_slug') or ''}|{row.get('submitted_at') or ''}"


def _truth_realized(row: dict[str, Any]) -> float:
    return float(row.get("pnl_usd") or 0.0)


def _artifact_realized(row: dict[str, Any]) -> float:
    return float(row.get("pnl_usd_realized", row.get("post_fee_pnl_usd")) or 0.0)


def _artifact_band(row: dict[str, Any]) -> str:
    price = float(row.get("entry_price") or 0.0)
    if 0.25 <= price < 0.32:
        return "01a_25_32"
    if 0.32 <= price < 0.40:
        return "01b_32_40"
    # Preserve the source artefact's published boundary exactly. It includes
    # price 0.50, while canonical price_subbucket classifies 0.50 as 02_50_70.
    if 0.40 <= price < 0.5000001:
        return "01c_40_50"
    return ""


def _band_reconciliation(
    truth_rows: list[dict[str, Any]], artifact_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    truth = {_key(row): row for row in truth_rows}
    artifact = {_key(row): row for row in artifact_rows}
    truth_keys = set(truth)
    artifact_keys = set(artifact)
    matched = sorted(truth_keys & artifact_keys)
    only_truth = sorted(truth_keys - artifact_keys)
    only_artifact = sorted(artifact_keys - truth_keys)

    truth_cost = sum(float(row.get("cost_usd") or 0.0) for row in truth.values())
    artifact_cost = sum(float(row.get("filled_cost_usd") or 0.0) for row in artifact.values())
    truth_pnl = sum(_truth_realized(row) for row in truth.values())
    artifact_pnl = sum(_artifact_realized(row) for row in artifact.values())
    population = sum(_truth_realized(truth[key]) for key in only_truth) - sum(
        _artifact_realized(artifact[key]) for key in only_artifact
    )
    modeled_fee = sum(
        float(row.get("modeled_unvalidated_fee_usd") or 0.0) for row in artifact.values()
    )
    residual = sum(
        _truth_realized(truth[key]) - _artifact_realized(artifact[key]) for key in matched
    )
    observed_gap = truth_pnl - artifact_pnl
    explained = population + residual
    decomposition_error = observed_gap - explained

    matched_cost_residual = sum(
        float(truth[key].get("cost_usd") or 0.0)
        - float(artifact[key].get("filled_cost_usd") or 0.0)
        for key in matched
    )
    cost_population = sum(float(truth[key].get("cost_usd") or 0.0) for key in only_truth) - sum(
        float(artifact[key].get("filled_cost_usd") or 0.0) for key in only_artifact
    )
    return {
        "ledger_rows": len(truth),
        "artefact_rows": len(artifact),
        "matched_rows": len(matched),
        "rows_only_in_ledger": len(only_truth),
        "rows_only_in_artefact": len(only_artifact),
        "row_keys_only_in_ledger": only_truth,
        "row_keys_only_in_artefact": only_artifact,
        "ledger_cost_usd": round(truth_cost, 6),
        "artefact_cost_usd": round(artifact_cost, 6),
        "cost_gap_usd": round(truth_cost - artifact_cost, 6),
        "cost_gap_decomposition": {
            "population_usd": round(cost_population, 6),
            "matched_cost_basis_residual_usd": round(matched_cost_residual, 6),
            "sum_usd": round(cost_population + matched_cost_residual, 6),
        },
        "ledger_pnl_usd_realized": round(truth_pnl, 6),
        "artefact_pnl_usd_realized": round(artifact_pnl, 6),
        "modeled_unvalidated_fee_usd": round(modeled_fee, 6),
        "observed_pnl_gap_usd": round(observed_gap, 6),
        "pnl_gap_decomposition": {
            "population_usd": round(population, 6),
            "matched_method_residual_usd": round(residual, 6),
            "sum_usd": round(explained, 6),
            "error_usd": round(decomposition_error, 6),
            "status": "PASS_WITHIN_0.01" if abs(decomposition_error) <= 0.01 else "FAIL",
        },
    }


def _truth_realized_row(row: dict[str, Any]) -> dict[str, Any]:
    shares = float(row.get("shares") or 0.0)
    cost = float(row.get("cost_usd") or 0.0)
    price = cost / shares if shares > 0.0 and 0.0 < cost / shares < 1.0 else float(row.get("limit_price") or 0.0)
    realized = _truth_realized(row)
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
    modeled_fee = modeled_unvalidated_polymarket_buy_fee_usd(shares=shares, price=price)
    return {
        **row,
        "fee_price": round(price, 6),
        "expected_fee_usd": fee,
        "modeled_unvalidated_fee_usd": modeled_fee,
        "pnl_usd_realized": round(realized, 6),
    }


def _book_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    cost = sum(float(row.get("cost_usd") or 0.0) for row in rows)
    realized = sum(float(row.get("pnl_usd_realized") or 0.0) for row in rows)
    authoritative_fee = sum(float(row.get("expected_fee_usd") or 0.0) for row in rows)
    post_fee = realized - authoritative_fee
    modeled_fee = sum(float(row.get("modeled_unvalidated_fee_usd") or 0.0) for row in rows)
    return {
        "resolved_fills": len(rows),
        "cost_usd": round(cost, 6),
        "payout_usd": round(cost + realized, 6),
        "pnl_usd_realized": round(realized, 6),
        "roi_pct_realized": round(100.0 * realized / cost, 6) if cost else None,
        "expected_fee_usd": round(authoritative_fee, 6),
        "post_fee_pnl_usd": round(post_fee, 6),
        "post_fee_roi_pct": round(100.0 * post_fee / cost, 6) if cost else None,
        "modeled_unvalidated_fee_usd": round(modeled_fee, 6),
        "modeled_unvalidated": True,
        "accounting_authority": False,
    }


def build_report(
    *, truth_events: list[dict[str, Any]], artifact_rows: list[dict[str, Any]], generated_at: str,
    decision_events: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    truth_resolved = [
        _truth_realized_row(row)
        for row in truth_events
        if row.get("resolved") and str(row.get("status") or "") == "FILLED"
    ]
    bands = {
        band: _band_reconciliation(
            [row for row in truth_resolved if row.get("price_subbucket") == band],
            [
                row
                for row in artifact_rows
                if band == _artifact_band(row)
            ],
        )
        for band in FOCUS_BANDS
    }
    decision_resolved = [
        _truth_realized_row(row)
        for row in (decision_events if decision_events is not None else truth_events)
        if row.get("resolved") and str(row.get("status") or "") == "FILLED"
    ]
    split = day_bounded_split(decision_resolved)
    holdout = _book_stats(split["holdout"])
    development_pnl = float(_book_stats(split["development"]).get("pnl_usd_realized") or 0.0)
    holdout_pnl = float(holdout.get("pnl_usd_realized") or 0.0)
    verdict = (
        "REALIZED_EDGE_BOTH_HALVES_POSITIVE"
        if development_pnl > 0.0 and holdout_pnl > 0.0
        else "REALIZED_NO_EDGE_BOTH_HALVES_NON_POSITIVE"
        if development_pnl <= 0.0 and holdout_pnl <= 0.0
        else "REALIZED_DEV_HOLDOUT_SIGN_SPLIT_NO_ESTABLISHED_EDGE"
    )
    return {
        "schema_version": 1,
        "kind": "band_pnl_surface_reconciliation",
        "flow_stage": "MEASURE/DEFEND",
        "generated_at": generated_at,
        "measurement_only": True,
        "live_mutation": False,
        "payout_usd_semantics": {
            "classification": "REALIZED_PAYOUT_MINUS_RECEIPT_MAPPED_COST",
            "provenance": (
                "src/wallet_copy/pnl_truth.py score_order/resolved_pnl: payout is shares "
                "when the selected side wins, zero otherwise; no ledger payout or fee field is read"
            ),
            "realized_identity": "pnl_usd_realized = payout_usd - cost_usd",
            "fee_formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
            "fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
            "fee_source": POLYMARKET_EMBEDDED_FEE_SOURCE,
            "modeled_unvalidated": True,
            "accounting_authority": False,
        },
        "bands": bands,
        "band_definition_mismatch": {
            "canonical_01c": "0.40 <= price < 0.50",
            "artefact_01c": "0.40 <= price < 0.5000001 (includes exact 0.50)",
            "exact_0_50_artefact_rows": sum(
                abs(float(row.get("entry_price") or 0.0) - 0.50) <= 1e-9
                for row in artifact_rows
            ),
        },
        "whole_book_day_bounded": {
            "sample_gate": {
                "status": "PASS" if split["sample_gate_pass"] else "ACCRUING",
                "development_rows": len(split["development"]),
                "holdout_rows": len(split["holdout"]),
                "distinct_days_per_bin": split["distinct_days_per_bin"],
                "split_integrity": split["split_integrity"],
            },
            "aggregate": _book_stats(decision_resolved),
            "development": _book_stats(split["development"]),
            "chronological_holdout": holdout,
            "verdict": verdict,
        },
        "decision": verdict,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--resolutions", default=str(DEFAULT_RESOLUTIONS))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ledger = json.loads(Path(args.ledger).read_text())
    resolutions = load_resolutions(args.resolutions)
    receipt_costs, _ = _load_receipt_costs()
    actual_trade_costs, _ = _load_actual_trade_costs()
    truth = build_pnl_truth(
        ledger,
        resolutions,
        receipt_costs=receipt_costs,
        actual_trade_costs=actual_trade_costs,
    )
    response_truth = build_pnl_truth(ledger, resolutions)
    by_condition, by_slug = _resolution_index(Path(args.resolutions))
    artifact_rows = _resolved_taker_rows(
        ledger,
        resolution_by_condition=by_condition,
        resolution_by_slug=by_slug,
    )
    report = build_report(
        truth_events=truth.get("events") or [],
        decision_events=response_truth.get("events") or [],
        artifact_rows=artifact_rows,
        generated_at=datetime.now(UTC).isoformat(),
    )
    atomic_write_json(Path(args.output), report)
    print(json.dumps({"decision": report["decision"], "bands": report["bands"]}, sort_keys=True))


if __name__ == "__main__":
    main()
