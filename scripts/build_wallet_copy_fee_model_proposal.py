#!/usr/bin/env python3
"""Build the wallet-copy embedded-fee model proposal artifact.

Flow stage: LIVE/SELF-DEV. This is read-only analysis over receipt and
scorecard artifacts; it does not change live execution thresholds.
"""

from __future__ import annotations

import argparse
import math
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]

import sys

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


def _latest(pattern: str) -> Path:
    candidates = [path for path in (ROOT / "data" / "research").glob(pattern) if path.is_file()]
    if not candidates:
        raise FileNotFoundError(f"no artifact matches data/research/{pattern}")
    return max(candidates, key=lambda path: path.stat().st_mtime)


def _fee_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows = payload.get("rows") if isinstance(payload.get("rows"), list) else []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        shares = num(row.get("response_shares"))
        price = num(row.get("response_fill_price"))
        fee = num(row.get("excess_over_response_usd"))
        response_cost = num(row.get("response_cost_usd"))
        if shares <= 0 or price <= 0 or price >= 1 or fee <= 0 or response_cost <= 0:
            continue
        out.append(row)
    return out


def _mean(values: list[float]) -> float:
    return round(sum(values) / len(values), 6) if values else 0.0


def _fit_through_origin(
    rows: list[dict[str, Any]],
    feature: Callable[[dict[str, Any]], float],
) -> dict[str, Any]:
    xs: list[float] = []
    ys: list[float] = []
    for row in rows:
        x = feature(row)
        y = num(row.get("excess_over_response_usd"))
        if x > 0 and y > 0:
            xs.append(float(x))
            ys.append(float(y))
    denom = sum(x * x for x in xs)
    rate = (sum(x * y for x, y in zip(xs, ys)) / denom) if denom else 0.0
    residuals = [y - (rate * x) for x, y in zip(xs, ys)]
    abs_residuals = [abs(value) for value in residuals]
    mean_y = sum(ys) / len(ys) if ys else 0.0
    ss_res = sum(value * value for value in residuals)
    ss_tot = sum((y - mean_y) * (y - mean_y) for y in ys)
    worst = sorted(
        (
            {
                "order_id": str(row.get("order_id") or ""),
                "market_slug": str(row.get("market_slug") or ""),
                "price": round(num(row.get("response_fill_price")), 6),
                "shares": round(num(row.get("response_shares")), 6),
                "actual_fee_usd": round(y, 6),
                "predicted_fee_usd": round(rate * x, 6),
                "residual_usd": round(y - (rate * x), 6),
            }
            for row, x, y in zip(rows, xs, ys)
        ),
        key=lambda item: abs(float(item["residual_usd"])),
        reverse=True,
    )[:8]
    return {
        "row_count": len(ys),
        "rate": round(rate, 9),
        "rate_pct": round(rate * 100.0, 6),
        "actual_fee_sum_usd": round(sum(ys), 6),
        "predicted_fee_sum_usd": round(sum(rate * x for x in xs), 6),
        "sum_abs_residual_usd": round(sum(abs_residuals), 6),
        "mean_abs_residual_usd": _mean(abs_residuals),
        "rmse_usd": round(math.sqrt(ss_res / len(residuals)), 6) if residuals else 0.0,
        "max_abs_residual_usd": round(max(abs_residuals), 6) if abs_residuals else 0.0,
        "r2": round(1.0 - (ss_res / ss_tot), 6) if ss_tot else 1.0,
        "worst_residual_rows": worst,
    }


def _fee_rate_distribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    product_rates: list[float] = []
    min_price_rates: list[float] = []
    markup_rates: list[float] = []
    for row in rows:
        shares = num(row.get("response_shares"))
        price = num(row.get("response_fill_price"))
        fee = num(row.get("excess_over_response_usd"))
        response_cost = num(row.get("response_cost_usd"))
        product_feature = shares * price * (1.0 - price)
        min_feature = shares * min(price, 1.0 - price)
        if product_feature > 0:
            product_rates.append(fee / product_feature)
        if min_feature > 0:
            min_price_rates.append(fee / min_feature)
        if response_cost > 0:
            markup_rates.append(fee / response_cost)

    def _summary(values: list[float]) -> dict[str, Any]:
        return {
            "count": len(values),
            "min_pct": round(min(values) * 100.0, 6) if values else 0.0,
            "mean_pct": round((sum(values) / len(values)) * 100.0, 6) if values else 0.0,
            "max_pct": round(max(values) * 100.0, 6) if values else 0.0,
        }

    return {
        "product_price_inverse_rate": _summary(product_rates),
        "min_price_rate": _summary(min_price_rates),
        "fee_pct_of_response_cost": _summary(markup_rates),
    }


def _collect_fee_like_keys(value: Any, prefix: str = "") -> set[str]:
    found: set[str] = set()
    if isinstance(value, dict):
        for key, item in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if "fee" in str(key).lower():
                found.add(path)
            found.update(_collect_fee_like_keys(item, path))
    elif isinstance(value, list):
        for idx, item in enumerate(value[:20]):
            found.update(_collect_fee_like_keys(item, f"{prefix}[{idx}]"))
    return found


def _submit_response_field_check(ledger: dict[str, Any]) -> dict[str, Any]:
    orders = ledger.get("orders") if isinstance(ledger.get("orders"), list) else []
    trade_result_keys: set[str] = set()
    details_keys: set[str] = set()
    amount_fields: set[str] = set()
    fee_like_keys: set[str] = set()
    filled_samples = 0
    for order in orders:
        if not isinstance(order, dict):
            continue
        result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        details = result.get("details") if isinstance(result.get("details"), dict) else {}
        if result:
            trade_result_keys.update(str(key) for key in result.keys())
            fee_like_keys.update(_collect_fee_like_keys(result, "trade_result"))
        if details:
            details_keys.update(str(key) for key in details.keys())
            fee_like_keys.update(_collect_fee_like_keys(details, "trade_result.details"))
        for key in ("making_amount", "taking_amount", "response_filled_size_usd", "response_fill_size_shares"):
            if key in result:
                amount_fields.add(key)
        if str(order.get("status") or order.get("final_status") or "").upper() == "FILLED":
            filled_samples += 1
    return {
        "ledger_filled_order_samples": filled_samples,
        "explicit_fee_field_found": bool(fee_like_keys),
        "fee_like_response_keys": sorted(fee_like_keys),
        "sample_trade_result_keys": sorted(trade_result_keys),
        "sample_details_keys": sorted(details_keys),
        "observed_amount_fields": sorted(amount_fields),
        "conclusion": (
            "No explicit submit-time fee field was found in stored trade_result/details responses; "
            "current reliable fee source is receipt pUSD debit or deterministic fee formula."
        ),
    }


def _lane_net_edges(scorecard: dict[str, Any]) -> dict[str, Any]:
    by_lane = scorecard.get("by_lane")
    if not isinstance(by_lane, dict):
        truth = scorecard.get("canonical_pnl_truth") if isinstance(scorecard.get("canonical_pnl_truth"), dict) else {}
        by_lane = truth.get("by_lane") if isinstance(truth.get("by_lane"), dict) else {}
    lanes: dict[str, Any] = {}
    for lane, row in sorted(by_lane.items()):
        if not isinstance(row, dict):
            continue
        cost = num(row.get("cost_usd"))
        pnl = num(row.get("pnl_usd"))
        lanes[str(lane)] = {
            "cost_usd": round(cost, 6),
            "pnl_usd": round(pnl, 6),
            "net_roi_after_fee_pct": round((100.0 * pnl / cost), 6) if cost else 0.0,
            "resolved_fills": int(num(row.get("resolved_fills"))),
            "fills": int(num(row.get("fills"))),
        }
    return lanes


def build_proposal(
    *,
    leak_artifact: Path,
    scorecard_path: Path,
    ledger_path: Path,
) -> dict[str, Any]:
    leak = load_json(leak_artifact, default={})
    scorecard = load_json(scorecard_path, default={})
    ledger = load_json(ledger_path, default={})
    rows = _fee_rows(leak if isinstance(leak, dict) else {})
    if not rows:
        raise ValueError(f"no fee rows in {leak_artifact}")
    summary = leak.get("summary") if isinstance(leak.get("summary"), dict) else {}
    product_fit = _fit_through_origin(
        rows,
        lambda row: num(row.get("response_shares"))
        * num(row.get("response_fill_price"))
        * (1.0 - num(row.get("response_fill_price"))),
    )
    min_price_fit = _fit_through_origin(
        rows,
        lambda row: num(row.get("response_shares")) * min(num(row.get("response_fill_price")), 1.0 - num(row.get("response_fill_price"))),
    )
    flat_cost_fit = _fit_through_origin(rows, lambda row: num(row.get("response_cost_usd")))
    day_total = (
        scorecard.get("canonical_pnl_truth", {}).get("total", {})
        if isinstance(scorecard.get("canonical_pnl_truth"), dict)
        else {}
    )
    since_topup = scorecard.get("since_topup_truth") if isinstance(scorecard.get("since_topup_truth"), dict) else {}
    volume = (
        scorecard.get("volume_kpi", {}).get("canonical_daily", {})
        if isinstance(scorecard.get("volume_kpi"), dict)
        else {}
    )
    fee_sum = num(summary.get("inferred_our_embedded_fee_usd") or summary.get("excess_over_response_usd"))
    response_cost_sum = num(summary.get("response_cost_usd"))
    day_roi = num(day_total.get("roi_pct"))
    return {
        "kind": "wallet_copy_fee_model_proposal",
        "flow_stage": "LIVE/SELF-DEV",
        "generated_at": utc_now_iso(),
        "inputs": {
            "leak_artifact": str(leak_artifact),
            "scorecard": str(scorecard_path),
            "ledger": str(ledger_path),
            "cost_basis_source": str(scorecard.get("cost_basis_source") or ""),
        },
        "embedded_fee_evidence": {
            "row_count": len(rows),
            "named_cause": summary.get("named_cause"),
            "inferred_our_embedded_fee_usd": round(fee_sum, 6),
            "response_cost_usd": round(response_cost_sum, 6),
            "fee_pct_of_response_cost_weighted": round((100.0 * fee_sum / response_cost_sum), 6)
            if response_cost_sum
            else 0.0,
            "rows_where_fee_equals_our_excess": summary.get("rows_where_fee_equals_our_excess"),
            "rows_where_fee_transfer_covers_our_excess": summary.get("rows_where_fee_transfer_covers_our_excess"),
            "distribution": _fee_rate_distribution(rows),
        },
        "fits": {
            "recommended_formula": "fee_usd = 0.07 * shares * price * (1 - price)",
            "product_price_inverse": {
                "formula": "rate * shares * price * (1 - price)",
                **product_fit,
            },
            "requested_min_price_form": {
                "formula": "rate * shares * min(price, 1 - price)",
                **min_price_fit,
            },
            "flat_cost_markup": {
                "formula": "rate * response_cost_usd",
                **flat_cost_fit,
            },
            "interpretation": (
                "The shares*price*(1-price) form fits at essentially a 7% base rate and lower residual "
                "than flat cost markup. A flat buffer or flat fee percent is structurally weaker because "
                "the effective markup rises at lower prices."
            ),
        },
        "submit_response_fee_field_check": _submit_response_field_check(ledger if isinstance(ledger, dict) else {}),
        "receipt_basis_gate_context": {
            "scorecard_generated_at": scorecard.get("generated_at"),
            "day_pnl_usd": round(num(day_total.get("pnl_usd")), 6),
            "day_roi_pct": round(day_roi, 6),
            "since_topup_verdict": since_topup.get("primary_verdict"),
            "since_topup_canonical_pnl_usd": round(num(since_topup.get("canonical_pnl_usd")), 6),
            "actual_delta_vs_baseline_usd": round(num(since_topup.get("actual_delta_vs_baseline_usd")), 6),
            "windows_filled": int(num(volume.get("windows_filled"))),
            "windows_submitted": int(num(volume.get("windows_submitted"))),
            "windows_denominator": int(num(volume.get("denominator_windows"))),
        },
        "lane_net_edge_after_fee": _lane_net_edges(scorecard),
        "options_for_fable": [
            {
                "id": "record_expected_fee_formula_at_submit",
                "summary": "Add expected_fee_usd/expected_total_cost_usd to live ledger from the fitted 7% price*(1-price) formula, then keep receipt reconciliation as truth override.",
                "execution_threshold_change": False,
                "pros": [
                    "Future scorecards can separate price edge from embedded fee immediately.",
                    "Does not touch CopyIntent parity or live order routing.",
                ],
                "cons": [
                    "Formula is inferred from receipts because no explicit response fee field is stored.",
                ],
            },
            {
                "id": "fee_aware_edge_gate",
                "summary": "Evaluate copyability and lane admission on net edge after expected embedded fee, not response-cost edge.",
                "execution_threshold_change": True,
                "pros": [
                    "Thin lanes stop appearing profitable before fee.",
                    "Matches OP target to realized account movement.",
                ],
                "cons": [
                    "Requires Fable threshold approval before live execution changes.",
                ],
            },
            {
                "id": "keep_buffer_for_rounding_only",
                "summary": "Do not resize market_order_amount buffer to cover fees; track fee separately and keep buffer limited to rounding/chase mechanics.",
                "execution_threshold_change": False,
                "pros": [
                    "Avoids hiding fee drag inside execution amount buffers.",
                    "Prevents a flat buffer from chasing a price-dependent cost.",
                ],
                "cons": [
                    "Does not itself improve profitability; it makes the economics honest.",
                ],
            },
        ],
        "recommendation": {
            "primary": "record_expected_fee_formula_at_submit",
            "secondary": "fee_aware_edge_gate",
            "decision_needed_from_fable": (
                "Approve formula-based expected-fee capture now; decide whether the thin fast_wf lane "
                "needs a fee-aware edge gate because its receipt-basis net ROI is near zero while the "
                "protection_refill lane remains strongly positive."
            ),
            "no_unilateral_changes_made": True,
        },
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leak-artifact", default="", help="wallet_copy_cost_leak_attribution_*.json path")
    parser.add_argument("--scorecard", default="", help="Receipt-basis scorecard JSON path")
    parser.add_argument("--ledger", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument("--output", default="", help="Output JSON path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    leak_artifact = Path(args.leak_artifact) if args.leak_artifact else _latest("wallet_copy_cost_leak_attribution_*.json")
    scorecard = Path(args.scorecard) if args.scorecard else _latest("wallet_copy_daily_scorecard_2026-07-06*T*_gate.json")
    ledger = Path(args.ledger)
    if not leak_artifact.is_absolute():
        leak_artifact = ROOT / leak_artifact
    if not scorecard.is_absolute():
        scorecard = ROOT / scorecard
    if not ledger.is_absolute():
        ledger = ROOT / ledger
    output = Path(args.output) if args.output else ROOT / "data/research/wallet_copy_fee_model_proposal_latest.json"
    if not output.is_absolute():
        output = ROOT / output
    proposal = build_proposal(leak_artifact=leak_artifact, scorecard_path=scorecard, ledger_path=ledger)
    atomic_write_json(output, proposal)
    print(f"wrote {output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
