#!/usr/bin/env python3
"""Measure resolved taker fills by BTC-5m price subband on day-bounded OOS bins."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_passive_at_source_holdout import day_bounded_split  # noqa: E402
from src.wallet_copy.fees import (  # noqa: E402
    POLYMARKET_EMBEDDED_FEE_FORMULA,
    POLYMARKET_EMBEDDED_FEE_RATE,
    POLYMARKET_EMBEDDED_FEE_SOURCE,
    expected_polymarket_buy_fee_usd,
    modeled_unvalidated_polymarket_buy_fee_usd,
)
from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = ROOT / "data/research/taker_price_subband_holdout_latest.json"
PRICE_SUBBANDS = {
    "00_below_25": (0.0, 0.25),
    "01a_25_32": (0.25, 0.32),
    "01b_32_40": (0.32, 0.40),
    "01c_40_50": (0.40, 0.5000001),
}


def _num(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _resolution_index(path: Path) -> tuple[dict[str, str], dict[str, str]]:
    by_condition: dict[str, str] = {}
    by_slug: dict[str, str] = {}
    try:
        handle = path.open()
    except FileNotFoundError:
        return by_condition, by_slug
    with handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            winner = str(row.get("direction") or "").upper()
            if winner not in {"UP", "DOWN"}:
                continue
            if row.get("condition_id"):
                by_condition[str(row["condition_id"])] = winner
            if row.get("market_slug"):
                by_slug[str(row["market_slug"])] = winner
    return by_condition, by_slug


def _stats(rows: list[dict[str, Any]], *, price_field: str = "entry_price") -> dict[str, Any]:
    cost = sum(float(row["filled_cost_usd"]) for row in rows)
    winner_cost = sum(float(row["filled_cost_usd"]) for row in rows if row["won"])
    winner_basis_shares = sum(float(row["filled_shares"]) for row in rows if row["won"])
    fees = sum(float(row["expected_fee_usd"]) for row in rows)
    realized_pnl = sum(float(row.get("pnl_usd_realized") or 0.0) for row in rows)
    pnl = sum(float(row.get("post_fee_pnl_usd") or 0.0) for row in rows)
    wins = sum(bool(row["won"]) for row in rows)
    weighted_mean_price = (
        sum(float(row[price_field]) * float(row["filled_cost_usd"]) for row in rows) / cost
        if cost
        else None
    )
    histogram: dict[str, int] = {}
    for row in rows:
        bucket_low = math.floor(float(row[price_field]) * 100.0 + 1e-9) / 100.0
        label = f"{bucket_low:.2f}_{bucket_low + 0.01:.2f}"
        histogram[label] = histogram.get(label, 0) + 1
    win_rate = wins / len(rows) if rows else None
    cost_weighted_win_share = winner_cost / cost if cost else None
    winner_harmonic_price = winner_cost / winner_basis_shares if winner_basis_shares else None
    fee_ratio = fees / cost if cost else None
    realized_roi = 100.0 * realized_pnl / cost if cost else None
    if realized_roi is not None and realized_roi < -100.000001:
        raise AssertionError(
            f"cash-secured binary long realized ROI below -100%: {realized_roi:.6f}%"
        )
    fee_inclusive_breakeven = (
        winner_harmonic_price * (1.0 + fee_ratio)
        if winner_harmonic_price is not None and fee_ratio is not None
        else None
    )
    breakeven_gap = (
        cost_weighted_win_share - fee_inclusive_breakeven
        if cost_weighted_win_share is not None and fee_inclusive_breakeven is not None
        else None
    )
    identity_rhs = (
        winner_harmonic_price * pnl / cost
        if winner_harmonic_price is not None and cost
        else None
    )
    basis_consistent = (
        breakeven_gap is None
        or identity_rhs is None
        or abs(breakeven_gap - identity_rhs) <= 1e-6
    )
    return {
        "rows": len(rows),
        "first_submitted_at": rows[0]["submitted_at"] if rows else None,
        "last_submitted_at": rows[-1]["submitted_at"] if rows else None,
        "wins": wins,
        "losses": sum(not bool(row["won"]) for row in rows),
        "win_rate_pct": round(100.0 * win_rate, 6) if win_rate is not None else None,
        "count_win_rate_pct": round(100.0 * win_rate, 6) if win_rate is not None else None,
        "cost_weighted_mean_entry_price": (
            round(weighted_mean_price, 6) if weighted_mean_price is not None else None
        ),
        "cost_weighted_win_share_pct": (
            round(100.0 * cost_weighted_win_share, 6)
            if cost_weighted_win_share is not None
            else None
        ),
        "winner_cost_weighted_harmonic_entry_price": (
            round(winner_harmonic_price, 6) if winner_harmonic_price is not None else None
        ),
        "fee_pct_of_filled_cost": round(100.0 * fee_ratio, 6) if fee_ratio is not None else None,
        "fee_inclusive_breakeven_cost_weighted_win_share_pct": (
            round(100.0 * fee_inclusive_breakeven, 6)
            if fee_inclusive_breakeven is not None
            else None
        ),
        "entry_price_histogram_0_01": dict(sorted(histogram.items())),
        "breakeven_gap_pct": (
            round(100.0 * breakeven_gap, 6) if breakeven_gap is not None else None
        ),
        "basis_consistency": "PASS" if basis_consistent else "FAIL",
        "basis_consistency_abs_error": (
            round(abs(breakeven_gap - identity_rhs), 12)
            if breakeven_gap is not None and identity_rhs is not None
            else None
        ),
        "filled_cost_usd": round(cost, 6),
        "expected_fee_usd": round(fees, 6),
        "modeled_unvalidated": True,
        "modeled_unvalidated_fee_usd": round(
            sum(float(row.get("modeled_unvalidated_fee_usd") or 0.0) for row in rows), 6
        ),
        "pnl_usd_realized": round(realized_pnl, 6),
        "roi_pct_realized": round(realized_roi, 6) if realized_roi is not None else None,
        "realized_long_loss_floor": "PASS",
        "post_fee_pnl_usd": round(pnl, 6),
        "post_fee_roi_pct": round(100.0 * pnl / cost, 6) if cost else None,
    }


def _pnl_concentration(rows: list[dict[str, Any]], *, top_n: int = 2) -> dict[str, Any]:
    ranked = sorted(
        rows,
        key=lambda row: float(row.get("post_fee_pnl_usd") or 0.0),
        reverse=True,
    )
    total = sum(float(row.get("post_fee_pnl_usd") or 0.0) for row in rows)
    top = ranked[:top_n]
    top_pnl = sum(float(row.get("post_fee_pnl_usd") or 0.0) for row in top)
    after_drop = total - top_pnl
    return {
        "top_n": top_n,
        "top_n_pnl_usd": round(top_pnl, 6),
        "pnl_concentration_top2_pct": (
            round(100.0 * top_pnl / total, 6) if total > 0.0 and top_n == 2 else None
        ),
        "post_fee_pnl_after_top_n_drop_usd": round(after_drop, 6),
        "pnl_usd_realized_after_top_n_drop": round(after_drop, 6),
        "positive_after_top_n_drop": after_drop > 0.0,
        "dropped_intents": [
            {
                "market_slug": row.get("market_slug"),
                "submitted_at": row.get("submitted_at"),
                "post_fee_pnl_usd": row.get("post_fee_pnl_usd"),
                "pnl_usd_realized": row.get("pnl_usd_realized", row.get("post_fee_pnl_usd")),
            }
            for row in top
        ],
    }


def _drop_k_curve(rows: list[dict[str, Any]], *, max_k: int = 5) -> list[dict[str, Any]]:
    ranked = sorted(
        rows,
        key=lambda row: float(row.get("post_fee_pnl_usd") or 0.0),
        reverse=True,
    )
    curve = []
    for k in range(max_k + 1):
        retained = ranked[k:]
        stats = _stats(retained)
        curve.append(
            {
                "drop_k": k,
                "retained_rows": len(retained),
                "post_fee_pnl_usd": stats["post_fee_pnl_usd"],
                "post_fee_roi_pct": stats["post_fee_roi_pct"],
                "pnl_usd_realized": stats["pnl_usd_realized"],
                "roi_pct_realized": stats["roi_pct_realized"],
                "sign": "POSITIVE" if float(stats["post_fee_pnl_usd"] or 0.0) > 0 else "NON_POSITIVE",
                "dropped_intents": [
                    {
                        "market_slug": row.get("market_slug"),
                        "submitted_at": row.get("submitted_at"),
                        "post_fee_pnl_usd": row.get("post_fee_pnl_usd"),
                        "pnl_usd_realized": row.get(
                            "pnl_usd_realized", row.get("post_fee_pnl_usd")
                        ),
                    }
                    for row in ranked[:k]
                ],
            }
        )
    return curve


def _leave_one_day_out(rows: list[dict[str, Any]]) -> dict[str, Any]:
    days = sorted(
        {
            str(row.get("submitted_at") or "")[:10]
            for row in rows
            if len(str(row.get("submitted_at") or "")) >= 10
        }
    )
    curve = []
    for day in days:
        retained = [
            row for row in rows if str(row.get("submitted_at") or "")[:10] != day
        ]
        stats = _stats(retained)
        pnl = float(stats["post_fee_pnl_usd"] or 0.0)
        curve.append(
            {
                "omitted_day": day,
                "omitted_rows": len(rows) - len(retained),
                "retained_rows": len(retained),
                "post_fee_pnl_usd": stats["post_fee_pnl_usd"],
                "post_fee_roi_pct": stats["post_fee_roi_pct"],
                "pnl_usd_realized": stats["pnl_usd_realized"],
                "roi_pct_realized": stats["roi_pct_realized"],
                "sign": "POSITIVE" if pnl > 0 else "NON_POSITIVE",
            }
        )
    all_positive = bool(curve) and all(row["sign"] == "POSITIVE" for row in curve)
    return {
        "distinct_days": len(days),
        "curve": curve,
        "all_leave_one_day_out_positive": all_positive,
        "verdict": (
            "SURVIVES_EVERY_SINGLE_DAY_REMOVAL"
            if all_positive
            else "ONE_OR_MORE_DAYS_CARRY_THE_EDGE"
        ),
    }


def _resolved_taker_rows(
    ledger: dict[str, Any],
    *,
    resolution_by_condition: dict[str, str],
    resolution_by_slug: dict[str, str],
) -> list[dict[str, Any]]:
    earliest_by_window: dict[str, dict[str, Any]] = {}
    orders = [order for order in ledger.get("orders") or [] if isinstance(order, dict)]
    orders.sort(key=lambda order: str(order.get("submitted_at") or ""))
    for order in orders:
        result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
        role = str(order.get("execution_role") or result.get("execution_role") or "").lower()
        status = str(order.get("final_status") or order.get("status") or "").upper()
        slug = str(order.get("market_slug") or "")
        if role != "taker" or status != "FILLED" or not slug or slug in earliest_by_window:
            continue
        condition_id = str(order.get("condition_id") or "")
        winner = resolution_by_condition.get(condition_id) or resolution_by_slug.get(slug)
        outcome = str(order.get("outcome") or "").upper()
        price = _num(order.get("limit_price") or result.get("entry_price"))
        shares = _num(
            order.get("response_fill_size_shares")
            or order.get("filled_shares")
            or result.get("fill_size_shares")
            or order.get("requested_shares")
        )
        cost = _num(
            order.get("response_filled_size_usd")
            or order.get("filled_size_usd")
            or result.get("filled_size_usd")
        )
        if cost <= 0 and shares > 0 and price > 0:
            cost = shares * price
        if shares <= 0 and cost > 0 and price > 0:
            shares = cost / price
        if winner not in {"UP", "DOWN"} or outcome not in {"UP", "DOWN"} or not 0 < price < 1:
            continue
        if shares <= 0 or cost <= 0:
            continue
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        modeled_fee = modeled_unvalidated_polymarket_buy_fee_usd(shares=shares, price=price)
        won = outcome == winner
        realized_pnl = round((shares if won else 0.0) - cost, 6)
        earliest_by_window[slug] = {
            "submitted_at": order.get("submitted_at"),
            "market_slug": slug,
            "condition_id": condition_id,
            "source_wallet": order.get("source_wallet"),
            "policy_id": order.get("policy_id"),
            "outcome": outcome,
            "resolved_winner": winner,
            "entry_price": round(price, 6),
            "realised_entry_price": round(cost / shares, 6),
            "filled_shares": round(shares, 6),
            "filled_cost_usd": round(cost, 6),
            "expected_fee_usd": fee,
            "modeled_unvalidated": False,
            "modeled_unvalidated_fee_usd": modeled_fee,
            "won": won,
            "pnl_usd_realized": realized_pnl,
            "post_fee_pnl_usd": round(realized_pnl - fee, 6),
        }
    return sorted(earliest_by_window.values(), key=lambda row: str(row["submitted_at"] or ""))


def build_report(
    ledger: dict[str, Any],
    *,
    resolution_by_condition: dict[str, str],
    resolution_by_slug: dict[str, str],
    min_rows_per_bin: int = 40,
    min_distinct_days_per_bin: int = 3,
) -> dict[str, Any]:
    resolved = _resolved_taker_rows(
        ledger,
        resolution_by_condition=resolution_by_condition,
        resolution_by_slug=resolution_by_slug,
    )
    def band_reports(price_field: str) -> dict[str, dict[str, Any]]:
        subbands: dict[str, dict[str, Any]] = {}
        for name, (low, high) in PRICE_SUBBANDS.items():
            rows = [row for row in resolved if low <= float(row[price_field]) < high]
            split = day_bounded_split(
                rows,
                min_rows_per_bin=min_rows_per_bin,
                min_distinct_days_per_bin=min_distinct_days_per_bin,
            )
            development = split["development"]
            holdout = split["holdout"]
            holdout_stats = _stats(holdout, price_field=price_field)
            gate = bool(split["sample_gate_pass"])
            verdict = (
                "POSITIVE_DAY_BOUNDED_HOLDOUT"
                if gate and float(holdout_stats["post_fee_pnl_usd"]) > 0
                else "NEGATIVE_DAY_BOUNDED_HOLDOUT"
                if gate
                else "ACCRUING_DAY_BOUNDED_SAMPLE_GATE"
            )
            subbands[name] = {
                "price_basis": price_field,
                "price_min_inclusive": low,
                "price_max_exclusive": high,
                "verdict": verdict,
                "sample_gate": {
                    "status": "PASS" if gate else "ACCRUING",
                    "minimum_rows_per_bin": min_rows_per_bin,
                    "minimum_distinct_days_per_bin": min_distinct_days_per_bin,
                    "development_rows": len(development),
                    "holdout_rows": len(holdout),
                    "distinct_days_per_bin": split["distinct_days_per_bin"],
                    "split_integrity": split["split_integrity"],
                },
                "rows_per_day": split["rows_per_day"],
                "aggregate": _stats(rows, price_field=price_field),
                "development": _stats(development, price_field=price_field),
                "chronological_holdout": holdout_stats,
            }
        return subbands

    subbands = band_reports("entry_price")
    realised_price_subbands = band_reports("realised_entry_price")
    slippage_ratios = sorted(
        float(row["realised_entry_price"]) / float(row["entry_price"]) for row in resolved
    )
    limit_to_realised: dict[str, dict[str, int]] = {}
    limit_band_realised_outcome: dict[str, dict[str, dict[str, Any]]] = {}
    for limit_name, (limit_low, limit_high) in PRICE_SUBBANDS.items():
        source_rows = [
            row for row in resolved if limit_low <= float(row["entry_price"]) < limit_high
        ]
        limit_to_realised[limit_name] = {
            realised_name: sum(
                realised_low <= float(row["realised_entry_price"]) < realised_high
                for row in source_rows
            )
            for realised_name, (realised_low, realised_high) in PRICE_SUBBANDS.items()
        }
        limit_band_realised_outcome[limit_name] = {}
        for realised_name, (realised_low, realised_high) in PRICE_SUBBANDS.items():
            cell_rows = [
                row
                for row in source_rows
                if realised_low <= float(row["realised_entry_price"]) < realised_high
            ]
            split = day_bounded_split(
                cell_rows,
                min_rows_per_bin=min_rows_per_bin,
                min_distinct_days_per_bin=min_distinct_days_per_bin,
            )
            development = split["development"]
            holdout = split["holdout"]
            holdout_stats = _stats(holdout, price_field="realised_entry_price")
            gate = bool(split["sample_gate_pass"])
            verdict = (
                "POSITIVE_DAY_BOUNDED_HOLDOUT"
                if gate and float(holdout_stats["post_fee_pnl_usd"] or 0.0) > 0.0
                else "NEGATIVE_DAY_BOUNDED_HOLDOUT"
                if gate
                else "ACCRUING_DAY_BOUNDED_SAMPLE_GATE"
            )
            limit_band_realised_outcome[limit_name][realised_name] = {
                "limit_price_band": limit_name,
                "realised_price_band": realised_name,
                "verdict": verdict,
                "sample_gate": {
                    "status": "PASS" if gate else "ACCRUING",
                    "minimum_rows_per_bin": min_rows_per_bin,
                    "minimum_distinct_days_per_bin": min_distinct_days_per_bin,
                    "development_rows": len(development),
                    "holdout_rows": len(holdout),
                    "distinct_days_per_bin": split["distinct_days_per_bin"],
                    "split_integrity": split["split_integrity"],
                },
                "rows_per_day": split["rows_per_day"],
                "aggregate": _stats(cell_rows, price_field="realised_entry_price"),
                "development": _stats(development, price_field="realised_entry_price"),
                "chronological_holdout": holdout_stats,
            }
    focus = subbands["01a_25_32"]
    focus_holdout = focus["chronological_holdout"]
    focus_rows = [row for row in resolved if 0.25 <= float(row["entry_price"]) < 0.32]
    focus_split = day_bounded_split(
        focus_rows,
        min_rows_per_bin=min_rows_per_bin,
        min_distinct_days_per_bin=min_distinct_days_per_bin,
    )
    focus_concentration = _pnl_concentration(
        focus_split["holdout"]
    )
    development_drop_k = _drop_k_curve(focus_split["development"])
    holdout_drop_k = _drop_k_curve(focus_split["holdout"])
    holdout_jackknife = _leave_one_day_out(focus_split["holdout"])
    drop_2_both_halves_positive = bool(
        development_drop_k[2]["sign"] == "POSITIVE"
        and holdout_drop_k[2]["sign"] == "POSITIVE"
    )
    focus_robustness = {
        "development_drop_k_curve": development_drop_k,
        "holdout_drop_k_curve": holdout_drop_k,
        "holdout_leave_one_day_out": holdout_jackknife,
        "drop_2_both_halves_positive": drop_2_both_halves_positive,
        "verdict": (
            "ROBUST_TO_DAY_AND_TOP2_REMOVAL"
            if holdout_jackknife["all_leave_one_day_out_positive"]
            and drop_2_both_halves_positive
            else "DAY_JACKKNIFE_PASS_CONCENTRATION_FAIL"
            if holdout_jackknife["all_leave_one_day_out_positive"]
            else "DAY_JACKKNIFE_FAIL"
        ),
    }
    size_ruling_request_ready = False
    return {
        "schema_version": 1,
        "kind": "taker_price_subband_day_bounded_holdout",
        "flow_stage": "MEASURE",
        "generated_at": datetime.now(UTC).isoformat(),
        "measurement_only": True,
        "paper_only": True,
        "live_orders_allowed": False,
        "money_basis": "REALIZED_PAYOUT_MINUS_IMMUTABLE_BANKED_COST",
        "modeled_fee_diagnostic": {
            "modeled_unvalidated": True,
            "accounting_authority": False,
            "active_rate": POLYMARKET_EMBEDDED_FEE_RATE,
            "source": POLYMARKET_EMBEDDED_FEE_SOURCE,
        },
        "legacy_field_aliases": {
            "post_fee_pnl_usd": "pnl_usd_realized while active fee rate is zero",
            "post_fee_roi_pct": "roi_pct_realized while active fee rate is zero",
            "decision_path_allowed": False,
        },
        "counting_basis": {
            "resolved_unique_taker_windows": len(resolved),
            "deduplication": "earliest FILLED taker order per market_slug",
            "economics": "actual filled shares/cost, resolved payout, expected embedded fee",
            "fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
            "fee_formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
        },
        "subbands": subbands,
        "realised_price_subbands": realised_price_subbands,
        "limit_band_realised_outcome": limit_band_realised_outcome,
        "limit_to_realised_slippage": {
            "rows": len(slippage_ratios),
            "median_realised_to_limit_ratio": (
                round(statistics.median(slippage_ratios), 6) if slippage_ratios else None
            ),
            "p10_realised_to_limit_ratio": (
                round(slippage_ratios[max(0, math.ceil(0.1 * len(slippage_ratios)) - 1)], 6)
                if slippage_ratios
                else None
            ),
            "realised_at_least_0_01_below_limit": sum(
                float(row["realised_entry_price"]) <= float(row["entry_price"]) - 0.01
                for row in resolved
            ),
            "realised_at_least_0_01_above_limit": sum(
                float(row["realised_entry_price"]) >= float(row["entry_price"]) + 0.01
                for row in resolved
            ),
            "limit_band_to_realised_band_counts": limit_to_realised,
        },
        "focus_subband": "01a_25_32",
        "focus_verdict": focus["verdict"],
        "focus_holdout_pnl_concentration": focus_concentration,
        "focus_robustness": focus_robustness,
        "size_ruling_request_ready": size_ruling_request_ready,
        "decision_rule": "band-scoped size requests struck; supply must move before size",
        "next_action": "quantify no-eligible-signal and inventory-no-edge supply foreclosures",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--resolutions", default=str(DEFAULT_RESOLUTIONS))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        ledger = json.loads(Path(args.ledger).read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        ledger = {}
    by_condition, by_slug = _resolution_index(Path(args.resolutions))
    report = build_report(
        ledger if isinstance(ledger, dict) else {},
        resolution_by_condition=by_condition,
        resolution_by_slug=by_slug,
    )
    atomic_write_json(Path(args.output), report)
    print(json.dumps({"focus_verdict": report["focus_verdict"], "subbands": report["subbands"]}, sort_keys=True))


if __name__ == "__main__":
    main()
