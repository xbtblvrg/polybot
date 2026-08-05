#!/usr/bin/env python3
"""Measure refused passive-at-source intents on a chronological resolved holdout.

Flow stage: MEASURE. This report is counterfactual and cannot mutate live policy.
"""

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

from src.wallet_copy.fees import (
    POLYMARKET_EMBEDDED_FEE_FORMULA,
    POLYMARKET_EMBEDDED_FEE_RATE,
    POLYMARKET_EMBEDDED_FEE_SOURCE,
    expected_polymarket_buy_fee_usd,
    modeled_unvalidated_polymarket_buy_fee_usd,
)
from src.wallet_copy.store import atomic_write_json


DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = ROOT / "data/research/passive_at_source_holdout_latest.json"
MIN_ROWS_PER_CHRONOLOGICAL_BIN = 40
MIN_DISTINCT_DAYS_PER_BIN = 3
COUNTERFACTUAL_SHARES = 5.0


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


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
            condition_id = str(row.get("condition_id") or "")
            market_slug = str(row.get("market_slug") or "")
            if condition_id:
                by_condition[condition_id] = winner
            if market_slug:
                by_slug[market_slug] = winner
    return by_condition, by_slug


def _passive_refusal(order: dict[str, Any]) -> bool:
    decision = order.get("trade_decision") if isinstance(order.get("trade_decision"), dict) else {}
    result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    return bool(
        str(decision.get("strategy_reason") or "") == "wallet_copy_passive_at_source"
        and str(result.get("error_class") or order.get("error_class") or "")
        == "maker_min_share_bump_exceeds_policy_cap"
        and not str(result.get("order_id") or "").strip()
    )


def _stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    cost = sum(float(row["counterfactual_cost_usd"]) for row in rows)
    realized_pnl = sum(float(row.get("pnl_usd_realized") or 0.0) for row in rows)
    pnl = sum(float(row.get("post_fee_pnl_usd") or 0.0) for row in rows)
    return {
        "rows": len(rows),
        "first_submitted_at": rows[0]["submitted_at"] if rows else None,
        "last_submitted_at": rows[-1]["submitted_at"] if rows else None,
        "wins": sum(bool(row["won"]) for row in rows),
        "losses": sum(not bool(row["won"]) for row in rows),
        "cost_usd": round(cost, 6),
        "expected_fee_usd": round(sum(float(row["expected_fee_usd"]) for row in rows), 6),
        "modeled_unvalidated_fee_usd": round(
            sum(float(row.get("modeled_unvalidated_fee_usd") or 0.0) for row in rows), 6
        ),
        "pnl_usd_realized": round(realized_pnl, 6),
        "roi_pct_realized": round(100.0 * realized_pnl / cost, 6) if cost else None,
        "post_fee_pnl_usd": round(pnl, 6),
        "post_fee_roi_pct": round(100.0 * pnl / cost, 6) if cost else None,
    }


def day_bounded_split(
    rows: list[dict[str, Any]],
    *,
    min_rows_per_bin: int = MIN_ROWS_PER_CHRONOLOGICAL_BIN,
    min_distinct_days_per_bin: int = MIN_DISTINCT_DAYS_PER_BIN,
) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: str(row.get("submitted_at") or ""))
    rows_per_day: dict[str, int] = {}
    for row in ordered:
        day = str(row.get("submitted_at") or "")[:10]
        rows_per_day[day] = rows_per_day.get(day, 0) + 1
    boundaries: list[int] = []
    cumulative = 0
    days = list(rows_per_day)
    for day in days[:-1]:
        cumulative += rows_per_day[day]
        boundaries.append(cumulative)
    midpoint = len(ordered) / 2.0
    split = min(boundaries, key=lambda value: (abs(value - midpoint), -value)) if boundaries else len(ordered)
    development = ordered[:split]
    holdout = ordered[split:]
    development_days = sorted({str(row.get("submitted_at") or "")[:10] for row in development})
    holdout_days = sorted({str(row.get("submitted_at") or "")[:10] for row in holdout})
    sample_gate = bool(
        len(development) >= min_rows_per_bin
        and len(holdout) >= min_rows_per_bin
        and len(development_days) >= min_distinct_days_per_bin
        and len(holdout_days) >= min_distinct_days_per_bin
    )
    return {
        "development": development,
        "holdout": holdout,
        "rows_per_day": rows_per_day,
        "development_days": development_days,
        "holdout_days": holdout_days,
        "distinct_days_per_bin": {
            "development": len(development_days),
            "holdout": len(holdout_days),
        },
        "split_integrity": "DAY_BOUNDED",
        "split_index": split,
        "sample_gate_pass": sample_gate,
    }


def build_report(
    ledger: dict[str, Any],
    *,
    resolution_by_condition: dict[str, str],
    resolution_by_slug: dict[str, str],
    min_rows_per_bin: int = MIN_ROWS_PER_CHRONOLOGICAL_BIN,
    min_distinct_days_per_bin: int = MIN_DISTINCT_DAYS_PER_BIN,
) -> dict[str, Any]:
    refused = [order for order in ledger.get("orders") or [] if isinstance(order, dict) and _passive_refusal(order)]
    refused.sort(key=lambda order: str(order.get("submitted_at") or ""))
    earliest_by_window: dict[str, dict[str, Any]] = {}
    seen_complete_windows: set[str] = set()
    incomplete = 0
    unresolved = 0
    for order in refused:
        slug = str(order.get("market_slug") or "")
        condition_id = str(order.get("condition_id") or "")
        outcome = str(order.get("outcome") or "").upper()
        try:
            price = float(order.get("limit_price") or 0.0)
        except (TypeError, ValueError):
            price = 0.0
        if not slug or not condition_id or outcome not in {"UP", "DOWN"} or not 0.0 < price < 1.0:
            incomplete += 1
            continue
        if slug in seen_complete_windows:
            continue
        seen_complete_windows.add(slug)
        winner = resolution_by_condition.get(condition_id) or resolution_by_slug.get(slug)
        if winner not in {"UP", "DOWN"}:
            unresolved += 1
            continue
        cost = COUNTERFACTUAL_SHARES * price
        fee = expected_polymarket_buy_fee_usd(shares=COUNTERFACTUAL_SHARES, price=price)
        modeled_fee = modeled_unvalidated_polymarket_buy_fee_usd(
            shares=COUNTERFACTUAL_SHARES, price=price
        )
        won = outcome == winner
        realized_pnl = round((COUNTERFACTUAL_SHARES if won else 0.0) - cost, 6)
        earliest_by_window[slug] = {
            "submitted_at": order.get("submitted_at"),
            "market_slug": slug,
            "condition_id": condition_id,
            "source_wallet": order.get("source_wallet"),
            "outcome": outcome,
            "resolved_winner": winner,
            "limit_price": price,
            "counterfactual_shares": COUNTERFACTUAL_SHARES,
            "counterfactual_cost_usd": round(cost, 6),
            "expected_fee_usd": fee,
            "modeled_unvalidated_fee_usd": modeled_fee,
            "won": won,
            "pnl_usd_realized": realized_pnl,
            "post_fee_pnl_usd": round(realized_pnl - fee, 6),
        }

    resolved = sorted(earliest_by_window.values(), key=lambda row: str(row["submitted_at"] or ""))
    split = day_bounded_split(
        resolved,
        min_rows_per_bin=min_rows_per_bin,
        min_distinct_days_per_bin=min_distinct_days_per_bin,
    )
    development = split["development"]
    holdout = split["holdout"]
    development_stats = _stats(development)
    holdout_stats = _stats(holdout)
    sample_gate = bool(split["sample_gate_pass"])
    if not sample_gate:
        verdict = "ACCRUING_DAY_BOUNDED_SAMPLE_GATE"
    elif float(holdout_stats["post_fee_pnl_usd"]) > 0.0:
        verdict = "POSITIVE_CHRONOLOGICAL_HOLDOUT"
    else:
        verdict = "NEGATIVE_CHRONOLOGICAL_HOLDOUT"

    price_bands: dict[str, dict[str, Any]] = {}
    for name, low, high in (("01b_25_40", 0.25, 0.40), ("01c_40_50", 0.40, 0.5000001)):
        price_bands[name] = _stats(
            [row for row in resolved if low <= float(row["limit_price"]) < high]
        )
    return {
        "schema_version": 1,
        "kind": "passive_at_source_chronological_holdout",
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
        "lane_lifecycle": {
            "status": "CLOSED_TERMINAL_NEGATIVE_FROZEN_COHORT",
            "terminal_reason": "passive_at_source_lane_closed",
            "closure_basis": "live CopyIntent policy now refuses passive-at-source before CLOB submit",
            "new_rows_expected": False,
            "frozen_cohort_reason": (
                "matcher intentionally includes only historical wallet_copy_passive_at_source + "
                "maker_min_share_bump_exceeds_policy_cap rows; the closed emitter cannot add rows"
            ),
        },
        "verdict": "CLOSED_TERMINAL_NEGATIVE_FROZEN_COHORT",
        "lineage_measurement_verdict": verdict,
        "sample_gate": {
            "status": "PASS" if sample_gate else "ACCRUING",
            "minimum_rows_per_chronological_bin": min_rows_per_bin,
            "minimum_distinct_days_per_chronological_bin": min_distinct_days_per_bin,
            "development_rows": len(development),
            "holdout_rows": len(holdout),
            "distinct_days_per_bin": split["distinct_days_per_bin"],
            "split_integrity": split["split_integrity"],
        },
        "rows_per_day": split["rows_per_day"],
        "counting_basis": {
            "candidate_attempts": len(refused),
            "deduplication": "earliest fully specified refused intent per market_slug",
            "resolved_unique_windows": len(resolved),
            "unresolved_unique_windows_encountered": unresolved,
            "incomplete_attempts": incomplete,
            "execution_assumption": (
                "counterfactual assumes a 5-share passive order fully fills at the source limit; "
                "resolution PnL is not evidence of historical queue fillability"
            ),
            "fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
            "fee_formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
        },
        "aggregate": _stats(resolved),
        "development": development_stats,
        "chronological_holdout": holdout_stats,
        "price_bands": price_bands,
        "decision_rule": (
            "day-bounded chronological bins with >=40 resolved unique windows and >=3 distinct "
            "calendar days each; only positive realized holdout may request a separate Fable ruling"
        ),
        "next_action": "none; emitter closed at a034c8c8, cohort cannot grow",
        "rows": resolved,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=str(DEFAULT_LEDGER))
    parser.add_argument("--resolutions", default=str(DEFAULT_RESOLUTIONS))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--min-rows-per-bin", type=int, default=MIN_ROWS_PER_CHRONOLOGICAL_BIN)
    parser.add_argument("--min-distinct-days-per-bin", type=int, default=MIN_DISTINCT_DAYS_PER_BIN)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    by_condition, by_slug = _resolution_index(Path(args.resolutions))
    report = build_report(
        _load_json(Path(args.ledger)),
        resolution_by_condition=by_condition,
        resolution_by_slug=by_slug,
        min_rows_per_bin=max(1, int(args.min_rows_per_bin)),
        min_distinct_days_per_bin=max(1, int(args.min_distinct_days_per_bin)),
    )
    atomic_write_json(Path(args.output), report)
    print(json.dumps({key: report[key] for key in ("verdict", "sample_gate", "aggregate", "chronological_holdout")}, sort_keys=True))


if __name__ == "__main__":
    main()
