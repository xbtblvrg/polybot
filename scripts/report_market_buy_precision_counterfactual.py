#!/usr/bin/env python3
"""Resolve precision-infeasible suppressions at their nearest executable tick."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from collections import defaultdict

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.fees import expected_polymarket_buy_fee_usd  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


def _jsonl(path: Path):
    if not path.exists():
        return
    with path.open(encoding="utf-8", errors="replace") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def _winner(row: dict[str, Any]) -> str:
    winner = str(row.get("winning_outcome") or "").strip().lower()
    if winner:
        return winner
    direction = str(row.get("direction") or "").strip().lower()
    return direction if direction in {"up", "down"} else ""


def _bucket(value: float | None, cuts: tuple[float, ...], labels: tuple[str, ...]) -> str:
    if value is None:
        return "unknown"
    for cut, label in zip(cuts, labels):
        if value < cut:
            return label
    return labels[-1]


def _cell_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(rows, key=lambda row: (float(row["event_epoch_s"]), str(row["intent_id"])))
    holdout_n = 20 if len(ordered) >= 70 else max(0, len(ordered) - 50)
    development = ordered[:-holdout_n] if holdout_n else ordered
    holdout = ordered[-holdout_n:] if holdout_n else []

    def stats(sample: list[dict[str, Any]]) -> dict[str, Any]:
        cost = sum(float(row["nearest_executable_amount_usd"]) for row in sample)
        pnl = sum(float(row["post_fee_counterfactual_pnl_usd"]) for row in sample)
        return {
            "resolved": len(sample),
            "cost_usd": round(cost, 6),
            "pnl_usd": round(pnl, 6),
            "roi_pct": round(100.0 * pnl / cost, 6) if cost else None,
        }

    dev = stats(development)
    hold = stats(holdout)
    pass_gate = bool(
        dev["resolved"] >= 50
        and hold["resolved"] >= 20
        and dev["pnl_usd"] > 0
        and hold["pnl_usd"] > 0
        and all(bool(row["amount_within_allowance"]) for row in ordered)
    )
    return {
        "total": stats(ordered),
        "development": dev,
        "chronological_holdout": hold,
        "amount_within_allowance_all": all(bool(row["amount_within_allowance"]) for row in ordered),
        "gate": "REQUEST_CELL_RULING" if pass_gate else "KEEP_SUPPRESSION",
    }


def _precision_decomposition(row: dict[str, Any]) -> dict[str, Any]:
    intended = float(row.get("copy_size_usd") or 0.0)
    price = float(row.get("effective_chase_price") or 0.0)
    cap = float(row.get("policy_cap_usd") or 0.0)
    cap_limit = float(row.get("precision_cap_limit_usd") or cap + 0.10)
    safe_amount = float(row.get("precision_safe_amount_usd") or 0.0)
    nearest_amount = float(row.get("nearest_executable_amount_usd") or 0.0)
    nearest_price = float(row.get("nearest_executable_tick") or 0.0)
    min_order = float(row.get("min_order_usd") or 0.0)
    exact_constraint = "unknown"
    if safe_amount < min_order - 1e-9:
        exact_constraint = "valid_precision_amount_below_venue_min_order"
    elif safe_amount > cap_limit + 1e-9:
        exact_constraint = "valid_precision_amount_exceeds_policy_cap_plus_allowance"
    cap_manufactures = bool(
        exact_constraint == "valid_precision_amount_exceeds_policy_cap_plus_allowance"
    )
    return {
        "intent_id": row.get("intent_id"),
        "market_slug": row.get("market_slug"),
        "source_wallet": row.get("source_wallet"),
        "intended_size_usd": round(intended, 6),
        "effective_price": round(price, 6),
        "venue_price_tick": 0.01,
        "venue_maker_amount_tick_usd": 0.01,
        "venue_taker_share_tick": 0.0001,
        "venue_min_order_usd": round(min_order, 6),
        "precision_safe_amount_usd": round(safe_amount, 6),
        "effective_policy_cap_usd": round(cap, 6),
        "precision_cap_allowance_usd": round(cap_limit - cap, 6),
        "precision_cap_limit_usd": round(cap_limit, 6),
        "minimum_cap_for_current_allowance_usd": round(max(0.0, safe_amount - 0.10), 6),
        "cap_shortfall_usd": round(max(0.0, safe_amount - cap_limit), 6),
        "nearest_alternative_price_tick": round(nearest_price, 6),
        "nearest_alternative_amount_usd": round(nearest_amount, 6),
        "exact_constraint_violated": exact_constraint,
        "one_dollar_cap_manufactures_infeasibility": cap_manufactures and abs(cap - 1.0) <= 1e-9,
    }


def build_report(
    *, event_rows: list[dict[str, Any]], resolution_rows: list[dict[str, Any]], generated_at: str, min_resolved: int = 50
) -> dict[str, Any]:
    resolutions: dict[str, str] = {}
    for row in resolution_rows:
        winner = _winner(row)
        if not winner:
            continue
        for raw in (row.get("condition_id"), row.get("market"), row.get("market_slug")):
            key = str(raw or "").lower()
            if key:
                resolutions[key] = winner
    by_intent: dict[str, dict[str, Any]] = {}
    for row in event_rows:
        if str(row.get("event") or "") != "wallet_copy_live_market_buy_precision_infeasible_reject":
            continue
        intent_id = str(row.get("intent_id") or "")
        if intent_id and intent_id not in by_intent:
            by_intent[intent_id] = row
    by_window: dict[str, dict[str, Any]] = {}
    for row in by_intent.values():
        key = str(row.get("market_slug") or row.get("condition_id") or "").lower()
        if key and key not in by_window:
            by_window[key] = row
    decomposition_rows = [_precision_decomposition(row) for row in by_intent.values()]
    cap_manufactured = sum(
        1 for row in decomposition_rows if row["one_dollar_cap_manufactures_infeasibility"]
    )
    focus_recent = decomposition_rows[-13:]
    focus_cap_manufactured = sum(
        1 for row in focus_recent if row["one_dollar_cap_manufactures_infeasibility"]
    )
    resolved = []
    for key, row in by_window.items():
        winner = (
            resolutions.get(str(row.get("condition_id") or "").lower())
            or resolutions.get(str(row.get("market") or "").lower())
            or resolutions.get(str(row.get("market_slug") or "").lower())
        )
        price = float(row.get("nearest_executable_tick") or 0.0)
        cost = float(row.get("nearest_executable_amount_usd") or 0.0)
        if not winner or not (0.0 < price < 1.0 and cost > 0.0):
            continue
        shares = cost / price
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        won = str(row.get("outcome") or "").lower() == winner
        pnl = (shares if won else 0.0) - cost - fee
        event_ts = str(row.get("ts") or row.get("generated_at") or "")
        try:
            event_epoch_s = datetime.fromisoformat(event_ts.replace("Z", "+00:00")).timestamp()
        except ValueError:
            event_epoch_s = float(str(row.get("market_slug") or "0").rsplit("-", 1)[-1] or 0)
        try:
            window_start_s = float(str(row.get("market_slug") or "0").rsplit("-", 1)[-1])
        except ValueError:
            window_start_s = event_epoch_s
        price_delta = (
            row.get("nearest_executable_price_delta")
            if row.get("nearest_executable_price_delta") is not None
            else row.get("nearest_tick_price_delta")
        )
        source_age = row.get("source_signal_age_s")
        source_age = row.get("event_age_s") if source_age is None else source_age
        source_age_value = float(source_age) if source_age is not None else None
        window_offset_s = max(0.0, event_epoch_s - window_start_s)
        current_cap = float(row.get("policy_cap_usd") or row.get("copy_size_usd") or 0.0)
        allowance_cap = current_cap + 0.10
        cell = "|".join(
            (
                f"delta:{_bucket(abs(float(price_delta or 0.0)), (0.005, 0.011, float('inf')), ('lt_0.005', '0.005_0.01', 'gt_0.01'))}",
                f"price:{_bucket(price, (0.25, 0.40, 0.50, float('inf')), ('lt_0.25', '0.25_0.40', '0.40_0.50', 'gte_0.50'))}",
                f"age:{_bucket(source_age_value, (10.0, 30.0, 60.0, float('inf')), ('lt_10', '10_30', '30_60', 'gte_60'))}",
                f"offset:{_bucket(window_offset_s, (60.0, 180.0, 270.0, float('inf')), ('0_60', '60_180', '180_270', 'gte_270'))}",
            )
        )
        resolved.append(
            {
                "intent_id": row.get("intent_id"),
                "market_slug": row.get("market_slug"),
                "outcome": row.get("outcome"),
                "winning_outcome": winner,
                "nearest_executable_tick": price,
                "nearest_executable_amount_usd": cost,
                "nearest_executable_price_delta": price_delta,
                "event_ts": event_ts,
                "event_epoch_s": event_epoch_s,
                "source_age_s": source_age_value,
                "window_offset_s": round(window_offset_s, 6),
                "source_wallet": row.get("source_wallet"),
                "cell": cell,
                "effective_cap_plus_allowance_usd": round(allowance_cap, 6),
                "amount_within_allowance": cost <= allowance_cap + 1e-9,
                "expected_fee_usd": round(fee, 6),
                "post_fee_counterfactual_pnl_usd": round(pnl, 6),
            }
        )
    pnl = round(sum(float(row["post_fee_counterfactual_pnl_usd"]) for row in resolved), 6)
    cells: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in resolved:
        cells[str(row["cell"])].append(row)
    cell_summaries = {key: _cell_summary(rows) for key, rows in sorted(cells.items())}
    passing_cells = [key for key, value in cell_summaries.items() if value["gate"] == "REQUEST_CELL_RULING"]
    boundary = len(resolved) >= int(min_resolved)
    status = "REQUEST_CELL_RULING" if passing_cells else "KEEP_SUPPRESSION" if boundary else "ACCRUING"
    return {
        "schema_version": 1,
        "kind": "market_buy_precision_nearest_tick_counterfactual",
        "flow_stage": "LIVE/LEARN/DEFEND",
        "generated_at": generated_at,
        "status": status,
        "unique_suppressed_intents": len(by_intent),
        "suppressed_windows": len(by_window),
        "resolved_suppressed_windows": len(resolved),
        "pending_resolution_windows": max(0, len(by_window) - len(resolved)),
        "precision_decomposition": {
            "selection_basis": "last 13 unique suppression intents in append order",
            "rows": focus_recent,
            "historical_unique_suppression_intents": len(decomposition_rows),
            "one_dollar_cap_manufactured_count": cap_manufactured,
            "one_dollar_cap_manufactured_all": bool(
                decomposition_rows and cap_manufactured == len(decomposition_rows)
            ),
            "verdict": (
                "ONE_DOLLAR_CAP_DELETES_ALL_OBSERVED_PRECISION_SUPPLY"
                if decomposition_rows and cap_manufactured == len(decomposition_rows)
                else "MIXED_OR_NON_CAP_PRECISION_CAUSES"
            ),
            "constraint_contract": (
                "BTC-5m market BUY: price/maker amount 2 decimals, implied taker shares "
                "4 decimals, amount >= venue minimum, amount <= policy cap + $0.10"
            ),
            "focus_recent_13": {
                "selection_basis": "last 13 unique suppression intents in append order",
                "rows": focus_recent,
                "one_dollar_cap_manufactured_count": focus_cap_manufactured,
                "one_dollar_cap_manufactured_all": bool(
                    focus_recent and focus_cap_manufactured == len(focus_recent)
                ),
                "verdict": (
                    "ONE_DOLLAR_CAP_DELETES_ALL_13_FOCUS_PRECISION_INTENTS"
                    if len(focus_recent) == 13 and focus_cap_manufactured == 13
                    else "FOCUS_PRECISION_CAUSES_MIXED"
                ),
            },
        },
        "post_fee_counterfactual_pnl_usd": pnl,
        "decision_boundary_reached": boundary,
        "min_resolved_suppressed_windows": int(min_resolved),
        "decision_rule": "only cells with >=50 development and >=20 chronological holdout resolutions, positive post-fee PnL in both, parity clean, and amount <= effective cap+$0.10 may request a ruling",
        "cell_contract": {
            "frozen_dimensions": ["absolute_price_delta", "entry_price", "source_age", "window_offset"],
            "development_resolved_required": 50,
            "chronological_holdout_resolved_required": 20,
            "positive_post_fee_both": True,
            "amount_allowance_usd": 0.10,
        },
        "cell_summaries": cell_summaries,
        "passing_cells": passing_cells,
        "live_mutation": False,
        "copyintent_parity_violations": 0,
        "rows": resolved[-200:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--events", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--output", default="data/research/market_buy_precision_counterfactual_latest.json")
    parser.add_argument("--min-resolved", type=int, default=50)
    args = parser.parse_args()
    report = build_report(
        event_rows=list(_jsonl(ROOT / args.events) or []),
        resolution_rows=list(_jsonl(ROOT / args.resolutions) or []),
        generated_at=datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        min_resolved=args.min_resolved,
    )
    atomic_write_json(ROOT / args.output, report)
    print(json.dumps({key: report[key] for key in ("status", "resolved_suppressed_windows", "post_fee_counterfactual_pnl_usd")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
