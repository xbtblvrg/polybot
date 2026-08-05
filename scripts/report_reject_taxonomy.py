#!/usr/bin/env python3
"""Classify live no-fill/reject windows into Fable's bounded taxonomy."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso

DEFAULT_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_HISTORY_STATE = ROOT / "data/research/wallet_copy_history_state.json"
DEFAULT_LEDGER = ROOT / "data/research/wallet_copy_live_execution_state.json"
DEFAULT_OUTPUT = ROOT / "data/research/wallet_copy_reject_taxonomy_latest.json"
DEFAULT_RESOLUTIONS = ROOT / "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"


TAXONOMY_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("freshness", ("window_time", "late_window", "stale", "age", "close")),
    ("band", ("price_band", "max_price", "min_price")),
    ("envelope", ("best_ask", "vwap", "min_order", "target_already_met", "sized_copy", "inventory")),
    ("parity", ("parity", "token", "condition", "copyintent")),
)


def classify_reason(reason: str) -> str:
    text = str(reason or "").lower()
    for category, needles in TAXONOMY_RULES:
        if any(needle in text for needle in needles):
            return category
    return "unknown"


def _counter_to_sorted_rows(counter: Counter[str]) -> list[dict[str, Any]]:
    total = sum(counter.values())
    return [
        {"key": key, "count": count, "pct": round(100.0 * count / total, 6) if total else 0.0}
        for key, count in counter.most_common()
    ]


def _float(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result else None


def _norm_wallet(value: Any) -> str:
    return str(value or "").strip().lower()


def _iso_to_ts(value: Any) -> float | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return None


def _side_from_outcome(value: Any) -> str:
    text = str(value or "").strip().upper()
    if text in {"YES", "UP"}:
        return "YES"
    if text in {"NO", "DOWN"}:
        return "NO"
    return ""


def _winner_side(resolution: dict[str, Any] | None) -> str:
    if not isinstance(resolution, dict):
        return ""
    return _side_from_outcome(resolution.get("direction") or resolution.get("winner") or resolution.get("outcome"))


def load_resolutions(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    rows: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(row, dict):
                continue
            slug = str(row.get("market_slug") or "").strip()
            if slug:
                rows[slug] = row
    return rows


def _ledger_reject_lookup(orders: list[Any]) -> dict[tuple[str, str, str], dict[str, Any]]:
    lookup: dict[tuple[str, str, str], dict[str, Any]] = {}
    for order in orders:
        if not isinstance(order, dict):
            continue
        if str(order.get("final_status") or order.get("status") or "").upper() != "REJECTED":
            continue
        side = _side_from_outcome(order.get("side"))
        key = (str(order.get("market_slug") or ""), _norm_wallet(order.get("source_wallet")), side)
        if not key[0] or not key[1] or not key[2]:
            continue
        lookup[key] = order
    return lookup


def _history_price_lookup(history_state: dict[str, Any] | None) -> dict[tuple[str, str, str], list[dict[str, Any]]]:
    if not isinstance(history_state, dict):
        return {}
    lookup: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for event in history_state.get("events") or []:
        if not isinstance(event, dict):
            continue
        price = _float(event.get("price") or event.get("limit_price"))
        side = _side_from_outcome(event.get("side") or event.get("outcome"))
        key = (str(event.get("market_slug") or ""), _norm_wallet(event.get("source_wallet")), side)
        if price is None or price <= 0.0 or not key[0] or not key[1] or not key[2]:
            continue
        lookup.setdefault(key, []).append(
            {
                "price": price,
                "event_ts": _float(event.get("event_ts")),
                "observed_ts": _float(event.get("observed_ts")),
                "event_id": event.get("event_id"),
            }
        )
    return lookup


def _history_source_price(
    lookup: dict[tuple[str, str, str], list[dict[str, Any]]],
    *,
    market_slug: str,
    source_wallet: str,
    intended_side: str,
    first_seen_at: Any,
    last_seen_at: Any,
) -> dict[str, Any] | None:
    rows = lookup.get((market_slug, source_wallet, intended_side)) or []
    if not rows:
        return None
    start_ts = _iso_to_ts(first_seen_at)
    end_ts = _iso_to_ts(last_seen_at)
    bounded = []
    if start_ts is not None and end_ts is not None:
        bounded = [
            row
            for row in rows
            if (row.get("observed_ts") is not None and start_ts - 5.0 <= float(row["observed_ts"]) <= end_ts + 5.0)
            or (row.get("event_ts") is not None and start_ts - 5.0 <= float(row["event_ts"]) <= end_ts + 5.0)
        ]
    candidates = bounded or rows
    return sorted(candidates, key=lambda row: float(row.get("observed_ts") or row.get("event_ts") or 0.0))[-1]


def _best_ask_missing_counterfactual(
    rollup: dict[str, Any],
    *,
    resolutions: dict[str, dict[str, Any]],
    ledger_rejects: dict[tuple[str, str, str], dict[str, Any]],
    history_prices: dict[tuple[str, str, str], list[dict[str, Any]]],
) -> dict[str, Any] | None:
    skip_counts = rollup.get("dominant_skip_reason_counts")
    if not isinstance(skip_counts, dict) or int(skip_counts.get("inventory_best_ask_missing") or 0) <= 0:
        return None
    outcomes = [str(outcome) for outcome in rollup.get("outcomes") or [] if str(outcome or "").strip()]
    intended_outcome = outcomes[0] if len(set(outcomes)) == 1 else ""
    intended_side = _side_from_outcome(intended_outcome)
    market_slug = str(rollup.get("market_slug") or "")
    source_wallet = _norm_wallet(rollup.get("source_wallet"))
    resolution = resolutions.get(market_slug)
    winner = _winner_side(resolution)
    would_have_won = None if not winner or not intended_side else intended_side == winner

    limit_price = None
    limit_price_source = "unavailable"
    ledger_reject = ledger_rejects.get((market_slug, source_wallet, intended_side))
    if isinstance(ledger_reject, dict):
        limit_price = _float(ledger_reject.get("limit_price"))
        if limit_price is not None:
            limit_price_source = "matching_ledger_reject"
    if limit_price is None:
        source_price = _history_source_price(
            history_prices,
            market_slug=market_slug,
            source_wallet=source_wallet,
            intended_side=intended_side,
            first_seen_at=rollup.get("first_seen_at"),
            last_seen_at=rollup.get("last_seen_at"),
        )
        if source_price is not None:
            limit_price = _float(source_price.get("price"))
            limit_price_source = "history_source_event_price"
    unit_pnl = None
    if limit_price is not None and limit_price > 0.0 and would_have_won is not None:
        unit_pnl = (1.0 / limit_price - 1.0) if would_have_won else -1.0
    return {
        "market_slug": market_slug,
        "source_wallet": source_wallet,
        "skip_count": int(skip_counts.get("inventory_best_ask_missing") or 0),
        "intended_outcome": intended_outcome,
        "intended_side": intended_side,
        "actual_winner_side": winner,
        "would_have_won": would_have_won,
        "limit_price": None if limit_price is None else round(limit_price, 6),
        "limit_price_source": limit_price_source,
        "counterfactual_pnl_usd_per_1usd": None if unit_pnl is None else round(unit_pnl, 6),
        "resolution_status": "resolved" if winner else "unresolved_or_missing",
    }


def _counterfactual_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("would_have_won") is not None]
    priced = [row for row in resolved if row.get("counterfactual_pnl_usd_per_1usd") is not None]
    pnl = round(sum(float(row.get("counterfactual_pnl_usd_per_1usd") or 0.0) for row in priced), 6)
    return {
        "bar": "reopen only if n>=10 resolved best_ask_missing counterfactuals are net-positive at intended/proxy limit prices",
        "sample_n": len(rows),
        "resolved_n": len(resolved),
        "unresolved_or_missing_n": len(rows) - len(resolved),
        "would_have_won_n": sum(1 for row in resolved if row.get("would_have_won") is True),
        "would_have_lost_n": sum(1 for row in resolved if row.get("would_have_won") is False),
        "priced_resolved_n": len(priced),
        "counterfactual_pnl_usd_per_1usd": pnl,
        "reopen_bar_pass": len(resolved) >= 10 and pnl > 0.0,
    }


def build_report(
    guard_state: dict[str, Any],
    ledger: dict[str, Any],
    *,
    resolutions: dict[str, dict[str, Any]] | None = None,
    history_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    participation = guard_state.get("window_participation") if isinstance(guard_state, dict) else {}
    rollups = participation.get("window_rollups") if isinstance(participation, dict) else []
    rows = []
    reason_counts: Counter[str] = Counter()
    taxonomy_counts: Counter[str] = Counter()
    miss_rows = []
    all_reject_or_skip_windows = 0
    orders = ledger.get("orders") if isinstance(ledger, dict) else []
    ledger_rejects_by_key = _ledger_reject_lookup(orders if isinstance(orders, list) else [])
    history_prices = _history_price_lookup(history_state)
    resolution_map = resolutions if isinstance(resolutions, dict) else {}
    best_ask_missing_counterfactuals = []
    for rollup in rollups if isinstance(rollups, list) else []:
        if not isinstance(rollup, dict):
            continue
        wallet_orders = int(rollup.get("wallet_eligible_orders") or 0)
        our_fills = int(rollup.get("our_fills") or 0)
        if wallet_orders <= 0 or our_fills > 0:
            continue
        all_reject_or_skip_windows += 1
        skip_counts = rollup.get("dominant_skip_reason_counts")
        if not isinstance(skip_counts, dict) or not skip_counts:
            reason = str(rollup.get("dominant_skip_reason") or "unknown")
            skip_counts = {reason: 1}
        row_reasons: Counter[str] = Counter()
        row_taxonomy: Counter[str] = Counter()
        for reason, count_value in skip_counts.items():
            count = int(count_value or 0)
            if count <= 0:
                continue
            category = classify_reason(str(reason))
            row_reasons[str(reason)] += count
            row_taxonomy[category] += count
            reason_counts[str(reason)] += count
            taxonomy_counts[category] += count
        row = {
            "market_slug": rollup.get("market_slug"),
            "source_wallet": str(rollup.get("source_wallet") or "").lower(),
            "wallet_eligible_orders": wallet_orders,
            "our_attempts": int(rollup.get("our_attempts") or 0),
            "our_submits": int(rollup.get("our_submits") or 0),
            "our_fills": our_fills,
            "missed_active_window": bool(rollup.get("missed_active_window")),
            "skip_reasons": dict(row_reasons),
            "taxonomy": dict(row_taxonomy),
        }
        counterfactual = _best_ask_missing_counterfactual(
            rollup,
            resolutions=resolution_map,
            ledger_rejects=ledger_rejects_by_key,
            history_prices=history_prices,
        )
        if counterfactual is not None:
            row["best_ask_missing_counterfactual"] = counterfactual
            best_ask_missing_counterfactuals.append(counterfactual)
        rows.append(row)
        if row["missed_active_window"]:
            miss_rows.append(row)

    ledger_rejects = [
        {
            "submitted_at": order.get("submitted_at"),
            "market_slug": order.get("market_slug"),
            "source_wallet": str(order.get("source_wallet") or "").lower(),
            "side": order.get("side"),
            "limit_price": order.get("limit_price"),
            "order_id": order.get("order_id"),
        }
        for order in orders if isinstance(order, dict)
        and str(order.get("final_status") or order.get("status") or "").upper() == "REJECTED"
    ]
    total_reasons = sum(taxonomy_counts.values())
    freshness_pct = round(100.0 * taxonomy_counts.get("freshness", 0) / total_reasons, 6) if total_reasons else 0.0
    return {
        "generated_at": utc_now_iso(),
        "flow_stage": "LIVE/LEARN",
        "scope": "guard_window_participation_all_no_fill_windows",
        "summary": {
            "all_reject_or_skip_windows": all_reject_or_skip_windows,
            "missed_active_windows": len(miss_rows),
            "ledger_rejects_total": len(ledger_rejects),
            "freshness_taxonomy_pct": freshness_pct,
            "freshness_rejects_expected_thin_flow": freshness_pct >= 90.0,
            "best_ask_missing_counterfactuals": _counterfactual_summary(best_ask_missing_counterfactuals),
        },
        "taxonomy_counts": _counter_to_sorted_rows(taxonomy_counts),
        "reason_counts": _counter_to_sorted_rows(reason_counts),
        "miss_rows": miss_rows,
        "rows": rows,
        "recent_ledger_rejects": ledger_rejects[-20:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", type=Path, default=DEFAULT_GUARD_STATE)
    parser.add_argument("--history-state", type=Path, default=DEFAULT_HISTORY_STATE)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--resolutions", type=Path, default=DEFAULT_RESOLUTIONS)
    args = parser.parse_args()
    report = build_report(
        json.loads(args.guard_state.read_text()),
        json.loads(args.ledger.read_text()),
        resolutions=load_resolutions(args.resolutions),
        history_state=json.loads(args.history_state.read_text()) if args.history_state.exists() else None,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
