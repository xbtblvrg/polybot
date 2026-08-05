#!/usr/bin/env python3
"""Report realized economics for min-live-floor pinned tranches.

Flow stage: LIVE/MEASURE/DEFEND. This packet is report-only: it reads the
live execution ledger and current scorecard, joins pinned filled orders to
canonical PnL rows, and writes evidence for the Fable n>=10 review gate.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_LIVE_STATE = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_pinned_tranche_economics_latest.json"
PIN_DIRECTION_ID = "2026-07-16T07:40Z-fable-seat-holder-min-live-pin"
DEFAULT_PROBE_TRIGGER_USD = -12.0
DEFAULT_TRIGGER_N = 10
WILSON_95_Z = 1.959963984540054


def _utc_now_iso() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _rooted(path: str | Path) -> Path:
    parsed = Path(path)
    return parsed if parsed.is_absolute() else ROOT / parsed


def _load_json(path: str | Path, default: Any) -> Any:
    try:
        return json.loads(_rooted(path).read_text())
    except Exception:
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _find_key_values(obj: Any, key: str) -> list[Any]:
    values: list[Any] = []
    if isinstance(obj, dict):
        for item_key, item_value in obj.items():
            if item_key == key:
                values.append(item_value)
            values.extend(_find_key_values(item_value, key))
    elif isinstance(obj, list):
        for item in obj:
            values.extend(_find_key_values(item, key))
    return values


def _first_key_value(obj: Any, key: str) -> Any:
    values = _find_key_values(obj, key)
    return values[0] if values else None


def _is_pinned_order(order: dict[str, Any]) -> bool:
    return any(_as_bool(value) for value in _find_key_values(order, "min_live_floor_pin"))


def _status(order: dict[str, Any]) -> str:
    return str(order.get("final_status") or order.get("status") or "").upper()


def _default_current_scorecard(day: str | None = None) -> Path:
    data_dir = ROOT / "data" / "research"
    day = day or datetime.now(UTC).date().isoformat()
    preferred = data_dir / f"wallet_copy_daily_scorecard_{day}_current.json"
    if preferred.exists():
        return preferred
    candidates = sorted(
        data_dir.glob("wallet_copy_daily_scorecard_*.json"),
        key=lambda path: (path.stat().st_mtime if path.exists() else 0.0, path.name),
        reverse=True,
    )
    for path in candidates:
        loaded = _load_json(path, {})
        if isinstance(loaded, dict) and loaded.get("kind") == "wallet_copy_daily_scorecard":
            return path
    return data_dir / "wallet_copy_daily_scorecard_current.json"


def _scorecard_events(scorecard: dict[str, Any]) -> list[dict[str, Any]]:
    canonical = scorecard.get("canonical_pnl_truth") if isinstance(scorecard.get("canonical_pnl_truth"), dict) else {}
    events = canonical.get("events") if isinstance(canonical.get("events"), list) else []
    return [row for row in events if isinstance(row, dict)]


def _event_indexes(scorecard: dict[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[tuple[str, str], dict[str, Any]]]:
    by_order_id: dict[str, dict[str, Any]] = {}
    by_market_submit: dict[tuple[str, str], dict[str, Any]] = {}
    for event in _scorecard_events(scorecard):
        order_id = str(event.get("order_id") or "")
        if order_id:
            by_order_id[order_id] = event
        key = (str(event.get("market_slug") or ""), str(event.get("submitted_at") or ""))
        if all(key):
            by_market_submit[key] = event
    return by_order_id, by_market_submit


def _matched_event(
    order: dict[str, Any],
    by_order_id: dict[str, dict[str, Any]],
    by_market_submit: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    order_id = str(order.get("order_id") or "")
    if order_id and order_id in by_order_id:
        return by_order_id[order_id]
    key = (str(order.get("market_slug") or ""), str(order.get("submitted_at") or ""))
    return by_market_submit.get(key, {})


def _pnl_bucket(value: float) -> str:
    if value <= -5.0:
        return "lte_-5"
    if value < -1.0:
        return "-5_to_-1"
    if value < 0.0:
        return "-1_to_0"
    if value == 0.0:
        return "zero"
    if value < 1.0:
        return "0_to_1"
    if value < 5.0:
        return "1_to_5"
    return "gte_5"


def _requested_payout_usd(*, cost_usd: float, requested_size_usd: float, limit_price: Any, actual_payout_usd: Any) -> float | None:
    actual_payout = _as_float(actual_payout_usd, default=0.0)
    if actual_payout > 0.0:
        return actual_payout
    price = _as_float(limit_price)
    stake = cost_usd if cost_usd > 0.0 else requested_size_usd
    if price <= 0.0 or stake <= 0.0:
        return None
    return stake / price


def _wilson_lower_bound(successes: int, total: int, z: float = WILSON_95_Z) -> float | None:
    if total <= 0:
        return None
    phat = successes / total
    z2 = z * z
    denominator = 1.0 + z2 / total
    centre = phat + z2 / (2.0 * total)
    margin = z * math.sqrt((phat * (1.0 - phat) + z2 / (4.0 * total)) / total)
    return (centre - margin) / denominator


def _bucket_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    buckets: dict[str, dict[str, Any]] = defaultdict(lambda: {"fills": 0, "wins": 0, "losses": 0, "cost_usd": 0.0, "pnl_usd": 0.0})
    for row in rows:
        bucket = str(row.get("price_bucket") or "unknown")
        out = buckets[bucket]
        pnl = _as_float(row.get("pnl_usd"))
        cost = _as_float(row.get("cost_usd"))
        out["fills"] += 1
        out["wins"] += int(pnl > 0.0)
        out["losses"] += int(pnl < 0.0)
        out["cost_usd"] += cost
        out["pnl_usd"] += pnl
    normalized: list[dict[str, Any]] = []
    for bucket, row in buckets.items():
        cost = float(row["cost_usd"])
        pnl = float(row["pnl_usd"])
        normalized.append(
            {
                "price_bucket": bucket,
                "fills": int(row["fills"]),
                "wins": int(row["wins"]),
                "losses": int(row["losses"]),
                "cost_usd": round(cost, 6),
                "pnl_usd": round(pnl, 6),
                "roi_pct": round(100.0 * pnl / cost, 6) if cost else None,
            }
        )
    normalized.sort(key=lambda row: (_as_float(row.get("pnl_usd")), _as_float(row.get("roi_pct"))))
    return {
        "worst_bucket": normalized[0] if normalized else None,
        "buckets": normalized,
    }


def build_report(
    *,
    live_state: dict[str, Any],
    scorecard: dict[str, Any],
    scorecard_path: Path,
    live_state_path: Path,
    output_path: Path,
    trigger_n: int = DEFAULT_TRIGGER_N,
    probe_trigger_usd: float = DEFAULT_PROBE_TRIGGER_USD,
) -> dict[str, Any]:
    by_order_id, by_market_submit = _event_indexes(scorecard)
    orders = live_state.get("orders") if isinstance(live_state.get("orders"), list) else []
    pinned_orders = [row for row in orders if isinstance(row, dict) and _is_pinned_order(row)]

    pinned_rows: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    unresolved_filled = 0
    for order in pinned_orders:
        status = _status(order)
        status_counts[status or "UNKNOWN"] += 1
        if status != "FILLED":
            continue
        event = _matched_event(order, by_order_id, by_market_submit)
        resolved = bool(event.get("resolved")) if event else False
        if not resolved:
            unresolved_filled += 1
        pnl = _as_float(event.get("pnl_usd")) if event else 0.0
        cost = _as_float(event.get("cost_usd")) if event else _as_float(order.get("requested_size_usd"))
        requested_size = _as_float(order.get("requested_size_usd"))
        requested_payout = _requested_payout_usd(
            cost_usd=cost,
            requested_size_usd=requested_size,
            limit_price=order.get("limit_price"),
            actual_payout_usd=event.get("payout_usd") if event else None,
        )
        row = {
            "order_id": order.get("order_id"),
            "intent_id": order.get("intent_id"),
            "submitted_at": order.get("submitted_at"),
            "market_slug": order.get("market_slug"),
            "source_wallet": order.get("source_wallet"),
            "wallet_name": order.get("wallet_name"),
            "execution_role": order.get("execution_role"),
            "status": status,
            "resolved": resolved,
            "side": event.get("side") if event else order.get("side"),
            "winner": event.get("winner") if event else None,
            "limit_price": order.get("limit_price"),
            "requested_size_usd": round(requested_size, 6),
            "cost_usd": round(cost, 6),
            "requested_payout_usd": round(requested_payout, 6) if requested_payout is not None else None,
            "payout_usd": round(_as_float(event.get("payout_usd")), 6) if event else None,
            "pnl_usd": round(pnl, 6) if event else None,
            "pnl_per_requested_usd": round(pnl / requested_size, 6) if requested_size else None,
            "price_bucket": event.get("price_bucket") if event else None,
            "min_live_floor_pin": True,
            "min_live_floor_pin_direction_id": _first_key_value(order, "min_live_floor_pin_direction_id"),
            "min_live_floor_pin_usd": _first_key_value(order, "min_live_floor_pin_usd"),
            "min_live_floor_pin_pre_pin_window_budget_usd": _first_key_value(
                order,
                "min_live_floor_pin_pre_pin_window_budget_usd",
            ),
            "min_live_floor_pin_max_usd": _first_key_value(order, "min_live_floor_pin_max_usd"),
        }
        pinned_rows.append(row)

    resolved_rows = [row for row in pinned_rows if row.get("resolved")]
    resolved_n = len(resolved_rows)
    pnl_total = sum(_as_float(row.get("pnl_usd")) for row in resolved_rows)
    cost_total = sum(_as_float(row.get("cost_usd")) for row in resolved_rows)
    requested_total = sum(_as_float(row.get("requested_size_usd")) for row in resolved_rows)
    requested_payout_total = sum(
        _as_float(row.get("requested_payout_usd")) for row in resolved_rows if row.get("requested_payout_usd") is not None
    )
    wins = sum(1 for row in resolved_rows if _as_float(row.get("pnl_usd")) > 0.0)
    losses = sum(1 for row in resolved_rows if _as_float(row.get("pnl_usd")) < 0.0)
    pnl_bucket_counts = Counter(_pnl_bucket(_as_float(row.get("pnl_usd"))) for row in resolved_rows)
    bucket_summary = _bucket_summary(resolved_rows)
    worst_fill = min(resolved_rows, key=lambda row: _as_float(row.get("pnl_usd")), default=None)
    distance = pnl_total - float(probe_trigger_usd)
    trigger_met = resolved_n >= int(trigger_n)
    breakeven_win_rate = cost_total / requested_payout_total if requested_payout_total > 0.0 else None
    wilson_lower = _wilson_lower_bound(wins, resolved_n)
    wilson_gt_breakeven = (
        None
        if wilson_lower is None or breakeven_win_rate is None
        else wilson_lower > breakeven_win_rate
    )
    breached_probe_trigger = pnl_total <= float(probe_trigger_usd)
    lte_minus_5_count = int(pnl_bucket_counts.get("lte_-5", 0))
    sizing_gate_20_30z = {
        "direction": "2026-07-16T09:15Z-fable-pinned-sizing-preregistration",
        "candidate_step": "pin window budget 1 -> 2 min tranches (max about $5/window)",
        "resolved_pinned_n_gte_50": resolved_n >= 50,
        "cumulative_pinned_pnl_gte_10": pnl_total >= 10.0,
        "wilson_95_lower_bound_gt_breakeven": wilson_gt_breakeven,
        "breached_probe_trigger_false": not breached_probe_trigger,
        "lte_minus_5_windows_zero": lte_minus_5_count == 0,
    }
    sizing_gate_20_30z["all_criteria_met"] = all(
        value is True
        for key, value in sizing_gate_20_30z.items()
        if key
        in {
            "resolved_pinned_n_gte_50",
            "cumulative_pinned_pnl_gte_10",
            "wilson_95_lower_bound_gt_breakeven",
            "breached_probe_trigger_false",
            "lte_minus_5_windows_zero",
        }
    )

    return {
        "kind": "wallet_copy_pinned_tranche_economics",
        "flow_stage": "LIVE/MEASURE/DEFEND",
        "generated_at": _utc_now_iso(),
        "status": "TRIGGER_MET_PACKET_READY" if trigger_met else "PENDING_TRIGGER",
        "rule": (
            "Fable 2026-07-16T08:22Z/08:47Z: when resolved PINNED fills reach "
            "n>=10, report realized PnL per pinned tranche, win rate, worst bucket, "
            "and distance to the -12.0 probe trigger before any sizing discussion."
        ),
        "direction_id": PIN_DIRECTION_ID,
        "inputs": {
            "live_state": str(live_state_path.relative_to(ROOT) if live_state_path.is_relative_to(ROOT) else live_state_path),
            "scorecard": str(scorecard_path.relative_to(ROOT) if scorecard_path.is_relative_to(ROOT) else scorecard_path),
            "scorecard_generated_at": scorecard.get("generated_at"),
            "scorecard_day_utc": scorecard.get("day_utc"),
            "output": str(output_path.relative_to(ROOT) if output_path.is_relative_to(ROOT) else output_path),
        },
        "threshold_change_allowed": False,
        "ledger_rewrite": False,
        "trigger": {
            "resolved_pinned_fill_floor": int(trigger_n),
            "resolved_pinned_fills": resolved_n,
            "met": trigger_met,
        },
        "summary": {
            "pinned_orders": len(pinned_orders),
            "pinned_status_counts": dict(sorted(status_counts.items())),
            "pinned_filled_orders": sum(1 for row in pinned_orders if _status(row) == "FILLED"),
            "resolved_pinned_fills": resolved_n,
            "unresolved_pinned_filled_orders": unresolved_filled,
            "pnl_usd": round(pnl_total, 6),
            "cost_usd": round(cost_total, 6),
            "requested_size_usd": round(requested_total, 6),
            "requested_payout_usd": round(requested_payout_total, 6) if requested_payout_total else None,
            "breakeven_win_rate_pct": round(100.0 * breakeven_win_rate, 6)
            if breakeven_win_rate is not None
            else None,
            "roi_pct": round(100.0 * pnl_total / cost_total, 6) if cost_total else None,
            "avg_pnl_per_resolved_pinned_fill_usd": round(pnl_total / resolved_n, 6) if resolved_n else None,
            "avg_pnl_per_requested_usd": round(pnl_total / requested_total, 6) if requested_total else None,
            "wins": wins,
            "losses": losses,
            "win_rate_pct": round(100.0 * wins / resolved_n, 6) if resolved_n else None,
            "wilson_95_lower_bound_win_rate_pct": round(100.0 * wilson_lower, 6)
            if wilson_lower is not None
            else None,
            "wilson_lower_bound_gt_breakeven": wilson_gt_breakeven,
            "pnl_bucket_counts": dict(sorted(pnl_bucket_counts.items())),
            "lte_minus_5_count": lte_minus_5_count,
            "worst_bucket": bucket_summary["worst_bucket"],
            "worst_fill": worst_fill,
            "probe_trigger_usd": float(probe_trigger_usd),
            "distance_to_probe_trigger_usd": round(distance, 6),
            "breached_probe_trigger": breached_probe_trigger,
            "sizing_gate_20_30z": sizing_gate_20_30z,
        },
        "price_buckets": bucket_summary["buckets"],
        "rows": pinned_rows,
    }


def _dated_output_path(output: Path, generated_at: str) -> Path:
    stamp = generated_at.replace("-", "").replace(":", "").replace("+00:00", "Z")
    stamp = stamp.replace("Z", "Z")
    return output.with_name(f"{output.stem}_{stamp}{output.suffix}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--live-state", default=DEFAULT_LIVE_STATE)
    parser.add_argument("--scorecard", default="")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--trigger-n", type=int, default=DEFAULT_TRIGGER_N)
    parser.add_argument("--probe-trigger-usd", type=float, default=DEFAULT_PROBE_TRIGGER_USD)
    parser.add_argument("--no-dated-copy", action="store_true")
    args = parser.parse_args()

    live_state_path = _rooted(args.live_state)
    scorecard_path = _rooted(args.scorecard) if args.scorecard else _default_current_scorecard()
    output_path = _rooted(args.output)
    live_state = _load_json(live_state_path, {})
    from src.wallet_copy.scorecard import load_fresh_scorecard

    scorecard = load_fresh_scorecard(scorecard_path)
    if not isinstance(live_state, dict) or not live_state:
        print(f"missing live state: {live_state_path}", file=sys.stderr)
        return 2
    if not isinstance(scorecard, dict) or scorecard.get("kind") != "wallet_copy_daily_scorecard":
        print(f"missing scorecard: {scorecard_path}", file=sys.stderr)
        return 2
    report = build_report(
        live_state=live_state,
        scorecard=scorecard,
        scorecard_path=scorecard_path,
        live_state_path=live_state_path,
        output_path=output_path,
        trigger_n=int(args.trigger_n),
        probe_trigger_usd=float(args.probe_trigger_usd),
    )
    atomic_write_json(output_path, report)
    if not args.no_dated_copy:
        atomic_write_json(_dated_output_path(output_path, str(report.get("generated_at"))), report)
    print(
        "pinned_tranche_economics "
        f"status={report['status']} "
        f"resolved={report['summary']['resolved_pinned_fills']} "
        f"pnl={report['summary']['pnl_usd']:+.6f} "
        f"win_rate={report['summary']['win_rate_pct']} "
        f"distance_to_probe={report['summary']['distance_to_probe_trigger_usd']:+.6f}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
