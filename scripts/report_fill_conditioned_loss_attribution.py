#!/usr/bin/env python3
"""Report fill-conditioned loss concentrations for today's live fills."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402
from src.wallet_copy.scorecard import load_fresh_scorecard  # noqa: E402

DEFAULT_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_fill_conditioned_loss_attribution_latest.json"


def _utc_now_iso() -> str:
    return datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return num(text, 0.0)


def _price_bucket_5c(price: float) -> str:
    if price < 0.25:
        return "00_00_25"
    if price >= 0.50:
        return "50_100"
    lower = int((price - 0.25) // 0.05)
    start = 25 + lower * 5
    end = start + 5
    return f"{start:02d}_{end:02d}"


def _window_time_bucket(event: dict[str, Any]) -> str:
    slug = str(event.get("market_slug") or "")
    try:
        window_start = float(slug.rsplit("-", 1)[1])
    except (IndexError, ValueError):
        window_start = 0.0
    submitted = _parse_ts(event.get("submitted_at") or event.get("ts"))
    age = submitted - window_start if window_start and submitted else None
    if age is None:
        return "unknown"
    if age < 60:
        return "00_60s"
    if age < 180:
        return "60_180s"
    if age < 300:
        return "180_300s"
    return "post_window"


def _signal_age_bucket(order: dict[str, Any]) -> str:
    latency = order.get("latency_budget") if isinstance(order.get("latency_budget"), dict) else {}
    hops = latency.get("hops") if isinstance(latency.get("hops"), dict) else {}
    age = num(hops.get("source_fill_block_to_exchange_ack_s"), 0.0)
    if age <= 0:
        age = num((order.get("source_intent") or {}).get("api_latency_s"), 0.0) if isinstance(order.get("source_intent"), dict) else 0.0
    if age <= 0:
        return "unknown"
    if age < 60:
        return "00_60s"
    if age < 120:
        return "60_120s"
    if age < 180:
        return "120_180s"
    return "180s_plus"


def _empty_group(dimension: str, value: str) -> dict[str, Any]:
    return {
        "dimension": dimension,
        "value": value,
        "fills": 0,
        "cost_usd": 0.0,
        "pnl_usd": 0.0,
        "wins": 0,
        "prices": [],
    }


def _add(group: dict[str, Any], *, cost: float, pnl: float, price: float) -> None:
    group["fills"] += 1
    group["cost_usd"] += cost
    group["pnl_usd"] += pnl
    group["wins"] += int(pnl > 0)
    if price > 0:
        group["prices"].append(price)


def _finalize(group: dict[str, Any]) -> dict[str, Any]:
    fills = int(group["fills"])
    cost = float(group["cost_usd"])
    pnl = float(group["pnl_usd"])
    prices = group["prices"]
    return {
        "dimension": group["dimension"],
        "value": group["value"],
        "fills": fills,
        "wins": int(group["wins"]),
        "win_rate_pct": round(100.0 * group["wins"] / fills, 6) if fills else None,
        "cost_usd": round(cost, 6),
        "pnl_usd": round(pnl, 6),
        "roi_pct": round(100.0 * pnl / cost, 6) if cost else None,
        "avg_price": round(sum(prices) / len(prices), 6) if prices else None,
    }


def _candidate_policy_change(row: dict[str, Any]) -> str:
    dimension = row.get("dimension")
    value = str(row.get("value") or "")
    if dimension == "price_bucket_5c":
        return f"candidate: exclude or downsize price bucket {value} until post-floor sample is positive"
    if dimension == "side":
        return f"candidate: side filter or downsize {value} until fill-conditioned ROI recovers"
    if dimension == "source_wallet":
        return f"candidate: demote or downsize wallet {value} subject to Fable and post-floor bar"
    if dimension == "signal_age_bucket":
        return f"candidate: require fresher signal age than {value} for taker/drip fills"
    if dimension == "window_time_bucket":
        return f"candidate: suppress fills in window-time bucket {value} pending positive evidence"
    return "candidate: inspect this concentration before further sizing"


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    scorecard = load_fresh_scorecard(args.scorecard)
    ledger = load_json(args.ledger, default={})
    orders = ledger.get("orders") if isinstance(ledger.get("orders"), list) else []
    orders_by_id = {str(order.get("order_id") or ""): order for order in orders if isinstance(order, dict)}
    events = (
        (scorecard.get("canonical_pnl_truth") or {}).get("events")
        if isinstance(scorecard.get("canonical_pnl_truth"), dict)
        else []
    )
    fills = [
        event
        for event in events
        if isinstance(event, dict)
        and str(event.get("status") or "").upper() == "FILLED"
        and bool(event.get("resolved"))
        and str(event.get("day_utc") or "") == str(args.day)
    ]
    groups: dict[tuple[str, str], dict[str, Any]] = {}
    band_groups: dict[tuple[str, str], dict[str, Any]] = {}
    post_floor_fills = 0
    post_floor_rejects: defaultdict[str, int] = defaultdict(int)
    post_floor_since = _parse_ts(args.post_floor_since)

    for event in fills:
        price = num(event.get("limit_price"), 0.0)
        cost = num(event.get("cost_usd"), 0.0)
        pnl = num(event.get("pnl_usd"), 0.0)
        order = orders_by_id.get(str(event.get("order_id") or ""), {})
        values = {
            "price_bucket_5c": _price_bucket_5c(price),
            "side": str(event.get("side") or "unknown").upper(),
            "window_time_bucket": _window_time_bucket(event),
            "signal_age_bucket": _signal_age_bucket(order),
            "source_wallet": str(event.get("source_wallet") or "unknown").lower(),
        }
        for dimension, value in values.items():
            key = (dimension, value)
            group = groups.setdefault(key, _empty_group(dimension, value))
            _add(group, cost=cost, pnl=pnl, price=price)
            if 0.25 <= price < 0.50:
                band_group = band_groups.setdefault(key, _empty_group(dimension, value))
                _add(band_group, cost=cost, pnl=pnl, price=price)
        if post_floor_since and _parse_ts(event.get("submitted_at") or event.get("ts")) >= post_floor_since and 0.25 <= price < 0.50:
            post_floor_fills += 1

    if post_floor_since:
        for order in orders:
            if not isinstance(order, dict):
                continue
            ts = _parse_ts(order.get("submitted_at") or order.get("updated_at"))
            price = num(order.get("limit_price"), 0.0)
            status = str(order.get("final_status") or order.get("status") or "").upper()
            if ts >= post_floor_since and 0.25 <= price < 0.50 and status == "REJECTED":
                reason = str(order.get("skip_reason") or order.get("reject_reason") or order.get("final_status_reason") or "unknown")
                post_floor_rejects[reason] += 1

    finalized = [_finalize(group) for group in groups.values()]
    band_finalized = [_finalize(group) for group in band_groups.values()]
    loss_rows = [row for row in band_finalized if num(row.get("pnl_usd"), 0.0) < 0]
    loss_rows.sort(key=lambda row: (num(row.get("pnl_usd"), 0.0), -int(row.get("fills") or 0)))
    top = loss_rows[: int(args.top)]
    for row in top:
        row["candidate_policy_change"] = _candidate_policy_change(row)

    total_cost = sum(num(event.get("cost_usd"), 0.0) for event in fills)
    total_pnl = sum(num(event.get("pnl_usd"), 0.0) for event in fills)
    return {
        "kind": "wallet_copy_fill_conditioned_loss_attribution",
        "flow_stage": "LIVE/LEARN",
        "generated_at": _utc_now_iso(),
        "day_utc": str(args.day),
        "inputs": {
            "ledger": args.ledger,
            "scorecard": args.scorecard,
            "post_floor_since": args.post_floor_since,
        },
        "summary": {
            "resolved_fills": len(fills),
            "cost_usd": round(total_cost, 6),
            "pnl_usd": round(total_pnl, 6),
            "roi_pct": round(100.0 * total_pnl / total_cost, 6) if total_cost else None,
            "post_floor_25_50_fills": post_floor_fills,
            "post_floor_25_50_rejects": dict(sorted(post_floor_rejects.items())),
            "top_loss_count": len(top),
            "top_loss_scope": "resolved_fills_with_limit_price_0.25_0.50",
        },
        "top_loss_concentrations": top,
        "band_groups_25_50": band_finalized,
        "groups": finalized,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--day", default=datetime.now(tz=UTC).date().isoformat())
    parser.add_argument("--post-floor-since", default="2026-07-07T21:15:02Z")
    parser.add_argument("--top", type=int, default=3)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args)
    atomic_write_json(args.output, report)
    print(json.dumps({"output": args.output, "summary": report["summary"], "top": report["top_loss_concentrations"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
