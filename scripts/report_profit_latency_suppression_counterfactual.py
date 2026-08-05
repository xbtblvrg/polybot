#!/usr/bin/env python3
"""Resolve profit-latency rejects and report the preregistered gate verdict."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

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


def _bucket(window_time_s: float) -> str:
    if window_time_s < 120.0:
        return "60_120"
    if window_time_s < 180.0:
        return "120_180"
    return "180_plus"


def _outcome(value: Any) -> str:
    outcome = str(value or "").strip().lower()
    return {"yes": "up", "no": "down"}.get(outcome, outcome)


def _price_band(price: float) -> str:
    return "01a_25_32" if 0.25 <= price < 0.32 else "out_of_01a"


def _day(value: Any) -> str:
    return str(value or "")[:10]


def _band_bucket_aggregates(resolved_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate every resolved row by price band and window bucket.

    The published ``rows`` list is truncated; these aggregates are not, so a band
    ruling is never read off a tail sample.
    """

    out: dict[str, Any] = {}
    for row in resolved_rows:
        band = out.setdefault(row["price_band"], {})
        cell = band.setdefault(
            row["bucket"],
            {"resolved_windows": 0, "post_fee_counterfactual_pnl_usd": 0.0, "cost_usd": 0.0, "days": set()},
        )
        cell["resolved_windows"] += 1
        cell["post_fee_counterfactual_pnl_usd"] += float(row["post_fee_counterfactual_pnl_usd"])
        cell["cost_usd"] += float(row["copy_size_usd"])
        if row["day"]:
            cell["days"].add(row["day"])
    for band in out.values():
        for cell in band.values():
            days = sorted(cell.pop("days"))
            cost = round(float(cell["cost_usd"]), 6)
            pnl = round(float(cell["post_fee_counterfactual_pnl_usd"]), 6)
            cell["cost_usd"] = cost
            cell["post_fee_counterfactual_pnl_usd"] = pnl
            cell["post_fee_roi_pct"] = round(100.0 * pnl / cost, 6) if cost > 0.0 else None
            cell["distinct_days"] = len(days)
            cell["first_day"] = days[0] if days else None
            cell["last_day"] = days[-1] if days else None
    return out


def _day_bounded_split(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Split one cohort into day-bounded development/holdout halves."""

    days = sorted({row["day"] for row in rows if row["day"]})
    if len(days) < 2:
        return {"status": "ACCRUING_INSUFFICIENT_DAYS", "distinct_days": len(days)}
    cut = days[len(days) // 2]
    halves: dict[str, dict[str, Any]] = {}
    for name, member in (
        ("development", lambda day: day < cut),
        ("chronological_holdout", lambda day: day >= cut),
    ):
        subset = [row for row in rows if row["day"] and member(row["day"])]
        cost = round(sum(float(row["copy_size_usd"]) for row in subset), 6)
        pnl = round(sum(float(row["post_fee_counterfactual_pnl_usd"]) for row in subset), 6)
        halves[name] = {
            "rows": len(subset),
            "distinct_days": len({row["day"] for row in subset}),
            "cost_usd": cost,
            "post_fee_counterfactual_pnl_usd": pnl,
            "post_fee_roi_pct": round(100.0 * pnl / cost, 6) if cost > 0.0 else None,
        }
    holdout_pnl = halves["chronological_holdout"]["post_fee_counterfactual_pnl_usd"]
    halves["split_integrity"] = "DAY_BOUNDED"
    halves["split_day"] = cut
    halves["status"] = "POSITIVE_HOLDOUT" if holdout_pnl > 0.0 else "NON_POSITIVE_HOLDOUT"
    return halves


def build_report(
    *,
    event_rows: list[dict[str, Any]],
    resolution_rows: list[dict[str, Any]],
    generated_at: str,
    min_resolved_windows: int = 100,
) -> dict[str, Any]:
    resolutions: dict[str, str] = {}
    for row in resolution_rows:
        winner = _winner(row)
        if not winner:
            continue
        for raw_key in (row.get("market"), row.get("condition_id"), row.get("market_slug")):
            key = str(raw_key or "").strip().lower()
            if key:
                resolutions[key] = winner

    by_intent: dict[str, dict[str, Any]] = {}
    for row in event_rows:
        if str(row.get("event") or "") != "wallet_copy_live_profit_latency_suppression_reject":
            continue
        intent_id = str(row.get("intent_id") or "").strip()
        if not intent_id:
            continue
        tags = row.get("taxonomy_tags") if isinstance(row.get("taxonomy_tags"), list) else []
        tags = [str(tag) for tag in tags]
        if not any(tag.startswith("window_time_gte_") for tag in tags) and not str(
            row.get("taxonomy") or ""
        ).startswith("window_time_gte_"):
            continue
        window_time_s = float(row.get("window_time_s") or 0.0)
        if window_time_s < 60.0:
            continue
        current = by_intent.get(intent_id)
        if current is None or window_time_s < float(current.get("window_time_s") or 0.0):
            by_intent[intent_id] = row

    # The live cap is one fill per market window, so the first suppressed
    # intent is the executable counterfactual when a window repeats.
    by_window: dict[str, dict[str, Any]] = {}
    for row in by_intent.values():
        window = str(
            row.get("market_slug") or row.get("condition_id") or row.get("market") or ""
        ).strip().lower()
        if not window:
            continue
        current = by_window.get(window)
        if current is None or float(row.get("window_time_s") or 0.0) < float(current.get("window_time_s") or 0.0):
            by_window[window] = row

    buckets: dict[str, dict[str, Any]] = {
        name: {"suppressed_windows": 0, "resolved_windows": 0, "post_fee_counterfactual_pnl_usd": 0.0}
        for name in ("60_120", "120_180", "180_plus")
    }
    resolved_rows: list[dict[str, Any]] = []
    for window, row in by_window.items():
        window_time_s = float(row.get("window_time_s") or 0.0)
        bucket = _bucket(window_time_s)
        buckets[bucket]["suppressed_windows"] += 1
        winner = (
            resolutions.get(str(row.get("condition_id") or "").lower())
            or resolutions.get(str(row.get("market") or "").lower())
            or resolutions.get(str(row.get("market_slug") or "").lower())
        )
        if not winner:
            continue
        price = float(row.get("limit_price") or 0.0)
        cost = float(row.get("copy_size_usd") or 0.0)
        if not (0.0 < price < 1.0 and cost > 0.0):
            continue
        shares = cost / price
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        won = _outcome(row.get("outcome")) == _outcome(winner)
        pnl = (shares if won else 0.0) - cost - fee
        buckets[bucket]["resolved_windows"] += 1
        buckets[bucket]["post_fee_counterfactual_pnl_usd"] += pnl
        resolved_rows.append(
            {
                "intent_id": row.get("intent_id"),
                "market_slug": row.get("market_slug"),
                "source_wallet": row.get("source_wallet"),
                "outcome": row.get("outcome"),
                "winning_outcome": winner,
                "won": won,
                "window_time_s": round(window_time_s, 6),
                "bucket": bucket,
                "ts": row.get("ts"),
                "day": _day(row.get("ts")),
                "price_band": _price_band(price),
                "limit_price": round(price, 6),
                "copy_size_usd": round(cost, 6),
                "expected_fee_usd": round(fee, 6),
                "post_fee_counterfactual_pnl_usd": round(pnl, 6),
            }
        )

    for values in buckets.values():
        values["post_fee_counterfactual_pnl_usd"] = round(
            float(values["post_fee_counterfactual_pnl_usd"]), 6
        )
    decision_band = {
        "resolved_windows": buckets["60_120"]["resolved_windows"] + buckets["120_180"]["resolved_windows"],
        "post_fee_counterfactual_pnl_usd": round(
            buckets["60_120"]["post_fee_counterfactual_pnl_usd"]
            + buckets["120_180"]["post_fee_counterfactual_pnl_usd"],
            6,
        ),
    }
    boundary = len(resolved_rows) >= int(min_resolved_windows)
    status = (
        "RAISE_TO_180"
        if boundary and decision_band["post_fee_counterfactual_pnl_usd"] > 0
        else "KEEP_60"
        if boundary
        else "ACCRUING"
    )
    return {
        "schema_version": 1,
        "kind": "profit_latency_suppression_counterfactual",
        "flow_stage": "LIVE/LEARN/DEFEND",
        "generated_at": generated_at,
        "status": status,
        "decision_boundary_reached": boundary,
        "min_resolved_suppressed_windows": int(min_resolved_windows),
        "unique_suppressed_intents": len(by_intent),
        "suppressed_windows": len(by_window),
        "resolved_suppressed_windows": len(resolved_rows),
        "pending_resolution_windows": max(0, len(by_window) - len(resolved_rows)),
        "decision_band_60_180": decision_band,
        "buckets": buckets,
        "price_band_buckets": _band_bucket_aggregates(resolved_rows),
        "focus_01a_60_120": _day_bounded_split(
            [
                row
                for row in resolved_rows
                if row["price_band"] == "01a_25_32" and row["bucket"] == "60_120"
            ]
        ),
        "live_mutation": False,
        "copyintent_parity_violations": 0,
        "decision_rule": "at >=100 resolved suppressed windows raise 60->180 only if [60,180) aggregate post-fee PnL is positive; >=180 never reopens",
        "rows_total": len(resolved_rows),
        "rows_published": min(300, len(resolved_rows)),
        "rows_truncation": "last 300 resolved rows only; band rulings must read price_band_buckets, never rows",
        "rows": resolved_rows[-300:],
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-log", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--resolution-log", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--output", default="data/research/profit_latency_suppression_counterfactual_latest.json")
    parser.add_argument("--min-resolved-windows", type=int, default=100)
    args = parser.parse_args()
    generated_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    report = build_report(
        event_rows=list(_jsonl(ROOT / args.event_log) or []),
        resolution_rows=list(_jsonl(ROOT / args.resolution_log) or []),
        generated_at=generated_at,
        min_resolved_windows=args.min_resolved_windows,
    )
    atomic_write_json(ROOT / args.output, report)
    print(
        json.dumps(
            {
                key: report[key]
                for key in ("status", "resolved_suppressed_windows", "decision_band_60_180")
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
