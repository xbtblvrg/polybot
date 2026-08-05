#!/usr/bin/env python3
"""Offline slippage sensitivity replay for the fresh top10 BTC-5m artifact."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_EVENT_LOG = Path("data/research/wallet_copy_top10_fresh_btc5m_events_20260705.jsonl")
DEFAULT_OUTPUT = Path("data/research/wallet_copy_top10_fresh_slippage_sensitivity_20260705.json")


def _finite_float(value: Any, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    return parsed if math.isfinite(parsed) else default


def _book_for(row: dict[str, Any]) -> dict[str, Any]:
    result = row.get("result") if isinstance(row.get("result"), dict) else {}
    book = result.get("book") if isinstance(result.get("book"), dict) else {}
    return book


def _top_for(book: dict[str, Any]) -> dict[str, Any]:
    top = book.get("top_of_book")
    return top if isinstance(top, dict) else {}


def load_events(path: Path, *, tail_bytes: int = 0) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        if tail_bytes > 0:
            handle.seek(max(0, path.stat().st_size - tail_bytes))
            if handle.tell() > 0:
                handle.readline()
        for line_no, raw_line in enumerate(handle, start=1):
            line = raw_line.decode("utf-8", errors="ignore")
            text = line.strip()
            if not text:
                continue
            try:
                rows.append(json.loads(text))
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_no}: invalid jsonl row") from exc
    return rows


def replay_event(row: dict[str, Any], *, slippage_bps: float | None, min_fill_ratio: float) -> dict[str, Any]:
    book = _book_for(row)
    top = _top_for(book)
    source_price = _finite_float(row.get("source_price") or book.get("source_price"))
    copy_size_usd = _finite_float(row.get("copy_size_usd") or book.get("copy_size_usd"))
    best_ask = _finite_float(top.get("best_ask") or book.get("best_ask"))
    best_bid = _finite_float(top.get("best_bid") or book.get("best_bid"))
    best_ask_depth_usd = _finite_float(top.get("best_ask_depth_usd"))
    latency_ms = _finite_float(row.get("receipt_to_fetch_latency_ms"), default=float("nan"))
    book_timestamp = top.get("book_timestamp") or book.get("book_timestamp")

    price_cap = None if slippage_bps is None else min(0.99, source_price * (1.0 + slippage_bps / 10000.0))
    required_slippage_bps = None
    if source_price > 0 and best_ask > 0:
        required_slippage_bps = (best_ask / source_price - 1.0) * 10000.0

    reason = "filled"
    fillable_usd = copy_size_usd
    if not book_timestamp or copy_size_usd <= 0 or source_price <= 0:
        reason = "book_unavailable_or_invalid"
        fillable_usd = 0.0
    elif best_ask <= 0 or best_ask_depth_usd <= 0:
        reason = "no_ask_liquidity"
        fillable_usd = 0.0
    elif price_cap is not None and best_ask > price_cap + 1e-12:
        reason = "price_above_slippage_cap"
        fillable_usd = 0.0
    else:
        fillable_usd = min(copy_size_usd, best_ask_depth_usd)
        if copy_size_usd > 0 and fillable_usd / copy_size_usd < min_fill_ratio:
            reason = "insufficient_top_ask_depth"

    fill_ratio = fillable_usd / copy_size_usd if copy_size_usd > 0 else 0.0
    copyable = reason == "filled" and fill_ratio >= min_fill_ratio
    if not copyable:
        fillable_usd = 0.0
        fill_ratio = 0.0

    filled_shares = fillable_usd / best_ask if best_ask > 0 else 0.0
    paper_pnl_usd = filled_shares * (best_bid - best_ask) if copyable and best_bid > 0 else 0.0

    return {
        "asset": row.get("asset"),
        "best_ask": round(best_ask, 8),
        "best_ask_depth_usd": round(best_ask_depth_usd, 8),
        "best_bid": round(best_bid, 8),
        "copy_size_usd": round(copy_size_usd, 8),
        "copyable": copyable,
        "fill_ratio": round(fill_ratio, 8),
        "filled_usd": round(fillable_usd, 8),
        "latency_ms": None if math.isnan(latency_ms) else round(latency_ms, 3),
        "paper_pnl_usd": round(paper_pnl_usd, 8),
        "price_cap": None if price_cap is None else round(price_cap, 8),
        "reason": reason,
        "required_slippage_bps": None if required_slippage_bps is None else round(required_slippage_bps, 3),
        "source_price": round(source_price, 8),
        "transaction_hash": row.get("transaction_hash"),
        "wallet": row.get("wallet"),
    }


def summarize_rung(rows: list[dict[str, Any]], *, label: str, slippage_bps: float | None, min_fill_ratio: float) -> dict[str, Any]:
    event_results = [replay_event(row, slippage_bps=slippage_bps, min_fill_ratio=min_fill_ratio) for row in rows]
    by_wallet: dict[str, dict[str, Any]] = {}
    wallet_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for event in event_results:
        wallet_rows[str(event.get("wallet") or "unknown")].append(event)
    for wallet, events in sorted(wallet_rows.items()):
        by_wallet[wallet] = {
            "copyable_events": sum(1 for event in events if event["copyable"]),
            "events": len(events),
            "paper_pnl_usd": round(sum(_finite_float(event.get("paper_pnl_usd")) for event in events), 6),
            "reasons": dict(sorted(Counter(str(event["reason"]) for event in events if not event["copyable"]).items())),
        }
    return {
        "copyable_events": sum(1 for event in event_results if event["copyable"]),
        "events": len(event_results),
        "label": label,
        "paper_pnl_usd": round(sum(_finite_float(event.get("paper_pnl_usd")) for event in event_results), 6),
        "per_wallet": by_wallet,
        "reject_reasons": dict(sorted(Counter(str(event["reason"]) for event in event_results if not event["copyable"]).items())),
        "slippage_bps": None if slippage_bps is None else round(slippage_bps, 3),
    }


def drift_latency_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    samples: list[dict[str, float]] = []
    for row in rows:
        book = _book_for(row)
        top = _top_for(book)
        source_price = _finite_float(row.get("source_price") or book.get("source_price"))
        best_ask = _finite_float(top.get("best_ask") or book.get("best_ask"))
        best_bid = _finite_float(top.get("best_bid") or book.get("best_bid"))
        latency_ms = _finite_float(row.get("receipt_to_fetch_latency_ms"), default=float("nan"))
        if source_price <= 0 or best_ask <= 0 or math.isnan(latency_ms):
            continue
        samples.append(
            {
                "best_ask_minus_source_bps": (best_ask / source_price - 1.0) * 10000.0,
                "best_bid_minus_source_bps": ((best_bid / source_price - 1.0) * 10000.0) if best_bid > 0 else 0.0,
                "latency_ms": latency_ms,
            }
        )

    buckets = {
        "lt_10s": [s for s in samples if s["latency_ms"] < 10_000],
        "10s_to_30s": [s for s in samples if 10_000 <= s["latency_ms"] < 30_000],
        "30s_to_45s": [s for s in samples if 30_000 <= s["latency_ms"] < 45_000],
        "gte_45s": [s for s in samples if s["latency_ms"] >= 45_000],
    }

    def stats(values: list[dict[str, float]]) -> dict[str, Any]:
        if not values:
            return {"count": 0}
        return {
            "avg_best_ask_minus_source_bps": round(statistics.fmean(v["best_ask_minus_source_bps"] for v in values), 3),
            "avg_best_bid_minus_source_bps": round(statistics.fmean(v["best_bid_minus_source_bps"] for v in values), 3),
            "avg_latency_ms": round(statistics.fmean(v["latency_ms"] for v in values), 3),
            "count": len(values),
            "max_latency_ms": round(max(v["latency_ms"] for v in values), 3),
        }

    return {
        "bucket_stats": {name: stats(values) for name, values in buckets.items()},
        "events_with_latency_and_book": len(samples),
        "overall": stats(samples),
    }


def build_report(rows: list[dict[str, Any]], *, base_slippage_bps: float, min_fill_ratio: float, event_log: Path) -> dict[str, Any]:
    rungs = [
        summarize_rung(rows, label="current", slippage_bps=base_slippage_bps, min_fill_ratio=min_fill_ratio),
        summarize_rung(rows, label="1.5x", slippage_bps=base_slippage_bps * 1.5, min_fill_ratio=min_fill_ratio),
        summarize_rung(rows, label="2x", slippage_bps=base_slippage_bps * 2.0, min_fill_ratio=min_fill_ratio),
        summarize_rung(rows, label="uncapped", slippage_bps=None, min_fill_ratio=min_fill_ratio),
    ]
    positive_rungs = [
        rung["label"]
        for rung in rungs
        if int(rung["copyable_events"]) >= 3 and _finite_float(rung["paper_pnl_usd"]) > 0
    ]
    return {
        "base_slippage_bps": base_slippage_bps,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "input_event_log": str(event_log),
        "kind": "top10_fresh_slippage_sensitivity",
        "latency_note": "Event log carries per-event receipt_to_fetch_latency_ms; artifact summary latency may undercount rerun tails.",
        "live_orders_allowed": False,
        "min_fill_ratio": min_fill_ratio,
        "paper_only": True,
        "park_rule": {
            "positive_rungs": positive_rungs,
            "status": "PARK_NO_POSITIVE_FRESH_BOOK_POLICY" if not positive_rungs else "KEEP_FOR_REVIEW",
            "threshold": "positive paper_pnl_usd and >=3 copyable events",
        },
        "replay_limitations": [
            "Uses recorded top_of_book depth only; full ask ladder was not persisted in the event log.",
            "Uncapped rung therefore means best-ask-only without slippage cap, not a reconstructed multi-level sweep.",
        ],
        "rungs": rungs,
        "schema_version": 1,
        "source_events": len(rows),
        "source_wallets": dict(sorted(Counter(str(row.get("wallet") or "unknown") for row in rows).items())),
        "drift_vs_latency": drift_latency_summary(rows),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-log", type=Path, default=DEFAULT_EVENT_LOG)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--tail-bytes", type=int, default=0, help="Read only the last N bytes of the event log; 0 reads the whole file.")
    parser.add_argument("--base-slippage-bps", type=float, default=250.0)
    parser.add_argument("--min-fill-ratio", type=float, default=0.999)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = load_events(args.event_log, tail_bytes=max(0, int(args.tail_bytes or 0)))
    report = build_report(
        rows,
        base_slippage_bps=args.base_slippage_bps,
        min_fill_ratio=args.min_fill_ratio,
        event_log=args.event_log,
    )
    atomic_write_json(args.output, report)
    print(json.dumps({"output": str(args.output), "park_rule": report["park_rule"], "rungs": report["rungs"]}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
