#!/usr/bin/env python3
"""Attribute every UTC-day BTC-5m window using guard and captured-book evidence."""

from __future__ import annotations

import argparse
import datetime as dt
import glob
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.report_coverage_gap_diagnosis import collect_participation, submitted_windows
from scripts.report_order149_depth_at_size import _sweep
from src.wallet_copy.store import atomic_write_json, load_json

WINDOW_S = 300
WINDOWS_PER_DAY = 288
TARGET_USD = 12.138


def _slug(start_s: int) -> str:
    return f"btc-updown-5m-{start_s}"


def _iso(start_s: int) -> str:
    return dt.datetime.fromtimestamp(start_s, dt.timezone.utc).isoformat().replace("+00:00", "Z")


def load_book_windows(paths: Iterable[Path], metadata: dict[str, Any]) -> dict[str, dict[str, Any]]:
    windows: dict[str, dict[str, Any]] = {}
    for path in paths:
        with path.open(encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(row, dict):
                    continue
                asset = str(row.get("asset_id") or "")
                meta = metadata.get(asset) if isinstance(metadata.get(asset), dict) else {}
                slug = str(meta.get("market_slug") or "")
                if not slug.startswith("btc-updown-5m-"):
                    continue
                asks = [item for item in (row.get("asks") or []) if isinstance(item, dict)]
                prices = [float(item.get("price") or 0.0) for item in asks if float(item.get("price") or 0.0) > 0]
                best_ask = min(prices) if prices else 0.0
                bucket = windows.setdefault(slug, {
                    "snapshot_count": 0,
                    "assets": set(),
                    "in_band_snapshot_count": 0,
                    "target_fillable_snapshot_count": 0,
                })
                bucket["snapshot_count"] += 1
                bucket["assets"].add(asset)
                if 0.25 <= best_ask <= 0.50:
                    bucket["in_band_snapshot_count"] += 1
                    cap = min(0.99, best_ask * 1.025)
                    if _sweep(asks, TARGET_USD, max_price=cap)["fill_ratio"] >= 0.999:
                        bucket["target_fillable_snapshot_count"] += 1
    return windows


def build_report(
    *,
    day_start_s: int,
    now_s: float,
    ledger: dict[str, Any],
    participation: dict[str, dict[str, Any]],
    book_windows: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    day_end_s = day_start_s + WINDOWS_PER_DAY * WINDOW_S
    submitted = submitted_windows(ledger, start_s=day_start_s, end_s=day_end_s)
    rows = []
    categories: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    recoverable = 0
    for start_s in range(day_start_s, day_end_s, WINDOW_S):
        slug = _slug(start_s)
        observed = participation.get(slug) or {}
        book = book_windows.get(slug) or {}
        reason_counts = Counter(observed.get("skip_reasons") or {})
        dominant_reason = (
            max(reason_counts.items(), key=lambda item: (int(item[1]), str(item[0])))[0]
            if reason_counts else None
        )
        if start_s + WINDOW_S > now_s:
            category = "FUTURE_PENDING"
        elif slug in submitted or int(observed.get("our_submits") or 0) > 0:
            category = "SUBMITTED"
        elif dominant_reason:
            category = "NAMED_GUARD_ABSTAIN"
            reasons[str(dominant_reason)] += 1
        elif int(book.get("snapshot_count") or 0) > 0:
            category = "BOOK_OBSERVED_NO_GUARD_REASON"
            if int(book.get("target_fillable_snapshot_count") or 0) > 0:
                recoverable += 1
        else:
            category = "NO_BOOK_OR_GUARD_EVIDENCE"
        categories[category] += 1
        in_band = int(book.get("in_band_snapshot_count") or 0)
        fillable = int(book.get("target_fillable_snapshot_count") or 0)
        rows.append({
            "slot": (start_s - day_start_s) // WINDOW_S,
            "market_slug": slug,
            "window_start_s": start_s,
            "window_start_iso": _iso(start_s),
            "category": category,
            "dominant_guard_reason": dominant_reason,
            "guard_reason_counts": dict(sorted(reason_counts.items())),
            "wallet_eligible_orders": int(observed.get("wallet_eligible_orders") or 0),
            "our_submits": int(observed.get("our_submits") or 0),
            "our_fills": int(observed.get("our_fills") or 0),
            "book_snapshot_count": int(book.get("snapshot_count") or 0),
            "book_asset_count": len(book.get("assets") or []),
            "in_band_snapshot_count": in_band,
            "target_usd": TARGET_USD,
            "target_fillable_snapshot_count": fillable,
            "target_fillable_rate": round(fillable / in_band, 6) if in_band else None,
        })
    elapsed = sum(count for name, count in categories.items() if name != "FUTURE_PENDING")
    unexplained = categories["BOOK_OBSERVED_NO_GUARD_REASON"] + categories["NO_BOOK_OR_GUARD_EVIDENCE"]
    return {
        "schema_version": 1,
        "kind": "order150_window_supply_attribution",
        "flow_stage": "ROTATE/MEASURE/LIVE",
        "paper_only": True,
        "live_orders_allowed": False,
        "day_utc": _iso(day_start_s)[:10],
        "generated_at": _iso(int(now_s)),
        "windows_total": WINDOWS_PER_DAY,
        "elapsed_windows": elapsed,
        "category_counts": dict(sorted(categories.items())),
        "named_guard_reasons": dict(sorted(reasons.items(), key=lambda item: (-item[1], item[0]))),
        "unexplained_elapsed_windows": unexplained,
        "book_observed_unreasoned_windows_with_12p138_fillability": recoverable,
        "rows": rows,
        "integrity": {
            "row_count": len(rows),
            "category_sum": sum(categories.values()),
            "exactly_one_category_per_window": len(rows) == WINDOWS_PER_DAY and sum(categories.values()) == WINDOWS_PER_DAY,
        },
        "rule": "exactly one category per UTC-day window; book evidence is joined through immutable token metadata; no future window is graded as a supply gap",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--day", required=True)
    parser.add_argument("--now-s", type=float, default=0.0)
    parser.add_argument("--ledger", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument("--guard-state", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--guard-events", default="data/research/wallet_copy_live_guard_events.jsonl")
    parser.add_argument("--metadata", default="data/research/wide_token_metadata_cache.json")
    parser.add_argument("--book-snapshots", action="append", required=True)
    parser.add_argument("--output", default="data/research/order150_window_supply_attribution_latest.json")
    args = parser.parse_args()
    day_start = int(dt.datetime.strptime(args.day, "%Y-%m-%d").replace(tzinfo=dt.timezone.utc).timestamp())
    now_s = float(args.now_s or time.time())
    guard_state = load_json(args.guard_state, default={})
    participation = collect_participation(
        guard_state=guard_state,
        guard_events_path=Path(args.guard_events),
        start_s=day_start,
        end_s=day_start + WINDOWS_PER_DAY * WINDOW_S,
    )
    paths = sorted({Path(item) for pattern in args.book_snapshots for item in glob.glob(pattern)})
    report = build_report(
        day_start_s=day_start,
        now_s=now_s,
        ledger=load_json(args.ledger, default={}),
        participation=participation,
        book_windows=load_book_windows(paths, load_json(args.metadata, default={})),
    )
    report["source_files"] = [str(path) for path in paths]
    atomic_write_json(args.output, report)
    print(json.dumps({key: report[key] for key in ("day_utc", "elapsed_windows", "category_counts", "unexplained_elapsed_windows", "book_observed_unreasoned_windows_with_12p138_fillability", "integrity")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
