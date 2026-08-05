#!/usr/bin/env python3
"""Report source-active BTC 5m windows for an active-set member."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_whale_consensus_paper_lane import DEFAULT_RTDS_JSONL, _feed_event_from_rtds, _iter_recent_jsonl  # noqa: E402
from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_OUTPUT = "data/research/member_source_active_windows_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS_JSONL)
    parser.add_argument("--source-wallet", required=True)
    parser.add_argument("--candidate-id", default="")
    parser.add_argument("--since-ts", type=float, default=0.0)
    parser.add_argument("--band-start-s", type=float, default=240.0)
    parser.add_argument("--band-end-s", type=float, default=300.0)
    parser.add_argument("--scan-limit", type=int, default=250_000)
    parser.add_argument("--scan-max-bytes", type=int, default=384_000_000)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def build_report(
    rows: list[dict[str, Any]],
    *,
    source_wallet: str,
    candidate_id: str = "",
    since_ts: float = 0.0,
    band_start_s: float = 240.0,
    band_end_s: float = 300.0,
    diagnostics: dict[str, Any] | None = None,
) -> dict[str, Any]:
    wallet = str(source_wallet or "").lower()
    counts: Counter[str] = Counter(diagnostics or {})
    by_window: dict[str, dict[str, Any]] = {}
    for row in rows:
        event = _feed_event_from_rtds(row)
        if event is None:
            counts["not_rtds_btc5m_trade"] += 1
            continue
        if event.source_wallet.lower() != wallet:
            counts["other_wallet"] += 1
            continue
        if event.side != "BUY":
            counts["non_buy_trade"] += 1
            continue
        if since_ts > 0 and float(event.event_ts) < float(since_ts):
            counts["before_since_ts"] += 1
            continue
        seconds_from_open = float(event.event_ts) - float(event.window_start_s)
        if seconds_from_open < float(band_start_s) or seconds_from_open > float(band_end_s):
            counts["outside_band"] += 1
            continue
        entry = by_window.setdefault(
            event.market_slug,
            {
                "market_slug": event.market_slug,
                "condition_id": event.condition_id,
                "window_start_s": event.window_start_s,
                "source_wallet": wallet,
                "candidate_id": candidate_id,
                "wallet_eligible_orders": 0,
                "outcomes": set(),
                "first_event_ts": event.event_ts,
                "last_event_ts": event.event_ts,
                "min_seconds_from_open": seconds_from_open,
                "max_seconds_from_open": seconds_from_open,
                "source_usd": 0.0,
            },
        )
        entry["wallet_eligible_orders"] += 1
        entry["outcomes"].add(event.outcome)
        entry["first_event_ts"] = min(float(entry["first_event_ts"]), float(event.event_ts))
        entry["last_event_ts"] = max(float(entry["last_event_ts"]), float(event.event_ts))
        entry["min_seconds_from_open"] = min(float(entry["min_seconds_from_open"]), seconds_from_open)
        entry["max_seconds_from_open"] = max(float(entry["max_seconds_from_open"]), seconds_from_open)
        entry["source_usd"] += float(event.source_usd)
    windows = []
    for entry in by_window.values():
        entry["outcomes"] = sorted(entry["outcomes"])
        entry["source_usd"] = round(float(entry["source_usd"]), 6)
        entry["min_seconds_from_open"] = round(float(entry["min_seconds_from_open"]), 6)
        entry["max_seconds_from_open"] = round(float(entry["max_seconds_from_open"]), 6)
        windows.append(entry)
    windows.sort(key=lambda item: float(item.get("window_start_s") or 0.0), reverse=True)
    return {
        "schema_version": 1,
        "kind": "member_source_active_windows_report",
        "generated_at": utc_now_iso(),
        "source_wallet": wallet,
        "candidate_id": candidate_id,
        "since_ts": float(since_ts),
        "band_start_s": float(band_start_s),
        "band_end_s": float(band_end_s),
        "source_active_windows": len(windows),
        "target_windows": 6,
        "target_met": len(windows) >= 6,
        "diagnostics": dict(sorted(counts.items())),
        "windows": windows,
    }


def main() -> int:
    args = parse_args()
    started = time.time()
    rows, diagnostics = _iter_recent_jsonl(args.rtds_jsonl, limit=int(args.scan_limit), max_bytes=int(args.scan_max_bytes))
    report = build_report(
        rows,
        source_wallet=args.source_wallet,
        candidate_id=args.candidate_id,
        since_ts=float(args.since_ts),
        band_start_s=float(args.band_start_s),
        band_end_s=float(args.band_end_s),
        diagnostics=diagnostics,
    )
    report["runtime_s"] = round(time.time() - started, 6)
    atomic_write_json(args.output, report)
    print(json.dumps({k: report[k] for k in ("source_wallet", "candidate_id", "source_active_windows", "target_met")}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
