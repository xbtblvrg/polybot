#!/usr/bin/env python3
"""Name the drop stage for sampled no-copy-signal watcher gaps."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json


DEFAULT_GAP = "data/research/wallet_copy_no_copy_signal_watcher_gap_latest.json"
DEFAULT_REALTIME_EVENTS = "data/research/wallet_copy_realtime_shadow_watch_events.jsonl"
DEFAULT_GUARD_EVENTS = "data/research/wallet_copy_guard_shadow_lanes_events.jsonl"
DEFAULT_OUTPUT = "data/research/wallet_copy_watcher_gap_drop_stage_latest.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gap-report", default=DEFAULT_GAP)
    parser.add_argument("--realtime-events", default=DEFAULT_REALTIME_EVENTS)
    parser.add_argument("--guard-events", default=DEFAULT_GUARD_EVENTS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    return parser.parse_args()


def _load_jsonl(path: str | Path) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    p = Path(path)
    if not p.exists():
        return out
    with p.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                out.append(row)
    return out


def _wallets(row: dict[str, Any]) -> set[str]:
    return {str(value).lower() for value in row.get("active_wallets_with_trades") or [] if str(value).startswith("0x")}


def _txs(row: dict[str, Any]) -> set[str]:
    return {
        str(trade.get("tx") or "").lower()
        for trade in row.get("sample_trades") or []
        if isinstance(trade, dict) and str(trade.get("tx") or "").startswith("0x")
    }


def _event_wallet(row: dict[str, Any]) -> str:
    return str(row.get("wallet") or row.get("source_wallet") or "").lower()


def _line_contains_any(row: dict[str, Any], needles: set[str]) -> bool:
    if not needles:
        return False
    text = json.dumps(row, sort_keys=True).lower()
    return any(needle in text for needle in needles)


def classify_rows(
    gap_rows: list[dict[str, Any]],
    realtime_events: list[dict[str, Any]],
    guard_events: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in gap_rows:
        if row.get("classification") != "WATCHER_GAP":
            continue
        slug = str(row.get("market_slug") or "")
        wallets = _wallets(row)
        txs = _txs(row)
        realtime_slug = [
            event
            for event in realtime_events
            if str(event.get("market_slug") or "") == slug
        ]
        realtime_wallet = [
            event
            for event in realtime_slug
            if _event_wallet(event) in wallets
        ]
        realtime_tx = [event for event in realtime_events if _line_contains_any(event, txs)]
        guard_slug = [
            event
            for event in guard_events
            if str(event.get("market_slug") or "") == slug or _line_contains_any(event, {slug})
        ]
        guard_tx = [event for event in guard_events if _line_contains_any(event, txs)]
        if realtime_tx or realtime_wallet:
            stage = "present_in_realtime_not_guard"
            if guard_slug or guard_tx:
                stage = "present_in_guard_but_no_live_copy_signal"
        else:
            stage = "missing_from_processed_realtime"
        max_source_age = max([float(event.get("source_age_s") or 0.0) for event in realtime_wallet], default=0.0)
        out.append(
            {
                "market_slug": slug,
                "window_start_s": row.get("window_start_s"),
                "active_wallet_trade_count": row.get("active_wallet_trade_count"),
                "sample_tx_count": len(txs),
                "processed_realtime_slug_events": len(realtime_slug),
                "processed_realtime_wallet_events": len(realtime_wallet),
                "processed_realtime_tx_events": len(realtime_tx),
                "guard_slug_events": len(guard_slug),
                "guard_tx_events": len(guard_tx),
                "max_processed_source_age_s": round(max_source_age, 6),
                "drop_stage": stage,
                "next": "inspect raw activity capture/offset resume for this window"
                if stage == "missing_from_processed_realtime"
                else "inspect guard filter taxonomy for this window",
            }
        )
    return out


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    gap = load_json(args.gap_report, default={})
    rows = gap.get("rows") if isinstance(gap, dict) and isinstance(gap.get("rows"), list) else []
    table = classify_rows(
        rows,
        _load_jsonl(args.realtime_events),
        _load_jsonl(args.guard_events),
    )
    counts = Counter(row["drop_stage"] for row in table)
    return {
        "generated_at": utc_now_iso(),
        "kind": "wallet_copy_watcher_gap_drop_stage",
        "flow_stage": "LIVE/PROMOTE/SELF-DEV",
        "inputs": {
            "gap_report": str(args.gap_report),
            "realtime_events": str(args.realtime_events),
            "guard_events": str(args.guard_events),
        },
        "summary": {
            "watcher_gap_rows": len(table),
            "drop_stage_counts": dict(sorted(counts.items())),
            "dominant_drop_stage": counts.most_common(1)[0][0] if counts else "",
        },
        "rows": table,
    }


def main() -> int:
    args = parse_args()
    report = build_report(args)
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
