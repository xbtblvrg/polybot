#!/usr/bin/env python3
"""Persistently score prospective top10 RTDS events against direct CLOB books."""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_top10_broad_paper_lane import (
    _disable_source_base_overrides,
    _restore_source_base_overrides,
    build_measurement_state,
)
from src.wallet_copy.live_tracker import CLOBMarketClient
from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json

DEFAULT_RTDS = "data/research/polygon_orderfilled_ws_shadow_resident.jsonl"
DEFAULT_LANE = "data/research/wallet_copy_top10_broad_paper_lane_state.json"
DEFAULT_OUTPUT = "data/research/wallet_copy_top10_broad_paper_measurement_state.json"
DEFAULT_EVENTS = "data/research/wallet_copy_top10_broad_paper_events.jsonl"
DEFAULT_LOCK = "data/research/wallet_copy_top10_direct_clob_paper_service.lock"
DIRECT_CLOB = "https://clob.polymarket.com"


def _read_new_rows(path: str, *, offset: int, inode: int) -> tuple[list[dict[str, Any]], int, int]:
    target = Path(path)
    if not target.exists():
        return [], offset, inode
    stat = target.stat()
    current_inode = int(stat.st_ino)
    if inode == 0:
        return [], int(stat.st_size), current_inode
    adjusted = False
    if inode != current_inode or stat.st_size < offset:
        offset = max(0, stat.st_size - 50_000_000)
        adjusted = True
    if stat.st_size - offset > 50_000_000:
        offset = max(0, stat.st_size - 50_000_000)
        adjusted = True
    rows: list[dict[str, Any]] = []
    with target.open("rb") as handle:
        handle.seek(offset)
        if adjusted and offset > 0:
            handle.readline()
        for raw in handle:
            try:
                row = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(row, dict) and row.get("event") in {
                "polygon_orderfilled_log",
                "rtds_trade_event",
                "wallet_copy_wallet_event",
            }:
                rows.append(row)
        offset = handle.tell()
    return rows, offset, current_inode


def _run_once(args: argparse.Namespace, state: dict[str, Any]) -> dict[str, Any]:
    lane = load_json(args.lane_state, default={})
    lane = lane if isinstance(lane, dict) else {}
    rows, offset, inode = _read_new_rows(
        args.rtds_jsonl,
        offset=int(state.get("source_offset") or 0),
        inode=int(state.get("source_inode") or 0),
    )
    started = time.monotonic()
    clob = CLOBMarketClient(host=DIRECT_CLOB, timeout_s=float(args.clob_timeout_s))
    measurement, events = build_measurement_state(
        lane_state=lane,
        polygon_rows=rows,
        clob=clob,
        prior_state=state,
        wallet_fraction=0.1,
        max_order_usd=2.0,
        min_order_usd=1.0,
        slippage_bps=250.0,
        min_fill_ratio=0.999,
        max_events=500,
        max_book_fetches=int(args.max_book_fetches),
        policy_id="top10_direct_clob_paper_v1",
        floor_copy_size_to_min_order=True,
        buy_events_only=True,
        market_category="btc_5m",
        diagnose_liquidity_enabled=False,
        diagnose_liquidity_limit=0,
        max_receipt_to_fetch_age_s=60.0,
    )
    measurement.update(
        {
            "flow_stage": "OBSERVE/LEARN/SELF-DEV/PROMOTE",
            "paper_only": True,
            "live_orders_allowed": False,
            "orders_submitted": 0,
            "source_offset": offset,
            "source_inode": inode,
            "service_cycle_count": int(state.get("service_cycle_count") or 0) + 1,
            "service_cycle_completed_at": utc_now_iso(),
            "service_cycle_duration_s": round(time.monotonic() - started, 6),
        }
    )
    measurement.setdefault("source", {}).update(
        {
            "path": args.rtds_jsonl,
            "input": "prospective_rtds_offset_reader",
            "direct_clob_base_url": DIRECT_CLOB,
            "source_base_overrides_disabled": True,
            "rows_read_this_cycle": len(rows),
        }
    )
    atomic_write_json(args.output, measurement)
    if events:
        append_jsonl_many(args.event_log, events)
    return measurement


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS)
    parser.add_argument("--lane-state", default=DEFAULT_LANE)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--event-log", default=DEFAULT_EVENTS)
    parser.add_argument("--lock-file", default=DEFAULT_LOCK)
    parser.add_argument("--clob-timeout-s", type=float, default=1.5)
    parser.add_argument("--max-book-fetches", type=int, default=20)
    parser.add_argument("--sleep-s", type=float, default=15.0)
    parser.add_argument("--iterations", type=int, default=0, help="0 runs continuously.")
    args = parser.parse_args()
    lock = Path(args.lock_file)
    lock.parent.mkdir(parents=True, exist_ok=True)
    handle = lock.open("a+")
    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print(json.dumps({"status": "ALREADY_RUNNING", "lock_file": str(lock)}))
        return 0
    prior_env = _disable_source_base_overrides(True)
    try:
        state = load_json(args.output, default={})
        state = state if isinstance(state, dict) else {}
        completed = 0
        while int(args.iterations) <= 0 or completed < int(args.iterations):
            state = _run_once(args, state)
            completed += 1
            if int(args.iterations) > 0 and completed >= int(args.iterations):
                break
            time.sleep(max(1.0, float(args.sleep_s)))
        print(
            json.dumps(
                {
                    "status": state.get("status"),
                    "service_cycle_count": state.get("service_cycle_count"),
                    "updated_at": state.get("updated_at"),
                },
                sort_keys=True,
            )
        )
        return 0
    finally:
        _restore_source_base_overrides(prior_env)
        fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


if __name__ == "__main__":
    raise SystemExit(main())
