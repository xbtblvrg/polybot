#!/usr/bin/env python3
"""Incrementally retain identity-clean BTC-5m 01a window epochs in daily partitions."""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.polymarket_addresses import EXCHANGE_ADDRESSES
from src.wallet_copy.realtime_feed import normalize_polygon_orderfilled_row
from src.wallet_copy.store import atomic_write_json
from scripts.merge_rtds_wallet_events import _gamma_token_metadata

WINDOW_RE = re.compile(r"btc-(?:updown|up-or-down)-5m-(\d+)")
DEFAULT_MAX_INCREMENT_BYTES = 4 * 1024 * 1024 * 1024


def _load(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_increment(path: Path, *, start: int, end: int):
    with path.open("rb") as handle:
        if start:
            handle.seek(start - 1)
            if handle.read(1) != b"\n":
                handle.readline()
        else:
            handle.seek(0)
        while handle.tell() < end:
            raw = handle.readline(end - handle.tell())
            if not raw:
                break
            try:
                row = json.loads(raw)
            except (json.JSONDecodeError, UnicodeDecodeError):
                continue
            if isinstance(row, dict):
                yield row


def _increment_bounds(events_path: Path, state: dict[str, Any], max_increment_bytes: int) -> tuple[os.stat_result, int, int, bool, int]:
    stat = events_path.stat()
    same_file = int(state.get("device") or -1) == stat.st_dev and int(state.get("inode") or -1) == stat.st_ino
    requested_start = int(state.get("next_byte_offset") or 0) if same_file else 0
    end = stat.st_size
    capture_gap = requested_start > end or end - requested_start > max_increment_bytes
    start = max(0, end - max_increment_bytes) if capture_gap else requested_start
    return stat, start, end, capture_gap, requested_start


def _increment_starts(events_path: Path, *, start: int, end: int) -> set[int]:
    starts: set[int] = set()
    for row in _read_increment(events_path, start=start, end=end):
        event = normalize_polygon_orderfilled_row(row)
        if event is None or event.event_ts is None:
            continue
        base = int(float(event.event_ts) // 300) * 300
        starts.update((base - 300, base, base + 300))
    return starts


def _merge_partition(path: Path, *, day_utc: str, rows: dict[str, dict[str, set[int]]], generated_at: str) -> None:
    current = _load(path)
    merged: dict[str, dict[str, set[int]]] = {}
    for row in current.get("rows", []):
        if not isinstance(row, dict) or not row.get("wallet"):
            continue
        wallet = str(row["wallet"]).lower()
        merged[wallet] = {
            "observed": {int(value) for value in row.get("observed_window_epochs", [])},
            "qualifying": {int(value) for value in row.get("qualifying_01a_window_epochs", [])},
        }
    for wallet, values in rows.items():
        target = merged.setdefault(wallet, {"observed": set(), "qualifying": set()})
        target["observed"].update(values["observed"])
        target["qualifying"].update(values["qualifying"])
    distinct_window_epochs = sorted(
        {epoch for values in merged.values() for epoch in values["observed"]}
    )
    atomic_write_json(path, {
        "kind": "orderfilled_01a_supply_daily_partition",
        "day_utc": day_utc,
        "generated_at": generated_at,
        "paper_only": True,
        "live_mutation": False,
        "distinct_window_epochs": distinct_window_epochs,
        "distinct_window_epoch_count": len(distinct_window_epochs),
        "coverage_of_288": round(len(distinct_window_epochs) / 288.0, 6),
        "rows": [
            {
                "wallet": wallet,
                "observed_window_epochs": sorted(values["observed"]),
                "qualifying_01a_window_epochs": sorted(values["qualifying"]),
            }
            for wallet, values in sorted(merged.items())
        ],
    })


def _hold_cursor_on_gamma_failure(
    next_state: dict[str, Any],
    *,
    bounds: tuple[os.stat_result, int, int, bool, int] | None,
    gamma_stats: dict[str, int],
) -> None:
    if bounds is None or int(gamma_stats.get("request_failures") or 0) <= 0:
        return
    next_state["next_byte_offset"] = bounds[4]
    next_state["status"] = "GAMMA_UNRESOLVED_HOLD"
    next_state["cursor_hold_reason"] = "gamma_request_failures_gt_zero; idempotent partition merge makes replay safe"


def accumulate(
    *,
    events_path: Path,
    metadata: dict[str, Any],
    state: dict[str, Any],
    max_increment_bytes: int,
    bounds: tuple[os.stat_result, int, int, bool, int] | None = None,
) -> tuple[dict[str, dict[str, dict[str, set[int]]]], dict[str, Any]]:
    if not state:
        stat = events_path.stat()
        return {}, {
            "status": "INITIALIZED_FROM_EOF",
            "device": stat.st_dev,
            "inode": stat.st_ino,
            "next_byte_offset": stat.st_size,
            "capture_gap": False,
            "rows_seen": 0,
            "rows_retained": 0,
        }
    stat, start, end, capture_gap, requested_start = bounds or _increment_bounds(events_path, state, max_increment_bytes)
    daily: dict[str, dict[str, dict[str, set[int]]]] = {}
    rows_seen = rows_retained = 0
    for row in _read_increment(events_path, start=start, end=end):
        rows_seen += 1
        event = normalize_polygon_orderfilled_row(row)
        if event is None or event.event_ts is None or event.price is None or not event.asset:
            continue
        info = metadata.get(str(event.asset)) or {}
        match = WINDOW_RE.search(str(info.get("market_slug") or ""))
        if not match:
            continue
        epoch = int(match.group(1))
        offset_s = float(event.event_ts) - epoch
        if not 0 <= offset_s < 300:
            continue
        wallet = event.maker if str(event.maker_side).upper() == "BUY" else event.taker
        wallet = str(wallet or "").lower()
        if not re.fullmatch(r"0x[0-9a-f]{40}", wallet) or wallet in EXCHANGE_ADDRESSES:
            continue
        day_utc = datetime.fromtimestamp(epoch, tz=UTC).date().isoformat()
        values = daily.setdefault(day_utc, {}).setdefault(wallet, {"observed": set(), "qualifying": set()})
        values["observed"].add(epoch)
        if 0.25 <= float(event.price) < 0.32 and offset_s < 60:
            values["qualifying"].add(epoch)
        rows_retained += 1
    return daily, {
        "status": "CAPTURE_GAP_TAIL_RECOVERY" if capture_gap else "PASS_INCREMENTAL",
        "device": stat.st_dev,
        "inode": stat.st_ino,
        "next_byte_offset": end,
        "capture_gap": capture_gap,
        "requested_start_offset": requested_start,
        "read_start_offset": start,
        "read_end_offset": end,
        "rows_seen": rows_seen,
        "rows_retained": rows_retained,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--events", default="data/research/polygon_orderfilled_ws_shadow_resident.jsonl")
    parser.add_argument("--metadata", default="data/research/wide_token_metadata_cache.json")
    parser.add_argument("--state", default="data/research/orderfilled_01a_supply_daily_accumulator_state.json")
    parser.add_argument("--partitions-dir", default="data/research/orderfilled_01a_supply_daily")
    parser.add_argument("--max-increment-bytes", type=int, default=DEFAULT_MAX_INCREMENT_BYTES)
    parser.add_argument("--gamma-base-url", default=os.getenv("POLYMARKET_GAMMA_BASE_URL", "https://gamma-api.polymarket.com"))
    parser.add_argument("--gamma-timeout-s", type=float, default=3.0)
    args = parser.parse_args()
    generated_at = datetime.now(tz=UTC).isoformat().replace("+00:00", "Z")
    state_path = Path(args.state)
    prior_state = _load(state_path)
    events_path = Path(args.events)
    metadata = _load(Path(args.metadata))
    bounds = None
    gamma_stats: dict[str, int] = {}
    if prior_state:
        bounds = _increment_bounds(events_path, prior_state, max(1, int(args.max_increment_bytes)))
        starts = _increment_starts(events_path, start=bounds[1], end=bounds[2])
        metadata.update(_gamma_token_metadata(str(args.gamma_base_url), starts=starts, timeout_s=float(args.gamma_timeout_s), stats=gamma_stats))
    daily, next_state = accumulate(
        events_path=events_path,
        metadata=metadata,
        state=prior_state,
        max_increment_bytes=max(1, int(args.max_increment_bytes)),
        bounds=bounds,
    )
    _hold_cursor_on_gamma_failure(next_state, bounds=bounds, gamma_stats=gamma_stats)
    partitions_dir = Path(args.partitions_dir)
    for day_utc, rows in daily.items():
        _merge_partition(partitions_dir / f"{day_utc}.json", day_utc=day_utc, rows=rows, generated_at=generated_at)
    next_state.update({
        "kind": "orderfilled_01a_supply_daily_accumulator_state",
        "generated_at": generated_at,
        "initialized_at": prior_state.get("initialized_at") or generated_at,
        "declared_max_cadence_s": 5 * 60 * 60,
        "scheduled_cadence_s": 15 * 60,
        "partition_days_written": sorted(daily),
        "gamma_lookup": gamma_stats,
        "paper_only": True,
        "live_mutation": False,
    })
    atomic_write_json(state_path, next_state)
    print(json.dumps({key: next_state[key] for key in ("status", "next_byte_offset", "rows_seen", "rows_retained", "partition_days_written")}), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
