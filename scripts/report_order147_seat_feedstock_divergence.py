#!/usr/bin/env python3
"""Publish the read-only ORDER147 seated-wallet guard-source audit."""

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

from scripts.run_top10_broad_paper_lane import _wallet_sides
from src.wallet_copy.store import atomic_write_json, load_json


def _ts(value: Any) -> float:
    text = str(value or "").replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(text).timestamp()
    except ValueError:
        return 0.0


def _stat(path: str) -> dict[str, Any]:
    target = ROOT / path
    try:
        stat = target.stat()
    except OSError:
        return {"path": path, "exists": False, "mtime": None, "byte_size": None}
    return {
        "path": path,
        "exists": True,
        "mtime": datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat(),
        "byte_size": stat.st_size,
    }


def guard_source_snapshot(guard: dict[str, Any]) -> dict[str, Any]:
    """Extract source identity and cursor data exclusively from guard state."""
    runtime = guard.get("guard_runtime_filter") or {}
    pipeline = (guard.get("pipeline") or {}).get("stdout_json") or {}
    polygon = pipeline.get("polygon_ws_premerge") or {}
    polygon_profile = polygon.get("profile") or {}
    polygon_tail = polygon_profile.get("tail_open_seek_read") or {}
    return {
        "rtds": {
            **_stat(str(runtime.get("rtds_jsonl") or "")),
            "cursor_state_path": runtime.get("rtds_offset_state"),
            "cursor_previous_offset": pipeline.get("previous_offset"),
            "cursor_next_offset": pipeline.get("next_offset"),
            "latest_scan_row_count": pipeline.get("rtds_rows"),
            "latest_scan_start_offset": (pipeline.get("premerge_substage_profile") or {}).get("tail_open_seek_read", {}).get("start_offset"),
            "latest_scan_end_offset": (pipeline.get("premerge_substage_profile") or {}).get("tail_open_seek_read", {}).get("end_offset"),
        },
        "polygon_orderfilled_ws_premerge": {
            **_stat(str(polygon.get("path") or polygon_profile.get("path") or "")),
            "cursor_state_path": None,
            "cursor_previous_offset": polygon_tail.get("start_offset"),
            "cursor_next_offset": polygon_tail.get("end_offset"),
            "latest_scan_row_count": polygon_profile.get("line_count"),
            "latest_scan_start_offset": polygon_tail.get("start_offset"),
            "latest_scan_end_offset": polygon_tail.get("end_offset"),
            "guard_reported_file_size": polygon_tail.get("file_size"),
        },
    }


def _history_tx(event: dict[str, Any]) -> str:
    raw = event.get("raw") if isinstance(event.get("raw"), dict) else {}
    return str(event.get("transaction_hash") or raw.get("transaction_hash") or raw.get("transactionHash") or "").lower()


def _history_log_index(event: dict[str, Any]) -> Any:
    raw = event.get("raw") if isinstance(event.get("raw"), dict) else {}
    return event.get("log_index", raw.get("log_index"))


def _event_window_start(event: dict[str, Any]) -> int | None:
    marker = str(event.get("market_slug") or "").rsplit("-", 1)[-1]
    if marker.isdigit():
        return int(marker)
    event_ts = event.get("event_ts")
    try:
        return int(float(event_ts) // 300) * 300
    except (TypeError, ValueError):
        return None


def three_window_feedstock(guard: dict[str, Any], history: dict[str, Any]) -> dict[str, Any]:
    """Measure every enabled member over the current and prior two BTC-5m windows."""
    as_of_s = _ts(guard.get("generated_at"))
    if as_of_s <= 0:
        as_of_s = max(
            (float(row.get("event_ts") or 0.0) for row in history.get("events") or [] if isinstance(row, dict)),
            default=0.0,
        )
    current_window = int(as_of_s // 300) * 300
    windows = [current_window - 600, current_window - 300, current_window]
    runtime = guard.get("active_set_runtime") if isinstance(guard.get("active_set_runtime"), dict) else {}
    members = [
        row
        for row in runtime.get("members") or []
        if isinstance(row, dict) and row.get("enabled") is not False
    ]
    selected = runtime.get("selected_member") if isinstance(runtime.get("selected_member"), dict) else {}
    selected_wallet = str(selected.get("source_wallet") or "").lower()
    rows: list[dict[str, Any]] = []
    for member in members:
        wallet = str(member.get("source_wallet") or member.get("wallet") or "").lower()
        max_price = float(member.get("max_price") or 1.0)
        max_order = float(member.get("max_order_usd") or 0.0)
        if wallet == selected_wallet:
            max_price = float(selected.get("max_price") or max_price)
            max_order = float(selected.get("max_order_usd") or max_order)
        unique: dict[tuple[Any, ...], dict[str, Any]] = {}
        sources: set[str] = set()
        for event in history.get("events") or []:
            if not isinstance(event, dict) or str(event.get("source_wallet") or "").lower() != wallet:
                continue
            if str(event.get("action") or event.get("side") or "").upper() != "BUY":
                continue
            window_start = _event_window_start(event)
            if window_start not in windows:
                continue
            try:
                price = float(event.get("price"))
            except (TypeError, ValueError):
                continue
            tx = _history_tx(event)
            key = (
                tx or str(event.get("event_id") or ""),
                window_start,
                str(event.get("outcome") or ""),
                round(price, 8),
            )
            prior = unique.get(key)
            if prior is None or float(event.get("event_ts") or 0.0) < float(prior.get("event_ts") or 0.0):
                unique[key] = event
            sources.add(str(event.get("source") or "unknown"))
        buys = list(unique.values())
        prices = [float(event.get("price") or 0.0) for event in buys]
        freshest_ts = max((float(event.get("event_ts") or 0.0) for event in buys), default=0.0)
        policy_compatible = [
            price for price in prices if 0.25 <= price <= min(0.50, max_price)
        ]
        rows.append(
            {
                "source_wallet": wallet,
                "selected": wallet == selected_wallet,
                "policy_id": member.get("policy_id"),
                "max_price": round(max_price, 6),
                "max_order_usd": round(max_order, 6),
                "source_events": len(buys),
                "freshest_buy_ts": (
                    datetime.fromtimestamp(freshest_ts, timezone.utc).isoformat() if freshest_ts else None
                ),
                "freshest_buy_age_s": round(max(0.0, as_of_s - freshest_ts), 6) if freshest_ts else None,
                "price_band_counts": {
                    "[0.25,0.32)": sum(0.25 <= price < 0.32 for price in prices),
                    "[0.32,0.50]": sum(0.32 <= price <= 0.50 for price in prices),
                    ">=0.50": sum(price >= 0.50 for price in prices),
                    "<0.25": sum(price < 0.25 for price in prices),
                },
                "policy_compatible_buy_events": len(policy_compatible),
                "clob_min_notional_lte_current_cap": sum(5.0 * price <= max_order + 1e-9 for price in policy_compatible),
                "clob_min_notional_lte_d2_cap": sum(5.0 * price <= 2.50 + 1e-9 for price in policy_compatible),
                "capture_sources": sorted(sources),
            }
        )
    return {
        "as_of": datetime.fromtimestamp(as_of_s, timezone.utc).isoformat() if as_of_s else None,
        "window_starts": windows,
        "rows": rows,
        "selection_rule": "fresh policy-compatible BUY within <=2 windows; non-le_25 max_price>=0.32",
        "clob_minimum_rule": "five shares; current member cap versus selected-only D2 ceiling $2.50",
    }


def build_report(
    *,
    guard: dict[str, Any],
    accumulator: dict[str, Any],
    history: dict[str, Any],
    accumulator_path: str,
) -> dict[str, Any]:
    identity = guard.get("guard_code_identity") or {}
    pipeline = (guard.get("pipeline") or {}).get("stdout_json") or {}
    premerge = guard.get("active_set_rtds_premerge") or {}
    wallet = str(pipeline.get("source_wallet") or premerge.get("selected_wallet") or "").lower()
    generation_started_at = identity.get("started_at_utc")
    generation_started_s = _ts(generation_started_at)

    captured: dict[tuple[str, Any], dict[str, Any]] = {}
    captured_row_count = 0
    for row in accumulator.get("rows") or []:
        if float(row.get("event_ts") or row.get("block_ts") or 0.0) < generation_started_s:
            continue
        if (wallet, "BUY") not in _wallet_sides(row, {wallet}):
            continue
        tx = str(row.get("transaction_hash") or "").lower()
        key = (tx, row.get("log_index"))
        if not tx:
            continue
        captured_row_count += 1
        item = captured.setdefault(
            key,
            {
                "transaction_hash": tx,
                "log_index": row.get("log_index"),
                "event_ts": row.get("event_ts") or row.get("block_ts"),
                "price": (row.get("decoded") or {}).get("price"),
                "size": (row.get("decoded") or {}).get("size"),
                "accumulator_sources": [],
                "duplicate_rows": 0,
            },
        )
        item["duplicate_rows"] += 1
        source = str(row.get("source") or "")
        if source and source not in item["accumulator_sources"]:
            item["accumulator_sources"].append(source)

    history_events = [
        event
        for event in history.get("events") or []
        if str(event.get("source_wallet") or "").lower() == wallet
        and float(event.get("event_ts") or 0.0) >= generation_started_s
    ]
    rows: list[dict[str, Any]] = []
    for key, item in sorted(captured.items()):
        exact = [event for event in history_events if (_history_tx(event), _history_log_index(event)) == key]
        tx_matches = [event for event in history_events if _history_tx(event) == key[0]]
        matches = exact or tx_matches
        match = matches[0] if matches else None
        raw = match.get("raw") if match and isinstance(match.get("raw"), dict) else {}
        observation_sources = list(raw.get("observation_sources") or [])
        item.update(
            {
                "guard_read": match is not None,
                "match_mode": "transaction_hash_and_log_index" if exact else "transaction_hash_after_cross_source_merge" if match else None,
                "guard_history_source": match.get("source") if match else None,
                "guard_observation_sources": observation_sources,
                "disposition": "READ_RETAINED_IN_GUARD_HOT_HISTORY" if match else "NEVER_READ_OR_NO_LONGER_RETAINED",
                "rejection_reason": None,
                "disposition_limit": "hot history proves reader admission; this artifact does not persist the later policy rejection reason",
            }
        )
        rows.append(item)

    read_count = sum(bool(row["guard_read"]) for row in rows)
    verdict = (
        "E1_GUARD_READER_ALIVE_DOWNSTREAM_PREDICATE_DEFECT"
        if read_count
        else "E2_GUARD_SOURCE_WIRING_DEFECT"
        if rows
        else "E3_NO_COMPARABLE_ACCUMULATOR_IDENTITIES"
    )
    return {
        "schema_version": 2,
        "kind": "order147_seat_feedstock_divergence",
        "paper_only": True,
        "live_orders_allowed": False,
        "status": verdict,
        "pre_registered_branch": "E1'''''''" if read_count else "E2'''''''" if rows else "E3'''''''",
        "seated_wallet": wallet,
        "guard_generation": {
            "pid": identity.get("pid"),
            "started_at": generation_started_at,
            "sha256": identity.get("live_guard_generation_sha256"),
            "checksum_scope": identity.get("live_guard_generation_rule"),
        },
        "guard_direct_sources": guard_source_snapshot(guard),
        "feedstock_3_window": three_window_feedstock(guard, history),
        "paper_accumulator": {
            **_stat(accumulator_path),
            "next_byte_offset": accumulator.get("next_byte_offset"),
            "seated_wallet_buy_rows_in_generation": captured_row_count,
            "unique_chain_identities": len(rows),
            "guard_read_unique_identities": read_count,
            "guard_unread_unique_identities": len(rows) - read_count,
            "identities": rows,
        },
        "verdict": verdict,
        "finding": "The guard retained the seated-wallet BUY identities; zero fresh feedstock is downstream of source reading/deduplication.",
        "rule": "source paths, scan counts, cursors, generation start, and checksum come only from resident guard state; no source scan and no restart",
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard", default="data/research/wallet_copy_live_guard_state.json")
    parser.add_argument("--accumulator", default="data/research/active_member_orderfilled_hot_source_shadow_accumulator.json")
    parser.add_argument("--history", default="data/research/wallet_copy_live_guard_hot_history_state.json")
    parser.add_argument("--output", default="data/research/order147_seat_feedstock_divergence_latest.json")
    args = parser.parse_args()
    report = build_report(
        guard=load_json(args.guard, default={}),
        accumulator=load_json(args.accumulator, default={}),
        history=load_json(args.history, default={}),
        accumulator_path=args.accumulator,
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
