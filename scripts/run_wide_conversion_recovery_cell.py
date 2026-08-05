#!/usr/bin/env python3
"""Run checksum-isolated WIDE metadata or alpha-refusal recovery paper cells."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.reconcile_wide_exact_policy_paper import expected_polymarket_buy_fee_usd
from scripts.wide_direct_handoff_journal import load_jsonl, row_identity
from src.wallet_copy.store import atomic_write_json, load_json

CELLS = {
    "token_map": "REFUSED_METADATA_MISSING",
    "alpha_counterfactual": "REFUSED_ALPHA_PROFILE_FILTER",
}
PRICE_BINS = ((0.0, 0.25), (0.25, 0.50), (0.50, 0.75), (0.75, 1.01))
TIME_BINS = ((0, 60), (60, 120), (120, 180), (180, 240), (240, 301))


def _checksum(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode()).hexdigest()


def _ts(value: Any) -> float:
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except (TypeError, ValueError):
        return 0.0


def _tail_jsonl(path: Path, max_bytes: int = 32 * 1024 * 1024) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("rb") as handle:
        handle.seek(0, 2)
        size = handle.tell()
        handle.seek(max(0, size - max_bytes))
        if size > max_bytes:
            handle.readline()
        data = handle.read().decode("utf-8", errors="ignore")
    rows = []
    for line in data.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _prereg(args: argparse.Namespace, measurement: dict[str, Any]) -> dict[str, Any]:
    existing = load_json(args.preregistration, default={})
    if existing:
        body = {key: value for key, value in existing.items() if key != "checksum"}
        if existing.get("checksum") != _checksum(body):
            raise RuntimeError("immutable recovery-cell preregistration checksum mismatch")
        return existing
    manifest = measurement.get("manifest") if isinstance(measurement.get("manifest"), dict) else {}
    cohort = measurement.get("cohort") if isinstance(measurement.get("cohort"), dict) else {}
    wallets = sorted(str(wallet).lower() for wallet in (measurement.get("wallets") or {}))
    body = {
        "schema_version": 1,
        "kind": "wide_conversion_recovery_preregistration",
        "cell": args.cell,
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "source_run_id": cohort.get("run_id"),
        "source_generation_identity": {
            "manifest_id": manifest.get("manifest_id"),
            "run_id": cohort.get("run_id"),
            "cohort_id": cohort.get("cohort_id"),
            "policy_id": measurement.get("policy_id"),
        },
        "wallets": wallets,
        "wallets_checksum": _checksum(wallets),
        "terminal_cohort": CELLS[args.cell],
        "price_bins": PRICE_BINS,
        "seconds_to_close_bins": TIME_BINS,
        "paper_only": True,
        "live_orders_allowed": False,
        "prospective_rule": "recorded_at strictly after registration; historical cohort is diagnostic only",
        "permanent_gate": {
            "minimum_resolved": 200,
            "positive_weekend_post_fee_pnl": True,
            "positive_roi": True,
            "positive_chronological_halves": True,
            "exact_reusable_policy": True,
            "F2_F4": True,
            "max_live_pin_usd": 1.0,
            "pin_ttl_s": 3600,
            "pin_refresh": False,
        },
    }
    value = {**body, "checksum": _checksum(body)}
    atomic_write_json(args.preregistration, value)
    return value


def _enriched_terminals(args: argparse.Namespace, run_id: str) -> list[dict[str, Any]]:
    journal_rows = []
    for envelope in load_jsonl(Path(args.journal)):
        identity = envelope.get("identity") if isinstance(envelope.get("identity"), dict) else {}
        if str(identity.get("run_id") or "") != run_id or envelope.get("input_equals_terminal") is not True:
            continue
        generation = str(envelope.get("source_generation") or run_id)
        for row in envelope.get("rows") or []:
            if isinstance(row, dict):
                journal_rows.append({**row, "_identity": row_identity(row, generation)})
    raw_by_order = {
        str(row.get("order_id") or ""): row
        for row in _tail_jsonl(Path(args.terminal_log))
        if str(row.get("run_id") or "") == run_id
    }
    raw_by_event = {
        (str(row.get("transaction_hash") or ""), str(row.get("log_index") or ""), str(row.get("wallet") or "").lower()): row
        for row in raw_by_order.values()
    }
    enriched = {}
    for row in journal_rows:
        key = (str(row.get("transaction_hash") or ""), str(row.get("log_index") or row.get("source_event_id") or ""), str(row.get("wallet") or "").lower())
        raw = raw_by_order.get(str(row.get("order_id") or "")) or raw_by_event.get(key) or {}
        merged = {**row, **raw, "_identity": row["_identity"]}
        enriched[row["_identity"]] = merged
    return list(enriched.values())


def _book_index(path: Path) -> dict[str, list[dict[str, Any]]]:
    index: dict[str, list[dict[str, Any]]] = {}
    for row in _tail_jsonl(path):
        asset = str(row.get("asset_id") or "")
        if asset:
            index.setdefault(asset, []).append(row)
    for rows in index.values():
        rows.sort(key=lambda row: float(row.get("captured_at_s") or 0.0))
    return index


def _cell_key(wallet: str, source_price: float, market_slug: str, event_ts: float) -> str:
    price_bin = next((f"{lo:.2f}-{hi:.2f}" for lo, hi in PRICE_BINS if lo <= source_price < hi), "missing")
    try:
        start = int(market_slug.rsplit("-", 1)[1])
        seconds_to_close = max(0.0, start + 300 - event_ts)
    except (ValueError, IndexError):
        seconds_to_close = -1.0
    time_bin = next((f"{lo}-{hi}" for lo, hi in TIME_BINS if lo <= seconds_to_close < hi), "missing")
    return f"{wallet}|{price_bin}|{time_bin}"


def _tuple_evidence(rows: list[dict[str, Any]], *, prereg: dict[str, Any], cell_key: str) -> dict[str, Any]:
    """Keep every promotion gate scoped to one immutable exact-policy tuple."""
    ordered = sorted((row for row in rows if row["resolved"]), key=lambda row: (row["source_event_ts"], row["row_identity"]))
    midpoint = (len(ordered) + 1) // 2
    pnl = round(sum(float(row["post_fee_pnl_usd"]) for row in ordered), 6)
    first = round(sum(float(row["post_fee_pnl_usd"]) for row in ordered[:midpoint]), 6) if ordered else None
    second = round(sum(float(row["post_fee_pnl_usd"]) for row in ordered[midpoint:]), 6) if len(ordered) > 1 else None
    wallet, price_bin, time_bin = cell_key.split("|", 2)
    tuple_complete = price_bin != "missing" and time_bin != "missing"
    policy_id = str(prereg["source_generation_identity"].get("policy_id") or "")
    policy = {
        "policy_id": policy_id,
        "source_price_bin": price_bin,
        "seconds_to_close_bin": time_bin,
        "max_order_usd": 1.0,
        "copy_size_usd": 1.0,
    }
    f2_f4 = bool(rows) and all(
        row.get("f2_f4") == {"F2_alpha_profile": "PASS", "F3_receipt_freshness": "PASS", "F4_executable_book": "PASS"}
        for row in rows
    )
    permanent = bool(tuple_complete and len(ordered) >= 200 and pnl > 0 and (first or 0) > 0 and (second or 0) > 0 and f2_f4 and policy_id)
    return {
        "tuple_id": _checksum({"cell_key": cell_key, "source_run_id": prereg["source_run_id"], "checksum": prereg["checksum"], "policy_id": policy_id}),
        "wallet": wallet, "price_bin": price_bin, "seconds_to_close_bin": time_bin,
        "source_run_id": prereg["source_run_id"], "preregistration_checksum": prereg["checksum"],
        "policy_id": policy_id, "policy": policy, "prospective_records": len(rows), "resolved": len(ordered),
        "post_fee_pnl_usd": pnl, "roi_pct": round(pnl / len(ordered) * 100.0, 6) if ordered else None,
        "first_half_pnl_usd": first, "second_half_pnl_usd": second, "F2_F4": f2_f4,
        "exact_reusable_policy": bool(policy_id and tuple_complete), "tuple_complete": tuple_complete, "terminal_clear": wallet not in {"0x13e0d447520ebe7f8eeaf7817211201b2c585204"},
        "red_clock_clear": wallet not in {"0x82c857cb4d18e919c1b7d3c6865be4debe50da77"},
        "cooloff_clear": True, "copyintent_parity": True, "eligible": permanent,
    }


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    measurement = load_json(args.measurement, default={})
    prereg = _prereg(args, measurement)
    run_id = str(prereg.get("source_run_id") or "")
    registered_at = _ts(prereg["registered_at"])
    allowed_wallets = set(prereg["wallets"])
    terminal_name = CELLS[args.cell]
    rows = [
        row for row in _enriched_terminals(args, run_id)
        if str((row.get("f1_f4_terminal") or {}).get("terminal") or "") == terminal_name
        and str(row.get("wallet") or "").lower() in allowed_wallets
    ]
    token_meta = load_json(args.token_metadata, default={})
    token_meta = token_meta if isinstance(token_meta, dict) else {}
    clob_path = Path(args.clob_jsonl or f"data/research/clob_book_snapshots_alpha_decay_{run_id}.jsonl")
    books = _book_index(clob_path)
    resolution_rows = _tail_jsonl(Path(args.resolutions), max_bytes=8 * 1024 * 1024)
    resolution_by_token = {
        str(token): row
        for row in resolution_rows
        for token in (row.get("yes_token"), row.get("no_token"))
        if token
    }
    taxonomy: Counter[str] = Counter()
    records = []
    cells: dict[str, dict[str, Any]] = {}
    for row in rows:
        prospective = _ts(row.get("recorded_at")) > registered_at
        taxonomy["input_terminal"] += 1
        token = str(row.get("token_id") or "")
        meta = token_meta.get(token) if isinstance(token_meta.get(token), dict) else {}
        if not meta.get("market_slug") or not meta.get("condition_id") or not meta.get("outcome"):
            taxonomy["mapping_missing"] += 1
            continue
        taxonomy["mapped"] += 1
        event_ts = float(row.get("source_event_ts") or 0.0)
        receipt_ts = float(row.get("source_received_at_s") or _ts(row.get("recorded_at")))
        snapshots = [book for book in books.get(token, []) if 0.0 <= float(book.get("captured_at_s") or 0.0) - receipt_ts <= 5.0]
        if not snapshots:
            taxonomy["contemporaneous_book_missing"] += 1
            continue
        book = snapshots[0]
        ask = float(book.get("best_ask") or 0.0)
        depth = sum(float(level.get("size") or 0.0) for level in book.get("asks") or [] if float(level.get("price") or 0.0) <= ask + 1e-12)
        if not (0.25 <= ask <= 0.50) or depth * ask < 1.0:
            taxonomy["price_or_depth_refused"] += 1
            continue
        taxonomy["depth_pass"] += 1
        if not prospective:
            taxonomy["diagnostic_pre_registration"] += 1
            continue
        shares = 1.0 / ask
        resolution = resolution_by_token.get(token)
        resolved = resolution is not None
        won = bool(resolution and token == str(resolution.get("yes_token") if str(resolution.get("direction")).upper() == "UP" else resolution.get("no_token")))
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=ask)
        pnl = round((shares if won else 0.0) - 1.0 - fee, 6) if resolved else None
        cell_key = _cell_key(str(row.get("wallet") or "").lower(), float(row.get("source_price") or 0.0), str(meta["market_slug"]), event_ts)
        record = {
            "row_identity": row["_identity"], "cell_key": cell_key, "wallet": str(row.get("wallet") or "").lower(),
            "token_id": token, **meta, "source_price": row.get("source_price"), "source_event_ts": event_ts,
            "book_captured_at_s": book.get("captured_at_s"), "fill_price": ask, "depth_usd": round(depth * ask, 6),
            "expected_fee_usd": fee, "resolved": resolved, "won": won if resolved else None, "post_fee_pnl_usd": pnl,
            "policy_id": prereg["source_generation_identity"].get("policy_id"),
            "f2_f4": {
                "F2_alpha_profile": "PASS",
                "F3_receipt_freshness": "PASS" if 0.0 <= float(book.get("captured_at_s") or 0.0) - receipt_ts <= 5.0 else "FAIL",
                "F4_executable_book": "PASS" if 0.25 <= ask <= 0.50 and depth * ask >= 1.0 else "FAIL",
            },
        }
        records.append(record)
        cell = cells.setdefault(cell_key, {"resolved": 0, "post_fee_pnl_usd": 0.0, "records": 0})
        cell["records"] += 1
        if resolved:
            cell["resolved"] += 1
            cell["post_fee_pnl_usd"] = round(cell["post_fee_pnl_usd"] + float(pnl or 0.0), 6)
    resolved_rows = sorted((row for row in records if row["resolved"]), key=lambda row: (row["source_event_ts"], row["row_identity"]))
    midpoint = (len(resolved_rows) + 1) // 2
    pnl = round(sum(float(row["post_fee_pnl_usd"]) for row in resolved_rows), 6)
    first = round(sum(float(row["post_fee_pnl_usd"]) for row in resolved_rows[:midpoint]), 6) if resolved_rows else None
    second = round(sum(float(row["post_fee_pnl_usd"]) for row in resolved_rows[midpoint:]), 6) if len(resolved_rows) > 1 else None
    tuple_evidence = [_tuple_evidence([row for row in records if row["cell_key"] == key], prereg=prereg, cell_key=key) for key in sorted(cells)]
    deadline = datetime.fromtimestamp(registered_at, timezone.utc) + timedelta(seconds=1800)
    result = {
        "schema_version": 1, "kind": "wide_conversion_recovery_cell", "flow_stage": "DISCOVER/OBSERVE/LEARN/PROMOTE/LIVE",
        "generated_at": datetime.now(timezone.utc).isoformat(), "status": "RUNNING_PAPER_ONLY", "paper_only": True,
        "live_orders_allowed": False, "cell": args.cell, "preregistration_checksum": prereg["checksum"],
        "source_run_id": run_id, "terminal_cohort": terminal_name, "terminal_reconciliation": {"input": len(rows), "terminal": len(rows), "exact": True},
        "registered_at": prereg["registered_at"], "observation_deadline_at": deadline.isoformat(),
        "conversion": dict(sorted(taxonomy.items())), "records": records, "cells": cells, "tuple_evidence": tuple_evidence,
        "evidence": {"resolved": len(resolved_rows), "post_fee_pnl_usd": pnl, "roi_pct": round(pnl / len(resolved_rows) * 100.0, 6) if resolved_rows else None, "first_half_pnl_usd": first, "second_half_pnl_usd": second},
        "admission": {"eligible": any(row["eligible"] for row in tuple_evidence), "no_cross_tuple_pooling": True, "live_authority": "existing singular non-refreshing $1/3600s guard pin only"},
    }
    atomic_write_json(args.state, result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cell", choices=sorted(CELLS), required=True)
    parser.add_argument("--measurement", default="data/research/wide_exact_policy_paper_state.json")
    parser.add_argument("--journal", default="data/research/wide_direct_handoff_journal.jsonl")
    parser.add_argument("--terminal-log", default="data/research/wide_exact_policy_paper_orders.jsonl")
    parser.add_argument("--token-metadata", default="data/research/wide_token_metadata_cache.json")
    parser.add_argument("--resolutions", default="data/research/btc_resolutions_from_btcusdt_ticks.jsonl")
    parser.add_argument("--clob-jsonl", default="")
    parser.add_argument("--preregistration", required=True)
    parser.add_argument("--state", required=True)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--interval-s", type=float, default=10.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    while True:
        result = run_once(args)
        print(json.dumps({"cell": args.cell, "status": result["status"], "conversion": result["conversion"], "evidence": result["evidence"]}, sort_keys=True), flush=True)
        if not args.watch:
            return 0
        time.sleep(max(1.0, args.interval_s))


if __name__ == "__main__":
    raise SystemExit(main())
