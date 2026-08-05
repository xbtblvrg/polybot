#!/usr/bin/env python3
"""Backfill Polygon receipts for wallet-copy event-log tx hashes.

This is a bounded verifier, not a trading process. It builds a reusable receipt
cache so older event-log rows that were written before onchain receipts were
enabled can still gain settlement evidence.
"""

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

from src.wallet_copy.live_tracker import OnchainReceiptClient
from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import atomic_write_json, load_json


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-log", action="append", default=[])
    parser.add_argument("--state", default="data/research/wallet_copy_onchain_receipts_state.json")
    parser.add_argument("--polygon-rpc-url", default="https://polygon-bor-rpc.publicnode.com")
    parser.add_argument("--timeout-s", type=float, default=1.0)
    parser.add_argument("--max-hashes", type=int, default=25)
    parser.add_argument("--retry-after-s", type=float, default=900.0)
    return parser.parse_args()


def _addr(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip().lower()
        if text.startswith("0x") and len(text) >= 42:
            return text[:42]
    return ""


def _tx(value: Any) -> str:
    if isinstance(value, str):
        text = value.strip().lower()
        if text.startswith("0x") and len(text) >= 10:
            return text
    return ""


def collect_hashes(paths: list[str]) -> dict[str, dict[str, Any]]:
    hashes: dict[str, dict[str, Any]] = {}
    for path in paths:
        p = Path(path)
        if not p.exists():
            continue
        with p.open(encoding="utf-8", errors="ignore") as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(row, dict):
                    continue
                wallet_event = row.get("wallet_event") if isinstance(row.get("wallet_event"), dict) else {}
                raw = wallet_event.get("raw") if isinstance(wallet_event.get("raw"), dict) else {}
                tx_hash = _tx(row.get("tx_hash")) or _tx(wallet_event.get("transaction_hash")) or _tx(raw.get("transactionHash"))
                if not tx_hash:
                    continue
                wallet = (
                    _addr(row.get("source_wallet"))
                    or _addr(wallet_event.get("source_wallet"))
                    or _addr(raw.get("proxyWallet"))
                    or _addr((row.get("copy_efficiency") or {}).get("source_wallet"))
                )
                entry = hashes.setdefault(
                    tx_hash,
                    {
                        "tx_hash": tx_hash,
                        "source_wallets": set(),
                        "event_logs": set(),
                        "sample_market_slugs": set(),
                        "sample_condition_ids": set(),
                    },
                )
                if wallet:
                    entry["source_wallets"].add(wallet)
                entry["event_logs"].add(str(p))
                market_slug = str(row.get("market_slug") or wallet_event.get("market_slug") or "")
                if market_slug:
                    entry["sample_market_slugs"].add(market_slug)
                condition_id = str(row.get("condition_id") or wallet_event.get("condition_id") or "")
                if condition_id:
                    entry["sample_condition_ids"].add(condition_id)
    return hashes


def _jsonable_source(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "tx_hash": entry.get("tx_hash"),
        "source_wallets": sorted(entry.get("source_wallets") or []),
        "event_logs": sorted(entry.get("event_logs") or []),
        "sample_market_slugs": sorted(entry.get("sample_market_slugs") or [])[:5],
        "sample_condition_ids": sorted(entry.get("sample_condition_ids") or [])[:5],
    }


def main() -> int:
    args = parse_args()
    event_logs = args.event_log or [
        "data/research/wallet_copy_active_hotlane_live_tracking_events.jsonl",
        "data/research/wallet_copy_live_tracking_events.jsonl",
        "data/research/wallet_copy_registry_sweep_live_tracking_events.jsonl",
    ]
    prior = load_json(args.state, default={})
    receipts = prior.get("receipts") if isinstance(prior.get("receipts"), dict) else {}
    now = time.time()
    hashes = collect_hashes(event_logs)
    client = OnchainReceiptClient(args.polygon_rpc_url, timeout_s=float(args.timeout_s))
    checked = 0
    status_counts: Counter[str] = Counter()
    errors: list[dict[str, Any]] = []
    for tx_hash, source in hashes.items():
        cached = receipts.get(tx_hash) if isinstance(receipts.get(tx_hash), dict) else {}
        last_checked = float(cached.get("checked_unix_ts") or 0.0)
        if cached.get("status") == "CONFIRMED":
            status_counts["CONFIRMED_CACHED"] += 1
            continue
        if last_checked and now - last_checked < float(args.retry_after_s):
            status_counts[str(cached.get("status") or "CACHED_RECENT")] += 1
            continue
        if checked >= max(0, int(args.max_hashes)):
            status_counts["DEFERRED_MAX_HASHES"] += 1
            continue
        source_payload = _jsonable_source(source)
        wallet_for_summary = (source_payload.get("source_wallets") or [""])[0]
        try:
            receipt = client.get_receipt(tx_hash)
            summary = client.summarize_receipt(receipt, tx_hash=tx_hash, wallet=wallet_for_summary)
            status = str(summary.get("status") or "UNKNOWN")
            receipts[tx_hash] = {
                **source_payload,
                **summary,
                "checked_at": utc_now_iso(),
                "checked_unix_ts": now,
                "rpc_url": args.polygon_rpc_url,
            }
            status_counts[status] += 1
        except Exception as exc:  # pragma: no cover - network dependent
            receipts[tx_hash] = {
                **source_payload,
                "status": "ERROR",
                "error": str(exc),
                "checked_at": utc_now_iso(),
                "checked_unix_ts": now,
                "rpc_url": args.polygon_rpc_url,
            }
            status_counts["ERROR"] += 1
            errors.append({"tx_hash": tx_hash, "error": str(exc)})
        checked += 1
    all_status_counts = Counter(str(row.get("status") or "UNKNOWN") for row in receipts.values() if isinstance(row, dict))
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_onchain_receipts_state",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "event_logs": event_logs,
        "tx_hashes_seen": len(hashes),
        "checked_this_run": checked,
        "status_counts_this_run": dict(status_counts),
        "receipt_status_counts": dict(all_status_counts),
        "errors": errors[:20],
        "receipts": receipts,
    }
    atomic_write_json(args.state, payload)
    print(json.dumps({k: v for k, v in payload.items() if k != "receipts"}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
