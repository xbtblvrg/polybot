#!/usr/bin/env python3
"""Capture Data API first-seen timestamps for Polygon WS registry wallets.

Read-only measurement helper: does not create CopyIntents and does not touch
paper/live execution state.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import append_jsonl


DEFAULT_DATA_API = os.getenv("POLYMARKET_DATA_API_BASE_URL", "http://127.0.0.1:8787/data-api")
DEFAULT_POLYGON_JSONL = "data/research/polygon_orderfilled_ws_capture_corrected_smoke.jsonl"
DEFAULT_OUTPUT = "data/research/dataapi_first_seen.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data-api-base-url", default=DEFAULT_DATA_API)
    parser.add_argument("--polygon-jsonl", default=DEFAULT_POLYGON_JSONL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--duration-s", type=float, default=1800.0)
    parser.add_argument("--interval-s", type=float, default=5.0)
    parser.add_argument("--timeout-s", type=float, default=2.0)
    parser.add_argument("--limit", type=int, default=100)
    parser.add_argument("--max-wallets", type=int, default=40)
    parser.add_argument("--wallet", action="append", default=[])
    parser.add_argument("--endpoint", action="append", default=["/activity"])
    parser.add_argument("--query-key", action="append", default=["user"])
    return parser.parse_args()


def _iter_jsonl(path: str) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    with target.open(encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x") and len(text) == 42:
        return text
    return ""


def _load_seen(path: str) -> set[tuple[str, str]]:
    seen: set[tuple[str, str]] = set()
    for row in _iter_jsonl(path):
        if row.get("event") != "dataapi_first_seen":
            continue
        wallet = _norm_wallet(row.get("wallet"))
        tx = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
        if wallet and tx:
            seen.add((wallet, tx))
    return seen


def _wallets_from_polygon(path: str, explicit_wallets: list[str], max_wallets: int) -> list[str]:
    wallets: list[str] = []
    seen: set[str] = set()
    for wallet in explicit_wallets:
        normalized = _norm_wallet(wallet)
        if normalized and normalized not in seen:
            wallets.append(normalized)
            seen.add(normalized)
    for row in _iter_jsonl(path):
        if row.get("event") != "polygon_orderfilled_log" or row.get("source") != "polygon_ws":
            continue
        for wallet in row.get("registry_wallets") or [row.get("selected_wallet")]:
            normalized = _norm_wallet(wallet)
            if normalized and normalized not in seen:
                wallets.append(normalized)
                seen.add(normalized)
                if len(wallets) >= max_wallets:
                    return wallets
    return wallets[:max_wallets]


def _rotated_wallets(wallets: list[str], start_offset: int) -> list[str]:
    if not wallets:
        return []
    offset = start_offset % len(wallets)
    return wallets[offset:] + wallets[:offset]


def _rows_from_response(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [row for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        data = payload.get("data")
        if isinstance(data, list):
            return [row for row in data if isinstance(row, dict)]
    return []


def _fetch_wallet_rows(
    session: requests.Session,
    base_url: str,
    wallet: str,
    *,
    endpoint: str,
    query_key: str,
    limit: int,
    timeout_s: float,
) -> list[dict[str, Any]]:
    response = session.get(
        base_url.rstrip("/") + endpoint,
        params={query_key: wallet, "limit": int(limit)},
        timeout=timeout_s,
        headers={"Accept": "application/json", "User-Agent": "wallet-copy-dataapi-first-seen/1.0"},
    )
    response.raise_for_status()
    return _rows_from_response(response.json())


def main() -> int:
    args = parse_args()
    poller_started_at_s = time.time()
    deadline = time.time() + max(1.0, float(args.duration_s))
    seen = _load_seen(args.output)
    session = requests.Session()
    cycles = 0
    rotation_offset = 0
    rows_written = 0
    errors: list[dict[str, Any]] = []
    while time.time() < deadline:
        cycle_started = time.time()
        cycles += 1
        wallets = _wallets_from_polygon(args.polygon_jsonl, args.wallet or [], int(args.max_wallets))
        cycle_wallets_polled: list[str] = []
        for wallet in _rotated_wallets(wallets, rotation_offset):
            if time.time() - cycle_started > max(1.0, float(args.interval_s) * 0.9):
                break
            wallet_polled = False
            for endpoint in args.endpoint or ["/activity"]:
                for query_key in args.query_key or ["user"]:
                    if time.time() - cycle_started > max(1.0, float(args.interval_s) * 0.9):
                        break
                    try:
                        rows = _fetch_wallet_rows(
                            session,
                            args.data_api_base_url,
                            wallet,
                            endpoint=endpoint,
                            query_key=query_key,
                            limit=int(args.limit),
                            timeout_s=float(args.timeout_s),
                        )
                    except Exception as exc:  # noqa: BLE001 - measurement evidence.
                        errors.append(
                            {
                                "event": "dataapi_first_seen_error",
                                "captured_at_s": time.time(),
                                "captured_at_iso": utc_now_iso(),
                                "wallet": wallet,
                                "endpoint": endpoint,
                                "query_key": query_key,
                                "error_type": type(exc).__name__,
                                "error": str(exc),
                            }
                        )
                        continue
                    wallet_polled = True
                    observed_at_s = time.time()
                    for row in rows:
                        tx = str(row.get("transactionHash") or row.get("transaction_hash") or "").lower()
                        if not tx or (wallet, tx) in seen:
                            continue
                        try:
                            dataapi_event_ts = float(row.get("timestamp"))
                        except (TypeError, ValueError):
                            dataapi_event_ts = None
                        seen.add((wallet, tx))
                        rows_written += 1
                        append_jsonl(
                            args.output,
                            {
                                "event": "dataapi_first_seen",
                                "captured_at_s": observed_at_s,
                                "captured_at_iso": utc_now_iso(),
                                "wallet": wallet,
                                "transactionHash": tx,
                                "poller_started_at_s": poller_started_at_s,
                                "poll_interval_s": float(args.interval_s),
                                "backfill": (
                                    dataapi_event_ts is not None
                                    and dataapi_event_ts < poller_started_at_s
                                ),
                                "endpoint": endpoint,
                                "query_key": query_key,
                                "side": row.get("side"),
                                "price": row.get("price"),
                                "size": row.get("size"),
                                "timestamp": row.get("timestamp"),
                                "raw": row,
                            },
                        )
            if wallet_polled:
                cycle_wallets_polled.append(wallet)
        if wallets and cycle_wallets_polled:
            rotation_offset = (rotation_offset + len(cycle_wallets_polled)) % len(wallets)
        append_jsonl(
            args.output,
            {
                "event": "dataapi_first_seen_cycle",
                "captured_at_s": time.time(),
                "captured_at_iso": utc_now_iso(),
                "cycle": cycles,
                "wallets_seen": len(wallets),
                "wallets_polled": cycle_wallets_polled,
                "next_rotation_offset": rotation_offset,
                "poller_started_at_s": poller_started_at_s,
                "paper_only": True,
                "live_orders_allowed": False,
            },
        )
        sleep_s = max(0.0, float(args.interval_s) - (time.time() - cycle_started))
        if sleep_s:
            time.sleep(sleep_s)
    summary = {
        "event": "dataapi_first_seen_summary",
        "captured_at_s": time.time(),
        "captured_at_iso": utc_now_iso(),
        "polygon_jsonl": args.polygon_jsonl,
        "output": args.output,
        "cycles": cycles,
        "rows_written": rows_written,
        "poller_started_at_s": poller_started_at_s,
        "poll_interval_s": float(args.interval_s),
        "next_rotation_offset": rotation_offset,
        "wallets_seen": len(_wallets_from_polygon(args.polygon_jsonl, args.wallet or [], int(args.max_wallets))),
        "errors": errors[-20:],
        "paper_only": True,
        "live_orders_allowed": False,
    }
    append_jsonl(args.output, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if not errors else 2


if __name__ == "__main__":
    raise SystemExit(main())
