#!/usr/bin/env python3
"""Probe CLOB best-ask route health for recent inventory skip windows."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import sys
import time
from pathlib import Path
from typing import Any
import re

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402

DEFAULT_GUARD_STATE = ROOT / "data/research/wallet_copy_live_guard_state.json"
DEFAULT_WALLET_EVENTS = ROOT / "data/research/wallet_copy_live_guard_wallet_events.jsonl"
DEFAULT_OUTPUT = ROOT / "data/research/inventory_best_ask_route_probe_latest.json"
BOOK_REASONS = {"inventory_best_ask_book_error", "inventory_best_ask_missing"}
_SLUG_START_RE = re.compile(r"(\d{9,})$")


def _display(path: Path) -> str:
    try:
        return str(path.relative_to(ROOT))
    except ValueError:
        return str(path)


def _iter_jsonl(path: Path, *, tail_bytes: int = 0):
    if tail_bytes > 0 and path.exists() and path.stat().st_size > tail_bytes:
        with path.open("rb") as handle:
            handle.seek(-tail_bytes, os.SEEK_END)
            handle.readline()
            for raw in handle:
                try:
                    yield json.loads(raw.decode("utf-8", errors="ignore"))
                except json.JSONDecodeError:
                    continue
        return
    if not path.exists():
        return
    with path.open("r", encoding="utf-8", errors="ignore") as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def _best_ask(book: dict[str, Any]) -> float:
    asks = [row for row in book.get("asks") or [] if isinstance(row, dict)]
    prices = [num(row.get("price")) for row in asks]
    prices = [price for price in prices if price > 0.0]
    return min(prices) if prices else 0.0


def _target_rollups(guard_state: dict[str, Any], *, limit: int) -> list[dict[str, Any]]:
    participation = guard_state.get("window_participation") if isinstance(guard_state, dict) else {}
    rollups = participation.get("window_rollups") if isinstance(participation, dict) else []
    targets: list[dict[str, Any]] = []
    for row in rollups if isinstance(rollups, list) else []:
        if not isinstance(row, dict):
            continue
        counts = row.get("dominant_skip_reason_counts") if isinstance(row.get("dominant_skip_reason_counts"), dict) else {}
        if not any(int(counts.get(reason) or 0) > 0 for reason in BOOK_REASONS):
            continue
        targets.append(row)
    return targets[: max(0, int(limit))]


def _window_start_s(market_slug: str) -> float | None:
    match = _SLUG_START_RE.search(str(market_slug or ""))
    if not match:
        return None
    return float(match.group(1))


def _event_key(row: dict[str, Any]) -> tuple[str, str, str]:
    return (
        str(row.get("market_slug") or row.get("event_slug") or ""),
        str(row.get("source_wallet") or "").lower(),
        str(row.get("outcome") or ""),
    )


def _events_by_rollup_key(wallet_events: Path, *, tail_bytes: int) -> dict[tuple[str, str, str], dict[str, Any]]:
    out: dict[tuple[str, str, str], dict[str, Any]] = {}
    for row in _iter_jsonl(wallet_events, tail_bytes=tail_bytes):
        if not isinstance(row, dict):
            continue
        key = _event_key(row)
        if not key[0] or not key[1] or not key[2] or not row.get("token_id"):
            continue
        current = out.get(key)
        ts = float(row.get("observed_ts") or row.get("event_ts") or 0.0)
        current_ts = float(current.get("observed_ts") or current.get("event_ts") or 0.0) if current else -1.0
        if current is None or ts >= current_ts:
            out[key] = row
    return out


def _probe_direct(token_id: str, *, timeout_s: float) -> dict[str, Any]:
    started = time.perf_counter()
    try:
        response = requests.get(
            f"{CLOBMarketClient.DIRECT_CLOB_HOST}/book",
            params={"token_id": token_id},
            timeout=max(0.1, float(timeout_s)),
            headers={"Accept": "application/json", "User-Agent": "inventory-best-ask-route-probe/1.0"},
        )
        status_code = response.status_code
        response.raise_for_status()
        payload = response.json()
        book = payload if isinstance(payload, dict) else {}
        best_ask = _best_ask(book)
        return {
            "route": "direct_clob_requests",
            "status": "PASS" if best_ask > 0.0 else "NO_ASKS",
            "http_status": status_code,
            "best_ask": round(best_ask, 6),
            "asks": len(book.get("asks") or []) if isinstance(book.get("asks"), list) else 0,
            "duration_s": round(time.perf_counter() - started, 6),
        }
    except Exception as exc:
        return {
            "route": "direct_clob_requests",
            "status": "ERROR",
            "error": f"{type(exc).__name__}: {exc}"[:500],
            "duration_s": round(time.perf_counter() - started, 6),
        }


def _probe_client(token_id: str, *, timeout_s: float) -> dict[str, Any]:
    started = time.perf_counter()
    client = CLOBMarketClient(timeout_s=max(0.1, float(timeout_s)), retries=2)
    try:
        book = client.get_book(token_id)
        best_ask = _best_ask(book if isinstance(book, dict) else {})
        return {
            "route": "clob_market_client",
            "status": "PASS" if best_ask > 0.0 else "NO_ASKS",
            "best_ask": round(best_ask, 6),
            "asks": len(book.get("asks") or []) if isinstance(book, dict) and isinstance(book.get("asks"), list) else 0,
            "route_report": client.last_route_report,
            "duration_s": round(time.perf_counter() - started, 6),
        }
    except Exception as exc:
        return {
            "route": "clob_market_client",
            "status": "ERROR",
            "route_report": client.last_route_report,
            "error": f"{type(exc).__name__}: {exc}"[:500],
            "duration_s": round(time.perf_counter() - started, 6),
        }


def build_probe(
    *,
    guard_state_path: Path,
    wallet_events: Path,
    output: Path,
    limit: int,
    timeout_s: float,
    tail_bytes: int,
) -> dict[str, Any]:
    guard_state = load_json(guard_state_path, default={})
    guard_state = guard_state if isinstance(guard_state, dict) else {}
    rollups = _target_rollups(guard_state, limit=limit)
    event_lookup = _events_by_rollup_key(wallet_events, tail_bytes=tail_bytes)
    targets: list[dict[str, Any]] = []
    seen_tokens: set[str] = set()
    for rollup in rollups:
        outcomes = [str(item) for item in rollup.get("outcomes") or [] if str(item or "")]
        outcome = outcomes[0] if len(set(outcomes)) == 1 else ""
        key = (
            str(rollup.get("market_slug") or ""),
            str(rollup.get("source_wallet") or "").lower(),
            outcome,
        )
        event = event_lookup.get(key)
        token_id = str(event.get("token_id") or "") if isinstance(event, dict) else ""
        if not token_id or token_id in seen_tokens:
            continue
        window_start_s = _window_start_s(key[0])
        window_close_s = None if window_start_s is None else window_start_s + 300.0
        probe_after_close_s = None if window_close_s is None else time.time() - window_close_s
        seen_tokens.add(token_id)
        targets.append(
            {
                "market_slug": key[0],
                "source_wallet": key[1],
                "outcome": outcome,
                "token_id": token_id,
                "window_start_s": window_start_s,
                "window_close_s": window_close_s,
                "probe_after_close_s": None if probe_after_close_s is None else round(probe_after_close_s, 6),
                "probe_after_market_close": bool(probe_after_close_s is not None and probe_after_close_s > 0.0),
                "dominant_skip_reason_counts": rollup.get("dominant_skip_reason_counts"),
                "event_price": event.get("price") if isinstance(event, dict) else None,
                "event_ts": event.get("event_ts") if isinstance(event, dict) else None,
                "observed_ts": event.get("observed_ts") if isinstance(event, dict) else None,
            }
        )
    probe_rows: list[dict[str, Any]] = []
    for target in targets:
        token_id = str(target["token_id"])
        probe_rows.append(
            {
                **target,
                "flow_stage": "LIVE/LEARN",
                "direct": _probe_direct(token_id, timeout_s=timeout_s),
                "client": _probe_client(token_id, timeout_s=timeout_s),
            }
        )
    direct_passes = sum(1 for row in probe_rows if row["direct"].get("status") == "PASS")
    client_passes = sum(1 for row in probe_rows if row["client"].get("status") == "PASS")
    stale_no_book = sum(
        1
        for row in probe_rows
        if row.get("probe_after_market_close")
        and row["direct"].get("status") == "ERROR"
        and "404" in str(row["direct"].get("error") or "")
        and row["client"].get("status") == "NO_ASKS"
    )
    status = "NO_TARGET_TOKENS"
    if probe_rows:
        if direct_passes == len(probe_rows) or client_passes == len(probe_rows):
            status = "PASS_ROUTE_HEALTHY"
        elif stale_no_book == len(probe_rows):
            status = "PASS_STALE_TARGETS_CLASSIFIED"
        else:
            status = "ANALYZE_ROUTE_ERRORS"
    report = {
        "schema_version": 1,
        "kind": "inventory_best_ask_route_probe",
        "flow_stage": "LIVE/LEARN",
        "generated_at": utc_now_iso(),
        "status": status,
        "guard_state": _display(guard_state_path),
        "wallet_events": _display(wallet_events),
        "target_rollups": len(rollups),
        "target_tokens": len(probe_rows),
        "summary": {
            "direct_passes": direct_passes,
            "client_passes": client_passes,
            "stale_no_book_classified": stale_no_book,
            "book_error_or_missing_reasons": sorted(BOOK_REASONS),
        },
        "rows": probe_rows,
        "live_orders_allowed": False,
        "paper_only": True,
        "next_action": (
            "book route healthy now; keep gate unchanged and watch for recurring route_error sample_decisions"
            if status == "PASS_ROUTE_HEALTHY"
            else "historical target tokens are closed/no-book now; rerun on next fresh in-band skip before gate changes"
            if status == "PASS_STALE_TARGETS_CLASSIFIED"
            else "repair CLOB route or classify no-ask liquidity before changing trading gates"
        ),
    }
    atomic_write_json(output, report)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--guard-state", default=str(DEFAULT_GUARD_STATE))
    parser.add_argument("--wallet-events", default=str(DEFAULT_WALLET_EVENTS))
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--limit", type=int, default=8)
    parser.add_argument("--timeout-s", type=float, default=2.0)
    parser.add_argument("--tail-bytes", type=int, default=128 * 1024 * 1024)
    args = parser.parse_args(argv)
    report = build_probe(
        guard_state_path=Path(args.guard_state),
        wallet_events=Path(args.wallet_events),
        output=Path(args.output),
        limit=int(args.limit),
        timeout_s=float(args.timeout_s),
        tail_bytes=int(args.tail_bytes),
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
