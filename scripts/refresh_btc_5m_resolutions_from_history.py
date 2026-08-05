#!/usr/bin/env python3
"""Build research BTC 5m resolution rows for wallet-copy history states.

The canonical resolution feed may lag newly onboarded operator wallets. This
script fills that research gap without hiding it: rows are marked as
Binance 1m kline-derived and research-only, so they can unblock paper PnL
measurement while live admission still requires CLOB-backed copy efficiency.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import sys
import urllib.parse
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

import certifi
import requests


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num
from src.wallet_copy.store import load_json


SLUG_TS_RE = re.compile(r"-(\d{10})$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--history-state", action="append", required=True)
    parser.add_argument(
        "--output",
        default="data/research/btc_resolutions_from_binance_1m_research_patch.jsonl",
    )
    parser.add_argument("--summary-output", default=None)
    parser.add_argument("--existing", default=None)
    parser.add_argument("--merge-existing", action="store_true")
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--timeout-s", type=float, default=10.0)
    return parser.parse_args()


def iso_from_ts(ts: int | float) -> str:
    return dt.datetime.fromtimestamp(float(ts), dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def utc_now_iso() -> str:
    return dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def slug_start(slug: str) -> int | None:
    match = SLUG_TS_RE.search(str(slug or ""))
    return int(match.group(1)) if match else None


def load_history_events(paths: list[str]) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for path in paths:
        payload = load_json(path, default={})
        for row in payload.get("events") or []:
            if isinstance(row, dict):
                events.append(row)
    return events


def collect_btc_5m_windows(events: list[dict[str, Any]]) -> dict[int, dict[str, Any]]:
    windows: dict[int, dict[str, Any]] = {}
    outcome_tokens: dict[int, dict[str, str]] = defaultdict(dict)
    condition_ids: dict[int, Counter[str]] = defaultdict(Counter)
    titles: dict[int, Counter[str]] = defaultdict(Counter)
    slugs: dict[int, Counter[str]] = defaultdict(Counter)

    for event in events:
        asset = str(event.get("asset") or "").upper()
        duration = str(event.get("duration") or "").lower()
        market_slug = str(event.get("market_slug") or event.get("event_slug") or "")
        start = slug_start(market_slug)
        if asset != "BTC" or duration != "5m" or start is None:
            continue
        slugs[start][market_slug] += 1
        title = str(event.get("title") or "")
        if title:
            titles[start][title] += 1
        condition_id = str(event.get("condition_id") or event.get("market_id") or "")
        if condition_id:
            condition_ids[start][condition_id] += 1
        outcome = str(event.get("outcome") or "")
        token_id = str(event.get("token_id") or "")
        if outcome.lower() == "up" and token_id:
            outcome_tokens[start]["yes_token"] = token_id
        elif outcome.lower() == "down" and token_id:
            outcome_tokens[start]["no_token"] = token_id

    for start in sorted(slugs):
        slug = slugs[start].most_common(1)[0][0]
        windows[start] = {
            "market_slug": slug,
            "question": titles[start].most_common(1)[0][0] if titles[start] else "",
            "condition_id": condition_ids[start].most_common(1)[0][0] if condition_ids[start] else "",
            **outcome_tokens[start],
        }
    return windows


def fetch_1m_klines(symbol: str, start_s: int, end_s: int, timeout_s: float) -> dict[int, list[Any]]:
    rows: dict[int, list[Any]] = {}
    cursor_ms = int(start_s * 1000)
    end_ms = int(end_s * 1000)
    session = requests.Session()
    while cursor_ms <= end_ms:
        params = {
            "symbol": symbol,
            "interval": "1m",
            "startTime": cursor_ms,
            "endTime": end_ms,
            "limit": 1000,
        }
        response = session.get(
            "https://api.binance.com/api/v3/klines",
            params=params,
            timeout=timeout_s,
            verify=certifi.where(),
            headers={"Accept": "application/json", "User-Agent": "wallet-copy-resolution-refresh/1.0"},
        )
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, list) or not payload:
            break
        last_open_ms = cursor_ms
        for row in payload:
            if not isinstance(row, list) or not row:
                continue
            open_s = int(row[0]) // 1000
            rows[open_s] = row
            last_open_ms = max(last_open_ms, int(row[0]))
        next_cursor = last_open_ms + 60_000
        if next_cursor <= cursor_ms:
            break
        cursor_ms = next_cursor
    return rows


def load_existing_rows(path: str | None) -> list[dict[str, Any]]:
    if not path:
        return []
    p = Path(path)
    if not p.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in p.read_text(encoding="utf-8", errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    return rows


def merge_key(row: dict[str, Any]) -> str:
    market_slug = str(row.get("market_slug") or "")
    if market_slug:
        return f"market_slug:{market_slug}"
    condition_id = str(row.get("condition_id") or "")
    if condition_id and condition_id.startswith("0x"):
        return f"condition_id:{condition_id.lower()}"
    expiry = int(num(row.get("expiry_unix_ts"), 0))
    return f"expiry:{expiry}" if expiry else json.dumps(row, sort_keys=True)


def build_rows(windows: dict[int, dict[str, Any]], klines: dict[int, list[Any]], symbol: str) -> tuple[list[dict[str, Any]], list[int]]:
    rows: list[dict[str, Any]] = []
    missing: list[int] = []
    computed_at = utc_now_iso()
    for start, meta in sorted(windows.items()):
        open_row = klines.get(start)
        close_row = klines.get(start + 240)
        if open_row is None or close_row is None:
            missing.append(start)
            continue
        open_price = float(open_row[1])
        close_price = float(close_row[4])
        delta = close_price - open_price
        direction = "UP" if delta > 0 else "DOWN" if delta < 0 else "TIE"
        rows.append(
            {
                "asset": "BTC",
                "binance_symbol": symbol,
                "btc_close_t": round(close_price, 8),
                "btc_open_t_minus_300": round(open_price, 8),
                "computed_at_iso": computed_at,
                "condition_id": meta.get("condition_id") or "",
                "delta_abs": round(delta, 8),
                "delta_pct": round((delta / open_price) * 100.0, 6) if open_price else 0.0,
                "direction": direction,
                "expiry_iso": iso_from_ts(start + 300),
                "expiry_unix_ts": start + 300,
                "market_slug": meta.get("market_slug") or f"btc-updown-5m-{start}",
                "no_token": meta.get("no_token") or "",
                "question": meta.get("question") or "",
                "research_only": True,
                "resolution_precision": "1m_kline_open_close",
                "source": "binance_spot_1m_kline_research_patch",
                "window_start_iso": iso_from_ts(start),
                "window_start_unix_ts": start,
                "window_type": "5m",
                "yes_token": meta.get("yes_token") or "",
            }
        )
    return rows, missing


def main() -> int:
    args = parse_args()
    events = load_history_events(args.history_state)
    windows = collect_btc_5m_windows(events)
    generated_rows: list[dict[str, Any]] = []
    missing: list[int] = []
    if windows:
        klines = fetch_1m_klines(
            args.symbol,
            min(windows) - 60,
            max(windows) + 600,
            float(args.timeout_s),
        )
        generated_rows, missing = build_rows(windows, klines, args.symbol)

    output_rows = generated_rows
    if args.merge_existing:
        merged = {merge_key(row): row for row in load_existing_rows(args.existing or args.output)}
        for row in generated_rows:
            merged[merge_key(row)] = row
        output_rows = sorted(
            merged.values(),
            key=lambda row: (int(num(row.get("expiry_unix_ts"), 0)), str(row.get("condition_id") or "")),
        )

    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8") as handle:
        for row in output_rows:
            handle.write(json.dumps(row, sort_keys=True, default=str))
            handle.write("\n")

    summary = {
        "history_states": args.history_state,
        "btc_5m_windows_found": len(windows),
        "generated_resolution_rows": len(generated_rows),
        "missing_window_starts": missing,
        "merge_existing": bool(args.merge_existing),
        "output": str(output),
        "output_rows": len(output_rows),
        "paper_research_only": True,
        "source": "binance_spot_1m_kline_research_patch",
    }
    summary_output = Path(args.summary_output) if args.summary_output else output.with_suffix(".summary.json")
    summary_output.parent.mkdir(parents=True, exist_ok=True)
    summary_output.write_text(json.dumps(summary, indent=2, sort_keys=True), encoding="utf-8")
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0 if not missing else 2


if __name__ == "__main__":
    raise SystemExit(main())
