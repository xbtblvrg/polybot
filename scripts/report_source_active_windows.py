#!/usr/bin/env python3
"""Report source-active BTC 5m windows for an active-set wallet."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, parse_ts, utc_now_iso  # noqa: E402
from src.wallet_copy.runtime_paths import DEFAULT_RTDS_ACTIVITY_JSONL  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


DEFAULT_RTDS_JSONL = DEFAULT_RTDS_ACTIVITY_JSONL
DEFAULT_OUTPUT = "data/research/ba8c5fbcc5_source_active_windows_state.json"
DEFAULT_WALLET = "0xba8c5fbcc5f58b0e4ae0c1413e0413f8c803e77d"
DEFAULT_SINCE = "2026-07-05T21:02:00Z"
DEFAULT_TAIL_BYTES = 1_610_612_736
BTC5M_RE = re.compile(r"^btc-updown-5m-(\d+)$")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS_JSONL)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--source-wallet", default=DEFAULT_WALLET)
    parser.add_argument("--since", default=DEFAULT_SINCE)
    parser.add_argument("--min-offset-s", type=float, default=240.0)
    parser.add_argument("--max-offset-s", type=float, default=300.0)
    parser.add_argument("--max-price", type=float, default=0.50)
    parser.add_argument("--required-windows", type=int, default=6)
    parser.add_argument("--tail-bytes", type=int, default=DEFAULT_TAIL_BYTES)
    parser.add_argument("--full-scan", action="store_true")
    return parser.parse_args()


def _iso_from_ts(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _window_start_from_slug(slug: str) -> float | None:
    match = BTC5M_RE.match(str(slug or ""))
    if not match:
        return None
    return float(match.group(1))


def _jsonl_rows(path: str | Path, *, tail_bytes: int | None) -> Iterable[dict[str, Any]]:
    target = Path(path)
    wallet_hint = b"source_wallet"
    with target.open("rb") as handle:
        if tail_bytes and tail_bytes > 0:
            handle.seek(max(0, os.fstat(handle.fileno()).st_size - int(tail_bytes)))
            if handle.tell() > 0:
                handle.readline()
        for raw in handle:
            if wallet_hint not in raw and b"proxyWallet" not in raw:
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(row, dict):
                yield row


def _event_wallet(row: dict[str, Any]) -> str:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return str(row.get("source_wallet") or raw.get("proxyWallet") or "").lower()


def _event_side(row: dict[str, Any]) -> str:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return str(row.get("side") or raw.get("side") or "").upper()


def _event_ts(row: dict[str, Any]) -> float:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return num(row.get("event_ts") or raw.get("timestamp"))


def _event_received_ts(row: dict[str, Any]) -> float:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return num(
        row.get("received_at_s")
        or row.get("captured_at_s")
        or row.get("observed_ts")
        or raw.get("received_at_s")
        or raw.get("captured_at_s")
        or row.get("event_ts")
        or raw.get("timestamp")
    )


def _event_price(row: dict[str, Any]) -> float:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return num(row.get("price") or raw.get("price"))


def _event_size(row: dict[str, Any]) -> float:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return num(row.get("size") or raw.get("size"))


def _event_outcome(row: dict[str, Any]) -> str:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return str(row.get("outcome") or raw.get("outcome") or "")


def _event_tx(row: dict[str, Any]) -> str:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    return str(row.get("transaction_hash") or raw.get("transactionHash") or "")


def _iso_span(rows: list[dict[str, Any]], key: str) -> tuple[str, str]:
    values = [str(item.get(key) or "") for item in rows if item.get(key)]
    if not values:
        return "", ""
    return min(values), max(values)


def _window_summary(slug: str, window_rows: list[dict[str, Any]], max_price: float | None) -> dict[str, Any]:
    policy_rows = [
        item
        for item in window_rows
        if max_price is None or num(item.get("price"), default=999.0) <= float(max_price)
    ]
    first_event_iso, last_event_iso = _iso_span(window_rows, "event_iso")
    first_received_iso, last_received_iso = _iso_span(window_rows, "received_iso")
    first_policy_event_iso, last_policy_event_iso = _iso_span(policy_rows, "event_iso")
    first_policy_received_iso, last_policy_received_iso = _iso_span(policy_rows, "received_iso")
    return {
        "first_event_iso": first_event_iso,
        "first_policy_event_iso": first_policy_event_iso,
        "first_policy_received_iso": first_policy_received_iso,
        "first_received_iso": first_received_iso,
        "last_event_iso": last_event_iso,
        "last_policy_event_iso": last_policy_event_iso,
        "last_policy_received_iso": last_policy_received_iso,
        "last_received_iso": last_received_iso,
        "le_max_price_rows": len(policy_rows),
        "market_slug": slug,
        "max_offset_s": max(num(item.get("offset_s")) for item in window_rows),
        "min_offset_s": min(num(item.get("offset_s")) for item in window_rows),
        "outcomes": sorted({str(item.get("outcome") or "") for item in window_rows}),
        "prices": sorted({num(item.get("price")) for item in window_rows}),
        "rows": len(window_rows),
        "window_start_s": num(window_rows[0].get("window_start_s")),
    }


def source_active_report(
    *,
    rtds_jsonl: str | Path,
    source_wallet: str,
    since_ts: float,
    min_offset_s: float,
    max_offset_s: float,
    max_price: float | None,
    required_windows: int,
    tail_bytes: int | None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    wallet = str(source_wallet).lower()
    target = Path(rtds_jsonl)
    file_size = target.stat().st_size if target.exists() else 0
    rows: list[dict[str, Any]] = []
    band_by_window: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in _jsonl_rows(target, tail_bytes=tail_bytes):
        if str(row.get("event") or "") != "rtds_trade_event":
            continue
        if _event_wallet(row) != wallet:
            continue
        slug = str(row.get("market_slug") or "")
        window_start_s = _window_start_from_slug(slug)
        if window_start_s is None:
            continue
        event_ts = _event_ts(row)
        if event_ts < since_ts:
            continue
        if _event_side(row) != "BUY":
            continue
        offset_s = event_ts - window_start_s
        price = _event_price(row)
        received_ts = _event_received_ts(row) or event_ts
        item = {
            "event_iso": _iso_from_ts(event_ts),
            "event_ts": round(event_ts, 6),
            "market_slug": slug,
            "offset_s": round(offset_s, 6),
            "outcome": _event_outcome(row),
            "price": price,
            "received_iso": _iso_from_ts(received_ts),
            "received_ts": round(received_ts, 6),
            "size": _event_size(row),
            "transaction_hash": _event_tx(row),
            "window_start_s": window_start_s,
        }
        rows.append(item)
        if min_offset_s <= offset_s <= max_offset_s:
            band_by_window[slug].append(item)

    band_rows = [item for window_rows in band_by_window.values() for item in window_rows]
    policy_rows = [
        item
        for item in band_rows
        if max_price is None or num(item.get("price"), default=999.0) <= float(max_price)
    ]
    policy_windows = {str(item.get("market_slug") or "") for item in policy_rows}
    source_active_windows = len(band_by_window)
    policy_eligible_windows = len(policy_windows)

    return {
        "kind": "source_active_windows_report_v1",
        "flow_stage": "LIVE/PROMOTE",
        "generated_at": generated_at or utc_now_iso(),
        "source_wallet": wallet,
        "rtds_jsonl": str(rtds_jsonl),
        "file_size_bytes": file_size,
        "scan_tail_bytes": tail_bytes,
        "since_ts": since_ts,
        "since_iso": _iso_from_ts(since_ts),
        "band": {
            "min_offset_s": min_offset_s,
            "max_offset_s": max_offset_s,
            "max_price": max_price,
        },
        "summary": {
            "btc5m_buy_rows_since": len(rows),
            "btc5m_buy_windows_since": len({str(item.get("market_slug") or "") for item in rows}),
            "source_active_rows": len(band_rows),
            "source_active_windows": source_active_windows,
            "policy_eligible_rows": len(policy_rows),
            "policy_eligible_windows": policy_eligible_windows,
            "required_source_active_windows": required_windows,
            "source_active_tally_status": "PASS" if source_active_windows >= required_windows else "PENDING",
            "policy_eligible_tally_status": "PASS" if policy_eligible_windows >= required_windows else "PENDING",
        },
        "windows": [_window_summary(slug, window_rows, max_price) for slug, window_rows in sorted(
            band_by_window.items(), key=lambda item: num(item[1][0].get("window_start_s"))
        )],
        "latest_rows": rows[-20:],
    }


def main() -> None:
    args = parse_args()
    since_ts = parse_ts(args.since)
    if since_ts is None:
        raise SystemExit(f"invalid --since timestamp: {args.since!r}")
    tail_bytes = None if args.full_scan else int(args.tail_bytes)
    report = source_active_report(
        rtds_jsonl=args.rtds_jsonl,
        source_wallet=args.source_wallet,
        since_ts=float(since_ts),
        min_offset_s=float(args.min_offset_s),
        max_offset_s=float(args.max_offset_s),
        max_price=args.max_price,
        required_windows=int(args.required_windows),
        tail_bytes=tail_bytes,
    )
    atomic_write_json(args.output, report)
    print(json.dumps(report["summary"], sort_keys=True))


if __name__ == "__main__":
    main()
