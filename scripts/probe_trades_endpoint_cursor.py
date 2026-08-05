#!/usr/bin/env python3
"""Probe the Data API trades endpoint for a cursor or useful market scope.

Flow stage: DISCOVER. Exactly six single-request attempts are made; no retry
or live-trading path is involved.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json  # noqa: E402


TRADES_URL = "https://data-api.polymarket.com/trades"


def _iso(ts: float) -> str:
    if ts <= 0:
        return ""
    return datetime.fromtimestamp(ts, tz=UTC).isoformat().replace("+00:00", "Z")


def _timestamp(row: dict[str, Any]) -> float:
    try:
        return float(row.get("timestamp") or 0.0)
    except (TypeError, ValueError):
        return 0.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--floor-ts", type=float, required=True)
    parser.add_argument("--market", required=True)
    parser.add_argument("--asset-id", required=True)
    parser.add_argument("--output", default="data/research/trades_endpoint_cursor_probe.json")
    parser.add_argument("--timeout-s", type=float, default=15.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    before_ts = int(args.floor_ts) - 1
    attempts = [
        ("before", before_ts),
        ("end", before_ts),
        ("end_date", _iso(float(before_ts))),
        ("market", str(args.market)),
        ("asset_id", str(args.asset_id)),
        ("condition_id", str(args.market)),
    ]
    rows_out = []
    for key, value in attempts:
        record: dict[str, Any] = {
            "request_key": key,
            "request_value": value,
            "http_status": None,
            "row_count": 0,
            "oldest_trade_ts": 0.0,
            "oldest_trade_iso": "",
            "moves_earlier_than_offset_10500_floor": False,
        }
        try:
            params = {"limit": 500, "takerOnly": "false", key: value}
            response = requests.get(
                TRADES_URL,
                params=params,
                timeout=float(args.timeout_s),
            )
            record["http_status"] = int(response.status_code)
            payload = response.json() if response.ok else []
            rows = [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
            timestamps = [_timestamp(row) for row in rows if _timestamp(row) > 0]
            oldest = min(timestamps) if timestamps else 0.0
            record.update(
                {
                    "row_count": len(rows),
                    "oldest_trade_ts": oldest,
                    "oldest_trade_iso": _iso(oldest),
                    "moves_earlier_than_offset_10500_floor": bool(
                        oldest and oldest < float(args.floor_ts)
                    ),
                }
            )
        except (requests.RequestException, ValueError) as exc:
            record["error"] = f"{type(exc).__name__}: {exc}"
        rows_out.append(record)

    successful = [
        row["request_key"]
        for row in rows_out
        if row["moves_earlier_than_offset_10500_floor"]
    ]
    payload = {
        "schema_version": 1,
        "kind": "trades_endpoint_cursor_probe",
        "flow_stage": "DISCOVER",
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "endpoint": TRADES_URL,
        "offset_10500_floor_ts": float(args.floor_ts),
        "offset_10500_floor_iso": _iso(float(args.floor_ts)),
        "request_count": len(rows_out),
        "attempts": rows_out,
        "parameters_paging_past_floor": successful,
        "market_scoping_available": any(
            key in {"market", "asset_id", "condition_id"} for key in successful
        ),
        "finding": (
            "NAMED_PARAMETER_PAGES_PAST_OFFSET_FLOOR"
            if successful
            else "NO_TESTED_CURSOR_OR_MARKET_SCOPE_PAGES_PAST_OFFSET_FLOOR"
        ),
    }
    atomic_write_json(args.output, payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
