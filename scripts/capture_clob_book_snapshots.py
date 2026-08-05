#!/usr/bin/env python3
"""Capture CLOB /book snapshots as an alpha-decay fallback source.

This is read-only market-data collection. It writes rows compatible with
`src.wallet_copy.alpha_decay.clob_market_points` by emitting `best_bid_ask`
events for token ids whose books are currently available.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.http_client import PolymarketRouteError
from src.wallet_copy.live_tracker import CLOBMarketClient
from src.wallet_copy.models import utc_now_iso
from src.wallet_copy.store import append_jsonl


DEFAULT_CLOB_BASE = os.getenv("POLYMARKET_CLOB_API_BASE_URL", "http://127.0.0.1:8787/clob")
TAIL_SCAN_BYTES = 128_000_000


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--asset-id", action="append", default=[])
    parser.add_argument("--asset-ids-file", default="data/research/alpha_decay_target_asset_ids.json")
    parser.add_argument("--output", default="data/research/clob_book_snapshots_alpha_decay.jsonl")
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--duration-s", type=float, default=300.0)
    parser.add_argument("--interval-s", type=float, default=1.0)
    parser.add_argument("--timeout-s", type=float, default=2.0)
    parser.add_argument("--clob-retries", type=int, default=1)
    parser.add_argument("--max-assets", type=int, default=20)
    parser.add_argument(
        "--polygon-jsonl",
        default="",
        help="Optional live Polygon OrderFilled JSONL to mine for active alpha-decay token ids while running.",
    )
    parser.add_argument("--polygon-scan-limit", type=int, default=50_000)
    parser.add_argument("--polygon-max-age-s", type=float, default=0.0)
    parser.add_argument(
        "--polygon-source",
        action="append",
        default=[],
        help="Polygon OrderFilled row sources to mine for asset ids. Defaults to polygon_ws.",
    )
    parser.add_argument(
        "--polygon-registry-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="When mining Polygon rows, keep rows involving registered/selected wallets only.",
    )
    parser.add_argument("--asset-refresh-s", type=float, default=5.0)
    parser.add_argument(
        "--disable-source-base-overrides",
        action="store_true",
        help="Temporarily clear POLYMARKET_CLOB_API_BASE_URL so --clob-base-url is used directly.",
    )
    return parser.parse_args()


def _asset_ids_from_file(path: str) -> list[str]:
    if not path:
        return []
    target = Path(path)
    if not target.exists():
        return []
    payload = json.loads(target.read_text(encoding="utf-8"))
    if isinstance(payload, list):
        return [str(item) for item in payload if str(item)]
    if isinstance(payload, dict):
        ids = payload.get("asset_ids")
        if isinstance(ids, list):
            return [str(item) for item in ids if str(item)]
    return []


def _iter_recent_jsonl(path: str, limit: int) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    chunks: list[bytes] = []
    scanned = 0
    with target.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        while position > 0 and scanned < TAIL_SCAN_BYTES:
            size = min(1_048_576, position, TAIL_SCAN_BYTES - scanned)
            position -= size
            handle.seek(position)
            chunk = handle.read(size)
            chunks.append(chunk)
            scanned += len(chunk)
            if sum(item.count(b"\n") for item in chunks) >= max(1, int(limit)):
                break
    rows: list[dict[str, Any]] = []
    for raw in reversed(b"".join(reversed(chunks)).splitlines()):
        try:
            row = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(row, dict):
            rows.append(row)
        if len(rows) >= max(1, int(limit)):
            break
    return list(reversed(rows))


def _polygon_sources(args: argparse.Namespace) -> tuple[str, ...]:
    return tuple(str(item) for item in (args.polygon_source or ["polygon_ws"]) if str(item))


def _row_ts(row: dict[str, Any]) -> float:
    keys = ("received_at_s", "captured_at_s", "block_ts")
    if str(row.get("source") or "") == "polygon_http_getLogs":
        keys = ("block_ts", "received_at_s", "captured_at_s")
    for key in keys:
        try:
            value = float(row.get(key) or 0.0)
        except (TypeError, ValueError):
            value = 0.0
        if value > 0:
            return value
    return 0.0


def _asset_ids_from_polygon_rows(
    rows: list[dict[str, Any]],
    *,
    max_assets: int,
    now_s: float | None = None,
    max_age_s: float = 0.0,
    registry_only: bool = True,
    sources: tuple[str, ...] | list[str] | set[str] | None = None,
) -> list[str]:
    stats: dict[str, dict[str, Any]] = {}
    now_value = float(now_s if now_s is not None else time.time())
    allowed_sources = {str(item) for item in (sources or ["polygon_ws"]) if str(item)}
    for row in rows:
        if row.get("event") != "polygon_orderfilled_log" or str(row.get("source") or "") not in allowed_sources:
            continue
        if registry_only and not (row.get("is_registry_wallet") or row.get("selected_wallet") or row.get("registry_wallets")):
            continue
        decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
        if decoded.get("decode_status") not in {None, "", "OK"}:
            continue
        asset_id = str(decoded.get("asset") or "").strip()
        if not asset_id:
            continue
        ts = _row_ts(row)
        if max_age_s > 0 and ts > 0 and now_value - ts > float(max_age_s):
            continue
        record = stats.setdefault(asset_id, {"asset_id": asset_id, "count": 0, "last_ts": 0.0})
        record["count"] += 1
        record["last_ts"] = max(float(record.get("last_ts") or 0.0), ts)
    ranked = sorted(
        stats.values(),
        key=lambda row: (
            -float(row.get("last_ts") or 0.0),
            -int(row.get("count") or 0),
            str(row.get("asset_id") or ""),
        ),
    )
    return [str(row["asset_id"]) for row in ranked[: max(0, int(max_assets))]]


def _merge_asset_ids(*groups: list[str], max_assets: int) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for group in groups:
        for asset_id in group:
            text = str(asset_id or "").strip()
            if not text or text in seen:
                continue
            out.append(text)
            seen.add(text)
            if len(out) >= max(1, int(max_assets)):
                return out
    return out


def _book_best_bid_ask(book: dict[str, Any]) -> tuple[float | None, float | None]:
    bids = []
    asks = []
    for row in book.get("bids") or []:
        if isinstance(row, dict):
            try:
                bids.append(float(row.get("price")))
            except (TypeError, ValueError):
                pass
    for row in book.get("asks") or []:
        if isinstance(row, dict):
            try:
                asks.append(float(row.get("price")))
            except (TypeError, ValueError):
                pass
    return (max(bids) if bids else None, min(asks) if asks else None)


def _normalized_levels(book: dict[str, Any], side: str) -> list[dict[str, float]]:
    levels: list[dict[str, float]] = []
    for row in book.get(side) or []:
        if not isinstance(row, dict):
            continue
        try:
            price = float(row.get("price"))
            size = float(row.get("size"))
        except (TypeError, ValueError):
            continue
        if 0 <= price <= 1 and size > 0:
            levels.append({"price": price, "size": size})
    levels.sort(key=lambda row: row["price"], reverse=side == "bids")
    return levels


def _snapshot_row_from_book(asset_id: str, book: dict[str, Any], *, captured_at_s: float) -> tuple[dict[str, Any], bool]:
    bid, ask = _book_best_bid_ask(book)
    route_report = book.get("__walletCopyClobRouteReport") if isinstance(book.get("__walletCopyClobRouteReport"), dict) else {}
    if bid is None or ask is None:
        return (
            {
                "event_type": "clob_book_snapshot_unavailable",
                "captured_at_s": captured_at_s,
                "captured_at_iso": utc_now_iso(),
                "asset_id": asset_id,
                "book_status": "EMPTY_OR_NO_ORDERBOOK",
                "empty_book_truth": bool(book.get("empty_book_truth") or route_report.get("empty_book_truth")),
                "route_report": route_report,
                "raw": {key: value for key, value in book.items() if key != "__walletCopyClobRouteReport"},
            },
            False,
        )
    return (
        {
            "event_type": "best_bid_ask",
            "captured_at_s": captured_at_s,
            "captured_at_iso": utc_now_iso(),
            "asset_id": asset_id,
            "best_bid": bid,
            "best_ask": ask,
            "spread": ask - bid,
            "bids": _normalized_levels(book, "bids"),
            "asks": _normalized_levels(book, "asks"),
            "source": "clob_rest_book_snapshot",
            "route_report": route_report,
        },
        True,
    )


def _explicit_asset_ids(args: argparse.Namespace) -> list[str]:
    return _merge_asset_ids(
        [*args.asset_id, *_asset_ids_from_file(args.asset_ids_file)],
        max_assets=int(args.max_assets),
    )


def _active_asset_ids(args: argparse.Namespace) -> tuple[list[str], dict[str, Any]]:
    explicit_assets = _explicit_asset_ids(args)
    polygon_assets: list[str] = []
    polygon_rows = 0
    if str(args.polygon_jsonl or "").strip():
        rows = _iter_recent_jsonl(str(args.polygon_jsonl), int(args.polygon_scan_limit))
        polygon_rows = len(rows)
        polygon_assets = _asset_ids_from_polygon_rows(
            rows,
            max_assets=int(args.max_assets),
            max_age_s=float(args.polygon_max_age_s),
            registry_only=bool(args.polygon_registry_only),
            sources=_polygon_sources(args),
        )
    merged = _merge_asset_ids(explicit_assets, polygon_assets, max_assets=int(args.max_assets))
    return merged, {
        "explicit_assets": len(explicit_assets),
        "polygon_rows_scanned": polygon_rows,
        "polygon_assets": len(polygon_assets),
        "polygon_jsonl": str(args.polygon_jsonl or ""),
        "polygon_sources": list(_polygon_sources(args)),
        "polygon_registry_only": bool(args.polygon_registry_only),
        "polygon_max_age_s": float(args.polygon_max_age_s),
    }


def main() -> int:
    args = parse_args()
    asset_ids, asset_source = _active_asset_ids(args)
    if not asset_ids and not str(args.polygon_jsonl or "").strip():
        raise SystemExit("no asset ids supplied")

    prior_clob_override = os.environ.get("POLYMARKET_CLOB_API_BASE_URL")
    if bool(args.disable_source_base_overrides):
        os.environ.pop("POLYMARKET_CLOB_API_BASE_URL", None)
    clob = CLOBMarketClient(host=args.clob_base_url, timeout_s=float(args.timeout_s), retries=int(args.clob_retries))
    deadline = time.time() + max(1.0, float(args.duration_s))
    next_asset_refresh = 0.0
    cycles = 0
    rows_written = 0
    unavailable = 0
    errors = 0
    no_asset_cycles = 0
    asset_refreshes = 0
    status_counts: Counter[str] = Counter()
    while time.time() < deadline:
        cycle_started = time.time()
        cycles += 1
        if cycle_started >= next_asset_refresh:
            asset_ids, asset_source = _active_asset_ids(args)
            next_asset_refresh = cycle_started + max(0.1, float(args.asset_refresh_s))
            asset_refreshes += 1
        if not asset_ids:
            no_asset_cycles += 1
            sleep_s = max(0.0, float(args.interval_s) - (time.time() - cycle_started))
            if sleep_s:
                time.sleep(sleep_s)
            continue
        for asset_id in asset_ids:
            captured_at_s = time.time()
            try:
                book = clob.get_book(asset_id)
                row, available = _snapshot_row_from_book(asset_id, book, captured_at_s=captured_at_s)
                append_jsonl(args.output, row)
                status_counts[str(row.get("event_type") or "unknown")] += 1
                if available:
                    rows_written += 1
                else:
                    unavailable += 1
            except PolymarketRouteError as exc:
                errors += 1
                status_counts["clob_book_snapshot_error"] += 1
                append_jsonl(
                    args.output,
                    {
                        "event_type": "clob_book_snapshot_error",
                        "captured_at_s": captured_at_s,
                        "captured_at_iso": utc_now_iso(),
                        "asset_id": asset_id,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "route_report": exc.route_report if isinstance(exc.route_report, dict) else {},
                    },
                )
            except Exception as exc:  # noqa: BLE001 - measurement evidence.
                errors += 1
                status_counts["clob_book_snapshot_error"] += 1
                append_jsonl(
                    args.output,
                    {
                        "event_type": "clob_book_snapshot_error",
                        "captured_at_s": captured_at_s,
                        "captured_at_iso": utc_now_iso(),
                        "asset_id": asset_id,
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                    },
                )
        sleep_s = max(0.0, float(args.interval_s) - (time.time() - cycle_started))
        if sleep_s:
            time.sleep(sleep_s)

    summary = {
        "event_type": "clob_book_snapshot_summary",
        "captured_at_s": time.time(),
        "captured_at_iso": utc_now_iso(),
        "asset_ids": len(asset_ids),
        "cycles": cycles,
        "rows_written": rows_written,
        "unavailable": unavailable,
        "errors": errors,
        "no_asset_cycles": no_asset_cycles,
        "asset_refreshes": asset_refreshes,
        "event_type_counts": dict(sorted(status_counts.items())),
        "output": args.output,
        "clob_base_url": args.clob_base_url,
        "source_base_overrides_disabled": bool(args.disable_source_base_overrides),
        "asset_source": asset_source,
    }
    append_jsonl(args.output, summary)
    print(json.dumps(summary, indent=2, sort_keys=True))
    if bool(args.disable_source_base_overrides):
        if prior_clob_override is None:
            os.environ.pop("POLYMARKET_CLOB_API_BASE_URL", None)
        else:
            os.environ["POLYMARKET_CLOB_API_BASE_URL"] = prior_clob_override
    return 0 if rows_written else 2


if __name__ == "__main__":
    raise SystemExit(main())
