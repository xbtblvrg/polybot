#!/usr/bin/env python3
"""Measure the OBSERVE-stage top-10 broad wallet paper lane.

This script is paper/research only. It consumes realtime wallet-attributed
rows, filters them to the selected broad wallets, checks current CLOB
fillability, and persists per-wallet paper metrics for rotation decisions.
It does not create CopyIntents and never submits live orders.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.http_client import PolymarketRouteError  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.market_categories import (  # noqa: E402
    market_categories_from_metadata,
    summarize_wallet_market_categories,
)
from src.wallet_copy.mission import WALLET_COPY_MISSION_CONTRACT  # noqa: E402
from src.wallet_copy.models import now_ts, num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402


DEFAULT_LANE_STATE = "data/research/wallet_copy_top10_broad_paper_lane_state.json"
DEFAULT_POLYGON_JSONL = "data/research/polygon_orderfilled_ws_capture.jsonl"
DEFAULT_RTDS_JSONL = ""
DEFAULT_OUTPUT = "data/research/wallet_copy_top10_broad_paper_measurement_state.json"
DEFAULT_EVENTS = "data/research/wallet_copy_top10_broad_paper_events.jsonl"
DEFAULT_CLOB_BASE = "http://127.0.0.1:8787/clob"
SOURCE_BASE_OVERRIDE_ENV_VARS = ("POLYMARKET_CLOB_API_BASE_URL",)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane-state", default=DEFAULT_LANE_STATE)
    parser.add_argument("--polygon-jsonl", default=DEFAULT_POLYGON_JSONL)
    parser.add_argument(
        "--rtds-jsonl",
        default=DEFAULT_RTDS_JSONL,
        help="Optional RTDS activity JSONL; when set, this is the primary realtime input.",
    )
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--event-log", default=DEFAULT_EVENTS)
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=1.5)
    parser.add_argument("--scan-limit", type=int, default=250_000)
    parser.add_argument("--max-events", type=int, default=500)
    parser.add_argument("--wallet-fraction", type=float, default=0.0)
    parser.add_argument("--max-order-usd", type=float, default=0.0)
    parser.add_argument("--min-order-usd", type=float, default=0.0)
    parser.add_argument("--policy-id", default="")
    parser.add_argument("--slippage-bps", type=float, default=250.0)
    parser.add_argument("--min-fill-ratio", type=float, default=0.999)
    parser.add_argument("--max-book-fetches", type=int, default=100)
    parser.add_argument(
        "--floor-copy-size-to-min-order",
        action="store_true",
        help="Paper-only measurement mode: score BUY copyability at max(min_order_usd, computed copy size).",
    )
    parser.add_argument(
        "--buy-events-only",
        action="store_true",
        help="Paper-only measurement mode: skip SELL rows so scoped BUY samples do not include exits.",
    )
    parser.add_argument(
        "--market-category",
        default="",
        help="Optional paper-only measurement filter, for example btc_5m.",
    )
    parser.add_argument(
        "--max-receipt-to-fetch-age-s",
        type=float,
        default=0.0,
        help="Optional fresh-event guard: skip events whose received_at_s is older than this before fetching a book.",
    )
    parser.add_argument(
        "--positive-control-json",
        default="",
        help="Optional positive-control probe artifact to embed in the measurement state.",
    )
    parser.add_argument("--ignore-prior-state", action="store_true")
    parser.add_argument("--diagnose-liquidity", action="store_true")
    parser.add_argument("--diagnose-liquidity-limit", type=int, default=100)
    parser.add_argument(
        "--disable-source-base-overrides",
        action="store_true",
        help="Paper-only measurement mode: ignore CLOB source-base overrides such as local relays.",
    )
    return parser.parse_args()


def _disable_source_base_overrides(disabled: bool) -> dict[str, str | None]:
    prior = {key: os.environ.get(key) for key in SOURCE_BASE_OVERRIDE_ENV_VARS}
    if disabled:
        for key in SOURCE_BASE_OVERRIDE_ENV_VARS:
            os.environ.pop(key, None)
    return prior


def _restore_source_base_overrides(prior: dict[str, str | None]) -> None:
    for key, value in prior.items():
        if value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = value


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _mission_sizing_defaults() -> dict[str, float]:
    runtime = WALLET_COPY_MISSION_CONTRACT.get("current_runtime_phase_contract")
    runtime = runtime if isinstance(runtime, dict) else {}
    policy = runtime.get("profitability_filter_contract")
    policy = policy if isinstance(policy, dict) else {}
    return {
        "wallet_fraction": num(policy.get("wallet_fraction"), 0.05),
        "max_order_usd": num(policy.get("max_order_usd"), 2.0),
        "min_order_usd": num(policy.get("min_live_order_usd"), 1.0),
    }


def _iter_recent_jsonl(path: str, limit: int) -> list[dict[str, Any]]:
    target = Path(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    with target.open("rb") as handle:
        handle.seek(0, 2)
        position = handle.tell()
        chunks: list[bytes] = []
        scanned = 0
        while position > 0 and scanned < 128_000_000:
            size = min(1_048_576, position, 128_000_000 - scanned)
            position -= size
            handle.seek(position)
            chunk = handle.read(size)
            chunks.append(chunk)
            scanned += len(chunk)
    for raw in reversed(b"".join(reversed(chunks)).splitlines()):
        try:
            row = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            continue
        if isinstance(row, dict) and row.get("event") in {
            "polygon_orderfilled_log",
            "rtds_trade_event",
            "wallet_copy_wallet_event",
        }:
            rows.append(row)
        if len(rows) >= max(1, int(limit)):
            break
    return list(reversed(rows))


def _selected_wallet_rows(lane_state: dict[str, Any]) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in lane_state.get("ranked_wallets") or []:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("wallet"))
        if wallet:
            rows[wallet] = row
    return rows


def _opposite_side(side: str) -> str:
    value = str(side or "").upper()
    if value == "BUY":
        return "SELL"
    if value == "SELL":
        return "BUY"
    return value


def _wallet_sides(row: dict[str, Any], selected_wallets: set[str]) -> list[tuple[str, str]]:
    if row.get("event") in {"rtds_trade_event", "wallet_copy_wallet_event"}:
        wallet = _norm_wallet(row.get("source_wallet") or row.get("proxyWallet"))
        side = str(row.get("side") or row.get("action") or "").upper()
        return [(wallet, side)] if wallet in selected_wallets and side else []

    decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
    maker = _norm_wallet(row.get("maker") or decoded.get("maker"))
    taker = _norm_wallet(row.get("taker") or decoded.get("taker") or row.get("topic3"))
    maker_side = str(decoded.get("maker_side") or decoded.get("side") or "").upper()
    out: list[tuple[str, str]] = []
    if maker and maker in selected_wallets:
        out.append((maker, maker_side))
    if taker and taker in selected_wallets:
        out.append((taker, _opposite_side(maker_side)))
    if not out:
        selected = _norm_wallet(row.get("selected_wallet"))
        side = str(decoded.get("side") or "").upper()
        if selected and selected in selected_wallets:
            out.append((selected, side))
    return [(wallet, side) for wallet, side in out if wallet and side]


def _empty_wallet_metrics(wallet: str, lane_row: dict[str, Any]) -> dict[str, Any]:
    market_categories = market_categories_from_metadata(lane_row)
    return {
        "wallet": wallet,
        "category": lane_row.get("category") or "",
        "market_categories": market_categories,
        "primary_market_category": market_categories[0] if market_categories else "unknown",
        "market_category_metrics": {},
        "user_name": lane_row.get("user_name") or "",
        "leaderboard_rank": lane_row.get("rank"),
        "leaderboard_pnl_max": lane_row.get("leaderboard_pnl_max"),
        "events_seen": 0,
        "buy_events": 0,
        "sell_events": 0,
        "copyable_buy_events": 0,
        "rejected_buy_events": 0,
        "below_min_order_events": 0,
        "paper_orders": 0,
        "open_positions": 0,
        "open_shares": 0.0,
        "open_cost_usd": 0.0,
        "realized_pnl_usd": 0.0,
        "unrealized_pnl_usd": 0.0,
        "paper_pnl_usd": 0.0,
        "copyable_rate_pct": None,
        "fill_ratio_sum": 0.0,
        "avg_fill_ratio": None,
        "last_event_ts": None,
        "last_observed_ts": None,
        "reject_reasons": {},
        "sample_status": "NO_REALTIME_EVENTS",
    }


def _position_key(wallet: str, asset: str) -> str:
    return f"{wallet}|{asset}"


def _copy_size_usd(price: float, size: float, *, wallet_fraction: float, max_order_usd: float) -> float:
    source_usd = max(0.0, float(price) * float(size))
    return round(min(source_usd * float(wallet_fraction), float(max_order_usd)), 6)


def _policy_id(
    *,
    explicit: str = "",
    wallet_fraction: float,
    max_order_usd: float,
    min_order_usd: float,
) -> str:
    if str(explicit or "").strip():
        return str(explicit).strip()
    fraction = str(round(float(wallet_fraction), 6)).replace(".", "p")
    max_order = str(round(float(max_order_usd), 6)).replace(".", "p")
    min_order = str(round(float(min_order_usd), 6)).replace(".", "p")
    return f"wf{fraction}_max{max_order}_min{min_order}"


def _book_top_of_book(book: dict[str, Any]) -> dict[str, Any]:
    asks = sorted(
        ((num(row.get("price")), num(row.get("size"))) for row in (book.get("asks") or []) if isinstance(row, dict)),
        key=lambda item: item[0],
    )
    bids = sorted(
        ((num(row.get("price")), num(row.get("size"))) for row in (book.get("bids") or []) if isinstance(row, dict)),
        key=lambda item: item[0],
        reverse=True,
    )
    best_ask = asks[0][0] if asks else 0.0
    best_bid = bids[0][0] if bids else 0.0
    best_ask_shares = sum(size for price, size in asks if best_ask > 0 and abs(price - best_ask) <= 1e-9)
    best_bid_shares = sum(size for price, size in bids if best_bid > 0 and abs(price - best_bid) <= 1e-9)
    return {
        "asset_id": book.get("asset_id"),
        "book_market": book.get("market"),
        "book_timestamp": book.get("timestamp"),
        "book_hash": book.get("hash"),
        "best_ask": round(best_ask, 6),
        "best_bid": round(best_bid, 6),
        "ask_levels": len(asks),
        "bid_levels": len(bids),
        "best_ask_depth_shares": round(best_ask_shares, 6),
        "best_bid_depth_shares": round(best_bid_shares, 6),
        "best_ask_depth_usd": round(best_ask * best_ask_shares, 6) if best_ask > 0 else 0.0,
        "best_bid_depth_usd": round(best_bid * best_bid_shares, 6) if best_bid > 0 else 0.0,
        "route_report": book.get("__walletCopyClobRouteReport") if isinstance(book.get("__walletCopyClobRouteReport"), dict) else {},
    }


def _book_unavailable_or_market_closed(top_of_book: dict[str, Any]) -> bool:
    timestamp = top_of_book.get("book_timestamp")
    if timestamp is None or str(timestamp).strip() == "":
        return True
    return num(top_of_book.get("best_bid")) <= 0.0 and num(top_of_book.get("best_ask")) <= 0.0


def _book_ts_s(value: Any) -> float | None:
    raw = num(value, 0.0)
    if raw <= 0:
        return None
    return float(raw / 1000.0 if raw > 10_000_000_000 else raw)


def _book_ts_s_near_source(value: Any, source_ts: float) -> float | None:
    raw = num(value, 0.0)
    if raw <= 0:
        return None
    candidates = [raw]
    if raw > 10_000_000_000:
        candidates.append(raw / 1000.0)
    if raw > 10_000_000_000:
        candidates.append(raw / 1_000_000.0)
    if source_ts > 0:
        return min(candidates, key=lambda item: abs(float(item) - float(source_ts)))
    return float(raw / 1000.0 if raw > 10_000_000_000 else raw)


def _shadow_scoring_fields(
    *,
    normalized: dict[str, Any],
    result: dict[str, Any],
    source_price: float,
    fetch_s: float,
    receipt_to_fetch_latency_ms: float | None,
) -> dict[str, Any]:
    """Return Fable-required paper-only realtime shadow score fields."""

    book = result.get("book") if isinstance(result.get("book"), dict) else {}
    top = book.get("top_of_book") if isinstance(book.get("top_of_book"), dict) else {}
    source_ts = num(normalized.get("event_ts"), 0.0)
    book_ts = _book_ts_s_near_source(top.get("book_timestamp") or book.get("book_timestamp"), source_ts)
    best_ask = num(top.get("best_ask"), num(book.get("best_ask"), 0.0))
    needed_bps = None
    if source_price > 0 and best_ask > 0:
        needed_bps = round(max(0.0, (best_ask / float(source_price) - 1.0) * 10000.0), 6)
    parity_limit = round(min(0.99, float(source_price) + 0.05), 6) if source_price > 0 else None
    maker_candidate = bool(
        parity_limit is not None
        and parity_limit <= 0.50 + 1e-9
        and (best_ask <= 0 or best_ask > parity_limit + 1e-9)
    )
    taker_fillable = str(result.get("status") or "").upper() == "FILLED"
    drift_buffer_taker_fillable = bool(parity_limit is not None and best_ask > 0 and best_ask <= parity_limit + 1e-9)
    return {
        "source_ts": source_ts or None,
        "book_ts": book_ts,
        "book_age_s": round(max(0.0, book_ts - source_ts), 6) if book_ts is not None and source_ts > 0 else None,
        "book_fetch_started_at_s": round(fetch_s, 6),
        "receipt_to_fetch_latency_ms": receipt_to_fetch_latency_ms,
        "taker_fillable": taker_fillable,
        "parity_fillable": bool(taker_fillable or drift_buffer_taker_fillable or maker_candidate),
        "drift_buffer_taker_fillable": drift_buffer_taker_fillable,
        "maker_fallback_candidate": maker_candidate,
        "needed_bps": needed_bps,
        "parity_limit_price": parity_limit,
        "shadow_contract": "paper_only_realtime_watch_no_submit",
    }


def _mark_positions(wallets: dict[str, dict[str, Any]], positions: dict[str, dict[str, Any]]) -> None:
    for metric in wallets.values():
        metric["open_positions"] = 0
        metric["open_shares"] = 0.0
        metric["open_cost_usd"] = 0.0
        metric["unrealized_pnl_usd"] = 0.0
    for position in positions.values():
        wallet = _norm_wallet(position.get("wallet"))
        if wallet not in wallets:
            continue
        shares = num(position.get("shares"))
        if shares <= 1e-9:
            continue
        metric = wallets[wallet]
        cost = num(position.get("cost_usd"))
        mark_price = num(position.get("mark_price"))
        metric["open_positions"] += 1
        metric["open_shares"] = round(num(metric.get("open_shares")) + shares, 6)
        metric["open_cost_usd"] = round(num(metric.get("open_cost_usd")) + cost, 6)
        metric["unrealized_pnl_usd"] = round(num(metric.get("unrealized_pnl_usd")) + shares * mark_price - cost, 6)
    for metric in wallets.values():
        metric["paper_pnl_usd"] = round(num(metric.get("realized_pnl_usd")) + num(metric.get("unrealized_pnl_usd")), 6)
        buy_events = int(metric.get("buy_events") or 0)
        if buy_events:
            metric["copyable_rate_pct"] = round(100.0 * int(metric.get("copyable_buy_events") or 0) / buy_events, 6)
            metric["avg_fill_ratio"] = round(num(metric.get("fill_ratio_sum")) / buy_events, 6)
            metric["sample_status"] = "HAS_BUY_SAMPLE"
        elif int(metric.get("events_seen") or 0):
            metric["copyable_rate_pct"] = 0.0
            metric["sample_status"] = "NO_BUY_SAMPLE"


def _record_reject(metric: dict[str, Any], reason: str) -> None:
    counts = Counter(metric.get("reject_reasons") or {})
    counts[str(reason or "unknown")] += 1
    metric["reject_reasons"] = dict(sorted(counts.items()))


def _clob_exception_detail(exc: Exception) -> dict[str, Any]:
    response = getattr(exc, "response", None)
    status_code = getattr(response, "status_code", None)
    endpoint = getattr(getattr(response, "request", None), "url", None) if response is not None else None
    reason = "clob_route_error" if isinstance(exc, PolymarketRouteError) else f"clob_error:{type(exc).__name__}"
    if status_code:
        reason = f"clob_http_{int(status_code)}:{type(exc).__name__}"
    return {
        "error_type": type(exc).__name__,
        "error": str(exc),
        "http_status_code": int(status_code) if status_code else None,
        "endpoint": endpoint,
        "reject_reason": reason,
    }


def _event_market_categories(row: dict[str, Any], decoded: dict[str, Any], lane_row: dict[str, Any]) -> list[str]:
    return market_categories_from_metadata(row, decoded, lane_row)


def _normalized_realtime_event(row: dict[str, Any]) -> dict[str, Any] | None:
    event_type = str(row.get("event") or "")
    if event_type == "polygon_orderfilled_log":
        source = str(row.get("source") or "")
        if source not in {"polygon_ws", "polygon_http_getLogs_tail"}:
            return None
        decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
        if decoded.get("decode_status") != "OK":
            return None
        asset = str(decoded.get("asset") or "")
        price = num(decoded.get("price"))
        size = num(decoded.get("size"))
        tx = str(row.get("transaction_hash") or row.get("transactionHash") or "").lower()
        if not asset or price <= 0 or size <= 0 or not tx:
            return None
        return {
            "source": source,
            "decoded": decoded,
            "asset": asset,
            "price": price,
            "size": size,
            "tx": tx,
            "source_event_id": str(row.get("log_index") or row.get("event_id") or ""),
            "event_ts": row.get("block_ts"),
            "received_at_s": row.get("received_at_s"),
        }
    if event_type == "rtds_trade_event":
        asset = str(row.get("asset") or row.get("asset_id") or "")
        price = num(row.get("price"))
        size = num(row.get("size"))
        tx = str(row.get("transaction_hash") or row.get("transactionHash") or "").lower()
        if not asset or price <= 0 or size <= 0 or not tx:
            return None
        decoded = {
            "decode_status": "OK",
            "side": str(row.get("side") or "").upper(),
            "asset": asset,
            "price": price,
            "size": size,
        }
        return {
            "source": "rtds_activity",
            "decoded": decoded,
            "asset": asset,
            "price": price,
            "size": size,
            "tx": tx,
            "source_event_id": str(row.get("event_id") or ""),
            "event_ts": row.get("event_ts"),
            "received_at_s": row.get("received_at_s"),
        }
    if event_type == "wallet_copy_wallet_event":
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        asset = str(row.get("token_id") or raw.get("asset") or "")
        price = num(row.get("price") or raw.get("price"))
        size = num(row.get("size") or raw.get("size"))
        tx = str(row.get("transaction_hash") or raw.get("transactionHash") or "").lower()
        if not asset or price <= 0 or size <= 0 or not tx:
            return None
        decoded = {
            "decode_status": "OK",
            "side": str(row.get("action") or raw.get("side") or "").upper(),
            "asset": asset,
            "price": price,
            "size": size,
        }
        return {
            "source": "dataapi_poll",
            "decoded": decoded,
            "asset": asset,
            "price": price,
            "size": size,
            "tx": tx,
            "source_event_id": str(row.get("event_id") or ""),
            "event_ts": row.get("event_ts") or raw.get("timestamp"),
            "received_at_s": row.get("observed_ts"),
        }
    return None


def _category_metric(metric: dict[str, Any], category: str) -> dict[str, Any]:
    category_metrics = metric.setdefault("market_category_metrics", {})
    if not isinstance(category_metrics, dict):
        category_metrics = {}
        metric["market_category_metrics"] = category_metrics
    row = category_metrics.setdefault(
        category or "unknown",
        {
            "events_seen": 0,
            "buy_events": 0,
            "sell_events": 0,
            "copyable_buy_events": 0,
            "rejected_buy_events": 0,
        },
    )
    return row if isinstance(row, dict) else {}


def _summarize_measurement_market_categories(ranked_wallets: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    summary = summarize_wallet_market_categories(ranked_wallets)
    for item in summary.values():
        item["events_seen"] = 0
        item["buy_events"] = 0
        item["sell_events"] = 0
        item["copyable_buy_events"] = 0
        item["rejected_buy_events"] = 0
        item["wallets_with_buy_sample"] = 0
        item["wallets_with_copyable_buys"] = 0
    for row in ranked_wallets:
        metrics = row.get("market_category_metrics") if isinstance(row.get("market_category_metrics"), dict) else {}
        if not metrics:
            primary = str(row.get("primary_market_category") or "unknown")
            metrics = {
                primary: {
                    "events_seen": int(row.get("events_seen") or 0),
                    "buy_events": int(row.get("buy_events") or 0),
                    "sell_events": int(row.get("sell_events") or 0),
                    "copyable_buy_events": int(row.get("copyable_buy_events") or 0),
                    "rejected_buy_events": int(row.get("rejected_buy_events") or 0),
                }
            }
        for category, raw in metrics.items():
            if not isinstance(raw, dict):
                continue
            item = summary.setdefault(
                str(category or "unknown"),
                {
                    "selected_wallets": 0,
                    "wallets_with_buy_sample": 0,
                    "wallets_with_copyable_buys": 0,
                    "events_seen": 0,
                    "buy_events": 0,
                    "sell_events": 0,
                    "copyable_buy_events": 0,
                    "rejected_buy_events": 0,
                    "paper_pnl_usd": 0.0,
                },
            )
            buy_events = int(raw.get("buy_events") or 0)
            copyable = int(raw.get("copyable_buy_events") or 0)
            item["events_seen"] += int(raw.get("events_seen") or 0)
            item["buy_events"] += buy_events
            item["sell_events"] += int(raw.get("sell_events") or 0)
            item["copyable_buy_events"] += copyable
            item["rejected_buy_events"] += int(raw.get("rejected_buy_events") or 0)
            if buy_events > 0:
                item["wallets_with_buy_sample"] += 1
            if copyable > 0:
                item["wallets_with_copyable_buys"] += 1
    for item in summary.values():
        buy_events = int(item.get("buy_events") or 0)
        item["copyable_rate_pct"] = round(100.0 * int(item.get("copyable_buy_events") or 0) / buy_events, 6) if buy_events else None
    return dict(sorted(summary.items()))


def _cached_book(
    clob: CLOBMarketClient,
    cache: dict[str, dict[str, Any]],
    asset: str,
    max_book_fetches: int,
) -> dict[str, Any] | None:
    if asset in cache:
        return cache[asset]
    if len(cache) >= max(0, int(max_book_fetches)):
        return None
    book = clob.get_book(asset)
    cache[asset] = book
    return book


def _score_buy(
    *,
    metric: dict[str, Any],
    positions: dict[str, dict[str, Any]],
    clob: CLOBMarketClient,
    book_cache: dict[str, dict[str, Any]],
    row: dict[str, Any],
    wallet: str,
    asset: str,
    price: float,
    size: float,
    copy_size_usd: float,
    slippage_bps: float,
    min_fill_ratio: float,
    max_book_fetches: int,
) -> dict[str, Any]:
    metric["buy_events"] += 1
    metric["paper_orders"] += 1
    if copy_size_usd <= 0:
        metric["below_min_order_events"] += 1
        metric["rejected_buy_events"] += 1
        _record_reject(metric, "copy_size_zero")
        return {"status": "REJECTED", "reason": "copy_size_zero"}
    book = _cached_book(clob, book_cache, asset, max_book_fetches)
    if book is None:
        metric["rejected_buy_events"] += 1
        _record_reject(metric, "clob_book_fetch_budget_exhausted")
        return {"status": "REJECTED", "reason": "clob_book_fetch_budget_exhausted"}
    summary = CLOBMarketClient.summarize_book(
        book,
        copy_size_usd=copy_size_usd,
        source_price=price,
        max_slippage_bps=slippage_bps,
    )
    top_of_book = _book_top_of_book(book)
    summary = {**summary, "top_of_book": top_of_book}
    if _book_unavailable_or_market_closed(top_of_book):
        summary = {
            **summary,
            "blocking_reason": "book_unavailable_or_market_closed",
            "instant_fill_status": "BLOCKED",
            "book_availability_status": "UNAVAILABLE_OR_MARKET_CLOSED",
        }
    fill_ratio = num(summary.get("fill_ratio"))
    metric["fill_ratio_sum"] = round(num(metric.get("fill_ratio_sum")) + fill_ratio, 6)
    if summary.get("instant_fill_status") != "PASS" or fill_ratio + 1e-9 < float(min_fill_ratio):
        metric["rejected_buy_events"] += 1
        reason = str(summary.get("blocking_reason") or "clob_fill_blocked")
        _record_reject(metric, reason)
        return {"status": "REJECTED", "reason": reason, "book": summary}
    filled_usd = num(summary.get("fillable_usd"))
    filled_shares = num(summary.get("fillable_shares"))
    fill_price = num(summary.get("avg_fill_price"))
    key = _position_key(wallet, asset)
    position = positions.get(key)
    if not isinstance(position, dict):
        position = {"wallet": wallet, "asset": asset, "shares": 0.0, "cost_usd": 0.0}
    position["shares"] = round(num(position.get("shares")) + filled_shares, 6)
    position["cost_usd"] = round(num(position.get("cost_usd")) + filled_usd, 6)
    position["avg_price"] = round(num(position.get("cost_usd")) / num(position.get("shares")), 6) if num(position.get("shares")) else 0.0
    position["mark_price"] = num(summary.get("best_bid"))
    position["mark_updated_at"] = utc_now_iso()
    position["last_tx"] = row.get("transaction_hash")
    positions[key] = position
    metric["copyable_buy_events"] += 1
    return {
        "status": "FILLED",
        "copy_size_usd": filled_usd,
        "filled_shares": filled_shares,
        "fill_price": fill_price,
        "book": summary,
    }


def _score_sell(
    *,
    metric: dict[str, Any],
    positions: dict[str, dict[str, Any]],
    clob: CLOBMarketClient,
    book_cache: dict[str, dict[str, Any]],
    wallet: str,
    asset: str,
    size: float,
    wallet_fraction: float,
    max_book_fetches: int,
) -> dict[str, Any]:
    metric["sell_events"] += 1
    key = _position_key(wallet, asset)
    position = positions.get(key)
    if not isinstance(position, dict) or num(position.get("shares")) <= 1e-9:
        return {"status": "RECORDED_ONLY", "reason": "no_open_position"}
    book = _cached_book(clob, book_cache, asset, max_book_fetches)
    if book is None:
        return {"status": "RECORDED_ONLY", "reason": "clob_book_fetch_budget_exhausted"}
    summary = CLOBMarketClient.summarize_book(book, copy_size_usd=1.0, source_price=0.5, max_slippage_bps=0.0)
    sell_price = num(summary.get("best_bid"))
    if sell_price <= 0:
        return {"status": "RECORDED_ONLY", "reason": "no_bid_liquidity"}
    reduce_shares = min(num(position.get("shares")), max(0.0, float(size) * float(wallet_fraction)))
    if reduce_shares <= 1e-9:
        return {"status": "RECORDED_ONLY", "reason": "zero_reduction"}
    avg_cost = num(position.get("cost_usd")) / num(position.get("shares")) if num(position.get("shares")) else 0.0
    proceeds = round(reduce_shares * sell_price, 6)
    cost_removed = round(reduce_shares * avg_cost, 6)
    metric["realized_pnl_usd"] = round(num(metric.get("realized_pnl_usd")) + proceeds - cost_removed, 6)
    position["shares"] = round(num(position.get("shares")) - reduce_shares, 6)
    position["cost_usd"] = round(max(0.0, num(position.get("cost_usd")) - cost_removed), 6)
    position["avg_price"] = round(num(position.get("cost_usd")) / num(position.get("shares")), 6) if num(position.get("shares")) else 0.0
    position["mark_price"] = sell_price
    position["mark_updated_at"] = utc_now_iso()
    positions[key] = position
    return {"status": "REDUCED", "proceeds_usd": proceeds, "realized_pnl_usd": round(proceeds - cost_removed, 6)}


def diagnose_liquidity(
    *,
    events: list[dict[str, Any]],
    clob: CLOBMarketClient,
    limit: int,
) -> dict[str, Any]:
    """Refetch CLOB books behind no-liquidity/route errors and classify truth."""

    candidates: dict[str, dict[str, Any]] = {}
    for event in events:
        result = event.get("result") if isinstance(event.get("result"), dict) else {}
        reason = str(result.get("reason") or "")
        error_type = str(result.get("error_type") or "")
        if reason not in {"no_ask_liquidity", "book_unavailable_or_market_closed"} and error_type != "PolymarketRouteError":
            continue
        asset = str(event.get("asset") or "")
        if not asset:
            continue
        row = candidates.setdefault(
            asset,
            {
                "asset": asset,
                "event_count": 0,
                "wallets": set(),
                "source_reasons": Counter(),
                "sample_events": [],
            },
        )
        row["event_count"] += 1
        row["wallets"].add(str(event.get("wallet") or ""))
        row["source_reasons"][reason or error_type or "unknown"] += 1
        if len(row["sample_events"]) < 3:
            row["sample_events"].append(
                {
                    "wallet": event.get("wallet"),
                    "source_price": event.get("source_price"),
                    "copy_size_usd": event.get("copy_size_usd"),
                    "transaction_hash": event.get("transaction_hash"),
                    "reason": reason,
                    "error_type": error_type,
                }
            )

    records: list[dict[str, Any]] = []
    classification_counts: Counter[str] = Counter()
    assets = list(candidates.values())[: max(0, int(limit))]
    for candidate in assets:
        asset = str(candidate["asset"])
        record = {
            "asset": asset,
            "event_count": int(candidate.get("event_count") or 0),
            "wallets": sorted(wallet for wallet in candidate.get("wallets", set()) if wallet),
            "source_reasons": dict(sorted((candidate.get("source_reasons") or Counter()).items())),
            "sample_events": candidate.get("sample_events") or [],
        }
        try:
            book = clob.get_book(asset)
            snapshot = _book_top_of_book(book)
            has_asks = num(snapshot.get("best_ask")) > 0 and num(snapshot.get("best_ask_depth_usd")) > 0
            source_reasons = set((candidate.get("source_reasons") or {}).keys())
            if _book_unavailable_or_market_closed(snapshot):
                classification = "book_unavailable_or_market_closed"
            elif not has_asks:
                classification = "empty_book_truth"
            elif "no_ask_liquidity" in source_reasons:
                classification = "stale_cache"
            else:
                classification = "route_bug"
            record.update(
                {
                    "refetch_status": "PASS",
                    "classification": classification,
                    "top_of_book": snapshot,
                }
            )
        except PolymarketRouteError as exc:
            classification = "route_bug"
            record.update(
                {
                    "refetch_status": "ROUTE_ERROR",
                    "classification": classification,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "route_report": exc.route_report if isinstance(exc.route_report, dict) else {},
                }
            )
        except Exception as exc:  # noqa: BLE001 - diagnosis must preserve bounded evidence.
            classification = "route_bug"
            record.update(
                {
                    "refetch_status": "ERROR",
                    "classification": classification,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                }
            )
        classification_counts[classification] += int(record.get("event_count") or 1)
        records.append(record)

    total_events = sum(int(row.get("event_count") or 0) for row in records)
    dominant = classification_counts.most_common(1)[0][0] if classification_counts else ""
    return {
        "enabled": True,
        "status": "ANALYZE" if records else "NO_TARGET_EVENTS",
        "updated_at": utc_now_iso(),
        "target_events": total_events,
        "target_assets": len(candidates),
        "diagnosed_assets": len(records),
        "classification_counts": dict(sorted(classification_counts.items())),
        "dominant_classification": dominant,
        "records": records,
        "next_action": (
            "treat empty-book assets as a wallet-selection/liquidity criterion"
            if dominant == "empty_book_truth"
            else "repair or route-around CLOB route instability" if dominant == "route_bug" else
            "avoid stale-cache book reuse for these assets" if dominant == "stale_cache" else
            "no no_ask_liquidity or route-error events were observed"
        ),
    }


def build_measurement_state(
    *,
    lane_state: dict[str, Any],
    polygon_rows: list[dict[str, Any]],
    clob: CLOBMarketClient,
    prior_state: dict[str, Any] | None = None,
    wallet_fraction: float | None = None,
    max_order_usd: float | None = None,
    min_order_usd: float | None = None,
    slippage_bps: float = 250.0,
    min_fill_ratio: float = 0.999,
    max_events: int = 500,
    max_book_fetches: int = 100,
    policy_id: str = "",
    floor_copy_size_to_min_order: bool = False,
    buy_events_only: bool = False,
    market_category: str = "",
    diagnose_liquidity_enabled: bool = False,
    diagnose_liquidity_limit: int = 100,
    max_receipt_to_fetch_age_s: float = 0.0,
    now_s: float | None = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    selected = _selected_wallet_rows(lane_state)
    defaults = _mission_sizing_defaults()
    wallet_fraction = defaults["wallet_fraction"] if wallet_fraction is None or wallet_fraction <= 0 else wallet_fraction
    max_order_usd = defaults["max_order_usd"] if max_order_usd is None or max_order_usd <= 0 else max_order_usd
    min_order_usd = defaults["min_order_usd"] if min_order_usd is None or min_order_usd <= 0 else min_order_usd
    policy_label = _policy_id(
        explicit=policy_id,
        wallet_fraction=wallet_fraction,
        max_order_usd=max_order_usd,
        min_order_usd=min_order_usd,
    )

    prior = prior_state if isinstance(prior_state, dict) else {}
    prior_wallets = prior.get("wallets") if isinstance(prior.get("wallets"), dict) else {}
    wallets = {
        wallet: {**_empty_wallet_metrics(wallet, lane_row), **(prior_wallets.get(wallet) if isinstance(prior_wallets.get(wallet), dict) else {})}
        for wallet, lane_row in selected.items()
    }
    positions = prior.get("positions") if isinstance(prior.get("positions"), dict) else {}
    positions = {str(key): dict(value) for key, value in positions.items() if isinstance(value, dict)}
    seen_ids = {str(item) for item in (prior.get("event_ids") or []) if item}
    new_events: list[dict[str, Any]] = []
    diagnostics = Counter()
    book_cache: dict[str, dict[str, Any]] = {}
    receipt_to_fetch_latencies_ms: list[float] = []
    clock_s = now_ts() if now_s is None else float(now_s)

    source_counts: Counter[str] = Counter()
    for row in polygon_rows:
        if len(new_events) >= max(0, int(max_events)):
            break
        event_type = str(row.get("event") or "")
        if event_type not in {"polygon_orderfilled_log", "rtds_trade_event", "wallet_copy_wallet_event"}:
            diagnostics["non_realtime_row"] += 1
            continue
        normalized = _normalized_realtime_event(row)
        if normalized is None:
            if event_type == "polygon_orderfilled_log" and row.get("source") != "polygon_ws":
                diagnostics["non_realtime_polygon_row"] += 1
            elif event_type == "polygon_orderfilled_log":
                decoded = row.get("decoded") if isinstance(row.get("decoded"), dict) else {}
                if decoded.get("decode_status") != "OK":
                    diagnostics["decode_not_ok"] += 1
                else:
                    diagnostics["missing_required_event_field"] += 1
            else:
                diagnostics["missing_required_event_field"] += 1
            continue
        decoded = normalized["decoded"]
        asset = str(normalized["asset"])
        price = num(normalized["price"])
        size = num(normalized["size"])
        tx = str(normalized["tx"])
        received_at_s = num(normalized.get("received_at_s"))
        if max_receipt_to_fetch_age_s > 0:
            if received_at_s <= 0:
                diagnostics["receipt_ts_missing_for_fresh_filter"] += 1
                continue
            receipt_age_s = max(0.0, clock_s - received_at_s)
            if receipt_age_s > float(max_receipt_to_fetch_age_s):
                diagnostics["receipt_to_fetch_age_gt_cap"] += 1
                continue
        source_counts[str(normalized["source"])] += 1
        for wallet, side in _wallet_sides(row, set(selected)):
            if bool(buy_events_only) and side != "BUY":
                diagnostics["sell_event_skipped_buy_only"] += 1
                continue
            metric = wallets[wallet]
            lane_row = selected.get(wallet, {})
            event_categories = _event_market_categories(row, decoded, lane_row)
            primary_event_category = event_categories[0] if event_categories else "unknown"
            copy_size = _copy_size_usd(price, size, wallet_fraction=wallet_fraction, max_order_usd=max_order_usd)
            legacy_event_id = "|".join([wallet, tx, str(normalized.get("source_event_id") or ""), asset, side])
            event_id = "|".join([policy_label, legacy_event_id])
            if event_id in seen_ids or legacy_event_id in seen_ids:
                diagnostics["duplicate_event"] += 1
                continue
            seen_ids.add(event_id)
            metric["events_seen"] += 1
            metric["last_event_ts"] = normalized.get("event_ts")
            metric["last_observed_ts"] = normalized.get("received_at_s")
            category_metric = _category_metric(metric, primary_event_category)
            if market_category and primary_event_category != market_category:
                diagnostics[f"market_category_skipped:{primary_event_category}"] += 1
                continue
            category_metric["events_seen"] = int(category_metric.get("events_seen") or 0) + 1
            floored_to_min = False
            try:
                if side == "BUY":
                    category_metric["buy_events"] = int(category_metric.get("buy_events") or 0) + 1
                    floored_to_min = bool(floor_copy_size_to_min_order and copy_size > 0.0 and copy_size < float(min_order_usd))
                    if floored_to_min:
                        copy_size = max(copy_size, float(min_order_usd))
                    if copy_size < float(min_order_usd):
                        metric["buy_events"] += 1
                        metric["paper_orders"] += 1
                        metric["below_min_order_events"] += 1
                        metric["rejected_buy_events"] += 1
                        _record_reject(metric, "copy_size_below_min_order")
                        result = {"status": "REJECTED", "reason": "copy_size_below_min_order"}
                    else:
                        result = _score_buy(
                            metric=metric,
                            positions=positions,
                            clob=clob,
                            book_cache=book_cache,
                            row=row,
                            wallet=wallet,
                            asset=asset,
                            price=price,
                            size=size,
                            copy_size_usd=copy_size,
                            slippage_bps=slippage_bps,
                            min_fill_ratio=min_fill_ratio,
                            max_book_fetches=max_book_fetches,
                        )
                    if result.get("status") == "FILLED":
                        category_metric["copyable_buy_events"] = int(category_metric.get("copyable_buy_events") or 0) + 1
                    elif result.get("status") in {"REJECTED", "ERROR"}:
                        category_metric["rejected_buy_events"] = int(category_metric.get("rejected_buy_events") or 0) + 1
                elif side == "SELL":
                    category_metric["sell_events"] = int(category_metric.get("sell_events") or 0) + 1
                    result = _score_sell(
                        metric=metric,
                        positions=positions,
                        clob=clob,
                        book_cache=book_cache,
                        wallet=wallet,
                        asset=asset,
                        size=size,
                        wallet_fraction=wallet_fraction,
                        max_book_fetches=max_book_fetches,
                    )
                else:
                    diagnostics[f"unsupported_side:{side}"] += 1
                    continue
            except Exception as exc:  # noqa: BLE001 - measurement evidence must persist errors.
                error_detail = _clob_exception_detail(exc)
                diagnostics[str(error_detail["reject_reason"])] += 1
                if side == "BUY":
                    metric["rejected_buy_events"] += 1
                    category_metric["rejected_buy_events"] = int(category_metric.get("rejected_buy_events") or 0) + 1
                    _record_reject(metric, str(error_detail["reject_reason"]))
                result = {"status": "ERROR", **error_detail}
            fetch_s = now_ts() if now_s is None else float(now_s)
            receipt_to_fetch_latency_ms = None
            if received_at_s > 0:
                receipt_to_fetch_latency_ms = round(max(0.0, fetch_s - received_at_s) * 1000.0, 3)
                receipt_to_fetch_latencies_ms.append(receipt_to_fetch_latency_ms)
            shadow_scoring = _shadow_scoring_fields(
                normalized=normalized,
                result=result if isinstance(result, dict) else {},
                source_price=price,
                fetch_s=fetch_s,
                receipt_to_fetch_latency_ms=receipt_to_fetch_latency_ms,
            )
            new_events.append(
                {
                    "event": "top10_broad_paper_measurement",
                    "captured_at": utc_now_iso(),
                    "policy_id": policy_label,
                    "paper_only": True,
                    "live_orders_allowed": False,
                    "wallet": wallet,
                    "side": side,
                    "market_categories": event_categories,
                    "primary_market_category": primary_event_category,
                    "asset": asset,
                    "source_price": price,
                    "source_size": size,
                    "copy_size_usd": copy_size,
                    "floor_copy_size_to_min_order": bool(floor_copy_size_to_min_order),
                    "copy_size_floored_to_min_order": bool(floored_to_min),
                    "transaction_hash": tx,
                    "block_ts": normalized.get("event_ts"),
                    "received_at_s": normalized.get("received_at_s"),
                    "book_fetch_started_at_s": round(fetch_s, 6),
                    "receipt_to_fetch_latency_ms": receipt_to_fetch_latency_ms,
                    "realtime_shadow_score": shadow_scoring,
                    "source": normalized.get("source"),
                    "result": result,
                }
            )

    _mark_positions(wallets, positions)
    ranked_wallets = sorted(
        wallets.values(),
        key=lambda row: (
            -num(row.get("paper_pnl_usd")),
            -num(row.get("copyable_rate_pct")),
            -(int(row.get("buy_events") or 0)),
            int(row.get("leaderboard_rank") or 999999),
            str(row.get("wallet") or ""),
        ),
    )
    sample_buy_wallets = sum(1 for row in ranked_wallets if int(row.get("buy_events") or 0) > 0)
    total_buy_events = sum(int(row.get("buy_events") or 0) for row in ranked_wallets)
    total_copyable_buy_events = sum(int(row.get("copyable_buy_events") or 0) for row in ranked_wallets)
    total_paper_pnl_usd = round(sum(num(row.get("paper_pnl_usd")) for row in ranked_wallets), 6)
    market_category_summary = _summarize_measurement_market_categories(ranked_wallets)
    latency_summary = {
        "enabled": max_receipt_to_fetch_age_s > 0,
        "max_receipt_to_fetch_age_s": float(max_receipt_to_fetch_age_s),
        "events_with_latency": len(receipt_to_fetch_latencies_ms),
        "max_ms": round(max(receipt_to_fetch_latencies_ms), 3) if receipt_to_fetch_latencies_ms else None,
        "min_ms": round(min(receipt_to_fetch_latencies_ms), 3) if receipt_to_fetch_latencies_ms else None,
    }
    blockers: list[str] = []
    if not sample_buy_wallets:
        blockers.append("top10_realtime_buy_sample_missing")
    else:
        if total_copyable_buy_events <= 0:
            blockers.append("top10_no_copyable_buys")
        if total_paper_pnl_usd <= 0.0:
            blockers.append("top10_paper_pnl_non_positive")
    status = "WATCH" if sample_buy_wallets else "ANALYZE"
    liquidity_diagnosis = (
        diagnose_liquidity(events=new_events, clob=clob, limit=int(diagnose_liquidity_limit))
        if diagnose_liquidity_enabled
        else {"enabled": False, "status": "SKIPPED"}
    )
    return {
        "schema_version": 1,
        "kind": "wallet_copy_top10_broad_paper_measurement_state",
        "flow_stage": "OBSERVE",
        "status": status,
        "updated_at": utc_now_iso(),
        "policy_id": policy_label,
        "paper_only": True,
        "live_orders_allowed": False,
        "copyintents_created": 0,
        "orders_submitted": 0,
        "sizing": {
            "policy_id": policy_label,
            "wallet_fraction": wallet_fraction,
            "max_order_usd": max_order_usd,
            "min_order_usd": min_order_usd,
            "slippage_bps": slippage_bps,
            "min_fill_ratio": min_fill_ratio,
            "floor_copy_size_to_min_order": bool(floor_copy_size_to_min_order),
            "buy_events_only": bool(buy_events_only),
            "market_category": market_category,
        },
        "source": {
            "input": "realtime_activity_jsonl",
            "selected_wallets": len(selected),
            "rows_scanned": len(polygon_rows),
            "new_events": len(new_events),
            "book_fetches": len(book_cache),
            "max_book_fetches": max_book_fetches,
            "source_counts": dict(sorted(source_counts.items())),
        },
        "summary": {
            "wallets": len(ranked_wallets),
            "wallets_with_realtime_events": sum(1 for row in ranked_wallets if int(row.get("events_seen") or 0) > 0),
            "wallets_with_buy_sample": sample_buy_wallets,
            "buy_events": total_buy_events,
            "copyable_buy_events": total_copyable_buy_events,
            "paper_pnl_usd": total_paper_pnl_usd,
            "diagnostics": dict(sorted(diagnostics.items())),
            "market_categories": market_category_summary,
            "receipt_to_fetch_latency": latency_summary,
        },
        "liquidity_diagnosis": liquidity_diagnosis,
        "ranked_wallets": ranked_wallets,
        "wallets": {str(row.get("wallet")): row for row in ranked_wallets},
        "positions": positions,
        "event_ids": sorted(seen_ids)[-250_000:],
        "blockers": blockers,
        "next_action": (
            "feed this measurement into the top-10 selector paper gate; cohort is not rotation-ready until a live-executable policy is both positive and copyable"
            if sample_buy_wallets and blockers
            else "compare observed paper PnL and copyable_rate against the live wallet for rotation"
            if sample_buy_wallets
            else "keep the top-10 paper lane running until selected wallets produce realtime BUY samples"
        ),
    }, new_events


def main() -> int:
    args = parse_args()
    prior_source_base = _disable_source_base_overrides(bool(args.disable_source_base_overrides))
    try:
        lane_state = load_json(args.lane_state, default={})
        if not isinstance(lane_state, dict):
            lane_state = {}
        prior_state = {} if bool(args.ignore_prior_state) else load_json(args.output, default={})
        if not isinstance(prior_state, dict):
            prior_state = {}
        clob = CLOBMarketClient(host=args.clob_base_url, timeout_s=float(args.clob_timeout_s))
        realtime_jsonl = str(args.rtds_jsonl or args.polygon_jsonl)
        state, new_events = build_measurement_state(
            lane_state=lane_state,
            polygon_rows=_iter_recent_jsonl(realtime_jsonl, int(args.scan_limit)),
            clob=clob,
            prior_state=prior_state,
            wallet_fraction=float(args.wallet_fraction) if args.wallet_fraction else None,
            max_order_usd=float(args.max_order_usd) if args.max_order_usd else None,
            min_order_usd=float(args.min_order_usd) if args.min_order_usd else None,
            slippage_bps=float(args.slippage_bps),
            min_fill_ratio=float(args.min_fill_ratio),
            max_events=int(args.max_events),
            max_book_fetches=int(args.max_book_fetches),
            policy_id=str(args.policy_id or ""),
            floor_copy_size_to_min_order=bool(args.floor_copy_size_to_min_order),
            buy_events_only=bool(args.buy_events_only),
            market_category=str(args.market_category or ""),
            diagnose_liquidity_enabled=bool(args.diagnose_liquidity),
            diagnose_liquidity_limit=int(args.diagnose_liquidity_limit),
            max_receipt_to_fetch_age_s=float(args.max_receipt_to_fetch_age_s),
        )
        state.setdefault("source", {})["path"] = realtime_jsonl
        state.setdefault("source", {})["source_base_overrides_disabled"] = bool(args.disable_source_base_overrides)
        if str(args.positive_control_json or "").strip():
            control = load_json(str(args.positive_control_json), default={})
            state["positive_control"] = control if isinstance(control, dict) else {"status": "INVALID_CONTROL_JSON"}
        atomic_write_json(args.output, state)
        if new_events:
            append_jsonl_many(args.event_log, new_events)
        print(json.dumps(state, indent=2, sort_keys=True))
        return 0 if state["status"] in {"WATCH", "ANALYZE"} else 2
    finally:
        _restore_source_base_overrides(prior_source_base)


if __name__ == "__main__":
    raise SystemExit(main())
