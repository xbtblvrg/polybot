#!/usr/bin/env python3
"""Compare an independent self-wallet trade feed against the live ledger.

Flow stage: LIVE/SELF-DEV. This is a read-only truth loop for our own wallet:
Data API trades plus recent Polygon OrderFilled logs are treated as the
external self-feed and are diffed against ledger FILLED rows.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.models import num, utc_now_iso
from src.wallet_copy.performance import load_resolutions
from src.wallet_copy.pnl_truth import winner_from_resolution
from src.wallet_copy.polymarket_addresses import EXCHANGE_ADDRESSES
from src.wallet_copy.realtime_feed import decode_polygon_orderfilled_v2
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json
from src.wallet_copy.scorecard import load_fresh_scorecard


DEFAULT_LEDGER = "data/research/wallet_copy_live_execution_state.json"
DEFAULT_SCORECARD = "data/research/wallet_copy_daily_scorecard_current.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_OUTPUT = "data/research/wallet_copy_self_feed_vs_ledger_latest.json"
DEFAULT_SELF_FEED_LOG = "data/research/wallet_copy_self_trades.jsonl"
DEFAULT_DATA_API_BASE = "https://data-api.polymarket.com"
DEFAULT_POLYGON_RPC_URL = "https://polygon-bor-rpc.publicnode.com"

ORDER_FILLED_TOPIC0 = "0xd543adfd945773f1a62f74f0ee55a5e3b9b1a28262980ba90b1a89f2ea84d8ee"

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", default=DEFAULT_LEDGER)
    parser.add_argument("--scorecard", default=DEFAULT_SCORECARD)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument("--self-feed-log", default=DEFAULT_SELF_FEED_LOG)
    parser.add_argument("--user", default="")
    parser.add_argument("--start-iso", default="")
    parser.add_argument("--end-iso", default="")
    parser.add_argument("--day", default="")
    parser.add_argument("--limit", type=int, default=200)
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--timeout-s", type=float, default=5.0)
    parser.add_argument("--ledger-missing-grace-s", type=float, default=300.0)
    parser.add_argument("--data-api-base-url", default=DEFAULT_DATA_API_BASE)
    parser.add_argument("--polygon-rpc-url", default=os.getenv("POLYGON_RPC_URL", DEFAULT_POLYGON_RPC_URL))
    parser.add_argument("--polygon-lookback-blocks", type=int, default=700)
    parser.add_argument("--enable-polygon", action=argparse.BooleanOptionalAction, default=True)
    return parser.parse_args()


def _norm_addr(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if text.startswith("0x") and len(text) == 42 else ""


def _load_dotenv_value(name: str) -> str:
    value = os.getenv(name, "").strip()
    if value:
        return value
    env_path = ROOT / ".env"
    if not env_path.exists():
        return ""
    for line in env_path.read_text(encoding="utf-8").splitlines():
        if line.startswith(f"{name}="):
            return line.split("=", 1)[1].strip().strip('"').strip("'")
    return ""


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "")
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return num(text, 0.0)


def _iso(ts: float | None) -> str | None:
    if ts is None or ts <= 0:
        return None
    return datetime.fromtimestamp(float(ts), tz=UTC).isoformat().replace("+00:00", "Z")


def _window_from_args(args: argparse.Namespace) -> tuple[float, float, str]:
    end_ts = _parse_ts(args.end_iso) if args.end_iso else time.time()
    if args.start_iso:
        return _parse_ts(args.start_iso), end_ts, "explicit_start_iso"
    scorecard = load_fresh_scorecard(args.scorecard)
    chain = scorecard.get("chain_reconciliation") if isinstance(scorecard, dict) else {}
    start_ts = _parse_ts(chain.get("reconciliation_start_iso") if isinstance(chain, dict) else None)
    if start_ts > 0:
        return start_ts, end_ts, "scorecard_reconciliation_start"
    if args.day:
        start = datetime.fromisoformat(str(args.day)).replace(tzinfo=UTC)
    else:
        now = datetime.now(tz=UTC)
        start = datetime(now.year, now.month, now.day, tzinfo=UTC)
    return start.timestamp(), end_ts, "utc_day_start"


def _order_tx(order: dict[str, Any]) -> str:
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    details = trade_result.get("details") if isinstance(trade_result.get("details"), dict) else {}
    tx_hashes = trade_result.get("tx_hashes") if isinstance(trade_result.get("tx_hashes"), list) else []
    detail_hashes = details.get("transactionsHashes") if isinstance(details.get("transactionsHashes"), list) else []
    candidates = [
        *tx_hashes,
        *detail_hashes,
        trade_result.get("transaction_hash"),
        order.get("transaction_hash"),
    ]
    return next((str(value).lower() for value in candidates if value), "")


def _ledger_cost(order: dict[str, Any]) -> float:
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    for key in ("actual_trade_cost_usd", "response_filled_size_usd", "making_amount", "size_usd"):
        value = num(trade_result.get(key), 0.0)
        if value > 0:
            return value
    for key in ("actual_trade_cost_usd", "filled_size_usd", "size_usd"):
        value = num(order.get(key), 0.0)
        if value > 0:
            return value
    price = _ledger_price(order)
    size = _ledger_size(order)
    return price * size if price > 0 and size > 0 else 0.0


def _ledger_price(order: dict[str, Any]) -> float:
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    for key in ("response_fill_price", "entry_price", "filled_price"):
        value = num(trade_result.get(key), 0.0)
        if value > 0:
            return value
    return num(order.get("price"), 0.0) or num(order.get("limit_price"), 0.0)


def _ledger_size(order: dict[str, Any]) -> float:
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    for key in ("response_fill_size_shares", "taking_amount", "order_size"):
        value = num(trade_result.get(key), 0.0)
        if value > 0:
            return value
    for key in ("shares", "filled_size", "size"):
        value = num(order.get(key), 0.0)
        if value > 0:
            return value
    return 0.0


def _normalize_ledger_fill(order: dict[str, Any]) -> dict[str, Any]:
    trade_result = order.get("trade_result") if isinstance(order.get("trade_result"), dict) else {}
    ts = _parse_ts(order.get("submitted_at") or order.get("updated_at") or trade_result.get("timestamp"))
    return {
        "source": "live_execution_ledger",
        "tx": _order_tx(order),
        "order_id": str(order.get("order_id") or trade_result.get("order_id") or ""),
        "token_id": str(order.get("token_id") or order.get("asset_id") or ""),
        "condition_id": str(order.get("condition_id") or trade_result.get("market_id") or ""),
        "market_slug": str(order.get("market_slug") or ""),
        "outcome": str(order.get("outcome") or ""),
        "side": str(trade_result.get("side") or "BUY").upper(),
        "price": round(_ledger_price(order), 10),
        "size": round(_ledger_size(order), 6),
        "cost_usd": round(_ledger_cost(order), 6),
        "event_ts": ts,
        "event_iso": _iso(ts),
        "raw_status": str(order.get("final_status") or order.get("status") or ""),
    }


def _fetch_data_api_trades(
    *,
    user: str,
    start_ts: float,
    end_ts: float,
    limit: int,
    max_pages: int,
    timeout_s: float,
    base_url: str,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not user:
        return [], {"status": "UNAVAILABLE", "reason": "user_missing"}
    rows: list[dict[str, Any]] = []
    urls: list[str] = []
    for page in range(max(1, int(max_pages))):
        query = urllib.parse.urlencode({"user": user, "takerOnly": "false", "limit": int(limit), "offset": page * int(limit)})
        url = f"{base_url.rstrip('/')}/trades?{query}"
        urls.append(url)
        request = urllib.request.Request(url, headers={"User-Agent": "wallet-copy-self-feed/1.0", "Accept": "application/json"})
        with urllib.request.urlopen(request, timeout=float(timeout_s)) as response:
            payload = json.loads(response.read().decode("utf-8"))
        page_rows = [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
        for row in page_rows:
            ts = _parse_ts(row.get("timestamp"))
            if start_ts <= ts <= end_ts:
                rows.append(row)
        if len(page_rows) < int(limit):
            return rows, {"status": "OK", "pages": page + 1, "truncated": False, "urls": urls}
        oldest_ts = min((_parse_ts(row.get("timestamp")) for row in page_rows), default=0.0)
        if oldest_ts and oldest_ts < start_ts:
            return rows, {"status": "OK", "pages": page + 1, "truncated": False, "urls": urls}
    return rows, {"status": "OK", "pages": int(max_pages), "truncated": True, "urls": urls}


def _normalize_data_api_trade(row: dict[str, Any]) -> dict[str, Any]:
    price = num(row.get("price"), 0.0)
    size = num(row.get("size"), 0.0)
    ts = _parse_ts(row.get("timestamp"))
    return {
        "source": "data_api_trades_user",
        "tx": str(row.get("transactionHash") or row.get("transaction_hash") or "").lower(),
        "order_id": str(row.get("orderId") or row.get("order_id") or row.get("clobOrderId") or ""),
        "token_id": str(row.get("asset") or row.get("tokenId") or row.get("token_id") or ""),
        "condition_id": str(row.get("conditionId") or row.get("condition_id") or ""),
        "market_slug": str(row.get("slug") or row.get("eventSlug") or ""),
        "outcome": str(row.get("outcome") or ""),
        "side": str(row.get("side") or "").upper(),
        "price": round(price, 10),
        "size": round(size, 6),
        "cost_usd": round(price * size, 6),
        "event_ts": ts,
        "event_iso": _iso(ts),
        "raw": row,
    }


def _outcome_side(outcome: Any) -> str:
    text = str(outcome or "").upper()
    if text in {"UP", "YES"}:
        return "YES"
    if text in {"DOWN", "NO"}:
        return "NO"
    return text


def _rpc_post(url: str, method: str, params: list[Any], *, timeout_s: float) -> Any:
    payload = json.dumps({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json", "User-Agent": "wallet-copy-self-feed/1.0"},
    )
    with urllib.request.urlopen(request, timeout=float(timeout_s)) as response:
        loaded = json.loads(response.read().decode("utf-8"))
    if isinstance(loaded, dict) and loaded.get("error"):
        raise RuntimeError(loaded["error"])
    return loaded.get("result") if isinstance(loaded, dict) else None


def _hex_int(value: Any) -> int | None:
    text = str(value or "")
    if not text.startswith("0x"):
        return None
    try:
        return int(text, 16)
    except ValueError:
        return None


def _topic_address(value: Any) -> str:
    text = str(value or "").lower()
    if text.startswith("0x") and len(text) == 66:
        return "0x" + text[-40:]
    return ""


def _address_topic(address: str) -> str:
    return "0x" + ("0" * 24) + address.lower().removeprefix("0x")


def _block_ts(rpc_url: str, block_number: int | None, cache: dict[int, float], timeout_s: float) -> float | None:
    if block_number is None:
        return None
    if block_number in cache:
        return cache[block_number]
    block = _rpc_post(rpc_url, "eth_getBlockByNumber", [hex(block_number), False], timeout_s=timeout_s)
    if not isinstance(block, dict):
        return None
    ts = _hex_int(block.get("timestamp"))
    if ts is None:
        return None
    cache[block_number] = float(ts)
    return float(ts)


def _normalize_polygon_log(
    row: dict[str, Any],
    *,
    user: str,
    rpc_url: str,
    timeout_s: float,
    block_ts_cache: dict[int, float],
) -> dict[str, Any] | None:
    topics = row.get("topics") if isinstance(row.get("topics"), list) else []
    if len(topics) < 4:
        return None
    maker = _topic_address(topics[2])
    taker = _topic_address(topics[3])
    if user not in {maker, taker}:
        return None
    block_number = _hex_int(row.get("blockNumber"))
    ts = _block_ts(rpc_url, block_number, block_ts_cache, timeout_s)
    summary = {
        "source": "polygon_http_getLogs_self",
        "address": str(row.get("address") or "").lower(),
        "transaction_hash": str(row.get("transactionHash") or "").lower(),
        "block_number": block_number,
        "block_ts": ts,
        "log_index": _hex_int(row.get("logIndex")),
        "topic0": topics[0] if topics else "",
        "topic1": topics[1] if len(topics) > 1 else "",
        "order_hash": topics[1] if len(topics) > 1 else "",
        "maker": maker,
        "taker": taker,
        "selected_wallet": user,
        "topics": topics,
        "data": row.get("data"),
    }
    decoded = decode_polygon_orderfilled_v2(summary, exchange_addresses=set(EXCHANGE_ADDRESSES))
    price = num(decoded.get("price"), 0.0)
    size = num(decoded.get("size"), 0.0)
    return {
        "source": "polygon_orderfilled_self",
        "tx": summary["transaction_hash"],
        "order_id": str(decoded.get("order_hash") or summary["order_hash"] or ""),
        "token_id": str(decoded.get("asset") or ""),
        "condition_id": str(decoded.get("condition_id") or ""),
        "market_slug": "",
        "outcome": "",
        "side": str(decoded.get("side") or "").upper(),
        "price": round(price, 10) if price else None,
        "size": round(size, 6) if size else None,
        "cost_usd": round(price * size, 6) if price and size else None,
        "event_ts": ts or 0.0,
        "event_iso": _iso(ts),
        "maker": maker,
        "taker": taker,
        "block_number": block_number,
        "log_index": summary["log_index"],
        "decode_status": decoded.get("decode_status"),
        "raw": {"log": row, "decoded": decoded},
    }


def _fetch_polygon_self_logs(
    *,
    user: str,
    rpc_url: str,
    start_ts: float,
    end_ts: float,
    lookback_blocks: int,
    timeout_s: float,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not user:
        return [], {"status": "UNAVAILABLE", "reason": "user_missing"}
    latest_hex = _rpc_post(rpc_url, "eth_blockNumber", [], timeout_s=timeout_s)
    latest = int(str(latest_hex), 16)
    from_block = max(0, latest - max(1, int(lookback_blocks)))
    user_topic = _address_topic(user)
    filter_specs = [
        ("maker", [ORDER_FILLED_TOPIC0, None, user_topic]),
        ("taker", [ORDER_FILLED_TOPIC0, None, None, user_topic]),
    ]
    rows: list[dict[str, Any]] = []
    block_ts_cache: dict[int, float] = {}
    calls = 0
    for role, topics in filter_specs:
        params = [
            {
                "fromBlock": hex(from_block),
                "toBlock": hex(latest),
                "address": list(EXCHANGE_ADDRESSES),
                "topics": topics,
            }
        ]
        calls += 1
        payload = _rpc_post(rpc_url, "eth_getLogs", params, timeout_s=timeout_s)
        for raw in payload or []:
            if not isinstance(raw, dict):
                continue
            row = _normalize_polygon_log(raw, user=user, rpc_url=rpc_url, timeout_s=timeout_s, block_ts_cache=block_ts_cache)
            if not row:
                continue
            row["matched_topic_role"] = role
            ts = float(row.get("event_ts") or 0.0)
            if ts <= 0 or start_ts <= ts <= end_ts:
                rows.append(row)
    deduped: dict[tuple[str, int | None], dict[str, Any]] = {}
    for row in rows:
        deduped[(str(row.get("tx") or ""), row.get("log_index") if isinstance(row.get("log_index"), int) else None)] = row
    return list(deduped.values()), {
        "status": "OK",
        "latest_block": latest,
        "from_block": from_block,
        "lookback_blocks": int(lookback_blocks),
        "calls": calls,
    }


def _aggregate_trade_rows(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for row in rows:
        tx = str(row.get("tx") or "").lower()
        if not tx:
            continue
        item = grouped.setdefault(
            tx,
            {
                "tx": tx,
                "rows": 0,
                "sources": set(),
                "order_ids": set(),
                "token_ids": set(),
                "condition_ids": set(),
                "market_slugs": set(),
                "outcomes": set(),
                "sides": set(),
                "size": 0.0,
                "cost_usd": 0.0,
                "prices": [],
                "min_event_ts": None,
                "max_event_ts": None,
            },
        )
        item["rows"] += 1
        item["sources"].add(str(row.get("source") or ""))
        for source_key, target_key in (
            ("order_id", "order_ids"),
            ("token_id", "token_ids"),
            ("condition_id", "condition_ids"),
            ("market_slug", "market_slugs"),
            ("outcome", "outcomes"),
            ("side", "sides"),
        ):
            value = str(row.get(source_key) or "")
            if value:
                item[target_key].add(value)
        item["size"] += num(row.get("size"), 0.0)
        item["cost_usd"] += num(row.get("cost_usd"), 0.0)
        price = num(row.get("price"), 0.0)
        if price > 0:
            item["prices"].append(price)
        ts = num(row.get("event_ts"), 0.0)
        if ts > 0:
            item["min_event_ts"] = ts if item["min_event_ts"] is None else min(item["min_event_ts"], ts)
            item["max_event_ts"] = ts if item["max_event_ts"] is None else max(item["max_event_ts"], ts)
    normalized: dict[str, dict[str, Any]] = {}
    for tx, item in grouped.items():
        prices = item["prices"]
        normalized[tx] = {
            "tx": tx,
            "rows": int(item["rows"]),
            "sources": sorted(value for value in item["sources"] if value),
            "order_ids": sorted(value for value in item["order_ids"] if value),
            "token_ids": sorted(value for value in item["token_ids"] if value),
            "condition_ids": sorted(value for value in item["condition_ids"] if value),
            "market_slugs": sorted(value for value in item["market_slugs"] if value),
            "outcomes": sorted(value for value in item["outcomes"] if value),
            "sides": sorted(value for value in item["sides"] if value),
            "size": round(float(item["size"]), 6),
            "cost_usd": round(float(item["cost_usd"]), 6),
            "avg_price": round(sum(prices) / len(prices), 10) if prices else None,
            "min_event_ts": item["min_event_ts"],
            "max_event_ts": item["max_event_ts"],
        }
    return normalized


def _event_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row.get("source") or ""),
            str(row.get("tx") or "").lower(),
            str(row.get("order_id") or ""),
            str(row.get("token_id") or ""),
            str(row.get("side") or ""),
            str(row.get("price") or ""),
            str(row.get("size") or ""),
        ]
    )


def _existing_event_keys(path: str | Path, *, max_lines: int = 200_000) -> set[str]:
    target = Path(path)
    if not target.exists():
        return set()
    keys: set[str] = set()
    try:
        lines = target.read_text(encoding="utf-8").splitlines()[-max_lines:]
    except OSError:
        return set()
    for line in lines:
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            keys.add(_event_key(row))
    return keys


def _persist_self_feed(path: str | Path, rows: list[dict[str, Any]]) -> int:
    existing = _existing_event_keys(path)
    new_rows: list[dict[str, Any]] = []
    captured_at = utc_now_iso()
    for row in rows:
        key = _event_key(row)
        if key in existing:
            continue
        existing.add(key)
        new_rows.append({"captured_at": captured_at, "flow_stage": "LIVE/SELF-DEV", **row})
    if new_rows:
        append_jsonl_many(path, new_rows)
    return len(new_rows)


def _dedupe_event_rows(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    seen: set[str] = set()
    deduped: list[dict[str, Any]] = []
    duplicates = 0
    for row in rows:
        key = _event_key(row)
        if key and key in seen:
            duplicates += 1
            continue
        if key:
            seen.add(key)
        deduped.append(row)
    return deduped, duplicates


def _compare_price(a: Any, b: Any, tolerance: float = 0.0001) -> bool:
    left = num(a, 0.0)
    right = num(b, 0.0)
    if left <= 0 or right <= 0:
        return True
    return abs(left - right) <= tolerance


def _overlaps(left: list[Any] | None, right: list[Any] | None) -> bool:
    left_set = {str(value) for value in (left or []) if str(value)}
    right_set = {str(value) for value in (right or []) if str(value)}
    return bool(left_set and right_set and left_set.intersection(right_set))


def _probable_split_fill_groups(
    amount_mismatch_rows: list[dict[str, Any]],
    self_missing_ledger_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    groups: list[dict[str, Any]] = []
    used_missing: set[str] = set()
    for mismatch in amount_mismatch_rows:
        ledger = mismatch.get("ledger") if isinstance(mismatch.get("ledger"), dict) else {}
        missing_cost = abs(num(mismatch.get("cost_delta_usd"), 0.0))
        if missing_cost <= 0:
            continue
        ledger_ts = num(ledger.get("max_event_ts"), 0.0)
        matches: list[dict[str, Any]] = []
        for row in self_missing_ledger_rows:
            self_item = row.get("self_feed") if isinstance(row.get("self_feed"), dict) else {}
            tx = str(self_item.get("tx") or row.get("tx") or "")
            if not tx or tx in used_missing:
                continue
            if not _overlaps(ledger.get("condition_ids"), self_item.get("condition_ids")):
                continue
            if ledger.get("market_slugs") and self_item.get("market_slugs") and not _overlaps(
                ledger.get("market_slugs"), self_item.get("market_slugs")
            ):
                continue
            if ledger.get("outcomes") and self_item.get("outcomes") and not _overlaps(
                ledger.get("outcomes"), self_item.get("outcomes")
            ):
                continue
            self_ts = num(self_item.get("max_event_ts"), 0.0)
            if ledger_ts > 0 and self_ts > 0 and abs(ledger_ts - self_ts) > 300.0:
                continue
            if abs(missing_cost - num(self_item.get("cost_usd"), 0.0)) > 0.05:
                continue
            matches.append({"tx": tx, "cost_usd": self_item.get("cost_usd"), "size": self_item.get("size")})
        if not matches:
            continue
        for match in matches:
            used_missing.add(str(match.get("tx") or ""))
        groups.append(
            {
                "classification": "probable_split_fill_or_incomplete_ledger_tx_hashes",
                "ledger_tx": mismatch.get("tx"),
                "ledger_order_ids": ledger.get("order_ids"),
                "ledger_cost_usd": ledger.get("cost_usd"),
                "ledger_size": ledger.get("size"),
                "data_api_cost_on_ledger_tx_usd": (mismatch.get("self_feed") or {}).get("cost_usd")
                if isinstance(mismatch.get("self_feed"), dict)
                else None,
                "cost_delta_usd": mismatch.get("cost_delta_usd"),
                "missing_companion_self_feed_txs": matches,
                "condition_ids": ledger.get("condition_ids"),
                "market_slugs": ledger.get("market_slugs"),
                "outcomes": ledger.get("outcomes"),
            }
        )
    return groups


def _self_missing_pnl_summary(
    self_missing_ledger_rows: list[dict[str, Any]],
    resolutions: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    cost_total = 0.0
    payout_total = 0.0
    resolved = 0
    unresolved = 0
    rows: list[dict[str, Any]] = []
    for row in self_missing_ledger_rows:
        self_item = row.get("self_feed") if isinstance(row.get("self_feed"), dict) else {}
        cost = num(self_item.get("cost_usd"), 0.0)
        size = num(self_item.get("size"), 0.0)
        condition_ids = self_item.get("condition_ids") if isinstance(self_item.get("condition_ids"), list) else []
        outcomes = self_item.get("outcomes") if isinstance(self_item.get("outcomes"), list) else []
        condition_id = str(condition_ids[0] if condition_ids else "")
        outcome = str(outcomes[0] if outcomes else "")
        winner = winner_from_resolution(resolutions.get(condition_id))
        is_resolved = bool(winner)
        payout = size if is_resolved and _outcome_side(outcome) == winner else 0.0
        pnl = round(payout - cost, 6) if is_resolved else None
        cost_total += cost
        if is_resolved:
            resolved += 1
            payout_total += payout
        else:
            unresolved += 1
        rows.append(
            {
                "tx": row.get("tx") or self_item.get("tx"),
                "condition_id": condition_id,
                "market_slugs": self_item.get("market_slugs"),
                "outcome": outcome,
                "winner": winner,
                "resolved": is_resolved,
                "cost_usd": round(cost, 6),
                "size": round(size, 6),
                "payout_usd": round(payout, 6) if is_resolved else None,
                "pnl_usd": pnl,
            }
        )
    return {
        "tx_groups": len(self_missing_ledger_rows),
        "resolved_tx_groups": resolved,
        "unresolved_tx_groups": unresolved,
        "cost_usd": round(cost_total, 6),
        "payout_usd": round(payout_total, 6),
        "pnl_usd": round(payout_total - cost_total, 6),
        "rows": sorted(
            rows,
            key=lambda item: abs(num(item.get("pnl_usd"), 0.0)) if item.get("pnl_usd") is not None else num(item.get("cost_usd"), 0.0),
            reverse=True,
        )[:50],
    }


def build_report(args: argparse.Namespace) -> dict[str, Any]:
    profile_started = time.perf_counter()
    profile_checkpoint = profile_started
    profile_stages: list[dict[str, Any]] = []

    def mark_profile(name: str) -> None:
        nonlocal profile_checkpoint
        now = time.perf_counter()
        profile_stages.append({"name": name, "duration_s": round(now - profile_checkpoint, 6)})
        profile_checkpoint = now

    start_ts, end_ts, window_source = _window_from_args(args)
    user = _norm_addr(args.user) or _norm_addr(_load_dotenv_value("POLYMARKET_PROXY"))
    ledger_payload = load_json(args.ledger, default={})
    ledger_payload = ledger_payload if isinstance(ledger_payload, dict) else {}
    all_orders = [row for row in ledger_payload.get("orders") or [] if isinstance(row, dict)]
    ledger_fills = [
        _normalize_ledger_fill(order)
        for order in all_orders
        if str(order.get("final_status") or order.get("status") or "").upper() == "FILLED"
        and start_ts <= _parse_ts(order.get("submitted_at") or order.get("updated_at")) <= end_ts
    ]
    mark_profile("load_and_normalize_ledger")
    try:
        data_api_raw, data_api_fetch = _fetch_data_api_trades(
            user=user,
            start_ts=start_ts,
            end_ts=end_ts,
            limit=int(args.limit),
            max_pages=int(args.max_pages),
            timeout_s=float(args.timeout_s),
            base_url=str(args.data_api_base_url or DEFAULT_DATA_API_BASE),
        )
        data_api_rows = [_normalize_data_api_trade(row) for row in data_api_raw]
    except Exception as exc:  # noqa: BLE001 - self-feed report must persist degraded source state.
        data_api_rows = []
        data_api_fetch = {"status": "ERROR", "error_type": type(exc).__name__, "error": str(exc)}
    mark_profile("fetch_data_api")
    polygon_rows: list[dict[str, Any]] = []
    if bool(getattr(args, "enable_polygon", True)):
        try:
            polygon_rows, polygon_fetch = _fetch_polygon_self_logs(
                user=user,
                rpc_url=str(args.polygon_rpc_url or DEFAULT_POLYGON_RPC_URL),
                start_ts=start_ts,
                end_ts=end_ts,
                lookback_blocks=int(args.polygon_lookback_blocks),
                timeout_s=float(args.timeout_s),
            )
        except Exception as exc:  # noqa: BLE001
            polygon_fetch = {"status": "ERROR", "error_type": type(exc).__name__, "error": str(exc)}
    else:
        polygon_fetch = {"status": "DISABLED"}
    mark_profile("fetch_polygon")

    data_api_rows_raw = list(data_api_rows)
    polygon_rows_raw = list(polygon_rows)
    data_api_rows, data_api_duplicate_event_rows = _dedupe_event_rows(data_api_rows_raw)
    polygon_rows, polygon_duplicate_event_rows = _dedupe_event_rows(polygon_rows_raw)
    self_feed_rows, self_feed_duplicate_event_rows = _dedupe_event_rows(data_api_rows + polygon_rows)
    appended = _persist_self_feed(args.self_feed_log, self_feed_rows)
    ledger_by_tx = _aggregate_trade_rows(ledger_fills)
    self_by_tx = _aggregate_trade_rows(self_feed_rows)
    data_api_by_tx = _aggregate_trade_rows(data_api_rows)
    polygon_by_tx = _aggregate_trade_rows(polygon_rows)
    mark_profile("dedupe_persist_and_aggregate")

    grace_s = max(0.0, float(args.ledger_missing_grace_s))
    now_ts = time.time()
    ledger_rows: list[dict[str, Any]] = []
    ledger_missing_critical: list[dict[str, Any]] = []
    ledger_missing_grace: list[dict[str, Any]] = []
    amount_mismatch_rows: list[dict[str, Any]] = []
    price_mismatch_rows: list[dict[str, Any]] = []
    for tx, ledger_item in ledger_by_tx.items():
        self_item = self_by_tx.get(tx)
        age_s = now_ts - float(ledger_item.get("max_event_ts") or 0.0)
        cost_delta = None
        size_delta = None
        price_match = True
        if self_item:
            cost_delta = round(num(ledger_item.get("cost_usd"), 0.0) - num(self_item.get("cost_usd"), 0.0), 6)
            size_delta = round(num(ledger_item.get("size"), 0.0) - num(self_item.get("size"), 0.0), 6)
            price_match = _compare_price(ledger_item.get("avg_price"), self_item.get("avg_price"))
            status = "MATCH"
            if abs(cost_delta) > 0.05 or abs(size_delta) > 0.00001:
                status = "AMOUNT_MISMATCH"
            elif not price_match:
                status = "MATCH_PRICE_ROUNDING"
        else:
            status = "MISSING_SELF_FEED_GRACE" if age_s <= grace_s else "LEDGER_MISSING_SELF_FEED"
        row = {
            "tx": tx,
            "status": status,
            "ledger": ledger_item,
            "self_feed": self_item,
            "data_api": data_api_by_tx.get(tx),
            "polygon": polygon_by_tx.get(tx),
            "age_s": round(age_s, 6),
            "cost_delta_usd": cost_delta,
            "size_delta": size_delta,
            "price_match": price_match,
        }
        ledger_rows.append(row)
        if status == "LEDGER_MISSING_SELF_FEED":
            ledger_missing_critical.append(row)
        elif status == "MISSING_SELF_FEED_GRACE":
            ledger_missing_grace.append(row)
        elif status == "AMOUNT_MISMATCH":
            amount_mismatch_rows.append(row)
        elif status == "MATCH_PRICE_ROUNDING":
            price_mismatch_rows.append(row)

    self_missing_ledger_rows: list[dict[str, Any]] = []
    for tx, self_item in self_by_tx.items():
        if tx not in ledger_by_tx:
            self_missing_ledger_rows.append({"tx": tx, "self_feed": self_item})
    mark_profile("compare_ledger_and_feed")
    probable_split_fill_groups = _probable_split_fill_groups(amount_mismatch_rows, self_missing_ledger_rows)
    probable_split_fill_missing_txs = {
        str(row.get("tx") or "")
        for group in probable_split_fill_groups
        for row in group.get("missing_companion_self_feed_txs", [])
        if isinstance(row, dict)
    }
    try:
        resolutions = load_resolutions(str(args.resolutions))
    except Exception:
        resolutions = {}
    self_missing_pnl = _self_missing_pnl_summary(self_missing_ledger_rows, resolutions)
    mark_profile("resolve_and_score_differences")

    critical_count = len(ledger_missing_critical) + len(self_missing_ledger_rows) + len(amount_mismatch_rows)
    matched_count = sum(1 for row in ledger_rows if row["status"] in {"MATCH", "MATCH_PRICE_ROUNDING"})
    status = "PASS" if critical_count == 0 else "CRITICAL"
    if status == "PASS" and ledger_missing_grace:
        status = "PENDING_GRACE"
    report = {
        "generated_at": utc_now_iso(),
        "kind": "wallet_copy_self_feed_vs_ledger",
        "flow_stage": "LIVE/SELF-DEV",
        "status": status,
        "user": user,
        "window": {
            "source": window_source,
            "start_ts": round(start_ts, 6),
            "start_iso": _iso(start_ts),
            "end_ts": round(end_ts, 6),
            "end_iso": _iso(end_ts),
        },
        "inputs": {
            "ledger": str(args.ledger),
            "scorecard": str(args.scorecard),
            "self_feed_log": str(args.self_feed_log),
            "data_api_base_url": str(args.data_api_base_url or DEFAULT_DATA_API_BASE),
            "polygon_rpc_url": str(args.polygon_rpc_url or DEFAULT_POLYGON_RPC_URL),
        },
        "source_fetch": {
            "data_api": {key: value for key, value in data_api_fetch.items() if key != "urls"},
            "polygon": polygon_fetch,
        },
        "summary": {
            "status": status,
            "ledger_filled_tx_groups": len(ledger_by_tx),
            "ledger_filled_rows": len(ledger_fills),
            "self_feed_tx_groups": len(self_by_tx),
            "self_feed_event_rows": len(self_feed_rows),
            "self_feed_duplicate_event_rows": self_feed_duplicate_event_rows,
            "data_api_trade_rows": len(data_api_rows),
            "data_api_raw_trade_rows": len(data_api_rows_raw),
            "data_api_duplicate_event_rows": data_api_duplicate_event_rows,
            "data_api_tx_groups": len(data_api_by_tx),
            "polygon_orderfilled_rows": len(polygon_rows),
            "polygon_raw_orderfilled_rows": len(polygon_rows_raw),
            "polygon_duplicate_event_rows": polygon_duplicate_event_rows,
            "polygon_tx_groups": len(polygon_by_tx),
            "matched_ledger_tx_groups": matched_count,
            "amount_mismatch_tx_groups": len(amount_mismatch_rows),
            "price_rounding_mismatch_tx_groups": len(price_mismatch_rows),
            "probable_split_fill_groups": len(probable_split_fill_groups),
            "probable_split_fill_missing_tx_groups": len(probable_split_fill_missing_txs),
            "self_feed_missing_ledger_cost_usd": self_missing_pnl.get("cost_usd"),
            "self_feed_missing_ledger_payout_usd": self_missing_pnl.get("payout_usd"),
            "self_feed_missing_ledger_pnl_usd": self_missing_pnl.get("pnl_usd"),
            "self_feed_missing_ledger_resolved_tx_groups": self_missing_pnl.get("resolved_tx_groups"),
            "self_feed_missing_ledger_unresolved_tx_groups": self_missing_pnl.get("unresolved_tx_groups"),
            "ledger_missing_self_feed_critical": len(ledger_missing_critical),
            "ledger_missing_self_feed_within_grace": len(ledger_missing_grace),
            "self_feed_missing_ledger_critical": len(self_missing_ledger_rows),
            "self_feed_rows_appended": appended,
            "ledger_missing_grace_s": grace_s,
            "classification": "independent_self_wallet_data_api_plus_polygon_orderfilled_vs_live_ledger",
        },
        "probable_split_fill_groups": probable_split_fill_groups[:50],
        "self_feed_missing_ledger_pnl": self_missing_pnl,
        "top_diffs": (
            ledger_missing_critical
            + amount_mismatch_rows
            + self_missing_ledger_rows
            + ledger_missing_grace
            + price_mismatch_rows
        )[:25],
        "ledger_rows": sorted(
            ledger_rows,
            key=lambda row: (row["status"] not in {"MATCH", "MATCH_PRICE_ROUNDING"}, row["tx"]),
        ),
        "self_missing_ledger_rows": self_missing_ledger_rows[:100],
        "runtime_profile": {
            "stages": profile_stages,
            "total_s": round(time.perf_counter() - profile_started, 6),
        },
    }
    return report


def run_reconcile(args: argparse.Namespace) -> dict[str, Any]:
    report = build_report(args)
    atomic_write_json(args.output, report)
    return report


def main() -> int:
    args = parse_args()
    report = run_reconcile(args)
    print(json.dumps(report["summary"], sort_keys=True))
    return 0 if report.get("status") in {"PASS", "PENDING_GRACE"} else 2


if __name__ == "__main__":
    raise SystemExit(main())
