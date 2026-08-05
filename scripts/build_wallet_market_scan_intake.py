#!/usr/bin/env python3
"""Market-wide crypto-5m wallet intake for wallet-copy discovery.

Flow stage: DISCOVER/LEARN. This is paper/research only: it harvests recent
crypto 5m trade wallets from the Polymarket data-api trade feed and prepares a
ranked intake artifact for the remote-history replay stage.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from collections import Counter
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.http_client import (  # noqa: E402
    PolymarketHttpClient,
    PolymarketRouteError,
    SOURCE_BASE_ENV_BY_HOST,
)
from src.wallet_copy.leaderboard import fetch_crypto_top_wallets  # noqa: E402
from src.wallet_copy.models import num, utc_now_iso  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


TRADES_URL = "https://data-api.polymarket.com/trades"
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")
CRYPTO_5M_SLUG_RE = re.compile(r"^(?P<symbol>[a-z0-9]+)-updown-5m-(?P<window>[0-9]{9,})$")
DEFAULT_CRYPTO_SYMBOLS = {
    "btc",
    "eth",
    "sol",
    "xrp",
    "bnb",
    "doge",
    "ada",
    "link",
    "avax",
    "sui",
    "hype",
    "pepe",
    "trump",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="data/research/wallet_market_scan_intake_latest.json")
    parser.add_argument("--ranked-output", default="data/research/wallet_market_scan_ranked.json")
    parser.add_argument("--lookback-days", type=float, default=7.0)
    parser.add_argument("--limit", type=int, default=500)
    parser.add_argument("--max-pages", type=int, default=60)
    parser.add_argument("--timeout-s", type=float, default=10.0)
    parser.add_argument("--max-wall-runtime-s", type=float, default=240.0)
    parser.add_argument("--sleep-s", type=float, default=0.15)
    parser.add_argument("--reset", action="store_true", help="Ignore prior rolling-union artifact.")
    parser.add_argument("--include-leaderboard", action="store_true")
    parser.add_argument("--leaderboard-limit", type=int, default=50)
    parser.add_argument("--leaderboard-pages", type=int, default=2)
    parser.add_argument("--leaderboard-timeout-s", type=float, default=10.0)
    parser.add_argument("--leaderboard-retries", type=int, default=1)
    parser.add_argument(
        "--market-scope-resolutions",
        default="",
        help="Optional resolution JSONL whose BTC-5m condition ids replace global offset pagination.",
    )
    parser.add_argument(
        "--market-scope-limit",
        type=int,
        default=0,
        help="Maximum condition ids to query; 0 uses max-pages.",
    )
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    return text if ADDRESS_RE.match(text) else ""


def _trade_wallet(row: dict[str, Any]) -> str:
    for key in ("proxyWallet", "proxy_wallet", "makerAddress", "maker_address", "wallet", "user"):
        value = row.get(key)
        if isinstance(value, dict):
            value = value.get("proxyWallet") or value.get("address") or value.get("wallet")
        wallet = _norm_wallet(value)
        if wallet:
            return wallet
    return ""


def _parse_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return num(text, 0.0)


def _iso_from_ts(value: float) -> str:
    if value <= 0:
        return ""
    return datetime.fromtimestamp(float(value), tz=UTC).isoformat().replace("+00:00", "Z")


def _slug(row: dict[str, Any]) -> str:
    return str(row.get("slug") or row.get("marketSlug") or row.get("market_slug") or "").strip().lower()


def crypto_5m_symbol(slug: str) -> str:
    match = CRYPTO_5M_SLUG_RE.match(str(slug or "").strip().lower())
    if not match:
        return ""
    symbol = match.group("symbol")
    return symbol if symbol in DEFAULT_CRYPTO_SYMBOLS else ""


def is_crypto_5m_trade(row: dict[str, Any]) -> bool:
    return bool(crypto_5m_symbol(_slug(row)))


def _notional_usd(row: dict[str, Any]) -> float:
    size = num(row.get("size"), 0.0)
    price = num(row.get("price"), 0.0)
    return max(0.0, float(size) * float(price))


def _trade_id(row: dict[str, Any]) -> str:
    tx = str(row.get("transactionHash") or row.get("transaction_hash") or "").strip().lower()
    if tx:
        return tx
    return "|".join(
        [
            _slug(row),
            str(_parse_ts(row.get("timestamp"))),
            _trade_wallet(row),
            str(row.get("side") or "").upper(),
            str(row.get("outcome") or ""),
            str(row.get("size") or ""),
            str(row.get("price") or ""),
        ]
    )


def _empty_wallet_stats(wallet: str) -> dict[str, Any]:
    return {
        "wallet": wallet,
        "source_wallet": wallet,
        "source_channels": [],
        "trade_count": 0,
        "crypto5m_trade_count": 0,
        "btc5m_trade_count": 0,
        "buy_count": 0,
        "sell_count": 0,
        "approx_notional_usd": 0.0,
        "first_seen_ts": 0.0,
        "last_seen_ts": 0.0,
        "symbols": {},
        "slugs": {},
        "sample_trades": [],
        "trade_ids_seen": [],
        "leaderboard": {},
    }


def _add_channel(stats: dict[str, Any], channel: str) -> None:
    channels = list(stats.get("source_channels") or [])
    if channel not in channels:
        channels.append(channel)
    stats["source_channels"] = channels


def merge_trade(stats_by_wallet: dict[str, dict[str, Any]], row: dict[str, Any], *, source: str) -> bool:
    wallet = _trade_wallet(row)
    if not wallet or not is_crypto_5m_trade(row):
        return False
    symbol = crypto_5m_symbol(_slug(row))
    ts = _parse_ts(row.get("timestamp"))
    stats = stats_by_wallet.setdefault(wallet, _empty_wallet_stats(wallet))
    trade_id = _trade_id(row)
    seen_trade_ids = list(stats.get("trade_ids_seen") or [])
    if trade_id and trade_id in set(seen_trade_ids):
        return False
    if trade_id:
        seen_trade_ids.append(trade_id)
        stats["trade_ids_seen"] = seen_trade_ids
    _add_channel(stats, source)
    stats["trade_count"] = int(stats.get("trade_count") or 0) + 1
    stats["crypto5m_trade_count"] = int(stats.get("crypto5m_trade_count") or 0) + 1
    if symbol == "btc":
        stats["btc5m_trade_count"] = int(stats.get("btc5m_trade_count") or 0) + 1
    side = str(row.get("side") or "").upper()
    if side == "BUY":
        stats["buy_count"] = int(stats.get("buy_count") or 0) + 1
    elif side == "SELL":
        stats["sell_count"] = int(stats.get("sell_count") or 0) + 1
    stats["approx_notional_usd"] = round(float(stats.get("approx_notional_usd") or 0.0) + _notional_usd(row), 6)
    if ts > 0:
        first_seen = float(stats.get("first_seen_ts") or 0.0)
        last_seen = float(stats.get("last_seen_ts") or 0.0)
        stats["first_seen_ts"] = ts if first_seen <= 0 else min(first_seen, ts)
        stats["last_seen_ts"] = max(last_seen, ts)
    symbols = dict(stats.get("symbols") or {})
    symbols[symbol] = int(symbols.get(symbol) or 0) + 1
    stats["symbols"] = symbols
    slugs = dict(stats.get("slugs") or {})
    slug = _slug(row)
    slugs[slug] = int(slugs.get(slug) or 0) + 1
    stats["slugs"] = slugs
    samples = list(stats.get("sample_trades") or [])
    if len(samples) < 5:
        samples.append(
            {
                "slug": slug,
                "symbol": symbol,
                "side": side,
                "outcome": str(row.get("outcome") or ""),
                "size": num(row.get("size"), 0.0),
                "price": num(row.get("price"), 0.0),
                "timestamp": ts,
                "transaction_hash": str(row.get("transactionHash") or row.get("transaction_hash") or ""),
            }
        )
    stats["sample_trades"] = samples
    return True


def _rank_score(stats: dict[str, Any], *, now_ts: float) -> float:
    last_seen = float(stats.get("last_seen_ts") or 0.0)
    age_hours = max(0.0, (float(now_ts) - last_seen) / 3600.0) if last_seen > 0 else 9999.0
    recency = max(0.0, 168.0 - age_hours)
    return round(
        int(stats.get("crypto5m_trade_count") or 0) * 1000.0
        + int(stats.get("btc5m_trade_count") or 0) * 250.0
        + min(float(stats.get("approx_notional_usd") or 0.0), 50000.0) / 50.0
        + recency,
        6,
    )


def _compact_wallet(stats: dict[str, Any], *, now_ts: float) -> dict[str, Any]:
    slugs = dict(stats.get("slugs") or {})
    symbols = dict(stats.get("symbols") or {})
    last_seen = float(stats.get("last_seen_ts") or 0.0)
    row = {
        "wallet": stats.get("wallet"),
        "source_wallet": stats.get("source_wallet") or stats.get("wallet"),
        "rank_score": _rank_score(stats, now_ts=now_ts),
        "alive": bool(int(stats.get("crypto5m_trade_count") or 0) > 0 and last_seen > 0),
        "profitability_status": "PENDING_REMOTE_HISTORY_REPLAY",
        "copyability_status": "PENDING_REMOTE_HISTORY_REPLAY",
        "source_channels": list(stats.get("source_channels") or []),
        "trade_count": int(stats.get("trade_count") or 0),
        "crypto5m_trade_count": int(stats.get("crypto5m_trade_count") or 0),
        "btc5m_trade_count": int(stats.get("btc5m_trade_count") or 0),
        "buy_count": int(stats.get("buy_count") or 0),
        "sell_count": int(stats.get("sell_count") or 0),
        "approx_notional_usd": round(float(stats.get("approx_notional_usd") or 0.0), 6),
        "first_seen_ts": float(stats.get("first_seen_ts") or 0.0),
        "first_seen_iso": _iso_from_ts(float(stats.get("first_seen_ts") or 0.0)),
        "last_seen_ts": last_seen,
        "last_seen_iso": _iso_from_ts(last_seen),
        "unique_slugs": len(slugs),
        "symbols": dict(sorted(symbols.items())),
        "top_slugs": [
            {"slug": slug, "trade_count": count}
            for slug, count in sorted(slugs.items(), key=lambda item: (-int(item[1]), str(item[0])))[:10]
        ],
        "sample_trades": list(stats.get("sample_trades") or [])[:5],
        "trade_ids_seen": list(stats.get("trade_ids_seen") or []),
    }
    if isinstance(stats.get("leaderboard"), dict) and stats["leaderboard"]:
        row["leaderboard"] = dict(stats["leaderboard"])
    return row


def _merge_previous(stats_by_wallet: dict[str, dict[str, Any]], previous: dict[str, Any], *, cutoff_ts: float) -> int:
    rows = previous.get("ranked_wallets") if isinstance(previous.get("ranked_wallets"), list) else []
    kept = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        wallet = _norm_wallet(row.get("source_wallet") or row.get("wallet"))
        if not wallet:
            continue
        last_seen = _parse_ts(row.get("last_seen_ts") or row.get("last_seen_iso"))
        if last_seen < cutoff_ts:
            continue
        stats = stats_by_wallet.setdefault(wallet, _empty_wallet_stats(wallet))
        stats.update(
            {
                "trade_count": int(row.get("trade_count") or 0),
                "crypto5m_trade_count": int(row.get("crypto5m_trade_count") or 0),
                "btc5m_trade_count": int(row.get("btc5m_trade_count") or 0),
                "buy_count": int(row.get("buy_count") or 0),
                "sell_count": int(row.get("sell_count") or 0),
                "approx_notional_usd": num(row.get("approx_notional_usd"), 0.0),
                "first_seen_ts": _parse_ts(row.get("first_seen_ts") or row.get("first_seen_iso")),
                "last_seen_ts": last_seen,
                "symbols": dict(row.get("symbols") or {}),
                "slugs": {
                    str(item.get("slug")): int(item.get("trade_count") or 0)
                    for item in (row.get("top_slugs") or [])
                    if isinstance(item, dict) and item.get("slug")
                },
                "sample_trades": list(row.get("sample_trades") or [])[:5],
                "trade_ids_seen": list(row.get("trade_ids_seen") or []),
            }
        )
        for channel in row.get("source_channels") or []:
            _add_channel(stats, str(channel))
        _add_channel(stats, "previous_rolling_union")
        kept += 1
    return kept


def _compact_route_report(report: dict[str, Any]) -> dict[str, Any]:
    attempts = report.get("attempts") if isinstance(report.get("attempts"), list) else []
    return {
        "status": report.get("status"),
        "route_class": report.get("route_class"),
        "route_report_id": report.get("route_report_id"),
        "host": report.get("host"),
        "attempt_count": report.get("attempt_count"),
        "source_base_override_configured": bool(report.get("source_base_override_configured")),
        "source_base_override_env_var": report.get("source_base_override_env_var"),
        "http_statuses": [
            attempt.get("http_status")
            for attempt in attempts
            if isinstance(attempt, dict) and attempt.get("http_status") is not None
        ],
    }


@contextmanager
def _cleared_source_base_override(host: str):
    env_var = SOURCE_BASE_ENV_BY_HOST.get(str(host or "").lower(), "")
    if not env_var:
        yield ""
        return
    previous = os.environ.get(env_var)
    os.environ[env_var] = ""
    try:
        yield env_var
    finally:
        if previous is None:
            os.environ.pop(env_var, None)
        else:
            os.environ[env_var] = previous


def _response_report(response: requests.Response) -> dict[str, Any]:
    report = _compact_route_report(getattr(response, "wallet_copy_route_report", {}) or {})
    report["http_status"] = int(getattr(response, "status_code", 0) or 0)
    return report


def _combine_direct_fallback_report(
    primary: dict[str, Any],
    direct: dict[str, Any],
    *,
    suppressed_env_var: str,
) -> dict[str, Any]:
    return {
        **direct,
        "fallback_source": "direct_data_api_after_relay_5xx",
        "suppressed_env_var": suppressed_env_var,
        "primary_route": primary,
        "direct_route": direct,
    }


def _fetch_trade_page(
    client: PolymarketHttpClient,
    *,
    limit: int,
    offset: int,
    timeout_s: float,
    market: str = "",
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    params: dict[str, Any] = {"takerOnly": "false", "limit": int(limit), "offset": int(offset)}
    if market:
        params["market"] = market
    response = client.request(
        "GET",
        TRADES_URL,
        params=params,
        request_role="wallet_market_scan_trades",
        timeout_s=float(timeout_s),
    )
    route_report = _response_report(response)
    if int(response.status_code) >= 500 and bool(route_report.get("source_base_override_configured")):
        with _cleared_source_base_override("data-api.polymarket.com") as suppressed_env_var:
            direct_response = client.request(
                "GET",
                TRADES_URL,
                params=params,
                request_role="wallet_market_scan_trades_direct_fallback",
                timeout_s=float(timeout_s),
            )
        direct_report = _response_report(direct_response)
        if int(direct_response.status_code) < 500:
            response = direct_response
            route_report = _combine_direct_fallback_report(
                route_report,
                direct_report,
                suppressed_env_var=suppressed_env_var,
            )
    if int(response.status_code) == 400 and int(offset) > 0:
        route_report["route_class"] = "DATA_API_PAGINATION_CAP"
        route_report["pagination_cap_reached"] = True
        route_report["pagination_cap_reason"] = "data-api trades returned HTTP 400 for positive offset"
        return [], route_report
    response.raise_for_status()
    payload = response.json()
    rows = [row for row in payload if isinstance(row, dict)] if isinstance(payload, list) else []
    return rows, route_report


def _market_scope_condition_ids(
    path: str,
    *,
    cutoff_ts: float,
    now_ts: float,
    limit: int,
) -> list[str]:
    if not str(path or "").strip():
        return []
    by_condition: dict[str, float] = {}
    with Path(path).open("r", encoding="utf-8") as handle:
        for raw in handle:
            try:
                row = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                continue
            if not isinstance(row, dict):
                continue
            if str(row.get("asset") or "").upper() != "BTC":
                continue
            if str(row.get("window_type") or "").lower() != "5m":
                continue
            start = num(row.get("window_start_unix_ts"))
            condition_id = str(row.get("condition_id") or "")
            if not condition_id or start < cutoff_ts or start > now_ts:
                continue
            by_condition[condition_id] = start
    ordered = [
        condition_id
        for condition_id, _start in sorted(by_condition.items(), key=lambda item: (item[1], item[0]))
    ]
    return ordered[: max(0, int(limit))]


def _leaderboard_intake(args: argparse.Namespace, stats_by_wallet: dict[str, dict[str, Any]]) -> dict[str, Any]:
    if not bool(getattr(args, "include_leaderboard", False)):
        return {"enabled": False, "wallets": 0, "errors": []}
    errors: list[str] = []
    rows = []
    try:
        rows = fetch_crypto_top_wallets(
            periods=("WEEK", "MONTH"),
            category="CRYPTO",
            order_by="PNL",
            limit=max(1, int(args.leaderboard_limit)),
            pages=max(1, int(args.leaderboard_pages)),
            max_pages=max(1, int(args.leaderboard_pages)),
            timeout_s=float(args.leaderboard_timeout_s),
            retries=max(1, int(args.leaderboard_retries)),
        )
    except Exception as exc:  # pragma: no cover - route failures are environment-specific.
        errors.append(f"{type(exc).__name__}: {exc}")
    seen = 0
    for row in rows:
        wallet = _norm_wallet(getattr(row, "proxy_wallet", ""))
        if not wallet:
            continue
        stats = stats_by_wallet.setdefault(wallet, _empty_wallet_stats(wallet))
        _add_channel(stats, "leaderboard")
        leaderboard = dict(stats.get("leaderboard") or {})
        period = str(getattr(row, "period", "") or "").upper()
        if period:
            leaderboard[period] = {
                "rank": getattr(row, "rank", None),
                "pnl": getattr(row, "pnl", None),
                "vol": getattr(row, "vol", None),
                "category": getattr(row, "category", None),
            }
        stats["leaderboard"] = leaderboard
        seen += 1
    return {"enabled": True, "wallets": seen, "errors": errors}


def build_market_scan(args: argparse.Namespace, *, now_ts: float | None = None) -> dict[str, Any]:
    generated_at = utc_now_iso()
    if now_ts is None:
        now_ts = datetime.fromisoformat(generated_at.replace("Z", "+00:00")).timestamp()
    lookback_s = max(0.0, float(args.lookback_days) * 86400.0)
    cutoff_ts = float(now_ts) - lookback_s
    market_scope_limit = int(getattr(args, "market_scope_limit", 0) or 0) or int(args.max_pages)
    market_scope_ids = _market_scope_condition_ids(
        str(getattr(args, "market_scope_resolutions", "") or ""),
        cutoff_ts=cutoff_ts,
        now_ts=float(now_ts),
        limit=market_scope_limit,
    )
    market_scoped = bool(market_scope_ids)
    stats_by_wallet: dict[str, dict[str, Any]] = {}
    previous = load_json(args.ranked_output, default={}) if not bool(getattr(args, "reset", False)) else {}
    previous = previous if isinstance(previous, dict) else {}
    previous_wallets_kept = _merge_previous(stats_by_wallet, previous, cutoff_ts=cutoff_ts)

    client = PolymarketHttpClient(
        timeout_s=float(args.timeout_s),
        retries=1,
        user_agent="wallet-market-scan-intake/1.0",
    )
    started = time.monotonic()
    pages_started = 0
    pages_completed = 0
    trades_scanned = 0
    crypto5m_trades_matched = 0
    oldest_trade_ts = 0.0
    newest_trade_ts = 0.0
    reached_cutoff = False
    budget_exhausted = False
    errors: list[dict[str, Any]] = []
    route_classes: Counter[str] = Counter()
    page_tail: list[dict[str, Any]] = []
    api_pagination_cap_reached = False
    short_page_truncation: dict[str, Any] | None = None

    page_count = min(int(args.max_pages), len(market_scope_ids)) if market_scoped else int(args.max_pages)
    for page in range(max(0, page_count)):
        elapsed = time.monotonic() - started
        if float(args.max_wall_runtime_s) > 0 and elapsed >= float(args.max_wall_runtime_s):
            budget_exhausted = True
            break
        offset = 0 if market_scoped else page * int(args.limit)
        market = market_scope_ids[page] if market_scoped else ""
        pages_started += 1
        try:
            fetch_kwargs: dict[str, Any] = {
                "limit": int(args.limit),
                "offset": offset,
                "timeout_s": float(args.timeout_s),
            }
            if market:
                fetch_kwargs["market"] = market
            rows, route_report = _fetch_trade_page(client, **fetch_kwargs)
        except (PolymarketRouteError, requests.RequestException, ValueError) as exc:
            response = getattr(exc, "response", None)
            if isinstance(response, requests.Response):
                route_report = _response_report(response)
            else:
                route_report = _compact_route_report(getattr(exc, "route_report", {}) if hasattr(exc, "route_report") else {})
            errors.append({"page": page, "offset": offset, "error": f"{type(exc).__name__}: {exc}", "route": route_report})
            break
        route_class = str(route_report.get("route_class") or "UNKNOWN")
        if bool(route_report.get("pagination_cap_reached")):
            api_pagination_cap_reached = True
            route_classes[route_class] += 1
            page_tail.append(
                {
                    "page": page,
                    "offset": offset,
                    "market": market or None,
                    "rows": 0,
                    "crypto5m_matches": 0,
                    "oldest_ts": 0.0,
                    "newest_ts": 0.0,
                    "route_class": route_class,
                    "pagination_cap_reached": True,
                }
            )
            page_tail = page_tail[-10:]
            break
        pages_completed += 1
        route_classes[route_class] += 1
        trades_scanned += len(rows)
        page_ts = [_parse_ts(row.get("timestamp")) for row in rows if isinstance(row, dict)]
        if page_ts:
            page_oldest = min(page_ts)
            page_newest = max(page_ts)
            oldest_trade_ts = page_oldest if oldest_trade_ts <= 0 else min(oldest_trade_ts, page_oldest)
            newest_trade_ts = max(newest_trade_ts, page_newest)
            if page_oldest < cutoff_ts:
                reached_cutoff = True
        page_matches = 0
        for row in rows:
            ts = _parse_ts(row.get("timestamp"))
            if ts < cutoff_ts:
                continue
            if merge_trade(stats_by_wallet, row, source="data_api_trades"):
                page_matches += 1
        crypto5m_trades_matched += page_matches
        page_tail.append(
            {
                "page": page,
                "offset": offset,
                "market": market or None,
                "rows": len(rows),
                "crypto5m_matches": page_matches,
                "oldest_ts": min(page_ts) if page_ts else 0.0,
                "newest_ts": max(page_ts) if page_ts else 0.0,
                "route_class": route_class,
            }
        )
        page_tail = page_tail[-10:]
        if market_scoped:
            continue
        if len(rows) < int(args.limit) and not reached_cutoff:
            short_page_truncation = {
                "class": "SHORT_PAGE_TRUNCATION",
                "page": page,
                "offset": offset,
                "rows": len(rows),
                "requested_limit": int(args.limit),
                "lookback_complete": False,
            }
            break
        if reached_cutoff:
            break
        if float(args.sleep_s) > 0:
            time.sleep(float(args.sleep_s))

    leaderboard = _leaderboard_intake(args, stats_by_wallet)
    ranked_wallets = sorted(
        (_compact_wallet(stats, now_ts=float(now_ts)) for stats in stats_by_wallet.values()),
        key=lambda row: (
            -float(row.get("rank_score") or 0.0),
            -int(row.get("crypto5m_trade_count") or 0),
            str(row.get("wallet") or ""),
        ),
    )
    active_wallets = [row for row in ranked_wallets if row.get("alive") and int(row.get("crypto5m_trade_count") or 0) > 0]
    prior_wallets = {
        _norm_wallet(row.get("source_wallet") or row.get("wallet"))
        for row in (previous.get("ranked_wallets") if isinstance(previous.get("ranked_wallets"), list) else [])
        if isinstance(row, dict)
    }
    new_active_wallets = [
        row.get("wallet")
        for row in active_wallets
        if _norm_wallet(row.get("wallet")) and _norm_wallet(row.get("wallet")) not in prior_wallets
    ]
    market_scope_complete = bool(
        market_scoped
        and pages_completed == len(market_scope_ids)
        and len(market_scope_ids) < market_scope_limit
        and not budget_exhausted
        and not errors
    )
    page_cap_exhausted = (
        bool(
            market_scoped
            and (
                pages_completed < len(market_scope_ids)
                or len(market_scope_ids) >= market_scope_limit
            )
        )
        or bool(not market_scoped and pages_completed >= max(0, int(args.max_pages)) and not reached_cutoff)
    )
    lookback_complete = bool(
        market_scope_complete
        or (not market_scoped and reached_cutoff and not budget_exhausted and not page_cap_exhausted and not errors)
    )
    if errors and pages_completed == 0:
        status = "ERROR"
    elif lookback_complete:
        status = "PASS_LOOKBACK_COMPLETE"
    else:
        status = "PARTIAL_REPLAY_PENDING"
    payload = {
        "schema_version": 1,
        "kind": "wallet_market_scan_ranked",
        "flow_stage": "DISCOVER/LEARN",
        "status": status,
        "generated_at": generated_at,
        "paper_only": True,
        "live_orders_allowed": False,
        "source": {
            "trades_url": TRADES_URL,
            "mode": "rolling_7d_union" if float(args.lookback_days) == 7.0 else "rolling_lookback_union",
            "previous_wallets_kept": previous_wallets_kept,
            "market_scoped": market_scoped,
            "market_scope_resolutions": str(getattr(args, "market_scope_resolutions", "") or ""),
            "market_scope_condition_ids_requested": len(market_scope_ids),
            "market_scope_condition_ids_completed": pages_completed if market_scoped else 0,
        },
        "window": {
            "lookback_days": float(args.lookback_days),
            "cutoff_ts": cutoff_ts,
            "cutoff_iso": _iso_from_ts(cutoff_ts),
            "now_ts": float(now_ts),
            "now_iso": _iso_from_ts(float(now_ts)),
            "oldest_trade_ts_seen": oldest_trade_ts,
            "oldest_trade_iso_seen": _iso_from_ts(oldest_trade_ts),
            "newest_trade_ts_seen": newest_trade_ts,
            "newest_trade_iso_seen": _iso_from_ts(newest_trade_ts),
            "lookback_complete": lookback_complete,
        },
        "rate_limit_budget": {
            "limit": int(args.limit),
            "max_pages": int(args.max_pages),
            "max_requests": int(args.max_pages) + (2 * int(args.leaderboard_pages) if bool(args.include_leaderboard) else 0),
            "timeout_s": float(args.timeout_s),
            "max_wall_runtime_s": float(args.max_wall_runtime_s),
            "sleep_s": float(args.sleep_s),
            "budget_exhausted": budget_exhausted,
            "page_cap_exhausted": page_cap_exhausted,
            "api_pagination_cap_reached": api_pagination_cap_reached,
            "short_page_truncation": short_page_truncation,
            "exhaustion_class": (
                "SHORT_PAGE_TRUNCATION"
                if short_page_truncation
                else "API_PAGINATION_CAP"
                if api_pagination_cap_reached
                else "WALL_RUNTIME_BUDGET"
                if budget_exhausted
                else "PAGE_CAP"
                if page_cap_exhausted
                else None
            ),
        },
        "summary": {
            "wallets_ranked": len(ranked_wallets),
            "active_wallets": len(active_wallets),
            "new_active_wallets": len(new_active_wallets),
            "trades_scanned": trades_scanned,
            "crypto5m_trades_matched": crypto5m_trades_matched,
            "pages_started": pages_started,
            "pages_completed": pages_completed,
            "scanned_alive_profitable": 0,
            "scanned_alive_profitable_definition": "remote-history replay positive copy PnL; intake-only batch sets zero until replay proof",
            "replay_status": "PENDING_REMOTE_HISTORY_REPLAY",
            "leaderboard_wallets": leaderboard.get("wallets"),
            "top_wallet": ranked_wallets[0].get("wallet") if ranked_wallets else "",
            "top_rank_score": ranked_wallets[0].get("rank_score") if ranked_wallets else None,
        },
        "leaderboard_intake": leaderboard,
        "route_class_counts": dict(sorted(route_classes.items())),
        "errors": errors,
        "page_tail": page_tail,
        "ranked_wallets": ranked_wallets,
        "next_action": "remote-history replay at scale: fetch each active wallet history and score alive x profitable x copyable",
    }
    return payload


def main() -> int:
    args = parse_args()
    payload = build_market_scan(args)
    if str(args.output or "").strip():
        atomic_write_json(args.output, payload)
    if str(args.ranked_output or "").strip() and str(args.ranked_output) != str(args.output):
        atomic_write_json(args.ranked_output, payload)
    print(json.dumps({"output": args.output, "ranked_output": args.ranked_output, "summary": payload["summary"]}, sort_keys=True))
    return 0 if payload.get("status") != "ERROR" else 2


if __name__ == "__main__":
    raise SystemExit(main())
