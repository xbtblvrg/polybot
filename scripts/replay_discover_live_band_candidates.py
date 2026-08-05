#!/usr/bin/env python3
"""Refresh DISCOVER live-band candidates with CLOB-backed paper replay."""

from __future__ import annotations

import argparse
import io
import json
import requests
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.fill_model import FillModelConfig  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import CopyIntent, WalletEvent, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order, summarize_scores  # noqa: E402
from src.wallet_copy.profit_engine import (  # noqa: E402
    CandidatePolicy,
    _fill_evidence_summary,
    _order_from_intent,
    intents_for_policy,
)
from src.wallet_copy.research import unique_wallet_events  # noqa: E402
from src.wallet_copy.store import atomic_write_json, load_json  # noqa: E402


DEFAULT_POLICY_ID = "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
DEFAULT_DISCOVER_CANDIDATES = "data/research/wallet_copy_discover_live_band_candidates.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_MAX_REJECTED_FILL_RATIO = 0.45
DEFAULT_SOURCE_FRESHNESS_LIMIT_S = 86400.0
REPLAY_SOURCE = "rtds_tail_clob_backed_profit_replay"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--discover-candidates", default=DEFAULT_DISCOVER_CANDIDATES)
    parser.add_argument("--registry", default="configs/wallet_copy/wallets.json")
    parser.add_argument("--source-jsonl", default="")
    parser.add_argument("--output", default="")
    parser.add_argument(
        "--wallet",
        action="append",
        default=[],
        help="Replay only these wallets while preserving every unselected candidate in the output.",
    )
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--policy-id", default=DEFAULT_POLICY_ID)
    parser.add_argument("--wallet-fraction", type=float, default=0.10)
    parser.add_argument("--max-order-usd", type=float, default=4.0)
    parser.add_argument("--min-buy-price", type=float, default=0.01)
    parser.add_argument("--max-buy-price", type=float, default=0.50)
    parser.add_argument("--max-unresolved-ratio", type=float, default=0.50)
    parser.add_argument(
        "--max-rejected-fill-ratio",
        type=float,
        default=DEFAULT_MAX_REJECTED_FILL_RATIO,
        help=(
            "Reject-ratio promotion gate for candidate replay. Fable 2026-07-05T18:36Z "
            "changed this from any rejected fill being disqualifying to reject_ratio <= 0.30. "
            "Fable 2026-07-07 raised to 0.45: rejects are protective skips (slippage cap / "
            "no-ask), pnl basis is realized-on-filled, so the gate bounds participation, "
            "not measurement."
        ),
    )
    parser.add_argument("--slippage-bps", type=float, default=250.0)
    parser.add_argument("--tail-bytes", type=int, default=0)
    parser.add_argument("--scan-limit", type=int, default=0)
    parser.add_argument("--max-events-per-wallet", type=int, default=0)
    parser.add_argument("--clob-timeout-s", type=float, default=0.8)
    parser.add_argument("--clob-retries", type=int, default=1)
    parser.add_argument("--max-clob-fetches", type=int, default=0)
    parser.add_argument(
        "--include-registry-wallets",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Add registry wallets with matching RTDS tail events to the replay candidate set.",
    )
    parser.add_argument(
        "--rescore-stored-orders",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Carry forward prior replay_orders and rescore them with fresh resolutions. "
            "This preserves time-sensitive CLOB fill evidence after markets close."
        ),
    )
    parser.add_argument(
        "--max-stored-orders-per-candidate",
        type=int,
        default=500,
        help="Maximum replay order rows to persist per candidate for later resolution rescoring.",
    )
    parser.add_argument(
        "--capture-unresolved-clob-books",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "Fetch current CLOB book evidence for still-unresolved RTDS events and persist those "
            "orders for later rescoring. Unresolved rows still fail promotion until resolved PnL is known."
        ),
    )
    return parser.parse_args()


def _norm_wallet(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text.startswith("0x") and len(text) >= 42:
        return text[:42]
    return ""


def _float(value: Any, default: float = 0.0) -> float:
    try:
        if value in (None, ""):
            return float(default)
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _int(value: Any, default: int = 0) -> int:
    try:
        if value in (None, ""):
            return int(default)
        return int(float(value))
    except (TypeError, ValueError):
        return int(default)


def _source_event_ts(row: dict[str, Any]) -> float:
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    ts = _float(
        row.get("event_ts")
        or row.get("observed_ts")
        or row.get("received_at_s")
        or raw.get("timestamp"),
        0.0,
    )
    while ts > 10_000_000_000:
        ts /= 1000.0
    return ts if ts > 0 else 0.0


def _window_start_from_slug(slug: str) -> int | None:
    marker = str(slug or "").rsplit("-", 1)[-1]
    return int(marker) if marker.isdigit() else None


def _iter_tail_lines(path: str | Path, *, tail_bytes: int) -> list[str]:
    target = Path(path)
    if not target.exists():
        raise FileNotFoundError(f"RTDS jsonl not found: {target}")
    size = target.stat().st_size
    start = max(0, size - max(0, int(tail_bytes)))
    with target.open("rb") as raw:
        raw.seek(start)
        if start > 0:
            raw.readline()
        text = io.TextIOWrapper(raw, encoding="utf-8", errors="replace")
        return [line for line in text if line.strip()]


def _candidate_wallets(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        wallet = _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet"))
        if wallet:
            out[wallet] = candidate
    return out


def _registry_candidate_wallets(path: str | Path) -> dict[str, dict[str, Any]]:
    payload = load_json(path, default={})
    wallets = payload.get("wallets") if isinstance(payload, dict) and isinstance(payload.get("wallets"), list) else []
    out: dict[str, dict[str, Any]] = {}
    for row in wallets:
        if not isinstance(row, dict):
            continue
        if row.get("enabled") is False:
            continue
        wallet = _norm_wallet(row.get("address") or row.get("wallet") or row.get("source_wallet"))
        if not wallet:
            continue
        name = str(row.get("name") or "").strip()
        out[wallet] = {
            "candidate_id": name or f"registry_live_band_{wallet[-12:]}",
            "wallet": wallet,
            "flow_stage": "DISCOVER/ROTATE",
            "status": "REGISTRY_DISCOVER_CANDIDATE",
            "registry_tags": row.get("tags") if isinstance(row.get("tags"), list) else [],
            "registry_market_filter": row.get("market_filter"),
            "registry_source": "wallet_copy_registry",
        }
    return out


def _wallet_event_from_rtds(row: dict[str, Any], *, wallet_name: str = "") -> WalletEvent | None:
    if row.get("event") != "rtds_trade_event":
        return None
    raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
    wallet = _norm_wallet(row.get("source_wallet") or raw.get("proxyWallet"))
    side = str(row.get("side") or raw.get("side") or "").upper()
    market_slug = str(row.get("market_slug") or raw.get("slug") or raw.get("eventSlug") or "")
    condition_id = str(row.get("condition_id") or raw.get("conditionId") or "")
    token_id = str(row.get("asset") or raw.get("asset") or raw.get("tokenId") or "")
    outcome = str(raw.get("outcome") or row.get("outcome") or "")
    price = _float(row.get("price") if row.get("price") is not None else raw.get("price"))
    size = _float(row.get("size") if row.get("size") is not None else raw.get("size"))
    event_ts = _float(row.get("event_ts") if row.get("event_ts") is not None else raw.get("timestamp"))
    observed_ts = _float(row.get("received_at_s") or row.get("captured_at_s"))
    if not (wallet and side and market_slug and condition_id and token_id and outcome and price and size):
        return None
    if event_ts <= 0 or observed_ts <= 0:
        return None
    tx_hash = str(row.get("transaction_hash") or raw.get("transactionHash") or "")
    event_id = stable_id(
        "we",
        {
            "source": "rtds_activity",
            "wallet": wallet,
            "tx": tx_hash,
            "condition_id": condition_id,
            "token_id": token_id,
            "outcome": outcome,
            "side": side,
            "price": round(float(price), 8),
            "size": round(float(size), 8),
            "event_ts": event_ts,
        },
    )
    return WalletEvent(
        source_wallet=wallet,
        wallet_name=wallet_name or str(raw.get("pseudonym") or raw.get("name") or wallet),
        row_type="trade",
        action=side,
        condition_id=condition_id,
        market_slug=market_slug,
        outcome=outcome,
        price=float(price),
        size=float(size),
        usdc_size=round(float(price) * float(size), 6),
        event_ts=float(event_ts),
        observed_ts=float(observed_ts),
        event_id=event_id,
        source="rtds_activity",
        market_id=condition_id,
        event_slug=market_slug,
        title=str(raw.get("title") or ""),
        asset="BTC" if market_slug.startswith("btc-updown-5m-") else "",
        duration="5m" if market_slug.startswith("btc-updown-5m-") else "",
        window_start_s=_window_start_from_slug(market_slug),
        token_id=token_id,
        outcome_index=_int(raw.get("outcomeIndex"), default=-1),
        transaction_hash=tx_hash,
        api_latency_s=max(0.0, float(observed_ts) - float(event_ts)),
        raw={**raw, "_walletCopySource": "rtds_activity"},
    )


def _wallet_event_from_sample(row: dict[str, Any], *, wallet: str, wallet_name: str = "") -> WalletEvent | None:
    market_slug = str(row.get("market_slug") or "")
    condition_id = str(row.get("condition_id") or "")
    token_id = str(row.get("asset") or row.get("token_id") or "")
    outcome = str(row.get("outcome") or "")
    price = _float(row.get("price"))
    size = _float(row.get("size"))
    event_ts = _float(row.get("event_ts"))
    observed_ts = _float(row.get("received_at_s") or row.get("observed_ts") or event_ts)
    if not (wallet and market_slug and condition_id and token_id and outcome and price and size and event_ts):
        return None
    tx_hash = str(row.get("transaction_hash") or "")
    event_id = stable_id(
        "we",
        {
            "source": "rtds_activity_sample",
            "wallet": wallet,
            "tx": tx_hash,
            "condition_id": condition_id,
            "token_id": token_id,
            "outcome": outcome,
            "side": "BUY",
            "price": round(float(price), 8),
            "size": round(float(size), 8),
            "event_ts": event_ts,
        },
    )
    return WalletEvent(
        source_wallet=wallet,
        wallet_name=wallet_name or str(row.get("pseudonym") or row.get("name") or wallet),
        row_type="trade",
        action="BUY",
        condition_id=condition_id,
        market_slug=market_slug,
        outcome=outcome,
        price=float(price),
        size=float(size),
        usdc_size=round(float(price) * float(size), 6),
        event_ts=float(event_ts),
        observed_ts=float(observed_ts),
        event_id=event_id,
        source="rtds_activity_sample",
        market_id=condition_id,
        event_slug=market_slug,
        asset="BTC" if market_slug.startswith("btc-updown-5m-") else "",
        duration="5m" if market_slug.startswith("btc-updown-5m-") else "",
        window_start_s=_window_start_from_slug(market_slug),
        token_id=token_id,
        transaction_hash=tx_hash,
        api_latency_s=max(0.0, float(observed_ts) - float(event_ts)),
        raw=dict(row),
    )


def _load_events_by_wallet(
    *,
    payload: dict[str, Any],
    source_jsonl: str,
    tail_bytes: int,
    scan_limit: int,
    extra_wallets: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, list[WalletEvent]], dict[str, Any]]:
    wallets = _candidate_wallets(payload)
    if extra_wallets:
        for wallet, candidate in extra_wallets.items():
            wallets.setdefault(wallet, candidate)
    rows = _iter_tail_lines(source_jsonl, tail_bytes=tail_bytes)
    if scan_limit > 0:
        rows = rows[-int(scan_limit) :]
    by_wallet: dict[str, list[WalletEvent]] = defaultdict(list)
    scanned_rows = 0
    parse_errors = 0
    matching_rows = 0
    newest_source_event_ts = 0.0
    for line in rows:
        scanned_rows += 1
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            parse_errors += 1
            continue
        if not isinstance(row, dict):
            continue
        newest_source_event_ts = max(newest_source_event_ts, _source_event_ts(row))
        raw = row.get("raw") if isinstance(row.get("raw"), dict) else {}
        wallet = _norm_wallet(row.get("source_wallet") or raw.get("proxyWallet"))
        if wallet not in wallets:
            continue
        event = _wallet_event_from_rtds(row, wallet_name=str(wallets[wallet].get("candidate_id") or wallet))
        if event is None:
            continue
        matching_rows += 1
        by_wallet[wallet].append(event)

    for wallet, candidate in wallets.items():
        if by_wallet.get(wallet):
            continue
        samples = candidate.get("sample_events") if isinstance(candidate.get("sample_events"), list) else []
        for sample in samples:
            if not isinstance(sample, dict):
                continue
            event = _wallet_event_from_sample(
                sample,
                wallet=wallet,
                wallet_name=str(candidate.get("candidate_id") or wallet),
            )
            if event is not None:
                by_wallet[wallet].append(event)

    return (
        {wallet: unique_wallet_events(events) for wallet, events in by_wallet.items()},
        {
            "source_jsonl": source_jsonl,
            "tail_bytes": int(tail_bytes),
            "scan_limit": int(scan_limit),
            "scanned_rows": scanned_rows,
            "candidate_wallets_scanned": len(wallets),
            "parse_errors": parse_errors,
            "matching_rows": matching_rows,
            "wallets_with_events": sum(1 for events in by_wallet.values() if events),
            "newest_source_event_ts": newest_source_event_ts or None,
        },
    )


def _event_sample(event: WalletEvent) -> dict[str, Any]:
    return {
        "source_wallet": event.source_wallet,
        "market_slug": event.market_slug,
        "condition_id": event.condition_id,
        "outcome": event.outcome,
        "asset": event.token_id,
        "token_id": event.token_id,
        "price": event.price,
        "size": event.size,
        "event_ts": event.event_ts,
        "observed_ts": event.observed_ts,
        "transaction_hash": event.transaction_hash,
    }


def _registry_candidates_from_events(
    *,
    registry_candidates: dict[str, dict[str, Any]],
    existing_wallets: set[str],
    events_by_wallet: dict[str, list[WalletEvent]],
) -> list[dict[str, Any]]:
    added: list[dict[str, Any]] = []
    for wallet, candidate in sorted(registry_candidates.items()):
        if wallet in existing_wallets:
            continue
        events = [
            event
            for event in events_by_wallet.get(wallet, [])
            if event.action.upper() == "BUY" and event.market_slug.startswith("btc-updown-5m-")
        ]
        if not events:
            continue
        sorted_events = sorted(events, key=lambda event: (event.event_ts or 0.0, event.event_id))
        added.append(
            {
                **candidate,
                "status": "REGISTRY_RTDS_TAIL_MATCH",
                "candidate_id": str(candidate.get("candidate_id") or f"registry_live_band_{wallet[-12:]}"),
                "matched_buy_events": len(sorted_events),
                "unique_windows": len({event.market_slug for event in sorted_events}),
                "total_source_usd": round(sum(float(event.usdc_size or 0.0) for event in sorted_events), 6),
                "sample_events": [_event_sample(event) for event in sorted_events[-8:]],
            }
        )
    return added


def _book_for_token(
    token_id: str,
    *,
    client: CLOBMarketClient,
    cache: dict[str, dict[str, Any]],
    stats: Counter[str],
    max_clob_fetches: int,
) -> dict[str, Any]:
    if token_id in cache:
        stats["book_cache_hits"] += 1
        return {**cache[token_id], "cache_hit": True}
    if max_clob_fetches > 0 and stats["book_fetches"] >= int(max_clob_fetches):
        stats["book_fetch_budget_exhausted"] += 1
        return {"status": "FETCH_BUDGET_EXHAUSTED", "token_id": token_id}
    started = time.perf_counter()
    stats["book_fetches"] += 1
    primary_error = ""
    primary_route_report: dict[str, Any] = {}
    try:
        book = client.get_book(token_id)
        route_report = book.get("__walletCopyClobRouteReport") if isinstance(book, dict) else {}
        payload = {
            "status": "OK",
            "book": book if isinstance(book, dict) else {},
            "cache_hit": False,
            "fetch_duration_s": round(max(0.0, time.perf_counter() - started), 6),
            "route_report": route_report if isinstance(route_report, dict) else {},
        }
        stats["book_fetch_pass"] += 1
    except Exception as exc:  # noqa: BLE001 - replay records evidence failures.
        primary_error = str(exc)[:500]
        primary_route_report = client.last_route_report if isinstance(client.last_route_report, dict) else {}
        stats["book_fetch_primary_errors"] += 1

        try:
            response = requests.get(
                f"{CLOBMarketClient.DIRECT_CLOB_HOST}/book",
                params={"token_id": str(token_id)},
                timeout=max(0.1, float(client.timeout_s)),
                headers={"Accept": "application/json", "User-Agent": "wallet-copy-discover-replay/1.0"},
            )
            response.raise_for_status()
            book = response.json()
            route_report = {
                "status": "PASS",
                "route_class": "DIRECT_CLOB_FALLBACK",
                "host": "clob.polymarket.com",
                "routed_host": "clob.polymarket.com",
                "request_role": "discover_replay_direct_clob_book_fetch",
                "primary_error": primary_error,
                "primary_route_class": primary_route_report.get("route_class"),
            }
            if isinstance(book, dict):
                book = dict(book)
                book["__walletCopyClobRouteReport"] = route_report
            payload = {
                "status": "OK",
                "book": book if isinstance(book, dict) else {},
                "cache_hit": False,
                "fetch_duration_s": round(max(0.0, time.perf_counter() - started), 6),
                "route_report": route_report,
            }
            stats["book_fetch_direct_fallback_pass"] += 1
            stats["book_fetch_pass"] += 1
        except requests.HTTPError as direct_exc:
            status_code = getattr(getattr(direct_exc, "response", None), "status_code", None)
            payload = {
                "status": "BOOK_NOT_FOUND_OR_CLOSED" if status_code == 404 else "ERROR",
                "token_id": token_id,
                "cache_hit": False,
                "fetch_duration_s": round(max(0.0, time.perf_counter() - started), 6),
                "error": str(direct_exc)[:500],
                "http_status": status_code,
                "primary_error": primary_error,
                "route_report": primary_route_report,
            }
            stats["book_fetch_errors"] += 1
        except Exception as direct_exc:  # noqa: BLE001 - replay records evidence failures.
            payload = {
                "status": "ERROR",
                "token_id": token_id,
                "cache_hit": False,
                "fetch_duration_s": round(max(0.0, time.perf_counter() - started), 6),
                "error": str(direct_exc)[:500],
                "primary_error": primary_error,
                "route_report": primary_route_report,
            }
            stats["book_fetch_errors"] += 1
    if "payload" not in locals():
        payload = {
            "status": "ERROR",
            "token_id": token_id,
            "cache_hit": False,
            "fetch_duration_s": round(max(0.0, time.perf_counter() - started), 6),
            "error": primary_error or "unknown_clob_book_fetch_error",
            "route_report": primary_route_report,
        }
        stats["book_fetch_errors"] += 1
    cache[token_id] = payload
    return payload


def _intent_with_clob_evidence(
    intent: CopyIntent,
    *,
    client: CLOBMarketClient,
    book_cache: dict[str, dict[str, Any]],
    stats: Counter[str],
    slippage_bps: float,
    max_clob_fetches: int,
) -> CopyIntent:
    token_id = str(intent.token_id or "")
    metadata = dict(intent.metadata or {})
    evidence = metadata.get("live_tracking_evidence")
    evidence = dict(evidence) if isinstance(evidence, dict) else {}
    if not token_id:
        clob_book = {"enabled": True, "status": "MISSING_TOKEN", "token_id": token_id}
        stats["missing_token"] += 1
    else:
        fetched = _book_for_token(
            token_id,
            client=client,
            cache=book_cache,
            stats=stats,
            max_clob_fetches=int(max_clob_fetches),
        )
        if fetched.get("status") == "OK":
            book = fetched.get("book") if isinstance(fetched.get("book"), dict) else {}
            summary = CLOBMarketClient.summarize_book(
                book,
                copy_size_usd=float(intent.copy_size_usd),
                source_price=float(intent.limit_price),
                max_slippage_bps=float(slippage_bps),
            )
            route_report = fetched.get("route_report") if isinstance(fetched.get("route_report"), dict) else {}
            clob_book = {
                "enabled": True,
                "status": "OK",
                "cache_key": token_id,
                "cache_hit": bool(fetched.get("cache_hit")),
                "fetch_duration_s": fetched.get("fetch_duration_s"),
                "route_status": route_report.get("status"),
                "route_class": route_report.get("route_class"),
                "route_report_id": route_report.get("route_report_id"),
                "route_host": route_report.get("host"),
                "routed_host": route_report.get("routed_host"),
                "request_role": route_report.get("request_role"),
                **summary,
            }
        else:
            clob_book = {
                "enabled": True,
                "status": fetched.get("status") or "ERROR",
                "cache_key": token_id,
                "fetch_duration_s": fetched.get("fetch_duration_s"),
                "error": fetched.get("error"),
                "http_status": fetched.get("http_status"),
                "token_id": token_id,
            }
    evidence["clob_book"] = clob_book
    metadata["live_tracking_evidence"] = evidence
    return CopyIntent.from_dict({**intent.asdict(), "metadata": metadata})


def _score_replay(
    intents: list[CopyIntent],
    *,
    resolutions: dict[str, dict[str, Any]],
    slippage_bps: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    fill_config = FillModelConfig(
        model_id="discover_live_band_clob_backed_replay_v1",
        fallback_slippage_bps=float(slippage_bps),
        allow_fallback_without_book=False,
    )
    orders = [
        _order_from_intent(intent, slippage_bps=float(slippage_bps), fill_config=fill_config)
        for intent in intents
    ]
    scored, summary, fill_summary = _score_orders(orders, resolutions=resolutions)
    return orders, scored, summary, fill_summary


def _score_orders(
    orders: list[dict[str, Any]],
    *,
    resolutions: dict[str, dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any]]:
    scored = [score_order(order, resolutions) for order in orders]
    summary = _lifecycle_closed_score_summary(orders, scored, summarize_scores(scored))
    return scored, summary, _fill_evidence_summary(orders, scored)


def _order_market_slug(order: dict[str, Any]) -> str:
    source_intent = order.get("source_intent") if isinstance(order.get("source_intent"), dict) else {}
    return str(order.get("market_slug") or source_intent.get("market_slug") or "")


def _order_lifecycle_closed(order: dict[str, Any], scored: dict[str, Any], *, now_ts: float) -> bool:
    if scored.get("resolved"):
        return True
    slug = _order_market_slug(order)
    start = _window_start_from_slug(slug)
    if start is None:
        return True
    duration = 300 if "5m" in slug.lower() else 900 if "15m" in slug.lower() else 300
    return float(now_ts) >= float(start + duration)


def _lifecycle_closed_score_summary(
    orders: list[dict[str, Any]],
    scored: list[dict[str, Any]],
    summary: dict[str, Any],
    *,
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Compute unresolved ratio over lifecycle-closed markets only.

    Open BTC windows are pending evidence, not failed replay evidence. PnL,
    win rate, and resolved order counts still come from all resolved fills.
    """

    now = time.time() if now_ts is None else float(now_ts)
    closed_flags = [
        _order_lifecycle_closed(order, score, now_ts=now)
        for order, score in zip(orders, scored)
    ]
    pending_slugs = {
        _order_market_slug(order)
        for order, closed in zip(orders, closed_flags)
        if not closed and _order_market_slug(order)
    }
    closed_count = sum(1 for closed in closed_flags if closed)
    pending_count = max(0, len(scored) - closed_count)
    closed_unresolved = sum(
        1 for closed, score in zip(closed_flags, scored) if closed and not score.get("resolved")
    )
    adjusted = dict(summary)
    adjusted["raw_orders"] = len(scored)
    adjusted["lifecycle_unresolved_basis"] = "closed_markets_only"
    adjusted["unresolved_ratio_scope"] = "lifecycle_closed_markets_only"
    adjusted["lifecycle_unresolved_denominator"] = int(closed_count)
    adjusted["lifecycle_closed_orders"] = int(closed_count)
    adjusted["pending_lifecycle_orders"] = int(pending_count)
    adjusted["pending_lifecycle_market_slugs_sample"] = sorted(pending_slugs)[:10]
    adjusted["closed_unresolved_orders"] = int(closed_unresolved)
    adjusted["unresolved_orders"] = int(closed_unresolved)
    adjusted["unresolved_ratio"] = round(closed_unresolved / closed_count, 6) if closed_count else 0.0
    return adjusted


def _order_key(order: dict[str, Any]) -> str:
    return str(order.get("order_id") or order.get("intent_id") or "")


def _order_rank(order: dict[str, Any]) -> tuple[int, int]:
    fill = order.get("fill_estimate") if isinstance(order.get("fill_estimate"), dict) else {}
    final_status = str(order.get("final_status") or order.get("status") or "").upper()
    clob = str(fill.get("source") or "") == "clob_book_evidence"
    filled = final_status == "FILLED"
    return (1 if filled else 0, 1 if clob else 0)


def _clean_blocking_reason(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text or text in {"none", "ok", "pass", "null"}:
        return ""
    return text.removeprefix("clob_")


def _blocking_reason_from_book_status(value: Any) -> str:
    status = str(value or "").strip().upper()
    if status == "BOOK_NOT_FOUND_OR_CLOSED":
        return "book_not_found_or_closed"
    if status == "FETCH_BUDGET_EXHAUSTED":
        return "book_fetch_budget_exhausted"
    if status == "MISSING_TOKEN":
        return "missing_token"
    if status == "ERROR":
        return "book_fetch_error"
    if status == "MISSING":
        return "missing_clob_book_evidence"
    return ""


def _replay_reject_blocking_reason(fill: dict[str, Any]) -> str:
    details = fill.get("reject_details") if isinstance(fill.get("reject_details"), dict) else {}
    book = fill.get("book") if isinstance(fill.get("book"), dict) else {}
    for value in (details.get("blocking_reason"), fill.get("blocking_reason"), book.get("blocking_reason")):
        reason = _clean_blocking_reason(value)
        if reason:
            return reason

    status_reason = _blocking_reason_from_book_status(details.get("book_status") or book.get("status"))
    if status_reason:
        return status_reason

    for value in (fill.get("reject_reason"), fill.get("reject_stage")):
        reason = _clean_blocking_reason(value)
        if reason:
            return reason

    blockers = [_clean_blocking_reason(item) for item in fill.get("blockers") or [] if _clean_blocking_reason(item)]
    specific = [item for item in blockers if item != "fill_ratio_below_minimum"]
    return specific[0] if specific else (blockers[0] if blockers else "")


def _annotate_reject_blocking_reason(order: dict[str, Any]) -> dict[str, Any]:
    fill = order.get("fill_estimate") if isinstance(order.get("fill_estimate"), dict) else {}
    status = str(order.get("final_status") or order.get("status") or fill.get("status") or "").upper()
    if status != "REJECTED" or not fill:
        return dict(order)
    reason = _replay_reject_blocking_reason(fill)
    if not reason:
        return dict(order)
    updated_fill = dict(fill)
    reject_details = (
        dict(updated_fill.get("reject_details")) if isinstance(updated_fill.get("reject_details"), dict) else {}
    )
    reject_details["blocking_reason"] = reason
    reject_details.setdefault("blocking_reason_source", "replay_reject_attribution_v1")
    updated_fill["reject_details"] = reject_details
    updated_fill["blocking_reason"] = reason
    return {**order, "fill_estimate": updated_fill}


def _reject_blocking_reason_counts(orders: list[dict[str, Any]]) -> Counter[str]:
    counts: Counter[str] = Counter()
    for order in orders:
        fill = order.get("fill_estimate") if isinstance(order.get("fill_estimate"), dict) else {}
        status = str(order.get("final_status") or order.get("status") or fill.get("status") or "").upper()
        if status != "REJECTED":
            continue
        counts[_replay_reject_blocking_reason(fill) or "unknown_reject"] += 1
    return counts


def _stored_orders(candidate: dict[str, Any]) -> list[dict[str, Any]]:
    replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
    orders = replay.get("replay_orders")
    if not isinstance(orders, list):
        return []
    return [dict(order) for order in orders if isinstance(order, dict) and _order_key(order)]


def _merge_replay_orders(
    stored_orders: list[dict[str, Any]],
    new_orders: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order_index: dict[str, int] = {}
    for order in [*stored_orders, *new_orders]:
        key = _order_key(order)
        if not key:
            continue
        existing = merged.get(key)
        if existing is None:
            order_index[key] = len(order_index)
            merged[key] = dict(order)
            continue
        if _order_rank(order) >= _order_rank(existing):
            merged[key] = dict(order)
    return [merged[key] for key, _ in sorted(order_index.items(), key=lambda item: item[1])]


def _persistable_orders(orders: list[dict[str, Any]], *, limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    ranked = sorted(
        orders,
        key=lambda order: (
            _order_rank(order),
            float((order.get("source_intent") or {}).get("event_ts") or 0.0)
            if isinstance(order.get("source_intent"), dict)
            else 0.0,
            str(order.get("order_id") or ""),
        ),
        reverse=True,
    )
    return ranked[: int(limit)]


def _sample_scored_orders(
    orders: list[dict[str, Any]],
    scored: list[dict[str, Any]],
    *,
    limit: int = 8,
) -> list[dict[str, Any]]:
    paired = list(zip(orders, scored))
    paired.sort(
        key=lambda pair: (
            _order_rank(pair[0]),
            1 if pair[1].get("resolved") else 0,
            abs(float(pair[1].get("pnl_usd") or 0.0)),
        ),
        reverse=True,
    )
    return [score for _, score in paired[: max(0, int(limit))]]


def _event_has_resolution(event: WalletEvent, resolutions: dict[str, dict[str, Any]]) -> bool:
    """Return whether an event's market can be PnL-scored now.

    Current CLOB books are only valid for live fillability. For replay
    profitability, a fill on a still-unresolved BTC 5m window is not promotion
    evidence yet, so skip it before fetching current book evidence.
    """

    keys = [
        str(event.condition_id or ""),
        str(event.token_id or ""),
    ]
    if event.window_start_s:
        keys.append(f"slug_start:{int(event.window_start_s)}")
    else:
        start = _window_start_from_slug(event.market_slug)
        if start is not None:
            keys.append(f"slug_start:{int(start)}")
    return any(bool(resolutions.get(key)) for key in keys if key)


def _failure_reasons(
    *,
    intent_count: int,
    policy_event_count: int = 0,
    resolution_skipped_events: int = 0,
    summary: dict[str, Any],
    fill_summary: dict[str, Any],
    max_unresolved_ratio: float,
    max_rejected_fill_ratio: float,
) -> list[str]:
    reasons: list[str] = []
    if intent_count <= 0:
        if policy_event_count > 0 and resolution_skipped_events >= policy_event_count:
            reasons.append("candidate_resolution_missing_for_policy_events")
        else:
            reasons.append("candidate_no_policy_buy_events")
    if int(fill_summary.get("candidate_clob_backed_orders") or 0) <= 0:
        reasons.append("candidate_missing_clob_fill_evidence")
    rejected = int(fill_summary.get("candidate_rejected_fill_count") or 0)
    fillable_denominator = int(fill_summary.get("filled_orders") or 0) + rejected
    reject_ratio = float(rejected / fillable_denominator) if fillable_denominator > 0 else 0.0
    if reject_ratio > float(max_rejected_fill_ratio):
        reasons.append("candidate_rejected_fill_ratio_above_maximum")
    if float(summary.get("unresolved_ratio") or 0.0) > float(max_unresolved_ratio):
        reasons.append("candidate_unresolved_ratio_above_maximum")
    if float(summary.get("pnl_usd") or 0.0) <= 0.0:
        reasons.append("candidate_paper_pnl_not_positive")
    return sorted(set(reasons))


def _binding_gate(reasons: list[str]) -> str:
    priority = (
        ("candidate_paper_pnl_not_positive", "paper_pnl_sign"),
        ("candidate_missing_clob_fill_evidence", "clob_backed_order_count"),
        ("candidate_unresolved_ratio_above_maximum", "unresolved_ratio"),
        ("candidate_rejected_fill_ratio_above_maximum", "rejected_fill_ratio"),
        ("candidate_rejected_fill_events_present", "rejected_fill_events"),
        ("candidate_resolution_missing_for_policy_events", "resolution_sample"),
        ("candidate_no_policy_buy_events", "policy_buy_event_count"),
    )
    reason_set = set(reasons)
    for reason, gate in priority:
        if reason in reason_set:
            return gate
    return reasons[0] if reasons else "strict_pass"


def _near_miss_replays(candidates: list[dict[str, Any]], *, limit: int = 3) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate in candidates:
        replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
        if str(replay.get("eligibility_status") or "").upper() == "PASS":
            continue
        reasons = [str(reason) for reason in replay.get("failure_reasons") or [] if str(reason)]
        if not reasons:
            continue
        paper_pnl = _float(replay.get("paper_pnl_usd"), _float(replay.get("realized_pnl_usd")))
        clob_backed_orders = _int(replay.get("candidate_clob_backed_orders"))
        copyable_events = _int(replay.get("copyable_buy_events"))
        unresolved_ratio = _float(replay.get("unresolved_ratio"), 1.0)
        rows.append(
            {
                "candidate_id": candidate.get("candidate_id") or "",
                "wallet": _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet")),
                "binding_gate": _binding_gate(reasons),
                "failure_reasons": reasons,
                "paper_pnl_usd": round(paper_pnl, 6),
                "copyable_buy_events": copyable_events,
                "candidate_clob_backed_orders": clob_backed_orders,
                "unresolved_ratio": round(unresolved_ratio, 6),
                "resolved_orders": _int(replay.get("resolved_orders")),
                "paper_orders": _int(replay.get("paper_orders")),
            }
        )
    rows.sort(
        key=lambda row: (
            float(row.get("paper_pnl_usd") or 0.0),
            int(row.get("candidate_clob_backed_orders") or 0),
            int(row.get("copyable_buy_events") or 0),
            -float(row.get("unresolved_ratio") or 0.0),
        ),
        reverse=True,
    )
    return rows[: max(0, int(limit))]


def _policy_from_args(args: argparse.Namespace) -> CandidatePolicy:
    return CandidatePolicy(
        policy_id=str(args.policy_id or DEFAULT_POLICY_ID),
        min_price=float(args.min_buy_price),
        max_price=float(args.max_buy_price),
        wallet_fraction=float(args.wallet_fraction),
        max_order_usd=float(args.max_order_usd),
    )


def replay_discover_candidates(args: argparse.Namespace) -> dict[str, Any]:
    path = Path(str(args.discover_candidates))
    payload = load_json(path, default={})
    if not isinstance(payload, dict):
        raise SystemExit(f"invalid discover candidates payload: {path}")
    source_jsonl = str(args.source_jsonl or payload.get("source_jsonl") or "")
    if not source_jsonl:
        raise SystemExit("source_jsonl missing; pass --source-jsonl")
    tail_bytes = int(args.tail_bytes or (payload.get("scan") or {}).get("tail_bytes") or 256 * 1024 * 1024)
    registry_path = str(getattr(args, "registry", "configs/wallet_copy/wallets.json") or "")
    selected_wallets = {
        wallet
        for value in (getattr(args, "wallet", None) or [])
        if (wallet := _norm_wallet(value))
    }
    if getattr(args, "wallet", None) and len(selected_wallets) != len(getattr(args, "wallet", None) or []):
        raise SystemExit("every --wallet must be a full 0x-prefixed address")
    registry_candidates = (
        _registry_candidate_wallets(registry_path)
        if bool(getattr(args, "include_registry_wallets", True)) and registry_path.strip()
        else {}
    )
    if selected_wallets:
        registry_candidates = {
            wallet: candidate for wallet, candidate in registry_candidates.items() if wallet in selected_wallets
        }
    events_by_wallet, load_summary = _load_events_by_wallet(
        payload=payload,
        source_jsonl=source_jsonl,
        tail_bytes=tail_bytes,
        scan_limit=int(args.scan_limit),
        extra_wallets=registry_candidates,
    )
    resolutions = load_resolutions(args.resolutions)
    policy = _policy_from_args(args)
    client = CLOBMarketClient(timeout_s=float(args.clob_timeout_s), retries=int(args.clob_retries))
    book_cache: dict[str, dict[str, Any]] = {}
    stats: Counter[str] = Counter()
    all_candidates = payload.get("candidates") if isinstance(payload.get("candidates"), list) else []
    candidates = all_candidates
    if selected_wallets:
        candidates = [
            candidate
            for candidate in all_candidates
            if isinstance(candidate, dict)
            and _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet")) in selected_wallets
        ]
    existing_wallets = set(_candidate_wallets(payload))
    registry_added_candidates = _registry_candidates_from_events(
        registry_candidates=registry_candidates,
        existing_wallets=existing_wallets,
        events_by_wallet=events_by_wallet,
    )
    candidates = [*candidates, *registry_added_candidates]
    updated_candidates: list[dict[str, Any]] = []
    status_counts: Counter[str] = Counter()
    failure_counts: Counter[str] = Counter()
    reject_blocking_reason_counts: Counter[str] = Counter()
    total_orders = 0
    total_clob_backed_orders = 0
    max_rejected_fill_ratio = float(getattr(args, "max_rejected_fill_ratio", DEFAULT_MAX_REJECTED_FILL_RATIO))
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        wallet = _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet"))
        events = events_by_wallet.get(wallet, [])
        events = [event for event in events if event.action.upper() == "BUY" and event.market_slug.startswith("btc-updown-5m-")]
        if int(args.max_events_per_wallet) > 0:
            events = sorted(events, key=lambda event: (event.event_ts or 0.0, event.event_id))[-int(args.max_events_per_wallet) :]
        policy_events = [
            event
            for event in events
            if float(event.price or 0.0) >= float(policy.min_price)
            and float(event.price or 0.0) <= float(policy.max_price)
        ]
        policy_events_with_resolution = [event for event in policy_events if _event_has_resolution(event, resolutions)]
        resolution_skipped_events = max(0, len(policy_events) - len(policy_events_with_resolution))
        replay_events = policy_events if bool(getattr(args, "capture_unresolved_clob_books", True)) else policy_events_with_resolution
        base_intents = intents_for_policy(replay_events, policy)
        intents = [
            _intent_with_clob_evidence(
                intent,
                client=client,
                book_cache=book_cache,
                stats=stats,
                slippage_bps=float(args.slippage_bps),
                max_clob_fetches=int(args.max_clob_fetches),
            )
            for intent in base_intents
        ]
        new_orders, new_scored, new_summary, new_fill_summary = _score_replay(
            intents,
            resolutions=resolutions,
            slippage_bps=float(args.slippage_bps),
        )
        stored_orders = _stored_orders(candidate) if bool(getattr(args, "rescore_stored_orders", True)) else []
        orders = [
            _annotate_reject_blocking_reason(order)
            for order in _merge_replay_orders(stored_orders, new_orders)
        ]
        scored, summary, fill_summary = _score_orders(orders, resolutions=resolutions)
        candidate_reject_counts = _reject_blocking_reason_counts(orders)
        reject_blocking_reason_counts.update(candidate_reject_counts)
        reasons = _failure_reasons(
            intent_count=len(orders),
            policy_event_count=len(policy_events),
            resolution_skipped_events=resolution_skipped_events,
            summary=summary,
            fill_summary=fill_summary,
            max_unresolved_ratio=float(args.max_unresolved_ratio),
            max_rejected_fill_ratio=max_rejected_fill_ratio,
        )
        replay_status = "PASS" if not reasons else "FAIL_NO_CLOB_BACKED_POSITIVE_PAPER_REPLAY"
        status_counts[replay_status] += 1
        for reason in reasons:
            failure_counts[reason] += 1
        total_orders += len(orders)
        total_clob_backed_orders += int(fill_summary.get("candidate_clob_backed_orders") or 0)
        max_stored_orders = int(getattr(args, "max_stored_orders_per_candidate", 500) or 0)
        replay = {
            "status": "COMPLETE",
            "eligibility_status": replay_status,
            "failure_reasons": reasons,
            "replay_source": REPLAY_SOURCE,
            "completed_at_iso": utc_now_iso(),
            "policy_id": policy.policy_id,
            "max_buy_price": round(float(args.max_buy_price), 6),
            "max_unresolved_ratio": round(float(args.max_unresolved_ratio), 6),
            "max_rejected_fill_ratio": round(max_rejected_fill_ratio, 6),
            "require_candidate_clob_fill_evidence": True,
            "capture_unresolved_clob_books": bool(getattr(args, "capture_unresolved_clob_books", True)),
            "paper_orders": len(orders),
            "new_paper_orders": len(new_orders),
            "stored_paper_orders_rescored": len(stored_orders),
            "policy_buy_events": len(policy_events),
            "policy_buy_events_with_resolution": len(policy_events_with_resolution),
            "resolution_skipped_policy_buy_events": resolution_skipped_events,
            "copyable_buy_events": int(fill_summary.get("filled_orders") or 0),
            "paper_pnl_usd": round(float(summary.get("pnl_usd") or 0.0), 6),
            "realized_pnl_usd": round(float(summary.get("pnl_usd") or 0.0), 6),
            "unresolved_ratio": round(float(summary.get("unresolved_ratio") or 0.0), 6),
            "resolved_orders": int(summary.get("resolved_orders") or 0),
            "lifecycle_unresolved_basis": summary.get("lifecycle_unresolved_basis"),
            "lifecycle_closed_orders": int(summary.get("lifecycle_closed_orders") or 0),
            "pending_lifecycle_orders": int(summary.get("pending_lifecycle_orders") or 0),
            "closed_unresolved_orders": int(summary.get("closed_unresolved_orders") or 0),
            "candidate_clob_backed_orders": int(fill_summary.get("candidate_clob_backed_orders") or 0),
            "candidate_clob_backed_resolved_orders": int(
                fill_summary.get("candidate_clob_backed_resolved_orders") or 0
            ),
            "candidate_rejected_fill_count": int(fill_summary.get("candidate_rejected_fill_count") or 0),
            "candidate_rejected_fill_ratio": round(
                float(
                    int(fill_summary.get("candidate_rejected_fill_count") or 0)
                    / max(
                        1,
                        int(fill_summary.get("filled_orders") or 0)
                        + int(fill_summary.get("candidate_rejected_fill_count") or 0),
                    )
                ),
                6,
            ),
            "reject_blocking_reason_counts": dict(sorted(candidate_reject_counts.items())),
            "fill_source_counts": fill_summary.get("fill_source_counts") or {},
            "score_summary": summary,
            "new_score_summary": new_summary,
            "new_fill_source_counts": new_fill_summary.get("fill_source_counts") or {},
            "sample_scored_orders": _sample_scored_orders(orders, scored),
            "replay_orders": _persistable_orders(orders, limit=max_stored_orders),
        }
        updated = dict(candidate)
        updated["paper_replay"] = replay
        updated_candidates.append(updated)

    output = dict(payload)
    if selected_wallets:
        updated_by_wallet = {
            _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet")): candidate
            for candidate in updated_candidates
        }
        output_candidates = [
            updated_by_wallet.get(
                _norm_wallet(candidate.get("wallet") or candidate.get("source_wallet")),
                candidate,
            )
            if isinstance(candidate, dict)
            else candidate
            for candidate in all_candidates
        ]
        output["selected_wallets_replayed"] = sorted(selected_wallets)
    else:
        output_candidates = updated_candidates
        output.pop("selected_wallets_replayed", None)
    output["candidates"] = output_candidates
    output["candidate_count"] = len(output_candidates)
    output["generated_at_iso"] = utc_now_iso()
    output["generated_at_s"] = time.time()
    newest_source_event_ts = _float(load_summary.get("newest_source_event_ts"), 0.0)
    newest_source_event_age_s = (
        max(0.0, float(output["generated_at_s"]) - newest_source_event_ts)
        if newest_source_event_ts > 0
        else None
    )
    source_fresh = bool(
        newest_source_event_age_s is not None
        and newest_source_event_age_s <= DEFAULT_SOURCE_FRESHNESS_LIMIT_S
    )
    output["status"] = "PASS_CURRENT_SOURCE" if source_fresh else "STALE_SOURCE_FAIL_CLOSED"
    output["promotion_grade"] = source_fresh
    output["source_freshness"] = {
        "source_jsonl": source_jsonl,
        "newest_source_event_ts": newest_source_event_ts or None,
        "newest_source_event_age_s": round(newest_source_event_age_s, 6)
        if newest_source_event_age_s is not None
        else None,
        "freshness_limit_s": DEFAULT_SOURCE_FRESHNESS_LIMIT_S,
        "pass": source_fresh,
        "failure_reason": None if source_fresh else "missing_or_stale_source_event_timestamp",
    }
    global_status_counts: Counter[str] = Counter()
    global_failure_counts: Counter[str] = Counter()
    global_complete_replays = 0
    for candidate in output_candidates:
        if not isinstance(candidate, dict):
            continue
        replay = candidate.get("paper_replay") if isinstance(candidate.get("paper_replay"), dict) else {}
        if not replay:
            continue
        global_complete_replays += 1
        eligibility = str(replay.get("eligibility_status") or replay.get("status") or "UNKNOWN")
        global_status_counts[eligibility] += 1
        for reason in replay.get("failure_reasons") or []:
            global_failure_counts[str(reason)] += 1
    output["replay_summary"] = {
        "flow_stage": "DISCOVER/ROTATE",
        "replay_source": REPLAY_SOURCE,
        "candidate_count": len(output_candidates),
        "total_candidate_count": len(output_candidates),
        "selected_candidate_count": len(updated_candidates),
        "complete_replays": global_complete_replays,
        "registry_wallets_scanned": len(registry_candidates),
        "registry_candidates_added": len(registry_added_candidates),
        "promotable_replays": int(global_status_counts.get("PASS") or 0),
        "status_counts": dict(sorted(global_status_counts.items())),
        "failure_reason_counts": dict(sorted(global_failure_counts.items())),
        "selected_status_counts": dict(sorted(status_counts.items())),
        "selected_failure_reason_counts": dict(sorted(failure_counts.items())),
        "total_paper_orders": total_orders,
        "total_candidate_clob_backed_orders": total_clob_backed_orders,
        "book_fetches": int(stats.get("book_fetches") or 0),
        "book_fetch_pass": int(stats.get("book_fetch_pass") or 0),
        "book_fetch_primary_errors": int(stats.get("book_fetch_primary_errors") or 0),
        "book_fetch_direct_fallback_pass": int(stats.get("book_fetch_direct_fallback_pass") or 0),
        "book_fetch_errors": int(stats.get("book_fetch_errors") or 0),
        "book_cache_hits": int(stats.get("book_cache_hits") or 0),
        "book_fetch_budget_exhausted": int(stats.get("book_fetch_budget_exhausted") or 0),
        "reject_blocking_reason_counts": dict(sorted(reject_blocking_reason_counts.items())),
        "load_summary": load_summary,
    }
    output["replay_summary"]["near_miss_replays"] = _near_miss_replays(updated_candidates, limit=3)
    output["next_action"] = (
        "rerun select_wallet_copy_promotion_rotation.py --discover-candidates; "
        "first positive CLOB-backed PASS is eligible for Fable pin"
    )
    out_path = Path(str(args.output or args.discover_candidates))
    atomic_write_json(out_path, output)
    return output


def main() -> int:
    args = parse_args()
    output = replay_discover_candidates(args)
    summary = output.get("replay_summary") if isinstance(output.get("replay_summary"), dict) else {}
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
