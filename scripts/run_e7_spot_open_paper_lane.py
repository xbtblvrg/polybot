#!/usr/bin/env python3
"""Run the E7 spot-vs-open BTC-5m paper lane.

Flow stage: OBSERVE/LEARN/PROMOTE. This is a paper-only evidence lane. It
checks the current BTC 5m window during T-90..T-60, computes the Binance
spot-vs-open signal, snapshots live CLOB books for both outcomes, and records
whether an $8 paper entry would fill at the actual ask. It never creates
CopyIntents and never touches live execution state.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.refresh_btc_5m_resolutions_from_gamma import _fetch_gamma_event, _json_list  # noqa: E402
from scripts.refresh_btc_5m_resolutions_from_history import fetch_1m_klines  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import num, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, json_file_lock, load_json  # noqa: E402


DEFAULT_LANE_ID = "e7_spot_open_btc5m_v1"
DEFAULT_STATE = "data/research/e7_spot_open_paper_lane_state.json"
DEFAULT_EVENT_LOG = "data/research/e7_spot_open_paper_lane_events.jsonl"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_CLOB_BASE = "http://127.0.0.1:8787/clob"
MAX_DELTA_WINDOWS = 288
GAMMA_ROUTE_ENV_VARS = (
    "POLYMARKET_GAMMA_API_BASE_URL",
    "POLYMARKET_SOURCE_PROXY_URL",
    "POLYMARKET_HTTPS_PROXY",
)
CLOB_ROUTE_ENV_VARS = (
    "POLYMARKET_CLOB_API_BASE_URL",
    "POLYMARKET_SOURCE_PROXY_URL",
    "POLYMARKET_HTTPS_PROXY",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--lane-id", default=DEFAULT_LANE_ID)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--event-log", default=DEFAULT_EVENT_LOG)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--threshold-bps", type=float, default=10.0)
    parser.add_argument("--order-usd", type=float, default=8.0)
    parser.add_argument("--max-entry-price", type=float, default=0.90)
    parser.add_argument("--signal-start-s", type=float, default=210.0)
    parser.add_argument("--signal-end-s", type=float, default=240.0)
    parser.add_argument("--timeout-s", type=float, default=5.0)
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--ask-sample-offsets", default="30,150")
    parser.add_argument("--ask-sample-tolerance-s", type=float, default=7.5)
    parser.add_argument("--paper-quote-offset-s", type=float, default=-1.0)
    parser.add_argument("--paper-size-mode", choices=("equal_dollars", "equal_shares"), default="equal_dollars")
    parser.add_argument("--maker-quote-tick-size", type=float, default=0.01)
    parser.add_argument("--maker-quote-cancel-before-close-s", type=float, default=30.0)
    parser.add_argument("--reset-state", action="store_true")
    parser.add_argument("--now-ts", type=float, default=0.0, help="test hook; defaults to current time")
    return parser.parse_args()


def _lane_id(args: argparse.Namespace) -> str:
    return str(getattr(args, "lane_id", "") or DEFAULT_LANE_ID)


def _paper_size_mode(args: argparse.Namespace) -> str:
    mode = str(getattr(args, "paper_size_mode", "equal_dollars") or "equal_dollars")
    return mode if mode in {"equal_dollars", "equal_shares"} else "equal_dollars"


def _variant_id_fields(args: argparse.Namespace) -> dict[str, str]:
    fields: dict[str, str] = {}
    lane_id = _lane_id(args)
    size_mode = _paper_size_mode(args)
    if lane_id != DEFAULT_LANE_ID:
        fields["lane_id"] = lane_id
    if size_mode != "equal_dollars":
        fields["paper_size_mode"] = size_mode
    return fields


def _window_start(now_ts: float) -> int:
    return int(float(now_ts) // 300) * 300


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), UTC).isoformat().replace("+00:00", "Z")


def _percentile(values: list[float], pct: float) -> float:
    clean = sorted(value for value in values if math.isfinite(value))
    if not clean:
        return 0.0
    if len(clean) == 1:
        return round(clean[0], 6)
    rank = (len(clean) - 1) * pct
    lower = int(math.floor(rank))
    upper = int(math.ceil(rank))
    if lower == upper:
        return round(clean[lower], 6)
    return round(clean[lower] + (clean[upper] - clean[lower]) * (rank - lower), 6)


def _distribution(values: list[float]) -> dict[str, Any]:
    clean = sorted(value for value in values if math.isfinite(value) and value > 0)
    return {
        "count": len(clean),
        "min": round(clean[0], 6) if clean else 0.0,
        "p50": _percentile(clean, 0.50),
        "p90": _percentile(clean, 0.90),
        "max": round(clean[-1], 6) if clean else 0.0,
    }


def _parse_offsets(raw: str) -> list[float]:
    offsets: list[float] = []
    for part in str(raw or "").split(","):
        part = part.strip()
        if not part:
            continue
        value = num(part)
        if math.isfinite(value) and 0.0 <= value < 300.0:
            offsets.append(round(value, 6))
    return sorted(set(offsets))


def _target_sample_offset(offset_s: float, configured_offsets: list[float], tolerance_s: float) -> float | None:
    best: tuple[float, float] | None = None
    for target in configured_offsets:
        distance = abs(float(offset_s) - float(target))
        if distance <= float(tolerance_s) and (best is None or distance < best[0]):
            best = (distance, target)
    return best[1] if best else None


def delta_window_distribution(records: list[dict[str, Any]]) -> dict[str, Any]:
    values = sorted(
        value
        for row in records
        if math.isfinite(value := num(row.get("best_abs_delta_bps"))) and value >= 0
    )
    return {
        "count": len(values),
        "p50_abs_delta_bps": _percentile(values, 0.50),
        "p90_abs_delta_bps": _percentile(values, 0.90),
        "max_abs_delta_bps": round(values[-1], 6) if values else 0.0,
    }


def gamma_degraded_signal_events(signal_events: list[dict[str, Any]]) -> int:
    count = 0
    for row in signal_events:
        if not isinstance(row, dict):
            continue
        market = row.get("market") if isinstance(row.get("market"), dict) else {}
        if market.get("gamma_fetch_error"):
            count += 1
            continue
        tokens = market.get("tokens") if isinstance(market.get("tokens"), dict) else {}
        entry = row.get("entry") if isinstance(row.get("entry"), dict) else {}
        if not tokens and str(entry.get("reject_reason") or "") == "missing_token":
            count += 1
    return count


def no_ask_signal_events(signal_events: list[dict[str, Any]]) -> int:
    count = 0
    for row in signal_events:
        if not isinstance(row, dict):
            continue
        predicted = str(row.get("predicted_outcome") or "")
        books = row.get("books") if isinstance(row.get("books"), dict) else {}
        book = books.get(predicted) if predicted else None
        if not isinstance(book, dict) or str(book.get("status") or "") != "OK":
            continue
        entry = row.get("entry") if isinstance(row.get("entry"), dict) else {}
        if str(entry.get("reject_reason") or "") == "missing_best_ask" and num(book.get("best_ask")) <= 0:
            count += 1
    return count


def _implied_predicted_price(predicted_outcome: str, books: dict[str, dict[str, Any]]) -> float:
    opposite = "Down" if predicted_outcome == "Up" else "Up" if predicted_outcome == "Down" else ""
    opposite_book = books.get(opposite) if opposite else None
    if not isinstance(opposite_book, dict) or str(opposite_book.get("status") or "") != "OK":
        return 0.0
    opposite_ask = num(opposite_book.get("best_ask"))
    if opposite_ask <= 0:
        return 0.0
    return round(max(0.0, min(1.0, 1.0 - opposite_ask)), 6)


def _annotate_signal_event(event: dict[str, Any]) -> dict[str, Any]:
    predicted = str(event.get("predicted_outcome") or "")
    books = event.get("books") if isinstance(event.get("books"), dict) else {}
    book = books.get(predicted) if predicted else None
    if not isinstance(book, dict):
        return event
    entry = event.get("entry") if isinstance(event.get("entry"), dict) else {}
    if str(book.get("status") or "") == "OK" and num(book.get("best_ask")) <= 0:
        book["predicted_side_has_ask"] = False
        if str(entry.get("reject_reason") or "") == "missing_best_ask":
            event["no_ask_signal"] = True
            event["implied_predicted_price"] = _implied_predicted_price(predicted, books)
    elif str(book.get("status") or "") == "OK":
        book["predicted_side_has_ask"] = True
    return event


def signal_offset_distribution(signal_events: list[dict[str, Any]]) -> dict[str, Any]:
    return _distribution([num(row.get("signal_offset_s")) for row in signal_events if isinstance(row, dict)])


def _delta_observation_from_cycle(
    event: dict[str, Any] | None,
    cycle_diag: dict[str, Any],
    *,
    observed_at: str,
) -> dict[str, Any] | None:
    spot = event.get("spot_signal") if isinstance(event, dict) else cycle_diag.get("spot")
    if not isinstance(spot, dict) or str(spot.get("status") or "") not in {"SIGNAL", "NO_SIGNAL_THRESHOLD"}:
        return None
    delta_bps = num(spot.get("delta_bps"))
    if not math.isfinite(delta_bps):
        return None
    window_start = int(num((event or {}).get("window_start_s") or cycle_diag.get("window_start_s")))
    if window_start <= 0:
        return None
    offset_s = num((event or {}).get("signal_offset_s") or cycle_diag.get("offset_s"))
    return {
        "window_start_s": window_start,
        "market_slug": str((event or {}).get("market_slug") or f"btc-updown-5m-{window_start}"),
        "observed_at": observed_at,
        "signal_offset_s": round(offset_s, 6),
        "delta_bps": round(delta_bps, 6),
        "abs_delta_bps": round(abs(delta_bps), 6),
        "spot_status": str(spot.get("status") or ""),
        "threshold_bps": num(spot.get("threshold_bps")),
    }


def merge_delta_window_records(
    prior_records: list[dict[str, Any]],
    observation: dict[str, Any] | None,
    *,
    cap: int = MAX_DELTA_WINDOWS,
) -> list[dict[str, Any]]:
    by_window: dict[int, dict[str, Any]] = {}
    for row in prior_records:
        if not isinstance(row, dict):
            continue
        window_start = int(num(row.get("window_start_s")))
        if window_start > 0:
            by_window[window_start] = dict(row)

    if isinstance(observation, dict):
        window_start = int(num(observation.get("window_start_s")))
        if window_start > 0:
            current = by_window.get(window_start, {"window_start_s": window_start})
            current["market_slug"] = str(observation.get("market_slug") or current.get("market_slug") or "")
            current["threshold_bps"] = num(observation.get("threshold_bps") or current.get("threshold_bps"))
            current["in_band_samples"] = int(num(current.get("in_band_samples"))) + 1
            current["last_delta_bps"] = round(num(observation.get("delta_bps")), 6)
            current["last_abs_delta_bps"] = round(num(observation.get("abs_delta_bps")), 6)
            current["last_signal_offset_s"] = round(num(observation.get("signal_offset_s")), 6)
            current["last_observed_at"] = str(observation.get("observed_at") or "")
            current["last_spot_status"] = str(observation.get("spot_status") or "")
            prior_best = num(current.get("best_abs_delta_bps"), -1.0)
            candidate_best = num(observation.get("abs_delta_bps"), -1.0)
            if candidate_best >= prior_best:
                current["best_abs_delta_bps"] = round(candidate_best, 6)
                current["best_delta_bps"] = round(num(observation.get("delta_bps")), 6)
                current["best_signal_offset_s"] = round(num(observation.get("signal_offset_s")), 6)
                current["best_observed_at"] = str(observation.get("observed_at") or "")
                current["best_spot_status"] = str(observation.get("spot_status") or "")
            by_window[window_start] = current

    return sorted(by_window.values(), key=lambda row: int(num(row.get("window_start_s"))))[-max(1, int(cap)) :]


def spot_signal_from_klines(
    klines: dict[int, list[Any]],
    *,
    window_start_s: int,
    now_ts: float,
    threshold_bps: float,
) -> dict[str, Any]:
    open_row = klines.get(int(window_start_s))
    signal_minute = int(float(now_ts) // 60) * 60
    signal_row = None
    for minute in sorted(key for key in klines if key <= signal_minute):
        if minute >= int(window_start_s):
            signal_row = klines[minute]
    if open_row is None or signal_row is None:
        return {"status": "MISSING_KLINE", "signal_minute_s": signal_minute}
    open_price = num(open_row[1] if len(open_row) > 1 else 0.0)
    signal_price = num(signal_row[4] if len(signal_row) > 4 else signal_row[1] if len(signal_row) > 1 else 0.0)
    if open_price <= 0 or signal_price <= 0:
        return {"status": "BAD_KLINE_PRICE", "signal_minute_s": signal_minute}
    delta_bps = (signal_price - open_price) / open_price * 10_000.0
    predicted = "Up" if delta_bps >= float(threshold_bps) else "Down" if delta_bps <= -float(threshold_bps) else ""
    return {
        "status": "SIGNAL" if predicted else "NO_SIGNAL_THRESHOLD",
        "window_open_price": round(open_price, 8),
        "signal_price": round(signal_price, 8),
        "signal_minute_s": signal_minute,
        "delta_bps": round(delta_bps, 6),
        "threshold_bps": round(float(threshold_bps), 6),
        "predicted_outcome": predicted,
    }


def _matching_market(events: list[dict[str, Any]], slug: str) -> dict[str, Any]:
    for event in events:
        if not isinstance(event, dict):
            continue
        for market in event.get("markets") or []:
            if isinstance(market, dict) and str(market.get("slug") or "") == slug:
                return dict(market)
    return {}


def _fetch_gamma_event_direct(slug: str, *, timeout_s: float, user_agent: str) -> list[dict[str, Any]]:
    prior_env: dict[str, str] = {}
    for name in GAMMA_ROUTE_ENV_VARS:
        if name in os.environ:
            prior_env[name] = os.environ.pop(name)
    try:
        return _fetch_gamma_event(slug, timeout_s=timeout_s, user_agent=user_agent)
    finally:
        for name, value in prior_env.items():
            os.environ[name] = value


def _with_cleared_env(env_vars: tuple[str, ...], func):
    prior_env: dict[str, str] = {}
    for name in env_vars:
        if name in os.environ:
            prior_env[name] = os.environ.pop(name)
    try:
        return func()
    finally:
        for name, value in prior_env.items():
            os.environ[name] = value


def _market_for_slug(slug: str, *, timeout_s: float) -> dict[str, Any]:
    user_agent = "wallet-copy-e7-paper-lane/1.0"
    attempts: list[dict[str, Any]] = []
    try:
        events = _fetch_gamma_event(slug, timeout_s=timeout_s, user_agent=user_agent)
    except Exception as exc:  # noqa: BLE001 - signal evidence must survive route degradation.
        attempts.append(
            {
                "route": "configured_gamma_route",
                "status": "ERROR",
                "error_type": type(exc).__name__,
                "error": str(exc)[:300],
            }
        )
    else:
        market = _matching_market(events, slug)
        attempts.append(
            {
                "route": "configured_gamma_route",
                "status": "PASS" if market else "NO_MATCH",
                "events": len(events),
            }
        )
        if market:
            market["_gamma_route_used"] = "configured_gamma_route"
            market["_gamma_route_attempts"] = attempts
            return market

    route_env_configured = any(os.environ.get(name, "").strip() for name in GAMMA_ROUTE_ENV_VARS)
    if route_env_configured:
        try:
            events = _fetch_gamma_event_direct(slug, timeout_s=timeout_s, user_agent=user_agent)
        except Exception as exc:  # noqa: BLE001 - fallback failure is persisted as evidence.
            attempts.append(
                {
                    "route": "direct_gamma_fallback",
                    "status": "ERROR",
                    "error_type": type(exc).__name__,
                    "error": str(exc)[:300],
                }
            )
        else:
            market = _matching_market(events, slug)
            attempts.append(
                {
                    "route": "direct_gamma_fallback",
                    "status": "PASS" if market else "NO_MATCH",
                    "events": len(events),
                }
            )
            if market:
                market["_gamma_route_used"] = "direct_gamma_fallback"
                market["_gamma_route_attempts"] = attempts
                market["_fetch_recovered_from_error"] = attempts[0] if attempts else {}
                return market

    error_attempt = next((row for row in reversed(attempts) if row.get("status") == "ERROR"), None)
    if error_attempt:
        return {
            "_fetch_error": str(error_attempt.get("error_type") or "GammaFetchError"),
            "_fetch_error_message": str(error_attempt.get("error") or "")[:300],
            "_gamma_route_attempts": attempts,
            "_gamma_route_used": "",
        }
    return {"_gamma_route_attempts": attempts, "_gamma_route_used": ""}


def _token_map(market: dict[str, Any]) -> dict[str, str]:
    if market.get("_fetch_error"):
        return {}
    outcomes = [str(item) for item in _json_list(market.get("outcomes"))]
    tokens = [str(item) for item in _json_list(market.get("clobTokenIds") or market.get("clob_token_ids"))]
    if not outcomes and len(tokens) >= 2:
        outcomes = ["Up", "Down"]
    out: dict[str, str] = {}
    for index, outcome in enumerate(outcomes):
        if index < len(tokens) and str(outcome) in {"Up", "Down"} and tokens[index]:
            out[str(outcome)] = tokens[index]
    return out


def _book_snapshot(
    *,
    clob: CLOBMarketClient,
    token_id: str,
    order_usd: float,
    max_entry_price: float,
) -> dict[str, Any]:
    if not token_id:
        return {"status": "MISSING_TOKEN", "instant_fill_status": "BLOCKED", "blocking_reason": "missing_token"}
    try:
        book = clob.get_book(token_id)
        summary = CLOBMarketClient.summarize_book(
            book,
            copy_size_usd=float(order_usd),
            source_price=float(max_entry_price),
            max_slippage_bps=0.0,
        )
        return {
            "status": "OK",
            **summary,
            "route_report": clob.last_route_report if isinstance(clob.last_route_report, dict) else {},
        }
    except Exception as exc:  # noqa: BLE001 - this is evidence, not a live path.
        return {
            "status": "ERROR",
            "instant_fill_status": "BLOCKED",
            "blocking_reason": "book_fetch_error",
            "error_type": type(exc).__name__,
            "error": str(exc)[:300],
            "route_report": clob.last_route_report if isinstance(clob.last_route_report, dict) else {},
        }


def _book_snapshot_with_direct_fallback(
    *,
    clob: CLOBMarketClient,
    token_id: str,
    order_usd: float,
    max_entry_price: float,
) -> dict[str, Any]:
    configured = _book_snapshot(
        clob=clob,
        token_id=token_id,
        order_usd=order_usd,
        max_entry_price=max_entry_price,
    )
    configured["book_route_used"] = "configured_clob_route"
    route_env_configured = any(os.environ.get(name, "").strip() for name in CLOB_ROUTE_ENV_VARS)
    if configured.get("status") != "ERROR" or not route_env_configured or not token_id:
        return configured

    def fetch_direct() -> dict[str, Any]:
        direct_clob = CLOBMarketClient(
            CLOBMarketClient.DIRECT_CLOB_HOST,
            timeout_s=float(clob.timeout_s),
            retries=int(clob.retries),
        )
        return _book_snapshot(
            clob=direct_clob,
            token_id=token_id,
            order_usd=order_usd,
            max_entry_price=max_entry_price,
        )

    direct = _with_cleared_env(CLOB_ROUTE_ENV_VARS, fetch_direct)
    direct["book_route_used"] = "direct_clob_fallback"
    direct["configured_clob_route_error"] = {
        "status": configured.get("status"),
        "error_type": configured.get("error_type"),
        "error": configured.get("error"),
        "route_report": configured.get("route_report") if isinstance(configured.get("route_report"), dict) else {},
    }
    return direct


def _ask_sample_from_books(books: dict[str, dict[str, Any]]) -> dict[str, Any]:
    by_outcome: dict[str, dict[str, Any]] = {}
    asks: list[float] = []
    for outcome in ("Up", "Down"):
        book = books.get(outcome) if isinstance(books, dict) else None
        if not isinstance(book, dict):
            continue
        best_ask = num(book.get("best_ask"))
        has_ask = str(book.get("status") or "") == "OK" and best_ask > 0
        by_outcome[outcome] = {
            "status": str(book.get("status") or ""),
            "best_ask": round(best_ask, 6) if math.isfinite(best_ask) else 0.0,
            "best_bid": round(num(book.get("best_bid")), 6),
            "has_ask": bool(has_ask),
            "book_route_used": str(book.get("book_route_used") or "configured_clob_route"),
            "blocking_reason": str(book.get("blocking_reason") or ""),
        }
        if has_ask:
            asks.append(best_ask)
    return {
        "outcomes": by_outcome,
        "ask_present_outcomes": sum(1 for row in by_outcome.values() if row.get("has_ask")),
        "both_sides_have_ask": len([value for value in asks if value > 0]) == 2,
        "min_best_ask": round(min(asks), 6) if asks else 0.0,
    }


def _paper_quote_enabled(args: argparse.Namespace) -> bool:
    return num(getattr(args, "paper_quote_offset_s", -1.0), -1.0) >= 0.0


def _quote_price_from_book(book: dict[str, Any], *, max_price: float, tick_size: float) -> tuple[float, str]:
    if str(book.get("status") or "") != "OK":
        return 0.0, str(book.get("blocking_reason") or "book_not_ok")
    best_bid = num(book.get("best_bid"))
    best_ask = num(book.get("best_ask"))
    tick = max(0.000001, float(tick_size))
    cap = max(0.000001, float(max_price))
    if best_ask > tick:
        quote_price = min(cap, best_ask - tick)
        if quote_price >= best_ask:
            return 0.0, "quote_would_cross_best_ask"
        return round(max(0.01, quote_price), 6), ""
    if best_bid > 0:
        return round(max(0.01, min(cap, best_bid + tick)), 6), ""
    return 0.0, "missing_book_liquidity"


def _build_paper_quote_events(
    args: argparse.Namespace,
    prior_quote_ids: set[str],
    ask_sample: dict[str, Any] | None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if not _paper_quote_enabled(args):
        return [], {"status": "DISABLED"}
    if not isinstance(ask_sample, dict):
        return [], {"status": "NO_ASK_SAMPLE"}
    target_offset = num(ask_sample.get("target_offset_s"))
    selected_offset = num(getattr(args, "paper_quote_offset_s", -1.0), -1.0)
    if abs(target_offset - selected_offset) > max(0.001, num(getattr(args, "ask_sample_tolerance_s", 0.0), 0.0)):
        return [], {
            "status": "WAITING_SELECTED_OFFSET",
            "target_offset_s": target_offset,
            "selected_offset_s": selected_offset,
        }

    books = ask_sample.get("books") if isinstance(ask_sample.get("books"), dict) else {}
    market = ask_sample.get("market") if isinstance(ask_sample.get("market"), dict) else {}
    tokens = market.get("tokens") if isinstance(market.get("tokens"), dict) else {}
    lane_id = _lane_id(args)
    size_mode = _paper_size_mode(args)
    events: list[dict[str, Any]] = []
    diagnostics: Counter[str] = Counter()
    for outcome in ("Up", "Down"):
        book = books.get(outcome) if isinstance(books.get(outcome), dict) else {}
        quote_price, reject_reason = _quote_price_from_book(
            book,
            max_price=float(args.max_entry_price),
            tick_size=float(getattr(args, "maker_quote_tick_size", 0.01)),
        )
        quote_id = stable_id(
            "e7q",
            {
                "market_slug": ask_sample.get("market_slug"),
                "outcome": outcome,
                "target_offset_s": target_offset,
                "paper_quote_offset_s": selected_offset,
                **_variant_id_fields(args),
            },
        )
        if quote_id in prior_quote_ids:
            diagnostics["duplicate_quote"] += 1
            continue
        quote_status = "QUOTED" if quote_price > 0.0 and not reject_reason else "NO_QUOTE"
        if quote_status == "QUOTED" and size_mode == "equal_shares":
            requested_shares = round(float(args.order_usd), 6)
            order_usd = round(requested_shares * quote_price, 6)
        elif quote_status == "QUOTED":
            order_usd = round(float(args.order_usd), 6)
            requested_shares = round(order_usd / max(quote_price, 0.000001), 6)
        else:
            order_usd = 0.0
            requested_shares = 0.0
        diagnostics[quote_status.lower()] += 1
        if reject_reason:
            diagnostics[f"reject_{reject_reason}"] += 1
        events.append(
            {
                "schema_version": 1,
                "event": "e7_paper_maker_quote",
                "lane": lane_id,
                "flow_stage": "OBSERVE/LEARN/PROMOTE",
                "quote_id": quote_id,
                "generated_at": utc_now_iso(),
                "paper_only": True,
                "live_orders_allowed": False,
                "market_slug": str(ask_sample.get("market_slug") or ""),
                "condition_id": str(market.get("condition_id") or ""),
                "outcome": outcome,
                "side": "YES" if outcome == "Up" else "NO",
                "token_id": str(tokens.get(outcome) or ""),
                "window_start_s": int(num(ask_sample.get("window_start_s"))),
                "window_end_s": int(num(ask_sample.get("window_end_s"))),
                "target_offset_s": target_offset,
                "sample_offset_s": num(ask_sample.get("sample_offset_s")),
                "quote_status": quote_status,
                "reject_reason": reject_reason,
                "quote_price": quote_price,
                "order_usd": order_usd,
                "paper_size_mode": size_mode,
                "size_reference_usd": round(float(args.order_usd), 6),
                "requested_shares": requested_shares,
                "tick_size": round(float(getattr(args, "maker_quote_tick_size", 0.01)), 6),
                "max_price": round(float(args.max_entry_price), 6),
                "cancel_before_close_s": round(float(getattr(args, "maker_quote_cancel_before_close_s", 30.0)), 6),
                "top_of_book": book,
                "source_sample_id": str(ask_sample.get("sample_id") or ""),
                "zero_live_assertion": {"status": "PASS", "orders_submitted": 0},
            }
        )
    diagnostics["quote_events"] = len(events)
    return events, dict(sorted(diagnostics.items()))


def ask_sample_summary(samples: list[dict[str, Any]]) -> dict[str, Any]:
    by_offset: dict[str, dict[str, Any]] = {}
    windows: set[int] = set()
    real_book_windows: set[int] = set()
    real_book_sample_events = 0
    for sample in samples:
        if not isinstance(sample, dict):
            continue
        window_start = int(num(sample.get("window_start_s")))
        if window_start > 0:
            windows.add(window_start)
        offset = round(num(sample.get("target_offset_s")), 6)
        offset_key = str(int(offset)) if float(offset).is_integer() else str(offset)
        bucket = by_offset.setdefault(
            offset_key,
            {
                "sample_events": 0,
                "windows": set(),
                "real_book_windows": set(),
                "real_book_sample_events": 0,
                "both_sides_have_ask": 0,
                "both_sides_have_ask_real_books": 0,
                "outcomes": {
                    "Up": {"samples": 0, "real_book_samples": 0, "ask_present": 0, "best_asks": []},
                    "Down": {"samples": 0, "real_book_samples": 0, "ask_present": 0, "best_asks": []},
                },
            },
        )
        bucket["sample_events"] += 1
        if window_start > 0:
            bucket["windows"].add(window_start)
        ask_sample = sample.get("ask_sample") if isinstance(sample.get("ask_sample"), dict) else {}
        outcomes = ask_sample.get("outcomes") if isinstance(ask_sample.get("outcomes"), dict) else {}
        sample_books_real = all(
            isinstance(outcomes.get(outcome), dict) and str(outcomes.get(outcome, {}).get("status") or "") == "OK"
            for outcome in ("Up", "Down")
        )
        if sample_books_real:
            real_book_sample_events += 1
            bucket["real_book_sample_events"] += 1
            if window_start > 0:
                real_book_windows.add(window_start)
                bucket["real_book_windows"].add(window_start)
        if ask_sample.get("both_sides_have_ask"):
            bucket["both_sides_have_ask"] += 1
            if sample_books_real:
                bucket["both_sides_have_ask_real_books"] += 1
        for outcome in ("Up", "Down"):
            row = outcomes.get(outcome) if isinstance(outcomes.get(outcome), dict) else {}
            outcome_bucket = bucket["outcomes"][outcome]
            outcome_bucket["samples"] += 1
            if str(row.get("status") or "") == "OK":
                outcome_bucket["real_book_samples"] += 1
            best_ask = num(row.get("best_ask"))
            if row.get("has_ask") and best_ask > 0:
                outcome_bucket["ask_present"] += 1
                outcome_bucket["best_asks"].append(best_ask)

    normalized_offsets: dict[str, Any] = {}
    for offset_key, bucket in sorted(by_offset.items(), key=lambda item: num(item[0])):
        sample_events = int(bucket["sample_events"])
        outcomes: dict[str, Any] = {}
        for outcome, outcome_bucket in bucket["outcomes"].items():
            samples_count = int(outcome_bucket["samples"])
            real_book_samples = int(outcome_bucket["real_book_samples"])
            ask_present = int(outcome_bucket["ask_present"])
            outcomes[outcome] = {
                "samples": samples_count,
                "real_book_samples": real_book_samples,
                "ask_present": ask_present,
                "ask_present_pct": round((ask_present / samples_count) * 100.0, 6) if samples_count else 0.0,
                "ask_present_pct_of_real_books": round((ask_present / real_book_samples) * 100.0, 6)
                if real_book_samples
                else 0.0,
                "best_ask_distribution": _distribution([num(value) for value in outcome_bucket["best_asks"]]),
            }
        real_book_sample_count = int(bucket["real_book_sample_events"])
        normalized_offsets[offset_key] = {
            "sample_events": sample_events,
            "unique_windows": len(bucket["windows"]),
            "real_book_sample_events": real_book_sample_count,
            "real_book_unique_windows": len(bucket["real_book_windows"]),
            "both_sides_have_ask": int(bucket["both_sides_have_ask"]),
            "both_sides_have_ask_pct": round((int(bucket["both_sides_have_ask"]) / sample_events) * 100.0, 6)
            if sample_events
            else 0.0,
            "both_sides_have_ask_pct_of_real_books": round(
                (int(bucket["both_sides_have_ask_real_books"]) / real_book_sample_count) * 100.0, 6
            )
            if real_book_sample_count
            else 0.0,
            "outcomes": outcomes,
        }

    return {
        "sample_events": len([row for row in samples if isinstance(row, dict)]),
        "unique_windows": len(windows),
        "real_book_sample_events": real_book_sample_events,
        "real_book_unique_windows": len(real_book_windows),
        "offsets": normalized_offsets,
    }


def _build_ask_sample_event(
    args: argparse.Namespace,
    prior_sample_ids: set[str],
    now_ts: float,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    configured_offsets = _parse_offsets(str(args.ask_sample_offsets))
    start = _window_start(now_ts)
    offset_s = float(now_ts) - float(start)
    target_offset = _target_sample_offset(offset_s, configured_offsets, float(args.ask_sample_tolerance_s))
    if target_offset is None:
        return None, {
            "status": "WAITING_ASK_SAMPLE_OFFSET",
            "window_start_s": start,
            "offset_s": round(offset_s, 6),
            "configured_offsets_s": configured_offsets,
        }
    slug = f"btc-updown-5m-{start}"
    lane_id = _lane_id(args)
    sample_id = stable_id("e7ask", {"market_slug": slug, "target_offset_s": target_offset, **_variant_id_fields(args)})
    if sample_id in prior_sample_ids:
        return None, {
            "status": "DUPLICATE_ASK_SAMPLE",
            "sample_id": sample_id,
            "window_start_s": start,
            "offset_s": round(offset_s, 6),
            "target_offset_s": target_offset,
        }
    market = _market_for_slug(slug, timeout_s=float(args.timeout_s))
    tokens = _token_map(market)
    clob = CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
    books = {
        outcome: _book_snapshot_with_direct_fallback(
            clob=clob,
            token_id=tokens.get(outcome, ""),
            order_usd=float(args.order_usd),
            max_entry_price=1.0,
        )
        for outcome in ("Up", "Down")
    }
    event = {
        "schema_version": 1,
        "event": "e7_fixed_offset_ask_sample",
        "lane": lane_id,
        "flow_stage": "OBSERVE/LEARN/PROMOTE",
        "sample_id": sample_id,
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "market_slug": slug,
        "window_start_s": start,
        "window_end_s": start + 300,
        "target_offset_s": target_offset,
        "sample_offset_s": round(offset_s, 6),
        "market": {
            "condition_id": str(market.get("conditionId") or market.get("condition_id") or ""),
            "tokens": tokens,
            "gamma_slug_found": bool(market and not market.get("_fetch_error")),
            "gamma_fetch_error": market.get("_fetch_error") or "",
            "gamma_route_attempts": market.get("_gamma_route_attempts") or [],
            "gamma_route_used": market.get("_gamma_route_used") or "",
        },
        "books": books,
        "ask_sample": _ask_sample_from_books(books),
        "zero_live_assertion": {"status": "PASS", "orders_submitted": 0},
    }
    return event, {
        "status": "ASK_SAMPLE_RECORDED",
        "sample_id": sample_id,
        "window_start_s": start,
        "offset_s": round(offset_s, 6),
        "target_offset_s": target_offset,
    }


def entry_from_books(
    *,
    predicted_outcome: str,
    books: dict[str, dict[str, Any]],
    order_usd: float,
    max_entry_price: float,
) -> dict[str, Any]:
    book = books.get(predicted_outcome) if predicted_outcome else None
    if not isinstance(book, dict):
        return {"entry_status": "NO_SIGNAL", "paper_fill": False, "reject_reason": "no_predicted_outcome"}
    best_ask = num(book.get("best_ask"))
    fillable_usd = num(book.get("fillable_usd"))
    if str(book.get("status") or "") != "OK":
        return {"entry_status": "NO_ENTRY", "paper_fill": False, "reject_reason": str(book.get("blocking_reason") or "book_not_ok")}
    if best_ask <= 0:
        return {"entry_status": "NO_ENTRY", "paper_fill": False, "reject_reason": "missing_best_ask"}
    if best_ask > float(max_entry_price) + 1e-9:
        return {"entry_status": "NO_ENTRY", "paper_fill": False, "reject_reason": "ask_above_cap", "best_ask": round(best_ask, 6)}
    if fillable_usd + 1e-9 < float(order_usd):
        return {
            "entry_status": "NO_ENTRY",
            "paper_fill": False,
            "reject_reason": "insufficient_depth",
            "best_ask": round(best_ask, 6),
            "fillable_usd": round(fillable_usd, 6),
        }
    entry_price = num(book.get("avg_fill_price"), best_ask)
    return {
        "entry_status": "FILLED",
        "paper_fill": True,
        "reject_reason": "",
        "entry_price": round(entry_price, 6),
        "filled_size_usd": round(float(order_usd), 6),
        "filled_shares": round(float(order_usd) / max(entry_price, 0.000001), 6),
        "best_ask": round(best_ask, 6),
        "fillable_usd": round(fillable_usd, 6),
    }


def _order_from_event(event: dict[str, Any]) -> dict[str, Any] | None:
    entry = event.get("entry") if isinstance(event.get("entry"), dict) else {}
    if not entry.get("paper_fill"):
        return None
    market = event.get("market") if isinstance(event.get("market"), dict) else {}
    outcome = str(event.get("predicted_outcome") or "")
    token_id = str((market.get("tokens") if isinstance(market.get("tokens"), dict) else {}).get(outcome) or "")
    lane_id = str(event.get("lane") or DEFAULT_LANE_ID)
    order_payload = {"e7_signal_id": event.get("signal_id")}
    if lane_id != DEFAULT_LANE_ID:
        order_payload["lane_id"] = lane_id
    order_id = stable_id("po", order_payload)
    return {
        "order_id": order_id,
        "intent_id": "",
        "source_wallet": "E7_SPOT_OPEN",
        "wallet_name": lane_id,
        "condition_id": str(market.get("condition_id") or ""),
        "market_slug": str(event.get("market_slug") or ""),
        "outcome": outcome,
        "side": "YES" if outcome == "Up" else "NO",
        "token_id": token_id,
        "limit_price": entry.get("entry_price"),
        "requested_size_usd": event.get("order_usd"),
        "requested_shares": entry.get("filled_shares"),
        "filled_size_usd": entry.get("filled_size_usd"),
        "filled_shares": entry.get("filled_shares"),
        "status": "FILLED",
        "final_status": "FILLED",
        "submitted_at": event.get("generated_at"),
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "fill_model": "e7_spot_open_actual_ask_v1",
        "source_intent": {
            "mode": "paper",
            "live_orders_allowed": False,
            "strategy_family": lane_id,
            "metadata": {"copy_model": "e7_spot_open", "e7_spot_open_btc5m_v1": event},
        },
        "e7_signal": event,
        "lifecycle": [
            {
                "ts": utc_now_iso(),
                "status": "FILLED",
                "message": "paper E7 entry filled against actual ask snapshot",
                "payload": {"signal_id": event.get("signal_id"), "entry": entry},
            }
        ],
    }


def _quote_fill_from_samples(quote: dict[str, Any], samples: list[dict[str, Any]]) -> dict[str, Any] | None:
    quote_price = num(quote.get("quote_price"))
    if quote_price <= 0:
        return None
    quote_offset = num(quote.get("target_offset_s"))
    market_slug = str(quote.get("market_slug") or "")
    outcome = str(quote.get("outcome") or "")
    requested_shares = num(quote.get("requested_shares"))
    for sample in samples:
        if str(sample.get("market_slug") or "") != market_slug:
            continue
        if num(sample.get("target_offset_s")) <= quote_offset:
            continue
        ask_sample = sample.get("ask_sample") if isinstance(sample.get("ask_sample"), dict) else {}
        outcomes = ask_sample.get("outcomes") if isinstance(ask_sample.get("outcomes"), dict) else {}
        row = outcomes.get(outcome) if isinstance(outcomes.get(outcome), dict) else {}
        best_ask = num(row.get("best_ask"))
        if str(row.get("status") or "") == "OK" and row.get("has_ask") and 0.0 < best_ask <= quote_price + 1e-9:
            return {
                "fill_sample_id": str(sample.get("sample_id") or ""),
                "fill_observed_at": str(sample.get("generated_at") or ""),
                "fill_target_offset_s": num(sample.get("target_offset_s")),
                "fill_sample_offset_s": num(sample.get("sample_offset_s")),
                "fill_price": round(best_ask, 6),
                "requested_shares": round(requested_shares, 6),
                "crossing_rule": "later_best_ask_lte_maker_quote",
            }
    return None


def _order_from_paper_quote(quote: dict[str, Any], samples: list[dict[str, Any]], *, now_ts: float) -> dict[str, Any] | None:
    if str(quote.get("quote_status") or "") != "QUOTED":
        return None
    fill = _quote_fill_from_samples(quote, samples)
    window_end_s = num(quote.get("window_end_s"))
    cancel_ts = max(0.0, window_end_s - num(quote.get("cancel_before_close_s"), 30.0))
    if fill:
        final_status = "FILLED"
        filled_size_usd = num(quote.get("order_usd"))
        filled_shares = num(quote.get("requested_shares"))
        lifecycle_message = "paper E7 maker quote crossed by later book"
    elif now_ts >= cancel_ts:
        final_status = "CANCELLED"
        filled_size_usd = 0.0
        filled_shares = 0.0
        lifecycle_message = "paper E7 maker quote cancelled before close"
    else:
        final_status = "OPEN"
        filled_size_usd = 0.0
        filled_shares = 0.0
        lifecycle_message = "paper E7 maker quote resting"
    lane_id = str(quote.get("lane") or DEFAULT_LANE_ID)
    order_id = stable_id("po", {"e7_quote_id": quote.get("quote_id")})
    return {
        "order_id": order_id,
        "intent_id": "",
        "source_wallet": "E7_SPOT_OPEN",
        "wallet_name": lane_id,
        "condition_id": str(quote.get("condition_id") or ""),
        "market_slug": str(quote.get("market_slug") or ""),
        "outcome": str(quote.get("outcome") or ""),
        "side": str(quote.get("side") or ""),
        "token_id": str(quote.get("token_id") or ""),
        "limit_price": quote.get("quote_price"),
        "requested_size_usd": quote.get("order_usd"),
        "requested_shares": quote.get("requested_shares"),
        "filled_size_usd": round(filled_size_usd, 6),
        "filled_shares": round(filled_shares, 6),
        "status": final_status,
        "final_status": final_status,
        "submitted_at": quote.get("generated_at"),
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "fill_model": "e7_maker_quote_later_book_cross_v1",
        "maker_quote": quote,
        "maker_fill_evidence": fill,
        "source_intent": {
            "mode": "paper",
            "live_orders_allowed": False,
            "strategy_family": lane_id,
            "metadata": {"copy_model": "e7_spot_open_maker_quote", "e7_paper_maker_quote": quote},
        },
        "lifecycle": [
            {
                "ts": utc_now_iso(),
                "status": final_status,
                "message": lifecycle_message,
                "payload": {"quote_id": quote.get("quote_id"), "fill": fill},
            }
        ],
    }


def _merge_orders(prior_orders: list[dict[str, Any]], new_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {str(order.get("order_id") or ""): dict(order) for order in prior_orders if order.get("order_id")}
    for order in new_orders:
        if order.get("order_id"):
            by_id[str(order.get("order_id"))] = dict(order)
    return list(by_id.values())[-50_000:]


def _summarize_orders(orders: list[dict[str, Any]], resolutions_path: str) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
    resolutions = load_resolutions(resolutions_path)
    scored_pairs = [(order, score_order(order, resolutions)) for order in orders]
    scored = [row for _, row in scored_pairs]
    filled_orders = [order for order in orders if str(order.get("final_status") or order.get("status") or "").upper() == "FILLED"]
    resolved = [row for row in scored if row.get("resolved")]
    wins = [row for row in resolved if row.get("win")]
    cost = sum(num(row.get("cost_usd")) for row in resolved)
    payout = sum(num(row.get("payout_usd")) for row in resolved)
    pnl = sum(num(row.get("pnl_usd")) for row in resolved)
    return (
        {
            "paper_orders": len(orders),
            "paper_quotes": len(orders),
            "paper_filled_orders": len(filled_orders),
            "book_verified_fills": len(filled_orders),
            "resolved_paper_fills": len(resolved),
            "unresolved_paper_fills": len(filled_orders) - len(resolved),
            "resolved_paper_wins": len(wins),
            "resolved_paper_losses": len(resolved) - len(wins),
            "resolved_paper_cost_usd": round(cost, 6),
            "resolved_paper_payout_usd": round(payout, 6),
            "resolved_paper_pnl_usd": round(pnl, 6),
            "resolved_paper_roi_pct": round((pnl / cost) * 100.0, 6) if cost else 0.0,
            "resolved_paper_wr_pct": round((len(wins) / len(resolved)) * 100.0, 6) if resolved else 0.0,
            "paper_quote_fill_topology": _paper_quote_fill_topology(scored_pairs),
        },
        scored[-500:],
        len(resolutions),
    )


def _paper_quote_fill_topology(scored_pairs: list[tuple[dict[str, Any], dict[str, Any]]]) -> dict[str, Any]:
    by_window: dict[str, dict[str, Any]] = {}
    for order, scored in scored_pairs:
        if not isinstance(order.get("maker_quote"), dict):
            continue
        market_slug = str(order.get("market_slug") or "")
        if not market_slug:
            continue
        row = by_window.setdefault(
            market_slug,
            {
                "market_slug": market_slug,
                "window_start_s": num(order.get("maker_quote", {}).get("window_start_s")),
                "quoted_outcomes": set(),
                "filled_outcomes": set(),
                "resolved_fills": 0,
                "unresolved_fills": 0,
                "resolved_pnl_usd": 0.0,
            },
        )
        outcome = str(order.get("outcome") or "")
        if outcome:
            row["quoted_outcomes"].add(outcome)
        status = str(order.get("final_status") or order.get("status") or "").upper()
        if status == "FILLED" and outcome:
            row["filled_outcomes"].add(outcome)
            if scored.get("resolved"):
                row["resolved_fills"] += 1
                row["resolved_pnl_usd"] = round(float(row["resolved_pnl_usd"]) + num(scored.get("pnl_usd")), 6)
            else:
                row["unresolved_fills"] += 1

    topology_counts: Counter[str] = Counter()
    resolved_pnl_by_topology: Counter[str] = Counter()
    rows: list[dict[str, Any]] = []
    for row in by_window.values():
        filled_count = len(row["filled_outcomes"])
        if filled_count >= 2:
            topology = "both_sides_filled"
        elif filled_count == 1:
            topology = "one_sided_filled"
        else:
            topology = "none_filled"
        topology_counts[topology] += 1
        resolved_pnl_by_topology[topology] += num(row.get("resolved_pnl_usd"))
        rows.append(
            {
                "market_slug": row["market_slug"],
                "window_start_s": row["window_start_s"],
                "quoted_outcomes": sorted(row["quoted_outcomes"]),
                "filled_outcomes": sorted(row["filled_outcomes"]),
                "topology": topology,
                "resolved_fills": int(row["resolved_fills"]),
                "unresolved_fills": int(row["unresolved_fills"]),
                "resolved_pnl_usd": round(num(row.get("resolved_pnl_usd")), 6),
            }
        )
    rows.sort(key=lambda item: num(item.get("window_start_s")))
    return {
        "windows": len(rows),
        "topology_counts": dict(sorted(topology_counts.items())),
        "resolved_pnl_by_topology": {key: round(value, 6) for key, value in sorted(resolved_pnl_by_topology.items())},
        "rows": rows[-200:],
    }


def _build_signal_event(args: argparse.Namespace, prior_signal_ids: set[str], now_ts: float) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    start = _window_start(now_ts)
    offset_s = float(now_ts) - float(start)
    if offset_s < float(args.signal_start_s) or offset_s > float(args.signal_end_s):
        return None, {"status": "WAITING_SIGNAL_WINDOW", "window_start_s": start, "offset_s": round(offset_s, 6)}
    slug = f"btc-updown-5m-{start}"
    lane_id = _lane_id(args)
    signal_id = stable_id(
        "e7",
        {
            "market_slug": slug,
            "threshold_bps": round(float(args.threshold_bps), 6),
            "signal_start_s": round(float(args.signal_start_s), 6),
            "signal_end_s": round(float(args.signal_end_s), 6),
            **_variant_id_fields(args),
        },
    )
    klines = fetch_1m_klines(args.symbol, start, int(now_ts) + 60, float(args.timeout_s))
    spot = spot_signal_from_klines(klines, window_start_s=start, now_ts=now_ts, threshold_bps=float(args.threshold_bps))
    if spot.get("status") != "SIGNAL":
        return None, {"status": spot.get("status"), "spot": spot, "window_start_s": start, "offset_s": round(offset_s, 6)}
    if signal_id in prior_signal_ids:
        return None, {
            "status": "DUPLICATE_SIGNAL",
            "signal_id": signal_id,
            "spot": spot,
            "window_start_s": start,
            "offset_s": round(offset_s, 6),
        }
    market = _market_for_slug(slug, timeout_s=float(args.timeout_s))
    tokens = _token_map(market)
    clob = CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
    books = {
        outcome: _book_snapshot_with_direct_fallback(
            clob=clob,
            token_id=tokens.get(outcome, ""),
            order_usd=float(args.order_usd),
            max_entry_price=float(args.max_entry_price),
        )
        for outcome in ("Up", "Down")
    }
    predicted = str(spot.get("predicted_outcome") or "")
    entry = entry_from_books(
        predicted_outcome=predicted,
        books=books,
        order_usd=float(args.order_usd),
        max_entry_price=float(args.max_entry_price),
    )
    return (
        {
            "schema_version": 1,
            "event": "e7_spot_open_signal",
            "lane": lane_id,
            "flow_stage": "OBSERVE/LEARN/PROMOTE",
            "signal_id": signal_id,
            "generated_at": utc_now_iso(),
            "paper_only": True,
            "live_orders_allowed": False,
            "market_slug": slug,
            "window_start_s": start,
            "window_end_s": start + 300,
            "signal_offset_s": round(offset_s, 6),
            "order_usd": round(float(args.order_usd), 6),
            "max_entry_price": round(float(args.max_entry_price), 6),
            "spot_signal": spot,
            "predicted_outcome": predicted,
            "market": {
                "condition_id": str(market.get("conditionId") or market.get("condition_id") or ""),
                "tokens": tokens,
                "gamma_slug_found": bool(market),
                "gamma_fetch_error": market.get("_fetch_error") or "",
                "gamma_fetch_error_message": market.get("_fetch_error_message") or "",
                "gamma_fetch_recovered_from_error": market.get("_fetch_recovered_from_error") or {},
                "gamma_route_attempts": market.get("_gamma_route_attempts") or [],
                "gamma_route_used": market.get("_gamma_route_used") or "",
            },
            "books": books,
            "entry": entry,
            "zero_live_assertion": {"status": "PASS", "orders_submitted": 0},
        },
        {"status": "SIGNAL_EVALUATED", "signal_id": signal_id, "window_start_s": start, "offset_s": round(offset_s, 6)},
    )


def _build_state_unlocked(args: argparse.Namespace) -> dict[str, Any]:
    now_ts = float(args.now_ts or time.time())
    lane_id = _lane_id(args)
    prior = {} if bool(args.reset_state) else load_json(args.state, default={})
    prior = prior if isinstance(prior, dict) else {}
    prior_events = [row for row in prior.get("signal_events") or [] if isinstance(row, dict)]
    prior_signal_ids = {str(row.get("signal_id") or "") for row in prior_events if row.get("signal_id")}
    prior_orders = [row for row in prior.get("orders") or [] if isinstance(row, dict)]
    event, cycle_diag = _build_signal_event(args, prior_signal_ids, now_ts)
    prior_ask_samples = [row for row in prior.get("ask_samples") or [] if isinstance(row, dict)]
    prior_sample_ids = {str(row.get("sample_id") or "") for row in prior_ask_samples if row.get("sample_id")}
    ask_sample, ask_sample_diag = _build_ask_sample_event(args, prior_sample_ids, now_ts)
    prior_quote_events = [row for row in prior.get("paper_quote_events") or [] if isinstance(row, dict)]
    prior_quote_ids = {str(row.get("quote_id") or "") for row in prior_quote_events if row.get("quote_id")}
    new_paper_quote_events, paper_quote_diag = _build_paper_quote_events(args, prior_quote_ids, ask_sample)
    state_updated_at = utc_now_iso()
    prior_delta_windows = [row for row in prior.get("delta_windows") or [] if isinstance(row, dict)]
    delta_observation = _delta_observation_from_cycle(event, cycle_diag, observed_at=state_updated_at)
    delta_windows = merge_delta_window_records(prior_delta_windows, delta_observation)
    delta_dist = delta_window_distribution(delta_windows)
    if isinstance(event, dict):
        event = _annotate_signal_event(event)
    new_events = [event] if isinstance(event, dict) else []
    new_ask_samples = [ask_sample] if isinstance(ask_sample, dict) else []
    paper_quote_events = (prior_quote_events + new_paper_quote_events)[-50_000:]
    ask_samples = (prior_ask_samples + new_ask_samples)[-50_000:]
    new_orders = [order for row in new_events if (order := _order_from_event(row)) is not None]
    quote_orders = [
        order
        for quote in paper_quote_events
        if (order := _order_from_paper_quote(quote, ask_samples, now_ts=now_ts)) is not None
    ]
    signal_events = [_annotate_signal_event(dict(row)) for row in (prior_events + new_events)][-50_000:]
    orders = _merge_orders(prior_orders, new_orders + quote_orders)
    order_summary, scored_orders, resolution_rows_indexed = _summarize_orders(orders, args.resolutions)
    degraded_signal_count = gamma_degraded_signal_events(signal_events)
    no_ask_signal_count = no_ask_signal_events(signal_events)
    predicted_asks = [
        num(((row.get("books") or {}).get(row.get("predicted_outcome")) or {}).get("best_ask"))
        for row in signal_events
        if row.get("predicted_outcome")
    ]
    entry_rejects = Counter(
        str(((row.get("entry") if isinstance(row.get("entry"), dict) else {}) or {}).get("reject_reason") or "filled")
        for row in signal_events
        if row.get("predicted_outcome")
    )
    signal_count = len(signal_events)
    book_fills = order_summary["book_verified_fills"]
    gate_status = (
        "PASS"
        if signal_count >= 100 and book_fills >= 50 and order_summary["resolved_paper_pnl_usd"] > 0
        else "PENDING"
    )
    ask_dist = _distribution(predicted_asks)
    park_watch = "PARK_REVIEW" if ask_dist["count"] >= 30 and ask_dist["p50"] >= 0.93 else "OK"
    append_jsonl_many(args.event_log, new_events + new_ask_samples + new_paper_quote_events)
    state = {
        "schema_version": 1,
        "kind": "e7_spot_open_paper_lane_state",
        "lane": lane_id,
        "flow_stage": "OBSERVE/LEARN/PROMOTE",
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "updated_at": state_updated_at,
        "parameters": {
            "threshold_bps": float(args.threshold_bps),
            "order_usd": float(args.order_usd),
            "lane_id": lane_id,
            "max_entry_price": float(args.max_entry_price),
            "signal_window_s": [float(args.signal_start_s), float(args.signal_end_s)],
            "clob_base_url": args.clob_base_url,
            "paper_quote_offset_s": float(getattr(args, "paper_quote_offset_s", -1.0)),
            "paper_size_mode": _paper_size_mode(args),
            "maker_quote_tick_size": float(getattr(args, "maker_quote_tick_size", 0.01)),
            "maker_quote_cancel_before_close_s": float(getattr(args, "maker_quote_cancel_before_close_s", 30.0)),
        },
        "diagnostics": {
            "cycle": cycle_diag,
            "new_signal_events": len(new_events),
            "new_paper_quote_events": len(paper_quote_events) - len(prior_quote_events),
            "new_orders": len(new_orders) + len(quote_orders),
            "entry_rejects": dict(sorted(entry_rejects.items())),
            "delta_observation": delta_observation or {},
            "ask_sample": ask_sample_diag,
            "paper_quote": paper_quote_diag,
        },
        "summary": {
            "signal_events": signal_count,
            "gamma_degraded_signal_events": degraded_signal_count,
            "no_ask_signal_events": no_ask_signal_count,
            **order_summary,
            "ask_at_entry_distribution": ask_dist,
            "signal_offset_distribution": signal_offset_distribution(signal_events),
            "delta_window_distribution": delta_dist,
            "ask_sample_events": len(ask_samples),
            "ask_sample_offsets_s": _parse_offsets(str(args.ask_sample_offsets)),
            "ask_sample_summary": ask_sample_summary(ask_samples),
            "paper_quote_events": len(paper_quote_events),
            "park_watch": park_watch,
            "live_orders_allowed": False,
            "paper_only": True,
        },
        "promotion_gate": {
            "status": gate_status,
            "signal_events_required": 100,
            "book_verified_fills_required": 50,
            "requires_positive_canonical_pnl": True,
            "ask_distribution_required": True,
            "park_if_p50_ask_gte": 0.93,
            "park_watch": park_watch,
        },
        "zero_live_assertion": {"status": "PASS", "orders_submitted": 0},
        "delta_windows": delta_windows,
        "signal_events": signal_events,
        "ask_samples": ask_samples,
        "paper_quote_events": paper_quote_events,
        "orders": orders,
        "resolution_scoring": {
            "resolution_path": args.resolutions,
            "resolution_rows_indexed": resolution_rows_indexed,
            "scored_orders": scored_orders,
        },
    }
    atomic_write_json(args.state, state)
    return state


def build_state(args: argparse.Namespace) -> dict[str, Any]:
    with json_file_lock(args.state):
        return _build_state_unlocked(args)


def main() -> int:
    args = parse_args()
    state = build_state(args)
    print(
        json.dumps(
            {
                "lane": state.get("lane"),
                "updated_at": state.get("updated_at"),
                "cycle": state.get("diagnostics", {}).get("cycle"),
                "signal_events": state.get("summary", {}).get("signal_events"),
                "signal_quad": {
                    "signal_events": state.get("summary", {}).get("signal_events"),
                    "gamma_degraded_signal_events": state.get("summary", {}).get("gamma_degraded_signal_events"),
                    "no_ask_signal_events": state.get("summary", {}).get("no_ask_signal_events"),
                    "book_verified_fills": state.get("summary", {}).get("book_verified_fills"),
                },
                "book_verified_fills": state.get("summary", {}).get("book_verified_fills"),
                "ask_sample_summary": state.get("summary", {}).get("ask_sample_summary"),
                "delta_window_distribution": state.get("summary", {}).get("delta_window_distribution"),
                "resolved_paper_pnl_usd": state.get("summary", {}).get("resolved_paper_pnl_usd"),
                "paper_size_mode": state.get("parameters", {}).get("paper_size_mode"),
                "zero_live_assertion": state.get("zero_live_assertion"),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
