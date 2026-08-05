#!/usr/bin/env python3
"""Watch final-90s BTC 5m CLOB penny offers for E7 calibration.

Flow stage: DISCOVER/LEARN. This is a paper-only market-data watcher. It
captures the current BTC 5m window during the final 90 seconds, records both
outcome books, and emits opportunity rows whenever an ask is available at or
below the configured penny threshold. It never creates CopyIntents and never
touches live execution state.
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

from scripts.refresh_btc_5m_resolutions_from_history import fetch_1m_klines  # noqa: E402
from scripts.run_e7_spot_open_paper_lane import (  # noqa: E402
    CLOB_ROUTE_ENV_VARS,
    _market_for_slug,
    _token_map,
    _with_cleared_env,
)
from src.wallet_copy.fees import (  # noqa: E402
    POLYMARKET_EMBEDDED_FEE_FORMULA,
    POLYMARKET_EMBEDDED_FEE_RATE,
    POLYMARKET_EMBEDDED_FEE_SOURCE,
    expected_polymarket_buy_fee_usd,
)
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import num, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, json_file_lock, load_json  # noqa: E402


LANE_ID = "btc5m_late_window_penny_watcher"
DEFAULT_STATE = "data/research/btc5m_late_window_penny_watcher_state.json"
DEFAULT_EVENT_LOG = "data/research/btc5m_late_window_penny_watcher_events.jsonl"
DEFAULT_CLOB_BASE = "http://127.0.0.1:8787/clob"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--event-log", default=DEFAULT_EVENT_LOG)
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--max-penny-ask", type=float, default=0.02)
    parser.add_argument("--final-window-s", type=float, default=90.0)
    parser.add_argument("--order-usd", type=float, default=1.0)
    parser.add_argument("--timeout-s", type=float, default=5.0)
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--iterations", type=int, default=1, help="0 means run until --duration-s expires or interrupted.")
    parser.add_argument("--duration-s", type=float, default=0.0)
    parser.add_argument("--interval-s", type=float, default=2.0)
    parser.add_argument("--reset-state", action="store_true")
    parser.add_argument("--now-ts", type=float, default=0.0, help="test hook; defaults to current time")
    return parser.parse_args()


def _window_start(now_ts: float) -> int:
    return int(float(now_ts) // 300) * 300


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(float(ts), UTC).isoformat().replace("+00:00", "Z")


def _parse_ts(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        text = str(value).strip()
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    if not math.isfinite(numeric) or numeric <= 0:
        return None
    if numeric > 1_000_000_000_000_000:
        return numeric / 1_000_000_000.0
    if numeric > 1_000_000_000_000:
        return numeric / 1_000.0
    return numeric


def _book_levels(book: dict[str, Any], side: str) -> list[tuple[float, float]]:
    levels: list[tuple[float, float]] = []
    for row in book.get(side) or []:
        if not isinstance(row, dict):
            continue
        price = num(row.get("price"))
        size = num(row.get("size"))
        if price > 0.0 and size > 0.0:
            levels.append((price, size))
    reverse = side == "bids"
    return sorted(levels, key=lambda item: item[0], reverse=reverse)


def _penny_depth(book: dict[str, Any], *, max_ask: float) -> dict[str, Any]:
    asks = _book_levels(book, "asks")
    eligible = [(price, size) for price, size in asks if price <= float(max_ask) + 1e-12]
    shares = sum(size for _, size in eligible)
    cost = sum(price * size for price, size in eligible)
    weighted = cost / shares if shares > 0.0 else 0.0
    return {
        "max_ask": round(float(max_ask), 6),
        "penny_levels": len(eligible),
        "penny_depth_shares": round(shares, 6),
        "penny_depth_cost_usd": round(cost, 6),
        "weighted_penny_ask": round(weighted, 6),
        "best_penny_ask": round(eligible[0][0], 6) if eligible else 0.0,
        "best_penny_shares": round(eligible[0][1], 6) if eligible else 0.0,
    }


def _snapshot_from_book(
    *,
    token_id: str,
    book: dict[str, Any],
    captured_at_s: float,
    fetch_started_at_s: float,
    route_report: dict[str, Any],
    order_usd: float,
    max_penny_ask: float,
) -> dict[str, Any]:
    summary = CLOBMarketClient.summarize_book(
        book,
        copy_size_usd=float(order_usd),
        source_price=1.0,
        max_slippage_bps=0.0,
    )
    asks = _book_levels(book, "asks")
    bids = _book_levels(book, "bids")
    book_ts_s = _parse_ts(book.get("timestamp") or summary.get("book_timestamp"))
    latency_s = round(captured_at_s - book_ts_s, 6) if book_ts_s is not None else None
    depth = _penny_depth(book, max_ask=float(max_penny_ask))
    return {
        "status": "OK",
        "token_id": str(token_id),
        "captured_at_s": round(captured_at_s, 6),
        "captured_at_iso": _iso(captured_at_s),
        "fetch_duration_s": round(max(0.0, captured_at_s - fetch_started_at_s), 6),
        "book_timestamp_raw": book.get("timestamp") or summary.get("book_timestamp"),
        "book_timestamp_s": round(book_ts_s, 6) if book_ts_s is not None else None,
        "book_timestamp_iso": _iso(book_ts_s) if book_ts_s is not None else "",
        "detection_to_book_timestamp_latency_s": latency_s,
        "latency_basis": "clob_book_timestamp_vs_detection_response_time"
        if book_ts_s is not None
        else "book_timestamp_missing_no_event_age_fallback",
        "best_bid": round(bids[0][0], 6) if bids else 0.0,
        "best_ask": round(asks[0][0], 6) if asks else 0.0,
        "spread": round(asks[0][0] - bids[0][0], 6) if asks and bids else 0.0,
        "ask_levels": len(asks),
        "bid_levels": len(bids),
        "route_report": route_report,
        **{
            key: value
            for key, value in summary.items()
            if key
            not in {
                "best_bid",
                "best_ask",
                "spread",
                "book_timestamp",
            }
        },
        **depth,
    }


def _book_snapshot(
    *,
    clob: CLOBMarketClient,
    token_id: str,
    order_usd: float,
    max_penny_ask: float,
) -> dict[str, Any]:
    if not token_id:
        return {
            "status": "MISSING_TOKEN",
            "token_id": "",
            "blocking_reason": "missing_token",
            "latency_basis": "not_fetched_missing_token",
        }
    started = time.time()
    try:
        book = clob.get_book(token_id)
    except Exception as exc:  # noqa: BLE001 - evidence-only watcher must persist route failures.
        captured = time.time()
        return {
            "status": "ERROR",
            "token_id": str(token_id),
            "captured_at_s": round(captured, 6),
            "captured_at_iso": _iso(captured),
            "fetch_duration_s": round(max(0.0, captured - started), 6),
            "error_type": type(exc).__name__,
            "error": str(exc)[:300],
            "route_report": clob.last_route_report if isinstance(clob.last_route_report, dict) else {},
            "latency_basis": "book_fetch_error_no_event_age_fallback",
        }
    captured = time.time()
    return _snapshot_from_book(
        token_id=token_id,
        book=book,
        captured_at_s=captured,
        fetch_started_at_s=started,
        route_report=clob.last_route_report if isinstance(clob.last_route_report, dict) else {},
        order_usd=float(order_usd),
        max_penny_ask=float(max_penny_ask),
    )


def _book_snapshot_with_direct_fallback(
    *,
    clob: CLOBMarketClient,
    token_id: str,
    order_usd: float,
    max_penny_ask: float,
) -> dict[str, Any]:
    configured = _book_snapshot(
        clob=clob,
        token_id=token_id,
        order_usd=float(order_usd),
        max_penny_ask=float(max_penny_ask),
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
            order_usd=float(order_usd),
            max_penny_ask=float(max_penny_ask),
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


def spot_context_from_klines(
    klines: dict[int, list[Any]],
    *,
    window_start_s: int,
    now_ts: float,
) -> dict[str, Any]:
    open_row = klines.get(int(window_start_s))
    signal_minute = int(float(now_ts) // 60) * 60
    signal_row = None
    for minute in sorted(key for key in klines if key <= signal_minute):
        if minute >= int(window_start_s):
            signal_row = klines[minute]
    if open_row is None or signal_row is None:
        return {
            "status": "MISSING_KLINE",
            "source": "binance_1m_kline_current_close_existing_e7_proxy",
            "signal_minute_s": signal_minute,
            "outcome_probabilities": {"Up": 0.5, "Down": 0.5},
        }
    open_price = num(open_row[1] if len(open_row) > 1 else 0.0)
    signal_price = num(signal_row[4] if len(signal_row) > 4 else signal_row[1] if len(signal_row) > 1 else 0.0)
    if open_price <= 0.0 or signal_price <= 0.0:
        return {
            "status": "BAD_KLINE_PRICE",
            "source": "binance_1m_kline_current_close_existing_e7_proxy",
            "signal_minute_s": signal_minute,
            "window_open_price": round(open_price, 8),
            "signal_price": round(signal_price, 8),
            "outcome_probabilities": {"Up": 0.5, "Down": 0.5},
        }
    delta_bps = (signal_price - open_price) / open_price * 10_000.0
    if delta_bps > 0.0:
        probabilities = {"Up": 1.0, "Down": 0.0}
        instant_outcome = "Up"
    elif delta_bps < 0.0:
        probabilities = {"Up": 0.0, "Down": 1.0}
        instant_outcome = "Down"
    else:
        probabilities = {"Up": 0.5, "Down": 0.5}
        instant_outcome = "Tie"
    return {
        "status": "OK",
        "source": "binance_1m_kline_current_close_existing_e7_proxy",
        "probability_basis": "instantaneous_spot_vs_window_open_not_forecast",
        "window_open_price": round(open_price, 8),
        "signal_price": round(signal_price, 8),
        "signal_minute_s": signal_minute,
        "delta_bps": round(delta_bps, 6),
        "instant_outcome_if_expired_now": instant_outcome,
        "outcome_probabilities": probabilities,
    }


def _opportunity_from_snapshot(
    *,
    capture: dict[str, Any],
    outcome: str,
    token_id: str,
    book: dict[str, Any],
    spot: dict[str, Any],
    max_penny_ask: float,
) -> dict[str, Any] | None:
    best_ask = num(book.get("best_ask"))
    if str(book.get("status") or "") != "OK" or best_ask <= 0.0 or best_ask > float(max_penny_ask) + 1e-12:
        return None
    probabilities = spot.get("outcome_probabilities") if isinstance(spot.get("outcome_probabilities"), dict) else {}
    win_probability = max(0.0, min(1.0, num(probabilities.get(outcome), 0.5)))
    shares = num(book.get("penny_depth_shares"))
    cost = num(book.get("penny_depth_cost_usd"))
    price = num(book.get("weighted_penny_ask"), best_ask)
    expected_fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
    expected_payout = shares * win_probability
    fee_aware_ev = expected_payout - cost - expected_fee
    opportunity_id = stable_id(
        "penny",
        {
            "market_slug": capture.get("market_slug"),
            "outcome": outcome,
            "token_id": token_id,
            "captured_at_s": book.get("captured_at_s"),
            "book_timestamp_s": book.get("book_timestamp_s"),
            "best_ask": best_ask,
        },
    )
    return {
        "schema_version": 1,
        "event": "btc5m_late_window_penny_opportunity",
        "lane": LANE_ID,
        "flow_stage": "DISCOVER/LEARN",
        "opportunity_id": opportunity_id,
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "market_slug": str(capture.get("market_slug") or ""),
        "window_start_s": int(num(capture.get("window_start_s"))),
        "window_end_s": int(num(capture.get("window_end_s"))),
        "sample_offset_s": num(capture.get("sample_offset_s")),
        "time_remaining_s": num(capture.get("time_remaining_s")),
        "outcome": outcome,
        "token_id": str(token_id),
        "best_ask": round(best_ask, 6),
        "best_bid": round(num(book.get("best_bid")), 6),
        "penny_depth_shares": round(shares, 6),
        "penny_depth_cost_usd": round(cost, 6),
        "weighted_penny_ask": round(price, 6),
        "best_penny_ask": round(num(book.get("best_penny_ask")), 6),
        "best_penny_shares": round(num(book.get("best_penny_shares")), 6),
        "spot_implied_win_probability": round(win_probability, 6),
        "spot_probability_basis": str(spot.get("probability_basis") or ""),
        "spot_delta_bps": round(num(spot.get("delta_bps")), 6),
        "spot_instant_outcome_if_expired_now": str(spot.get("instant_outcome_if_expired_now") or ""),
        "expected_embedded_fee_usd": expected_fee,
        "expected_fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
        "expected_fee_formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
        "expected_fee_source": POLYMARKET_EMBEDDED_FEE_SOURCE,
        "spot_implied_fee_aware_ev_usd": round(fee_aware_ev, 6),
        "spot_implied_fee_aware_ev_per_share": round(fee_aware_ev / shares, 6) if shares > 0.0 else 0.0,
        "book_timestamp_s": book.get("book_timestamp_s"),
        "book_timestamp_iso": book.get("book_timestamp_iso") or "",
        "detected_at_s": book.get("captured_at_s"),
        "detected_at_iso": book.get("captured_at_iso") or "",
        "detection_to_book_timestamp_latency_s": book.get("detection_to_book_timestamp_latency_s"),
        "latency_basis": book.get("latency_basis"),
        "book_route_used": book.get("book_route_used"),
        "route_report": book.get("route_report") if isinstance(book.get("route_report"), dict) else {},
        "zero_live_assertion": {"status": "PASS", "orders_submitted": 0},
    }


def _window_records(prior: list[dict[str, Any]], capture: dict[str, Any] | None, opportunities: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_window: dict[int, dict[str, Any]] = {}
    for row in prior:
        if not isinstance(row, dict):
            continue
        start = int(num(row.get("window_start_s")))
        if start > 0:
            by_window[start] = dict(row)
    if isinstance(capture, dict):
        start = int(num(capture.get("window_start_s")))
        if start > 0:
            row = by_window.setdefault(
                start,
                {
                    "window_start_s": start,
                    "market_slug": str(capture.get("market_slug") or ""),
                    "first_captured_at": str(capture.get("generated_at") or ""),
                    "capture_samples": 0,
                    "penny_opportunities": 0,
                    "outcomes_with_penny": [],
                },
            )
            row["market_slug"] = str(capture.get("market_slug") or row.get("market_slug") or "")
            row["capture_samples"] = int(num(row.get("capture_samples"))) + 1
            row["last_captured_at"] = str(capture.get("generated_at") or "")
            row["last_sample_offset_s"] = num(capture.get("sample_offset_s"))
            row["min_sample_offset_s"] = min(num(row.get("min_sample_offset_s"), 999.0), num(capture.get("sample_offset_s")))
            row["max_sample_offset_s"] = max(num(row.get("max_sample_offset_s")), num(capture.get("sample_offset_s")))
            outcomes = set(str(item) for item in row.get("outcomes_with_penny") or [] if str(item))
            for opportunity in opportunities:
                if int(num(opportunity.get("window_start_s"))) != start:
                    continue
                outcomes.add(str(opportunity.get("outcome") or ""))
                row["penny_opportunities"] = int(num(row.get("penny_opportunities"))) + 1
            row["outcomes_with_penny"] = sorted(outcomes)
            by_window[start] = row
    return sorted(by_window.values(), key=lambda row: int(num(row.get("window_start_s"))))[-500:]


def _summary(
    *,
    capture_events: list[dict[str, Any]],
    penny_opportunities: list[dict[str, Any]],
    windows: list[dict[str, Any]],
    diagnostics: dict[str, Any],
) -> dict[str, Any]:
    observed_windows = [row for row in windows if int(num(row.get("capture_samples"))) > 0]
    windows_with_penny = [row for row in observed_windows if int(num(row.get("penny_opportunities"))) > 0]
    depth_usd = [num(row.get("penny_depth_cost_usd")) for row in penny_opportunities]
    latency_values = [
        num(row.get("detection_to_book_timestamp_latency_s"))
        for row in penny_opportunities
        if row.get("detection_to_book_timestamp_latency_s") is not None
    ]
    by_outcome: Counter[str] = Counter(str(row.get("outcome") or "") for row in penny_opportunities)
    ready_windows = len(observed_windows)
    return {
        "capture_events": len(capture_events),
        "observed_windows": ready_windows,
        "penny_opportunities": len(penny_opportunities),
        "windows_with_penny": len(windows_with_penny),
        "opportunity_frequency_pct": round(100.0 * len(windows_with_penny) / ready_windows, 6)
        if ready_windows
        else 0.0,
        "opportunities_by_outcome": dict(sorted(by_outcome.items())),
        "total_penny_depth_usd": round(sum(depth_usd), 6),
        "avg_penny_depth_usd": round(sum(depth_usd) / len(depth_usd), 6) if depth_usd else 0.0,
        "max_penny_depth_usd": round(max(depth_usd), 6) if depth_usd else 0.0,
        "avg_detection_to_book_latency_s": round(sum(latency_values) / len(latency_values), 6)
        if latency_values
        else None,
        "latest_cycle": diagnostics,
        "calibration_gate": {
            "required_observed_windows": 100,
            "current_observed_windows": ready_windows,
            "remaining_observed_windows": max(0, 100 - ready_windows),
            "ready_for_fable_packet": ready_windows >= 100,
        },
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _build_capture(args: argparse.Namespace, now_ts: float) -> tuple[dict[str, Any] | None, list[dict[str, Any]], dict[str, Any]]:
    start = _window_start(now_ts)
    offset_s = float(now_ts) - float(start)
    time_remaining_s = max(0.0, 300.0 - offset_s)
    if time_remaining_s > float(args.final_window_s) or offset_s >= 300.0:
        return None, [], {
            "status": "WAITING_FINAL_WINDOW",
            "window_start_s": start,
            "offset_s": round(offset_s, 6),
            "time_remaining_s": round(time_remaining_s, 6),
            "final_window_s": round(float(args.final_window_s), 6),
        }

    slug = f"btc-updown-5m-{start}"
    klines = fetch_1m_klines(args.symbol, start, int(now_ts) + 60, float(args.timeout_s))
    spot = spot_context_from_klines(klines, window_start_s=start, now_ts=now_ts)
    market = _market_for_slug(slug, timeout_s=float(args.timeout_s))
    tokens = _token_map(market)
    clob = CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
    books = {
        outcome: _book_snapshot_with_direct_fallback(
            clob=clob,
            token_id=tokens.get(outcome, ""),
            order_usd=float(args.order_usd),
            max_penny_ask=float(args.max_penny_ask),
        )
        for outcome in ("Up", "Down")
    }
    capture_id = stable_id(
        "pencap",
        {
            "market_slug": slug,
            "sample_epoch_s": int(now_ts),
            "max_penny_ask": round(float(args.max_penny_ask), 6),
        },
    )
    capture = {
        "schema_version": 1,
        "event": "btc5m_late_window_penny_capture",
        "lane": LANE_ID,
        "flow_stage": "DISCOVER/LEARN",
        "capture_id": capture_id,
        "generated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "market_slug": slug,
        "window_start_s": start,
        "window_end_s": start + 300,
        "sample_offset_s": round(offset_s, 6),
        "time_remaining_s": round(time_remaining_s, 6),
        "max_penny_ask": round(float(args.max_penny_ask), 6),
        "spot": spot,
        "market": {
            "condition_id": str(market.get("conditionId") or market.get("condition_id") or ""),
            "tokens": tokens,
            "gamma_slug_found": bool(market and not market.get("_fetch_error")),
            "gamma_fetch_error": market.get("_fetch_error") or "",
            "gamma_fetch_error_message": market.get("_fetch_error_message") or "",
            "gamma_fetch_recovered_from_error": market.get("_fetch_recovered_from_error") or {},
            "gamma_route_attempts": market.get("_gamma_route_attempts") or [],
            "gamma_route_used": market.get("_gamma_route_used") or "",
        },
        "books": books,
        "zero_live_assertion": {"status": "PASS", "orders_submitted": 0},
    }
    opportunities = [
        opportunity
        for outcome in ("Up", "Down")
        if (
            opportunity := _opportunity_from_snapshot(
                capture=capture,
                outcome=outcome,
                token_id=tokens.get(outcome, ""),
                book=books.get(outcome, {}),
                spot=spot,
                max_penny_ask=float(args.max_penny_ask),
            )
        )
        is not None
    ]
    return capture, opportunities, {
        "status": "CAPTURED",
        "window_start_s": start,
        "offset_s": round(offset_s, 6),
        "time_remaining_s": round(time_remaining_s, 6),
        "penny_opportunities": len(opportunities),
        "book_status_counts": dict(sorted(Counter(str(row.get("status") or "") for row in books.values()).items())),
        "spot_status": str(spot.get("status") or ""),
    }


def _build_state_unlocked(args: argparse.Namespace) -> dict[str, Any]:
    now_ts = float(args.now_ts or time.time())
    prior = {} if bool(args.reset_state) else load_json(args.state, default={})
    prior = prior if isinstance(prior, dict) else {}
    prior_captures = [row for row in prior.get("capture_events") or [] if isinstance(row, dict)]
    prior_opportunities = [row for row in prior.get("penny_opportunities") or [] if isinstance(row, dict)]
    prior_windows = [row for row in prior.get("windows") or [] if isinstance(row, dict)]
    capture, opportunities, diagnostics = _build_capture(args, now_ts)
    new_events = ([capture] if isinstance(capture, dict) else []) + opportunities
    if new_events:
        append_jsonl_many(args.event_log, new_events)
    capture_events = (prior_captures + ([capture] if isinstance(capture, dict) else []))[-50_000:]
    penny_opportunities = (prior_opportunities + opportunities)[-50_000:]
    windows = _window_records(prior_windows, capture, opportunities)
    updated_at = utc_now_iso()
    summary = _summary(
        capture_events=capture_events,
        penny_opportunities=penny_opportunities,
        windows=windows,
        diagnostics=diagnostics,
    )
    state = {
        "schema_version": 1,
        "kind": "btc5m_late_window_penny_watcher_state",
        "lane": LANE_ID,
        "flow_stage": "DISCOVER/LEARN",
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "updated_at": updated_at,
        "parameters": {
            "symbol": str(args.symbol),
            "max_penny_ask": round(float(args.max_penny_ask), 6),
            "final_window_s": round(float(args.final_window_s), 6),
            "order_usd": round(float(args.order_usd), 6),
            "clob_base_url": str(args.clob_base_url),
            "spot_source": "binance_1m_kline_current_close_existing_e7_proxy",
            "expected_fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
            "expected_fee_source": POLYMARKET_EMBEDDED_FEE_SOURCE,
        },
        "diagnostics": {"cycle": diagnostics, "new_events": len(new_events), "new_opportunities": len(opportunities)},
        "summary": summary,
        "calibration_gate": summary["calibration_gate"],
        "zero_live_assertion": {"status": "PASS", "orders_submitted": 0},
        "windows": windows,
        "capture_events": capture_events,
        "penny_opportunities": penny_opportunities,
    }
    atomic_write_json(args.state, state)
    return state


def build_state(args: argparse.Namespace) -> dict[str, Any]:
    with json_file_lock(args.state):
        return _build_state_unlocked(args)


def main() -> int:
    args = parse_args()
    deadline = time.time() + float(args.duration_s) if float(args.duration_s) > 0.0 else None
    remaining = int(args.iterations)
    latest_state: dict[str, Any] = {}
    while True:
        latest_state = build_state(args)
        print(
            json.dumps(
                {
                    "lane": latest_state.get("lane"),
                    "updated_at": latest_state.get("updated_at"),
                    "cycle": latest_state.get("diagnostics", {}).get("cycle"),
                    "summary": latest_state.get("summary"),
                    "zero_live_assertion": latest_state.get("zero_live_assertion"),
                },
                sort_keys=True,
            )
        )
        if remaining > 0:
            remaining -= 1
            if remaining == 0:
                break
        if deadline is not None and time.time() >= deadline:
            break
        if remaining == 0 and deadline is None:
            break
        time.sleep(max(0.1, float(args.interval_s)))
    return 0 if latest_state.get("summary") else 1


if __name__ == "__main__":
    raise SystemExit(main())
