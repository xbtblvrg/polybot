#!/usr/bin/env python3
"""Run the E5 maker-first BTC-5m paper lane on recent RTDS flow.

Flow stage: OBSERVE/LEARN. This lane is paper-only. It creates hypothetical
maker quotes from observed BTC-5m wallet BUY signals, then marks a quote filled
only when a later RTDS trade crosses the resting quote price. It never submits
live orders and never changes the live guard path.
"""

from __future__ import annotations

import argparse
import os
import json
import subprocess
import sys
import time
from collections import Counter, defaultdict
from contextlib import contextmanager
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_whale_consensus_paper_lane import (  # noqa: E402
    DEFAULT_RTDS_JSONL,
    WhaleConsensusFeedEvent,
    _feed_event_from_rtds,
    _iter_recent_jsonl,
)
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.fees import (  # noqa: E402
    POLYMARKET_EMBEDDED_FEE_FORMULA,
    POLYMARKET_EMBEDDED_FEE_SOURCE,
    expected_polymarket_buy_fee_usd,
)
from src.wallet_copy.models import CopyIntent, num, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.performance import load_resolutions, score_order  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402


DEFAULT_STATE = "data/research/maker_first_btc5m_paper_state.json"
DEFAULT_EVENT_LOG = "data/research/maker_first_btc5m_paper_events.jsonl"
DEFAULT_RESOLUTION_STATE = "data/research/maker_first_btc5m_resolution_state.json"
DEFAULT_BOOK_AWARE_STATE = "data/research/maker_first_btc5m_book_aware_state.json"
DEFAULT_ARBITRATION_STATE = "data/research/maker_first_btc5m_full_population_arbitration_latest.json"
DEFAULT_LIVE_INTENTS_STATE = "data/research/maker_first_btc5m_live_intents_latest.json"
DEFAULT_GAMMA_REFRESH_SUMMARY = "data/research/maker_first_btc5m_gamma_refresh_summary.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
DEFAULT_CLOB_BASE = "http://127.0.0.1:8787/clob"

LANE_ID = "e5_maker_first_btc5m_v1"
SIGNAL_GATED_LANE_ID = "e5_signal_gated_maker_btc5m_v1"
SIGNAL_GATED_COPY_MODEL = "signal_gated_maker_btc5m"
SIGNAL_GATED_INTENT_SOURCE = "E5_SIGNAL_GATED_MAKER"
NO_FALLBACK_RESOLVED_REQUIRED = 150
NO_FALLBACK_FAIL_RESOLVED = 300
NO_FALLBACK_MAKER_FILL_RATE_REQUIRED_PCT = 90.0
NO_FALLBACK_ENFORCEMENT_START = "2026-07-07T00:00:00Z"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS_JSONL)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--event-log", default=DEFAULT_EVENT_LOG)
    parser.add_argument("--resolution-state", default=DEFAULT_RESOLUTION_STATE)
    parser.add_argument("--book-aware-state", default=DEFAULT_BOOK_AWARE_STATE)
    parser.add_argument(
        "--arbitration-state",
        default="",
        help="Dedicated arbitration output; default is derived from --book-aware-state.",
    )
    parser.add_argument("--live-intents-state", default=DEFAULT_LIVE_INTENTS_STATE)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--scan-limit", type=int, default=50_000)
    parser.add_argument("--scan-max-bytes", type=int, default=192_000_000)
    parser.add_argument("--max-feed-events", type=int, default=8_000)
    parser.add_argument("--max-event-age-s", type=float, default=1_800.0)
    parser.add_argument("--quote-lookback-s", type=float, default=1_800.0)
    parser.add_argument("--max-quotes", type=int, default=24)
    parser.add_argument("--order-usd", type=float, default=1.0)
    parser.add_argument(
        "--fixed-shares",
        type=float,
        default=5.0,
        help="Share-native maker size. Fable 2026-07-24 ruled exact five shares after the immutable regrade.",
    )
    parser.add_argument("--max-order-usd", type=float, default=8.0)
    parser.add_argument("--max-price", type=float, default=0.50)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--quote-latency-s", type=float, default=0.25)
    parser.add_argument("--cancel-before-close-s", type=float, default=30.0)
    parser.add_argument("--lane-id", default=LANE_ID)
    parser.add_argument("--intent-source-wallet", default="E5_MAKER_FIRST")
    parser.add_argument("--window-offset-min-s", type=float, default=0.0)
    parser.add_argument("--window-offset-max-s", type=float, default=270.0)
    parser.add_argument("--outcomes", default="Up,Down")
    parser.add_argument("--skip-clob-book", action="store_true")
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument(
        "--allow-book-fallback-quotes",
        action="store_true",
        help="Permit direct-fallback/no-book quotes. Default enforces the E5 prospective no-fallback rule.",
    )
    parser.add_argument(
        "--signal-gated-maker",
        action="store_true",
        help="Build the Fable 2026-07-08 signal-gated maker variant from fresh member BUY signals only.",
    )
    parser.add_argument("--signal-gated-max-age-s", type=float, default=5.0)
    parser.add_argument(
        "--signal-gated-parallel-age-s",
        default="10,30",
        help="Comma-separated paper-only measurement gates written beside the canonical 5s signal-gated lane.",
    )
    parser.add_argument("--signal-gated-toxicity-denylist", default="configs/wallet_copy/toxicity_denylist.json")
    parser.add_argument("--gamma-refresh-summary", default=DEFAULT_GAMMA_REFRESH_SUMMARY)
    parser.add_argument("--gamma-refresh-max-windows", type=int, default=120)
    parser.add_argument("--gamma-refresh-timeout-s", type=float, default=1.5)
    parser.add_argument("--gamma-refresh-sleep-s", type=float, default=0.0)
    parser.add_argument("--gamma-refresh-max-wall-runtime-s", type=float, default=20.0)
    parser.add_argument("--gamma-refresh-child-timeout-s", type=float, default=35.0)
    parser.add_argument("--reset-state", action="store_true")
    return parser.parse_args()


def load_recent_events(
    path: str,
    *,
    scan_limit: int,
    scan_max_bytes: int,
    max_feed_events: int,
    max_event_age_s: float,
    now_ts: float,
) -> tuple[list[WhaleConsensusFeedEvent], dict[str, int]]:
    rows, diagnostics = _iter_recent_jsonl(path, limit=scan_limit, max_bytes=scan_max_bytes)
    counts = Counter(diagnostics)
    events: list[WhaleConsensusFeedEvent] = []
    seen: set[str] = set()
    for row in rows:
        event = _feed_event_from_rtds(row)
        if event is None:
            counts["not_rtds_btc5m_trade"] += 1
            continue
        age_s = max(0.0, float(now_ts) - float(event.observed_ts))
        if max_event_age_s > 0 and age_s > float(max_event_age_s):
            counts["stale_observed_event"] += 1
            continue
        if event.event_id in seen:
            counts["duplicate_event"] += 1
            continue
        seen.add(event.event_id)
        events.append(event)
        if len(events) >= max(1, int(max_feed_events)):
            break
    counts["accepted_feed_events"] = len(events)
    return sorted(events, key=lambda item: (item.observed_ts, item.event_ts, item.event_id)), dict(sorted(counts.items()))


def _quote_price(
    signal: WhaleConsensusFeedEvent,
    *,
    tick_size: float,
    max_price: float,
    top_of_book: dict[str, Any] | None = None,
) -> float:
    top_of_book = top_of_book if isinstance(top_of_book, dict) else {}
    best_bid = num(top_of_book.get("best_bid"))
    best_ask = num(top_of_book.get("best_ask"))
    ceiling = min(float(max_price), float(signal.price) + float(tick_size))
    if best_bid > 0:
        one_tick_inside = min(ceiling, best_bid + float(tick_size))
        if best_ask > 0 and one_tick_inside >= best_ask:
            one_tick_inside = max(best_bid, best_ask - float(tick_size))
        return round(min(float(max_price), max(0.01, one_tick_inside)), 6)
    one_tick_inside = max(0.01, float(signal.price) - float(tick_size))
    return round(min(ceiling, one_tick_inside), 6)


def _top_of_book_placeholder() -> dict[str, Any]:
    return {
        "status": "not_fetched_minimal_build",
        "best_bid": None,
        "best_ask": None,
        "spread": None,
        "route_report": {
            "status": "NOT_REQUESTED",
            "route_class": "PAPER_MINIMAL_BUILD",
            "reason": "E5 smoke runner records the field; live promotion must route through the single guard.",
        },
    }


@contextmanager
def _temporarily_clear_env(name: str):
    sentinel = object()
    prior = os.environ.get(name, sentinel)
    os.environ.pop(name, None)
    try:
        yield
    finally:
        if prior is sentinel:
            os.environ.pop(name, None)
        else:
            os.environ[name] = str(prior)


def _summarize_clob_book(
    *,
    clob: CLOBMarketClient,
    book: dict[str, Any],
    order_usd: float,
    source_price: float,
    route_report_extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = CLOBMarketClient.summarize_book(
        book,
        copy_size_usd=float(order_usd),
        source_price=float(source_price),
        max_slippage_bps=0.0,
    )
    route_report = clob.last_route_report if isinstance(clob.last_route_report, dict) else {}
    if route_report_extra:
        route_report = {**route_report, **route_report_extra}
    return {"status": "OK", **summary, "route_report": route_report}


def _top_of_book_for_signal(
    signal: WhaleConsensusFeedEvent,
    *,
    fetch_clob_book: bool,
    clob: CLOBMarketClient | None,
    order_usd: float,
) -> dict[str, Any]:
    if not fetch_clob_book:
        return _top_of_book_placeholder()
    if clob is None or not signal.token_id:
        row = _top_of_book_placeholder()
        row["status"] = "missing_clob_client_or_token"
        row["route_report"] = {"status": "NOT_REQUESTED", "route_class": "MISSING_TOKEN_OR_CLIENT"}
        return row
    try:
        book = clob.get_book(signal.token_id)
        return _summarize_clob_book(
            clob=clob,
            book=book,
            order_usd=float(order_usd),
            source_price=float(signal.price),
        )
    except Exception as exc:  # noqa: BLE001 - paper lane persists route failures as evidence.
        primary_route_report = clob.last_route_report if clob and isinstance(clob.last_route_report, dict) else {}
        try:
            direct = CLOBMarketClient(
                CLOBMarketClient.DIRECT_CLOB_HOST,
                timeout_s=float(getattr(clob, "timeout_s", 1.0) or 1.0),
                fallback_hosts=(),
                retries=int(getattr(clob, "retries", 1) or 1),
            )
            # The shared HTTP client honors POLYMARKET_CLOB_API_BASE_URL for
            # clob.polymarket.com. This final fallback must be truly direct;
            # the relay has already been tried and may be busy.
            with _temporarily_clear_env("POLYMARKET_CLOB_API_BASE_URL"):
                book = direct.get_book(signal.token_id)
            return _summarize_clob_book(
                clob=direct,
                book=book,
                order_usd=float(order_usd),
                source_price=float(signal.price),
                route_report_extra={
                    "fallback_source": "direct_clob_after_primary_failure",
                    "primary_error_type": type(exc).__name__,
                    "primary_error": str(exc)[:300],
                    "primary_route_report": primary_route_report,
                    "suppressed_env_var": "POLYMARKET_CLOB_API_BASE_URL",
                },
            )
        except Exception as fallback_exc:  # noqa: BLE001 - evidence-only paper route diagnostic.
            fallback_route_report = (
                direct.last_route_report
                if "direct" in locals() and isinstance(direct.last_route_report, dict)
                else {}
            )
            fallback_error_type = type(fallback_exc).__name__
            fallback_error = str(fallback_exc)[:300]
        return {
            "status": "ERROR",
            "best_bid": None,
            "best_ask": None,
            "spread": None,
            "error_type": type(exc).__name__,
            "error": str(exc)[:300],
            "fallback_error_type": fallback_error_type,
            "fallback_error": fallback_error,
            "route_report": {
                **primary_route_report,
                "fallback_source": "direct_clob_after_primary_failure",
                "fallback_status": "ERROR",
                "fallback_route_report": fallback_route_report,
                "suppressed_env_var": "POLYMARKET_CLOB_API_BASE_URL",
            },
        }


def _maker_vs_taker_counterfactual(
    signal: WhaleConsensusFeedEvent,
    *,
    order_usd: float,
    top_of_book: dict[str, Any] | None = None,
) -> dict[str, Any]:
    top_of_book = top_of_book if isinstance(top_of_book, dict) else {}
    if top_of_book.get("status") == "OK":
        return {
            "status": str(top_of_book.get("instant_fill_status") or "UNKNOWN"),
            "role": "equivalent_fak_taker_copy",
            "avg_fill_price": top_of_book.get("avg_fill_price"),
            "fillable_usd": top_of_book.get("fillable_usd"),
            "fill_ratio": top_of_book.get("fill_ratio"),
            "blocking_reason": top_of_book.get("blocking_reason"),
        }
    price = max(0.000001, float(signal.price))
    return {
        "status": "NOT_MEASURED_NO_BOOK",
        "role": "equivalent_fak_taker_copy",
        "assumed_taker_price": round(price, 6),
        "counterfactual_shares_at_source_price": round(float(order_usd) / price, 6),
        "reason": "top_of_book_not_fetched_in_minimal_build",
    }


def _price_bucket(price: float) -> str:
    if price < 0.25:
        return "00_00_25"
    if price < 0.50:
        return "01_25_50"
    if price < 0.70:
        return "02_50_70"
    return "03_70_100"


def _toxicity_direction(outcome: str, side: str = "") -> str:
    text = str(outcome or side or "").strip().upper()
    if text in {"UP", "YES"}:
        return "UP"
    if text in {"DOWN", "NO"}:
        return "DOWN"
    return ""


def _load_toxicity_deny_cells(config_path: str) -> tuple[set[tuple[str, str, str]], dict[str, Any]]:
    loaded = load_json(config_path, default={})
    if not isinstance(loaded, dict):
        return set(), {"enabled": False, "reason": "config_invalid", "path": config_path}
    raw_cells = loaded.get("cells") if isinstance(loaded.get("cells"), list) else []
    cells: set[tuple[str, str, str]] = set()
    for row in raw_cells:
        if not isinstance(row, dict):
            continue
        wallet = str(row.get("source_wallet") or "").lower()
        bucket = str(row.get("price_bucket") or "")
        direction = str(row.get("direction") or "").upper()
        if wallet and bucket:
            cells.add((wallet, bucket, direction))
    return cells, {
        "enabled": True,
        "path": config_path,
        "generated_at": loaded.get("generated_at"),
        "configured_cells": len(cells),
        "rule": "source_wallet_x_price_bucket_x_direction_cells_carry_over_from_live_guard",
    }


def _signal_hits_toxicity_cell(
    *,
    source_wallet: str,
    quote_price: float,
    outcome: str,
    cells: set[tuple[str, str, str]],
) -> tuple[bool, dict[str, Any]]:
    wallet = str(source_wallet or "").lower()
    bucket = _price_bucket(float(quote_price))
    direction = _toxicity_direction(outcome)
    cell = (wallet, bucket, direction)
    wildcard_cell = (wallet, bucket, "")
    hit = cell in cells or wildcard_cell in cells
    return hit, {
        "source_wallet": wallet,
        "price_bucket": bucket,
        "direction": direction,
        "limit_price": round(float(quote_price), 6),
        "reject_reason": "toxicity_protection" if hit else "",
    }


def _order_is_enforced_no_fallback(order: dict[str, Any]) -> bool:
    signal = order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}
    return bool(signal.get("enforced_no_fallback_book")) or str(signal.get("book_evidence_mode") or "") == "enforced_no_fallback"


def _fallback_reason(top_of_book: dict[str, Any]) -> str:
    route = top_of_book.get("route_report") if isinstance(top_of_book.get("route_report"), dict) else {}
    return str(route.get("fallback_source") or route.get("route_class") or "direct_fallback")


def _counter_safe_reason(value: Any) -> str:
    text = str(value or "unknown").strip().lower()
    safe = "".join(ch if ch.isalnum() else "_" for ch in text).strip("_")
    return safe or "unknown"


def _no_fallback_book_reject_reason(top_of_book: dict[str, Any] | None) -> str:
    if not isinstance(top_of_book, dict) or not top_of_book:
        return "missing_top_of_book"
    status = str(top_of_book.get("status") or "")
    if status != "OK":
        return f"top_of_book_{_counter_safe_reason(status or 'missing_status')}"
    if not str(top_of_book.get("book_hash") or ""):
        return "missing_book_hash"
    if _route_uses_direct_fallback(top_of_book):
        return f"direct_fallback_{_counter_safe_reason(_fallback_reason(top_of_book))}"
    return ""


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    ordered = sorted(float(value) for value in values)
    if len(ordered) == 1:
        return round(ordered[0], 6)
    rank = (len(ordered) - 1) * max(0.0, min(100.0, float(pct))) / 100.0
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    fraction = rank - lower
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * fraction, 6)


def _signal_age_summary(values: list[float]) -> dict[str, Any]:
    rounded = [round(max(0.0, float(value)), 6) for value in values]
    return {
        "count": len(rounded),
        "p50_s": _percentile(rounded, 50),
        "p90_s": _percentile(rounded, 90),
        "max_s": round(max(rounded), 6) if rounded else None,
    }


def _stale_signal_attribution(
    trigger: WhaleConsensusFeedEvent,
    *,
    now_ts: float,
    max_signal_age_s: float,
) -> dict[str, Any]:
    origin_to_observed_s = max(0.0, float(trigger.observed_ts) - float(trigger.event_ts))
    observed_to_lane_read_s = max(0.0, float(now_ts) - float(trigger.observed_ts))
    attribution_hint = (
        "capture_or_lane_polling_lag"
        if observed_to_lane_read_s >= float(max_signal_age_s)
        else "source_emit_to_feed_observation_lag"
    )
    return {
        "source_stream": "rtds_jsonl",
        "source_event_id": trigger.event_id,
        "source_wallet": trigger.source_wallet,
        "market_slug": trigger.market_slug,
        "outcome": trigger.outcome,
        "transaction_hash": trigger.transaction_hash,
        "source_event_ts": round(float(trigger.event_ts), 6),
        "feed_observed_ts": round(float(trigger.observed_ts), 6),
        "lane_read_ts": round(float(now_ts), 6),
        "max_signal_age_s": round(float(max_signal_age_s), 6),
        "source_signal_age_s": round(observed_to_lane_read_s, 6),
        "origin_to_observation_latency_s": round(origin_to_observed_s, 6),
        "feed_observed_to_lane_read_s": round(observed_to_lane_read_s, 6),
        "attribution_hint": attribution_hint,
    }


def _open_signal_groups(
    events: list[WhaleConsensusFeedEvent],
    *,
    quote_lookback_s: float,
    now_ts: float,
) -> list[WhaleConsensusFeedEvent]:
    latest: dict[tuple[str, str], WhaleConsensusFeedEvent] = {}
    for event in events:
        if event.side != "BUY":
            continue
        if quote_lookback_s > 0 and max(0.0, float(now_ts) - float(event.observed_ts)) > float(quote_lookback_s):
            continue
        window_end_s = float(event.window_start_s) + 300.0
        if float(event.observed_ts) >= window_end_s:
            continue
        key = (event.market_slug, event.outcome)
        current = latest.get(key)
        if current is None or (event.observed_ts, event.event_ts, event.event_id) > (
            current.observed_ts,
            current.event_ts,
            current.event_id,
        ):
            latest[key] = event
    return sorted(latest.values(), key=lambda item: (item.observed_ts, item.market_slug, item.outcome))


def maker_signal_to_intent(signal: dict[str, Any]) -> CopyIntent:
    order_usd = max(0.0, num(signal.get("order_usd"), 1.0))
    price = max(0.000001, num(signal.get("quote_price"), 0.5))
    fixed_shares = max(0.0, num(signal.get("size_shares")))
    shares = fixed_shares if fixed_shares > 0 else order_usd / price
    lane_id = str(signal.get("intent_wallet_name") or signal.get("lane") or LANE_ID)
    intent_source_wallet = str(signal.get("intent_source_wallet") or "E5_MAKER_FIRST")
    copy_model = str(signal.get("copy_model") or "maker_first_btc5m")
    metadata = {
        "copy_model": copy_model,
        "e5_maker_first_btc5m_v1": signal,
        "row_type": "e5_maker_quote_signal",
        "source_fingerprint": str(signal.get("quote_id") or ""),
        "live_candidate_member": False,
        "signal_gate": signal.get("signal_gate") if isinstance(signal.get("signal_gate"), dict) else {},
        "promotion_gate": {
            "resolved_paper_fills_required": NO_FALLBACK_RESOLVED_REQUIRED,
            "requires_positive_pnl": True,
            "maker_fill_rate_required_pct": NO_FALLBACK_MAKER_FILL_RATE_REQUIRED_PCT,
            "enforced_no_fallback_book_required": True,
            "gate": "promotion_150_prospective_no_fallback_positive",
        },
    }
    return CopyIntent(
        intent_id=stable_id("ci", {"e5_maker_quote_id": signal.get("quote_id")}),
        source_wallet=intent_source_wallet,
        wallet_name=lane_id,
        source_event_id=str(signal.get("quote_id") or ""),
        condition_id=str(signal.get("condition_id") or ""),
        market_slug=str(signal.get("market_slug") or ""),
        outcome=str(signal.get("outcome") or ""),
        side=str(signal.get("side") or ""),
        limit_price=round(price, 6),
        wallet_usdc_size=round(order_usd, 6),
        copy_size_usd=round(order_usd, 6),
        shares=round(shares, 6),
        observed_ts=num(signal.get("quote_ts")),
        strategy_family=lane_id,
        policy_id="e5_maker_quote_le_50_min_1_cap_8",
        sizing_policy_id=(
            str(signal.get("sizing_policy_id") or "fixed_shares_5")
            if fixed_shares > 0
            else f"fixed_usd_{str(round(order_usd, 6)).replace('.', 'p')}"
        ),
        mode="paper",
        action="BUY",
        order_type="PAPER_MAKER_QUOTE",
        token_id=str(signal.get("token_id") or ""),
        event_ts=num(signal.get("source_event_ts")) or None,
        api_latency_s=max(0.0, num(signal.get("quote_ts")) - num(signal.get("source_event_ts"))),
        live_orders_allowed=False,
        reason=(
            "E5 signal-gated maker BTC-5m paper quote"
            if copy_model == SIGNAL_GATED_COPY_MODEL
            else "E5 maker-first BTC-5m paper quote"
        ),
        metadata=metadata,
    )


def build_quote_signals(
    events: list[WhaleConsensusFeedEvent],
    *,
    prior_quote_ids: set[str],
    now_ts: float,
    quote_lookback_s: float,
    max_quotes: int,
    order_usd: float,
    max_order_usd: float,
    max_price: float,
    tick_size: float,
    quote_latency_s: float,
    cancel_before_close_s: float,
    fetch_clob_book: bool = False,
    clob: CLOBMarketClient | None = None,
    enforce_no_fallback_book: bool = False,
    lane_id: str = LANE_ID,
    copy_model: str = "maker_first_btc5m",
    intent_source_wallet: str = "E5_MAKER_FIRST",
    signal_gated_max_age_s: float | None = None,
    toxicity_deny_cells: set[tuple[str, str, str]] | None = None,
    fixed_shares: float | None = None,
    window_offset_min_s: float = 0.0,
    window_offset_max_s: float = 270.0,
    outcomes: set[str] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    diagnostics: Counter[str] = Counter()
    signals: list[dict[str, Any]] = []
    gate_enabled = signal_gated_max_age_s is not None and float(signal_gated_max_age_s) > 0.0
    toxicity_deny_cells = toxicity_deny_cells or set()
    signal_age_values: list[float] = []
    stale_signal_attribution_samples: list[dict[str, Any]] = []
    for trigger in _open_signal_groups(events, quote_lookback_s=quote_lookback_s, now_ts=now_ts):
        source_signal_age_s = max(0.0, float(now_ts) - float(trigger.observed_ts))
        if gate_enabled:
            diagnostics["signal_gated_candidates"] += 1
            signal_age_values.append(source_signal_age_s)
            if source_signal_age_s > float(signal_gated_max_age_s or 0.0):
                diagnostics["signal_gated_stale_signal"] += 1
                if len(stale_signal_attribution_samples) < 25:
                    stale_signal_attribution_samples.append(
                        _stale_signal_attribution(
                            trigger,
                            now_ts=now_ts,
                            max_signal_age_s=float(signal_gated_max_age_s or 0.0),
                        )
                    )
                continue
        quote_ts = float(trigger.observed_ts) + max(0.0, float(quote_latency_s))
        window_end_s = float(trigger.window_start_s) + 300.0
        window_offset_s = quote_ts - float(trigger.window_start_s)
        if window_offset_s < float(window_offset_min_s) or window_offset_s >= float(window_offset_max_s):
            diagnostics["window_offset_outside_selected_cell"] += 1
            continue
        if outcomes is not None and trigger.outcome not in outcomes:
            diagnostics["outcome_outside_selected_cell"] += 1
            continue
        if quote_ts >= window_end_s - float(cancel_before_close_s):
            diagnostics["signal_inside_cancel_zone"] += 1
            continue
        if float(trigger.price) > float(max_price):
            diagnostics["source_price_above_max"] += 1
            continue
        size_usd = min(max(float(order_usd), 1.0), max(1.0, float(max_order_usd)))
        top_of_book = _top_of_book_for_signal(
            trigger,
            fetch_clob_book=bool(fetch_clob_book),
            clob=clob,
            order_usd=size_usd,
        )
        if enforce_no_fallback_book:
            reject_reason = _no_fallback_book_reject_reason(top_of_book)
            if reject_reason:
                diagnostics[f"no_fallback_skip_{reject_reason}"] += 1
                continue
        quote_price = _quote_price(trigger, tick_size=tick_size, max_price=max_price, top_of_book=top_of_book)
        if quote_price > float(max_price):
            diagnostics["quote_above_max_price"] += 1
            continue
        toxicity_hit, toxicity_row = _signal_hits_toxicity_cell(
            source_wallet=trigger.source_wallet,
            quote_price=quote_price,
            outcome=trigger.outcome,
            cells=toxicity_deny_cells,
        )
        if toxicity_hit:
            diagnostics["signal_gated_toxicity_cell_skip"] += 1
            diagnostics[f"signal_gated_toxicity_cell_skip_{toxicity_row['price_bucket']}"] += 1
            continue
        maker_size_shares = float(fixed_shares or 0.0)
        if maker_size_shares > 0:
            size_usd = round(maker_size_shares * quote_price, 6)
        quote_id = stable_id(
            "e5q",
            {
                "market_slug": trigger.market_slug,
                "outcome": trigger.outcome,
                "source_event_id": trigger.event_id,
                "quote_price": quote_price,
                "quote_ts": round(quote_ts, 6),
                "lane": lane_id,
            },
        )
        if quote_id in prior_quote_ids:
            diagnostics["duplicate_quote"] += 1
            continue
        signals.append(
            {
                "schema_version": 1,
                "event": "e5_maker_quote_signal",
                "flow_stage": "OBSERVE",
                "copy_model": copy_model,
                "lane": lane_id,
                "intent_wallet_name": lane_id,
                "intent_source_wallet": intent_source_wallet,
                "quote_id": quote_id,
                "generated_at": utc_now_iso(),
                "market_slug": trigger.market_slug,
                "condition_id": trigger.condition_id,
                "outcome": trigger.outcome,
                "side": "YES" if trigger.outcome == "Up" else "NO",
                "quote_price": quote_price,
                "quote_ts": round(quote_ts, 6),
                "window_start_s": trigger.window_start_s,
                "window_end_s": window_end_s,
                "cancel_before_close_s": float(cancel_before_close_s),
                "order_usd": round(size_usd, 6),
                "size_shares": round(maker_size_shares, 6) if maker_size_shares > 0 else None,
                "sizing_policy_id": "fixed_shares_5" if abs(maker_size_shares - 5.0) <= 1e-9 else "",
                "tick_size": round(float(tick_size), 6),
                "max_price": round(float(max_price), 6),
                "source_event_id": trigger.event_id,
                "source_wallet": trigger.source_wallet,
                "source_event_ts": trigger.event_ts,
                "source_observed_ts": trigger.observed_ts,
                "source_signal_age_s": round(source_signal_age_s, 6),
                "source_price": round(float(trigger.price), 6),
                "source_size": round(float(trigger.size), 6),
                "source_usd": round(float(trigger.source_usd), 6),
                "token_id": trigger.token_id,
                "transaction_hash": trigger.transaction_hash,
                "signal_gate": {
                    "enabled": bool(gate_enabled),
                    "max_signal_age_s": float(signal_gated_max_age_s or 0.0) if gate_enabled else None,
                    "source_signal_age_s": round(source_signal_age_s, 6),
                    "fresh_signal": bool(not gate_enabled or source_signal_age_s <= float(signal_gated_max_age_s or 0.0)),
                    "drift_direction": _toxicity_direction(trigger.outcome),
                    "toxicity_protection": {**toxicity_row, "hit": False},
                    "late_window_rule": "quote_ts < window_end_s - cancel_before_close_s",
                },
                "enforced_no_fallback_book": bool(enforce_no_fallback_book),
                "book_evidence_mode": "enforced_no_fallback" if enforce_no_fallback_book else "observed",
                "book_evidence_rule": (
                    "top_of_book.status == OK and book_hash present and route is not direct CLOB fallback"
                    if enforce_no_fallback_book
                    else "observed book evidence; fallback allowed for measurement"
                ),
                "top_of_book": top_of_book,
                "maker_vs_taker_counterfactual": _maker_vs_taker_counterfactual(
                    trigger,
                    order_usd=size_usd,
                    top_of_book=top_of_book,
                ),
            }
        )
        if max_quotes and len(signals) >= int(max_quotes):
            break
    if gate_enabled:
        diagnostics["signal_gated_age_count"] = len(signal_age_values)
        diagnostics["signal_gated_stale_attribution_sample_count"] = len(stale_signal_attribution_samples)
        diagnostics["signal_gated_stale_attribution_samples"] = stale_signal_attribution_samples
        for key, value in _signal_age_summary(signal_age_values).items():
            diagnostics[f"signal_gated_age_{key}"] = value
    diagnostics["signals"] = len(signals)
    return signals, dict(sorted(diagnostics.items()))


def _crossing_fill(signal: dict[str, Any], events: list[WhaleConsensusFeedEvent]) -> dict[str, Any] | None:
    quote_ts = num(signal.get("quote_ts"))
    quote_price = num(signal.get("quote_price"))
    token_id = str(signal.get("token_id") or "")
    requested_shares = num(signal.get("order_usd")) / max(quote_price, 0.000001)
    for event in events:
        if event.market_slug != signal.get("market_slug"):
            continue
        if event.outcome != signal.get("outcome"):
            continue
        if token_id and event.token_id != token_id:
            continue
        if float(event.observed_ts) <= quote_ts:
            continue
        if event.side == "SELL" and float(event.price) <= quote_price:
            if float(event.size) + 1e-9 < requested_shares:
                continue
            return {
                "fill_event_id": event.event_id,
                "fill_observed_ts": event.observed_ts,
                "fill_event_ts": event.event_ts,
                "fill_price": round(float(event.price), 6),
                "fill_size": round(float(event.size), 6),
                "requested_shares": round(requested_shares, 6),
                "transaction_hash": event.transaction_hash,
                "source_wallet": event.source_wallet,
                "crossing_rule": "later_sell_trade_price_lte_quote",
            }
    return None


def _order_from_signal(signal: dict[str, Any], events: list[WhaleConsensusFeedEvent], *, now_ts: float) -> dict[str, Any]:
    intent = maker_signal_to_intent(signal)
    fill = _crossing_fill(signal, events)
    window_end_s = num(signal.get("window_end_s"))
    cancel_ts = max(0.0, window_end_s - num(signal.get("cancel_before_close_s"), 30.0))
    if fill:
        final_status = "FILLED"
        filled_size_usd = num(signal.get("order_usd"))
        filled_shares = filled_size_usd / max(num(signal.get("quote_price")), 0.000001)
        lifecycle_message = "paper maker quote crossed by later RTDS sell"
    elif now_ts >= cancel_ts:
        final_status = "CANCELLED"
        filled_size_usd = 0.0
        filled_shares = 0.0
        lifecycle_message = "paper maker quote cancelled before close"
    else:
        final_status = "OPEN"
        filled_size_usd = 0.0
        filled_shares = 0.0
        lifecycle_message = "paper maker quote resting"
    order_id = stable_id("po", {"e5_quote_id": signal.get("quote_id")})
    return {
        "order_id": order_id,
        "intent_id": intent.intent_id,
        "source_wallet": intent.source_wallet,
        "wallet_name": intent.wallet_name,
        "condition_id": intent.condition_id,
        "market_slug": intent.market_slug,
        "outcome": intent.outcome,
        "side": intent.side,
        "limit_price": intent.limit_price,
        "requested_size_usd": intent.copy_size_usd,
        "requested_shares": intent.shares,
        "filled_size_usd": round(filled_size_usd, 6),
        "filled_shares": round(filled_shares, 6),
        "status": final_status,
        "final_status": final_status,
        "submitted_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "fill_model": "e5_maker_first_crossing_rtds_v1",
        "maker_quote": signal,
        "maker_fill_evidence": fill,
        "source_intent": intent.asdict(),
        "lifecycle": [
            {
                "ts": utc_now_iso(),
                "status": final_status,
                "message": lifecycle_message,
                "payload": {"quote_id": signal.get("quote_id"), "fill": fill},
            }
        ],
    }


def _merge_orders(prior_orders: list[dict[str, Any]], new_orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_id = {str(order.get("order_id") or ""): dict(order) for order in prior_orders if order.get("order_id")}
    for order in new_orders:
        by_id[str(order.get("order_id") or "")] = dict(order)
    return list(by_id.values())[-50_000:]


def _refresh_open_orders(
    prior_orders: list[dict[str, Any]],
    events: list[WhaleConsensusFeedEvent],
    *,
    now_ts: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    refreshed: list[dict[str, Any]] = []
    changed: list[dict[str, Any]] = []
    for order in prior_orders:
        row = dict(order)
        if str(row.get("final_status") or "").upper() != "OPEN":
            refreshed.append(row)
            continue
        signal = row.get("maker_quote") if isinstance(row.get("maker_quote"), dict) else {}
        fill = _crossing_fill(signal, events)
        window_end_s = num(signal.get("window_end_s"))
        cancel_ts = max(0.0, window_end_s - num(signal.get("cancel_before_close_s"), 30.0))
        final_status = ""
        message = ""
        if fill:
            final_status = "FILLED"
            row["filled_size_usd"] = round(num(signal.get("order_usd")), 6)
            row["filled_shares"] = round(
                num(signal.get("order_usd")) / max(num(signal.get("quote_price")), 0.000001),
                6,
            )
            row["maker_fill_evidence"] = fill
            message = "open paper maker quote crossed by later RTDS sell"
        elif now_ts >= cancel_ts:
            final_status = "CANCELLED"
            row["filled_size_usd"] = 0.0
            row["filled_shares"] = 0.0
            message = "open paper maker quote cancelled before close"
        if final_status:
            row["status"] = final_status
            row["final_status"] = final_status
            row["updated_at"] = utc_now_iso()
            lifecycle = list(row.get("lifecycle") or [])
            lifecycle.append(
                {
                    "ts": utc_now_iso(),
                    "status": final_status,
                    "message": message,
                    "payload": {"quote_id": signal.get("quote_id"), "fill": fill},
                }
            )
            row["lifecycle"] = lifecycle
            changed.append(row)
        refreshed.append(row)
    return refreshed, changed


def _score_summary(orders: list[dict[str, Any]], resolutions_path: str) -> tuple[dict[str, Any], list[dict[str, Any]], int]:
    resolutions = load_resolutions(resolutions_path)
    filled = [order for order in orders if str(order.get("final_status") or "").upper() == "FILLED"]
    scored = [score_order(order, resolutions) for order in filled]
    summary = _summarize_scored_orders(orders, filled, scored)
    # The full scored population is required by the book-aware/no-fallback
    # admission authority.  Persistence is bounded at the state-write sites;
    # truncating here silently limited the authoritative resolver to 500 fills.
    return summary, scored, len(resolutions)


def _parse_age_gates(value: Any) -> list[float]:
    gates: list[float] = []
    for raw in str(value or "").split(","):
        raw = raw.strip()
        if not raw:
            continue
        try:
            gate = float(raw)
        except ValueError:
            continue
        if gate <= 0:
            continue
        if gate not in gates:
            gates.append(gate)
    return gates


def _age_label(age_s: float) -> str:
    value = float(age_s)
    return f"{int(value)}s" if value.is_integer() else f"{str(value).replace('.', 'p')}s"


def _parallel_path(path: str, age_s: float, suffix: str) -> str:
    parsed = Path(path)
    label = _age_label(age_s)
    stem = parsed.stem
    if stem.endswith("_state"):
        stem = stem[: -len("_state")]
    if stem.endswith("_events"):
        stem = stem[: -len("_events")]
    return str(parsed.with_name(f"{stem}_{label}_{suffix}{parsed.suffix}"))


def _build_signal_gated_measurement_variant(
    *,
    args: argparse.Namespace,
    events: list[WhaleConsensusFeedEvent],
    now_ts: float,
    clob: CLOBMarketClient,
    toxicity_cells: set[tuple[str, str, str]],
    age_s: float,
) -> dict[str, Any]:
    label = _age_label(age_s)
    state_path = _parallel_path(args.state, age_s, "paper_state")
    event_log_path = _parallel_path(args.event_log, age_s, "paper_events")
    book_aware_path = _parallel_path(args.book_aware_state, age_s, "book_aware_state")
    lane_id = f"{SIGNAL_GATED_LANE_ID}_{label}"
    prior = {} if bool(getattr(args, "reset_state", False)) else load_json(state_path, default={})
    prior = prior if isinstance(prior, dict) else {}
    prior_orders = [row for row in prior.get("orders") or [] if isinstance(row, dict)]
    prior_quote_ids = {
        str(((order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}) or {}).get("quote_id") or "")
        for order in prior_orders
    }
    prior_quote_ids.discard("")
    signals, signal_diagnostics = build_quote_signals(
        events,
        prior_quote_ids=prior_quote_ids,
        now_ts=now_ts,
        quote_lookback_s=float(args.quote_lookback_s),
        max_quotes=int(args.max_quotes),
        order_usd=float(args.order_usd),
        max_order_usd=float(args.max_order_usd),
        max_price=float(args.max_price),
        tick_size=float(args.tick_size),
        quote_latency_s=float(args.quote_latency_s),
        cancel_before_close_s=float(args.cancel_before_close_s),
        fetch_clob_book=not bool(args.skip_clob_book),
        clob=clob,
        enforce_no_fallback_book=not bool(args.allow_book_fallback_quotes),
        lane_id=lane_id,
        copy_model=f"{SIGNAL_GATED_COPY_MODEL}_{label}",
        intent_source_wallet=SIGNAL_GATED_INTENT_SOURCE,
        signal_gated_max_age_s=float(age_s),
        toxicity_deny_cells=toxicity_cells,
        fixed_shares=float(getattr(args, "fixed_shares", 5.0) or 0.0),
    )
    refreshed_orders, updated_orders = _refresh_open_orders(prior_orders, events, now_ts=now_ts)
    new_orders = [_order_from_signal(signal, events, now_ts=now_ts) for signal in signals]
    orders = _merge_orders(refreshed_orders, new_orders)
    scoring_summary, scored_orders, resolution_rows_indexed = _score_summary(orders, args.resolutions)
    copyintent_parity_violations = sum(
        1
        for order in orders
        if not isinstance(order.get("source_intent"), dict)
        or bool((order.get("source_intent") or {}).get("live_orders_allowed"))
    )
    append_jsonl_many(event_log_path, updated_orders + new_orders)
    book_aware_state = _build_book_aware_state(
        orders=orders,
        scored_orders=scored_orders,
        resolutions_path=args.resolutions,
        event_log=event_log_path,
        gamma_refresh={"status": "SHARED_CANONICAL_RUN", "source": str(args.gamma_refresh_summary)},
        copyintent_parity_violations=copyintent_parity_violations,
        lane_id=lane_id,
        signal_gated_maker=True,
    )
    state = {
        "schema_version": 1,
        "kind": "signal_gated_maker_btc5m_parallel_measurement_state",
        "lane": lane_id,
        "flow_stage": "OBSERVE/LEARN",
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "variant": "signal_gated_maker_parallel_measurement",
        "canonical_lane": SIGNAL_GATED_LANE_ID,
        "promotion_bar_applies": False,
        "measurement_only": True,
        "updated_at": utc_now_iso(),
        "parameters": {
            "signal_gated_max_age_s": float(age_s),
            "canonical_signal_gated_max_age_s": float(getattr(args, "signal_gated_max_age_s", 5.0) or 0.0),
            "order_usd": float(args.order_usd),
            "max_price": float(args.max_price),
            "enforce_no_fallback_book": not bool(args.allow_book_fallback_quotes),
        },
        "diagnostics": {"signals": signal_diagnostics, "new_orders": len(new_orders), "updated_orders": len(updated_orders)},
        "current_signals": signals,
        "current_intents": [maker_signal_to_intent(signal).asdict() for signal in signals],
        "orders": orders,
        "summary": {
            **scoring_summary,
            "signals": len(signals),
            "new_orders": len(new_orders),
            "paper_only": True,
            "live_orders_allowed": False,
            "signal_gated_max_age_s": float(age_s),
            "promotion_bar_applies": False,
        },
        "resolution_scoring": {
            "kind": "e5_signal_gated_parallel_resolution_scoring_v1",
            "updated_at": utc_now_iso(),
            "resolution_path": str(args.resolutions),
            "resolution_rows_indexed": resolution_rows_indexed,
            "scored_orders": scored_orders[-500:],
        },
        "book_aware_state_path": book_aware_path,
        "book_aware_summary": book_aware_state["summary"],
    }
    atomic_write_json(state_path, state)
    atomic_write_json(book_aware_path, book_aware_state)
    return {
        "age_s": float(age_s),
        "label": label,
        "state_path": state_path,
        "event_log": event_log_path,
        "book_aware_state": book_aware_path,
        "summary": state["summary"],
        "diagnostics": state["diagnostics"],
    }


def _summarize_scored_orders(
    orders: list[dict[str, Any]],
    filled: list[dict[str, Any]],
    scored: list[dict[str, Any]],
) -> dict[str, Any]:
    resolved = [row for row in scored if row.get("resolved")]
    wins = [row for row in resolved if row.get("win")]
    cost = sum(num(row.get("cost_usd")) for row in resolved)
    payout = sum(num(row.get("payout_usd")) for row in resolved)
    pnl = sum(num(row.get("pnl_usd")) for row in resolved)
    cancelled_orders = sum(1 for order in orders if str(order.get("final_status") or "").upper() == "CANCELLED")
    open_orders = sum(1 for order in orders if str(order.get("final_status") or "").upper() == "OPEN")
    terminal_orders = len(filled) + cancelled_orders
    raw_fill_rate = round((len(filled) / len(orders)) * 100.0, 6) if orders else 0.0
    terminal_fill_rate = round((len(filled) / terminal_orders) * 100.0, 6) if terminal_orders else 0.0
    return {
        "paper_quotes": len(orders),
        "filled_orders": len(filled),
        "open_orders": open_orders,
        "cancelled_orders": cancelled_orders,
        "terminal_orders": terminal_orders,
        "raw_maker_fill_rate_pct": raw_fill_rate,
        "maker_fill_rate_pct": raw_fill_rate,
        "terminal_maker_fill_rate_pct": terminal_fill_rate,
        "resolved_paper_fills": len(resolved),
        "unresolved_paper_fills": len(filled) - len(resolved),
        "resolved_paper_wins": len(wins),
        "resolved_paper_losses": len(resolved) - len(wins),
        "resolved_paper_cost_usd": round(cost, 6),
        "resolved_paper_payout_usd": round(payout, 6),
        "resolved_paper_pnl_usd": round(pnl, 6),
        "resolved_paper_roi_pct": round((pnl / cost) * 100.0, 6) if cost > 0 else 0.0,
        "resolved_paper_wr_pct": round((len(wins) / len(resolved)) * 100.0, 6) if resolved else 0.0,
    }


def _book_evidence(order: dict[str, Any]) -> dict[str, Any] | None:
    signal = order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}
    top = signal.get("top_of_book") if isinstance(signal.get("top_of_book"), dict) else {}
    if top.get("status") != "OK" or not str(top.get("book_hash") or ""):
        return None
    return top


def _route_uses_direct_fallback(top_of_book: dict[str, Any]) -> bool:
    route = top_of_book.get("route_report") if isinstance(top_of_book.get("route_report"), dict) else {}
    return str(route.get("fallback_source") or "") == "direct_clob_after_primary_failure"


def _prospective_resolved_fill_ledger(
    *,
    current_scored_orders: list[dict[str, Any]],
    prior_state: dict[str, Any] | None,
    source_file: str,
    lane_id: str,
) -> dict[str, Any]:
    prior_state = prior_state if isinstance(prior_state, dict) else {}
    prior_ledger = (
        prior_state.get("prospective_no_fallback_resolved_fill_ledger")
        if isinstance(prior_state.get("prospective_no_fallback_resolved_fill_ledger"), dict)
        else {}
    )
    entries_by_id: dict[str, dict[str, Any]] = {}
    for row in prior_ledger.get("entries") or []:
        if not isinstance(row, dict):
            continue
        fill_id = str(row.get("fill_id") or row.get("order_id") or "")
        if fill_id:
            entries_by_id[fill_id] = dict(row)

    current_ids: set[str] = set()
    for row in current_scored_orders:
        if not isinstance(row, dict) or not bool(row.get("resolved")):
            continue
        fill_id = str(row.get("order_id") or row.get("intent_id") or "")
        if not fill_id:
            continue
        current_ids.add(fill_id)
        entries_by_id[fill_id] = {
            "fill_id": fill_id,
            "order_id": str(row.get("order_id") or ""),
            "market_slug": str(row.get("market_slug") or ""),
            "outcome": str(row.get("outcome") or ""),
            "cost_usd": round(num(row.get("cost_usd")), 6),
            "payout_usd": round(num(row.get("payout_usd")), 6),
            "pnl_usd": round(num(row.get("pnl_usd")), 6),
            "win": bool(row.get("win")),
            "source_file": source_file,
            "lane": lane_id,
        }

    prior_count = int(prior_ledger.get("distinct_resolved_fill_ids") or len(prior_ledger.get("entries") or []))
    current_source_count = len(current_ids)
    entries = sorted(entries_by_id.values(), key=lambda row: str(row.get("fill_id") or ""))
    distinct_count = len(entries)
    monotonic_decrease_detected = bool(prior_count and distinct_count < prior_count)
    return {
        "schema_version": 1,
        "lane": lane_id,
        "source_file": source_file,
        "gate_metric": "distinct_resolved_fill_ids_since_enforcement_start",
        "enforcement_start": NO_FALLBACK_ENFORCEMENT_START,
        "distinct_resolved_fill_ids": distinct_count,
        "current_source_distinct_resolved_fill_ids": current_source_count,
        "prior_distinct_resolved_fill_ids": prior_count,
        "source_restated_lower_than_prior": bool(prior_count and current_source_count < prior_count),
        "monotonicity_tripwire": {
            "status": "UNTRUSTED" if monotonic_decrease_detected else "PASS",
            "decrease_detected": monotonic_decrease_detected,
        },
        "entries": entries,
    }


def _full_population_arbitration(
    *,
    filled_orders: list[dict[str, Any]],
    scored_orders: list[dict[str, Any]],
    now_ts: float | None = None,
) -> dict[str, Any]:
    """Apply Fable's full-population, post-fee E5 promotion authority."""

    current_ts = time.time() if now_ts is None else float(now_ts)
    scored_by_order_id = {
        str(row.get("order_id") or ""): row
        for row in scored_orders
        if str(row.get("order_id") or "")
    }
    unresolved_old = []
    resolved_rows: list[tuple[float, float, float]] = []
    for order in filled_orders:
        order_id = str(order.get("order_id") or "")
        scored = scored_by_order_id.get(order_id, {})
        quote = order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}
        chronological_ts = num(quote.get("window_start_s"))
        if chronological_ts <= 0:
            chronological_ts = num((order.get("maker_fill_evidence") or {}).get("fill_event_ts"))
        if chronological_ts <= 0:
            chronological_ts = num(quote.get("quote_ts"))
        if not bool(scored.get("resolved")):
            if current_ts > num(quote.get("window_end_s")) + 300.0:
                unresolved_old.append(order_id)
            continue
        shares = num(scored.get("shares"), num(order.get("filled_shares")))
        cost = num(scored.get("cost_usd"))
        price = cost / shares if shares > 0 else num(order.get("limit_price"))
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        resolved_rows.append((chronological_ts, num(scored.get("pnl_usd")) - fee, cost))

    resolved_rows.sort(key=lambda row: row[0])
    split = len(resolved_rows) // 2
    first_half = resolved_rows[:split]
    second_half = resolved_rows[split:]
    post_fee_pnl = sum(row[1] for row in resolved_rows)
    cost = sum(row[2] for row in resolved_rows)
    first_half_pnl = sum(row[1] for row in first_half)
    second_half_pnl = sum(row[1] for row in second_half)
    roi_pct = (post_fee_pnl / cost) * 100.0 if cost > 0 else 0.0
    conditions = {
        "no_unresolved_inventory_older_than_one_window": not unresolved_old,
        "resolved_fills_gte_1000": len(resolved_rows) >= 1_000,
        "full_population_post_fee_pnl_positive": post_fee_pnl > 0,
        "first_chronological_half_post_fee_pnl_positive": first_half_pnl > 0,
        "second_chronological_half_post_fee_pnl_positive": second_half_pnl > 0,
        "full_population_post_fee_roi_gte_2pct": roi_pct >= 2.0,
    }
    passed = all(conditions.values())
    return {
        "decision": "PASS_AUTO_PROMOTE" if passed else "PARK_E5_UNRESOLVED_OR_NEGATIVE",
        "passed": passed,
        "conditions": conditions,
        "filled_population": len(filled_orders),
        "resolved_fills": len(resolved_rows),
        "unresolved_fills": len(filled_orders) - len(resolved_rows),
        "unresolved_old_fills": len(unresolved_old),
        "post_fee_pnl_usd": round(post_fee_pnl, 6),
        "post_fee_roi_pct": round(roi_pct, 6),
        "first_half": {
            "resolved_fills": len(first_half),
            "post_fee_pnl_usd": round(first_half_pnl, 6),
        },
        "second_half": {
            "resolved_fills": len(second_half),
            "post_fee_pnl_usd": round(second_half_pnl, 6),
        },
        "fee_model": {
            "source": POLYMARKET_EMBEDDED_FEE_SOURCE,
            "formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
        },
        "authority": "Fable DIRECTION 2026-07-31T01:47:10Z",
    }


def _full_population_arbitration_artifact(
    *,
    lane: str,
    updated_at: str,
    source_state_path: str,
    arbitration: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "kind": "maker_first_btc5m_full_population_arbitration",
        "flow_stage": "PROMOTE/LEARN",
        "lane": lane,
        "updated_at": updated_at,
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "source_state_path": source_state_path,
        "full_population_arbitration": arbitration,
        "decision": arbitration.get("decision"),
        "passed": bool(arbitration.get("passed")),
    }


def _arbitration_state_path(args: argparse.Namespace) -> str:
    explicit = str(getattr(args, "arbitration_state", "") or "").strip()
    if explicit:
        return explicit
    book_path = Path(str(args.book_aware_state))
    if str(book_path) == DEFAULT_BOOK_AWARE_STATE:
        return DEFAULT_ARBITRATION_STATE
    stem = book_path.stem
    if stem.endswith("_book_aware_state"):
        stem = stem[: -len("_book_aware_state")]
    return str(book_path.with_name(f"{stem}_full_population_arbitration_latest.json"))


def _build_book_aware_state(
    *,
    orders: list[dict[str, Any]],
    scored_orders: list[dict[str, Any]],
    resolutions_path: str,
    event_log: str,
    gamma_refresh: dict[str, Any],
    copyintent_parity_violations: int,
    lane_id: str = LANE_ID,
    signal_gated_maker: bool = False,
    prior_state: dict[str, Any] | None = None,
    source_file: str = DEFAULT_BOOK_AWARE_STATE,
) -> dict[str, Any]:
    scored_by_order_id = {str(row.get("order_id") or ""): row for row in scored_orders}
    evidence_by_order_id = {
        str(order.get("order_id") or ""): _book_evidence(order)
        for order in orders
        if str(order.get("order_id") or "")
    }
    book_orders = [order for order in orders if evidence_by_order_id.get(str(order.get("order_id") or ""))]
    non_fallback_book_orders = [
        order
        for order in book_orders
        if not _route_uses_direct_fallback(evidence_by_order_id.get(str(order.get("order_id") or "")) or {})
    ]
    prospective_no_fallback_orders = [order for order in non_fallback_book_orders if _order_is_enforced_no_fallback(order)]
    filled_book_orders = [
        order for order in book_orders if str(order.get("final_status") or order.get("status") or "").upper() == "FILLED"
    ]
    filled_non_fallback_book_orders = [
        order
        for order in non_fallback_book_orders
        if str(order.get("final_status") or order.get("status") or "").upper() == "FILLED"
    ]
    filled_prospective_no_fallback_orders = [
        order
        for order in prospective_no_fallback_orders
        if str(order.get("final_status") or order.get("status") or "").upper() == "FILLED"
    ]
    scored_book_orders = [
        scored_by_order_id[str(order.get("order_id") or "")]
        for order in filled_book_orders
        if str(order.get("order_id") or "") in scored_by_order_id
    ]
    scored_non_fallback_book_orders = [
        scored_by_order_id[str(order.get("order_id") or "")]
        for order in filled_non_fallback_book_orders
        if str(order.get("order_id") or "") in scored_by_order_id
    ]
    scored_prospective_no_fallback_orders = [
        scored_by_order_id[str(order.get("order_id") or "")]
        for order in filled_prospective_no_fallback_orders
        if str(order.get("order_id") or "") in scored_by_order_id
    ]
    summary = _summarize_scored_orders(book_orders, filled_book_orders, scored_book_orders)
    non_fallback_summary = _summarize_scored_orders(
        non_fallback_book_orders,
        filled_non_fallback_book_orders,
        scored_non_fallback_book_orders,
    )
    prospective_no_fallback_summary = _summarize_scored_orders(
        prospective_no_fallback_orders,
        filled_prospective_no_fallback_orders,
        scored_prospective_no_fallback_orders,
    )
    prospective_resolved_fill_ledger = _prospective_resolved_fill_ledger(
        current_scored_orders=scored_prospective_no_fallback_orders,
        prior_state=prior_state,
        source_file=source_file,
        lane_id=lane_id,
    )
    full_population_arbitration = _full_population_arbitration(
        filled_orders=filled_prospective_no_fallback_orders,
        scored_orders=scored_prospective_no_fallback_orders,
    )
    prospective_gate_untrusted = (
        prospective_resolved_fill_ledger["monotonicity_tripwire"]["status"] != "PASS"
    )
    fallback_reason_counts: Counter[str] = Counter()
    for top in evidence_by_order_id.values():
        if isinstance(top, dict) and _route_uses_direct_fallback(top):
            fallback_reason_counts[_fallback_reason(top)] += 1
    direct_fallback_count = sum(fallback_reason_counts.values())
    success_pct = round((len(book_orders) / len(orders)) * 100.0, 6) if orders else 0.0
    fallback_share_pct = round((direct_fallback_count / len(book_orders)) * 100.0, 6) if book_orders else 0.0
    prospective_gate_resolved_fills = int(prospective_resolved_fill_ledger["distinct_resolved_fill_ids"])
    legacy_prospective_gate_pass = (
        not prospective_gate_untrusted
        and prospective_gate_resolved_fills >= NO_FALLBACK_RESOLVED_REQUIRED
        and prospective_no_fallback_summary["resolved_paper_pnl_usd"] > 0
        and prospective_no_fallback_summary["terminal_maker_fill_rate_pct"] >= NO_FALLBACK_MAKER_FILL_RATE_REQUIRED_PCT
        and copyintent_parity_violations == 0
    )
    legacy_prospective_gate_fail = (
        not prospective_gate_untrusted
        and prospective_gate_resolved_fills >= NO_FALLBACK_FAIL_RESOLVED
        and not legacy_prospective_gate_pass
    )
    prospective_gate_status = (
        "UNTRUSTED_COUNTER"
        if prospective_gate_untrusted
        else str(full_population_arbitration["decision"])
    )
    return {
        "schema_version": 1,
        "kind": (
            "signal_gated_maker_btc5m_book_aware_state"
            if signal_gated_maker
            else "maker_first_btc5m_book_aware_state"
        ),
        "lane": lane_id,
        "flow_stage": "OBSERVE/PROMOTE",
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "variant": "signal_gated_maker" if signal_gated_maker else "continuous_maker_first",
        "updated_at": utc_now_iso(),
        "source_event_log": str(event_log),
        "resolution_path": str(resolutions_path),
        "gamma_refresh": gamma_refresh,
        "summary": {
            **summary,
            "book_fetch_success_pct": success_pct,
            "book_evidence_orders": len(book_orders),
            "book_evidence_filled_orders": len(filled_book_orders),
            "non_fallback_book_evidence_orders": len(non_fallback_book_orders),
            "non_fallback_book_evidence_filled_orders": len(filled_non_fallback_book_orders),
            "prospective_no_fallback_book_orders": len(prospective_no_fallback_orders),
            "prospective_no_fallback_filled_orders": len(filled_prospective_no_fallback_orders),
            "direct_fallback_orders": direct_fallback_count,
            "direct_fallback_share_pct": fallback_share_pct,
            "fallback_reason_histogram": dict(sorted(fallback_reason_counts.items())),
            "copyintent_parity_violations": copyintent_parity_violations,
            "book_evidence_rule": "top_of_book.status == OK and book_hash present",
        },
        "non_fallback_summary": non_fallback_summary,
        "prospective_no_fallback_summary": prospective_no_fallback_summary,
        "prospective_no_fallback_resolved_fill_ledger": prospective_resolved_fill_ledger,
        "full_population_arbitration": full_population_arbitration,
        "promotion_gate": {
            "gate_source_file": source_file,
            "gate_lane": lane_id,
            "gate_metric": "full_population_arbitration.resolved_fills",
            "resolved_paper_fills": full_population_arbitration["resolved_fills"],
            "legacy_append_only_resolved_fill_ids": prospective_gate_resolved_fills,
            "prospective_no_fallback_resolved_fills_required": NO_FALLBACK_RESOLVED_REQUIRED,
            "prospective_no_fallback_fail_resolved_fills": NO_FALLBACK_FAIL_RESOLVED,
            "requires_positive_prospective_no_fallback_pnl": True,
            "maker_fill_rate_required_pct": NO_FALLBACK_MAKER_FILL_RATE_REQUIRED_PCT,
            "maker_fill_rate_denominator": "terminal_quotes_filled_plus_cancelled",
            "monotonicity_tripwire": prospective_resolved_fill_ledger["monotonicity_tripwire"],
            "promotion_150_prospective_no_fallback_positive": prospective_gate_status,
            "auto_promote_to_live_guard_on_pass": bool(full_population_arbitration["passed"]),
            "demote_thesis_on_fail": bool(
                legacy_prospective_gate_fail or not full_population_arbitration["passed"]
            ),
            "full_population_arbitration_decision": full_population_arbitration["decision"],
            "legacy_150_fill_gate_would_pass": bool(legacy_prospective_gate_pass),
            "legacy_book_aware_resolved_fills_required": 50,
            "legacy_promotion_50_book_aware_resolved_positive": (
                "PASS"
                if summary["resolved_paper_fills"] >= 50
                and summary["resolved_paper_pnl_usd"] > 0
                and copyintent_parity_violations == 0
                else "PENDING"
            ),
            "copyintent_parity_violations": copyintent_parity_violations,
            "note": (
                "Fable 2026-07-07T08:28Z: only prospectively enforced no-fallback, book-hash-backed "
                "maker quotes count toward live promotion."
            ),
        },
        "scored_orders": scored_book_orders[-500:],
        "scored_non_fallback_orders": scored_non_fallback_book_orders[-500:],
        "scored_prospective_no_fallback_orders": scored_prospective_no_fallback_orders[-500:],
    }


def _run_canonical_gamma_refresh(args: argparse.Namespace) -> dict[str, Any]:
    summary_path = str(getattr(args, "gamma_refresh_summary", DEFAULT_GAMMA_REFRESH_SUMMARY))
    command = [
        sys.executable,
        str(ROOT / "scripts" / "refresh_btc_5m_resolutions_from_gamma.py"),
        "--ledger",
        "data/research/wallet_copy_live_execution_state.json",
        "--profit-state",
        str(getattr(args, "state", DEFAULT_STATE)),
        "--existing",
        str(getattr(args, "resolutions", DEFAULT_RESOLUTIONS)),
        "--output",
        str(getattr(args, "resolutions", DEFAULT_RESOLUTIONS)),
        "--summary-output",
        summary_path,
        "--merge-existing",
        "--max-windows",
        str(int(getattr(args, "gamma_refresh_max_windows", 120) or 0)),
        "--timeout-s",
        str(float(getattr(args, "gamma_refresh_timeout_s", 1.5) or 1.5)),
        "--sleep-s",
        str(float(getattr(args, "gamma_refresh_sleep_s", 0.0) or 0.0)),
        "--max-wall-runtime-s",
        str(float(getattr(args, "gamma_refresh_max_wall_runtime_s", 20.0) or 0.0)),
    ]
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=float(getattr(args, "gamma_refresh_child_timeout_s", 35.0) or 35.0),
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "status": "TIMEOUT",
            "returncode": None,
            "duration_s": round(time.perf_counter() - started, 6),
            "summary_output": summary_path,
            "command": command,
            "stdout_tail": str(exc.stdout or "")[-1000:],
            "stderr_tail": str(exc.stderr or "")[-1000:],
            "defect": "canonical_gamma_refresh_timeout",
            "next_action": "rerun E5 scorer; canonical Gamma refresh remains first step",
        }
    summary = load_json(summary_path, default={})
    summary = summary if isinstance(summary, dict) else {}
    status = "PASS" if int(completed.returncode) in {0, 2} else "ERROR"
    return {
        "status": status,
        "returncode": int(completed.returncode),
        "duration_s": round(time.perf_counter() - started, 6),
        "summary_output": summary_path,
        "command": command,
        "refresh_summary": summary,
        "stdout_tail": str(completed.stdout or "")[-1000:],
        "stderr_tail": str(completed.stderr or "")[-1000:],
    }


def _write_live_intents_feed(
    args: argparse.Namespace,
    *,
    lane_id: str,
    updated_at: str,
    current_intents: list[dict[str, Any]],
    book_aware_state: dict[str, Any],
) -> None:
    atomic_write_json(
        getattr(args, "live_intents_state", DEFAULT_LIVE_INTENTS_STATE),
        {
            "schema_version": 1,
            "kind": "e5_maker_first_live_intents_feed",
            "lane": lane_id,
            "flow_stage": "OBSERVE/PROMOTE/LIVE",
            "paper_only": True,
            "can_trade": False,
            "live_orders_allowed": False,
            "updated_at": updated_at,
            "source_state": str(args.state),
            "gate_source": str(args.book_aware_state),
            "current_intents": current_intents,
            "promotion_gate": book_aware_state.get("promotion_gate", {}),
            "prospective_no_fallback_summary": book_aware_state.get("prospective_no_fallback_summary", {}),
        },
    )


def build_state(args: argparse.Namespace) -> dict[str, Any]:
    now_ts = time.time()
    signal_gated_maker = bool(getattr(args, "signal_gated_maker", False))
    lane_id = (
        SIGNAL_GATED_LANE_ID
        if signal_gated_maker
        else str(getattr(args, "lane_id", LANE_ID) or LANE_ID)
    )
    copy_model = SIGNAL_GATED_COPY_MODEL if signal_gated_maker else "maker_first_btc5m"
    intent_source_wallet = (
        SIGNAL_GATED_INTENT_SOURCE
        if signal_gated_maker
        else str(getattr(args, "intent_source_wallet", "E5_MAKER_FIRST") or "E5_MAKER_FIRST")
    )
    gamma_refresh = _run_canonical_gamma_refresh(args)
    prior = {} if bool(args.reset_state) else load_json(args.state, default={})
    prior = prior if isinstance(prior, dict) else {}
    prior_orders = [row for row in prior.get("orders") or [] if isinstance(row, dict)]
    prior_quote_ids = {
        str(((order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}) or {}).get("quote_id") or "")
        for order in prior_orders
    }
    prior_quote_ids.discard("")

    events, feed_diagnostics = load_recent_events(
        args.rtds_jsonl,
        scan_limit=int(args.scan_limit),
        scan_max_bytes=int(args.scan_max_bytes),
        max_feed_events=int(args.max_feed_events),
        max_event_age_s=float(args.max_event_age_s),
        now_ts=now_ts,
    )
    clob = CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
    toxicity_cells, toxicity_summary = (
        _load_toxicity_deny_cells(str(getattr(args, "signal_gated_toxicity_denylist", "")))
        if signal_gated_maker
        else (
            set(),
            {
                "enabled": False,
                "reason": "signal_gated_maker_disabled",
                "path": str(getattr(args, "signal_gated_toxicity_denylist", "")),
            },
        )
    )
    signals, signal_diagnostics = build_quote_signals(
        events,
        prior_quote_ids=prior_quote_ids,
        now_ts=now_ts,
        quote_lookback_s=float(args.quote_lookback_s),
        max_quotes=int(args.max_quotes),
        order_usd=float(args.order_usd),
        max_order_usd=float(args.max_order_usd),
        max_price=float(args.max_price),
        tick_size=float(args.tick_size),
        quote_latency_s=float(args.quote_latency_s),
        cancel_before_close_s=float(args.cancel_before_close_s),
        fetch_clob_book=not bool(args.skip_clob_book),
        clob=clob,
        enforce_no_fallback_book=not bool(args.allow_book_fallback_quotes),
        lane_id=lane_id,
        copy_model=copy_model,
        intent_source_wallet=intent_source_wallet,
        signal_gated_max_age_s=(
            float(getattr(args, "signal_gated_max_age_s", 5.0) or 0.0) if signal_gated_maker else None
        ),
        toxicity_deny_cells=toxicity_cells,
        fixed_shares=float(getattr(args, "fixed_shares", 5.0) or 0.0),
        window_offset_min_s=float(getattr(args, "window_offset_min_s", 0.0) or 0.0),
        window_offset_max_s=float(getattr(args, "window_offset_max_s", 270.0) or 270.0),
        outcomes={
            value.strip().title()
            for value in str(getattr(args, "outcomes", "Up,Down") or "").split(",")
            if value.strip()
        },
    )
    current_intents = [maker_signal_to_intent(signal).asdict() for signal in signals]
    prior_book_aware_state = load_json(args.book_aware_state, default={}) if not bool(args.reset_state) else {}
    prior_book_aware_state = prior_book_aware_state if isinstance(prior_book_aware_state, dict) else {}
    # Publish the strict live handoff before the expensive 50k-order scoring
    # pass. The guard independently revalidates the authoritative gate, exact
    # CopyIntent parity, book hash, and 30-second freshness before submission.
    _write_live_intents_feed(
        args,
        lane_id=lane_id,
        updated_at=utc_now_iso(),
        current_intents=current_intents,
        book_aware_state=prior_book_aware_state,
    )
    refreshed_orders, updated_orders = _refresh_open_orders(prior_orders, events, now_ts=now_ts)
    new_orders = [_order_from_signal(signal, events, now_ts=now_ts) for signal in signals]
    orders = _merge_orders(refreshed_orders, new_orders)
    scoring_summary, scored_orders, resolution_rows_indexed = _score_summary(orders, args.resolutions)
    copyintent_parity_violations = sum(
        1
        for order in orders
        if not isinstance(order.get("source_intent"), dict)
        or bool((order.get("source_intent") or {}).get("live_orders_allowed"))
    )
    resolved_order_ids = {
        str(score.get("order_id") or "")
        for score in scored_orders
        if bool(score.get("resolved")) and str(score.get("order_id") or "")
    }
    unresolved_old = [
        order
        for order in orders
        if str(order.get("final_status") or "").upper() == "FILLED"
        and str(order.get("order_id") or "") not in resolved_order_ids
        and now_ts > num(((order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}) or {}).get("window_end_s")) + 300.0
    ]
    gate_status = (
        "PASS"
        if scoring_summary["resolved_paper_fills"] >= 50
        and scoring_summary["resolved_paper_pnl_usd"] > 0
        and scoring_summary["maker_fill_rate_pct"] >= 20.0
        and not unresolved_old
        and copyintent_parity_violations == 0
        else "PENDING"
    )
    append_jsonl_many(args.event_log, updated_orders + new_orders)
    book_aware_state = _build_book_aware_state(
        orders=orders,
        scored_orders=scored_orders,
        resolutions_path=args.resolutions,
        event_log=args.event_log,
        gamma_refresh=gamma_refresh,
        copyintent_parity_violations=copyintent_parity_violations,
        lane_id=lane_id,
        signal_gated_maker=signal_gated_maker,
        prior_state=prior_book_aware_state,
        source_file=str(args.book_aware_state),
    )
    parallel_gate_results: list[dict[str, Any]] = []
    if signal_gated_maker:
        canonical_age = float(getattr(args, "signal_gated_max_age_s", 5.0) or 0.0)
        for age_s in _parse_age_gates(getattr(args, "signal_gated_parallel_age_s", "")):
            if abs(float(age_s) - canonical_age) < 1e-9:
                continue
            parallel_gate_results.append(
                _build_signal_gated_measurement_variant(
                    args=args,
                    events=events,
                    now_ts=now_ts,
                    clob=clob,
                    toxicity_cells=toxicity_cells,
                    age_s=float(age_s),
                )
            )
    state = {
        "schema_version": 1,
        "kind": (
            "signal_gated_maker_btc5m_paper_state"
            if signal_gated_maker
            else "maker_first_btc5m_paper_state"
        ),
        "lane": lane_id,
        "flow_stage": "OBSERVE",
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "variant": "signal_gated_maker" if signal_gated_maker else "continuous_maker_first",
        "updated_at": utc_now_iso(),
        "parameters": {
            "order_usd": float(args.order_usd),
            "fixed_shares": float(getattr(args, "fixed_shares", 5.0) or 0.0),
            "max_order_usd": float(args.max_order_usd),
            "max_price": float(args.max_price),
            "tick_size": float(args.tick_size),
            "quote_latency_s": float(args.quote_latency_s),
            "cancel_before_close_s": float(args.cancel_before_close_s),
            "window_offset_min_s": float(getattr(args, "window_offset_min_s", 0.0) or 0.0),
            "window_offset_max_s": float(getattr(args, "window_offset_max_s", 270.0) or 270.0),
            "outcomes": str(getattr(args, "outcomes", "Up,Down")),
            "max_event_age_s": float(args.max_event_age_s),
            "quote_lookback_s": float(args.quote_lookback_s),
            "fetch_clob_book": not bool(args.skip_clob_book),
            "enforce_no_fallback_book": not bool(args.allow_book_fallback_quotes),
            "allow_book_fallback_quotes": bool(args.allow_book_fallback_quotes),
            "clob_base_url": args.clob_base_url,
            "signal_gated_maker": signal_gated_maker,
            "signal_gated_max_age_s": (
                float(getattr(args, "signal_gated_max_age_s", 5.0) or 0.0) if signal_gated_maker else None
            ),
            "signal_gated_parallel_age_s": (
                _parse_age_gates(getattr(args, "signal_gated_parallel_age_s", "")) if signal_gated_maker else []
            ),
            "signal_gated_toxicity_denylist": str(getattr(args, "signal_gated_toxicity_denylist", "")),
        },
        "diagnostics": {
            "feed": feed_diagnostics,
            "signals": signal_diagnostics,
            "signal_gated_toxicity": toxicity_summary,
            "parallel_signal_gates": parallel_gate_results,
            "new_orders": len(new_orders),
            "updated_orders": len(updated_orders),
            "gamma_refresh": gamma_refresh,
        },
        "current_signals": signals,
        "current_intents": current_intents,
        "orders": orders,
        "summary": {
            **scoring_summary,
            "accepted_feed_events": feed_diagnostics.get("accepted_feed_events", 0),
            "signals": len(signals),
            "parallel_signal_gates": [
                {
                    "age_s": row.get("age_s"),
                    "state_path": row.get("state_path"),
                    "signals": ((row.get("summary") or {}).get("signals") if isinstance(row.get("summary"), dict) else None),
                    "paper_quotes": (
                        (row.get("summary") or {}).get("paper_quotes") if isinstance(row.get("summary"), dict) else None
                    ),
                    "resolved_paper_fills": (
                        (row.get("summary") or {}).get("resolved_paper_fills")
                        if isinstance(row.get("summary"), dict)
                        else None
                    ),
                    "resolved_paper_pnl_usd": (
                        (row.get("summary") or {}).get("resolved_paper_pnl_usd")
                        if isinstance(row.get("summary"), dict)
                        else None
                    ),
                    "age_p90_s": (
                        ((row.get("diagnostics") or {}).get("signals") or {}).get("signal_gated_age_p90_s")
                        if isinstance(row.get("diagnostics"), dict)
                        and isinstance((row.get("diagnostics") or {}).get("signals"), dict)
                        else None
                    ),
                }
                for row in parallel_gate_results
            ],
            "new_orders": len(new_orders),
            "live_orders_allowed": False,
            "paper_only": True,
        },
        "promotion_gate": {
            **scoring_summary,
            "promotion_50_resolved_positive": gate_status,
            "resolved_paper_fills_required": 50,
            "requires_positive_pnl": True,
            "maker_fill_rate_required_pct": 20.0,
            "active_gate": "book_aware_state.promotion_gate.promotion_150_prospective_no_fallback_positive",
            "active_gate_decision": book_aware_state["promotion_gate"][
                "promotion_150_prospective_no_fallback_positive"
            ],
            "active_gate_note": (
                "Legacy aggregate gate retained for history; Fable 08:28Z gate is prospective "
                "no-fallback book-aware state."
            ),
            "no_unresolved_inventory_older_than_one_window": not unresolved_old,
            "unresolved_old_fills": len(unresolved_old),
            "copyintent_parity_violations": copyintent_parity_violations,
            "note": "E5 maker-first paper lane; promotion requires Fable decision after gate PASS.",
        },
        "resolution_scoring": {
            "kind": "e5_maker_first_resolution_scoring_v1",
            "updated_at": utc_now_iso(),
            "resolution_path": str(args.resolutions),
            "resolution_rows_indexed": resolution_rows_indexed,
            "scored_orders": scored_orders[-500:],
        },
        "book_aware_state_path": str(args.book_aware_state),
        "book_aware_summary": book_aware_state["summary"],
    }
    atomic_write_json(args.state, state)
    atomic_write_json(args.book_aware_state, book_aware_state)
    atomic_write_json(
        _arbitration_state_path(args),
        _full_population_arbitration_artifact(
            lane=lane_id,
            updated_at=state["updated_at"],
            source_state_path=str(args.book_aware_state),
            arbitration=book_aware_state["full_population_arbitration"],
        ),
    )
    _write_live_intents_feed(
        args,
        lane_id=lane_id,
        updated_at=state["updated_at"],
        current_intents=current_intents,
        book_aware_state=book_aware_state,
    )
    atomic_write_json(
        args.resolution_state,
        {
            "schema_version": 1,
            "kind": "maker_first_btc5m_resolution_state",
            "lane": lane_id,
            "flow_stage": "OBSERVE/PROMOTE",
            "paper_only": True,
            "can_trade": False,
            "live_orders_allowed": False,
            "variant": "signal_gated_maker" if signal_gated_maker else "continuous_maker_first",
            "updated_at": utc_now_iso(),
            "summary": {
                **scoring_summary,
                "copyintent_parity_violations": copyintent_parity_violations,
                "unresolved_old_fills": len(unresolved_old),
                "paper_only": True,
                "live_orders_allowed": False,
            },
            "promotion_gate": state["promotion_gate"],
            "resolution_scoring": state["resolution_scoring"],
            "book_aware_state_path": str(args.book_aware_state),
            "book_aware_summary": book_aware_state["summary"],
        },
    )
    return state


def main() -> int:
    args = parse_args()
    state = build_state(args)
    print(json.dumps(state.get("summary", {}), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
