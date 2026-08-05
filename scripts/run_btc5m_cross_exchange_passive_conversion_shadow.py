#!/usr/bin/env python3
"""Resident paper-only passive conversion successor for the BTC-5m cross-exchange lane.

The default legacy lane stays inert until the sole guard records
EXPIRED_ZERO_CONVERSION. Comparator cells use always-paper activation, place
at most one simulated post-only quote per window, and grade
that quote only from later, directly observed CLOB books. It never submits
an order and never mutates live configuration.
"""

from __future__ import annotations

import argparse
import math
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_e7_spot_open_paper_lane import DEFAULT_CLOB_BASE, _book_snapshot_with_direct_fallback
from scripts.run_btc5m_cross_exchange_probability_edge_paper_lane import (
    MAX_BUY_PRICE,
    MIN_BUY_PRICE,
    ORDER_USD,
)
from src.wallet_copy.fees import expected_polymarket_buy_fee_usd
from src.wallet_copy.live_tracker import CLOBMarketClient
from src.wallet_copy.models import stable_id, utc_now_iso
from src.wallet_copy.performance import load_resolutions, score_order
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json


LANE_ID = "paper_struct_btc5m_cross_exchange_passive_conversion"
MODEL_CHECKSUM = "848f22460923a497d0026973be9b1f6e2f43659ccdd1556979907a4ac9e98170"
DEFAULT_ACTUATOR = "data/research/btc5m_cross_exchange_probability_edge_live_actuator_latest.json"
DEFAULT_SOURCE = "data/research/btc5m_cross_exchange_probability_edge_paper_lane_state.json"
DEFAULT_STATE = "data/research/btc5m_cross_exchange_passive_conversion_state.json"
DEFAULT_EVENTS = "data/research/btc5m_cross_exchange_passive_conversion_events.jsonl"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--actuator-state", default=DEFAULT_ACTUATOR)
    parser.add_argument("--source-state", default=DEFAULT_SOURCE)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--event-log", default=DEFAULT_EVENTS)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--cancel-before-close-s", type=float, default=30.0)
    parser.add_argument("--signal-offset-s", type=int, default=30)
    parser.add_argument(
        "--activation-policy",
        choices=("expired-zero-conversion", "always-paper"),
        default="expired-zero-conversion",
    )
    parser.add_argument("--interval-s", type=float, default=5.0)
    parser.add_argument("--now-ts", type=float, default=0.0)
    parser.add_argument("--watch", action="store_true")
    return parser.parse_args()


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _quote_decision(
    signal: dict[str, Any],
    *,
    now_ts: float,
    signal_offset_s: int = 30,
) -> dict[str, Any]:
    window_start = int(_num(signal.get("window_start_s")))
    probability = _num(signal.get("calibrated_probability"))
    book = signal.get("book") if isinstance(signal.get("book"), dict) else {}
    best_bid = _num(book.get("best_bid"))
    best_ask = _num(book.get("best_ask"))
    quote_price = math.floor(min(MAX_BUY_PRICE, best_bid) * 100.0 + 1e-9) / 100.0
    shares = ORDER_USD / quote_price if quote_price > 0 else 0.0
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=quote_price)
    net_edge = probability - quote_price - (fee / shares if shares > 0 else 0.0)
    reasons: list[str] = []
    if int(now_ts // 300) * 300 != window_start:
        reasons.append("not_current_window")
    if now_ts < window_start + signal_offset_s:
        reasons.append("before_frozen_signal_offset")
    if now_ts >= window_start + 300 - 30:
        reasons.append("inside_cancel_zone")
    if not str(signal.get("token_id") or ""):
        reasons.append("missing_token")
    if not MIN_BUY_PRICE <= quote_price <= MAX_BUY_PRICE:
        reasons.append("unchanged_hard_entry_bounds")
    if best_ask <= 0 or quote_price >= best_ask:
        reasons.append("not_strict_post_only")
    if net_edge <= 0:
        reasons.append("net_edge_nonpositive")
    return {
        "eligible": not reasons,
        "reasons": reasons,
        "window_start_s": window_start,
        "market_slug": signal.get("market_slug"),
        "condition_id": signal.get("condition_id"),
        "outcome": signal.get("outcome"),
        "token_id": signal.get("token_id"),
        "calibrated_probability": probability,
        "best_bid_at_quote": best_bid,
        "best_ask_at_quote": best_ask,
        "quote_price": quote_price,
        "order_usd": ORDER_USD,
        "requested_shares": round(shares, 6),
        "expected_fee_usd": fee,
        "net_edge_per_share": round(net_edge, 8),
        "strict_post_only": True,
        "paper_only": True,
        "live_orders_allowed": False,
    }


def _score_orders(orders: list[dict[str, Any]], resolutions_path: str) -> dict[str, Any]:
    resolutions = load_resolutions(resolutions_path)
    scored = [score_order(order, resolutions) for order in orders if order.get("status") == "FILLED"]
    resolved = [row for row in scored if row.get("resolved")]
    pnl = sum(_num(row.get("pnl_usd")) for row in resolved)
    return {
        "quotes": len(orders),
        "fills": sum(1 for row in orders if row.get("status") == "FILLED"),
        "open": sum(1 for row in orders if row.get("status") == "OPEN"),
        "cancelled": sum(1 for row in orders if row.get("status") == "CANCELLED"),
        "resolved_fills": len(resolved),
        "wins": sum(1 for row in resolved if row.get("win")),
        "realized_pnl_usd": round(pnl, 6),
        "positive": pnl > 0,
    }


def _queue_ahead_at_price(book: Any, quote_price: float) -> float:
    if isinstance(book, dict):
        levels = book.get("bids") or []
    else:
        levels = getattr(book, "bids", []) or []
    queue = 0.0
    for level in levels:
        price = _num(level.get("price")) if isinstance(level, dict) else _num(getattr(level, "price", 0))
        size = _num(level.get("size")) if isinstance(level, dict) else _num(getattr(level, "size", 0))
        if abs(price - quote_price) <= 1e-9:
            queue += size
    return round(queue, 6)


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    now_ts = float(args.now_ts or time.time())
    prior = load_json(args.state, default={})
    orders = [dict(row) for row in prior.get("orders", []) if isinstance(row, dict)]
    actuator = load_json(args.actuator_state, default={})
    source = load_json(args.source_state, default={})
    source_checksum = str(((source.get("frozen_model") or {}).get("checksum")) or "")
    always_paper = str(args.activation_policy) == "always-paper"
    source_offset_exact = str(
        ((source.get("preregistration") or {}).get("feature_schema") or {}).get(
            "signal_offset_s"
        )
    ) == str(int(args.signal_offset_s))
    model_exact = bool(source_checksum) and (
        source_offset_exact
        if always_paper
        else source_checksum == MODEL_CHECKSUM
    )
    gate_open = model_exact and (
        always_paper
        or (
            str(actuator.get("status") or "") == "EXPIRED_ZERO_CONVERSION"
            and int(actuator.get("orders_accepted") or 0) == 0
        )
    )
    events: list[dict[str, Any]] = []
    clob = CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
    for order in orders:
        if order.get("status") != "OPEN":
            continue
        window_start = int(order.get("window_start_s") or 0)
        if now_ts >= window_start + 300 - float(args.cancel_before_close_s):
            order["status"] = "CANCELLED"
            order["updated_at"] = utc_now_iso()
            order["terminal_reason"] = "cancel_before_close"
            events.append({"event": "passive_quote_cancelled", **order})
            continue
        book = _book_snapshot_with_direct_fallback(
            clob=clob,
            token_id=str(order.get("token_id") or ""),
            order_usd=ORDER_USD,
            max_entry_price=float(order.get("quote_price") or 0.0),
        )
        observed_ask = _num(book.get("best_ask"))
        if str(book.get("status") or "") == "OK" and 0 < observed_ask <= _num(order.get("quote_price")):
            order["status"] = "FILLED"
            order["updated_at"] = utc_now_iso()
            order["fill_price"] = observed_ask
            order["fill_evidence"] = {
                "kind": "later_direct_clob_book_cross",
                "observed_at": utc_now_iso(),
                "book_hash": book.get("book_hash"),
                "book_timestamp": book.get("book_timestamp"),
                "best_ask": observed_ask,
                "rule": "later_best_ask_lte_resting_post_only_quote",
            }
            events.append({"event": "passive_quote_filled", **order})
    decision: dict[str, Any] = {"eligible": False, "reasons": ["activation_gate_closed"]}
    signal = ((source.get("current_cycle") or {}).get("signal")) if isinstance(source, dict) else None
    if gate_open and isinstance(signal, dict):
        decision = _quote_decision(
            signal,
            now_ts=now_ts,
            signal_offset_s=int(args.signal_offset_s),
        )
        window_start = int(decision.get("window_start_s") or 0)
        if decision["eligible"] and not any(int(row.get("window_start_s") or 0) == window_start for row in orders):
            try:
                raw_quote_book = clob.get_book(str(decision.get("token_id") or ""))
                queue_ahead_shares = _queue_ahead_at_price(
                    raw_quote_book,
                    _num(decision.get("quote_price")),
                )
            except Exception:
                queue_ahead_shares = 0.0
            quote = {
                "order_id": stable_id("po", {"lane": LANE_ID, "window_start_s": window_start}),
                "lane": LANE_ID,
                "window_start_s": window_start,
                "market_slug": decision.get("market_slug"),
                "condition_id": decision.get("condition_id"),
                "outcome": decision.get("outcome"),
                "side": "YES" if decision.get("outcome") == "Up" else "NO",
                "token_id": decision.get("token_id"),
                "limit_price": decision.get("quote_price"),
                "quote_price": decision.get("quote_price"),
                "requested_size_usd": ORDER_USD,
                "requested_shares": decision.get("requested_shares"),
                "filled_size_usd": 0.0,
                "filled_shares": 0.0,
                "status": "OPEN",
                "submitted_at": utc_now_iso(),
                "updated_at": utc_now_iso(),
                "paper_only": True,
                "live_orders_allowed": False,
                "post_only": True,
                "queue_ahead_shares_at_quote": queue_ahead_shares,
                "queue_model": {
                    "rule": "same-price resting bid size is ahead; later direct best ask <= quote proves trade-through",
                    "fill_requires_trade_through": True,
                },
                "model_checksum": source_checksum,
                "quote_evidence": decision,
            }
            orders.append(quote)
            events.append({"event": "passive_quote_opened", **quote})
    if events:
        append_jsonl_many(args.event_log, events)
    summary = _score_orders(orders, args.resolutions)
    payload = {
        "schema_version": 1,
        "kind": "btc5m_cross_exchange_passive_conversion_shadow",
        "generated_at": utc_now_iso(),
        "flow_stage": "LIVE/ROTATE/OBSERVE/LEARN/PROMOTE",
        "lane_id": LANE_ID,
        "status": "PAPER_ACTIVE" if gate_open else "WAITING_FOR_ACTIVATION",
        "activation_gate": {
            "pass": gate_open,
            "actuator_status": actuator.get("status"),
            "actuator_orders_accepted": actuator.get("orders_accepted"),
            "activation_policy": args.activation_policy,
            "signal_offset_s": int(args.signal_offset_s),
            "model_checksum": source_checksum,
            "model_checksum_exact": model_exact,
        },
        "current_decision": decision,
        "orders": orders[-5000:],
        "summary": summary,
        "one_quote_per_window": True,
        "cancel_before_close_s": float(args.cancel_before_close_s),
        "fill_evidence_rule": "later direct CLOB best ask <= resting quote",
        "paper_only": True,
        "live_orders_allowed": False,
        "orders_submitted_live": 0,
    }
    atomic_write_json(args.state, payload)
    return payload


def main() -> int:
    args = parse_args()
    while True:
        run_once(args)
        if not args.watch:
            return 0
        time.sleep(max(1.0, float(args.interval_s)))


if __name__ == "__main__":
    raise SystemExit(main())
