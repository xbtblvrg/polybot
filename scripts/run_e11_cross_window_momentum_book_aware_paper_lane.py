#!/usr/bin/env python3
"""Run E11 cross-window momentum through the E5 book-aware maker machinery."""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_e7_spot_open_paper_lane import _market_for_slug, _token_map
from scripts.run_maker_first_btc5m_paper_lane import (
    DEFAULT_CLOB_BASE,
    DEFAULT_RTDS_JSONL,
    _build_book_aware_state,
    _crossing_fill,
    _merge_orders,
    _refresh_open_orders,
    _score_summary,
    build_quote_signals,
    load_recent_events,
)
from scripts.run_whale_consensus_paper_lane import WhaleConsensusFeedEvent
from src.wallet_copy.live_tracker import CLOBMarketClient
from src.wallet_copy.models import CopyIntent, num, stable_id, utc_now_iso
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json


LANE_ID = "e11_cross_window_momentum_book_aware_v1"
SOURCE_WALLET = "E11_CROSS_WINDOW_MOMENTUM"
DEFAULT_STATE = "data/research/e11_cross_window_momentum_book_aware_state.json"
DEFAULT_EVENT_LOG = "data/research/e11_cross_window_momentum_book_aware_events.jsonl"
DEFAULT_BOOK_AWARE_STATE = "data/research/e11_cross_window_momentum_book_aware_book_state.json"
DEFAULT_RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
PROMOTION_RESOLVED_REQUIRED = 50
PROMOTION_MAKER_FILL_RATE_REQUIRED_PCT = 90.0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=DEFAULT_STATE)
    parser.add_argument("--event-log", default=DEFAULT_EVENT_LOG)
    parser.add_argument("--book-aware-state", default=DEFAULT_BOOK_AWARE_STATE)
    parser.add_argument("--rtds-jsonl", default=DEFAULT_RTDS_JSONL)
    parser.add_argument("--resolutions", default=DEFAULT_RESOLUTIONS)
    parser.add_argument("--scan-limit", type=int, default=50_000)
    parser.add_argument("--scan-max-bytes", type=int, default=192_000_000)
    parser.add_argument("--max-feed-events", type=int, default=8_000)
    parser.add_argument("--max-event-age-s", type=float, default=1_800.0)
    parser.add_argument("--order-usd", type=float, default=1.0)
    parser.add_argument("--max-order-usd", type=float, default=8.0)
    parser.add_argument("--max-price", type=float, default=0.45)
    parser.add_argument("--tick-size", type=float, default=0.01)
    parser.add_argument("--quote-latency-s", type=float, default=0.25)
    parser.add_argument("--cancel-before-close-s", type=float, default=30.0)
    parser.add_argument("--max-quotes", type=int, default=1)
    parser.add_argument("--gamma-timeout-s", type=float, default=4.0)
    parser.add_argument("--clob-base-url", default=DEFAULT_CLOB_BASE)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--reset-state", action="store_true")
    return parser.parse_args()


def _window_start(now_ts: float) -> int:
    return int(float(now_ts) // 300 * 300)


def _load_resolved_rows(path: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    p = Path(path)
    if not p.exists():
        return rows
    for line in p.read_text().splitlines():
        if not line.strip():
            continue
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict) and str(row.get("market_slug") or "").startswith("btc-updown-5m-"):
            rows.append(row)
    return rows


def _winner(row: dict[str, Any]) -> str:
    explicit = str(row.get("winner") or row.get("winning_outcome") or row.get("resolved_outcome") or "")
    if explicit in {"Up", "Down"}:
        return explicit
    direction = str(row.get("direction") or "").upper()
    if direction == "UP":
        return "Up"
    if direction == "DOWN":
        return "Down"
    return ""


def _latest_prior_winner(rows: list[dict[str, Any]], *, current_start: int) -> dict[str, Any]:
    candidates = [
        row
        for row in rows
        if int(row.get("window_start_s") or row.get("window_start_unix_ts") or 0) < int(current_start)
        and _winner(row) in {"Up", "Down"}
    ]
    return max(candidates, key=lambda row: int(row.get("window_start_s") or row.get("window_start_unix_ts") or 0), default={})


def build_momentum_event(*, resolutions: str, now_ts: float, gamma_timeout_s: float) -> tuple[WhaleConsensusFeedEvent | None, dict[str, Any]]:
    current_start = _window_start(now_ts)
    prior = _latest_prior_winner(_load_resolved_rows(resolutions), current_start=current_start)
    if not prior:
        return None, {"status": "NO_PRIOR_RESOLVED_WINNER", "current_window_start_s": current_start}
    outcome = _winner(prior)
    slug = f"btc-updown-5m-{current_start}"
    market = _market_for_slug(slug, timeout_s=float(gamma_timeout_s))
    tokens = _token_map(market)
    token_id = str(tokens.get(outcome) or "")
    if not token_id:
        return None, {
            "status": "NO_CURRENT_TOKEN",
            "market_slug": slug,
            "outcome": outcome,
            "prior_market_slug": prior.get("market_slug"),
            "gamma_fetch_error": market.get("_fetch_error") or "",
            "gamma_route_attempts": market.get("_gamma_route_attempts") or [],
        }
    event_ts = max(float(current_start) + 1.0, float(now_ts) - 1.0)
    event = WhaleConsensusFeedEvent(
        source_wallet=SOURCE_WALLET,
        market_slug=slug,
        condition_id=str(market.get("conditionId") or market.get("condition_id") or ""),
        outcome=outcome,
        side="BUY",
        price=0.45,
        size=round(1.0 / 0.45, 6),
        event_ts=event_ts,
        observed_ts=float(now_ts),
        token_id=token_id,
        transaction_hash="",
        event_id=stable_id("e11sig", {"market_slug": slug, "outcome": outcome, "prior": prior.get("market_slug")}),
    )
    return event, {
        "status": "SIGNAL",
        "market_slug": slug,
        "outcome": outcome,
        "prior_market_slug": prior.get("market_slug"),
        "prior_winner": outcome,
        "condition_id": event.condition_id,
        "token_id": token_id,
        "gamma_route_used": market.get("_gamma_route_used") or "",
    }


def e11_signal_to_intent(signal: dict[str, Any]) -> CopyIntent:
    order_usd = max(0.0, num(signal.get("order_usd"), 1.0))
    price = max(0.000001, num(signal.get("quote_price"), 0.45))
    metadata = {
        "copy_model": "e11_cross_window_momentum_book_aware",
        LANE_ID: signal,
        "row_type": "e11_cross_window_momentum_quote_signal",
        "source_fingerprint": str(signal.get("quote_id") or ""),
        "live_candidate_member": False,
        "promotion_gate": {
            "resolved_paper_fills_required": PROMOTION_RESOLVED_REQUIRED,
            "requires_positive_pnl": True,
            "maker_fill_rate_required_pct": PROMOTION_MAKER_FILL_RATE_REQUIRED_PCT,
            "maker_fill_rate_denominator": "terminal_quotes_filled_plus_cancelled",
            "enforced_no_fallback_book_required": True,
            "gate": "promotion_50_terminal_no_fallback_positive",
        },
    }
    return CopyIntent(
        intent_id=stable_id("ci", {"e11_quote_id": signal.get("quote_id")}),
        source_wallet=SOURCE_WALLET,
        wallet_name=LANE_ID,
        source_event_id=str(signal.get("quote_id") or ""),
        condition_id=str(signal.get("condition_id") or ""),
        market_slug=str(signal.get("market_slug") or ""),
        outcome=str(signal.get("outcome") or ""),
        side=str(signal.get("side") or ""),
        limit_price=round(price, 6),
        wallet_usdc_size=round(order_usd, 6),
        copy_size_usd=round(order_usd, 6),
        shares=round(order_usd / price, 6) if price > 0 else 0.0,
        observed_ts=num(signal.get("quote_ts")),
        strategy_family=LANE_ID,
        policy_id="e11_momentum_book_aware_le_45_min_1_cap_8",
        sizing_policy_id=f"fixed_usd_{str(round(order_usd, 6)).replace('.', 'p')}",
        mode="paper",
        action="BUY",
        order_type="PAPER_MAKER_QUOTE",
        token_id=str(signal.get("token_id") or ""),
        event_ts=num(signal.get("source_event_ts")) or None,
        api_latency_s=max(0.0, num(signal.get("quote_ts")) - num(signal.get("source_event_ts"))),
        live_orders_allowed=False,
        reason="E11 cross-window momentum book-aware paper quote",
        metadata=metadata,
    )


def _e11_cancel_reason(signal: dict[str, Any], *, lifecycle_message: str = "") -> str:
    if "re-quote" in lifecycle_message.lower() or "requote" in lifecycle_message.lower():
        return "re_quote"
    top_of_book = signal.get("top_of_book") if isinstance(signal.get("top_of_book"), dict) else {}
    counterfactual = (
        signal.get("maker_vs_taker_counterfactual")
        if isinstance(signal.get("maker_vs_taker_counterfactual"), dict)
        else {}
    )
    blocking_reason = str(top_of_book.get("blocking_reason") or counterfactual.get("blocking_reason") or "").lower()
    if blocking_reason and blocking_reason not in {"none", "pass", "ok"}:
        return "book_moved"
    return "window_expiry"


def _e11_order_from_signal(signal: dict[str, Any], events: list[WhaleConsensusFeedEvent], *, now_ts: float) -> dict[str, Any]:
    intent = e11_signal_to_intent(signal)
    fill = _crossing_fill(signal, events)
    window_end_s = num(signal.get("window_end_s"))
    cancel_ts = max(0.0, window_end_s - num(signal.get("cancel_before_close_s"), 30.0))
    cancel_reason = ""
    if fill:
        final_status = "FILLED"
        filled_size_usd = num(signal.get("order_usd"))
        filled_shares = filled_size_usd / max(num(signal.get("quote_price")), 0.000001)
        lifecycle_message = "paper E11 maker quote crossed by later RTDS sell"
    elif now_ts >= cancel_ts:
        final_status = "CANCELLED"
        filled_size_usd = 0.0
        filled_shares = 0.0
        lifecycle_message = "paper E11 maker quote cancelled before close"
        cancel_reason = _e11_cancel_reason(signal, lifecycle_message=lifecycle_message)
    else:
        final_status = "OPEN"
        filled_size_usd = 0.0
        filled_shares = 0.0
        lifecycle_message = "paper E11 maker quote resting"
    order_id = stable_id("po", {"e11_quote_id": signal.get("quote_id")})
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
        "cancel_reason": cancel_reason,
        "submitted_at": utc_now_iso(),
        "updated_at": utc_now_iso(),
        "paper_only": True,
        "live_orders_allowed": False,
        "fill_model": "e11_cross_window_momentum_book_aware_crossing_rtds_v1",
        "maker_quote": signal,
        "maker_fill_evidence": fill,
        "source_intent": intent.asdict(),
        "lifecycle": [
            {
                "ts": utc_now_iso(),
                "status": final_status,
                "message": lifecycle_message,
                "payload": {"quote_id": signal.get("quote_id"), "fill": fill, "cancel_reason": cancel_reason},
            }
        ],
    }


def _e11_order_cancel_reason(order: dict[str, Any]) -> str:
    if str(order.get("final_status") or order.get("status") or "").upper() != "CANCELLED":
        return ""
    explicit = str(order.get("cancel_reason") or "")
    if explicit:
        return explicit
    lifecycle = order.get("lifecycle") if isinstance(order.get("lifecycle"), list) else []
    lifecycle_text = " ".join(str(row.get("message") or "") for row in lifecycle if isinstance(row, dict))
    signal = order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}
    return _e11_cancel_reason(signal, lifecycle_message=lifecycle_text)


def _e11_cancel_reason_counts(orders: list[dict[str, Any]]) -> dict[str, int]:
    counts: Counter[str] = Counter()
    for order in orders:
        reason = _e11_order_cancel_reason(order)
        if reason:
            counts[reason] += 1
    return dict(sorted(counts.items()))


def _e11_promotion_gate(book_aware_state: dict[str, Any], *, copyintent_parity_violations: int) -> dict[str, Any]:
    summary = book_aware_state.get("prospective_no_fallback_summary")
    summary = summary if isinstance(summary, dict) else {}
    gate_pass = (
        int(summary.get("resolved_paper_fills") or 0) >= PROMOTION_RESOLVED_REQUIRED
        and float(summary.get("resolved_paper_pnl_usd") or 0.0) > 0.0
        and float(summary.get("terminal_maker_fill_rate_pct") or 0.0) >= PROMOTION_MAKER_FILL_RATE_REQUIRED_PCT
        and int(copyintent_parity_violations) == 0
    )
    return {
        **summary,
        "resolved_paper_fills_required": PROMOTION_RESOLVED_REQUIRED,
        "requires_positive_pnl": True,
        "maker_fill_rate_required_pct": PROMOTION_MAKER_FILL_RATE_REQUIRED_PCT,
        "maker_fill_rate_denominator": "terminal_quotes_filled_plus_cancelled",
        "enforced_no_fallback_book_required": True,
        "promotion_50_terminal_no_fallback_positive": "PASS" if gate_pass else "PENDING",
        "copyintent_parity_violations": int(copyintent_parity_violations),
        "note": "E11 book-aware paper gate; Fable 2026-07-07T08:45Z ordered terminal fill-rate denominator.",
    }


def _prior_market_outcomes(orders: list[dict[str, Any]]) -> set[tuple[str, str]]:
    keys: set[tuple[str, str]] = set()
    for order in orders:
        signal = order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}
        market_slug = str(signal.get("market_slug") or order.get("market_slug") or "")
        outcome = str(signal.get("outcome") or order.get("outcome") or "")
        if market_slug and outcome:
            keys.add((market_slug, outcome))
    return keys


def _dedupe_window_signals(
    signals: list[dict[str, Any]],
    prior_orders: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    prior_keys = _prior_market_outcomes(prior_orders)
    kept: list[dict[str, Any]] = []
    skipped = 0
    for signal in signals:
        key = (str(signal.get("market_slug") or ""), str(signal.get("outcome") or ""))
        if key in prior_keys:
            skipped += 1
            continue
        kept.append(signal)
    return kept, skipped


def _dedupe_prior_orders(prior_orders: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    kept: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    skipped = 0
    for order in prior_orders:
        signal = order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}
        key = (
            str(signal.get("market_slug") or order.get("market_slug") or ""),
            str(signal.get("outcome") or order.get("outcome") or ""),
        )
        if key[0] and key[1] and key in seen:
            skipped += 1
            continue
        if key[0] and key[1]:
            seen.add(key)
        kept.append(order)
    return kept, skipped


def build_state(args: argparse.Namespace) -> dict[str, Any]:
    now_ts = time.time()
    prior = {} if bool(args.reset_state) else load_json(args.state, default={})
    prior = prior if isinstance(prior, dict) else {}
    prior_orders = [row for row in prior.get("orders") or [] if isinstance(row, dict)]
    prior_orders, duplicate_prior_orders = _dedupe_prior_orders(prior_orders)
    prior_quote_ids = {
        str(((order.get("maker_quote") if isinstance(order.get("maker_quote"), dict) else {}) or {}).get("quote_id") or "")
        for order in prior_orders
    }
    prior_quote_ids.discard("")
    event, signal_status = build_momentum_event(
        resolutions=str(args.resolutions),
        now_ts=now_ts,
        gamma_timeout_s=float(args.gamma_timeout_s),
    )
    signal_events = [event] if event is not None else []
    fill_events, feed_diagnostics = load_recent_events(
        args.rtds_jsonl,
        scan_limit=int(args.scan_limit),
        scan_max_bytes=int(args.scan_max_bytes),
        max_feed_events=int(args.max_feed_events),
        max_event_age_s=float(args.max_event_age_s),
        now_ts=now_ts,
    )
    fill_events = sorted([*fill_events, *signal_events], key=lambda item: (item.observed_ts, item.event_ts, item.event_id))
    clob = CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
    signals, signal_diagnostics = build_quote_signals(
        signal_events,
        prior_quote_ids=prior_quote_ids,
        now_ts=now_ts,
        quote_lookback_s=300.0,
        max_quotes=int(args.max_quotes),
        order_usd=float(args.order_usd),
        max_order_usd=float(args.max_order_usd),
        max_price=float(args.max_price),
        tick_size=float(args.tick_size),
        quote_latency_s=float(args.quote_latency_s),
        cancel_before_close_s=float(args.cancel_before_close_s),
        fetch_clob_book=True,
        clob=clob,
        enforce_no_fallback_book=True,
    )
    signals, duplicate_window_signals = _dedupe_window_signals(signals, prior_orders)
    if duplicate_window_signals:
        signal_diagnostics["duplicate_window_signal"] = duplicate_window_signals
        signal_diagnostics["signals"] = len(signals)
    for signal in signals:
        signal["lane"] = LANE_ID
        signal["intent_wallet_name"] = LANE_ID
        signal["intent_source_wallet"] = SOURCE_WALLET
        signal["copy_model"] = "e11_cross_window_momentum_book_aware"
    refreshed_orders, updated_orders = _refresh_open_orders(prior_orders, fill_events, now_ts=now_ts)
    new_orders = [_e11_order_from_signal(signal, fill_events, now_ts=now_ts) for signal in signals]
    orders = _merge_orders(refreshed_orders, new_orders)
    for order in orders:
        if str(order.get("final_status") or order.get("status") or "").upper() == "CANCELLED" and not order.get("cancel_reason"):
            order["cancel_reason"] = _e11_order_cancel_reason(order)
    cancel_reason_counts = _e11_cancel_reason_counts(orders)
    scoring_summary, scored_orders, resolution_rows_indexed = _score_summary(orders, args.resolutions)
    copyintent_parity_violations = sum(
        1 for order in orders if not isinstance(order.get("source_intent"), dict) or bool(order["source_intent"].get("live_orders_allowed"))
    )
    append_jsonl_many(args.event_log, updated_orders + new_orders)
    book_aware_state = _build_book_aware_state(
        orders=orders,
        scored_orders=scored_orders,
        resolutions_path=str(args.resolutions),
        event_log=str(args.event_log),
        gamma_refresh={"status": "NOT_USED_E11_USES_EXISTING_RESOLUTION_INDEX"},
        copyintent_parity_violations=copyintent_parity_violations,
    )
    book_aware_state["lane"] = LANE_ID
    book_aware_state["kind"] = "e11_cross_window_momentum_book_aware_book_state"
    book_aware_state["promotion_gate"] = _e11_promotion_gate(
        book_aware_state,
        copyintent_parity_violations=copyintent_parity_violations,
    )
    state = {
        "schema_version": 1,
        "kind": "e11_cross_window_momentum_book_aware_state",
        "lane": LANE_ID,
        "flow_stage": "LEARN/PROMOTE",
        "paper_only": True,
        "can_trade": False,
        "live_orders_allowed": False,
        "updated_at": utc_now_iso(),
        "signal_status": signal_status,
        "parameters": {
            "rtds_jsonl": str(args.rtds_jsonl),
            "scan_limit": int(args.scan_limit),
            "scan_max_bytes": int(args.scan_max_bytes),
            "max_feed_events": int(args.max_feed_events),
            "max_event_age_s": float(args.max_event_age_s),
            "max_price": float(args.max_price),
            "order_usd": float(args.order_usd),
            "enforce_no_fallback_book": True,
        },
        "diagnostics": {
            "feed": feed_diagnostics,
            "signals": signal_diagnostics,
            "new_orders": len(new_orders),
            "updated_orders": len(updated_orders),
            "duplicate_prior_orders_removed": duplicate_prior_orders,
        },
        "current_signals": signals,
        "current_intents": [e11_signal_to_intent(signal).asdict() for signal in signals],
        "orders": orders,
        "summary": {
            **scoring_summary,
            "signals": len(signals),
            "new_orders": len(new_orders),
            "paper_only": True,
            "live_orders_allowed": False,
            "cancel_reason_counts": cancel_reason_counts,
        },
        "resolution_scoring": {"resolution_rows_indexed": resolution_rows_indexed, "scored_orders": scored_orders},
        "book_aware_state_path": str(args.book_aware_state),
        "book_aware_summary": book_aware_state["summary"],
        "promotion_gate": book_aware_state["promotion_gate"],
        "cancel_reason_counts": cancel_reason_counts,
    }
    atomic_write_json(args.state, state)
    atomic_write_json(args.book_aware_state, book_aware_state)
    return state


def main() -> int:
    args = parse_args()
    state = build_state(args)
    print(json.dumps({"signal_status": state.get("signal_status"), **state.get("summary", {})}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
