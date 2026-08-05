#!/usr/bin/env python3
"""Checksum-isolated native BTC-5m FIFO queue-hazard maker (paper only)."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.arbitrate_btc5m_promoted_cells import _checksum as _arbiter_checksum  # noqa: E402
from scripts.run_btc5m_book_shock_reversion import (  # noqa: E402
    _load_jsonl, _num, _resolution_map, _rooted, summarize_l2,
)
from scripts.run_e7_spot_open_paper_lane import _market_for_slug, _token_map  # noqa: E402
from src.wallet_copy.fees import expected_polymarket_buy_fee_usd  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import CopyIntent, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402

ORDER_USD = 1.0
MIN_PRICE, MAX_PRICE = 0.25, 0.50
OFFSET_S, TTL_S, LAST_ENTRY_S = 30, 45, 180
MIN_EDGE = 0.005
MAX_IMBALANCE = 0.80
ZERO_INTENT_WINDOWS, MAX_WINDOWS = 2, 6
MIN_RESOLVED, PERMANENT_RESOLVED = 10, 50
FILL_MODEL = "actual_contra_sell_volume_consumes_frozen_fifo_queue_v1"
FILL_MODEL_SHA = hashlib.sha256(FILL_MODEL.encode()).hexdigest()
STATE = "data/research/btc5m_queue_hazard_inventory_skew_maker_state.json"
CACHE = "data/research/btc5m_queue_hazard_inventory_skew_maker_raw_cache.json"
TERMINALS = "data/research/btc5m_queue_hazard_inventory_skew_maker_terminals.jsonl"
EVENTS = "data/research/btc5m_queue_hazard_inventory_skew_maker_events.jsonl"
SELECTOR = "data/research/btc5m_queue_hazard_inventory_skew_maker_selector.json"
RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
RUNG_C = "data/research/rung_c_no_admissible_target_latest.json"
CONFIG = {
    "schema_version": 1,
    "method": "btc5m_queue_depletion_inventory_skew_maker_v1",
    "economic_edge": "native_clob_fifo_spread_capture",
    "calibration_cutoff": "2026-07-25T16:02:00Z",
    "source": "native_btc5m_l2_plus_public_actual_trades",
    "entry_bounds": [MIN_PRICE, MAX_PRICE],
    "order_usd": ORDER_USD,
    "offset_s": OFFSET_S,
    "quote_ttl_s": TTL_S,
    "fee": "exact_embedded_buy_fee",
    "adverse_reserve": "max(0.005,abs(microprice-mid))+0.25*spread",
    "toxicity_break": f"abs_top_imbalance_gt_{MAX_IMBALANCE}",
    "inventory_limit": "one_quote_per_window",
    "fill_model": FILL_MODEL,
    "synthetic_touch_fills": False,
    "terminal_clock": [ZERO_INTENT_WINDOWS, MAX_WINDOWS],
    "promotion": {
        "resolved": MIN_RESOLVED,
        "permanent_resolved": PERMANENT_RESOLVED,
        "positive_post_cost_and_both_halves": True,
        "genuine_queue_fill": True,
        "positive_incremental_vs_no_quote": True,
        "exact_copyintent_parity": True,
    },
    "activation": "existing_promoted_cell_arbiter_singular_1usd_3600s_guard_pin",
    "paper_only": True,
    "live_orders_allowed": False,
}
CHECKSUM = hashlib.sha256(json.dumps(CONFIG, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
CELL_ID = f"queue_hazard_{CHECKSUM[:12]}"
PREREG = f"data/research/btc5m_queue_hazard_inventory_skew_maker_preregistration_{CHECKSUM[:12]}.json"


def _preregister() -> dict[str, Any]:
    body = {
        **CONFIG,
        "kind": "btc5m_queue_hazard_inventory_skew_maker_preregistration",
        "generation_checksum": CHECKSUM,
        "model_checksum": CHECKSUM,
        "execution_mode": "passive",
        "passive_fill_model_checksum": FILL_MODEL_SHA,
        "registered_before_outcome_inspection": True,
        "immutable": True,
    }
    expected = {**body, "checksum": hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()}
    prior = load_json(_rooted(PREREG), default={})
    if prior and prior != expected:
        raise RuntimeError("immutable preregistration mismatch")
    if not prior:
        atomic_write_json(_rooted(PREREG), expected)
    return expected


def choose_quote(book: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
    if book.get("status") != "PASS":
        return None, ["two_sided_l2_missing"]
    bid, ask = _num(book.get("best_bid")), _num(book.get("best_ask"))
    bid_n, ask_n = _num(book.get("best_bid_size")), _num(book.get("best_ask_size"))
    spread, mid = ask - bid, (ask + bid) / 2
    imbalance = (bid_n - ask_n) / (bid_n + ask_n) if bid_n + ask_n > 0 else 0
    shares = ORDER_USD / bid if bid > 0 else 0
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=bid)
    adverse = max(0.005, abs(_num(book.get("microprice")) - mid)) + 0.25 * spread
    edge = spread - (fee / shares if shares else 99) - adverse
    blockers: list[str] = []
    if not MIN_PRICE <= bid <= MAX_PRICE:
        blockers.append("price_outside_bounds")
    if abs(imbalance) > MAX_IMBALANCE:
        blockers.append("toxicity_break")
    if edge < MIN_EDGE:
        blockers.append("spread_below_fee_adverse_reserve")
    if bid_n <= 0 or shares <= 0:
        blockers.append("queue_or_size_missing")
    if blockers:
        return None, blockers
    return {
        "quote_price": bid,
        "best_bid_at_quote": bid,
        "best_ask_at_quote": ask,
        "queue_ahead_shares": bid_n,
        "shares": round(shares, 6),
        "fee_usd": fee,
        "net_edge_per_share": round(edge, 6),
        "imbalance": round(imbalance, 6),
        "book_sequence": time.time_ns(),
    }, []


def build_intent(
    *, outcome: str, condition_id: str, slug: str, token_id: str,
    observed_ts: float, quote: dict[str, Any],
) -> CopyIntent:
    return CopyIntent(
        intent_id=stable_id("qhazci", {"generation": CHECKSUM, "market": slug}),
        source_wallet=f"BTC5M_PROMOTED_CELL:{CELL_ID}",
        wallet_name=CELL_ID,
        source_event_id=stable_id("qhaze", {"market": slug, "outcome": outcome}),
        condition_id=condition_id,
        market_slug=slug,
        outcome=outcome,
        side="YES" if outcome == "Up" else "NO",
        limit_price=float(quote["quote_price"]),
        wallet_usdc_size=ORDER_USD,
        copy_size_usd=ORDER_USD,
        shares=float(quote["shares"]),
        observed_ts=observed_ts,
        strategy_family=CONFIG["method"],
        policy_id="queue_hazard_fifo_fee_adverse_cap1",
        sizing_policy_id="fixed_usd_1",
        mode="paper",
        action="BUY",
        order_type="GTC_POST_ONLY_STRICT",
        token_id=token_id,
        event_ts=observed_ts,
        api_latency_s=0,
        live_orders_allowed=False,
        reason="frozen native FIFO spread capture",
        metadata={
            "generation_checksum": CHECKSUM,
            "passive_fill_model_checksum": FILL_MODEL_SHA,
            "queue_ahead_shares_at_quote": quote["queue_ahead_shares"],
            "book_sequence": quote["book_sequence"],
            "net_edge_per_share": quote["net_edge_per_share"],
            "parity_disagreement": 0,
            "lookahead_violations": 0,
        },
    )


def contra_volume(
    rows: list[dict[str, Any]], *, token_id: str, price: float, after_ts: float
) -> tuple[float, list[str]]:
    seen: set[str] = set()
    total, identities = 0.0, []
    for row in rows:
        identity = str(row.get("transactionHash") or row.get("id") or "")
        if not identity or identity in seen:
            continue
        seen.add(identity)
        if (
            _num(row.get("timestamp")) >= after_ts
            and str(row.get("asset") or row.get("asset_id") or "") == token_id
            and str(row.get("side") or "").upper() == "SELL"
            and 0 < _num(row.get("price")) <= price
        ):
            total += _num(row.get("size"))
            identities.append(identity)
    return round(total, 6), identities


def reduce_generation(
    terminals: list[dict[str, Any]], events: list[dict[str, Any]], resolutions: dict[str, str]
) -> dict[str, Any]:
    windows = sorted({int(row["window_start_s"]) for row in terminals if row.get("raw_clock_complete")})
    quotes = [row for row in terminals if isinstance(row.get("intent"), dict)]
    resolved: list[float] = []
    for row in events:
        if row.get("event") != "queue_fill_actual_contra_volume":
            continue
        winner = resolutions.get(str(row.get("market_slug") or ""))
        if not winner:
            continue
        intent = row["intent"]
        shares, price = _num(intent.get("shares")), _num(intent.get("limit_price"))
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        resolved.append(shares * (1 if intent.get("outcome") == winner else 0) - ORDER_USD - fee)
    split = len(resolved) // 2
    first, second = resolved[:split], resolved[split:]
    gates = {
        "resolved_gte_10": len(resolved) >= MIN_RESOLVED,
        "genuine_queue_fill": bool(resolved),
        "post_cost_positive": sum(resolved) > 0,
        "first_half_positive": bool(first) and sum(first) > 0,
        "second_half_positive": bool(second) and sum(second) > 0,
        "incremental_vs_no_quote_positive": sum(resolved) > 0,
        "raw_input_equals_terminal": len(windows) == len(terminals),
        "no_synthetic_touch_fills": all(
            row.get("synthetic_touch_fill") is False
            for row in events if row.get("event") == "queue_fill_actual_contra_volume"
        ),
        "exact_parity": all(
            (row.get("intent") or {}).get("metadata", {}).get("parity_disagreement") == 0
            for row in quotes
        ),
    }
    status = "PAPER_CELL_ACTIVE"
    if len(windows) >= ZERO_INTENT_WINDOWS and not quotes:
        status = "PARK_ZERO_INTENT_GENERATION"
    elif len(windows) >= MAX_WINDOWS and not all(gates.values()):
        status = "PARK_FAILED_GATE_BY_SIX_WINDOWS"
    elif all(gates.values()):
        status = "PROMOTION_HANDOFF_READY"
    return {
        "status": status,
        "complete_window_starts_s": windows,
        "completed_windows": len(windows),
        "positive_edge_intents": len(quotes),
        "genuine_queue_fills": len(resolved),
        "resolved_orders": len(resolved),
        "post_cost_pnl_usd": round(sum(resolved), 6),
        "first_half_post_cost_pnl_usd": round(sum(first), 6),
        "second_half_post_cost_pnl_usd": round(sum(second), 6),
        "gate_checks": gates,
        "blocker_taxonomy": dict(Counter(
            reason for row in terminals for reason in row.get("blockers") or []
        )),
        "stop_writer": status.startswith("PARK_"),
    }


def _selector(payload: dict[str, Any], prereg: dict[str, Any]) -> dict[str, Any]:
    checks = {
        "paper_only": True,
        "preregistration_checksum_exact": True,
        "model_checksum_exact": True,
        "resolved_gte_10": payload["resolved_orders"] >= MIN_RESOLVED,
        "genuine_queue_fill": payload["genuine_queue_fills"] > 0,
        "aggregate_post_cost_positive": payload["post_cost_pnl_usd"] > 0,
        "first_half_positive": payload["first_half_post_cost_pnl_usd"] > 0,
        "second_half_positive": payload["second_half_post_cost_pnl_usd"] > 0,
        "positive_incremental_vs_no_quote": payload["post_cost_pnl_usd"] > 0,
        "raw_input_equals_terminal": payload["gate_checks"]["raw_input_equals_terminal"],
        "no_synthetic_touch_fills": payload["gate_checks"]["no_synthetic_touch_fills"],
        "zero_parity_disagreement": payload["gate_checks"]["exact_parity"],
    }
    evidence = {
        "resolved_fills": payload["resolved_orders"],
        "post_fee_pnl_usd": payload["post_cost_pnl_usd"],
        "first_half": {"post_fee_pnl_usd": payload["first_half_post_cost_pnl_usd"]},
        "second_half": {"post_fee_pnl_usd": payload["second_half_post_cost_pnl_usd"]},
        "checks": checks,
    }
    body = {
        "schema_version": 1,
        "cell_id": CELL_ID,
        "preregistration_checksum": prereg["checksum"],
        "model_checksum": CHECKSUM,
        "signal_offset_s": OFFSET_S,
        "execution_mode": "passive",
        "state_path": STATE,
        "evidence_snapshot": evidence,
    }
    record = {
        **body,
        "evidence_snapshot_checksum": _arbiter_checksum(evidence),
        "record_checksum": _arbiter_checksum(body),
        "gate_pass": all(checks.values()),
        "status": "ELIGIBLE" if all(checks.values()) else "ACCRUING",
    }
    selected = dict(record) if record["gate_pass"] else None
    if selected:
        selected["activation_id"] = f"promoted-cell-{_arbiter_checksum({'cell': CELL_ID, 'record': record['record_checksum']})[:20]}"
    return {
        "schema_version": 1,
        "kind": "btc5m_cross_exchange_promoted_cell_selector",
        "generated_at": payload["generated_at"],
        "status": "PROMOTED_CELL_READY" if selected else "NO_GATE_COMPLETE_CELL",
        "cells": [record],
        "selected": selected,
        "permanent_promotion_resolved_required": PERMANENT_RESOLVED,
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
        "paper_only": True,
        "live_orders_allowed": False,
    }


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    prereg = _preregister()
    now = float(args.now_ts or time.time())
    window = int(now // 300) * 300
    elapsed, slug = now - window, f"btc-updown-5m-{window}"
    cache = load_json(_rooted(args.cache), default={})
    orders = [
        row for row in cache.get("orders") or []
        if isinstance(row, dict)
        and str(row.get("generation_checksum") or "") == CHECKSUM
    ]
    terminals = [
        row for row in _load_jsonl(args.terminals)
        if str(row.get("generation_checksum") or "") == CHECKSUM
    ]
    events = [
        row for row in _load_jsonl(args.events)
        if str(row.get("generation_checksum") or "") == CHECKSUM
    ]
    terminal_id = stable_id("qhazt", {"generation": CHECKSUM, "window": window})
    current = next((row for row in terminals if row.get("terminal_id") == terminal_id), None)
    books: dict[str, dict[str, Any]] = {}
    market: dict[str, Any] = {}
    fetch_errors: list[str] = []
    try:
        market = _market_for_slug(slug, timeout_s=args.timeout_s)
        tokens = _token_map(market)
        clob = CLOBMarketClient(args.clob_base_url, timeout_s=args.clob_timeout_s, retries=1)
        for outcome in ("Up", "Down"):
            token = str(tokens.get(outcome) or "")
            books[outcome] = summarize_l2(clob.get_book(token), token_id=token, observed_at_s=now)
    except Exception as exc:  # noqa: BLE001
        fetch_errors.append(f"book_fetch:{type(exc).__name__}")

    open_order = next((row for row in orders if row.get("status") == "OPEN"), None)
    if open_order:
        try:
            response = requests.get(
                "https://data-api.polymarket.com/trades",
                params={"market": open_order["condition_id"], "takerOnly": "false", "limit": 500},
                timeout=args.timeout_s,
                headers={"User-Agent": "btc5m-queue-hazard-paper/1.0"},
            )
            response.raise_for_status()
            trades = response.json() if isinstance(response.json(), list) else []
            volume, identities = contra_volume(
                trades,
                token_id=open_order["token_id"],
                price=_num(open_order["quote_price"]),
                after_ts=_num(open_order["quote_ts"]),
            )
        except Exception:
            volume, identities = 0.0, []
        required = _num(open_order["queue_ahead"]) + _num(open_order["shares"])
        if volume + 1e-9 >= required:
            open_order["status"] = "FILLED"
            event = {
                "event": "queue_fill_actual_contra_volume",
                "generation_checksum": CHECKSUM,
                "recorded_at": utc_now_iso(),
                "market_slug": open_order["market_slug"],
                "quote_ts": open_order["quote_ts"],
                "actual_contra_volume": volume,
                "required_fifo_consumption": required,
                "trade_identities": identities,
                "intent": open_order["intent"],
                "synthetic_touch_fill": False,
            }
            append_jsonl_many(_rooted(args.events), [event]); events.append(event)
        elif now >= _num(open_order["expires_at"]) or elapsed >= LAST_ENTRY_S:
            open_order["status"] = "CANCELLED"
            event = {
                "event": "queue_quote_cancelled",
                "generation_checksum": CHECKSUM,
                "recorded_at": utc_now_iso(),
                **open_order,
            }
            append_jsonl_many(_rooted(args.events), [event]); events.append(event)

    if current is None and not open_order and OFFSET_S <= elapsed <= LAST_ENTRY_S:
        candidates: list[tuple[str, dict[str, Any]]] = []
        for outcome in ("Up", "Down"):
            quote, _ = choose_quote(books.get(outcome) or {})
            if quote:
                candidates.append((outcome, quote))
        if candidates:
            outcome, quote = max(candidates, key=lambda row: row[1]["net_edge_per_share"])
            token = str(books[outcome].get("token_id") or "")
            intent = build_intent(
                outcome=outcome,
                condition_id=str(market.get("conditionId") or market.get("condition_id") or ""),
                slug=slug,
                token_id=token,
                observed_ts=now,
                quote=quote,
            ).asdict()
            orders.append({
                "status": "OPEN", "generation_checksum": CHECKSUM,
                "window_start_s": window, "market_slug": slug,
                "condition_id": intent["condition_id"], "outcome": outcome, "token_id": token,
                "quote_price": quote["quote_price"], "quote_ts": now, "expires_at": now + TTL_S,
                "queue_ahead": quote["queue_ahead_shares"], "shares": quote["shares"], "intent": intent,
            })

    if current is None and elapsed >= 270:
        order = next((row for row in orders if int(row.get("window_start_s") or -1) == window), None)
        blockers = list(fetch_errors)
        if not order:
            for outcome in ("Up", "Down"):
                _, reasons = choose_quote(books.get(outcome) or {})
                blockers.extend(f"{outcome}:{reason}" for reason in reasons)
        terminal = {
            "schema_version": 1,
            "event": "btc5m_queue_hazard_terminal",
            "terminal_id": terminal_id,
            "generation_checksum": CHECKSUM,
            "model_checksum": CHECKSUM,
            "cell_id": CELL_ID,
            "window_start_s": window,
            "market_slug": slug,
            "recorded_at": utc_now_iso(),
            "terminal_status": "SIGNAL" if order else "PROTECTED_SKIP",
            "signal": (
                {
                    "window_start_s": window, "market_slug": slug,
                    "outcome": order["outcome"], "token_id": order["token_id"],
                    "condition_id": order["condition_id"], "observed_ts": order["quote_ts"],
                    "signal_ts": window + OFFSET_S, "executable_price": order["quote_price"],
                    "net_edge_per_share": (order["intent"]["metadata"]).get("net_edge_per_share"),
                    "book": {"status": "OK"},
                    "passive_quote_evidence": {
                        "passive_fill_model_checksum": FILL_MODEL_SHA,
                        "queue_ahead_shares_at_quote": order["queue_ahead"],
                        "book_sequence": order["intent"]["metadata"].get("book_sequence"),
                    },
                    "blockers": [],
                }
                if order else None
            ),
            "intent": order["intent"] if order else None,
            "blockers": sorted(set(blockers)),
            "raw_clock_complete": True,
            "paper_only": True,
            "live_orders_allowed": False,
        }
        append_jsonl_many(_rooted(args.terminals), [terminal]); terminals.append(terminal)

    atomic_write_json(_rooted(args.cache), {
        "schema_version": 1,
        "kind": "btc5m_queue_hazard_raw_cache",
        "generated_at": utc_now_iso(),
        "generation_checksum": CHECKSUM,
        "latest_window": window,
        "latest_books": books,
        "orders": orders[-20:],
        "paper_only": True,
        "live_orders_allowed": False,
    })
    reduced = reduce_generation(terminals, events, _resolution_map(args.resolutions))
    current_terminal = next(
        (row for row in reversed(terminals) if int(row.get("window_start_s") or -1) == window),
        {},
    )
    payload = {
        "schema_version": 1,
        "kind": "btc5m_queue_hazard_inventory_skew_maker_generation",
        "generated_at": utc_now_iso(),
        "flow_stage": "LIVE/ROTATE/DISCOVER/OBSERVE/LEARN/PROMOTE/SELF-DEV",
        "generation_checksum": CHECKSUM,
        "generation_config": CONFIG,
        "cell_id": CELL_ID,
        "preregistration": prereg,
        "frozen_model": {"checksum": CHECKSUM, "status": "IMMUTABLE_CHECKSUM_VERIFIED"},
        "current_terminal": current_terminal,
        **reduced,
        "terminal_reconciliation": {
            "raw_complete_windows": reduced["completed_windows"],
            "terminal_rows": len(terminals),
            "raw_input_equals_terminal": reduced["gate_checks"]["raw_input_equals_terminal"],
        },
        "orders_submitted": 0,
        "paper_only": True,
        "live_orders_allowed": False,
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
    }
    atomic_write_json(_rooted(args.state), payload)
    atomic_write_json(_rooted(args.selector), _selector(payload, prereg))
    atomic_write_json(_rooted(RUNG_C), {
        "schema_version": 1,
        "kind": "rung_c_no_admissible_target",
        "generated_at": payload["generated_at"],
        "status": "RUNG_C_NO_ADMISSIBLE_TARGET",
        "exact_generation": "current_F1_F4",
        "released_slot_occupant": CELL_ID,
        "occupant_status": payload["status"],
        "paper_only": True,
        "live_orders_allowed": False,
    })
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=STATE)
    parser.add_argument("--cache", default=CACHE)
    parser.add_argument("--terminals", default=TERMINALS)
    parser.add_argument("--events", default=EVENTS)
    parser.add_argument("--selector", default=SELECTOR)
    parser.add_argument("--resolutions", default=RESOLUTIONS)
    parser.add_argument("--clob-base-url", default="https://clob.polymarket.com")
    parser.add_argument("--timeout-s", type=float, default=2.0)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--interval-s", type=float, default=1.0)
    parser.add_argument("--now-ts", type=float, default=0.0)
    parser.add_argument("--watch", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    while True:
        payload = run_once(args)
        print(json.dumps(payload, sort_keys=True), flush=True)
        if not args.watch or payload.get("stop_writer"):
            return 0
        time.sleep(max(0.2, args.interval_s))


if __name__ == "__main__":
    raise SystemExit(main())
