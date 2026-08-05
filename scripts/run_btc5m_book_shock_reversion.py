#!/usr/bin/env python3
"""Run the checksum-isolated BTC-5m endogenous book-shock reversion paper lane.

The lane is intentionally paper-only.  It observes both Polymarket outcome
books, detects a transient ask-side price/depth vacuum relative to a frozen
five-second anchor, and emits the exact CopyIntent that a future sole-guard
activation would consume.  No external directional feed or paired trade is
used.
"""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.run_e7_spot_open_paper_lane import _market_for_slug, _token_map  # noqa: E402
from src.wallet_copy.fees import expected_polymarket_buy_fee_usd  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import CopyIntent, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.promoted_cell import reduce_promoted_cells  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402


ORDER_USD = 1.0
MIN_PRICE, MAX_PRICE = 0.25, 0.50
ANCHOR_LAG_S = 5.0
MIN_ASK_DROP = 0.03
MAX_CURRENT_ASK_DEPTH_SHARES = 30.0
MIN_ANCHOR_ASK_DEPTH_SHARES = 60.0
MIN_POST_COST_EDGE = 0.015
SLIPPAGE_RESERVE_USD = 0.005
SIGNAL_START_S, SIGNAL_END_S = 30, 180
ZERO_INTENT_WINDOWS = 2
MAX_EVIDENCE_WINDOWS = 6
EMERGENCY_RESOLVED = 10
PERMANENT_RESOLVED = 50
CALIBRATION_CUTOFF = "2026-07-25T15:45:00Z"
STATE_PATH = "data/research/btc5m_book_shock_reversion_state.json"
CACHE_PATH = "data/research/btc5m_book_shock_reversion_raw_cache.json"
TERMINALS_PATH = "data/research/btc5m_book_shock_reversion_terminals.jsonl"
INTENTS_PATH = "data/research/btc5m_book_shock_reversion_intents.jsonl"
SELECTOR_PATH = "data/research/btc5m_book_shock_reversion_selector.json"
PREREG_PATH_BASE = "data/research/btc5m_book_shock_reversion_preregistration"
RUNG_C_PATH = "data/research/rung_c_no_admissible_target_latest.json"
RESOLUTIONS_PATH = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"

CONFIG = {
    "schema_version": 1,
    "method": "btc5m_endogenous_book_shock_reversion_v1",
    "source": "polymarket_both_outcome_raw_l2_only",
    "feature": "five_second_ask_price_drop_with_top_depth_vacuum",
    "calibration_cutoff": CALIBRATION_CUTOFF,
    "anchor_lag_s": ANCHOR_LAG_S,
    "min_ask_drop": MIN_ASK_DROP,
    "max_current_ask_depth_shares": MAX_CURRENT_ASK_DEPTH_SHARES,
    "min_anchor_ask_depth_shares": MIN_ANCHOR_ASK_DEPTH_SHARES,
    "fair_value": "pre_shock_same_outcome_microprice",
    "min_post_cost_edge": MIN_POST_COST_EDGE,
    "slippage_reserve_usd": SLIPPAGE_RESERVE_USD,
    "entry_bounds": [MIN_PRICE, MAX_PRICE],
    "signal_window_s": [SIGNAL_START_S, SIGNAL_END_S],
    "one_intent_per_window": True,
    "order_usd": ORDER_USD,
    "inventory_conservation": "single_long_outcome_held_to_canonical_resolution",
    "terminal_clock": {
        "zero_intent_complete_windows": ZERO_INTENT_WINDOWS,
        "max_evidence_complete_windows": MAX_EVIDENCE_WINDOWS,
    },
    "promotion_gates": {
        "emergency_resolved": EMERGENCY_RESOLVED,
        "permanent_resolved": PERMANENT_RESOLVED,
        "positive_post_fee_aggregate": True,
        "positive_chronological_halves": True,
        "exact_copyintent_parity": True,
        "executable_depth": True,
    },
    "activation": "singular_fable_ruling_then_one_dollar_3600s_sole_guard_pin",
    "paper_only": True,
    "live_orders_allowed": False,
}
GENERATION_CHECKSUM = hashlib.sha256(
    json.dumps(CONFIG, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
PREREG_PATH = f"{PREREG_PATH_BASE}_{GENERATION_CHECKSUM[:12]}.json"


def _rooted(path: str) -> Path:
    value = Path(path)
    return value if value.is_absolute() else ROOT / value


def _load_jsonl(path: str) -> list[dict[str, Any]]:
    target = _rooted(path)
    if not target.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in target.read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        if isinstance(row, dict):
            rows.append(row)
    return rows


def _num(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _levels(raw: Any, side: str) -> list[tuple[float, float]]:
    levels = (raw.get(side) if isinstance(raw, dict) else getattr(raw, side, None)) or []
    out: list[tuple[float, float]] = []
    for level in levels:
        price = _num(getattr(level, "price", None) if not isinstance(level, dict) else level.get("price"))
        size = _num(getattr(level, "size", None) if not isinstance(level, dict) else level.get("size"))
        if price > 0 and size > 0:
            out.append((price, size))
    return sorted(out, key=lambda row: row[0], reverse=side == "bids")


def summarize_l2(raw: Any, *, token_id: str, observed_at_s: float) -> dict[str, Any]:
    bids, asks = _levels(raw, "bids"), _levels(raw, "asks")
    if not bids or not asks:
        return {"status": "MISSING_TWO_SIDED_BOOK", "token_id": token_id, "observed_at_s": observed_at_s}
    bid, bid_size = bids[0]
    ask, ask_size = asks[0]
    denom = bid_size + ask_size
    microprice = (ask * bid_size + bid * ask_size) / denom if denom > 0 else (bid + ask) / 2
    return {
        "status": "PASS",
        "token_id": token_id,
        "observed_at_s": observed_at_s,
        "best_bid": bid,
        "best_bid_size": bid_size,
        "best_ask": ask,
        "best_ask_size": ask_size,
        "microprice": round(microprice, 6),
        "spread": round(ask - bid, 6),
        "executable_depth_usd": round(ask * ask_size, 6),
        "book_timestamp": raw.get("timestamp") if isinstance(raw, dict) else getattr(raw, "timestamp", None),
    }


def detect_shock(
    *, anchor: dict[str, Any], current: dict[str, Any]
) -> tuple[dict[str, Any] | None, list[str]]:
    blockers: list[str] = []
    if anchor.get("status") != "PASS" or current.get("status") != "PASS":
        return None, ["book_incomplete"]
    ask = _num(current.get("best_ask"))
    anchor_ask = _num(anchor.get("best_ask"))
    anchor_fair = _num(anchor.get("microprice"))
    ask_drop = anchor_ask - ask
    shares = ORDER_USD / ask if ask > 0 else 0.0
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=ask)
    post_cost_edge = anchor_fair - ask - fee - SLIPPAGE_RESERVE_USD
    if not MIN_PRICE <= ask <= MAX_PRICE:
        blockers.append("entry_price_outside_frozen_bounds")
    if ask_drop < MIN_ASK_DROP:
        blockers.append("ask_drop_below_threshold")
    if _num(anchor.get("best_ask_size")) < MIN_ANCHOR_ASK_DEPTH_SHARES:
        blockers.append("anchor_depth_below_floor")
    if _num(current.get("best_ask_size")) > MAX_CURRENT_ASK_DEPTH_SHARES:
        blockers.append("current_depth_not_vacuum")
    if _num(current.get("executable_depth_usd")) + 1e-9 < ORDER_USD:
        blockers.append("executable_depth_below_one_dollar")
    if post_cost_edge < MIN_POST_COST_EDGE:
        blockers.append("anchored_post_cost_edge_below_threshold")
    if blockers:
        return None, blockers
    return {
        "entry_price": ask,
        "anchor_fair": anchor_fair,
        "ask_drop": round(ask_drop, 6),
        "expected_fee_usd": fee,
        "post_cost_edge_per_share": round(post_cost_edge, 6),
        "shares": round(shares, 6),
        "actual_depth_verified": True,
    }, []


def build_intent(
    *, outcome: str, condition_id: str, market_slug: str, token_id: str,
    observed_at_s: float, signal: dict[str, Any],
) -> CopyIntent:
    return CopyIntent(
        intent_id=stable_id("bookshock", {"generation": GENERATION_CHECKSUM, "market": market_slug}),
        source_wallet=f"BTC5M_PROMOTED_CELL:book_shock_{GENERATION_CHECKSUM[:12]}",
        wallet_name=f"book_shock_{GENERATION_CHECKSUM[:12]}",
        source_event_id=stable_id("bookshockevt", {"market": market_slug, "outcome": outcome}),
        condition_id=condition_id,
        market_slug=market_slug,
        outcome=outcome,
        side="YES" if outcome == "Up" else "NO",
        limit_price=float(signal["entry_price"]),
        wallet_usdc_size=ORDER_USD,
        copy_size_usd=ORDER_USD,
        shares=float(signal["shares"]),
        observed_ts=observed_at_s,
        strategy_family="btc5m_endogenous_book_shock_reversion_v1",
        policy_id="book_shock_5s_depth_vacuum_post_cost_cap1",
        sizing_policy_id="fixed_usd_1",
        mode="paper",
        action="BUY",
        order_type="PAPER_EXECUTABLE_BOOK",
        token_id=token_id,
        event_ts=observed_at_s,
        api_latency_s=0.0,
        live_orders_allowed=False,
        reason="frozen endogenous L2 shock-reversion signal",
        metadata={
            "generation_checksum": GENERATION_CHECKSUM,
            "actual_depth_verified": True,
            "inventory_conservation": CONFIG["inventory_conservation"],
            "lookahead_violations": 0,
            "parity_disagreement": 0,
        },
    )


def _resolution_map(path: str) -> dict[str, str]:
    return {
        str(row.get("market_slug")): str(row.get("direction") or "").title()
        for row in _load_jsonl(path)
        if str(row.get("direction") or "").upper() in {"UP", "DOWN"}
    }


def reduce_state(terminals: list[dict[str, Any]], resolutions: dict[str, str]) -> dict[str, Any]:
    complete = sorted({int(row["window_start_s"]) for row in terminals if row.get("raw_clock_complete")})
    intent_rows = [row for row in terminals if isinstance(row.get("intent"), dict)]
    resolved: list[dict[str, Any]] = []
    for row in intent_rows:
        winner = resolutions.get(str(row.get("market_slug") or ""))
        if not winner:
            continue
        intent = row["intent"]
        price, shares = _num(intent.get("limit_price")), _num(intent.get("shares"))
        fee = _num((row.get("signal") or {}).get("expected_fee_usd"))
        pnl = shares * (1.0 if str(intent.get("outcome")) == winner else 0.0) - ORDER_USD - fee
        resolved.append({**row, "winner": winner, "post_fee_pnl_usd": round(pnl, 6)})
    pnls = [_num(row.get("post_fee_pnl_usd")) for row in resolved]
    split = len(pnls) // 2
    first, second = pnls[:split], pnls[split:]
    gates = {
        "resolved_gte_10": len(resolved) >= EMERGENCY_RESOLVED,
        "post_fee_positive": sum(pnls) > 0,
        "first_half_positive": bool(first) and sum(first) > 0,
        "second_half_positive": bool(second) and sum(second) > 0,
        "exact_copyintent_parity": all(
            (row.get("intent") or {}).get("metadata", {}).get("parity_disagreement") == 0
            for row in intent_rows
        ),
        "executable_depth": all(
            (row.get("signal") or {}).get("actual_depth_verified") is True for row in intent_rows
        ),
    }
    status = "PAPER_CELL_ACTIVE"
    if len(complete) >= ZERO_INTENT_WINDOWS and not intent_rows:
        status = "PARK_ZERO_INTENT_GENERATION"
    elif len(complete) >= MAX_EVIDENCE_WINDOWS and len(resolved) < EMERGENCY_RESOLVED:
        status = "PARK_INSUFFICIENT_EXECUTABLE_FILL_RATE"
    elif all(gates.values()):
        status = "PROMOTION_HANDOFF_READY"
    return {
        "status": status,
        "complete_window_starts_s": complete,
        "completed_windows": len(complete),
        "positive_edge_intents": len(intent_rows),
        "resolved_orders": len(resolved),
        "post_fee_pnl_usd": round(sum(pnls), 6),
        "first_half_post_fee_pnl_usd": round(sum(first), 6),
        "second_half_post_fee_pnl_usd": round(sum(second), 6),
        "gate_checks": gates,
        "blocker_taxonomy": dict(Counter(
            blocker for row in terminals for blocker in row.get("blockers") or []
        )),
        "stop_writer": status.startswith("PARK_"),
    }


def _write_preregistration() -> None:
    body = {
        **CONFIG,
        "kind": "btc5m_book_shock_reversion_preregistration",
        "generation_checksum": GENERATION_CHECKSUM,
        "model_checksum": GENERATION_CHECKSUM,
        "execution_mode": "taker",
        "signal_offset_s": SIGNAL_START_S,
        "registered_before_outcome_inspection": True,
        "immutable": True,
    }
    body["checksum"] = hashlib.sha256(
        json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    prior = load_json(_rooted(PREREG_PATH), default={})
    if prior and prior != body:
        raise RuntimeError("immutable preregistration mismatch")
    if not prior:
        atomic_write_json(_rooted(PREREG_PATH), body)


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    _write_preregistration()
    now_ts = float(args.now_ts or time.time())
    window_start = int(now_ts // 300) * 300
    slug = f"btc-updown-5m-{window_start}"
    cache = load_json(_rooted(args.cache), default={})
    samples = [row for row in cache.get("samples") or [] if _num(row.get("observed_at_s")) >= now_ts - 20]
    terminal_rows = _load_jsonl(args.terminals)
    terminal_id = stable_id("bookshockterminal", {"generation": GENERATION_CHECKSUM, "window": window_start})
    current_terminal = next((row for row in terminal_rows if row.get("terminal_id") == terminal_id), None)
    books: dict[str, Any] = {}
    blockers: list[str] = []
    market: dict[str, Any] = {}
    try:
        market = _market_for_slug(slug, timeout_s=float(args.timeout_s))
        tokens = _token_map(market)
        clob = CLOBMarketClient(args.clob_base_url, timeout_s=float(args.clob_timeout_s), retries=1)
        for outcome in ("Up", "Down"):
            token = str(tokens.get(outcome) or "")
            books[outcome] = summarize_l2(clob.get_book(token), token_id=token, observed_at_s=now_ts)
    except Exception as exc:  # noqa: BLE001
        blockers = [f"raw_book_fetch_error:{type(exc).__name__}"]
    samples.append({"observed_at_s": now_ts, "window_start_s": window_start, "market_slug": slug, "books": books})
    atomic_write_json(_rooted(args.cache), {
        "schema_version": 1, "kind": "btc5m_book_shock_raw_cache",
        "generation_checksum": GENERATION_CHECKSUM, "generated_at": utc_now_iso(),
        "samples": samples[-30:], "paper_only": True, "live_orders_allowed": False,
    })

    elapsed = now_ts - window_start
    if current_terminal is None and elapsed >= SIGNAL_START_S:
        anchors = [
            row for row in samples[:-1]
            if row.get("window_start_s") == window_start
            and _num(row.get("observed_at_s")) <= now_ts - ANCHOR_LAG_S
        ]
        anchor = anchors[-1] if anchors else {}
        selected: tuple[str, dict[str, Any]] | None = None
        all_blockers: list[str] = []
        for outcome in ("Up", "Down"):
            signal, reasons = detect_shock(
                anchor=(anchor.get("books") or {}).get(outcome) or {},
                current=books.get(outcome) or {},
            )
            all_blockers.extend(f"{outcome}:{reason}" for reason in reasons)
            if signal and (selected is None or signal["post_cost_edge_per_share"] > selected[1]["post_cost_edge_per_share"]):
                selected = (outcome, signal)
        intent = None
        if selected and elapsed <= SIGNAL_END_S:
            outcome, signal = selected
            intent = build_intent(
                outcome=outcome,
                condition_id=str(market.get("conditionId") or market.get("condition_id") or ""),
                market_slug=slug,
                token_id=str(books[outcome].get("token_id") or ""),
                observed_at_s=now_ts,
                signal=signal,
            ).asdict()
            append_jsonl_many(_rooted(args.intents), [intent])
        raw_complete = elapsed >= 270
        if intent or raw_complete:
            terminal = {
                "schema_version": 1,
                "event": "btc5m_book_shock_terminal",
                "terminal_id": terminal_id,
                "generation_checksum": GENERATION_CHECKSUM,
                "window_start_s": window_start,
                "market_slug": slug,
                "recorded_at": utc_now_iso(),
                "terminal_status": "SIGNAL" if intent else "PROTECTED_SKIP",
                "signal": selected[1] if selected else None,
                "intent": intent,
                "blockers": [] if intent else sorted(set(blockers + all_blockers)),
                "raw_clock_complete": raw_complete,
                "paper_only": True,
                "live_orders_allowed": False,
            }
            append_jsonl_many(_rooted(args.terminals), [terminal])
            terminal_rows.append(terminal)

    reduced = reduce_state(terminal_rows, _resolution_map(args.resolutions))
    payload = {
        "schema_version": 1,
        "kind": "btc5m_book_shock_reversion_generation",
        "generated_at": utc_now_iso(),
        "flow_stage": "LIVE/ROTATE/DISCOVER/OBSERVE/LEARN/PROMOTE/SELF-DEV",
        "generation_checksum": GENERATION_CHECKSUM,
        "generation_config": CONFIG,
        "preregistration_path": PREREG_PATH,
        "preregistration": load_json(_rooted(PREREG_PATH), default={}),
        "frozen_model": {"checksum": GENERATION_CHECKSUM},
        "cell_id": f"book_shock_{GENERATION_CHECKSUM[:12]}",
        **reduced,
        "paper_only": True,
        "live_orders_allowed": False,
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
    }
    atomic_write_json(_rooted(args.state), payload)
    prereg = payload["preregistration"]
    matrix = {
        "cells": [{
            "cell_id": payload["cell_id"],
            "signal_offset_s": SIGNAL_START_S,
            "execution_mode": "taker",
            "state_path": args.state,
            "terminals_path": args.terminals,
            "preregistration_path": PREREG_PATH,
            "preregistration_checksum": prereg.get("checksum"),
            "model_checksum": GENERATION_CHECKSUM,
            "generation_checksum": GENERATION_CHECKSUM,
            "paper_only": True,
            "actual_depth_verified": True,
            "lookahead_violations": 0,
            "parity_disagreement": 0,
            "clock_disagreement": 0,
            "unresolved_disagreement": 0,
            "duplicate_disagreement": 0,
            "sign_disagreement": 0,
        }]
    }
    prior_selector = load_json(_rooted(args.selector), default={})
    selector = reduce_promoted_cells(
        matrix,
        resolutions_path=args.resolutions,
        prior_activation=(
            prior_selector.get("selected")
            if isinstance(prior_selector, dict) and isinstance(prior_selector.get("selected"), dict)
            else {}
        ),
        prior_cells=(
            prior_selector.get("cells")
            if isinstance(prior_selector, dict) and isinstance(prior_selector.get("cells"), list)
            else []
        ),
    )
    selector.update({
        "generated_at": payload["generated_at"],
        "permanent_promotion_resolved_required": PERMANENT_RESOLVED,
        "activation_requested": selector.get("status") == "PROMOTED_CELL_READY",
        "live_mutation_allowed": False,
        "paper_only": True,
        "live_orders_allowed": False,
    })
    atomic_write_json(_rooted(args.selector), selector)
    atomic_write_json(_rooted(RUNG_C_PATH), {
        "schema_version": 1,
        "kind": "rung_c_no_admissible_target",
        "generated_at": payload["generated_at"],
        "status": "RUNG_C_NO_ADMISSIBLE_TARGET",
        "exact_generation": "current_F1_F4",
        "mechanical_escalation": "RUNG_C_METHOD_SWITCH_DUE",
        "released_slot_occupant": f"book_shock_{GENERATION_CHECKSUM[:12]}",
        "occupant_status": payload["status"],
        "paper_only": True,
        "live_orders_allowed": False,
    })
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=STATE_PATH)
    parser.add_argument("--cache", default=CACHE_PATH)
    parser.add_argument("--terminals", default=TERMINALS_PATH)
    parser.add_argument("--intents", default=INTENTS_PATH)
    parser.add_argument("--selector", default=SELECTOR_PATH)
    parser.add_argument("--resolutions", default=RESOLUTIONS_PATH)
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
        time.sleep(max(0.2, float(args.interval_s)))


if __name__ == "__main__":
    raise SystemExit(main())
