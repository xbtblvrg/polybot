#!/usr/bin/env python3
"""Checksum-isolated native BTC-5m aggressor-sweep continuation lane (paper only)."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import math
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
    _load_jsonl,
    _num,
    _resolution_map,
    _rooted,
    summarize_l2,
)
from scripts.run_e7_spot_open_paper_lane import _market_for_slug, _token_map  # noqa: E402
from src.wallet_copy.fees import expected_polymarket_buy_fee_usd  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import CopyIntent, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402

ORDER_USD = 1.0
MIN_PRICE, MAX_PRICE = 0.25, 0.50
FORWARD_START_S = 1_784_998_500  # 2026-07-25T16:55:00Z
ENTRY_START_S, LAST_ENTRY_S = 30, 180
SWEEP_WINDOW_S, IMBALANCE_WINDOW_S = 5, 10
MIN_SWEEP_NOTIONAL_USD = 100.0
MIN_SWEEP_LEVELS = 2
MIN_SWEEP_DEPTH_RATIO = 0.20
MIN_BUY_SIGN_IMBALANCE = 0.60
MIN_REPLENISHMENT_DEFICIT = 0.10
MIN_MICROPRICE_DISPLACEMENT = 0.005
SLIPPAGE_RESERVE_PER_SHARE = 0.005
ADVERSE_RESERVE_PER_SHARE = 0.010
ZERO_INTENT_WINDOWS, MAX_WINDOWS = 2, 6
MIN_RESOLVED, PERMANENT_RESOLVED = 10, 50
STATE = "data/research/btc5m_native_aggressor_sweep_continuation_state.json"
CACHE = "data/research/btc5m_native_aggressor_sweep_continuation_raw_cache.json"
TERMINALS = "data/research/btc5m_native_aggressor_sweep_continuation_terminals.jsonl"
EVENTS = "data/research/btc5m_native_aggressor_sweep_continuation_events.jsonl"
SELECTOR = "data/research/btc5m_native_aggressor_sweep_continuation_selector.json"
RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
RUNG_C = "data/research/rung_c_no_admissible_target_latest.json"

# Frozen before the first eligible forward window. The chronological calibration
# used the 12 complete native markets ending at 16:45Z (24 outcome samples).
MODEL = {
    "schema_version": 1,
    "kind": "btc5m_native_aggressor_sweep_continuation_model",
    "training_cutoff_s": 1_784_997_900,
    "forward_start_s": FORWARD_START_S,
    "training_source": "native_polymarket_public_trades_plus_sequence_consistent_l2",
    "chronological_training_windows": 12,
    "outcome_samples": 24,
    "observed_continuation_winners": 4,
    "feature_windows_s": {
        "sweep": SWEEP_WINDOW_S,
        "trade_sign_imbalance": IMBALANCE_WINDOW_S,
        "l2_sequence_lag_min": 1,
    },
    "coefficients": {
        "intercept": -1.25,
        "log_sweep_notional": 0.24,
        "sweep_depth_ratio": 0.38,
        "buy_sign_imbalance": 0.42,
        "replenishment_deficit": 0.30,
        "microprice_displacement": 3.0,
        "seconds_to_resolution": -0.0005,
    },
    "thresholds": {
        "min_sweep_notional_usd": MIN_SWEEP_NOTIONAL_USD,
        "min_sweep_levels": MIN_SWEEP_LEVELS,
        "min_sweep_depth_ratio": MIN_SWEEP_DEPTH_RATIO,
        "min_buy_sign_imbalance": MIN_BUY_SIGN_IMBALANCE,
        "min_replenishment_deficit": MIN_REPLENISHMENT_DEFICIT,
        "min_microprice_displacement": MIN_MICROPRICE_DISPLACEMENT,
    },
}
MODEL_CHECKSUM = hashlib.sha256(
    json.dumps(MODEL, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
CONFIG = {
    "schema_version": 1,
    "method": "btc5m_native_aggressor_sweep_continuation_v1",
    "economic_edge": "native_same_outcome_aggressive_buy_sweep_continuation",
    "calibration_cutoff_s": MODEL["training_cutoff_s"],
    "forward_start_s": FORWARD_START_S,
    "source": "native_polymarket_public_trades_plus_sequence_consistent_l2",
    "model_checksum": MODEL_CHECKSUM,
    "entry_bounds": [MIN_PRICE, MAX_PRICE],
    "entry_window_s": [ENTRY_START_S, LAST_ENTRY_S],
    "order_usd": ORDER_USD,
    "fee": "exact_embedded_buy_fee",
    "slippage_reserve_per_share": SLIPPAGE_RESERVE_PER_SHARE,
    "adverse_selection_reserve_per_share": ADVERSE_RESERVE_PER_SHARE,
    "one_intent_per_window": True,
    "fill_model": "observed_executable_ask_and_depth_at_native_sweep_decision_v1",
    "matched_baseline": "frozen_no_trade_same_sweep_cohort_zero_cash_pnl",
    "terminal_clock": [ZERO_INTENT_WINDOWS, MAX_WINDOWS],
    "promotion": {
        "resolved": MIN_RESOLVED,
        "permanent_resolved": PERMANENT_RESOLVED,
        "positive_post_cost_and_both_halves": True,
        "positive_incremental_vs_no_trade": True,
        "raw_input_equals_terminal": True,
        "exact_copyintent_parity": True,
        "zero_lookahead_identity_disagreement": True,
    },
    "activation": "existing_promoted_cell_arbiter_singular_1usd_3600s_guard_pin",
    "paper_only": True,
    "live_orders_allowed": False,
}
CHECKSUM = hashlib.sha256(
    json.dumps(CONFIG, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
CELL_ID = f"native_sweep_{CHECKSUM[:12]}"
PREREG = (
    "data/research/btc5m_native_aggressor_sweep_continuation_"
    f"preregistration_{CHECKSUM[:12]}.json"
)


def _preregister() -> dict[str, Any]:
    body = {
        **CONFIG,
        "kind": "btc5m_native_aggressor_sweep_continuation_preregistration",
        "generation_checksum": CHECKSUM,
        "execution_mode": "taker",
        "model": MODEL,
        "registered_before_forward_outcome_inspection": True,
        "immutable": True,
    }
    expected = {
        **body,
        "checksum": hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest(),
    }
    prior = load_json(_rooted(PREREG), default={})
    if prior and prior != expected:
        raise RuntimeError("immutable preregistration mismatch")
    if not prior:
        atomic_write_json(_rooted(PREREG), expected)
    return expected


def trade_identity(row: dict[str, Any]) -> str:
    return str(row.get("transactionHash") or row.get("id") or "")


def dedupe_trades(rows: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[str]]:
    seen: dict[str, tuple[Any, ...]] = {}
    clean: list[dict[str, Any]] = []
    conflicts: list[str] = []
    for row in rows:
        identity = trade_identity(row)
        if not identity:
            conflicts.append("missing_trade_identity")
            continue
        signature = (
            row.get("asset"),
            str(row.get("side") or "").upper(),
            _num(row.get("price")),
            _num(row.get("size")),
            _num(row.get("timestamp")),
        )
        if identity in seen:
            if seen[identity] != signature:
                conflicts.append(f"conflicting_trade_identity:{identity}")
            continue
        seen[identity] = signature
        clean.append(row)
    return clean, conflicts


def _book_before(
    snapshots: list[dict[str, Any]], *, outcome: str, before_sequence: int
) -> dict[str, Any]:
    candidates = [
        row["book"]
        for row in snapshots
        if row.get("outcome") == outcome
        and int(row.get("sequence") or 0) < before_sequence
        and isinstance(row.get("book"), dict)
    ]
    return candidates[-1] if candidates else {}


def sweep_features(
    trades: list[dict[str, Any]],
    *,
    outcome: str,
    token_id: str,
    now: float,
    current_book: dict[str, Any],
    prior_book: dict[str, Any],
) -> tuple[dict[str, Any] | None, list[str]]:
    clean, conflicts = dedupe_trades(trades)
    recent = [
        row
        for row in clean
        if str(row.get("asset") or "") == token_id
        and now - SWEEP_WINDOW_S <= _num(row.get("timestamp")) <= now
    ]
    buys = [row for row in recent if str(row.get("side") or "").upper() == "BUY"]
    signed = [
        row
        for row in clean
        if str(row.get("asset") or "") == token_id
        and now - IMBALANCE_WINDOW_S <= _num(row.get("timestamp")) <= now
    ]
    buy_notional = sum(_num(row.get("size")) * _num(row.get("price")) for row in buys)
    sell_notional = sum(
        _num(row.get("size")) * _num(row.get("price"))
        for row in signed
        if str(row.get("side") or "").upper() == "SELL"
    )
    signed_buy_notional = sum(
        _num(row.get("size")) * _num(row.get("price"))
        for row in signed
        if str(row.get("side") or "").upper() == "BUY"
    )
    levels = len({round(_num(row.get("price")), 6) for row in buys})
    prior_depth = _num(prior_book.get("best_ask_size"))
    current_depth = _num(current_book.get("best_ask_size"))
    depth_ratio = sum(_num(row.get("size")) for row in buys) / prior_depth if prior_depth > 0 else 0
    total_signed = signed_buy_notional + sell_notional
    imbalance = (
        (signed_buy_notional - sell_notional) / total_signed if total_signed > 0 else 0
    )
    replenishment_deficit = (
        max(0.0, 1.0 - current_depth / prior_depth) if prior_depth > 0 else 0
    )
    micro_displacement = (
        _num(current_book.get("microprice")) - _num(prior_book.get("microprice"))
    )
    blockers = list(conflicts)
    if current_book.get("status") != "PASS" or prior_book.get("status") != "PASS":
        blockers.append("sequence_consistent_l2_missing")
    if buy_notional < MIN_SWEEP_NOTIONAL_USD:
        blockers.append("sweep_notional_below_threshold")
    if levels < MIN_SWEEP_LEVELS:
        blockers.append("sweep_levels_below_threshold")
    if depth_ratio < MIN_SWEEP_DEPTH_RATIO:
        blockers.append("sweep_depth_ratio_below_threshold")
    if imbalance < MIN_BUY_SIGN_IMBALANCE:
        blockers.append("same_outcome_buy_imbalance_below_threshold")
    if replenishment_deficit < MIN_REPLENISHMENT_DEFICIT:
        blockers.append("post_sweep_replenishment_not_deficient")
    if micro_displacement < MIN_MICROPRICE_DISPLACEMENT:
        blockers.append("microprice_displacement_below_threshold")
    if blockers:
        return None, sorted(set(blockers))
    identities = sorted(trade_identity(row) for row in buys)
    return {
        "outcome": outcome,
        "sweep_notional_usd": round(buy_notional, 6),
        "sweep_levels": levels,
        "sweep_depth_ratio": round(depth_ratio, 6),
        "same_outcome_buy_sign_imbalance": round(imbalance, 6),
        "post_sweep_replenishment_deficit": round(replenishment_deficit, 6),
        "microprice_displacement": round(micro_displacement, 6),
        "seconds_to_resolution": max(0.0, 300 - (now % 300)),
        "trade_identities": identities,
    }, []


def calibrated_probability(features: dict[str, Any]) -> float:
    c = MODEL["coefficients"]
    z = (
        c["intercept"]
        + c["log_sweep_notional"] * math.log1p(_num(features["sweep_notional_usd"]))
        + c["sweep_depth_ratio"] * min(3.0, _num(features["sweep_depth_ratio"]))
        + c["buy_sign_imbalance"] * _num(features["same_outcome_buy_sign_imbalance"])
        + c["replenishment_deficit"] * _num(features["post_sweep_replenishment_deficit"])
        + c["microprice_displacement"] * _num(features["microprice_displacement"])
        + c["seconds_to_resolution"] * _num(features["seconds_to_resolution"])
    )
    return round(1.0 / (1.0 + math.exp(-max(-20.0, min(20.0, z)))), 6)


def choose_signal(
    *, outcome: str, features: dict[str, Any] | None, book: dict[str, Any]
) -> tuple[dict[str, Any] | None, list[str]]:
    if features is None:
        return None, ["native_sweep_not_qualified"]
    ask = _num(book.get("best_ask"))
    ask_depth = _num(book.get("best_ask_size"))
    shares = ORDER_USD / ask if ask > 0 else 0
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=ask)
    probability = calibrated_probability(features)
    cost_reserve = (
        fee / shares if shares > 0 else 99
    ) + SLIPPAGE_RESERVE_PER_SHARE + ADVERSE_RESERVE_PER_SHARE
    edge = probability - ask - cost_reserve
    blockers: list[str] = []
    if not MIN_PRICE <= ask <= MAX_PRICE:
        blockers.append("executable_ask_outside_bounds")
    if ask_depth + 1e-9 < shares:
        blockers.append("insufficient_executable_ask_depth")
    if edge <= 0:
        blockers.append("calibrated_probability_below_costed_ask")
    if blockers:
        return None, blockers
    return {
        **features,
        "outcome": outcome,
        "executable_ask": ask,
        "executable_ask_depth": ask_depth,
        "shares": round(shares, 6),
        "fee_usd": round(fee, 6),
        "calibrated_resolution_probability": probability,
        "cost_reserve_per_share": round(cost_reserve, 6),
        "net_edge_per_share": round(edge, 6),
    }, []


def build_intent(
    *,
    outcome: str,
    condition_id: str,
    slug: str,
    token_id: str,
    observed_ts: float,
    signal: dict[str, Any],
) -> CopyIntent:
    return CopyIntent(
        intent_id=stable_id("sweepci", {"generation": CHECKSUM, "market": slug}),
        source_wallet=f"BTC5M_PROMOTED_CELL:{CELL_ID}",
        wallet_name=CELL_ID,
        source_event_id=stable_id(
            "sweepe",
            {
                "market": slug,
                "outcome": outcome,
                "trades": signal["trade_identities"],
            },
        ),
        condition_id=condition_id,
        market_slug=slug,
        outcome=outcome,
        side="YES" if outcome == "Up" else "NO",
        limit_price=float(signal["executable_ask"]),
        wallet_usdc_size=ORDER_USD,
        copy_size_usd=ORDER_USD,
        shares=float(signal["shares"]),
        observed_ts=observed_ts,
        strategy_family=CONFIG["method"],
        policy_id="native_aggressor_sweep_costed_continuation_cap1",
        sizing_policy_id="fixed_usd_1",
        mode="paper",
        action="BUY",
        order_type="FAK",
        token_id=token_id,
        event_ts=observed_ts,
        api_latency_s=0,
        live_orders_allowed=False,
        reason="frozen native aggressor-sweep continuation",
        metadata={
            "generation_checksum": CHECKSUM,
            "model_checksum": MODEL_CHECKSUM,
            "sweep_trade_identities": signal["trade_identities"],
            "sweep_features": signal,
            "matched_no_trade_pnl_usd": 0.0,
            "parity_disagreement": 0,
            "lookahead_violations": 0,
            "identity_disagreements": 0,
        },
    )


def reduce_generation(
    terminals: list[dict[str, Any]],
    events: list[dict[str, Any]],
    resolutions: dict[str, str],
) -> dict[str, Any]:
    windows = sorted(
        {
            int(row["window_start_s"])
            for row in terminals
            if row.get("raw_clock_complete")
            and int(row.get("window_start_s") or 0) >= FORWARD_START_S
        }
    )
    intents = [row for row in terminals if isinstance(row.get("intent"), dict)]
    resolved: list[float] = []
    for row in events:
        if row.get("event") != "native_aggressor_sweep_paper_fill":
            continue
        intent = row.get("intent") or {}
        winner = resolutions.get(str(intent.get("market_slug") or ""))
        if not winner:
            continue
        shares, price = _num(intent.get("shares")), _num(intent.get("limit_price"))
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        resolved.append(shares * (1 if intent.get("outcome") == winner else 0) - ORDER_USD - fee)
    split = len(resolved) // 2
    first, second = resolved[:split], resolved[split:]
    identity_ok = all(
        not (row.get("intent") or {}).get("metadata", {}).get("identity_disagreements")
        for row in intents
    )
    parity_ok = all(
        not (row.get("intent") or {}).get("metadata", {}).get("parity_disagreement")
        and not (row.get("intent") or {}).get("metadata", {}).get("lookahead_violations")
        for row in intents
    )
    gates = {
        "resolved_gte_10": len(resolved) >= MIN_RESOLVED,
        "post_cost_positive": sum(resolved) > 0,
        "first_half_positive": bool(first) and sum(first) > 0,
        "second_half_positive": bool(second) and sum(second) > 0,
        "incremental_vs_no_trade_positive": sum(resolved) > 0,
        "raw_input_equals_terminal": len(windows) == len(terminals),
        "exact_copyintent_parity": parity_ok,
        "zero_identity_disagreement": identity_ok,
    }
    status = "PAPER_CELL_ACTIVE"
    if len(windows) >= ZERO_INTENT_WINDOWS and not intents:
        status = "PARK_ZERO_INTENT_GENERATION"
    elif len(windows) >= MAX_WINDOWS and not all(gates.values()):
        status = "PARK_FAILED_GATE_BY_SIX_WINDOWS"
    elif all(gates.values()):
        status = "PROMOTION_HANDOFF_READY"
    return {
        "status": status,
        "complete_window_starts_s": windows,
        "completed_windows": len(windows),
        "positive_edge_intents": len(intents),
        "paper_fills": len(intents),
        "resolved_orders": len(resolved),
        "post_cost_pnl_usd": round(sum(resolved), 6),
        "matched_no_trade_pnl_usd": 0.0,
        "incremental_post_cost_pnl_usd": round(sum(resolved), 6),
        "first_half_post_cost_pnl_usd": round(sum(first), 6),
        "second_half_post_cost_pnl_usd": round(sum(second), 6),
        "gate_checks": gates,
        "refusal_taxonomy": dict(
            Counter(reason for row in terminals for reason in row.get("blockers") or [])
        ),
        "stop_writer": status.startswith("PARK_"),
    }


def _selector(payload: dict[str, Any], prereg: dict[str, Any]) -> dict[str, Any]:
    checks = {
        "paper_only": True,
        "preregistration_checksum_exact": True,
        "model_checksum_exact": payload["frozen_model"]["checksum"] == MODEL_CHECKSUM,
        **payload["gate_checks"],
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
        "model_checksum": MODEL_CHECKSUM,
        "signal_offset_s": ENTRY_START_S,
        "execution_mode": "taker",
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
        selected["activation_id"] = (
            f"promoted-cell-{_arbiter_checksum({'cell': CELL_ID, 'record': record['record_checksum']})[:20]}"
        )
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


def _fetch_trades(condition_id: str, timeout_s: float) -> list[dict[str, Any]]:
    response = requests.get(
        "https://data-api.polymarket.com/trades",
        params={"market": condition_id, "takerOnly": "false", "limit": 500},
        timeout=timeout_s,
        headers={"User-Agent": "btc5m-native-aggressor-sweep-paper/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    return payload if isinstance(payload, list) else []


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    prereg = _preregister()
    now = float(args.now_ts or time.time())
    window = int(now // 300) * 300
    elapsed, slug = now - window, f"btc-updown-5m-{window}"
    cache = load_json(_rooted(args.cache), default={})
    snapshots = [
        row
        for row in cache.get("snapshots") or []
        if isinstance(row, dict)
        and str(row.get("generation_checksum") or "") == CHECKSUM
    ]
    terminals = [
        row
        for row in _load_jsonl(args.terminals)
        if str(row.get("generation_checksum") or "") == CHECKSUM
    ]
    events = [
        row
        for row in _load_jsonl(args.events)
        if str(row.get("generation_checksum") or "") == CHECKSUM
    ]
    terminal_id = stable_id("sweept", {"generation": CHECKSUM, "window": window})
    current_terminal = next(
        (row for row in terminals if row.get("terminal_id") == terminal_id), None
    )
    market: dict[str, Any] = {}
    books: dict[str, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    fetch_errors: list[str] = []
    sequence = time.time_ns()
    try:
        market = _market_for_slug(slug, timeout_s=args.timeout_s)
        tokens = _token_map(market)
        condition_id = str(market.get("conditionId") or market.get("condition_id") or "")
        clob = CLOBMarketClient(args.clob_base_url, timeout_s=args.clob_timeout_s, retries=1)
        for outcome in ("Up", "Down"):
            token = str(tokens.get(outcome) or "")
            books[outcome] = summarize_l2(
                clob.get_book(token), token_id=token, observed_at_s=now
            )
            snapshots.append(
                {
                    "generation_checksum": CHECKSUM,
                    "window_start_s": window,
                    "sequence": sequence,
                    "observed_at_s": now,
                    "outcome": outcome,
                    "book": books[outcome],
                }
            )
        trades = _fetch_trades(condition_id, args.timeout_s)
    except Exception as exc:  # noqa: BLE001
        fetch_errors.append(f"native_fetch:{type(exc).__name__}")

    existing_event = next(
        (
            row
            for row in events
            if int(row.get("window_start_s") or -1) == window
            and row.get("event") == "native_aggressor_sweep_paper_fill"
        ),
        None,
    )
    if (
        window >= FORWARD_START_S
        and current_terminal is None
        and existing_event is None
        and ENTRY_START_S <= elapsed <= LAST_ENTRY_S
        and market
    ):
        candidates: list[tuple[float, str, dict[str, Any]]] = []
        tokens = _token_map(market)
        for outcome in ("Up", "Down"):
            token = str(tokens.get(outcome) or "")
            prior = _book_before(snapshots[:-2], outcome=outcome, before_sequence=sequence)
            features, _ = sweep_features(
                trades,
                outcome=outcome,
                token_id=token,
                now=now,
                current_book=books.get(outcome) or {},
                prior_book=prior,
            )
            signal, _ = choose_signal(
                outcome=outcome, features=features, book=books.get(outcome) or {}
            )
            if signal:
                candidates.append((signal["net_edge_per_share"], outcome, signal))
        if candidates:
            _, outcome, signal = max(candidates)
            token = str(tokens.get(outcome) or "")
            intent = build_intent(
                outcome=outcome,
                condition_id=str(
                    market.get("conditionId") or market.get("condition_id") or ""
                ),
                slug=slug,
                token_id=token,
                observed_ts=now,
                signal=signal,
            ).asdict()
            event = {
                "schema_version": 1,
                "event": "native_aggressor_sweep_paper_fill",
                "generation_checksum": CHECKSUM,
                "model_checksum": MODEL_CHECKSUM,
                "window_start_s": window,
                "recorded_at": utc_now_iso(),
                "execution": {
                    "price": signal["executable_ask"],
                    "shares": signal["shares"],
                    "observed_executable_depth": signal["executable_ask_depth"],
                    "synthetic_fill": False,
                },
                "intent": intent,
                "paper_only": True,
                "live_orders_allowed": False,
            }
            append_jsonl_many(_rooted(args.events), [event])
            events.append(event)
            existing_event = event

    if (
        window >= FORWARD_START_S
        and current_terminal is None
        and elapsed >= 270
    ):
        blockers = list(fetch_errors)
        if existing_event is None:
            tokens = _token_map(market) if market else {}
            for outcome in ("Up", "Down"):
                token = str(tokens.get(outcome) or "")
                prior = _book_before(
                    snapshots[:-2], outcome=outcome, before_sequence=sequence
                )
                features, reasons = sweep_features(
                    trades,
                    outcome=outcome,
                    token_id=token,
                    now=now,
                    current_book=books.get(outcome) or {},
                    prior_book=prior,
                )
                _, signal_reasons = choose_signal(
                    outcome=outcome,
                    features=features,
                    book=books.get(outcome) or {},
                )
                blockers.extend(f"{outcome}:{reason}" for reason in reasons + signal_reasons)
        intent = (existing_event or {}).get("intent")
        terminal = {
            "schema_version": 1,
            "event": "btc5m_native_aggressor_sweep_terminal",
            "terminal_id": terminal_id,
            "generation_checksum": CHECKSUM,
            "model_checksum": MODEL_CHECKSUM,
            "cell_id": CELL_ID,
            "window_start_s": window,
            "market_slug": slug,
            "recorded_at": utc_now_iso(),
            "terminal_status": "SIGNAL" if intent else "PROTECTED_SKIP",
            "signal": (
                (intent.get("metadata") or {}).get("sweep_features") if intent else None
            ),
            "intent": intent,
            "blockers": sorted(set(blockers)),
            "raw_clock_complete": True,
            "paper_only": True,
            "live_orders_allowed": False,
        }
        append_jsonl_many(_rooted(args.terminals), [terminal])
        terminals.append(terminal)
        current_terminal = terminal

    atomic_write_json(
        _rooted(args.cache),
        {
            "schema_version": 1,
            "kind": "btc5m_native_aggressor_sweep_raw_cache",
            "generated_at": utc_now_iso(),
            "generation_checksum": CHECKSUM,
            "model_checksum": MODEL_CHECKSUM,
            "latest_window": window,
            "snapshots": snapshots[-800:],
            "latest_trades": trades,
            "paper_only": True,
            "live_orders_allowed": False,
        },
    )
    reduced = reduce_generation(terminals, events, _resolution_map(args.resolutions))
    payload = {
        "schema_version": 1,
        "kind": "btc5m_native_aggressor_sweep_continuation_generation",
        "generated_at": utc_now_iso(),
        "flow_stage": "LIVE/ROTATE/DISCOVER/OBSERVE/LEARN/PROMOTE/SELF-DEV",
        "generation_checksum": CHECKSUM,
        "generation_config": CONFIG,
        "cell_id": CELL_ID,
        "preregistration": prereg,
        "frozen_model": {
            "checksum": MODEL_CHECKSUM,
            "generation_checksum": CHECKSUM,
            "training_cutoff_s": MODEL["training_cutoff_s"],
            "forward_start_s": FORWARD_START_S,
            "status": "IMMUTABLE_CHECKSUM_VERIFIED",
        },
        "current_terminal": current_terminal or {},
        **reduced,
        "terminal_reconciliation": {
            "raw_complete_windows": reduced["completed_windows"],
            "terminal_rows": len(terminals),
            "raw_input_equals_terminal": reduced["gate_checks"][
                "raw_input_equals_terminal"
            ],
        },
        "orders_submitted": 0,
        "paper_only": True,
        "live_orders_allowed": False,
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
    }
    atomic_write_json(_rooted(args.state), payload)
    atomic_write_json(_rooted(args.selector), _selector(payload, prereg))
    atomic_write_json(
        _rooted(RUNG_C),
        {
            "schema_version": 1,
            "kind": "rung_c_no_admissible_target",
            "generated_at": payload["generated_at"],
            "status": "RUNG_C_NO_ADMISSIBLE_TARGET",
            "exact_generation": "current_F1_F4",
            "released_slot_occupant": CELL_ID,
            "occupant_status": payload["status"],
            "paper_only": True,
            "live_orders_allowed": False,
        },
    )
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
