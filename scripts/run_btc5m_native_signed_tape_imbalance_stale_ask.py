#!/usr/bin/env python3
"""BTC-native signed tape imbalance into a stale BTC-5m ask (paper only)."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.arbitrate_btc5m_promoted_cells import _checksum as _arbiter_checksum  # noqa: E402
from scripts.run_btc5m_book_shock_reversion import _load_jsonl, _num, _resolution_map, _rooted, summarize_l2  # noqa: E402
from scripts.run_btc5m_native_aggressor_sweep_continuation import _fetch_trades, dedupe_trades  # noqa: E402
from scripts.run_e7_spot_open_paper_lane import _market_for_slug, _token_map  # noqa: E402
from src.wallet_copy.fees import expected_polymarket_buy_fee_usd  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import CopyIntent, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402

FORWARD_START_S = 1_785_006_600  # 2026-07-25T19:10:00Z
ORDER_USD = 1.0
MIN_PRICE, MAX_PRICE = 0.25, 0.50
ENTRY_START_S, LAST_ENTRY_S = 30, 180
EVENT_WINDOW_S = 5.0
MAX_TAPE_TO_ENTRY_LAG_S = 3.0
MIN_ABS_SIGNED_NOTIONAL_USD = 20.0
MIN_SIGN_IMBALANCE = 0.55
MIN_MICROPRICE_MOVE = 0.008
MAX_ASK_REPRICE_FRACTION = 0.50
MAX_RECEIPT_GAP_S = 5.0
SLIPPAGE_PER_SHARE = 0.005
ADVERSE_PER_SHARE = 0.010
ZERO_INTENT_WINDOWS, MAX_WINDOWS, MIN_RESOLVED = 2, 6, 10
STATE = "data/research/btc5m_native_signed_tape_imbalance_stale_ask_state.json"
CACHE = "data/research/btc5m_native_signed_tape_imbalance_stale_ask_raw_cache.json"
TERMINALS = "data/research/btc5m_native_signed_tape_imbalance_stale_ask_terminals.jsonl"
EVENTS = "data/research/btc5m_native_signed_tape_imbalance_stale_ask_events.jsonl"
SELECTOR = "data/research/btc5m_native_signed_tape_imbalance_stale_ask_selector.json"
RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
LEGACY_ARBITER_OUTPUT = "data/research/btc5m_cross_exchange_promoted_cell_latest.json"
ARBITER_SOURCES = [
    "data/research/btc5m_multivenue_ttl_passive_residual_selector.json",
    "data/research/btc5m_multivenue_ttl_maker_first_residual_selector.json",
    "data/research/btc5m_perp_microstructure_selector.json",
    SELECTOR,
]
CONFIG = {
    "schema_version": 1,
    "method": "btc5m_native_signed_tape_imbalance_stale_ask_v1",
    "economic_edge": "native_btc_signed_tape_imbalance_before_resting_ask_reprice",
    "market_identity": {
        "target": "btc-updown-5m-{window}",
        "outcomes": ["Up", "Down"],
    },
    "source_schema": "immutable_native_btc_trade_receipts_and_gap_bounded_dual_outcome_l2_v1",
    "calibration_method": "rolling_signed_notional_imbalance_to_stale_executable_ask",
    "training_cutoff_s": 1_785_006_000,
    "forward_start_s": FORWARD_START_S,
    "features": {
        "imbalance_window_s": EVENT_WINDOW_S,
        "min_abs_signed_notional_usd": MIN_ABS_SIGNED_NOTIONAL_USD,
        "min_sign_imbalance": MIN_SIGN_IMBALANCE,
        "min_microprice_displacement": MIN_MICROPRICE_MOVE,
        "max_tape_to_entry_lag_s": MAX_TAPE_TO_ENTRY_LAG_S,
        "max_ask_reprice_fraction_of_microprice_move": MAX_ASK_REPRICE_FRACTION,
        "max_receipt_gap_s": MAX_RECEIPT_GAP_S,
    },
    "entry_bounds": [MIN_PRICE, MAX_PRICE],
    "entry_window_s": [ENTRY_START_S, LAST_ENTRY_S],
    "order_usd": ORDER_USD,
    "costs": {
        "fee": "exact_embedded_buy_fee",
        "slippage_per_share": SLIPPAGE_PER_SHARE,
        "adverse_selection_per_share": ADVERSE_PER_SHARE,
    },
    "fill_model": "contemporaneous_observed_btc_executable_ask_and_size",
    "one_intent_per_window": True,
    "signal_rule": "rolling_btc_signed_notional_imbalance_to_stale_same_or_complement_outcome_ask; higher_net_edge_wins; exact_equal_edge_abstains",
    "matched_baseline": "same_signed_tape_event_windows_with_immutable_empty_btc_cash_flows",
    "terminal_clock": {
        "zero_intent_complete_windows": ZERO_INTENT_WINDOWS,
        "max_complete_windows": MAX_WINDOWS,
        "required_resolutions": MIN_RESOLVED,
        "sixth_terminal_deadline_s": FORWARD_START_S + (MAX_WINDOWS - 1) * 300 + 270,
    },
    "promotion": {
        "positive_post_cost_total_and_halves": True,
        "positive_increment_vs_matched_no_trade": True,
        "raw_equals_terminal": True,
        "zero_identity_clock_lookahead_parity_disagreement": True,
        "single_guard_accepted_order_within_two_post_activation_windows": True,
    },
    "activation": {
        "arbiter": "scripts/arbitrate_btc5m_promoted_cells.py",
        "legacy_guard_watched_output": LEGACY_ARBITER_OUTPUT,
        "pin": "singular_1usd_nonrefreshing_3600s",
    },
    "paper_only": True,
    "live_orders_allowed": False,
}
CHECKSUM = hashlib.sha256(json.dumps(CONFIG, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
CELL_ID = f"signed_tape_{CHECKSUM[:12]}"
PREREG = f"data/research/btc5m_native_signed_tape_imbalance_stale_ask_preregistration_{CHECKSUM[:12]}.json"


def canonical_checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def preregister() -> dict[str, Any]:
    body = {
        **CONFIG,
        "kind": "btc5m_native_signed_tape_imbalance_stale_ask_preregistration",
        "generation_checksum": CHECKSUM,
        "model_checksum": CHECKSUM,
        "registered_before_forward_outcome_inspection": True,
        "immutable": True,
    }
    expected = {**body, "checksum": canonical_checksum(body)}
    prior = load_json(_rooted(PREREG), default={})
    if prior and prior != expected:
        raise RuntimeError("immutable preregistration mismatch")
    if not prior:
        atomic_write_json(_rooted(PREREG), expected)
    return expected


def trade_receipts(rows: list[dict[str, Any]], *, asset_name: str, receipt_ts: float) -> list[dict[str, Any]]:
    receipts: list[dict[str, Any]] = []
    for row in rows:
        payload = {
            "asset_name": asset_name,
            "trade_identity": str(row.get("transactionHash") or row.get("id") or ""),
            "token_id": str(row.get("asset") or ""),
            "side": str(row.get("side") or "").upper(),
            "price": _num(row.get("price")),
            "size": _num(row.get("size")),
            "exchange_timestamp_s": _num(row.get("timestamp")),
        }
        receipts.append({
            **payload,
            "receipt_timestamp_s": receipt_ts,
            "payload_hash": canonical_checksum(payload),
        })
    return receipts


def raw_integrity(trades: list[dict[str, Any]], books: list[dict[str, Any]], terminal_ts: float) -> dict[str, Any]:
    identities: dict[tuple[str, str], set[str]] = {}
    missing = lookahead = 0
    for row in trades:
        identity = str(row.get("trade_identity") or "")
        key = (str(row.get("asset_name") or ""), identity)
        if not identity:
            missing += 1
        identities.setdefault(key, set()).add(str(row.get("payload_hash") or ""))
        if _num(row.get("exchange_timestamp_s")) > _num(row.get("receipt_timestamp_s")):
            lookahead += 1
        if _num(row.get("receipt_timestamp_s")) > terminal_ts:
            lookahead += 1
    identity_conflicts = missing + sum(1 for (asset, identity), hashes in identities.items() if asset and identity and len(hashes) > 1)
    cycles: dict[int, list[dict[str, Any]]] = {}
    for row in books:
        cycles.setdefault(int(row.get("receipt_sequence") or 0), []).append(row)
    expected = {("BTC", outcome) for outcome in ("Up", "Down")}
    incomplete = sum(
        1
        for rows in cycles.values()
        if {
            (str(row.get("asset_name")), str(row.get("outcome")))
            for row in rows
        } != expected
        or any(not row.get("book_hash") for row in rows)
    )
    ordered = sorted(cycles)
    times = [max(_num(row.get("receipt_timestamp_s")) for row in cycles[seq]) for seq in ordered]
    gaps = [later - prior for prior, later in zip(times, times[1:])]
    continuity = incomplete + sum(gap > MAX_RECEIPT_GAP_S for gap in gaps)
    return {
        "trade_receipt_count": len(trades),
        "book_receipt_count": len(books),
        "dual_book_receipt_cycles": len(ordered),
        "identity_conflicts": identity_conflicts,
        "lookahead_violations": lookahead,
        "clock_disagreements": sum(
            1 for row in trades if _num(row.get("exchange_timestamp_s")) <= 0
        ),
        "continuity_disagreements": continuity,
        "max_receipt_gap_s": round(max(gaps, default=0.0), 6),
        "trade_receipts_checksum": canonical_checksum(trades),
        "book_receipts_checksum": canonical_checksum(books),
    }


def _prior_book(snapshots: list[dict[str, Any]], asset_name: str, outcome: str, before_ts: float) -> dict[str, Any]:
    rows = [
        row
        for row in snapshots
        if row.get("asset_name") == asset_name
        and row.get("outcome") == outcome
        and _num(row.get("receipt_timestamp_s")) <= before_ts
    ]
    return (rows[-1].get("book") or {}) if rows else {}


def choose_signal(
    *,
    markets: dict[str, dict[str, Any]],
    trades_by_asset: dict[str, list[dict[str, Any]]],
    snapshots: list[dict[str, Any]],
    books: dict[str, dict[str, dict[str, Any]]],
    now: float,
    elapsed: float,
) -> tuple[dict[str, Any] | None, list[str]]:
    blockers: list[str] = []
    candidates: list[dict[str, Any]] = []
    market = markets.get("BTC") or {}
    tokens = _token_map(market)
    rows = trades_by_asset.get("BTC") or []
    for source_outcome in ("Up", "Down"):
        token = str(tokens.get(source_outcome) or "")
        tape = [
            row for row in rows
            if str(row.get("asset") or "") == token
            and now - EVENT_WINDOW_S <= _num(row.get("timestamp")) <= now
        ]
        buy_notional = sum(_num(row.get("price")) * _num(row.get("size")) for row in tape if str(row.get("side") or "").upper() == "BUY")
        sell_notional = sum(_num(row.get("price")) * _num(row.get("size")) for row in tape if str(row.get("side") or "").upper() == "SELL")
        total = buy_notional + sell_notional
        signed = buy_notional - sell_notional
        imbalance = abs(signed) / total if total else 0.0
        if not tape or not total:
            blockers.append(f"{source_outcome}:btc_signed_tape_missing")
            continue
        target_outcome = source_outcome if signed > 0 else ("Down" if source_outcome == "Up" else "Up")
        event_ts = max(_num(row.get("timestamp")) for row in tape)
        tape_lag = max(0.0, now - event_ts)
        source_book = (books.get("BTC") or {}).get(source_outcome) or {}
        source_prior = _prior_book(snapshots, "BTC", source_outcome, now - EVENT_WINDOW_S)
        signed_direction = 1.0 if signed > 0 else -1.0
        micro_move = signed_direction * (
            _num(source_book.get("microprice")) - _num(source_prior.get("microprice"))
        )
        target_book = (books.get("BTC") or {}).get(target_outcome) or {}
        target_prior = _prior_book(snapshots, "BTC", target_outcome, now - EVENT_WINDOW_S)
        ask_reprice = abs(
            _num(target_book.get("best_ask")) - _num(target_prior.get("best_ask"))
        )
        ask, depth = _num(target_book.get("best_ask")), _num(target_book.get("best_ask_size"))
        shares = ORDER_USD / ask if ask > 0 else 0.0
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=ask)
        p_fair = (
            _num(source_book.get("microprice"))
            if signed > 0
            else 1.0 - _num(source_book.get("microprice"))
        )
        reserve = (fee / shares if shares else 99.0) + SLIPPAGE_PER_SHARE + ADVERSE_PER_SHARE
        edge = p_fair - ask - reserve
        local: list[str] = []
        if abs(signed) < MIN_ABS_SIGNED_NOTIONAL_USD:
            local.append("abs_signed_notional_below_20")
        if imbalance < MIN_SIGN_IMBALANCE:
            local.append("sign_imbalance_below_0p55")
        if micro_move < MIN_MICROPRICE_MOVE:
            local.append("signed_microprice_displacement_below_0p008")
        if tape_lag > MAX_TAPE_TO_ENTRY_LAG_S:
            local.append("tape_to_entry_lag_above_3s")
        if source_prior.get("status") != "PASS" or target_prior.get("status") != "PASS" or target_book.get("status") != "PASS":
            local.append("btc_sequence_consistent_book_missing")
        if micro_move > 0 and ask_reprice > MAX_ASK_REPRICE_FRACTION * micro_move:
            local.append("btc_ask_already_repriced")
        if not ENTRY_START_S <= elapsed <= LAST_ENTRY_S:
            local.append("entry_time_outside_30_180s")
        if not MIN_PRICE <= ask <= MAX_PRICE:
            local.append("btc_executable_ask_outside_0p25_0p50")
        if depth + 1e-9 < shares:
            local.append("btc_executable_depth_below_one_dollar")
        if edge <= 0:
            local.append("btc_post_cost_edge_nonpositive")
        if local:
            blockers.extend(f"{source_outcome}:{reason}" for reason in local)
            continue
        candidates.append({
            "outcome": target_outcome,
            "source_outcome": source_outcome,
            "event_ts": event_ts,
            "trade_identities": sorted(str(row.get("transactionHash") or row.get("id") or "") for row in tape),
            "buy_notional_usd": round(buy_notional, 6),
            "sell_notional_usd": round(sell_notional, 6),
            "signed_notional_usd": round(signed, 6),
            "sign_imbalance": round(imbalance, 6),
            "microprice_displacement": round(micro_move, 6),
            "tape_to_entry_lag_s": round(tape_lag, 6),
            "ask_reprice": round(ask_reprice, 6),
            "p_fair": round(p_fair, 6),
            "executable_ask": ask,
            "executable_ask_depth": depth,
            "shares": round(shares, 6),
            "fee_usd": round(fee, 6),
            "net_edge_per_share": round(edge, 6),
        })
    ranked = sorted(candidates, key=lambda row: row["net_edge_per_share"], reverse=True)
    if len(ranked) > 1 and ranked[0]["outcome"] != ranked[1]["outcome"] and ranked[0]["net_edge_per_share"] == ranked[1]["net_edge_per_share"]:
        return None, ["opposite_concurrent_events_equal_edge"]
    return (ranked[0], []) if ranked else (None, sorted(set(blockers)))


def build_intent(signal: dict[str, Any], market: dict[str, Any], now: float) -> dict[str, Any]:
    outcome = str(signal["outcome"])
    slug = str(market.get("slug") or f"btc-updown-5m-{int(now // 300) * 300}")
    return CopyIntent(
        intent_id=stable_id("signedtapeci", {"generation": CHECKSUM, "market": slug}),
        source_wallet=f"BTC5M_PROMOTED_CELL:{CELL_ID}",
        wallet_name=CELL_ID,
        source_event_id=stable_id("signedtapee", {"market": slug, "event_ts": signal["event_ts"], "outcome": outcome}),
        condition_id=str(market.get("conditionId") or market.get("condition_id") or ""),
        market_slug=slug,
        outcome=outcome,
        side="YES" if outcome == "Up" else "NO",
        limit_price=_num(signal["executable_ask"]),
        wallet_usdc_size=ORDER_USD,
        copy_size_usd=ORDER_USD,
        shares=_num(signal["shares"]),
        observed_ts=now,
        strategy_family=CONFIG["method"],
        policy_id="native_signed_tape_imbalance_stale_ask_costed_cap1",
        sizing_policy_id="fixed_usd_1",
        mode="paper",
        action="BUY",
        order_type="FAK",
        token_id=str(_token_map(market).get(outcome) or ""),
        event_ts=_num(signal["event_ts"]),
        api_latency_s=max(0.0, now - _num(signal["event_ts"])),
        live_orders_allowed=False,
        reason="native BTC signed tape imbalance before executable ask reprice",
        metadata={"generation_checksum": CHECKSUM, "model_checksum": CHECKSUM, "features": signal},
    ).asdict()


def parity_disagreements(terminal: dict[str, Any]) -> int:
    intent = terminal.get("intent")
    if not isinstance(intent, dict):
        return 0
    execution, signal = terminal.get("execution") or {}, terminal.get("signal") or {}
    checks = (
        intent.get("mode") == "paper",
        intent.get("action") == "BUY",
        intent.get("order_type") == "FAK",
        _num(intent.get("copy_size_usd")) == ORDER_USD,
        intent.get("live_orders_allowed") is False,
        str((intent.get("metadata") or {}).get("generation_checksum") or "") == CHECKSUM,
        _num(intent.get("limit_price")) == _num(execution.get("price")),
        _num(intent.get("shares")) == _num(execution.get("shares")),
        _num(signal.get("executable_ask_depth")) >= _num(intent.get("shares")),
    )
    return sum(not check for check in checks)


def reduce_generation(terminals: list[dict[str, Any]], events: list[dict[str, Any]], resolutions: dict[str, str]) -> dict[str, Any]:
    rows = [
        row for row in terminals
        if row.get("generation_checksum") == CHECKSUM
        and int(row.get("window_start_s") or 0) >= FORWARD_START_S
    ]
    windows = sorted({int(row["window_start_s"]) for row in rows if row.get("raw_clock_complete")})
    intents = [row for row in rows if isinstance(row.get("intent"), dict)]
    pnl: list[float] = []
    for event in events:
        if event.get("generation_checksum") != CHECKSUM:
            continue
        intent = event.get("intent") or {}
        winner = resolutions.get(str(intent.get("market_slug") or ""))
        if not winner:
            continue
        shares, price = _num(intent.get("shares")), _num(intent.get("limit_price"))
        pnl.append(shares * (1 if intent.get("outcome") == winner else 0) - ORDER_USD - expected_polymarket_buy_fee_usd(shares=shares, price=price))
    split = len(pnl) // 2
    matched = sum(
        _num(flow)
        for row in rows
        for cohort in (row.get("matched_no_trade_cohort") or [])
        for flow in (cohort.get("cash_flows_usd") or [])
    )
    integrity = {
        key: sum(int((row.get("raw_integrity") or {}).get(key) or 0) for row in rows)
        for key in ("identity_conflicts", "lookahead_violations", "clock_disagreements", "continuity_disagreements")
    }
    parity = sum(parity_disagreements(row) for row in rows)
    raw_equal = len(rows) == len(windows) and all(
        row.get("raw_evidence_checksum") == canonical_checksum({
            "trade_receipts": row.get("trade_receipts") or [],
            "book_receipts": row.get("book_receipts") or [],
        })
        for row in rows
    )
    gates = {
        "resolved_gte_10": len(pnl) >= MIN_RESOLVED,
        "post_cost_positive": sum(pnl) > 0,
        "first_half_positive": bool(pnl[:split]) and sum(pnl[:split]) > 0,
        "second_half_positive": bool(pnl[split:]) and sum(pnl[split:]) > 0,
        "incremental_positive": sum(pnl) - matched > 0,
        "raw_input_equals_terminal": raw_equal,
        "actual_executable_depth": all(_num((row.get("signal") or {}).get("executable_ask_depth")) >= _num((row.get("intent") or {}).get("shares")) for row in intents),
        "zero_identity_clock_lookahead_parity_disagreement": not any(integrity.values()) and parity == 0,
    }
    status = "PAPER_CELL_ACTIVE"
    if len(windows) >= ZERO_INTENT_WINDOWS and not intents:
        status = "PARK_ZERO_INTENT_GENERATION"
    elif len(windows) >= MAX_WINDOWS and (len(pnl) < MIN_RESOLVED or not all(gates.values())):
        status = "PARK_FAILED_GATE_BY_SIX_WINDOWS"
    elif all(gates.values()):
        status = "PROMOTION_HANDOFF_READY"
    return {
        "status": status,
        "complete_window_starts_s": windows,
        "completed_windows": len(windows),
        "positive_edge_intents": len(intents),
        "paper_fills": len(intents),
        "resolved_orders": len(pnl),
        "post_cost_pnl_usd": round(sum(pnl), 6),
        "matched_no_trade_pnl_usd": round(matched, 6),
        "first_half_post_cost_pnl_usd": round(sum(pnl[:split]), 6),
        "second_half_post_cost_pnl_usd": round(sum(pnl[split:]), 6),
        "gate_checks": gates,
        "measured_integrity": {**integrity, "parity_disagreements": parity},
        "refusal_taxonomy": dict(Counter(reason for row in rows for reason in (row.get("blockers") or []))),
        "stop_writer": status.startswith("PARK_"),
    }


def selector(payload: dict[str, Any], prereg: dict[str, Any]) -> dict[str, Any]:
    if payload["status"].startswith("PARK_"):
        return {
            "schema_version": 1, "kind": "btc5m_cross_exchange_promoted_cell_selector_terminal_tombstone",
            "generated_at": payload["generated_at"], "status": f"TERMINAL_{payload['status']}",
            "generation_checksum": CHECKSUM, "terminal_decision": payload["status"],
            "selected": None, "cells": [], "gate_pass": False, "stop_writer": True,
            "active_capacity": False, "due": False, "historical_arbiter_only": True,
            "paper_only": True, "live_orders_allowed": False,
        }
    checks = {"paper_only": True, "preregistration_checksum_exact": True, "model_checksum_exact": True, **payload["gate_checks"]}
    evidence = {
        "resolved_fills": payload["resolved_orders"], "post_fee_pnl_usd": payload["post_cost_pnl_usd"],
        "first_half": {"post_fee_pnl_usd": payload["first_half_post_cost_pnl_usd"]},
        "second_half": {"post_fee_pnl_usd": payload["second_half_post_cost_pnl_usd"]},
        "checks": checks,
    }
    body = {
        "schema_version": 1, "cell_id": CELL_ID, "preregistration_checksum": prereg["checksum"],
        "model_checksum": CHECKSUM, "signal_offset_s": ENTRY_START_S, "execution_mode": "taker",
        "state_path": STATE, "evidence_snapshot": evidence,
    }
    record = {**body, "evidence_snapshot_checksum": _arbiter_checksum(evidence), "record_checksum": _arbiter_checksum(body), "gate_pass": all(checks.values()), "status": "ELIGIBLE" if all(checks.values()) else "ACCRUING"}
    selected = dict(record) if record["gate_pass"] else None
    if selected:
        selected["activation_id"] = f"promoted-cell-{_arbiter_checksum({'cell': CELL_ID, 'record': record['record_checksum']})[:20]}"
    return {
        "schema_version": 1, "kind": "btc5m_cross_exchange_promoted_cell_selector",
        "generated_at": payload["generated_at"], "status": "PROMOTED_CELL_READY" if selected else "NO_GATE_COMPLETE_CELL",
        "cells": [record], "selected": selected, "single_submitter": "scripts/run_wallet_copy_live_guard.py",
        "paper_only": True, "live_orders_allowed": False,
    }


def publish_arbiter(args: argparse.Namespace) -> dict[str, Any]:
    command = [sys.executable, str(ROOT / "scripts/arbitrate_btc5m_promoted_cells.py")]
    for source in ARBITER_SOURCES:
        command.extend(["--source", source])
    command.extend(["--output", args.legacy_arbiter_output, "--max-age-s", "30"])
    result = subprocess.run(command, cwd=ROOT, capture_output=True, text=True, timeout=15, check=False)
    if result.returncode:
        raise RuntimeError(f"arbiter failed rc={result.returncode}")
    return json.loads(result.stdout)


def run_once(args: argparse.Namespace) -> dict[str, Any]:
    prereg = preregister()
    now = float(args.now_ts or time.time())
    window = int(now // 300) * 300
    elapsed = now - window
    cache = load_json(_rooted(args.cache), default={})
    snapshots = [row for row in (cache.get("snapshots") or []) if row.get("generation_checksum") == CHECKSUM]
    receipts = [row for row in (cache.get("trade_receipts") or []) if row.get("generation_checksum") == CHECKSUM]
    terminals = [row for row in _load_jsonl(args.terminals) if row.get("generation_checksum") == CHECKSUM]
    events = [row for row in _load_jsonl(args.events) if row.get("generation_checksum") == CHECKSUM]
    terminal_id = stable_id("signedtapeterm", {"generation": CHECKSUM, "window": window})
    current_terminal = next((row for row in terminals if row.get("terminal_id") == terminal_id), None)
    markets: dict[str, dict[str, Any]] = {}
    books: dict[str, dict[str, dict[str, Any]]] = {}
    trades_by_asset: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    sequence = time.time_ns()
    try:
        clob = CLOBMarketClient(args.clob_base_url, timeout_s=args.clob_timeout_s, retries=1)
        for asset_name, prefix in (("BTC", "btc"),):
            market = _market_for_slug(f"{prefix}-updown-5m-{window}", timeout_s=args.timeout_s)
            markets[asset_name] = market
            tokens = _token_map(market)
            books[asset_name] = {}
            for outcome in ("Up", "Down"):
                token = str(tokens.get(outcome) or "")
                book = summarize_l2(clob.get_book(token), token_id=token, observed_at_s=now)
                books[asset_name][outcome] = book
                snapshots.append({
                    "generation_checksum": CHECKSUM, "window_start_s": window,
                    "asset_name": asset_name, "outcome": outcome,
                    "receipt_sequence": sequence, "receipt_timestamp_s": now,
                    "book_hash": canonical_checksum(book), "book": book,
                })
            condition = str(market.get("conditionId") or market.get("condition_id") or "")
            rows = _fetch_trades(condition, args.timeout_s)
            trades_by_asset[asset_name] = rows
            receipts.extend({
                **row, "generation_checksum": CHECKSUM, "window_start_s": window,
            } for row in trade_receipts(rows, asset_name=asset_name, receipt_ts=now))
    except Exception as exc:  # noqa: BLE001
        errors.append(f"native_signed_tape_fetch:{type(exc).__name__}")
    existing_event = next((row for row in events if int(row.get("window_start_s") or -1) == window), None)
    blockers: list[str] = list(errors)
    if window >= FORWARD_START_S and current_terminal is None and existing_event is None and markets and ENTRY_START_S <= elapsed <= LAST_ENTRY_S:
        signal, blockers = choose_signal(
            markets=markets, trades_by_asset=trades_by_asset, snapshots=snapshots[:-2],
            books=books, now=now, elapsed=elapsed,
        )
        if signal:
            intent = build_intent(signal, markets["BTC"], now)
            event = {
                "schema_version": 1, "event": "btc5m_native_signed_tape_imbalance_stale_ask_paper_fill",
                "generation_checksum": CHECKSUM, "window_start_s": window, "recorded_at": utc_now_iso(),
                "execution": {"price": signal["executable_ask"], "shares": signal["shares"], "observed_executable_depth": signal["executable_ask_depth"], "synthetic_fill": False},
                "intent": intent, "paper_only": True, "live_orders_allowed": False,
            }
            append_jsonl_many(_rooted(args.events), [event])
            events.append(event)
            existing_event = event
    if window >= FORWARD_START_S and current_terminal is None and elapsed >= 270:
        if existing_event is None and not blockers:
            _, blockers = choose_signal(
                markets=markets, trades_by_asset=trades_by_asset, snapshots=snapshots[:-2],
                books=books, now=now, elapsed=elapsed,
            )
        window_trades = [row for row in receipts if int(row.get("window_start_s") or -1) == window]
        window_books = [
            {key: row.get(key) for key in ("asset_name", "outcome", "receipt_sequence", "receipt_timestamp_s", "book_hash")}
            for row in snapshots if int(row.get("window_start_s") or -1) == window
        ]
        integrity = raw_integrity(window_trades, window_books, now)
        raw = {"trade_receipts": window_trades, "book_receipts": window_books}
        intent = (existing_event or {}).get("intent")
        signal_row = ((intent or {}).get("metadata") or {}).get("features") or {}
        terminal = {
            "schema_version": 1, "event": "btc5m_native_signed_tape_imbalance_stale_ask_terminal",
            "terminal_id": terminal_id, "generation_checksum": CHECKSUM, "model_checksum": CHECKSUM,
            "cell_id": CELL_ID, "window_start_s": window, "market_slug": f"btc-updown-5m-{window}",
            "recorded_at": utc_now_iso(), "terminal_status": "SIGNAL" if intent else "PROTECTED_SKIP",
            "signal": ((intent or {}).get("metadata") or {}).get("features") if intent else None,
            "intent": intent, "execution": (existing_event or {}).get("execution") or {},
            "blockers": sorted(set(blockers)), "trade_receipts": window_trades, "book_receipts": window_books,
            "raw_integrity": integrity, "raw_evidence_checksum": canonical_checksum(raw),
            "matched_no_trade_cohort": [
                {"source_outcome": signal_row.get("source_outcome"), "event_ts": signal_row.get("event_ts"), "trade_identities": signal_row.get("trade_identities"), "orders_executed": 0, "cash_flows_usd": []}
                for _ in ([signal_row] if signal_row else [])
            ],
            "raw_clock_complete": True, "paper_only": True, "live_orders_allowed": False,
        }
        append_jsonl_many(_rooted(args.terminals), [terminal])
        terminals.append(terminal)
        current_terminal = terminal
    atomic_write_json(_rooted(args.cache), {
        "schema_version": 1, "kind": "btc5m_native_signed_tape_imbalance_stale_ask_raw_cache",
        "generated_at": utc_now_iso(), "generation_checksum": CHECKSUM, "latest_window": window,
        "snapshots": snapshots[-1800:], "trade_receipts": receipts[-6000:],
        "paper_only": True, "live_orders_allowed": False,
    })
    reduced = reduce_generation(terminals, events, _resolution_map(args.resolutions))
    payload = {
        "schema_version": 1, "kind": "btc5m_native_signed_tape_imbalance_stale_ask_generation",
        "generated_at": utc_now_iso(), "flow_stage": "LIVE/ROTATE/DISCOVER/OBSERVE/LEARN/PROMOTE",
        "generation_checksum": CHECKSUM, "generation_config": CONFIG, "cell_id": CELL_ID,
        "preregistration": prereg, "frozen_model": {"checksum": CHECKSUM, "training_cutoff_s": CONFIG["training_cutoff_s"], "forward_start_s": FORWARD_START_S, "status": "IMMUTABLE_CHECKSUM_VERIFIED"},
        "current_terminal": current_terminal or {}, **reduced,
        "terminal_reconciliation": {"raw_complete_windows": reduced["completed_windows"], "terminal_rows": len(terminals), "raw_input_equals_terminal": reduced["gate_checks"]["raw_input_equals_terminal"]},
        "orders_submitted": 0, "paper_only": True, "live_orders_allowed": False,
        "single_submitter": "scripts/run_wallet_copy_live_guard.py",
    }
    atomic_write_json(_rooted(args.state), payload)
    atomic_write_json(_rooted(args.selector), selector(payload, prereg))
    arbiter = publish_arbiter(args)
    payload["legacy_arbiter_publication"] = {
        "output": args.legacy_arbiter_output, "status": arbiter.get("status"),
        "candidate_count": arbiter.get("candidate_count"), "resident_guard_pid_reloaded": False,
    }
    atomic_write_json(_rooted(args.state), payload)
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", default=STATE)
    parser.add_argument("--cache", default=CACHE)
    parser.add_argument("--terminals", default=TERMINALS)
    parser.add_argument("--events", default=EVENTS)
    parser.add_argument("--selector", default=SELECTOR)
    parser.add_argument("--resolutions", default=RESOLUTIONS)
    parser.add_argument("--legacy-arbiter-output", default=LEGACY_ARBITER_OUTPUT)
    parser.add_argument("--clob-base-url", default="https://clob.polymarket.com")
    parser.add_argument("--timeout-s", type=float, default=2.0)
    parser.add_argument("--clob-timeout-s", type=float, default=1.0)
    parser.add_argument("--interval-s", type=float, default=1.0)
    parser.add_argument("--now-ts", type=float, default=0.0)
    parser.add_argument("--watch", action="store_true")
    parser.add_argument("--preregister-only", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.preregister_only:
        print(json.dumps(preregister(), sort_keys=True))
        return 0
    while True:
        payload = run_once(args)
        print(json.dumps(payload, sort_keys=True), flush=True)
        if not args.watch or payload.get("stop_writer"):
            return 0
        time.sleep(max(0.2, args.interval_s))


if __name__ == "__main__":
    raise SystemExit(main())
