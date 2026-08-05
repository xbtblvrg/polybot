#!/usr/bin/env python3
"""Native first ETH-or-SOL leader-lag into BTC-5m (paper only)."""

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

FORWARD_START_S = 1_785_005_400  # 2026-07-25T18:50:00Z
ORDER_USD = 1.0
MIN_PRICE, MAX_PRICE = 0.25, 0.50
ENTRY_START_S, LAST_ENTRY_S = 30, 180
EVENT_WINDOW_S = 3.0
MAX_LEADER_TO_BTC_LAG_S = 2.0
MIN_SWEEP_NOTIONAL_USD = 25.0
MIN_SWEEP_LEVELS = 2
MIN_SIGN_IMBALANCE = 0.60
MIN_LEADER_MICROPRICE_MOVE = 0.010
MAX_BTC_REPRICE_FRACTION = 0.50
MAX_RECEIPT_GAP_S = 5.0
SLIPPAGE_PER_SHARE = 0.005
ADVERSE_PER_SHARE = 0.010
ZERO_INTENT_WINDOWS, MAX_WINDOWS, MIN_RESOLVED = 2, 6, 10
STATE = "data/research/btc5m_polymarket_first_leader_cross_asset_lag_state.json"
CACHE = "data/research/btc5m_polymarket_first_leader_cross_asset_lag_raw_cache.json"
TERMINALS = "data/research/btc5m_polymarket_first_leader_cross_asset_lag_terminals.jsonl"
EVENTS = "data/research/btc5m_polymarket_first_leader_cross_asset_lag_events.jsonl"
SELECTOR = "data/research/btc5m_polymarket_first_leader_cross_asset_lag_selector.json"
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
    "method": "btc5m_polymarket_first_leader_cross_asset_lag_v1",
    "economic_edge": "native_first_eth_or_sol_price_discovery_before_btc_outcome_reprice",
    "market_identity": {
        "leaders": ["eth-updown-5m-{window}", "sol-updown-5m-{window}"],
        "target": "btc-updown-5m-{window}",
        "outcomes": ["Up", "Down"],
    },
    "source_schema": "immutable_native_trade_receipts_and_gap_bounded_dual_outcome_l2_v1",
    "calibration_method": "first_valid_leader_or_with_conflict_abstain",
    "training_cutoff_s": 1_785_004_800,
    "forward_start_s": FORWARD_START_S,
    "features": {
        "leader_sweep_window_s": EVENT_WINDOW_S,
        "leader_min_notional_usd": MIN_SWEEP_NOTIONAL_USD,
        "leader_min_price_levels": MIN_SWEEP_LEVELS,
        "leader_min_sign_imbalance": MIN_SIGN_IMBALANCE,
        "leader_min_microprice_displacement": MIN_LEADER_MICROPRICE_MOVE,
        "max_leader_to_btc_lag_s": MAX_LEADER_TO_BTC_LAG_S,
        "btc_max_reprice_fraction": MAX_BTC_REPRICE_FRACTION,
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
    "signal_rule": "at_least_one_valid_leader_same_direction; opposite_valid_leaders_within_2s_abstain",
    "matched_baseline": "same_first_leader_windows_with_immutable_empty_btc_cash_flows",
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
CELL_ID = f"first_leader_{CHECKSUM[:12]}"
PREREG = f"data/research/btc5m_polymarket_first_leader_cross_asset_lag_preregistration_{CHECKSUM[:12]}.json"


def canonical_checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def preregister() -> dict[str, Any]:
    body = {
        **CONFIG,
        "kind": "btc5m_polymarket_first_leader_cross_asset_lag_preregistration",
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
    expected = {(asset, outcome) for asset in ("ETH", "SOL", "BTC") for outcome in ("Up", "Down")}
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
        "six_book_receipt_cycles": len(ordered),
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


def leader_sweep(
    *,
    asset_name: str,
    outcome: str,
    token_id: str,
    rows: list[dict[str, Any]],
    snapshots: list[dict[str, Any]],
    current_book: dict[str, Any],
    now: float,
) -> tuple[dict[str, Any] | None, list[str]]:
    clean, conflicts = dedupe_trades(rows)
    recent = [
        row
        for row in clean
        if str(row.get("asset") or "") == token_id
        and now - EVENT_WINDOW_S <= _num(row.get("timestamp")) <= now
    ]
    buys = [row for row in recent if str(row.get("side") or "").upper() == "BUY"]
    if not buys:
        return None, [*conflicts, "aggressive_buy_sweep_missing"]
    event_ts = max(_num(row.get("timestamp")) for row in buys)
    prior = _prior_book(snapshots, asset_name, outcome, event_ts)
    buy_notional = sum(_num(row.get("price")) * _num(row.get("size")) for row in buys)
    sell_notional = sum(
        _num(row.get("price")) * _num(row.get("size"))
        for row in recent
        if str(row.get("side") or "").upper() == "SELL"
    )
    total = buy_notional + sell_notional
    imbalance = (buy_notional - sell_notional) / total if total > 0 else 0.0
    levels = len({round(_num(row.get("price")), 6) for row in buys})
    displacement = _num(current_book.get("microprice")) - _num(prior.get("microprice"))
    blockers = list(conflicts)
    if prior.get("status") != "PASS" or current_book.get("status") != "PASS":
        blockers.append("leader_dual_outcome_book_incomplete")
    if buy_notional < MIN_SWEEP_NOTIONAL_USD:
        blockers.append("leader_sweep_notional_below_25")
    if levels < MIN_SWEEP_LEVELS:
        blockers.append("leader_sweep_levels_below_two")
    if imbalance < MIN_SIGN_IMBALANCE:
        blockers.append("leader_sign_imbalance_below_0p60")
    if displacement < MIN_LEADER_MICROPRICE_MOVE:
        blockers.append("leader_microprice_displacement_below_0p010")
    if blockers:
        return None, sorted(set(blockers))
    return {
        "asset_name": asset_name,
        "outcome": outcome,
        "event_ts": event_ts,
        "buy_notional_usd": round(buy_notional, 6),
        "price_levels": levels,
        "sign_imbalance": round(imbalance, 6),
        "microprice_displacement": round(displacement, 6),
        "post_event_microprice": _num(current_book.get("microprice")),
        "trade_identities": sorted(str(row.get("transactionHash") or row.get("id") or "") for row in buys),
    }, []


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
    events_by_outcome: dict[str, list[dict[str, Any]]] = {}
    for outcome in ("Up", "Down"):
        leaders: list[dict[str, Any]] = []
        leader_reasons: list[str] = []
        for asset_name in ("ETH", "SOL"):
            tokens = _token_map(markets.get(asset_name) or {})
            feature, reasons = leader_sweep(
                asset_name=asset_name,
                outcome=outcome,
                token_id=str(tokens.get(outcome) or ""),
                rows=trades_by_asset.get(asset_name) or [],
                snapshots=snapshots,
                current_book=(books.get(asset_name) or {}).get(outcome) or {},
                now=now,
            )
            if feature:
                leaders.append(feature)
            leader_reasons.extend(f"{asset_name}:{reason}" for reason in reasons)
        events_by_outcome[outcome] = leaders
        if not leaders:
            blockers.extend(f"{outcome}:{reason}" for reason in leader_reasons)
            continue
        first_leader = min(leaders, key=lambda row: _num(row["event_ts"]))
        leader_lag = max(0.0, now - _num(first_leader["event_ts"]))
        btc_book = (books.get("BTC") or {}).get(outcome) or {}
        btc_prior = _prior_book(snapshots, "BTC", outcome, _num(first_leader["event_ts"]))
        btc_reprice = abs(_num(btc_book.get("microprice")) - _num(btc_prior.get("microprice")))
        leader_move = _num(first_leader["microprice_displacement"])
        ask, depth = _num(btc_book.get("best_ask")), _num(btc_book.get("best_ask_size"))
        shares = ORDER_USD / ask if ask > 0 else 0.0
        fee = expected_polymarket_buy_fee_usd(shares=shares, price=ask)
        p_fair = _num(first_leader["post_event_microprice"])
        reserve = (fee / shares if shares else 99.0) + SLIPPAGE_PER_SHARE + ADVERSE_PER_SHARE
        edge = p_fair - ask - reserve
        local: list[str] = []
        if leader_lag > MAX_LEADER_TO_BTC_LAG_S:
            local.append("leader_to_btc_lag_above_2s")
        if btc_prior.get("status") != "PASS" or btc_book.get("status") != "PASS":
            local.append("btc_sequence_consistent_book_missing")
        if btc_reprice >= MAX_BTC_REPRICE_FRACTION * leader_move:
            local.append("btc_book_already_repriced")
        if not ENTRY_START_S <= elapsed <= LAST_ENTRY_S:
            local.append("entry_time_outside_30_180s")
        if not MIN_PRICE <= ask <= MAX_PRICE:
            local.append("btc_executable_ask_outside_0p25_0p50")
        if depth + 1e-9 < shares:
            local.append("btc_executable_depth_below_one_dollar")
        if edge <= 0:
            local.append("btc_post_cost_edge_nonpositive")
        if local:
            blockers.extend(f"{outcome}:{reason}" for reason in local)
            continue
        candidates.append({
            "outcome": outcome,
            "leaders": leaders,
            "first_leader": first_leader,
            "leader_to_btc_lag_s": round(leader_lag, 6),
            "btc_microprice_reprice": round(btc_reprice, 6),
            "p_fair": round(p_fair, 6),
            "executable_ask": ask,
            "executable_ask_depth": depth,
            "shares": round(shares, 6),
            "fee_usd": round(fee, 6),
            "net_edge_per_share": round(edge, 6),
        })
    conflicts = [
        abs(_num(up["event_ts"]) - _num(down["event_ts"]))
        for up in events_by_outcome.get("Up", [])
        for down in events_by_outcome.get("Down", [])
    ]
    if conflicts and min(conflicts) <= MAX_LEADER_TO_BTC_LAG_S:
        return None, ["opposite_leader_direction_conflict_within_2s"]
    return (max(candidates, key=lambda row: row["net_edge_per_share"]), []) if candidates else (None, sorted(set(blockers)))


def build_intent(signal: dict[str, Any], market: dict[str, Any], now: float) -> dict[str, Any]:
    outcome = str(signal["outcome"])
    slug = str(market.get("slug") or f"btc-updown-5m-{int(now // 300) * 300}")
    return CopyIntent(
        intent_id=stable_id("firstleadci", {"generation": CHECKSUM, "market": slug}),
        source_wallet=f"BTC5M_PROMOTED_CELL:{CELL_ID}",
        wallet_name=CELL_ID,
        source_event_id=stable_id("firstleade", {"market": slug, "leaders": signal["leaders"]}),
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
        policy_id="native_first_leader_or_conflict_abstain_costed_cap1",
        sizing_policy_id="fixed_usd_1",
        mode="paper",
        action="BUY",
        order_type="FAK",
        token_id=str(_token_map(market).get(outcome) or ""),
        event_ts=max(_num(row["event_ts"]) for row in signal["leaders"]),
        api_latency_s=max(0.0, now - max(_num(row["event_ts"]) for row in signal["leaders"])),
        live_orders_allowed=False,
        reason="first native ETH-or-SOL leader sweep before BTC reprice",
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
    terminal_id = stable_id("firstleadterm", {"generation": CHECKSUM, "window": window})
    current_terminal = next((row for row in terminals if row.get("terminal_id") == terminal_id), None)
    markets: dict[str, dict[str, Any]] = {}
    books: dict[str, dict[str, dict[str, Any]]] = {}
    trades_by_asset: dict[str, list[dict[str, Any]]] = {}
    errors: list[str] = []
    sequence = time.time_ns()
    try:
        clob = CLOBMarketClient(args.clob_base_url, timeout_s=args.clob_timeout_s, retries=1)
        for asset_name, prefix in (("ETH", "eth"), ("SOL", "sol"), ("BTC", "btc")):
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
            if asset_name in {"ETH", "SOL"}:
                condition = str(market.get("conditionId") or market.get("condition_id") or "")
                rows = _fetch_trades(condition, args.timeout_s)
                trades_by_asset[asset_name] = rows
                receipts.extend({
                    **row, "generation_checksum": CHECKSUM, "window_start_s": window,
                } for row in trade_receipts(rows, asset_name=asset_name, receipt_ts=now))
    except Exception as exc:  # noqa: BLE001
        errors.append(f"native_first_leader_fetch:{type(exc).__name__}")
    existing_event = next((row for row in events if int(row.get("window_start_s") or -1) == window), None)
    blockers: list[str] = list(errors)
    if window >= FORWARD_START_S and current_terminal is None and existing_event is None and markets and ENTRY_START_S <= elapsed <= LAST_ENTRY_S:
        signal, blockers = choose_signal(
            markets=markets, trades_by_asset=trades_by_asset, snapshots=snapshots[:-6],
            books=books, now=now, elapsed=elapsed,
        )
        if signal:
            intent = build_intent(signal, markets["BTC"], now)
            event = {
                "schema_version": 1, "event": "btc5m_first_leader_leader_lag_paper_fill",
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
                markets=markets, trades_by_asset=trades_by_asset, snapshots=snapshots[:-6],
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
        leader_rows = ((intent or {}).get("metadata") or {}).get("features", {}).get("leaders") or []
        terminal = {
            "schema_version": 1, "event": "btc5m_polymarket_first_leader_cross_asset_lag_terminal",
            "terminal_id": terminal_id, "generation_checksum": CHECKSUM, "model_checksum": CHECKSUM,
            "cell_id": CELL_ID, "window_start_s": window, "market_slug": f"btc-updown-5m-{window}",
            "recorded_at": utc_now_iso(), "terminal_status": "SIGNAL" if intent else "PROTECTED_SKIP",
            "signal": ((intent or {}).get("metadata") or {}).get("features") if intent else None,
            "intent": intent, "execution": (existing_event or {}).get("execution") or {},
            "blockers": sorted(set(blockers)), "trade_receipts": window_trades, "book_receipts": window_books,
            "raw_integrity": integrity, "raw_evidence_checksum": canonical_checksum(raw),
            "matched_no_trade_cohort": [
                {"leader_asset": row.get("asset_name"), "event_ts": row.get("event_ts"), "trade_identities": row.get("trade_identities"), "orders_executed": 0, "cash_flows_usd": []}
                for row in leader_rows
            ],
            "raw_clock_complete": True, "paper_only": True, "live_orders_allowed": False,
        }
        append_jsonl_many(_rooted(args.terminals), [terminal])
        terminals.append(terminal)
        current_terminal = terminal
    atomic_write_json(_rooted(args.cache), {
        "schema_version": 1, "kind": "btc5m_polymarket_first_leader_cross_asset_lag_raw_cache",
        "generated_at": utc_now_iso(), "generation_checksum": CHECKSUM, "latest_window": window,
        "snapshots": snapshots[-1800:], "trade_receipts": receipts[-6000:],
        "paper_only": True, "live_orders_allowed": False,
    })
    reduced = reduce_generation(terminals, events, _resolution_map(args.resolutions))
    payload = {
        "schema_version": 1, "kind": "btc5m_polymarket_first_leader_cross_asset_lag_generation",
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
