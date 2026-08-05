#!/usr/bin/env python3
"""Native BTC-5m complement lead-lag taker generation (paper only)."""

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
from scripts.run_btc5m_book_shock_reversion import (  # noqa: E402
    _load_jsonl,
    _num,
    _resolution_map,
    _rooted,
    summarize_l2,
)
from scripts.run_btc5m_native_aggressor_sweep_continuation import (  # noqa: E402
    _fetch_trades,
    dedupe_trades,
)
from scripts.run_e7_spot_open_paper_lane import _market_for_slug, _token_map  # noqa: E402
from src.wallet_copy.fees import expected_polymarket_buy_fee_usd  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient  # noqa: E402
from src.wallet_copy.models import CopyIntent, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402

ORDER_USD = 1.0
MIN_PRICE, MAX_PRICE = 0.25, 0.50
FORWARD_START_S = 1_785_001_200  # 2026-07-25T17:40:00Z
ENTRY_START_S, LAST_ENTRY_S = 30, 180
EVENT_WINDOW_S = 3.0
MIN_LAG_S, MAX_LAG_S = 0.25, 2.0
MIN_SELL_NOTIONAL_USD = 50.0
MIN_SELL_LEVELS = 2
MIN_VISIBLE_BID_CONSUMPTION = 0.20
MIN_SELL_SIGN_IMBALANCE = 0.60
MIN_LEADER_MICROPRICE_FALL = 0.010
MAX_COMPLEMENT_REPRICE_FRACTION = 0.50
SLIPPAGE_RESERVE_PER_SHARE = 0.005
ADVERSE_RESERVE_PER_SHARE = 0.010
ZERO_INTENT_WINDOWS, MAX_WINDOWS = 2, 12
MIN_RESOLVED, PERMANENT_RESOLVED = 10, 50
MAX_BOOK_RECEIPT_GAP_S = 5.0
STATE = "data/research/btc5m_native_complement_lead_lag_taker_state.json"
CACHE = "data/research/btc5m_native_complement_lead_lag_taker_raw_cache.json"
TERMINALS = "data/research/btc5m_native_complement_lead_lag_taker_terminals.jsonl"
EVENTS = "data/research/btc5m_native_complement_lead_lag_taker_events.jsonl"
SELECTOR = "data/research/btc5m_native_complement_lead_lag_taker_selector.json"
LEGACY_ARBITER_OUTPUT = "data/research/btc5m_cross_exchange_promoted_cell_latest.json"
RESOLUTIONS = "data/research/btc_resolutions_from_btcusdt_ticks.jsonl"
RUNG_C = "data/research/rung_c_no_admissible_target_latest.json"
ARBITER_SOURCES = [
    "data/research/btc5m_multivenue_ttl_passive_residual_selector.json",
    "data/research/btc5m_multivenue_ttl_maker_first_residual_selector.json",
    "data/research/btc5m_perp_microstructure_selector.json",
    SELECTOR,
]
CONFIG = {
    "schema_version": 1,
    "method": "btc5m_native_complement_lead_lag_taker_v1",
    "economic_edge": "native_cross_outcome_sell_sweep_complement_lag",
    "source_schema": "immutable_trade_receipts_plus_gap_bounded_dual_token_book_receipts_v2",
    "calibration_cutoff_s": 1_784_999_700,
    "forward_start_s": FORWARD_START_S,
    "features": {
        "leader_sell_event_window_s": EVENT_WINDOW_S,
        "leader_min_recorded_bid_levels": MIN_SELL_LEVELS,
        "leader_min_notional_usd": MIN_SELL_NOTIONAL_USD,
        "leader_min_pre_event_visible_bid_consumption": MIN_VISIBLE_BID_CONSUMPTION,
        "leader_min_sell_sign_imbalance": MIN_SELL_SIGN_IMBALANCE,
        "leader_min_microprice_fall": MIN_LEADER_MICROPRICE_FALL,
        "opposite_continuous_lag_s": [MIN_LAG_S, MAX_LAG_S],
        "opposite_max_reprice_fraction": MAX_COMPLEMENT_REPRICE_FRACTION,
        "p_fair": "1-leader_post_event_microprice",
    },
    "entry_bounds": [MIN_PRICE, MAX_PRICE],
    "entry_window_s": [ENTRY_START_S, LAST_ENTRY_S],
    "order_usd": ORDER_USD,
    "fee": "exact_embedded_buy_fee",
    "slippage_reserve_per_share": SLIPPAGE_RESERVE_PER_SHARE,
    "adverse_selection_reserve_per_share": ADVERSE_RESERVE_PER_SHARE,
    "fill_model": "contemporaneous_observed_executable_opposite_ask_and_size_v1",
    "one_intent_per_window": True,
    "matched_baseline": "checksum_frozen_no_trade_leader_sell_cohort",
    "terminal_clock": {
        "zero_intent_complete_windows": ZERO_INTENT_WINDOWS,
        "max_complete_windows": MAX_WINDOWS,
        "required_isolated_resolutions": MIN_RESOLVED,
        "twelfth_terminal_deadline_s": FORWARD_START_S + (MAX_WINDOWS - 1) * 300 + 270,
    },
    "integrity": {
        "max_dual_token_book_receipt_gap_s": MAX_BOOK_RECEIPT_GAP_S,
        "trade_identity": "transactionHash_or_id",
        "trade_times": ["exchange_timestamp_s", "receipt_timestamp_s"],
        "book_receipt": ["receipt_sequence", "receipt_timestamp_s", "book_hash"],
        "reducer_reproducibility": "two_independent_canonical_output_checksums",
        "parity": "derived_from_immutable_terminal_intent_and_execution",
        "matched_no_trade_pnl": "sum_of_immutable_empty_cash_flow_rows",
    },
    "promotion": {
        "resolved": MIN_RESOLVED,
        "permanent_resolved": PERMANENT_RESOLVED,
        "positive_post_cost_and_both_halves": True,
        "positive_incremental_vs_no_trade": True,
        "actual_executable_depth": True,
        "raw_input_equals_terminal": True,
        "zero_sequence_lookahead_identity_parity_disagreement": True,
        "two_run_checksum_idempotence": True,
    },
    "activation": {
        "arbiter": "scripts/arbitrate_btc5m_promoted_cells.py",
        "legacy_pid_88943_watched_output": LEGACY_ARBITER_OUTPUT,
        "pin": "singular_1usd_nonrefreshing_3600s",
    },
    "paper_only": True,
    "live_orders_allowed": False,
}
CHECKSUM = hashlib.sha256(
    json.dumps(CONFIG, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()
CELL_ID = f"native_complement_{CHECKSUM[:12]}"
PREREG = (
    "data/research/btc5m_native_complement_lead_lag_taker_"
    f"preregistration_{CHECKSUM[:12]}.json"
)


def _preregister() -> dict[str, Any]:
    body = {
        **CONFIG,
        "kind": "btc5m_native_complement_lead_lag_taker_preregistration",
        "generation_checksum": CHECKSUM,
        "model_checksum": CHECKSUM,
        "execution_mode": "taker",
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


def _canonical_checksum(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def immutable_trade_receipts(
    rows: list[dict[str, Any]], *, receipt_timestamp_s: float
) -> list[dict[str, Any]]:
    """Persist exchange identity/time separately from our receipt time."""
    receipts: list[dict[str, Any]] = []
    for row in rows:
        identity = str(row.get("transactionHash") or row.get("id") or "")
        payload = {
            "asset": str(row.get("asset") or ""),
            "side": str(row.get("side") or "").upper(),
            "price": _num(row.get("price")),
            "size": _num(row.get("size")),
            "exchange_timestamp_s": _num(row.get("timestamp")),
        }
        receipts.append(
            {
                "trade_identity": identity,
                **payload,
                "receipt_timestamp_s": receipt_timestamp_s,
                "payload_hash": _canonical_checksum(payload),
            }
        )
    return receipts


def measured_raw_integrity(
    *,
    trade_receipts: list[dict[str, Any]],
    book_receipts: list[dict[str, Any]],
    terminal_recorded_at_s: float,
) -> dict[str, Any]:
    identity_payloads: dict[str, set[str]] = {}
    missing_identities = 0
    lookahead = 0
    for row in trade_receipts:
        identity = str(row.get("trade_identity") or "")
        if not identity:
            missing_identities += 1
        identity_payloads.setdefault(identity, set()).add(str(row.get("payload_hash") or ""))
        exchange_ts = _num(row.get("exchange_timestamp_s"))
        receipt_ts = _num(row.get("receipt_timestamp_s"))
        if exchange_ts > receipt_ts or receipt_ts > terminal_recorded_at_s:
            lookahead += 1
    identity_conflicts = missing_identities + sum(
        1 for identity, hashes in identity_payloads.items() if identity and len(hashes) > 1
    )

    cycles: dict[int, list[dict[str, Any]]] = {}
    for row in book_receipts:
        cycles.setdefault(int(row.get("receipt_sequence") or 0), []).append(row)
    ordered_cycles = sorted(cycles)
    incomplete_cycles = sum(
        1
        for rows in cycles.values()
        if {str(row.get("outcome") or "") for row in rows} != {"Up", "Down"}
        or any(not row.get("book_hash") for row in rows)
    )
    receipt_times = [
        max(_num(row.get("receipt_timestamp_s")) for row in cycles[sequence])
        for sequence in ordered_cycles
    ]
    gaps = [
        later - earlier for earlier, later in zip(receipt_times, receipt_times[1:])
    ]
    sequence_disagreements = incomplete_cycles + sum(
        1
        for earlier, later in zip(ordered_cycles, ordered_cycles[1:])
        if later <= earlier
    )
    max_gap = max(gaps, default=0.0)
    continuity_disagreements = sequence_disagreements + sum(
        1 for gap in gaps if gap > MAX_BOOK_RECEIPT_GAP_S
    )
    return {
        "trade_receipt_count": len(trade_receipts),
        "book_receipt_count": len(book_receipts),
        "dual_token_receipt_cycles": len(ordered_cycles),
        "identity_conflicts": identity_conflicts,
        "lookahead_violations": lookahead,
        "sequence_disagreements": sequence_disagreements,
        "continuity_disagreements": continuity_disagreements,
        "max_book_receipt_gap_s": round(max_gap, 6),
        "trade_receipts_checksum": _canonical_checksum(trade_receipts),
        "book_receipts_checksum": _canonical_checksum(book_receipts),
    }


def intent_parity_disagreements(terminal: dict[str, Any]) -> int:
    intent = terminal.get("intent")
    if not isinstance(intent, dict):
        return 0
    execution = terminal.get("execution") or {}
    signal = terminal.get("signal") or {}
    expected = (
        intent.get("mode") == "paper",
        intent.get("action") == "BUY",
        intent.get("order_type") == "FAK",
        _num(intent.get("copy_size_usd")) == ORDER_USD,
        _num(intent.get("wallet_usdc_size")) == ORDER_USD,
        intent.get("live_orders_allowed") is False,
        str((intent.get("metadata") or {}).get("generation_checksum") or "") == CHECKSUM,
        _num(intent.get("limit_price")) == _num(execution.get("price")),
        _num(intent.get("shares")) == _num(execution.get("shares")),
        _num(signal.get("executable_ask_depth")) >= _num(intent.get("shares")),
    )
    return sum(not check for check in expected)


def _snapshots_before(
    snapshots: list[dict[str, Any]],
    *,
    outcome: str,
    before_ts: float,
) -> list[dict[str, Any]]:
    return [
        row
        for row in snapshots
        if row.get("outcome") == outcome
        and _num(row.get("observed_at_s")) <= before_ts
        and isinstance(row.get("book"), dict)
    ]


def leader_sell_features(
    trades: list[dict[str, Any]],
    *,
    leader_outcome: str,
    leader_token: str,
    opposite_outcome: str,
    now: float,
    snapshots: list[dict[str, Any]],
    current_leader_book: dict[str, Any],
    current_opposite_book: dict[str, Any],
) -> tuple[dict[str, Any] | None, list[str]]:
    clean, conflicts = dedupe_trades(trades)
    leader_rows = [
        row
        for row in clean
        if str(row.get("asset") or "") == leader_token
        and now - EVENT_WINDOW_S <= _num(row.get("timestamp")) <= now
    ]
    sells = [
        row for row in leader_rows if str(row.get("side") or "").upper() == "SELL"
    ]
    if not sells:
        return None, sorted(set([*conflicts, "leader_sell_event_missing"]))
    event_ts = max(_num(row.get("timestamp")) for row in sells)
    pre_leader_rows = _snapshots_before(
        snapshots, outcome=leader_outcome, before_ts=event_ts
    )
    pre_opposite_rows = _snapshots_before(
        snapshots, outcome=opposite_outcome, before_ts=event_ts
    )
    pre_leader = (pre_leader_rows[-1].get("book") or {}) if pre_leader_rows else {}
    pre_opposite = (pre_opposite_rows[-1].get("book") or {}) if pre_opposite_rows else {}
    sell_notional = sum(
        _num(row.get("size")) * _num(row.get("price")) for row in sells
    )
    buy_notional = sum(
        _num(row.get("size")) * _num(row.get("price"))
        for row in leader_rows
        if str(row.get("side") or "").upper() == "BUY"
    )
    total_notional = sell_notional + buy_notional
    sell_imbalance = (
        (sell_notional - buy_notional) / total_notional if total_notional > 0 else 0
    )
    sell_levels = len({round(_num(row.get("price")), 6) for row in sells})
    pre_bid_depth = _num(pre_leader.get("best_bid_size"))
    consumed = sum(_num(row.get("size")) for row in sells)
    bid_consumption = consumed / pre_bid_depth if pre_bid_depth > 0 else 0
    leader_fall = _num(pre_leader.get("microprice")) - _num(
        current_leader_book.get("microprice")
    )
    opposite_reprice = abs(
        _num(current_opposite_book.get("best_ask"))
        - _num(pre_opposite.get("best_ask"))
    )
    lag_s = now - event_ts
    blockers = list(conflicts)
    if (
        pre_leader.get("status") != "PASS"
        or pre_opposite.get("status") != "PASS"
        or current_leader_book.get("status") != "PASS"
        or current_opposite_book.get("status") != "PASS"
    ):
        blockers.append("sequence_consistent_dual_outcome_l2_missing")
    if sell_levels < MIN_SELL_LEVELS:
        blockers.append("leader_sell_levels_below_two")
    if sell_notional < MIN_SELL_NOTIONAL_USD:
        blockers.append("leader_sell_notional_below_50")
    if bid_consumption < MIN_VISIBLE_BID_CONSUMPTION:
        blockers.append("leader_visible_bid_consumption_below_20pct")
    if sell_imbalance < MIN_SELL_SIGN_IMBALANCE:
        blockers.append("leader_sell_imbalance_below_0p60")
    if leader_fall < MIN_LEADER_MICROPRICE_FALL:
        blockers.append("leader_microprice_fall_below_0p010")
    if not MIN_LAG_S <= lag_s <= MAX_LAG_S:
        blockers.append("opposite_lag_outside_250ms_2s")
    if opposite_reprice >= MAX_COMPLEMENT_REPRICE_FRACTION * max(leader_fall, 0):
        blockers.append("opposite_ask_already_repriced")
    if blockers:
        return None, sorted(set(blockers))
    return {
        "leader_outcome": leader_outcome,
        "opposite_outcome": opposite_outcome,
        "leader_sell_event_ts": event_ts,
        "leader_sell_notional_usd": round(sell_notional, 6),
        "leader_sell_levels": sell_levels,
        "leader_visible_bid_consumption": round(bid_consumption, 6),
        "leader_sell_sign_imbalance": round(sell_imbalance, 6),
        "leader_microprice_fall": round(leader_fall, 6),
        "leader_post_event_microprice": _num(current_leader_book.get("microprice")),
        "opposite_pre_event_ask": _num(pre_opposite.get("best_ask")),
        "opposite_current_ask": _num(current_opposite_book.get("best_ask")),
        "opposite_reprice": round(opposite_reprice, 6),
        "observed_lag_s": round(lag_s, 6),
        "leader_trade_identities": sorted(
            str(row.get("transactionHash") or row.get("id") or "") for row in sells
        ),
    }, []


def choose_signal(
    features: dict[str, Any] | None,
    *,
    opposite_book: dict[str, Any],
    elapsed_s: float,
) -> tuple[dict[str, Any] | None, list[str]]:
    if features is None:
        return None, ["complement_lead_lag_not_qualified"]
    ask = _num(opposite_book.get("best_ask"))
    depth = _num(opposite_book.get("best_ask_size"))
    shares = ORDER_USD / ask if ask > 0 else 0
    fee = expected_polymarket_buy_fee_usd(shares=shares, price=ask)
    reserve = (
        fee / shares if shares > 0 else 99
    ) + SLIPPAGE_RESERVE_PER_SHARE + ADVERSE_RESERVE_PER_SHARE
    p_fair = 1 - _num(features.get("leader_post_event_microprice"))
    edge = p_fair - ask - reserve
    blockers: list[str] = []
    if not MIN_PRICE <= ask <= MAX_PRICE:
        blockers.append("opposite_executable_ask_outside_bounds")
    if not ENTRY_START_S <= elapsed_s <= LAST_ENTRY_S:
        blockers.append("elapsed_window_outside_30_180s")
    if depth + 1e-9 < shares:
        blockers.append("opposite_executable_depth_below_one_dollar")
    if edge <= 0:
        blockers.append("complement_fair_below_costed_ask")
    if blockers:
        return None, blockers
    return {
        **features,
        "p_fair_opposite": round(p_fair, 6),
        "executable_ask": ask,
        "executable_ask_depth": depth,
        "shares": round(shares, 6),
        "fee_usd": round(fee, 6),
        "cost_reserve_per_share": round(reserve, 6),
        "net_edge_per_share": round(edge, 6),
    }, []


def build_intent(
    *,
    condition_id: str,
    slug: str,
    token_id: str,
    observed_ts: float,
    signal: dict[str, Any],
) -> CopyIntent:
    outcome = str(signal["opposite_outcome"])
    return CopyIntent(
        intent_id=stable_id("complagci", {"generation": CHECKSUM, "market": slug}),
        source_wallet=f"BTC5M_PROMOTED_CELL:{CELL_ID}",
        wallet_name=CELL_ID,
        source_event_id=stable_id(
            "complage",
            {
                "market": slug,
                "leader_trades": signal["leader_trade_identities"],
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
        policy_id="native_complement_lead_lag_costed_cap1",
        sizing_policy_id="fixed_usd_1",
        mode="paper",
        action="BUY",
        order_type="FAK",
        token_id=token_id,
        event_ts=observed_ts,
        api_latency_s=0,
        live_orders_allowed=False,
        reason="native leader SELL complement lag",
        metadata={
            "generation_checksum": CHECKSUM,
            "model_checksum": CHECKSUM,
            "features": signal,
        },
    )


def _reduce_generation_core(
    terminals: list[dict[str, Any]],
    events: list[dict[str, Any]],
    resolutions: dict[str, str],
) -> dict[str, Any]:
    generation_terminals = [
        row
        for row in terminals
        if str(row.get("generation_checksum") or CHECKSUM) == CHECKSUM
        and int(row.get("window_start_s") or 0) >= FORWARD_START_S
    ]
    windows = sorted(
        {
            int(row["window_start_s"])
            for row in generation_terminals
            if row.get("raw_clock_complete")
        }
    )
    intents = [
        row for row in generation_terminals if isinstance(row.get("intent"), dict)
    ]
    resolved: list[float] = []
    for row in events:
        if row.get("event") != "native_complement_lead_lag_paper_fill":
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
    integrity_counts = {
        key: sum(
            int((row.get("raw_integrity") or {}).get(key) or 0)
            for row in generation_terminals
        )
        for key in (
            "sequence_disagreements",
            "continuity_disagreements",
            "lookahead_violations",
            "identity_conflicts",
        )
    }
    parity_disagreements = sum(
        intent_parity_disagreements(row) for row in generation_terminals
    )
    matched_rows = [
        cohort
        for row in generation_terminals
        for cohort in (row.get("matched_no_trade_cohort") or [])
        if isinstance(cohort, dict)
    ]
    matched_cash_flows = [
        _num(flow)
        for row in matched_rows
        for flow in (row.get("cash_flows_usd") or [])
    ]
    matched_pnl = sum(matched_cash_flows)
    exact_reconciliation = (
        len(windows) == len(generation_terminals)
        and len(windows) == len(set(windows))
        and all(
            (row.get("raw_evidence_checksum") or "")
            == _canonical_checksum(
                {
                    "trade_receipts": row.get("trade_receipts") or [],
                    "book_receipts": row.get("book_receipts") or [],
                }
            )
            for row in generation_terminals
        )
    )
    gates = {
        "resolved_gte_10": len(resolved) >= MIN_RESOLVED,
        "post_cost_positive": sum(resolved) > 0,
        "first_half_positive": bool(first) and sum(first) > 0,
        "second_half_positive": bool(second) and sum(second) > 0,
        "incremental_vs_no_trade_positive": sum(resolved) - matched_pnl > 0,
        "actual_executable_depth": all(
            _num((row.get("signal") or {}).get("executable_ask_depth"))
            >= _num((row.get("intent") or {}).get("shares"))
            for row in intents
        ),
        "raw_input_equals_terminal": exact_reconciliation,
        "zero_sequence_lookahead_identity_parity_disagreement": (
            not any(integrity_counts.values()) and parity_disagreements == 0
        ),
    }
    status = "PAPER_CELL_ACTIVE"
    if len(windows) >= ZERO_INTENT_WINDOWS and not intents:
        status = "PARK_ZERO_INTENT_GENERATION"
    elif len(windows) >= MAX_WINDOWS and (
        len(resolved) < MIN_RESOLVED or not all(gates.values())
    ):
        status = "PARK_FAILED_GATE_BY_TWELVE_WINDOWS"
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
        "matched_no_trade_pnl_usd": round(matched_pnl, 6),
        "matched_no_trade_cash_flow_count": len(matched_cash_flows),
        "incremental_post_cost_pnl_usd": round(sum(resolved) - matched_pnl, 6),
        "first_half_post_cost_pnl_usd": round(sum(first), 6),
        "second_half_post_cost_pnl_usd": round(sum(second), 6),
        "gate_checks": gates,
        "refusal_taxonomy": dict(
            Counter(
                reason
                for row in generation_terminals
                for reason in row.get("blockers") or []
            )
        ),
        "measured_integrity": {
            **integrity_counts,
            "parity_disagreements": parity_disagreements,
        },
        "stop_writer": status.startswith("PARK_"),
    }


def reduce_generation(
    terminals: list[dict[str, Any]],
    events: list[dict[str, Any]],
    resolutions: dict[str, str],
) -> dict[str, Any]:
    """Run independent canonical reductions and persist their equality proof."""
    first = _reduce_generation_core(
        json.loads(json.dumps(terminals)),
        json.loads(json.dumps(events)),
        json.loads(json.dumps(resolutions)),
    )
    second = _reduce_generation_core(
        json.loads(json.dumps(terminals, sort_keys=True)),
        json.loads(json.dumps(events, sort_keys=True)),
        json.loads(json.dumps(resolutions, sort_keys=True)),
    )
    first_checksum = _canonical_checksum(first)
    second_checksum = _canonical_checksum(second)
    reproducible = first_checksum == second_checksum
    first["reducer_reproducibility"] = {
        "first_output_checksum": first_checksum,
        "second_output_checksum": second_checksum,
        "equal": reproducible,
    }
    first["gate_checks"]["two_run_checksum_idempotence"] = reproducible
    if not reproducible and first["status"] == "PROMOTION_HANDOFF_READY":
        first["status"] = (
            "PARK_FAILED_GATE_BY_TWELVE_WINDOWS"
            if first["completed_windows"] >= MAX_WINDOWS
            else "PAPER_CELL_ACTIVE"
        )
        first["stop_writer"] = first["status"].startswith("PARK_")
    return first


def _selector(payload: dict[str, Any], prereg: dict[str, Any]) -> dict[str, Any]:
    if str(payload.get("status") or "").startswith("PARK_"):
        return {
            "schema_version": 1,
            "kind": "btc5m_cross_exchange_promoted_cell_selector_terminal_tombstone",
            "generated_at": payload["generated_at"],
            "status": f"TERMINAL_{payload['status']}",
            "generation_checksum": CHECKSUM,
            "terminal_decision": payload["status"],
            "selected": None,
            "cells": [],
            "gate_pass": False,
            "stop_writer": True,
            "active_capacity": False,
            "due": False,
            "historical_arbiter_only": True,
            "paper_only": True,
            "live_orders_allowed": False,
        }
    checks = {
        "paper_only": True,
        "preregistration_checksum_exact": True,
        "model_checksum_exact": True,
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
        "model_checksum": CHECKSUM,
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


def publish_legacy_arbiter(args: argparse.Namespace) -> dict[str, Any]:
    command = [
        sys.executable,
        str(ROOT / "scripts/arbitrate_btc5m_promoted_cells.py"),
    ]
    for source in ARBITER_SOURCES:
        command.extend(["--source", source])
    command.extend(["--output", args.legacy_arbiter_output, "--max-age-s", "30"])
    completed = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=15,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"legacy arbiter failed rc={completed.returncode}")
    payload = json.loads(completed.stdout)
    if not isinstance(payload, dict):
        raise RuntimeError("legacy arbiter returned non-object")
    return payload


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
    trade_receipts = [
        row
        for row in cache.get("trade_receipts") or []
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
    terminal_id = stable_id("complagt", {"generation": CHECKSUM, "window": window})
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
            book_hash = _canonical_checksum(books[outcome])
            snapshots.append(
                {
                    "generation_checksum": CHECKSUM,
                    "window_start_s": window,
                    "sequence": sequence,
                    "receipt_sequence": sequence,
                    "receipt_timestamp_s": now,
                    "observed_at_s": now,
                    "outcome": outcome,
                    "book_hash": book_hash,
                    "book": books[outcome],
                }
            )
        trades = _fetch_trades(condition_id, args.timeout_s)
        trade_receipts.extend(
            {
                **row,
                "generation_checksum": CHECKSUM,
                "window_start_s": window,
            }
            for row in immutable_trade_receipts(trades, receipt_timestamp_s=now)
        )
    except Exception as exc:  # noqa: BLE001
        fetch_errors.append(f"native_fetch:{type(exc).__name__}")

    existing_event = next(
        (
            row
            for row in events
            if int(row.get("window_start_s") or -1) == window
            and row.get("event") == "native_complement_lead_lag_paper_fill"
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
        tokens = _token_map(market)
        candidates: list[tuple[float, dict[str, Any]]] = []
        for leader, opposite in (("Up", "Down"), ("Down", "Up")):
            features, _ = leader_sell_features(
                trades,
                leader_outcome=leader,
                leader_token=str(tokens.get(leader) or ""),
                opposite_outcome=opposite,
                now=now,
                snapshots=snapshots[:-2],
                current_leader_book=books.get(leader) or {},
                current_opposite_book=books.get(opposite) or {},
            )
            signal, _ = choose_signal(
                features,
                opposite_book=books.get(opposite) or {},
                elapsed_s=elapsed,
            )
            if signal:
                candidates.append((signal["net_edge_per_share"], signal))
        if candidates:
            _, signal = max(candidates, key=lambda row: row[0])
            opposite = str(signal["opposite_outcome"])
            intent = build_intent(
                condition_id=str(
                    market.get("conditionId") or market.get("condition_id") or ""
                ),
                slug=slug,
                token_id=str(tokens.get(opposite) or ""),
                observed_ts=now,
                signal=signal,
            ).asdict()
            event = {
                "schema_version": 1,
                "event": "native_complement_lead_lag_paper_fill",
                "generation_checksum": CHECKSUM,
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

    if window >= FORWARD_START_S and current_terminal is None and elapsed >= 270:
        blockers = list(fetch_errors)
        if existing_event is None:
            tokens = _token_map(market) if market else {}
            for leader, opposite in (("Up", "Down"), ("Down", "Up")):
                features, reasons = leader_sell_features(
                    trades,
                    leader_outcome=leader,
                    leader_token=str(tokens.get(leader) or ""),
                    opposite_outcome=opposite,
                    now=now,
                    snapshots=snapshots[:-2],
                    current_leader_book=books.get(leader) or {},
                    current_opposite_book=books.get(opposite) or {},
                )
                _, signal_reasons = choose_signal(
                    features,
                    opposite_book=books.get(opposite) or {},
                    elapsed_s=elapsed,
                )
                blockers.extend(
                    f"{leader}->{opposite}:{reason}"
                    for reason in [*reasons, *signal_reasons]
                )
        intent = (existing_event or {}).get("intent")
        window_trade_receipts = [
            row
            for row in trade_receipts
            if int(row.get("window_start_s") or -1) == window
        ]
        window_book_receipts = [
            {
                key: row.get(key)
                for key in (
                    "outcome",
                    "receipt_sequence",
                    "receipt_timestamp_s",
                    "book_hash",
                )
            }
            for row in snapshots
            if int(row.get("window_start_s") or -1) == window
        ]
        recorded_at_s = now
        raw_integrity = measured_raw_integrity(
            trade_receipts=window_trade_receipts,
            book_receipts=window_book_receipts,
            terminal_recorded_at_s=recorded_at_s,
        )
        matched_cohort = [
            {
                "trade_identity": row.get("trade_identity"),
                "exchange_timestamp_s": row.get("exchange_timestamp_s"),
                "receipt_timestamp_s": row.get("receipt_timestamp_s"),
                "payload_hash": row.get("payload_hash"),
                "cash_flows_usd": [],
                "orders_executed": 0,
            }
            for row in window_trade_receipts
            if str(row.get("side") or "").upper() == "SELL"
        ]
        raw_evidence = {
            "trade_receipts": window_trade_receipts,
            "book_receipts": window_book_receipts,
        }
        terminal = {
            "schema_version": 1,
            "event": "btc5m_native_complement_lead_lag_terminal",
            "terminal_id": terminal_id,
            "generation_checksum": CHECKSUM,
            "model_checksum": CHECKSUM,
            "cell_id": CELL_ID,
            "window_start_s": window,
            "market_slug": slug,
            "recorded_at": utc_now_iso(),
            "terminal_status": "SIGNAL" if intent else "PROTECTED_SKIP",
            "signal": (intent.get("metadata") or {}).get("features") if intent else None,
            "intent": intent,
            "execution": (existing_event or {}).get("execution") or {},
            "blockers": sorted(set(blockers)),
            "trade_receipts": window_trade_receipts,
            "book_receipts": window_book_receipts,
            "raw_integrity": raw_integrity,
            "raw_evidence_checksum": _canonical_checksum(raw_evidence),
            "matched_no_trade_cohort": matched_cohort,
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
            "kind": "btc5m_native_complement_lead_lag_raw_cache",
            "generated_at": utc_now_iso(),
            "generation_checksum": CHECKSUM,
            "latest_window": window,
            "snapshots": snapshots[-1200:],
            "trade_receipts": trade_receipts[-5000:],
            "latest_trades": trades,
            "paper_only": True,
            "live_orders_allowed": False,
        },
    )
    reduced = reduce_generation(terminals, events, _resolution_map(args.resolutions))
    payload = {
        "schema_version": 1,
        "kind": "btc5m_native_complement_lead_lag_taker_generation",
        "generated_at": utc_now_iso(),
        "flow_stage": "LIVE/ROTATE/DISCOVER/OBSERVE/LEARN/PROMOTE/SELF-DEV",
        "generation_checksum": CHECKSUM,
        "generation_config": CONFIG,
        "cell_id": CELL_ID,
        "preregistration": prereg,
        "frozen_model": {
            "checksum": CHECKSUM,
            "training_cutoff_s": CONFIG["calibration_cutoff_s"],
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
    arbiter = publish_legacy_arbiter(args)
    payload["legacy_arbiter_publication"] = {
        "output": args.legacy_arbiter_output,
        "status": arbiter.get("status"),
        "candidate_count": arbiter.get("candidate_count"),
        "source_diagnostics": arbiter.get("source_diagnostics"),
        "resident_guard_pid_reloaded": False,
    }
    atomic_write_json(_rooted(args.state), payload)
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
    parser.add_argument("--legacy-arbiter-output", default=LEGACY_ARBITER_OUTPUT)
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
