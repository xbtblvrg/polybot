#!/usr/bin/env python3
"""Guarded live execution entrypoint for wallet-copy CopyIntents.

Default mode is a non-mutating live-arm proof. Real CLOB submission requires:

- a PASS profit/live-readiness state,
- a fresh candidate CopyIntent set,
- CLOB token mapping parity,
- --execute-live,
- --explicit-live-operator-go,
- --live-orders-allowed,
- --operator-approval-id.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import hashlib
import json
import math
import os
import re
import requests
import statistics
import sys
import threading
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from functools import partial
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.wallet_copy.execution import (  # noqa: E402
    CopyExecutionAdapter,
    ExecutionGate,
    LiveAdmissionSnapshot,
    LiveExecutionLedgerConfig,
    LiveWalletCopyLifecycle,
    build_copy_intent_parity_capsule,
    promote_intent_for_live,
)
from src.wallet_copy.fees import (  # noqa: E402
    POLYMARKET_EMBEDDED_FEE_FORMULA,
    POLYMARKET_EMBEDDED_FEE_RATE,
    POLYMARKET_EMBEDDED_FEE_SOURCE,
    expected_polymarket_buy_fee_usd,
    modeled_unvalidated_polymarket_buy_fee_usd,
)
from src.wallet_copy.alpha_decay import btc_5m_move_slice_for_values  # noqa: E402
from src.wallet_copy.inventory import InventoryConfig, build_inventory_plans, inventory_plan_to_intent  # noqa: E402
from src.wallet_copy.live_tracker import CLOBMarketClient, GammaMarketClient  # noqa: E402
from src.wallet_copy.mission import (  # noqa: E402
    BEST_ASK_FLOOR_COVERED_COPY_MODELS,
    RULED_01A_GATE_PROBE_PRICE_MIN,
    RULED_01A_ENTRY_PRICE_MIN,
    mission_contract,
)
from src.wallet_copy.models import CopyIntent, WalletEvent, num, stable_id, utc_now_iso  # noqa: E402
from src.wallet_copy.participation import (  # noqa: E402
    PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS,
    annotate_participation_item,
    annotate_participation_window,
    summarize_adjusted_participation,
)
from src.wallet_copy.profit_engine import (  # noqa: E402
    CandidatePolicy,
    intents_for_policy,
    load_events_from_history,
    load_history_window_index,
    policy_accepts_event,
)
from src.wallet_copy.status import ANALYZE, CORRECTION, PASS  # noqa: E402
from src.wallet_copy.store import append_jsonl_many, atomic_write_json, load_json  # noqa: E402
from src.config import Config  # noqa: E402
from src.trade_executor import _market_buy_amount_with_valid_share_precision  # noqa: E402


LIVE_BUILD_MAX_OBSERVED_AGE_S = 3.0
INVENTORY_FAST_ARM_MAX_OBSERVED_AGE_S = 5.0
INVENTORY_MAKER_FALLBACK_PRICE_CEILING = 0.50
INVENTORY_BEST_ASK_MAX_CONCURRENCY = 6
INVENTORY_BEST_ASK_BOOK_CACHE_TTL_S = 2.0
INVENTORY_BEST_ASK_MAX_AGE_AT_GATE_S = 1.0
INVENTORY_FUTURE_WINDOW_LOOKAHEAD_S = 7200.0
INVENTORY_COPY_MODELS = set(BEST_ASK_FLOOR_COVERED_COPY_MODELS)
DRIP_COPY_MODEL = "drip"
PROFIT_LATENCY_WINDOW_TIME_SUPPRESS_GTE_S = 180.0
PROFIT_LATENCY_SIGNAL_AGE_SUPPRESS_GTE_S = 60.0
DRIP_MIN_TRANCHE_USD = 1.0
DRIP_MAX_TRANCHE_USD = 2.5
DRIP_MAX_TRANCHES_PER_WINDOW = 12
DRIP_CLOB_MIN_SHARES = 5.0
MIN_LIVE_FLOOR_PIN_MAX_USD = 2.50
MIN_LIVE_FLOOR_PIN_DIRECTION_ID = "2026-07-16T07:40Z-fable-seat-holder-min-live-pin"
_A689_CANARY_POLICY_ID = "a6896d11_price_reject_canary_0.10_cap_2_le_70"
_A689_CANARY_DRIP_MIN_CAP_USD = 1.0
_F418_READMISSION_WALLET = "0xf418d3a1a941292f9c8707d62a14980c5beb95a3"
_F418_READMISSION_POLICY_ID = "fast_wf_0.10_cap_4_all_prices_minusd_0_all_window"
_F418_READMISSION_CAP_USD = 1.0
DEFAULT_PER_WINDOW_FILL_CAP = 1
WINDOW_FILL_CAP_DIRECTION_ID = "2026-07-11T08:21Z-fable-defend-cap1"
STRONG_TIER_SOURCE_TARGET_MIN_USD = 16.0
STRONG_TIER_WINDOW_BUDGET_MAX_USD = 24.0
STRONG_TIER_MAX_WINDOWS_PER_DAY = 6
STRONG_TIER_MAX_CONCURRENT_WINDOWS = 2
DEFAULT_PROMOTION_ROTATION_STATE = "data/research/wallet_copy_promotion_rotation_state.json"
DEFAULT_RTDS_WATERMARK_STATE = "data/research/wallet_copy_rtds_observation_watermarks.json"
DEFAULT_RTDS_SIGNAL_WATERMARK_STATE = "data/research/wallet_copy_rtds_signal_watermarks.json"
DEFAULT_ENTRY_PRICE_BAND_GATE_CONFIG = "configs/wallet_copy/entry_price_band_gate.json"
AUTO_DEGRADE_ACTIVE_SET_STATE = ROOT / "data/research/wallet_copy_active_set_auto_degrade_state.json"
PASSIVE_AT_SOURCE_HOLDOUT_STATE = ROOT / "data/research/passive_at_source_holdout_latest.json"
UNKNOWN_CLOB_TOKEN_PREFIX = "__wallet_copy_unknown_"
_ALPHA_DECAY_PROFILE_CACHE: dict[str, dict[str, Any]] = {}
_INVENTORY_BEST_ASK_BOOK_CACHE: dict[str, dict[str, Any]] = {}
_INVENTORY_BEST_ASK_BOOK_CACHE_LOCK = threading.Lock()
_LIVE_TRADE_EXECUTOR: Any | None = None
_LIVE_TRADE_EXECUTOR_SIGNATURE: tuple[Any, ...] | None = None
_LIVE_SUBMIT_LOCK = threading.Lock()


def _passive_at_source_holdout_sealed() -> bool:
    holdout = load_json(PASSIVE_AT_SOURCE_HOLDOUT_STATE, default={}, cache_readonly=True)
    if not isinstance(holdout, dict):
        return False
    return bool(
        holdout.get("live_orders_allowed") is False
        or str(holdout.get("verdict") or "") == "CLOSED_TERMINAL_NEGATIVE_FROZEN_COHORT"
    )


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return float(default)
    try:
        return float(raw)
    except (TypeError, ValueError):
        return float(default)


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or str(raw).strip() == "":
        return int(default)
    try:
        return int(raw)
    except (TypeError, ValueError):
        return int(default)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profit-state", default="data/research/wallet_copy_profit_engine_state.json")
    parser.add_argument("--promotion-rotation-state", default=DEFAULT_PROMOTION_ROTATION_STATE)
    parser.add_argument("--history-state", default="data/research/wallet_copy_live_guard_hot_history_state.json")
    parser.add_argument("--history-window-index", default="data/research/wallet_copy_history_window_index.json")
    parser.add_argument("--rtds-watermark-state", default=DEFAULT_RTDS_WATERMARK_STATE)
    parser.add_argument("--rtds-signal-watermark-state", default=DEFAULT_RTDS_SIGNAL_WATERMARK_STATE)
    parser.add_argument("--state", default="data/research/wallet_copy_live_execution_arm_state.json")
    parser.add_argument("--live-ledger-state", default="data/research/wallet_copy_live_execution_state.json")
    parser.add_argument("--live-ledger-event-log", default="data/research/wallet_copy_live_execution_events.jsonl")
    parser.add_argument("--operator-approval-id", default=os.getenv("WALLET_COPY_OPERATOR_APPROVAL_ID", ""))
    parser.add_argument("--execute-live", action="store_true")
    parser.add_argument("--explicit-live-operator-go", action="store_true")
    parser.add_argument("--live-orders-allowed", action="store_true")
    parser.add_argument(
        "--allow-no-fresh-intents",
        action="store_true",
        help="Return an armed/no-op state instead of rc=2 when no candidate intent is fresh enough.",
    )
    parser.add_argument("--runtime-live-paused-flag", default="runtime_live_paused.flag")
    parser.add_argument("--candidate-id", default="", help="Optional candidate override; defaults to runtime admission.")
    parser.add_argument(
        "--selected-candidate-override-state",
        default="",
        help="Guard-written selected runtime candidate override; applied only when it matches --candidate-id.",
    )
    parser.add_argument("--max-intents", type=int, default=1)
    parser.add_argument(
        "--max-event-age-s",
        type=float,
        default=30.0,
        help="Freshness guard for live submission; use 0 only for offline tests/previews.",
    )
    parser.add_argument(
        "--live-build-max-observed-age-s",
        type=float,
        default=LIVE_BUILD_MAX_OBSERVED_AGE_S,
        help=(
            "BTC-5m live build backlog guard: build CopyIntents only from source "
            "events received within this many seconds. Use 0 only for offline tests."
        ),
    )
    parser.add_argument("--gamma-timeout-s", type=float, default=5.0)
    parser.add_argument(
        "--min-live-order-usd",
        type=float,
        default=1.0,
        help="Do not submit live market BUY CopyIntents below the CLOB minimum; skip instead of overcopying.",
    )
    parser.add_argument("--max-window-usd", type=float, default=10.0)
    parser.add_argument("--max-per-wallet-usd", type=float, default=2.0)
    parser.add_argument("--min-inventory-plan-usd", type=float, default=1.0)
    parser.add_argument("--min-agreeing-wallets", type=int, default=2)
    parser.add_argument("--max-price-spread", type=float, default=0.08)
    parser.add_argument(
        "--wallet-copy-max-buy-price",
        type=float,
        default=0.0,
        help="Hard live BUY entry cap. BUY CopyIntents above this price are filtered before live parity/execution.",
    )
    parser.add_argument(
        "--wallet-copy-min-buy-price",
        type=float,
        default=0.0,
        help="Hard live BUY entry floor. BUY CopyIntents below this price are filtered before live parity/execution.",
    )
    parser.add_argument(
        "--profit-latency-window-time-suppress-gte-s",
        type=float,
        default=_env_float(
            "WALLET_COPY_PROFIT_LATENCY_WINDOW_TIME_SUPPRESS_GTE_S",
            PROFIT_LATENCY_WINDOW_TIME_SUPPRESS_GTE_S,
        ),
        help="Suppress live BUY submissions at or beyond this BTC-5m window age; 0 disables.",
    )
    parser.add_argument(
        "--profit-latency-signal-age-suppress-gte-s",
        type=float,
        default=_env_float(
            "WALLET_COPY_PROFIT_LATENCY_SIGNAL_AGE_SUPPRESS_GTE_S",
            PROFIT_LATENCY_SIGNAL_AGE_SUPPRESS_GTE_S,
        ),
        help="Suppress live BUY submissions whose source signal age is at or beyond this many seconds; 0 disables.",
    )
    parser.add_argument("--alpha-decay-report", default="data/research/alpha_decay_report.json")
    parser.add_argument("--toxicity-denylist-config", default="configs/wallet_copy/toxicity_denylist.json")
    parser.add_argument("--entry-price-band-gate-config", default=DEFAULT_ENTRY_PRICE_BAND_GATE_CONFIG)
    parser.add_argument("--enable-drift-buffer", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--max-drift-buffer-price", type=float, default=0.05)
    parser.add_argument("--enable-maker-fallback", action=argparse.BooleanOptionalAction, default=False)
    parser.add_argument(
        "--copy-model",
        choices=("per_order", "inventory", "drip"),
        default="drip",
        help="LIVE copy model. drip emits micro-tranches while preserving inventory target logic.",
    )
    parser.add_argument(
        "--inventory-late-window-stop-s",
        type=float,
        default=60.0,
        help="Stop inventory convergence when this many seconds or less remain in a BTC-5m window.",
    )
    parser.add_argument(
        "--inventory-max-converge-orders-per-window",
        type=int,
        default=6,
        help="Maximum live inventory convergence attempts per source wallet/window/outcome.",
    )
    parser.add_argument(
        "--inventory-best-ask-timeout-s",
        type=float,
        default=1.0,
        help="CLOB /book timeout for the pre-submit inventory best-ask gate.",
    )
    parser.add_argument(
        "--inventory-future-window-lookahead-s",
        type=float,
        default=INVENTORY_FUTURE_WINDOW_LOOKAHEAD_S,
        help=(
            "Include indexed source-wallet BTC-5m windows this far ahead for inventory copy. "
            "Default 0 keeps live arming scoped to current/previous windows."
        ),
    )
    parser.add_argument("--drip-min-tranche-usd", type=float, default=DRIP_MIN_TRANCHE_USD)
    parser.add_argument("--drip-max-tranche-usd", type=float, default=DRIP_MAX_TRANCHE_USD)
    parser.add_argument("--drip-max-tranches-per-window", type=int, default=DRIP_MAX_TRANCHES_PER_WINDOW)
    parser.add_argument(
        "--per-window-fill-cap",
        type=int,
        default=_env_int("WALLET_COPY_PER_WINDOW_FILL_CAP", DEFAULT_PER_WINDOW_FILL_CAP),
        help=(
            "Maximum FILLED or still-SUBMITTED live rows allowed per BTC-5m market_slug "
            "across all members before later intents are skipped with window_fill_cap; 0 disables."
        ),
    )
    return parser.parse_args()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _runtime_copy_contract() -> dict[str, Any]:
    runtime_phase = _as_dict(mission_contract().get("current_runtime_phase_contract"))
    return {
        "copy_mode": runtime_phase.get("live_mode"),
        "copy_style": runtime_phase.get("copy_style"),
        "profitability_first": bool(runtime_phase.get("profitability_first")),
        "strict_source_order_1_to_1_required": bool(runtime_phase.get("strict_source_order_1_to_1_required")),
        "selected_intent_parity_required": bool(runtime_phase.get("selected_intent_parity_required")),
        "profitability_filter": _as_dict(runtime_phase.get("profitability_filter_contract")),
    }


def _mission_primary_live_candidate() -> dict[str, Any]:
    runtime_phase = _as_dict(mission_contract().get("current_runtime_phase_contract"))
    return _as_dict(runtime_phase.get("primary_live_candidate"))


def _mission_active_live_set_contract() -> dict[str, Any]:
    runtime_phase = _as_dict(mission_contract().get("current_runtime_phase_contract"))
    return _as_dict(runtime_phase.get("active_live_set"))


def _mission_active_live_set_is_empty(active_set: dict[str, Any] | None = None) -> bool:
    active_set = active_set if isinstance(active_set, dict) else _mission_active_live_set_contract()
    return bool(active_set.get("live_set_empty_until_replacement"))


def _mission_active_set_defensive_sizing() -> dict[str, Any]:
    active_set = _mission_active_live_set_contract()
    sizing = active_set.get("overnight_defensive_sizing")
    return sizing if isinstance(sizing, dict) and sizing.get("enabled") else {}


def _apply_mission_active_set_defensive_sizing(members: list[dict[str, Any]]) -> list[dict[str, Any]]:
    sizing = _mission_active_set_defensive_sizing()
    if not sizing:
        return members
    try:
        multiplier = float(sizing.get("budget_multiplier"))
    except (TypeError, ValueError):
        multiplier = 1.0
    if multiplier <= 0 or multiplier >= 1:
        return members
    adjusted: list[dict[str, Any]] = []
    for row in members:
        member = dict(row)
        if member.get("max_order_usd") is not None:
            member["max_order_usd"] = round(float(member.get("max_order_usd") or 0.0) * multiplier, 6)
        policy = dict(member.get("policy")) if isinstance(member.get("policy"), dict) else {}
        if policy.get("max_order_usd") is not None:
            policy["max_order_usd"] = round(float(policy.get("max_order_usd") or 0.0) * multiplier, 6)
            member["policy"] = policy
        member["defensive_sizing"] = {
            "direction_id": sizing.get("direction_id"),
            "budget_multiplier": multiplier,
            "strong_tier_suspended": bool(sizing.get("strong_tier_suspended")),
        }
        adjusted.append(member)
    return adjusted


def _mission_strong_tier_suspended() -> bool:
    sizing = _mission_active_set_defensive_sizing()
    return bool(sizing.get("strong_tier_suspended"))


def _mission_active_live_set_members() -> list[dict[str, Any]]:
    active_set = _mission_active_live_set_contract()
    if _mission_active_live_set_is_empty(active_set):
        return []
    rows = active_set.get("members") if isinstance(active_set.get("members"), list) else []
    members = [dict(row) for row in rows if isinstance(row, dict) and row.get("enabled") is not False]
    overlay = load_json(AUTO_DEGRADE_ACTIVE_SET_STATE, default={})
    overlay_rows = overlay.get("members") if isinstance(overlay, dict) and isinstance(overlay.get("members"), list) else []
    if members and overlay_rows:
        existing_wallets = {
            str(row.get("source_wallet") or row.get("wallet") or "").lower()
            for row in members
        }
        for row in overlay_rows:
            if not isinstance(row, dict) or row.get("enabled") is False:
                continue
            wallet = str(row.get("source_wallet") or row.get("wallet") or "").lower()
            if not wallet or wallet in existing_wallets:
                continue
            members.append(dict(row))
            existing_wallets.add(wallet)
    if members:
        return _apply_mission_active_set_defensive_sizing(members)
    if rows:
        return []
    primary = _mission_primary_live_candidate()
    return _apply_mission_active_set_defensive_sizing([primary]) if primary else []


_ACTIVE_SET_LIVE_ADMISSIBLE_STATUSES = {
    PASS,
    "AUTO_DEGRADE_PROTECTION_BOUNDED",
    "AUTO_DEGRADE_RUNTIME_ROSTER_PROTECTION_BOUNDED",
    "FABLE_0608_ADMITTED_STANDARD_POLICY",
    "FABLE_1415_BACKFILL_RANKED_QUEUE_TOP",
    "FABLE_1622_RANK1_READY_QUEUE_ADMITTED",
    "FABLE_1718_RANK2_READY_QUEUE_ADMITTED",
    "FABLE_1934_HALF_SIZE_PIN_CLEARANCE_READY",
    "FABLE_2145_BUCKET_CONCENTRATION_READMIT_PROVEN_CELL",
    "FABLE_PIN_BOOKCOVERED_PASS",
    "PRE_LOCK_MEMBER",
    "MEMBER_BAR_QUALIFIED",
}


def _runtime_policy_from_mission(policy_id: str) -> dict[str, Any]:
    profitability_filter = _as_dict(
        _as_dict(mission_contract().get("current_runtime_phase_contract")).get(
            "profitability_filter_contract"
        )
    )
    return {
        "policy_id": str(policy_id or profitability_filter.get("policy_id") or ""),
        "min_price": num(profitability_filter.get("min_price"), 0.01),
        "max_price": num(profitability_filter.get("max_price"), 1.0),
        "min_wallet_usdc": num(profitability_filter.get("min_wallet_usdc"), 0.0),
        "max_wallet_usdc": num(profitability_filter.get("max_wallet_usdc"), 0.0),
        "min_seconds_from_open": profitability_filter.get("min_seconds_from_open"),
        "max_seconds_from_open": profitability_filter.get("max_seconds_from_open"),
        "wallet_fraction": num(profitability_filter.get("wallet_fraction"), 0.10),
        "max_order_usd": num(profitability_filter.get("max_order_usd"), 4.0),
        "min_order_usd": num(profitability_filter.get("min_live_order_usd"), 1.0),
    }


def _policy_from_mission_member(member: dict[str, Any], *, fallback_policy_id: str = "") -> dict[str, Any]:
    policy_id = str(member.get("policy_id") or fallback_policy_id or "")
    policy = _runtime_policy_from_mission(policy_id)
    member_policy = _as_dict(member.get("policy"))
    for key in (
        "policy_id",
        "min_price",
        "max_price",
        "min_wallet_usdc",
        "max_wallet_usdc",
        "min_seconds_from_open",
        "max_seconds_from_open",
        "wallet_fraction",
        "max_order_usd",
        "min_order_usd",
        "late_window_stop_s",
        "drip_min_tranche_usd",
        "drip_max_tranche_usd",
        "drip_max_tranches_per_window",
        "maker_min_share_funding_cap_usd",
        "maker_min_share_original_policy_cap_usd",
        "maker_min_share_base_request_cap_usd",
        "maker_fallback_defense_cap_usd",
    ):
        if key in member_policy and member_policy.get(key) is not None:
            policy[key] = member_policy.get(key)
    top_level_numeric_fields = {
        "max_price": "max_price",
        "wallet_fraction": "wallet_fraction",
        "max_order_usd": "max_order_usd",
        "min_live_order_usd": "min_order_usd",
        # The guard's probe-cap defense deliberately publishes these fields
        # both on the member and in its policy.  Fast-lane member snapshots can
        # carry only the top-level copy, so preserve the venue-minimum contract
        # instead of silently replacing it with CandidatePolicy's zero defaults.
        "maker_min_share_funding_cap_usd": "maker_min_share_funding_cap_usd",
        "maker_min_share_original_policy_cap_usd": "maker_min_share_original_policy_cap_usd",
        "maker_min_share_base_request_cap_usd": "maker_min_share_base_request_cap_usd",
        "maker_fallback_defense_cap_usd": "maker_fallback_defense_cap_usd",
    }
    for member_key, policy_key in top_level_numeric_fields.items():
        if member.get(member_key) is not None:
            policy[policy_key] = num(member.get(member_key), policy.get(policy_key))
    if policy_id and not policy.get("policy_id"):
        policy["policy_id"] = policy_id
    return policy


def _mission_active_member_candidate(
    *,
    candidate_id_pin: str = "",
    source_wallet_pin: str = "",
    policy_id_pin: str = "",
) -> dict[str, Any]:
    candidate_id_pin = str(candidate_id_pin or "")
    source_wallet_pin = str(source_wallet_pin or "").lower()
    policy_id_pin = str(policy_id_pin or "")
    for member in _mission_active_live_set_members():
        candidate_id = str(member.get("candidate_id") or "")
        source_wallet = str(member.get("source_wallet") or member.get("wallet") or "").lower()
        policy_id = str(member.get("policy_id") or policy_id_pin or "")
        if candidate_id_pin and candidate_id != candidate_id_pin:
            continue
        if source_wallet_pin and source_wallet != source_wallet_pin:
            continue
        if policy_id_pin and policy_id and policy_id != policy_id_pin:
            continue
        if not candidate_id or not source_wallet:
            continue
        policy = _policy_from_mission_member(member, fallback_policy_id=policy_id)
        member_status = str(member.get("status") or PASS)
        runtime_status = PASS if member_status in _ACTIVE_SET_LIVE_ADMISSIBLE_STATUSES else member_status
        summary = _as_dict(member.get("summary"))
        summary.setdefault("active_set_member_status", member_status)
        return {
            "candidate_id": candidate_id,
            "candidate_type": str(member.get("candidate_type") or "SINGLE_WALLET"),
            "status": runtime_status,
            "source_wallet": source_wallet,
            "policy_id": policy.get("policy_id"),
            "policy": policy,
            "metadata": {
                "source": "mission_active_live_set",
                "source_wallet": source_wallet,
                "wallet_name": member.get("wallet_name") or member.get("name") or f"active_set_{source_wallet[-8:]}",
                "active_set_member_id": member.get("member_id") or candidate_id,
                "active_set_role": member.get("role") or "member",
                "active_set_member_status": member_status,
            },
            "summary": summary,
            "admission_identity": {"source_wallet": source_wallet, "policy_id": policy.get("policy_id")},
            "runtime_copy_evidence_resolved_blockers": [],
            "live_target_profile": {
                "status": PASS,
                "blockers": [],
                "operator_approval_id": "OP-LIVE-20260703-BELA",
                "source": "mission_active_live_set",
            },
        }
    return {}


def _promotion_rotation_runtime_candidate(
    promotion_rotation_state: str,
    *,
    candidate_id_pin: str = "",
    source_wallet_pin: str = "",
    policy_id_pin: str = "",
) -> dict[str, Any]:
    state = load_json(promotion_rotation_state or DEFAULT_PROMOTION_ROTATION_STATE, default={})
    state = state if isinstance(state, dict) else {}
    decision = _as_dict(state.get("decision"))
    best = _as_dict(_as_dict(state.get("paper_promotion")).get("best_candidate"))
    if decision.get("status") != PASS or decision.get("action") != "FABLE_ROTATION_DECISION_READY":
        return {}
    if decision.get("rotation_application_allowed") is not True:
        return {}
    if not bool(decision.get("rotation_triggered")):
        return {}
    if best.get("status") != PASS:
        return {}

    primary = _mission_primary_live_candidate()
    candidate_id = str(candidate_id_pin or primary.get("candidate_id") or "")
    source_wallet = str(source_wallet_pin or primary.get("source_wallet") or "").lower()
    policy_id = str(policy_id_pin or primary.get("policy_id") or best.get("best_policy_id") or "")
    if not candidate_id or not source_wallet or not policy_id:
        return {}
    if str(decision.get("best_candidate_wallet") or "").lower() != source_wallet:
        return {}
    if str(best.get("wallet") or "").lower() != source_wallet:
        return {}
    if str(best.get("best_policy_id") or "") != policy_id:
        return {}

    paper_pnl = num(best.get("best_policy_paper_pnl_usd"), 0.0)
    copyable_orders = int(best.get("best_policy_copyable_buy_events") or 0)
    if paper_pnl <= 0 or copyable_orders <= 0:
        return {}

    policy = _runtime_policy_from_mission(policy_id)
    return {
        "candidate_id": candidate_id,
        "candidate_type": "SINGLE_WALLET",
        "status": PASS,
        "source_wallet": source_wallet,
        "policy_id": policy_id,
        "policy": policy,
        "metadata": {
            "source": "promotion_rotation_state",
            "source_wallet": source_wallet,
            "wallet_name": best.get("wallet_name") or f"promotion_rotation_{source_wallet[-8:]}",
            "fable_rotation_decision": True,
            "promotion_rotation_generated_at": state.get("generated_at"),
            "paper_pnl_usd": paper_pnl,
            "copyable_buy_events": copyable_orders,
        },
        "summary": {
            "paper_pnl_usd": paper_pnl,
            "copyable_buy_events": copyable_orders,
            "evidence_source": best.get("evidence_source"),
        },
        "admission_identity": {"source_wallet": source_wallet, "policy_id": policy_id},
        "runtime_copy_evidence_resolved_blockers": [],
        "live_target_profile": {
            "status": PASS,
            "blockers": [],
            "operator_approval_id": "OP-LIVE-20260703-BELA",
            "source": "promotion_rotation_state",
        },
    }


def _mission_runtime_candidate_policy(candidate: dict[str, Any]) -> dict[str, Any]:
    if not candidate:
        return candidate
    runtime_phase = _as_dict(mission_contract().get("current_runtime_phase_contract"))
    primary = _as_dict(runtime_phase.get("primary_live_candidate"))
    profitability_filter = _as_dict(runtime_phase.get("profitability_filter_contract"))
    if not profitability_filter:
        return candidate

    candidate_id = str(candidate.get("candidate_id") or "")
    primary_candidate_id = str(primary.get("candidate_id") or "")
    candidate_wallet = _candidate_source_wallet(candidate)
    candidate_policy_id = str(_as_dict(candidate.get("policy")).get("policy_id") or candidate.get("policy_id") or "")
    matched_member = {}
    if primary_candidate_id and candidate_id == primary_candidate_id:
        matched_member = primary
    elif str(primary.get("source_wallet") or "").lower() == candidate_wallet and (
        not primary.get("policy_id") or str(primary.get("policy_id")) == candidate_policy_id
    ):
        matched_member = primary
    else:
        for member in _mission_active_live_set_members():
            member_candidate_id = str(member.get("candidate_id") or "")
            member_wallet = str(member.get("source_wallet") or member.get("wallet") or "").lower()
            member_policy_id = str(member.get("policy_id") or "")
            if member_candidate_id and candidate_id == member_candidate_id:
                matched_member = member
                break
            if member_wallet and member_wallet == candidate_wallet and (
                not member_policy_id or not candidate_policy_id or member_policy_id == candidate_policy_id
            ):
                matched_member = member
                break
    if not matched_member:
        return candidate

    policy = _as_dict(candidate.get("policy"))
    aligned_policy = dict(policy)
    member_policy = _policy_from_mission_member(matched_member, fallback_policy_id=candidate_policy_id)
    changed = False
    for key, value in member_policy.items():
        if value is not None and aligned_policy.get(key) != value:
            aligned_policy[key] = value
            changed = True
    if not changed:
        return candidate

    aligned = dict(candidate)
    aligned["policy"] = aligned_policy
    if aligned_policy.get("policy_id"):
        aligned["policy_id"] = aligned_policy.get("policy_id")
    return aligned


_RUNTIME_COPY_RESOLVABLE_BLOCKERS = {
    "candidate_missing_clob_fill_evidence",
    "candidate_window_order_concentration_above_maximum",
    "candidate_window_order_concentration_ratio_above_maximum",
}

_ADVISORY_BLOCKER_PREFIXES_WHEN_LIVE_TARGET_PASS = (
    "development_program_bridge_pending_current_poll_inventory",
)


def _runtime_resolvable_or_advisory_blockers(candidate: dict[str, Any]) -> set[str]:
    live_target_profile = _as_dict(candidate.get("live_target_profile"))
    live_target_pass = live_target_profile.get("status") == PASS
    blockers = {str(blocker) for blocker in candidate.get("blockers") or [] if str(blocker)}
    blockers.update(
        str(blocker)
        for blocker in candidate.get("runtime_copy_evidence_resolved_blockers") or []
        if str(blocker)
    )
    resolved = {blocker for blocker in blockers if blocker in _RUNTIME_COPY_RESOLVABLE_BLOCKERS}
    if live_target_pass:
        resolved.update(
            blocker
            for blocker in blockers
            if any(blocker.startswith(prefix) for prefix in _ADVISORY_BLOCKER_PREFIXES_WHEN_LIVE_TARGET_PASS)
        )
    return resolved


def _hard_paper_blockers_for_candidate(profit_state: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    """Return paper blockers that runtime CLOB proof is not allowed to clear."""

    candidate_id = str(candidate.get("candidate_id") or "")
    blockers: list[str] = []
    candidate_resolvable = _runtime_resolvable_or_advisory_blockers(candidate)
    for blocker in candidate.get("runtime_copy_evidence_resolved_blockers") or []:
        blocker = str(blocker)
        if blocker and blocker not in candidate_resolvable:
            blockers.append(f"runtime_resolved_hard_paper_blocker:{blocker}")
    if not candidate_id:
        return sorted(set(blockers))

    paper_rows: list[dict[str, Any]] = []
    for key in ("ranked_candidates", "pass_candidates"):
        for row in profit_state.get(key) or []:
            if isinstance(row, dict):
                paper_rows.append(row)
    for key in ("best_candidate", "forward_candidate"):
        row = profit_state.get(key)
        if isinstance(row, dict):
            paper_rows.append(row)

    for row in paper_rows:
        if str(row.get("candidate_id") or "") != candidate_id:
            continue
        row_resolvable = _runtime_resolvable_or_advisory_blockers(row)
        for blocker in row.get("blockers") or []:
            blocker = str(blocker)
            if blocker and blocker not in row_resolvable:
                blockers.append(f"paper_ranked_candidate_blocker:{blocker}")
        break
    return sorted(set(blockers))


def _selected_candidate(payload: dict[str, Any], *, override_id: str = "") -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for key in (
        "runtime_admission_candidate",
        "forward_runtime_candidate",
        "best_runtime_candidate",
        "best_candidate",
        "forward_candidate",
    ):
        row = payload.get(key)
        if isinstance(row, dict):
            candidates.append(row)
    for row in payload.get("forward_queue_runtime_candidates") or []:
        if isinstance(row, dict):
            candidates.append(row)

    wanted = str(override_id or (_as_dict(payload.get("decision")).get("runtime_admission_candidate_id") or ""))
    if wanted:
        for candidate in candidates:
            if str(candidate.get("candidate_id") or "") == wanted:
                return candidate

    for candidate in candidates:
        if str(candidate.get("status") or "") == PASS:
            return candidate
    return candidates[0] if candidates else {}


def _selected_candidate_override(args: argparse.Namespace) -> dict[str, Any]:
    path = str(getattr(args, "selected_candidate_override_state", "") or "").strip()
    wanted = str(getattr(args, "candidate_id", "") or "").strip()
    if not path or not wanted:
        return {}
    state = load_json(path, default={})
    if not isinstance(state, dict):
        return {}
    candidate = state.get("candidate") if isinstance(state.get("candidate"), dict) else state
    if not isinstance(candidate, dict):
        return {}
    if str(candidate.get("candidate_id") or "") != wanted:
        return {}
    return dict(candidate)


def _apply_selected_candidate_override(candidate: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    if not candidate or not override:
        return candidate
    if str(candidate.get("candidate_id") or "") != str(override.get("candidate_id") or ""):
        return candidate
    override_policy = _as_dict(override.get("policy"))
    if not override_policy:
        return candidate

    updated = dict(candidate)
    policy = dict(_as_dict(updated.get("policy")))
    policy.update({key: value for key, value in override_policy.items() if value is not None})
    updated["policy"] = policy
    if policy.get("policy_id"):
        updated["policy_id"] = policy.get("policy_id")
    if override.get("source_wallet"):
        updated["source_wallet"] = str(override.get("source_wallet") or "").lower()
    updated["selected_candidate_override_applied"] = {
        "enabled": True,
        "source": "guard_selected_candidate_override_state",
        "candidate_id": updated.get("candidate_id"),
        "source_wallet": updated.get("source_wallet"),
        "policy_max_order_usd": policy.get("max_order_usd"),
        "policy_drip_max_tranche_usd": policy.get("drip_max_tranche_usd"),
    }
    return updated


def _candidate_policy(candidate: dict[str, Any]) -> CandidatePolicy | None:
    policy = _as_dict(candidate.get("policy"))
    policy_id = str(policy.get("policy_id") or "")
    if not policy_id:
        return None
    return CandidatePolicy(
        policy_id=policy_id,
        min_price=num(policy.get("min_price"), 0.01),
        max_price=num(policy.get("max_price"), 1.0),
        min_wallet_usdc=num(policy.get("min_wallet_usdc"), 0.0),
        max_wallet_usdc=num(policy.get("max_wallet_usdc"), 0.0),
        min_seconds_from_open=(
            None if policy.get("min_seconds_from_open") is None else num(policy.get("min_seconds_from_open"))
        ),
        max_seconds_from_open=(
            None if policy.get("max_seconds_from_open") is None else num(policy.get("max_seconds_from_open"))
        ),
        wallet_fraction=num(policy.get("wallet_fraction"), 0.05),
        max_order_usd=num(policy.get("max_order_usd"), 2.0),
        min_order_usd=num(policy.get("min_order_usd"), 0.0),
        maker_min_share_funding_cap_usd=num(
            policy.get("maker_min_share_funding_cap_usd"), 0.0
        ),
        maker_min_share_original_policy_cap_usd=num(
            policy.get("maker_min_share_original_policy_cap_usd"), 0.0
        ),
        maker_min_share_base_request_cap_usd=num(
            policy.get("maker_min_share_base_request_cap_usd"), 0.0
        ),
        maker_fallback_defense_cap_usd=num(
            policy.get("maker_fallback_defense_cap_usd"), 0.0
        ),
    )


def _candidate_late_window_stop_s(candidate: dict[str, Any], args: argparse.Namespace) -> float:
    default = float(getattr(args, "inventory_late_window_stop_s", 60.0))
    policy = _as_dict(candidate.get("policy"))
    raw = policy.get("late_window_stop_s", candidate.get("late_window_stop_s"))
    if raw is None:
        return default
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return value if value >= 0.0 else default


def _candidate_source_wallet(candidate: dict[str, Any]) -> str:
    metadata = _as_dict(candidate.get("metadata"))
    return str(candidate.get("source_wallet") or metadata.get("source_wallet") or "").lower()


def _min_live_floor_pin_enabled_for_candidate(
    candidate: dict[str, Any],
    policy: CandidatePolicy | None = None,
) -> bool:
    candidate_id = str(candidate.get("candidate_id") or "").strip()
    source_wallet = _candidate_source_wallet(candidate)
    if not candidate_id or not source_wallet:
        return False
    return True


def _intent_age_s(intent: CopyIntent, *, now_ts: float) -> float | None:
    ts = intent.event_ts if intent.event_ts is not None else intent.observed_ts
    if ts is None or float(ts) <= 0:
        return None
    return max(0.0, now_ts - float(ts))


def _intent_observed_age_s(intent: CopyIntent, *, now_ts: float) -> float | None:
    if intent.observed_ts is None or float(intent.observed_ts) <= 0:
        return None
    return max(0.0, now_ts - float(intent.observed_ts))


def _intent_observation_event_age_s(intent: CopyIntent) -> float | None:
    if intent.api_latency_s is not None and float(intent.api_latency_s) >= 0:
        return max(0.0, float(intent.api_latency_s))
    if (
        intent.event_ts is not None
        and intent.observed_ts is not None
        and float(intent.event_ts) > 0
        and float(intent.observed_ts) > 0
    ):
        return max(0.0, float(intent.observed_ts) - float(intent.event_ts))
    return None


def _btc_5m_window_start_s(market_slug: str) -> float | None:
    slug = str(market_slug or "")
    if not slug.startswith("btc-updown-5m-"):
        return None
    marker = slug.rsplit("-", 1)[-1]
    if not marker.isdigit():
        return None
    return float(marker)


def _event_btc_5m_window_start_s(event: WalletEvent) -> float | None:
    window_start = _btc_5m_window_start_s(event.market_slug)
    if window_start is not None:
        return window_start
    slug_text = " ".join(
        str(value or "").lower()
        for value in (event.market_slug, event.event_slug, event.title)
    )
    if "bitcoin-up-or-down" not in slug_text and "bitcoin up or down" not in slug_text:
        return None
    if "5 minutes" not in slug_text and not re.search(
        r"\b\d{1,2}:\d{2}\s*(?:am|pm)\s*-\s*\d{1,2}:\d{2}\s*(?:am|pm)\b",
        slug_text,
    ):
        return None
    basis_ts = float(event.event_ts or 0.0)
    if basis_ts <= 0:
        return None
    if event.window_start_s is not None and float(event.window_start_s) > 0:
        return float(event.window_start_s)
    return float(int(basis_ts // 300.0) * 300)


def _canonical_btc_5m_market_slug(event: WalletEvent) -> str:
    window_start = _event_btc_5m_window_start_s(event)
    if window_start is None:
        return str(event.market_slug or event.condition_id or "")
    return f"btc-updown-5m-{int(window_start)}"


def _event_runtime_diagnostics(
    event: WalletEvent,
    *,
    now_ts: float,
    max_event_age_s: float,
    live_build_max_observed_age_s: float = LIVE_BUILD_MAX_OBSERVED_AGE_S,
) -> dict[str, Any]:
    event_age = None
    if event.event_ts is not None and float(event.event_ts) > 0:
        event_age = max(0.0, now_ts - float(event.event_ts))
    observed_age = None
    if event.observed_ts is not None and float(event.observed_ts) > 0:
        observed_age = max(0.0, now_ts - float(event.observed_ts))
    if event.api_latency_s is not None and float(event.api_latency_s) >= 0:
        observation_event_age = max(0.0, float(event.api_latency_s))
    elif event.event_ts is not None and event.observed_ts is not None and float(event.event_ts) > 0:
        observation_event_age = max(0.0, float(event.observed_ts) - float(event.event_ts))
    else:
        observation_event_age = None

    window_start = _event_btc_5m_window_start_s(event)
    btc_5m_scope_ok = window_start is not None
    window_close = None if window_start is None else window_start + 300.0
    observed_after_close = bool(window_close is not None and float(event.observed_ts) > window_close)
    market_closed_now = bool(window_close is not None and now_ts >= window_close)
    fresh_by_event_age = bool(
        max_event_age_s <= 0
        or (event_age is not None and event_age <= float(max_event_age_s))
    )
    fresh_by_observation_event_age = bool(
        max_event_age_s <= 0
        or (
            observation_event_age is not None
            and observation_event_age <= float(max_event_age_s)
            and observed_age is not None
            and observed_age <= float(max_event_age_s)
        )
    )
    fresh_for_live_build = bool(
        live_build_max_observed_age_s <= 0
        or (observed_age is not None and observed_age <= float(live_build_max_observed_age_s))
    )
    is_buy = str(event.action or "").upper() == "BUY"
    live_tradeable_window_open = bool(
        is_buy
        and btc_5m_scope_ok
        and (fresh_by_event_age or fresh_by_observation_event_age)
        and fresh_for_live_build
        and not market_closed_now
        and not observed_after_close
    )
    return {
        "event_id": event.event_id,
        "market_slug": event.market_slug,
        "action": event.action,
        "outcome": event.outcome,
        "price": round(float(event.price), 6),
        "usdc_size": round(float(event.usdc_size), 6),
        "event_age_s": None if event_age is None else round(event_age, 6),
        "observed_age_s": None if observed_age is None else round(observed_age, 6),
        "observation_event_age_s": None if observation_event_age is None else round(observation_event_age, 6),
        "window_start_s": window_start,
        "window_close_s": window_close,
        "btc_5m_scope_ok": btc_5m_scope_ok,
        "market_closed_now": market_closed_now,
        "observed_after_market_close": observed_after_close,
        "fresh_by_event_age": fresh_by_event_age,
        "fresh_by_observation_event_age": fresh_by_observation_event_age,
        "fresh_for_live_build": fresh_for_live_build,
        "live_build_max_observed_age_s": round(float(live_build_max_observed_age_s), 6),
        "live_tradeable_window_open": live_tradeable_window_open,
    }


def _drop_closed_btc5m_feedstock(
    events: list[WalletEvent],
    *,
    now_ts: float,
) -> tuple[list[WalletEvent], dict[str, Any]]:
    """Keep closed BTC-5m history out of the live intent builder.

    This is a feedstock/cadence correction only.  Unknown/non-BTC events stay
    available to the existing scope diagnostics, and no freshness or policy
    threshold changes value.
    """
    retained: list[WalletEvent] = []
    dropped: list[dict[str, Any]] = []
    for event in events:
        window_start = _event_btc_5m_window_start_s(event)
        if window_start is not None and now_ts >= window_start + 300.0:
            dropped.append(
                {
                    "event_id": event.event_id,
                    "market_slug": event.market_slug,
                    "window_start_s": window_start,
                    "window_close_s": window_start + 300.0,
                    "reason": "market_closed_before_intent_build",
                }
            )
            continue
        retained.append(event)
    return retained, {
        "enabled": True,
        "input_events": len(events),
        "retained_events": len(retained),
        "dropped_events": len(dropped),
        "drop_counts": (
            {"market_closed_before_intent_build": len(dropped)} if dropped else {}
        ),
        "sample_dropped_events": dropped[:20],
    }


def _live_candidate_build_events(
    events: list[WalletEvent],
    policy: CandidatePolicy,
    *,
    now_ts: float,
    max_event_age_s: float,
    live_build_max_observed_age_s: float = LIVE_BUILD_MAX_OBSERVED_AGE_S,
) -> tuple[list[WalletEvent], dict[str, Any]]:
    retained: list[WalletEvent] = []
    skip_counts: Counter[str] = Counter()
    policy_reject_counts: Counter[str] = Counter()
    latest: dict[str, Any] = {}
    latest_key: tuple[float, str] = (-1.0, "")
    samples: list[dict[str, Any]] = []
    sample_limit = 20

    ordered_events = sorted(
        events,
        key=lambda event: (event.observed_ts or event.event_ts or 0.0, event.event_id),
        reverse=True,
    )

    for index, event in enumerate(ordered_events):
        diagnostics = _event_runtime_diagnostics(
            event,
            now_ts=now_ts,
            max_event_age_s=max_event_age_s,
            live_build_max_observed_age_s=live_build_max_observed_age_s,
        )
        event_key = (
            float(event.event_ts if event.event_ts is not None else event.observed_ts or 0.0),
            str(event.event_id or ""),
        )
        if event_key > latest_key:
            latest_key = event_key
            latest = diagnostics

        if live_build_max_observed_age_s > 0 and diagnostics.get("fresh_for_live_build") is False:
            remaining = len(ordered_events) - index
            skip_counts["stale_build_observed_age"] += remaining
            if len(samples) < sample_limit:
                samples.append({**diagnostics, "skip_reason": "stale_build_observed_age"})
            break

        if not diagnostics["live_tradeable_window_open"]:
            if str(event.action or "").upper() != "BUY":
                reason = "not_buy"
            elif not diagnostics["btc_5m_scope_ok"]:
                reason = "not_btc_5m"
            elif diagnostics["market_closed_now"]:
                reason = "market_closed_now"
            elif diagnostics["observed_after_market_close"]:
                reason = "observed_after_market_close"
            elif not diagnostics["fresh_for_live_build"]:
                reason = "stale_build_observed_age"
            else:
                reason = "stale"
            skip_counts[reason] += 1
            if len(samples) < sample_limit:
                samples.append({**diagnostics, "skip_reason": reason})
            continue

        accepted, policy_reason = policy_accepts_event(policy, event)
        if not accepted:
            policy_reject_counts[policy_reason] += 1
            if len(samples) < sample_limit:
                samples.append({**diagnostics, "skip_reason": f"policy_{policy_reason}"})
            continue
        retained.append(event)

    retained = sorted(
        retained,
        key=lambda event: (event.observed_ts or event.event_ts or 0.0, event.event_id),
        reverse=True,
    )
    return retained, {
        "enabled": True,
        "flow_stage": "LIVE",
        "input_events": len(events),
        "retained_events": len(retained),
        "filtered_events": len(events) - len(retained),
        "max_event_age_s": round(float(max_event_age_s), 6),
        "live_build_max_observed_age_s": round(float(live_build_max_observed_age_s), 6),
        "skip_counts": dict(sorted(skip_counts.items())),
        "policy_reject_counts": dict(sorted(policy_reject_counts.items())),
        "latest_source_event_runtime": latest,
        "sample_filtered_events": samples,
    }


def _event_window_key(event: WalletEvent) -> tuple[str, str, str, str]:
    return (
        event.source_wallet.lower(),
        _canonical_btc_5m_market_slug(event),
        str(event.condition_id or ""),
        str(event.outcome or ""),
    )


def _event_window_close_ts(event: WalletEvent) -> float | None:
    window_start = _event_btc_5m_window_start_s(event)
    if window_start is None:
        return None
    return window_start + 300.0


def _wallet_event_source(event: WalletEvent) -> str:
    raw = event.raw if isinstance(event.raw, dict) else {}
    return str(raw.get("detection_source") or raw.get("_walletCopySource") or event.source or "").strip()


def _wallet_event_observation_sources(event: WalletEvent) -> list[str]:
    raw = event.raw if isinstance(event.raw, dict) else {}
    sources = raw.get("observation_sources")
    if isinstance(sources, list):
        result = [str(source) for source in sources if str(source or "")]
    else:
        result = []
    source = _wallet_event_source(event)
    if source:
        result.append(source)
    return sorted(dict.fromkeys(result))


def _inventory_event_identity_key(event: WalletEvent) -> tuple[Any, ...]:
    tx = str(event.transaction_hash or "").strip().lower()
    if tx:
        return (
            "tx",
            event.source_wallet.lower(),
            tx,
            event.condition_id,
            event.token_id,
            event.outcome,
            event.action.upper(),
            round(float(event.price), 8),
            round(float(event.size), 8),
        )
    return (
        "semantic",
        event.source_wallet.lower(),
        event.condition_id,
        event.token_id,
        event.outcome,
        event.action.upper(),
        round(float(event.price), 8),
        round(float(event.size), 8),
        event.event_ts,
    )


def _prefer_inventory_detection_event(existing: WalletEvent, candidate: WalletEvent) -> WalletEvent:
    existing_seen = float(existing.observed_ts or existing.event_ts or 0.0)
    candidate_seen = float(candidate.observed_ts or candidate.event_ts or 0.0)
    if candidate_seen <= 0:
        winner = existing
        loser = candidate
    elif existing_seen <= 0 or candidate_seen < existing_seen:
        winner = candidate
        loser = existing
    elif candidate_seen == existing_seen and _wallet_event_source(candidate) == "rtds_activity":
        winner = candidate
        loser = existing
    else:
        winner = existing
        loser = candidate

    raw = dict(winner.raw or {})
    sources = set(_wallet_event_observation_sources(winner))
    sources.update(_wallet_event_observation_sources(loser))
    winner_source = _wallet_event_source(winner)
    loser_source = _wallet_event_source(loser)
    if winner_source:
        raw["detection_source"] = winner_source
    if sources:
        raw["observation_sources"] = sorted(sources)
    if loser.observed_ts:
        raw["alternate_observed_ts"] = float(loser.observed_ts)
        if loser_source:
            raw["alternate_detection_source"] = loser_source
    return WalletEvent.from_dict({**winner.asdict(), "raw": raw})


def _dedupe_inventory_events(events: list[WalletEvent]) -> list[WalletEvent]:
    deduped: dict[tuple[Any, ...], WalletEvent] = {}
    for event in events:
        key = _inventory_event_identity_key(event)
        existing = deduped.get(key)
        deduped[key] = event if existing is None else _prefer_inventory_detection_event(existing, event)
    return list(deduped.values())


def _copy_model_is_inventory_like(value: Any) -> bool:
    return str(value or "").strip() in INVENTORY_COPY_MODELS


def _intent_copy_model(intent: CopyIntent) -> str:
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    return str(metadata.get("copy_model") or "").strip()


def _copy_model_from_order(row: dict[str, Any]) -> str:
    source_intent = _as_dict(row.get("source_intent"))
    metadata = _as_dict(source_intent.get("metadata"))
    return str(row.get("copy_model") or metadata.get("copy_model") or "").strip()


def _parse_order_ts(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value or "").strip()
    if not text:
        return 0.0
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return dt.datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def _utc_day_bounds(now_ts: float) -> tuple[float, float]:
    now = dt.datetime.fromtimestamp(float(now_ts), tz=dt.timezone.utc)
    start = dt.datetime(now.year, now.month, now.day, tzinfo=dt.timezone.utc)
    return start.timestamp(), (start + dt.timedelta(days=1)).timestamp()


def _order_window_key(row: dict[str, Any]) -> str:
    source_intent = _as_dict(row.get("source_intent"))
    wallet = str(row.get("source_wallet") or source_intent.get("source_wallet") or "").lower()
    market_slug = str(row.get("market_slug") or source_intent.get("market_slug") or "")
    condition_id = str(row.get("condition_id") or source_intent.get("condition_id") or "")
    outcome = str(row.get("outcome") or source_intent.get("outcome") or "")
    if not wallet or not (market_slug or condition_id) or not outcome:
        return ""
    return "|".join((wallet, market_slug or condition_id, condition_id, outcome))


def _strong_tier_usage(live_ledger_state: str, *, now_ts: float) -> dict[str, Any]:
    ledger = load_json(live_ledger_state, default={}, cache_readonly=True)
    orders = ledger.get("orders") if isinstance(ledger, dict) else []
    day_start, day_end = _utc_day_bounds(now_ts)
    windows_today: set[str] = set()
    concurrent_windows: set[str] = set()
    for row in orders if isinstance(orders, list) else []:
        if not isinstance(row, dict) or str(row.get("final_status") or "").upper() not in {"FILLED", "SUBMITTED"}:
            continue
        if _copy_model_from_order(row) != DRIP_COPY_MODEL:
            continue
        source_intent = _as_dict(row.get("source_intent"))
        metadata = _as_dict(source_intent.get("metadata"))
        drip = _as_dict(metadata.get("inventory_v3_drip"))
        if str(drip.get("signal_tier") or "") != "strong":
            continue
        key = _order_window_key(row)
        if not key:
            continue
        submitted_ts = _parse_order_ts(row.get("submitted_at") or row.get("updated_at"))
        if day_start <= submitted_ts < day_end:
            windows_today.add(key)
        close_ts = num(drip.get("window_close_ts"), 0.0)
        if close_ts > float(now_ts):
            concurrent_windows.add(key)
    return {
        "strong_window_keys_today": sorted(windows_today),
        "strong_windows_today": len(windows_today),
        "strong_concurrent_window_keys": sorted(concurrent_windows),
        "strong_concurrent_windows": len(concurrent_windows),
        "max_windows_per_day": STRONG_TIER_MAX_WINDOWS_PER_DAY,
        "max_concurrent_windows": STRONG_TIER_MAX_CONCURRENT_WINDOWS,
    }


def _drip_min_order_usd(
    *,
    price: float,
    min_order_usd: float,
    min_tranche_usd: float,
    max_effective_min_usd: float | None = None,
) -> float:
    share_floor_usd = DRIP_CLOB_MIN_SHARES * max(0.0, float(price))
    effective = max(0.0, float(min_order_usd), float(min_tranche_usd), share_floor_usd)
    if max_effective_min_usd is not None and max_effective_min_usd > 0:
        effective = min(effective, float(max_effective_min_usd))
    return effective


def _policy_drip_min_cap_usd(policy: CandidatePolicy, *, source_wallet: str = "") -> float | None:
    if policy.maker_min_share_funding_cap_usd > 0:
        return max(0.0, float(policy.max_order_usd))
    if str(policy.policy_id or "") == _A689_CANARY_POLICY_ID:
        return _A689_CANARY_DRIP_MIN_CAP_USD
    if (
        str(source_wallet or "").lower() == _F418_READMISSION_WALLET
        and str(policy.policy_id or "") == _F418_READMISSION_POLICY_ID
    ):
        return _F418_READMISSION_CAP_USD
    return None


def _submitted_position_by_key(live_ledger_state: str) -> dict[tuple[str, str, str, str], dict[str, float]]:
    ledger = load_json(live_ledger_state, default={}, cache_readonly=True)
    orders = ledger.get("orders") if isinstance(ledger, dict) else []
    positions: dict[tuple[str, str, str, str], dict[str, float]] = {}
    for row in orders if isinstance(orders, list) else []:
        if not isinstance(row, dict):
            continue
        final_status = str(row.get("final_status") or "").upper()
        source_intent = _as_dict(row.get("source_intent"))
        wallet = str(row.get("source_wallet") or source_intent.get("source_wallet") or "").lower()
        market_slug = str(row.get("market_slug") or source_intent.get("market_slug") or "")
        condition_id = str(row.get("condition_id") or source_intent.get("condition_id") or "")
        outcome = str(row.get("outcome") or source_intent.get("outcome") or "")
        if not wallet or not (market_slug or condition_id) or not outcome:
            continue
        is_inventory_order = _copy_model_is_inventory_like(_copy_model_from_order(row))
        key = (wallet, market_slug or condition_id, condition_id, outcome)
        if is_inventory_order:
            attempt_bucket = positions.setdefault(
                key,
                {"shares": 0.0, "usd": 0.0, "orders": 0.0, "converge_orders": 0.0, "fills": 0.0},
            )
            attempt_bucket["converge_orders"] = float(attempt_bucket.get("converge_orders") or 0.0) + 1.0
        requested_shares = num(row.get("requested_shares"), num(source_intent.get("shares")))
        requested_usd = num(row.get("requested_size_usd"), num(source_intent.get("copy_size_usd")))
        filled_shares = num(row.get("filled_shares"))
        filled_usd = num(row.get("filled_size_usd"))
        shares = filled_shares if filled_shares > 0 else requested_shares
        usd = filled_usd if filled_usd > 0 else requested_usd
        if final_status not in {"FILLED", "SUBMITTED"}:
            continue
        if shares <= 0:
            continue
        bucket = positions.setdefault(
            key,
            {"shares": 0.0, "usd": 0.0, "orders": 0.0, "converge_orders": 0.0, "fills": 0.0},
        )
        bucket["shares"] += shares
        bucket["usd"] += max(0.0, usd)
        bucket["orders"] += 1.0
        if final_status == "FILLED":
            bucket["fills"] += 1.0
    return positions


def _event_to_inventory_intent(
    *,
    key: tuple[str, str, str, str],
    events: list[WalletEvent],
    policy: CandidatePolicy,
    now_ts: float,
    submitted_positions: dict[tuple[str, str, str, str], dict[str, float]],
    late_window_stop_s: float,
    live_build_max_observed_age_s: float,
    max_converge_orders_per_window: int,
    copy_model: str = "inventory",
    strong_tier_usage: dict[str, Any] | None = None,
    strong_tier_suspended: bool = False,
    drip_min_tranche_usd: float = DRIP_MIN_TRANCHE_USD,
    drip_max_tranche_usd: float = DRIP_MAX_TRANCHE_USD,
    observation_watermarks: dict[str, Any] | None = None,
    min_live_floor_pin_enabled: bool = False,
) -> tuple[CopyIntent | None, dict[str, Any]]:
    ordered = sorted(
        _dedupe_inventory_events(events),
        key=lambda event: (event.event_ts or 0.0, event.observed_ts or event.event_ts or 0.0, event.event_id),
    )
    if not ordered:
        return None, {
            "status": "SKIPPED",
            "reason": "inventory_window_empty",
            "wallet_eligible_orders": 0,
            "our_submits": 0,
            "our_fills": 0,
            "dominant_skip_reason": "inventory_window_empty",
        }
    position = submitted_positions.get(key) or {
        "shares": 0.0,
        "usd": 0.0,
        "orders": 0.0,
        "converge_orders": 0.0,
        "fills": 0.0,
    }
    participation_base = {
        "key": key,
        "wallet_eligible_orders": len(ordered),
        "our_submits": int(position.get("orders") or 0),
        "our_fills": int(position.get("fills") or 0),
        "our_attempts": int(position.get("converge_orders") or position.get("orders") or 0),
    }

    def skipped(reason: str, **extra: Any) -> dict[str, Any]:
        return {
            "status": "SKIPPED",
            "reason": reason,
            **participation_base,
            "dominant_skip_reason": reason,
            **extra,
        }

    def stale_drop_audit() -> dict[str, Any]:
        samples: list[dict[str, Any]] = []
        events_by_source_tx: dict[str, dict[str, Any]] = {}
        threshold = float(live_build_max_observed_age_s)
        stale_count = 0
        for event in ordered:
            try:
                observed_ts = float(event.observed_ts or event.event_ts or 0.0)
            except (TypeError, ValueError):
                observed_ts = 0.0
            drop_age_s = max(0.0, now_ts - observed_ts) if observed_ts > 0 else None
            if threshold > 0 and (drop_age_s is None or drop_age_s > threshold):
                stale_count += 1
                raw = event.raw if isinstance(event.raw, dict) else {}
                source_tx_hash = str(event.transaction_hash or raw.get("transaction_hash") or "").strip()
                source_tx_key = source_tx_hash or str(event.event_id or "").strip()
                if not source_tx_key:
                    source_tx_key = "|".join(
                        str(item)
                        for item in (
                            event.source_wallet.lower(),
                            event.market_slug,
                            event.outcome,
                            event.event_ts,
                            event.observed_ts,
                        )
                    )
                keyed_event = {
                    "source_tx_hash": source_tx_hash,
                    "event_id": event.event_id,
                    "source_wallet": event.source_wallet.lower(),
                    "market_slug": event.market_slug,
                    "outcome": event.outcome,
                    "event_ts": event.event_ts,
                    "observed_ts": event.observed_ts,
                    "drop_age_s": None if drop_age_s is None else round(drop_age_s, 6),
                    "detection_source": _wallet_event_source(event),
                    "observation_sources": _wallet_event_observation_sources(event),
                }
                if len(events_by_source_tx) < 25 and source_tx_key not in events_by_source_tx:
                    events_by_source_tx[source_tx_key] = keyed_event
                if len(samples) < 10:
                    samples.append(keyed_event)
        return {
            "flow_stage": "LIVE/LEARN",
            "reason": "observed-age stale drop before inventory intent build",
            "live_build_max_observed_age_s": round(threshold, 6),
            "stale_drop_events": stale_count,
            "events_by_source_tx": events_by_source_tx,
            "sample_events": samples,
        }

    latest = max(
        ordered,
        key=lambda event: (event.event_ts or 0.0, event.observed_ts or event.event_ts or 0.0, event.event_id),
    )
    latest_observed_ts = float(latest.observed_ts or latest.event_ts or 0.0)
    detection_source = _wallet_event_source(latest)
    observation_sources = _wallet_event_observation_sources(latest)
    latest_raw = latest.raw if isinstance(latest.raw, dict) else {}
    alternate_observed_ts = latest_raw.get("alternate_observed_ts")
    alternate_detection_source = latest_raw.get("alternate_detection_source")
    watermark_row = {}
    if isinstance(observation_watermarks, dict):
        watermark_row = observation_watermarks.get(str(key[0]).lower()) or observation_watermarks.get(str(key[0])) or {}
        watermark_row = watermark_row if isinstance(watermark_row, dict) else {}
    try:
        watermark_ts = float(watermark_row.get("latest_checked_ts") or 0.0)
    except (TypeError, ValueError):
        watermark_ts = 0.0
    effective_latest_observed_ts = max(latest_observed_ts, watermark_ts)
    latest_observed_age_s = (
        max(0.0, now_ts - effective_latest_observed_ts) if effective_latest_observed_ts > 0 else None
    )
    source_latest_observed_age_s = max(0.0, now_ts - latest_observed_ts) if latest_observed_ts > 0 else None
    freshness_confirmed_unchanged = bool(
        watermark_ts > latest_observed_ts
        and latest_observed_age_s is not None
        and (
            live_build_max_observed_age_s <= 0
            or latest_observed_age_s <= float(live_build_max_observed_age_s)
        )
    )
    window_start_s = _event_btc_5m_window_start_s(latest)
    stale_by_live_build_guard = bool(
        live_build_max_observed_age_s > 0
        and (latest_observed_age_s is None or latest_observed_age_s > float(live_build_max_observed_age_s))
    )
    latest_is_fast_arm_fresh = bool(
        source_latest_observed_age_s is not None
        and source_latest_observed_age_s < INVENTORY_FAST_ARM_MAX_OBSERVED_AGE_S
    )
    close_ts = _event_window_close_ts(latest)
    time_to_close_s = None if close_ts is None else close_ts - now_ts
    participation_base.update(
        {
            "window_start_s": None if window_start_s is None else round(float(window_start_s), 6),
            "window_close_ts": None if close_ts is None else round(float(close_ts), 6),
            "latest_observed_ts": round(latest_observed_ts, 6) if latest_observed_ts > 0 else None,
            "effective_latest_observed_ts": (
                round(effective_latest_observed_ts, 6) if effective_latest_observed_ts > 0 else None
            ),
            "source_latest_observed_age_s": (
                None if source_latest_observed_age_s is None else round(source_latest_observed_age_s, 6)
            ),
            "freshness_watermark_ts": round(watermark_ts, 6) if watermark_ts > 0 else None,
            "freshness_confirmed_unchanged": freshness_confirmed_unchanged,
            "observed_slug_epoch_delta_s": (
                round(effective_latest_observed_ts - float(window_start_s), 6)
                if effective_latest_observed_ts > 0 and window_start_s is not None
                else None
            ),
            "source_detection_observed_ts": round(latest_observed_ts, 6) if latest_observed_ts > 0 else None,
            "detection_source": detection_source,
            "observation_sources": observation_sources,
            "alternate_observed_ts": alternate_observed_ts,
            "alternate_detection_source": alternate_detection_source,
        }
    )
    if close_ts is None:
        return None, skipped("inventory_window_close_unknown")
    if time_to_close_s is not None and time_to_close_s <= float(late_window_stop_s):
        return None, skipped(
            "inventory_late_window_guard",
            time_to_close_s=round(float(time_to_close_s), 6),
            late_window_stop_s=round(float(late_window_stop_s), 6),
        )
    source_shares = sum(max(0.0, float(event.size)) for event in ordered)
    source_usd = sum(max(0.0, float(event.usdc_size)) for event in ordered)
    if source_shares <= 0 or source_usd <= 0:
        return None, skipped("inventory_source_size_zero")
    source_vwap = source_usd / source_shares
    latest_price = max(0.0, float(latest.price))
    latest_source_shares = max(0.0, float(latest.size))
    latest_source_usd = max(0.0, float(latest.usdc_size))
    if latest_source_usd <= 0 and latest_price > 0 and latest_source_shares > 0:
        latest_source_usd = latest_price * latest_source_shares
    copy_model = DRIP_COPY_MODEL if str(copy_model or "").strip() == DRIP_COPY_MODEL else "inventory"
    wallet_fraction = max(0.0, float(policy.wallet_fraction))
    latest_guard_sized_copy_usd = latest_source_usd * wallet_fraction
    target_shares = source_shares * wallet_fraction
    max_order_usd = max(0.0, float(policy.max_order_usd))
    uncapped_target_usd = target_shares * source_vwap
    strong_usage = strong_tier_usage if isinstance(strong_tier_usage, dict) else {}
    strong_key = "|".join(str(item) for item in key)
    strong_windows_today = set(str(item) for item in strong_usage.get("strong_window_keys_today") or [])
    strong_concurrent_windows = set(str(item) for item in strong_usage.get("strong_concurrent_window_keys") or [])
    strong_candidate = bool(
        copy_model == DRIP_COPY_MODEL
        and uncapped_target_usd >= STRONG_TIER_SOURCE_TARGET_MIN_USD
        and source_vwap > 0
    )
    signal_tier = "baseline"
    strong_tier_cap_reason = ""
    if strong_candidate:
        already_strong_window = strong_key in strong_windows_today or strong_key in strong_concurrent_windows
        if strong_tier_suspended:
            strong_tier_cap_reason = "strong_tier_suspended_by_defensive_sizing"
        elif (
            not already_strong_window
            and len(strong_windows_today) >= STRONG_TIER_MAX_WINDOWS_PER_DAY
        ):
            strong_tier_cap_reason = "strong_tier_daily_window_cap_reached"
        elif (
            not already_strong_window
            and len(strong_concurrent_windows) >= STRONG_TIER_MAX_CONCURRENT_WINDOWS
        ):
            strong_tier_cap_reason = "strong_tier_concurrent_window_cap_reached"
        else:
            signal_tier = "strong"
    window_budget_usd = max_order_usd
    if signal_tier == "strong":
        window_budget_usd = min(
            STRONG_TIER_WINDOW_BUDGET_MAX_USD,
            max(STRONG_TIER_SOURCE_TARGET_MIN_USD, uncapped_target_usd),
        )
    if window_budget_usd > 0:
        latest_guard_sized_copy_usd = min(latest_guard_sized_copy_usd, window_budget_usd)
    if window_budget_usd > 0 and source_vwap > 0:
        target_shares = min(target_shares, window_budget_usd / source_vwap)
    our_shares = max(0.0, float(position.get("shares") or 0.0))
    converge_orders = int(position.get("converge_orders") or position.get("orders") or 0)
    max_converge_orders = max(0, int(max_converge_orders_per_window))
    if max_converge_orders > 0 and converge_orders >= max_converge_orders:
        return None, skipped(
            "inventory_max_converge_orders_reached",
            converge_orders=converge_orders,
            max_converge_orders_per_window=max_converge_orders,
        )
    gap_shares = max(0.0, target_shares - our_shares)
    gap_usd = gap_shares * source_vwap
    min_order_usd = max(0.0, float(policy.min_order_usd))
    if gap_usd <= 0:
        return None, skipped(
            "inventory_target_already_met",
            target_shares=round(target_shares, 6),
            our_shares=round(our_shares, 6),
        )
    gap_below_policy_min_order = bool(min_order_usd > 0 and gap_usd < min_order_usd)
    if gap_below_policy_min_order and our_shares > 0:
        return None, skipped(
            "inventory_residual_gap_below_min_order",
            source_inventory_usd=round(source_usd, 6),
            source_inventory_vwap=round(source_vwap, 6),
            latest_source_usd=round(latest_source_usd, 6),
            source_order_usd=round(latest_source_usd, 6),
            target_shares=round(target_shares, 6),
            target_usd_at_vwap=round(target_shares * source_vwap, 6),
            our_shares=round(our_shares, 6),
            our_position_shares=round(our_shares, 6),
            our_position_usd=round(float(position.get("usd") or 0.0), 6),
            gap_shares=round(gap_shares, 6),
            gap_usd_at_vwap=round(gap_usd, 6),
            guard_sized_copy_usd=round(latest_guard_sized_copy_usd, 6),
            guard_sized_copy_below_min_order=bool(
                min_order_usd > 0 and latest_guard_sized_copy_usd < min_order_usd
            ),
            min_order_usd=round(min_order_usd, 6),
            latest_observed_age_s=None if latest_observed_age_s is None else round(latest_observed_age_s, 6),
            would_floor_min_order_usd=round(min_order_usd, 6),
        )

    latest_event_id = str(latest.event_id or "")
    drip_price_basis = latest_price if (latest_is_fast_arm_fresh and latest_price > 0) else source_vwap
    drip_min_order_usd = _drip_min_order_usd(
        price=drip_price_basis,
        min_order_usd=min_order_usd,
        min_tranche_usd=float(drip_min_tranche_usd),
        max_effective_min_usd=_policy_drip_min_cap_usd(policy, source_wallet=key[0]),
    )
    if copy_model == DRIP_COPY_MODEL and gap_usd < drip_min_order_usd and our_shares > 0:
        return None, skipped(
            "drip_residual_gap_below_min_tranche",
            source_inventory_usd=round(source_usd, 6),
            source_inventory_vwap=round(source_vwap, 6),
            target_shares=round(target_shares, 6),
            target_usd_at_vwap=round(target_shares * source_vwap, 6),
            our_shares=round(our_shares, 6),
            our_position_shares=round(our_shares, 6),
            our_position_usd=round(float(position.get("usd") or 0.0), 6),
            gap_shares=round(gap_shares, 6),
            gap_usd_at_vwap=round(gap_usd, 6),
            drip_min_tranche_usd=round(drip_min_order_usd, 6),
            min_order_usd=round(min_order_usd, 6),
            clob_min_shares=round(DRIP_CLOB_MIN_SHARES, 6),
        )
    min_live_floor_pin = False
    min_live_floor_pin_pre_budget_usd: float | None = None
    if (
        copy_model == DRIP_COPY_MODEL
        and min_live_floor_pin_enabled
        and window_budget_usd > 0
        and drip_min_order_usd > window_budget_usd
        and drip_min_order_usd <= MIN_LIVE_FLOOR_PIN_MAX_USD
    ):
        min_live_floor_pin = True
        min_live_floor_pin_pre_budget_usd = window_budget_usd
        window_budget_usd = drip_min_order_usd
        latest_guard_sized_copy_usd = min(latest_source_usd * wallet_fraction, window_budget_usd)
        if source_vwap > 0:
            target_shares = min(source_shares * wallet_fraction, window_budget_usd / source_vwap)
            gap_shares = max(0.0, target_shares - our_shares)
            gap_usd = gap_shares * source_vwap
            gap_below_policy_min_order = bool(min_order_usd > 0 and gap_usd < min_order_usd)
    fast_order_usd = latest_guard_sized_copy_usd
    remaining_gap_usd_at_latest_price = gap_shares * latest_price
    fast_gap_usd = min(fast_order_usd, remaining_gap_usd_at_latest_price)
    fast_gap_shares = fast_gap_usd / latest_price if latest_price > 0 else 0.0
    fast_arm = bool(
        latest_is_fast_arm_fresh
        and latest_price > 0
        and latest_source_shares > 0
        and latest_source_usd > 0
        and fast_gap_usd > 0
        and (min_order_usd <= 0 or fast_gap_usd >= min_order_usd)
    )
    if stale_by_live_build_guard and not fast_arm:
        return None, skipped(
            "inventory_window_state_stale",
            latest_observed_age_s=None if latest_observed_age_s is None else round(latest_observed_age_s, 6),
            source_latest_observed_age_s=(
                None if source_latest_observed_age_s is None else round(source_latest_observed_age_s, 6)
            ),
            live_build_max_observed_age_s=round(float(live_build_max_observed_age_s), 6),
            freshness_watermark_ts=round(watermark_ts, 6) if watermark_ts > 0 else None,
            freshness_confirmed_unchanged=freshness_confirmed_unchanged,
            fast_arm_fresh=latest_is_fast_arm_fresh,
            fast_arm_candidate_copy_usd=round(fast_gap_usd, 6),
            fast_arm_min_order_usd=round(min_order_usd, 6),
            stale_drop_audit=stale_drop_audit(),
        )
    inventory_snapshot = {
        "schema_version": 1,
        "flow_stage": "LIVE",
        "copy_model": copy_model,
        "fast_arm": fast_arm,
        "source_wallet": key[0],
        "market_slug": key[1],
        "condition_id": key[2],
        "outcome": key[3],
        "event_count": len(ordered),
        "raw_event_count": len(events),
        "latest_observed_ts": round(latest_observed_ts, 6) if latest_observed_ts > 0 else None,
        "latest_observed_age_s": None if latest_observed_age_s is None else round(latest_observed_age_s, 6),
        "source_latest_observed_age_s": (
            None if source_latest_observed_age_s is None else round(source_latest_observed_age_s, 6)
        ),
        "effective_latest_observed_ts": (
            round(effective_latest_observed_ts, 6) if effective_latest_observed_ts > 0 else None
        ),
        "freshness_watermark_ts": round(watermark_ts, 6) if watermark_ts > 0 else None,
        "freshness_confirmed_unchanged": freshness_confirmed_unchanged,
        "source_detection_observed_ts": round(latest_observed_ts, 6) if latest_observed_ts > 0 else None,
        "detection_source": detection_source,
        "observation_sources": observation_sources,
        "alternate_observed_ts": alternate_observed_ts,
        "alternate_detection_source": alternate_detection_source,
        "source_inventory_shares": round(source_shares, 6),
        "source_inventory_usd": round(source_usd, 6),
        "source_inventory_vwap": round(source_vwap, 6),
        "policy_fraction": round(wallet_fraction, 6),
        "policy_max_order_usd": round(max_order_usd, 6),
        "uncapped_target_usd_at_vwap": round(uncapped_target_usd, 6),
        "window_budget_usd": round(window_budget_usd, 6),
        "min_live_floor_pin": bool(min_live_floor_pin),
        "signal_tier": signal_tier,
        "strong_tier_candidate": strong_candidate,
        "strong_tier_cap_reason": strong_tier_cap_reason,
        "target_shares": round(target_shares, 6),
        "target_usd_at_vwap": round(target_shares * source_vwap, 6),
        "our_position_shares": round(our_shares, 6),
        "our_position_usd": round(float(position.get("usd") or 0.0), 6),
        "our_position_orders": int(position.get("orders") or 0),
        "wallet_eligible_orders": len(ordered),
        "our_submits": int(position.get("orders") or 0),
        "our_fills": int(position.get("fills") or 0),
        "our_attempts": converge_orders,
        "dominant_skip_reason": "eligible",
        "converge_orders": converge_orders,
        "max_converge_orders_per_window": max_converge_orders,
        "gap_shares": round(gap_shares, 6),
        "gap_usd_at_vwap": round(gap_usd, 6),
        "gap_below_policy_min_order": gap_below_policy_min_order,
        "min_order_usd": round(min_order_usd, 6),
        "window_start_s": None if window_start_s is None else round(float(window_start_s), 6),
        "window_close_ts": round(float(close_ts), 6),
        "time_to_close_s": round(float(time_to_close_s or 0.0), 6),
        "late_window_stop_s": round(float(late_window_stop_s), 6),
        "latest_observed_ts": round(latest_observed_ts, 6),
        "latest_observed_age_s": None if latest_observed_age_s is None else round(latest_observed_age_s, 6),
        "source_latest_observed_ts": round(latest_observed_ts, 6) if latest_observed_ts > 0 else None,
        "effective_latest_observed_ts": (
            round(effective_latest_observed_ts, 6) if effective_latest_observed_ts > 0 else None
        ),
        "observed_slug_epoch_delta_s": (
            round(effective_latest_observed_ts - float(window_start_s), 6)
            if effective_latest_observed_ts > 0 and window_start_s is not None
            else None
        ),
        "latest_source_event_id": latest_event_id,
        "fast_arm_max_observed_age_s": round(float(INVENTORY_FAST_ARM_MAX_OBSERVED_AGE_S), 6),
        "fast_arm_source_price": round(latest_price, 6),
        "fast_arm_source_shares": round(latest_source_shares, 6),
        "fast_arm_source_usd": round(latest_source_usd, 6),
        "fast_arm_candidate_copy_usd": round(fast_gap_usd, 6),
        "fast_arm_candidate_shares": round(fast_gap_shares, 6),
        "fast_arm_remaining_gap_usd_at_latest_price": round(remaining_gap_usd_at_latest_price, 6),
        "intent_basis": "latest_event" if fast_arm else "window_inventory_vwap",
        "intent_limit_price": round(latest_price if fast_arm else source_vwap, 6),
        "execution_rule": "converge_when_best_ask_at_or_below_vwap_plus_buffer",
    }
    if min_live_floor_pin:
        inventory_snapshot.update(
            {
                "min_live_floor_pin_direction_id": MIN_LIVE_FLOOR_PIN_DIRECTION_ID,
                "min_live_floor_pin_usd": round(window_budget_usd, 6),
                "min_live_floor_pin_pre_pin_window_budget_usd": round(
                    float(min_live_floor_pin_pre_budget_usd or 0.0),
                    6,
                ),
                "min_live_floor_pin_max_usd": round(MIN_LIVE_FLOOR_PIN_MAX_USD, 6),
                "min_live_floor_pin_rule": (
                    "live-selected drip lane may pin exactly one CLOB five-share min tranche "
                    "when the effective min is <= the $2.50 hard bound"
                ),
            }
        )
    if fast_arm:
        inventory_snapshot["execution_rule"] = "fast_arm_latest_event_when_fresh_gap_meets_min_order"
    intent_price = latest_price if fast_arm else source_vwap
    intent_shares = fast_gap_shares if fast_arm else gap_shares
    intent_usd = fast_gap_usd if fast_arm else gap_usd
    min_order_floor_applied = False
    min_order_floor_usd = 0.0
    if copy_model == DRIP_COPY_MODEL:
        remaining_slots = max(1, max_converge_orders - converge_orders) if max_converge_orders > 0 else 1
        drip_price = latest_price if (latest_is_fast_arm_fresh and latest_price > 0) else source_vwap
        drip_min_usd = _drip_min_order_usd(
            price=drip_price,
            min_order_usd=min_order_usd,
            min_tranche_usd=float(drip_min_tranche_usd),
            max_effective_min_usd=_policy_drip_min_cap_usd(policy, source_wallet=key[0]),
        )
        if window_budget_usd > 0 and drip_min_usd > window_budget_usd:
            return None, skipped(
                "drip_min_tranche_exceeds_window_budget",
                source_inventory_usd=round(source_usd, 6),
                source_inventory_vwap=round(source_vwap, 6),
                target_shares=round(target_shares, 6),
                target_usd_at_vwap=round(target_shares * source_vwap, 6),
                our_shares=round(our_shares, 6),
                gap_shares=round(gap_shares, 6),
                gap_usd_at_vwap=round(gap_usd, 6),
                drip_min_tranche_usd=round(drip_min_usd, 6),
                drip_max_tranche_usd=round(float(drip_max_tranche_usd), 6),
                window_budget_usd=round(window_budget_usd, 6),
                policy_max_order_usd=round(max_order_usd, 6),
                min_order_usd=round(min_order_usd, 6),
                process_min_live_order_usd=round(drip_min_usd, 6),
                clob_min_shares=round(DRIP_CLOB_MIN_SHARES, 6),
                probe_cap_min_order_floor_blocked=True,
                rule="do not floor a drip/probe-cap intent above the active window budget",
            )
        drip_target_usd = gap_usd / float(remaining_slots)
        drip_max_usd = max(drip_min_usd, float(drip_max_tranche_usd))
        drip_tranche_usd = max(drip_min_usd, min(drip_max_usd, drip_target_usd))
        if our_shares <= 0.0 and gap_usd < drip_min_usd:
            drip_tranche_usd = drip_min_usd
            min_order_floor_applied = True
            min_order_floor_usd = drip_min_usd
        else:
            drip_tranche_usd = min(gap_usd, drip_tranche_usd)
        intent_price = drip_price
        intent_usd = drip_tranche_usd
        intent_shares = drip_tranche_usd / drip_price if drip_price > 0 else 0.0
        inventory_snapshot["intent_basis"] = "drip_latest_event" if drip_price == latest_price else "drip_window_vwap"
        inventory_snapshot["intent_limit_price"] = round(intent_price, 6)
        inventory_snapshot["execution_rule"] = "drip_micro_tranche_at_or_below_vwap_plus_buffer"
        inventory_snapshot["inventory_v3_drip"] = {
            "schema_version": 1,
            "flow_stage": "LIVE",
            "copy_model": DRIP_COPY_MODEL,
            "signal_tier": signal_tier,
            "window_budget_usd": round(window_budget_usd, 6),
            "min_live_floor_pin": bool(min_live_floor_pin),
            "uncapped_target_usd_at_vwap": round(uncapped_target_usd, 6),
            "target_usd_at_vwap": round(target_shares * source_vwap, 6),
            "gap_usd_at_vwap": round(gap_usd, 6),
            "tranche_usd": round(intent_usd, 6),
            "tranche_shares": round(intent_shares, 6),
            "tranche_limit_price": round(intent_price, 6),
            "remaining_tranche_slots": remaining_slots,
            "drip_min_tranche_usd": round(float(drip_min_tranche_usd), 6),
            "drip_max_tranche_usd": round(float(drip_max_tranche_usd), 6),
            "effective_min_tranche_usd": round(drip_min_usd, 6),
            "clob_min_shares": round(DRIP_CLOB_MIN_SHARES, 6),
            "strong_tier_candidate": strong_candidate,
            "strong_tier_cap_reason": strong_tier_cap_reason,
            "strong_tier_caps": {
                "max_windows_per_day": STRONG_TIER_MAX_WINDOWS_PER_DAY,
                "max_concurrent_windows": STRONG_TIER_MAX_CONCURRENT_WINDOWS,
                "windows_today_before": int(strong_usage.get("strong_windows_today") or 0),
                "concurrent_windows_before": int(strong_usage.get("strong_concurrent_windows") or 0),
            },
            "window_close_ts": round(float(close_ts), 6),
        }
        if min_live_floor_pin:
            inventory_snapshot["inventory_v3_drip"].update(
                {
                    "min_live_floor_pin_direction_id": MIN_LIVE_FLOOR_PIN_DIRECTION_ID,
                    "min_live_floor_pin_usd": round(window_budget_usd, 6),
                    "min_live_floor_pin_pre_pin_window_budget_usd": round(
                        float(min_live_floor_pin_pre_budget_usd or 0.0),
                        6,
                    ),
                    "min_live_floor_pin_max_usd": round(MIN_LIVE_FLOOR_PIN_MAX_USD, 6),
                }
            )
    elif our_shares <= 0.0 and gap_usd > 0.0 and min_order_usd > 0.0 and gap_usd < min_order_usd:
        intent_usd = min_order_usd
        intent_shares = min_order_usd / intent_price if intent_price > 0 else 0.0
        min_order_floor_applied = True
        min_order_floor_usd = min_order_usd
    inventory_snapshot["guard_sized_copy_usd"] = round(intent_usd, 6)
    inventory_snapshot["guard_sized_copy_below_min_order"] = bool(min_order_usd > 0 and intent_usd < min_order_usd)
    inventory_snapshot["probe_cap_min_order_floor_applied"] = bool(min_order_floor_applied)
    inventory_snapshot["would_floor_min_order_usd"] = round(min_order_floor_usd, 6) if min_order_floor_applied else None
    if copy_model == DRIP_COPY_MODEL:
        inventory_snapshot["inventory_v3_drip"]["probe_cap_min_order_floor_applied"] = bool(min_order_floor_applied)
        inventory_snapshot["inventory_v3_drip"]["would_floor_min_order_usd"] = (
            round(min_order_floor_usd, 6) if min_order_floor_applied else None
        )
    source_event_id = stable_id(
        "drip" if copy_model == DRIP_COPY_MODEL else ("invfast" if fast_arm else "inv2"),
        {
            "key": key,
            "latest_event_id": latest_event_id,
            "latest_observed_ts": round(latest_observed_ts, 6),
            "target_shares": round(target_shares, 6),
            "our_shares": round(our_shares, 6),
            "gap_shares": round(intent_shares, 6),
            "intent_usd": round(intent_usd, 6),
            "intent_price": round(intent_price, 6),
            "copy_model": copy_model,
            "converge_orders": converge_orders,
            "fast_arm": fast_arm,
        },
    )
    side = "YES" if str(latest.outcome or "").strip().lower() in {"up", "yes"} else "NO"
    canonical_market_slug = key[1]
    intent = CopyIntent(
        source_wallet=latest.source_wallet,
        wallet_name=latest.wallet_name,
        source_event_id=source_event_id,
        source_row_event_id=latest.event_id,
        condition_id=latest.condition_id,
        market_slug=canonical_market_slug,
        market_id=latest.market_id,
        outcome=latest.outcome,
        side=side,
        limit_price=round(intent_price, 6),
        wallet_usdc_size=round(latest_source_usd if fast_arm else source_usd, 6),
        copy_size_usd=round(intent_usd, 6),
        shares=round(intent_shares, 6),
        observed_ts=latest_observed_ts,
        strategy_family=(
            "wallet_copy_window_inventory_v3_drip"
            if copy_model == DRIP_COPY_MODEL
            else "wallet_copy_window_inventory_v2"
        ),
        policy_id=policy.policy_id,
        sizing_policy_id="window_inventory_fraction",
        mode="paper",
        order_type="PAPER_SOURCE_FILL",
        token_id=latest.token_id,
        event_ts=latest.event_ts if fast_arm else max((event.event_ts or 0.0 for event in ordered), default=0.0) or None,
        api_latency_s=max(0.0, latest_observed_ts - float(latest.event_ts or latest_observed_ts)),
        live_orders_allowed=False,
        reason=(
            "drip inventory micro-tranche intent"
            if copy_model == DRIP_COPY_MODEL
            else ("fresh inventory fast-arm latest event intent" if fast_arm else "window inventory convergence intent")
        ),
        metadata={
            "copy_model": copy_model,
            "inventory_v2": inventory_snapshot,
            **(
                {"inventory_v3_drip": inventory_snapshot["inventory_v3_drip"]}
                if copy_model == DRIP_COPY_MODEL
                else {}
            ),
            "row_type": "window_inventory",
            "asset": latest.asset,
            "duration": latest.duration,
            "source_market_slug": latest.market_slug,
            "source_fingerprint": source_event_id,
            "detection_source": detection_source,
            "observation_sources": observation_sources,
            "alternate_observed_ts": alternate_observed_ts,
            "alternate_detection_source": alternate_detection_source,
            "transaction_hash": latest.transaction_hash,
            "wallet_copy_policy": {
                "policy_id": policy.policy_id,
                "wallet_fraction": policy.wallet_fraction,
                "max_order_usd": policy.max_order_usd,
                "effective_live_cap_usd": round(max(float(policy.max_order_usd or 0.0), float(min_order_usd or 0.0)), 6),
                "min_order_usd": policy.min_order_usd,
                "maker_min_share_funding_cap_usd": policy.maker_min_share_funding_cap_usd,
                "maker_min_share_original_policy_cap_usd": policy.maker_min_share_original_policy_cap_usd,
                "maker_min_share_base_request_cap_usd": policy.maker_min_share_base_request_cap_usd,
                "maker_fallback_defense_cap_usd": policy.maker_fallback_defense_cap_usd,
                "effective_window_budget_usd": window_budget_usd,
                "min_live_floor_pin": bool(min_live_floor_pin),
                "signal_tier": signal_tier,
            },
            **(
                {
                    "min_live_floor_pin": True,
                    "min_live_floor_pin_direction_id": MIN_LIVE_FLOOR_PIN_DIRECTION_ID,
                    "min_live_floor_pin_usd": round(window_budget_usd, 6),
                    "min_live_floor_pin_pre_pin_window_budget_usd": round(
                        float(min_live_floor_pin_pre_budget_usd or 0.0),
                        6,
                    ),
                    "min_live_floor_pin_max_usd": round(MIN_LIVE_FLOOR_PIN_MAX_USD, 6),
                }
                if min_live_floor_pin
                else {}
            ),
        },
    )
    return intent, {"status": "PASS", "key": key, "intent_id": intent.intent_id, **inventory_snapshot}


def _inventory_window_participation_row(row: dict[str, Any]) -> dict[str, Any]:
    key = row.get("key") if isinstance(row.get("key"), (list, tuple)) else ()
    key_values = [str(item or "") for item in key]
    status = str(row.get("status") or "")
    reason = str(row.get("dominant_skip_reason") or row.get("reason") or "")
    if not reason and status == "PASS":
        reason = "eligible"
    return {
        "flow_stage": "LIVE",
        "status": status,
        "intent_id": str(row.get("intent_id") or ""),
        "source_wallet": str(row.get("source_wallet") or (key_values[0] if len(key_values) > 0 else "")).lower(),
        "market_slug": str(row.get("market_slug") or (key_values[1] if len(key_values) > 1 else "")),
        "condition_id": str(row.get("condition_id") or (key_values[2] if len(key_values) > 2 else "")),
        "outcome": str(row.get("outcome") or (key_values[3] if len(key_values) > 3 else "")),
        "copy_model": str(row.get("copy_model") or ""),
        "signal_tier": str(row.get("signal_tier") or ""),
        "wallet_eligible_orders": int(row.get("wallet_eligible_orders") or row.get("event_count") or 0),
        "our_submits": int(row.get("our_submits") or 0),
        "our_fills": int(row.get("our_fills") or 0),
        "our_attempts": int(row.get("our_attempts") or row.get("converge_orders") or 0),
        "dominant_skip_reason": reason or "unknown",
        "latest_observed_age_s": row.get("latest_observed_age_s"),
        "source_latest_observed_age_s": row.get("source_latest_observed_age_s"),
        "latest_observed_ts": row.get("latest_observed_ts"),
        "effective_latest_observed_ts": row.get("effective_latest_observed_ts"),
        "source_detection_observed_ts": row.get("source_detection_observed_ts"),
        "detection_source": row.get("detection_source"),
        "observation_sources": row.get("observation_sources") or [],
        "alternate_observed_ts": row.get("alternate_observed_ts"),
        "alternate_detection_source": row.get("alternate_detection_source"),
        "freshness_watermark_ts": row.get("freshness_watermark_ts"),
        "freshness_confirmed_unchanged": row.get("freshness_confirmed_unchanged"),
        "time_to_close_s": row.get("time_to_close_s"),
        "source_inventory_vwap": row.get("source_inventory_vwap"),
        "source_inventory_usd": row.get("source_inventory_usd"),
        "source_order_usd": row.get("source_order_usd") or row.get("latest_source_usd"),
        "latest_source_usd": row.get("latest_source_usd"),
        "target_shares": row.get("target_shares"),
        "target_usd_at_vwap": row.get("target_usd_at_vwap"),
        "our_position_shares": row.get("our_position_shares"),
        "our_position_usd": row.get("our_position_usd"),
        "gap_shares": row.get("gap_shares"),
        "gap_usd_at_vwap": row.get("gap_usd_at_vwap"),
        "guard_sized_copy_usd": row.get("guard_sized_copy_usd") or row.get("gap_usd_at_vwap"),
        "guard_sized_copy_below_min_order": row.get("guard_sized_copy_below_min_order"),
        "min_order_usd": row.get("min_order_usd"),
        "window_budget_usd": row.get("window_budget_usd"),
        "policy_max_order_usd": row.get("policy_max_order_usd"),
        "drip_min_tranche_usd": row.get("drip_min_tranche_usd"),
        "drip_max_tranche_usd": row.get("drip_max_tranche_usd"),
        "probe_cap_min_order_floor_blocked": row.get("probe_cap_min_order_floor_blocked"),
        "would_floor_min_order_usd": row.get("would_floor_min_order_usd"),
        "min_live_floor_pin": row.get("min_live_floor_pin"),
        "min_live_floor_pin_usd": row.get("min_live_floor_pin_usd"),
        "min_live_floor_pin_pre_pin_window_budget_usd": row.get(
            "min_live_floor_pin_pre_pin_window_budget_usd"
        ),
        "min_live_floor_pin_direction_id": row.get("min_live_floor_pin_direction_id"),
        "window_start_s": row.get("window_start_s"),
        "window_close_ts": row.get("window_close_ts"),
        "observed_slug_epoch_delta_s": row.get("observed_slug_epoch_delta_s"),
        "stale_drop_audit": row.get("stale_drop_audit") if isinstance(row.get("stale_drop_audit"), dict) else None,
    }


def _build_inventory_v2_intents(
    events: list[WalletEvent],
    policy: CandidatePolicy,
    *,
    now_ts: float,
    max_event_age_s: float,
    live_build_max_observed_age_s: float,
    live_ledger_state: str,
    late_window_stop_s: float,
    max_converge_orders_per_window: int,
    copy_model: str = "inventory",
    drip_min_tranche_usd: float = DRIP_MIN_TRANCHE_USD,
    drip_max_tranche_usd: float = DRIP_MAX_TRANCHE_USD,
    strong_tier_suspended: bool = False,
    observation_watermarks: dict[str, Any] | None = None,
    min_live_floor_pin_enabled: bool = False,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    groups: dict[tuple[str, str, str, str], list[WalletEvent]] = {}
    skip_counts: Counter[str] = Counter()
    policy_reject_counts: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    latest: dict[str, Any] = {}
    latest_key: tuple[float, str] = (-1.0, "")
    for event in sorted(events, key=lambda item: (item.observed_ts or item.event_ts or 0.0, item.event_id), reverse=True):
        diagnostics = _event_runtime_diagnostics(
            event,
            now_ts=now_ts,
            max_event_age_s=max_event_age_s,
            live_build_max_observed_age_s=live_build_max_observed_age_s,
        )
        event_key = (float(event.observed_ts or event.event_ts or 0.0), str(event.event_id or ""))
        if event_key > latest_key:
            latest_key = event_key
            latest = diagnostics
        if str(event.action or "").upper() != "BUY":
            skip_counts["not_buy"] += 1
            continue
        if not diagnostics["btc_5m_scope_ok"]:
            skip_counts["not_btc_5m"] += 1
            continue
        if diagnostics["market_closed_now"]:
            skip_counts["market_closed_now"] += 1
            continue
        if diagnostics["observed_after_market_close"]:
            skip_counts["observed_after_market_close"] += 1
            continue
        accepted, policy_reason = policy_accepts_event(policy, event)
        if not accepted:
            policy_reject_counts[policy_reason] += 1
            continue
        groups.setdefault(_event_window_key(event), []).append(event)

    intents: list[CopyIntent] = []
    group_rows: list[dict[str, Any]] = []
    submitted_positions = _submitted_position_by_key(live_ledger_state)
    strong_usage = _strong_tier_usage(live_ledger_state, now_ts=now_ts) if copy_model == DRIP_COPY_MODEL else {}
    for key, rows in groups.items():
        intent, row = _event_to_inventory_intent(
            key=key,
            events=rows,
            policy=policy,
            now_ts=now_ts,
            submitted_positions=submitted_positions,
            late_window_stop_s=late_window_stop_s,
            live_build_max_observed_age_s=live_build_max_observed_age_s,
            max_converge_orders_per_window=int(max_converge_orders_per_window),
            copy_model=copy_model,
            strong_tier_usage=strong_usage,
            strong_tier_suspended=strong_tier_suspended,
            drip_min_tranche_usd=float(drip_min_tranche_usd),
            drip_max_tranche_usd=float(drip_max_tranche_usd),
            observation_watermarks=observation_watermarks,
            min_live_floor_pin_enabled=bool(min_live_floor_pin_enabled),
        )
        if intent is None:
            skip_counts[str(row.get("reason") or "inventory_group_skipped")] += 1
            if len(samples) < 5:
                samples.append(row)
            group_rows.append(row)
            continue
        intents.append(intent)
        group_rows.append(row)

    intents = sorted(
        intents,
        key=lambda intent: (
            float(intent.copy_size_usd),
            intent.observed_ts or intent.event_ts or 0.0,
            intent.intent_id,
        ),
        reverse=True,
    )
    return intents, {
        "enabled": True,
        "flow_stage": "LIVE",
        "copy_model": copy_model,
        "input_events": len(events),
        "candidate_groups": len(groups),
        "retained_intents": len(intents),
        "submitted_position_groups": len(submitted_positions),
        "strong_tier_usage": strong_usage,
        "drip_min_tranche_usd": round(float(drip_min_tranche_usd), 6),
        "drip_max_tranche_usd": round(float(drip_max_tranche_usd), 6),
        "min_live_floor_pin_enabled": bool(min_live_floor_pin_enabled),
        "min_live_floor_pin_direction_id": (
            MIN_LIVE_FLOOR_PIN_DIRECTION_ID if min_live_floor_pin_enabled else None
        ),
        "max_event_age_s": round(float(max_event_age_s), 6),
        "live_build_max_observed_age_s": round(float(live_build_max_observed_age_s), 6),
        "late_window_stop_s": round(float(late_window_stop_s), 6),
        "max_converge_orders_per_window": int(max_converge_orders_per_window),
        "skip_counts": dict(sorted(skip_counts.items())),
        "policy_reject_counts": dict(sorted(policy_reject_counts.items())),
        "latest_source_event_runtime": latest,
        "inventory_window_participation": [_inventory_window_participation_row(row) for row in group_rows[:20]],
        "sample_inventory_groups": group_rows[:5],
        "sample_filtered_events": samples,
    }


def _load_observation_watermarks(path: str, *, source_wallet: str = "") -> dict[str, Any]:
    data = load_json(path, default={})
    data = data if isinstance(data, dict) else {}
    wallets = data.get("wallets") if isinstance(data.get("wallets"), dict) else {}
    if not source_wallet:
        return {str(wallet).lower(): row for wallet, row in wallets.items() if isinstance(row, dict)}
    wallet = str(source_wallet or "").strip().lower()
    row = wallets.get(wallet) or wallets.get(str(source_wallet or ""))
    if not isinstance(row, dict):
        return {}
    return {wallet: row}


def _signal_watermark_state(args: argparse.Namespace) -> str:
    signal_state = str(getattr(args, "rtds_signal_watermark_state", "") or "").strip()
    if signal_state:
        return signal_state
    return str(getattr(args, "rtds_watermark_state", DEFAULT_RTDS_WATERMARK_STATE))


def _execution_profiles_by_wallet(alpha_decay_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    execution_profiles = _as_dict(alpha_decay_report.get("execution_profiles"))
    profiles = execution_profiles.get("profiles_by_wallet")
    if isinstance(profiles, dict):
        return {
            str(wallet).lower(): row
            for wallet, row in profiles.items()
            if isinstance(row, dict)
        }
    return {}


def _load_alpha_decay_profile_cache(path: str | Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]], dict[str, Any]]:
    report_path = Path(path)
    cache_key = str(report_path)
    try:
        stat = report_path.stat()
        signature: tuple[int | None, int | None] = (int(stat.st_mtime_ns), int(stat.st_size))
    except OSError:
        signature = (None, None)

    cached = _ALPHA_DECAY_PROFILE_CACHE.get(cache_key)
    if cached and cached.get("signature") == signature:
        profiles = cached.get("profiles")
        report = cached.get("report")
        if isinstance(profiles, dict) and isinstance(report, dict):
            return report, profiles, {
                "enabled": True,
                "flow_stage": "LIVE",
                "path": cache_key,
                "cache_hit": True,
                "profiles_loaded": len(profiles),
                "signature": {"mtime_ns": signature[0], "size_bytes": signature[1]},
            }

    report = load_json(report_path, default={})
    report = report if isinstance(report, dict) else {}
    profiles = _execution_profiles_by_wallet(report)
    _ALPHA_DECAY_PROFILE_CACHE[cache_key] = {
        "signature": signature,
        "report": report,
        "profiles": profiles,
    }
    return report, profiles, {
        "enabled": True,
        "flow_stage": "LIVE",
        "path": cache_key,
        "cache_hit": False,
        "profiles_loaded": len(profiles),
        "signature": {"mtime_ns": signature[0], "size_bytes": signature[1]},
    }


def _eligible_move_slice_count(profile: dict[str, Any]) -> int:
    explicit = profile.get("eligible_move_slice_count")
    if explicit is not None:
        return int(num(explicit))
    slices_by_key = _as_dict(profile.get("move_slices_by_key"))
    return sum(1 for row in slices_by_key.values() if isinstance(row, dict) and row.get("eligible") is True)


def _matching_move_slice_profile(intent: CopyIntent, *, profile: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    move_slice = btc_5m_move_slice_for_values(
        market_slug=intent.market_slug,
        event_ts=intent.event_ts,
        price=float(intent.limit_price),
    )
    slices_by_key = _as_dict(profile.get("move_slices_by_key"))
    slice_profile = _as_dict(slices_by_key.get(str(move_slice.get("move_slice_key") or "")))
    return slice_profile, move_slice


def _drift_buffer_for_intent(
    intent: CopyIntent,
    *,
    profile: dict[str, Any],
    max_buffer_price: float,
) -> tuple[CopyIntent | None, dict[str, Any]]:
    """Apply the BTC-5m adaptive buffer as a CopyIntent transform.

    The transformed intent is used for both parity proof and live decision
    construction, preserving the single CopyIntent contract.
    """

    if str(intent.action or "").upper() != "BUY" or _btc_5m_window_start_s(intent.market_slug) is None:
        return intent, {"status": "SKIPPED", "reason": "not_btc5m_buy"}
    metadata = dict(intent.metadata or {})
    if _copy_model_is_inventory_like(metadata.get("copy_model")):
        source_limit = float(intent.limit_price)
        if 0.25 <= source_limit < 0.50:
            wallet_policy = (
                metadata.get("wallet_copy_policy")
                if isinstance(metadata.get("wallet_copy_policy"), dict)
                else {}
            )
            base_request_cap = float(
                wallet_policy.get("maker_min_share_base_request_cap_usd")
                or wallet_policy.get("effective_live_cap_usd")
                or intent.copy_size_usd
            )
            base_request_usd = min(float(intent.copy_size_usd), base_request_cap)
            base_request_shares = round(base_request_usd / source_limit, 6)
            metadata["drift_buffer"] = {
                "status": "PROTECTED_PASSIVE_EXACT_FIVE_PASS_THROUGH",
                "flow_stage": "LIVE/DEFEND",
                "source_limit_price": round(source_limit, 6),
                "buffered_limit_price": round(source_limit, 6),
                "buffer_price": 0.0,
                "expected_edge": None,
                "expected_edge_after_buffer": None,
                "measured_drift_source": "protected_passive_maker_boundary",
                "profile_source": f"{metadata.get('copy_model')}_not_alpha_decay_gated",
                "rule": "preserve_original_btc5m_inventory_or_drip_buy_price_inside_exact_five_passive_interval",
            }
            return CopyIntent.from_dict(
                {
                    **intent.asdict(),
                    "limit_price": round(source_limit, 6),
                    "copy_size_usd": round(base_request_usd, 6),
                    "shares": base_request_shares,
                    "metadata": metadata,
                }
            ), metadata["drift_buffer"]
        buffer_price = min(max(0.0, float(max_buffer_price)), 0.01)
        buffered_limit = round(min(0.99, source_limit + buffer_price), 4)
        buffered_shares = round(float(intent.copy_size_usd) / buffered_limit, 6) if buffered_limit > 0 else intent.shares
        metadata["drift_buffer"] = {
            "status": "APPLIED" if buffered_limit > source_limit else "PASS_THROUGH",
            "flow_stage": "LIVE",
            "source_limit_price": round(source_limit, 6),
            "buffered_limit_price": buffered_limit,
            "buffer_price": round(buffer_price, 9),
            "expected_edge": None,
            "expected_edge_after_buffer": None,
            "measured_drift_source": "inventory_vwap_tick_buffer",
            "profile_source": (
                "inventory_v2_not_alpha_decay_gated"
                if str(metadata.get("copy_model") or "") == "inventory"
                else f"{metadata.get('copy_model')}_not_alpha_decay_gated"
            ),
            "rule": "allow_inventory_or_drip_converge_at_running_vwap_plus_one_tick_before_best_ask_gate",
        }
        return CopyIntent.from_dict(
            {
                **intent.asdict(),
                "limit_price": buffered_limit,
                "shares": buffered_shares,
                "metadata": metadata,
            }
        ), metadata["drift_buffer"]
    selected_profile = profile
    profile_source = "wallet_execution_profile"
    move_slice_profile, move_slice = _matching_move_slice_profile(intent, profile=profile)
    if selected_profile.get("eligible") is not True and move_slice_profile.get("eligible") is True:
        selected_profile = move_slice_profile
        profile_source = "wallet_move_slice_profile"
    if selected_profile.get("eligible") is not True:
        return None, {
            "status": "BLOCKED",
            "reason": "execution_profile_not_eligible",
            "wallet": intent.source_wallet.lower(),
            "wallet_profile_eligible": bool(profile.get("eligible")),
            "move_slice_key": move_slice.get("move_slice_key"),
            "move_slice_profile_eligible": bool(move_slice_profile.get("eligible")),
            "move_slice_profile_blockers": move_slice_profile.get("blockers") or [],
        }
    mean_edge = num(selected_profile.get("mean_edge"))
    edge_stats = _as_dict(selected_profile.get("edge_stats"))
    measured_drift = max(0.0, num(edge_stats.get("p50"), mean_edge))
    buffer_price = min(max(0.0, measured_drift), max(0.0, mean_edge), max(0.0, float(max_buffer_price)))
    expected_edge_after_buffer = round(mean_edge - buffer_price, 9)
    if expected_edge_after_buffer <= 0.0:
        return None, {
            "status": "BLOCKED",
            "reason": "expected_edge_not_above_buffer_cost",
            "wallet": intent.source_wallet.lower(),
            "profile_source": profile_source,
            "move_slice_key": move_slice.get("move_slice_key"),
            "mean_edge": round(mean_edge, 9),
            "measured_drift": round(measured_drift, 9),
            "buffer_price": round(buffer_price, 9),
            "expected_edge_after_buffer": expected_edge_after_buffer,
        }
    source_limit = float(intent.limit_price)
    buffered_limit = min(0.99, source_limit + buffer_price)
    buffered_limit = round(buffered_limit, 4)
    buffered_shares = round(float(intent.copy_size_usd) / buffered_limit, 6) if buffered_limit > 0 else intent.shares
    metadata = dict(intent.metadata or {})
    metadata["drift_buffer"] = {
        "status": "APPLIED" if buffered_limit > source_limit else "PASS_THROUGH",
        "flow_stage": "LIVE",
        "source_limit_price": round(source_limit, 6),
        "buffered_limit_price": buffered_limit,
        "buffer_price": round(buffer_price, 9),
        "expected_edge": round(mean_edge, 9),
        "expected_edge_after_buffer": expected_edge_after_buffer,
        "measured_drift_source": "alpha_decay_edge_stats_p50_at_profile_latency",
        "profile_source": profile_source,
        "profile_latency_horizon_s": selected_profile.get("latency_horizon_s"),
        "profile_fill_sample": selected_profile.get("fill_sample"),
        "profile_copyable_rate_pct": selected_profile.get("copyable_rate_pct"),
        "move_slice_key": move_slice.get("move_slice_key"),
        "seconds_bucket": move_slice.get("seconds_bucket"),
        "entry_price_band": move_slice.get("entry_price_band"),
    }
    return CopyIntent.from_dict(
        {
            **intent.asdict(),
            "limit_price": buffered_limit,
            "shares": buffered_shares,
            "metadata": metadata,
        }
    ), metadata["drift_buffer"]


def _apply_drift_buffer_policy(
    intents: list[CopyIntent],
    *,
    alpha_decay_report: dict[str, Any],
    execution_profiles: dict[str, dict[str, Any]] | None = None,
    profile_cache_summary: dict[str, Any] | None = None,
    enabled: bool,
    max_buffer_price: float,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    if not enabled:
        return intents, {"enabled": False, "status": "SKIPPED", "input_intents": len(intents), "output_intents": len(intents)}
    profiles = execution_profiles if isinstance(execution_profiles, dict) else _execution_profiles_by_wallet(alpha_decay_report)
    output: list[CopyIntent] = []
    decisions: list[dict[str, Any]] = []
    blockers: Counter[str] = Counter()
    for intent in intents:
        profile = profiles.get(intent.source_wallet.lower()) or {}
        transformed, decision = _drift_buffer_for_intent(
            intent,
            profile=profile,
            max_buffer_price=float(max_buffer_price),
        )
        decision = {**decision, "intent_id": intent.intent_id, "source_wallet": intent.source_wallet.lower()}
        decisions.append(decision)
        if transformed is None:
            blockers[str(decision.get("reason") or "drift_buffer_blocked")] += 1
            continue
        output.append(transformed)
    return output, {
        "enabled": True,
        "flow_stage": "LIVE",
        "status": PASS if not blockers else ANALYZE,
        "input_intents": len(intents),
        "output_intents": len(output),
        "blocked_intents": len(intents) - len(output),
        "profiles_loaded": len(profiles),
        "profiles_with_move_slices": sum(1 for row in profiles.values() if _as_dict(row).get("move_slices_by_key")),
        "eligible_move_slices_loaded": sum(_eligible_move_slice_count(_as_dict(row)) for row in profiles.values()),
        "profile_cache": profile_cache_summary or {"enabled": False},
        "max_buffer_price": round(float(max_buffer_price), 6),
        "blocker_counts": dict(sorted(blockers.items())),
        "sample_decisions": decisions[:20],
    }


def _inventory_best_ask_from_book(book: dict[str, Any]) -> float:
    asks = [row for row in book.get("asks") or [] if isinstance(row, dict)]
    prices = [num(row.get("price")) for row in asks]
    prices = [price for price in prices if price > 0]
    return min(prices) if prices else 0.0


def _direct_clob_book(token_id: str, *, timeout_s: float) -> dict[str, Any]:
    response = requests.get(
        f"{CLOBMarketClient.DIRECT_CLOB_HOST}/book",
        params={"token_id": str(token_id)},
        timeout=max(0.1, float(timeout_s)),
        headers={"Accept": "application/json", "User-Agent": "wallet-copy-live-execution/1.0"},
    )
    response.raise_for_status()
    payload = response.json()
    book = dict(payload) if isinstance(payload, dict) else {}
    book["__walletCopyClobRouteReport"] = {
        "status": PASS,
        "route_class": "DIRECT_PASS",
        "host": "clob.polymarket.com",
        "routed_host": "clob.polymarket.com",
        "request_role": "inventory_best_ask_direct_clob_fallback",
        "source_base_override_configured": False,
    }
    return book


def _cached_inventory_best_ask_book(token_id: str) -> dict[str, Any] | None:
    now = time.monotonic()
    with _INVENTORY_BEST_ASK_BOOK_CACHE_LOCK:
        cached = _INVENTORY_BEST_ASK_BOOK_CACHE.get(str(token_id))
        if not isinstance(cached, dict):
            return None
        if float(cached.get("expires_at") or 0.0) <= now:
            _INVENTORY_BEST_ASK_BOOK_CACHE.pop(str(token_id), None)
            return None
        book = cached.get("book")
        return dict(book) if isinstance(book, dict) else None


def _remember_inventory_best_ask_book(token_id: str, book: dict[str, Any]) -> None:
    cached_book = dict(book)
    cached_book.setdefault("__bestAskObservedAtS", time.time())
    with _INVENTORY_BEST_ASK_BOOK_CACHE_LOCK:
        _INVENTORY_BEST_ASK_BOOK_CACHE[str(token_id)] = {
            "expires_at": time.monotonic() + INVENTORY_BEST_ASK_BOOK_CACHE_TTL_S,
            "book": cached_book,
        }


def _relay_fast_path_error(text: str) -> bool:
    lowered = str(text or "").lower()
    return any(marker in lowered for marker in ("503", "502", "504", "route_error", "relay"))


def _inventory_best_ask_probe(
    intent: CopyIntent,
    *,
    client: CLOBMarketClient,
    timeout_s: float,
) -> dict[str, Any]:
    started = time.perf_counter()
    evaluated_ts = time.time()
    token_id = str(intent.token_id or "")
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    drift = metadata.get("drift_buffer") if isinstance(metadata.get("drift_buffer"), dict) else {}
    source_price = num(drift.get("source_limit_price"), float(intent.limit_price))
    copy_size_usd = max(0.0, float(intent.copy_size_usd or 0.0))
    event_age_s = max(0.0, evaluated_ts - float(intent.event_ts)) if intent.event_ts is not None else None
    decision: dict[str, Any] = {
        "intent_id": intent.intent_id,
        "source_wallet": intent.source_wallet.lower(),
        "market_slug": intent.market_slug,
        "outcome": intent.outcome,
        "token_id": token_id,
        "limit_price": round(float(intent.limit_price), 6),
        "source_price": round(float(source_price), 6),
        "copy_size_usd": round(copy_size_usd, 6),
        "event_ts": intent.event_ts,
        "observed_ts": intent.observed_ts,
        "event_age_s": round(event_age_s, 6) if event_age_s is not None else None,
        "wallet_action": str(intent.action or "BUY").upper(),
        "best_ask_observed_at_s": round(evaluated_ts, 6),
        "status": "BLOCKED",
        "reason": "",
        "direct_clob_fallback_attempted": False,
        "direct_clob_fallback_passed": False,
        "direct_clob_fallback_error": False,
        "relay_503_fast_path": False,
    }
    if not token_id:
        decision["reason"] = "inventory_best_ask_token_missing"
        decision["fetch_duration_s"] = round(time.perf_counter() - started, 6)
        return decision
    cached = _cached_inventory_best_ask_book(token_id)
    if cached is not None:
        decision["best_ask_observed_at_s"] = round(
            num(cached.get("__bestAskObservedAtS"), evaluated_ts), 6
        )
        best_ask = _inventory_best_ask_from_book(cached)
        route_report = _as_dict(cached.get("__walletCopyClobRouteReport"))
        fill = CLOBMarketClient.summarize_book(
            cached,
            copy_size_usd=copy_size_usd,
            source_price=float(source_price),
            max_slippage_bps=150.0,
        )
        decision.update(
            {
                "best_ask": round(best_ask, 6),
                "max_copy_price": fill.get("max_copy_price"),
                "fillable_usd": fill.get("fillable_usd"),
                "fill_ratio": fill.get("fill_ratio"),
                "instant_fill_status": fill.get("instant_fill_status"),
                "blocking_reason": fill.get("blocking_reason"),
                "book_timestamp": fill.get("book_timestamp"),
                "book_hash": fill.get("book_hash"),
                "clob_route_status": route_report.get("status"),
                "book_cache_hit": True,
                "book_route_primary": "cache",
                "book_route_winner": "cache",
            }
        )
        decision["fetch_duration_s"] = round(time.perf_counter() - started, 6)
        return decision
    decision["book_cache_hit"] = False
    decision["book_route_primary"] = "direct_clob"
    decision["direct_clob_fallback_attempted"] = True
    try:
        book = _direct_clob_book(token_id, timeout_s=timeout_s)
        if isinstance(book, dict):
            book.setdefault("__bestAskObservedAtS", time.time())
        _remember_inventory_best_ask_book(token_id, book if isinstance(book, dict) else {})
        decision["best_ask_observed_at_s"] = round(
            num(book.get("__bestAskObservedAtS"), time.time())
            if isinstance(book, dict)
            else time.time(),
            6,
        )
        best_ask = _inventory_best_ask_from_book(book if isinstance(book, dict) else {})
        route_report = _as_dict(book.get("__walletCopyClobRouteReport")) if isinstance(book, dict) else {}
        fill = CLOBMarketClient.summarize_book(
            book if isinstance(book, dict) else {},
            copy_size_usd=copy_size_usd,
            source_price=float(source_price),
            max_slippage_bps=150.0,
        )
        decision.update(
            {
                "best_ask": round(best_ask, 6),
                "max_copy_price": fill.get("max_copy_price"),
                "fillable_usd": fill.get("fillable_usd"),
                "fill_ratio": fill.get("fill_ratio"),
                "instant_fill_status": fill.get("instant_fill_status"),
                "blocking_reason": fill.get("blocking_reason"),
                "book_timestamp": fill.get("book_timestamp"),
                "book_hash": fill.get("book_hash"),
                "clob_route_status": route_report.get("status"),
                "book_route_winner": "direct_clob",
                "direct_clob_fallback_passed": best_ask > 0,
            }
        )
    except Exception as exc:  # pragma: no cover - defensive around live CLOB route faults
        primary_error = str(exc)[:500]
        decision["direct_clob_primary_error"] = primary_error
        decision["direct_clob_fallback_error"] = True
        decision["relay_secondary_attempted"] = True
        try:
            book = client.get_book(token_id)
            if isinstance(book, dict):
                book.setdefault("__bestAskObservedAtS", time.time())
            _remember_inventory_best_ask_book(token_id, book if isinstance(book, dict) else {})
            decision["best_ask_observed_at_s"] = round(
                num(book.get("__bestAskObservedAtS"), time.time())
                if isinstance(book, dict)
                else time.time(),
                6,
            )
            best_ask = _inventory_best_ask_from_book(book if isinstance(book, dict) else {})
            route_report = _as_dict(book.get("__walletCopyClobRouteReport")) if isinstance(book, dict) else {}
            fill = CLOBMarketClient.summarize_book(
                book if isinstance(book, dict) else {},
                copy_size_usd=copy_size_usd,
                source_price=float(source_price),
                max_slippage_bps=150.0,
            )
            decision.update(
                {
                    "best_ask": round(best_ask, 6),
                    "max_copy_price": fill.get("max_copy_price"),
                    "fillable_usd": fill.get("fillable_usd"),
                    "fill_ratio": fill.get("fill_ratio"),
                    "instant_fill_status": fill.get("instant_fill_status"),
                    "blocking_reason": fill.get("blocking_reason"),
                    "book_timestamp": fill.get("book_timestamp"),
                    "book_hash": fill.get("book_hash"),
                    "clob_route_status": route_report.get("status"),
                    "book_route_winner": "relay_secondary",
                    "primary_book_error": primary_error,
                }
            )
        except Exception as fallback_exc:  # pragma: no cover - defensive around direct CLOB faults
            relay_error = str(fallback_exc)[:500]
            decision.update(
                {
                    "reason": "inventory_best_ask_book_error",
                    "error": primary_error,
                    "relay_secondary_error": True,
                    "relay_secondary_error_text": relay_error,
                    "direct_clob_fallback_error_text": relay_error,
                    "relay_503_fast_path": _relay_fast_path_error(relay_error),
                }
            )
    decision["fetch_duration_s"] = round(time.perf_counter() - started, 6)
    return decision


async def _inventory_best_ask_probes(
    intents: list[CopyIntent],
    *,
    client: CLOBMarketClient,
    timeout_s: float,
    max_concurrency: int = INVENTORY_BEST_ASK_MAX_CONCURRENCY,
) -> list[dict[str, Any]]:
    if not intents:
        return []
    concurrency = max(1, min(int(max_concurrency), len(intents)))
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor(max_workers=concurrency, thread_name_prefix="wallet-copy-clob-book") as executor:
        tasks = [
            loop.run_in_executor(
                executor,
                partial(_inventory_best_ask_probe, intent, client=client, timeout_s=timeout_s),
            )
            for intent in intents
        ]
        return list(await asyncio.gather(*tasks))


def _apply_inventory_best_ask_gate(
    intents: list[CopyIntent],
    *,
    timeout_s: float,
    enable_maker_fallback: bool = False,
    passive_at_source_sealed: bool | None = None,
    generation_sha256: str = "",
    max_sweep_drift_price: float = 0.05,
    enforce_copy_model_coverage: bool = True,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    gate_started = time.perf_counter()
    inventory_intents = [
        intent
        for intent in intents
        if isinstance(intent.metadata, dict) and _copy_model_is_inventory_like(intent.metadata.get("copy_model"))
    ]
    relay_timeout_s = max(0.1, min(float(timeout_s) / 3.0, 0.3))
    client = CLOBMarketClient(timeout_s=relay_timeout_s, retries=1)
    fetch_started = time.perf_counter()
    probe_results = asyncio.run(
        _inventory_best_ask_probes(
            inventory_intents,
            client=client,
            timeout_s=max(0.1, float(timeout_s)),
        )
    )
    fetch_duration_s = time.perf_counter() - fetch_started
    apply_started = time.perf_counter()
    probe_iter = iter(probe_results)
    output: list[CopyIntent] = []
    decisions: list[dict[str, Any]] = []
    blockers: Counter[str] = Counter()
    cache_hits = sum(1 for row in probe_results if row.get("book_cache_hit"))
    direct_primary_passes = sum(1 for row in probe_results if row.get("book_route_winner") == "direct_clob")
    relay_secondary_attempts = sum(1 for row in probe_results if row.get("relay_secondary_attempted"))
    relay_secondary_passes = sum(1 for row in probe_results if row.get("book_route_winner") == "relay_secondary")
    relay_secondary_errors = sum(1 for row in probe_results if row.get("relay_secondary_error"))
    relay_503_fast_path = sum(1 for row in probe_results if row.get("relay_503_fast_path"))
    maker_fallback_candidates = 0
    passive_at_source_sealed = (
        _passive_at_source_holdout_sealed()
        if passive_at_source_sealed is None
        else bool(passive_at_source_sealed)
    )
    for intent in intents:
        if not (
            isinstance(intent.metadata, dict)
            and _copy_model_is_inventory_like(intent.metadata.get("copy_model"))
        ):
            if enforce_copy_model_coverage and str(intent.action or "BUY").upper() == "BUY":
                reason = "copy_model_outside_best_ask_floor_coverage"
                blockers[reason] += 1
                decisions.append(
                    {
                        "status": "BLOCKED",
                        "reason": reason,
                        "intent_id": intent.intent_id,
                        "source_wallet": intent.source_wallet,
                        "market_slug": intent.market_slug,
                        "copy_model": (
                            intent.metadata.get("copy_model")
                            if isinstance(intent.metadata, dict)
                            else None
                        ),
                    }
                )
            else:
                output.append(intent)
            continue
        limit_price = float(intent.limit_price)
        decision = dict(next(probe_iter))
        if decision.get("reason") == "inventory_best_ask_token_missing":
            blockers[decision["reason"]] += 1
            decisions.append(decision)
            continue
        if decision.get("reason") == "inventory_best_ask_book_error":
            blockers[decision["reason"]] += 1
            decisions.append(decision)
            continue
        best_ask = num(decision.get("best_ask"))
        if best_ask <= 0:
            decision["reason"] = "inventory_best_ask_missing"
            blockers[decision["reason"]] += 1
            decisions.append(decision)
            continue
        gate_evaluated_at_s = time.time()
        best_ask_observed_at_s = num(decision.get("best_ask_observed_at_s"))
        gate_probe_best_ask_age_at_gate_s = max(
            0.0, gate_evaluated_at_s - best_ask_observed_at_s
        )
        decision["gate_evaluated_at_s"] = round(gate_evaluated_at_s, 6)
        decision["gate_probe_best_ask_age_at_gate_s"] = round(
            gate_probe_best_ask_age_at_gate_s, 6
        )
        if best_ask < RULED_01A_GATE_PROBE_PRICE_MIN - 1e-9:
            decision["reason"] = "below_ruled_entry_floor"
            decision["ruled_entry_floor"] = RULED_01A_ENTRY_PRICE_MIN
            decision["gate_probe_entry_floor"] = RULED_01A_GATE_PROBE_PRICE_MIN
            blockers[decision["reason"]] += 1
            decisions.append(decision)
            continue
        if gate_probe_best_ask_age_at_gate_s > INVENTORY_BEST_ASK_MAX_AGE_AT_GATE_S:
            stale_probe = dict(decision)
            token_id = str(decision.get("token_id") or intent.token_id or "")
            with _INVENTORY_BEST_ASK_BOOK_CACHE_LOCK:
                _INVENTORY_BEST_ASK_BOOK_CACHE.pop(token_id, None)
            decision = _inventory_best_ask_probe(
                intent,
                client=client,
                timeout_s=max(0.1, float(timeout_s)),
            )
            decision["stale_reprobe"] = {
                "attempted": True,
                "initial_gate_probe_best_ask": round(best_ask, 6),
                "initial_gate_probe_best_ask_age_at_gate_s": round(
                    gate_probe_best_ask_age_at_gate_s, 6
                ),
                "initial_book_route_winner": stale_probe.get("book_route_winner"),
            }
            if decision.get("reason") in {
                "inventory_best_ask_token_missing",
                "inventory_best_ask_book_error",
            }:
                blockers[str(decision["reason"])] += 1
                decisions.append(decision)
                continue
            best_ask = num(decision.get("best_ask"))
            if best_ask <= 0:
                decision["reason"] = "inventory_best_ask_missing"
                blockers[decision["reason"]] += 1
                decisions.append(decision)
                continue
            gate_evaluated_at_s = time.time()
            best_ask_observed_at_s = num(decision.get("best_ask_observed_at_s"))
            gate_probe_best_ask_age_at_gate_s = max(
                0.0, gate_evaluated_at_s - best_ask_observed_at_s
            )
            decision["gate_evaluated_at_s"] = round(gate_evaluated_at_s, 6)
            decision["gate_probe_best_ask_age_at_gate_s"] = round(
                gate_probe_best_ask_age_at_gate_s, 6
            )
            if best_ask < RULED_01A_GATE_PROBE_PRICE_MIN - 1e-9:
                decision["reason"] = "below_ruled_entry_floor"
                decision["ruled_entry_floor"] = RULED_01A_ENTRY_PRICE_MIN
                decision["gate_probe_entry_floor"] = RULED_01A_GATE_PROBE_PRICE_MIN
                blockers[decision["reason"]] += 1
                decisions.append(decision)
                continue
            if gate_probe_best_ask_age_at_gate_s > INVENTORY_BEST_ASK_MAX_AGE_AT_GATE_S:
                decision["reason"] = "stale_book_at_gate"
                decision["max_gate_probe_best_ask_age_at_gate_s"] = (
                    INVENTORY_BEST_ASK_MAX_AGE_AT_GATE_S
                )
                blockers[decision["reason"]] += 1
                decisions.append(decision)
                continue
        source_price = num(decision.get("source_price"), limit_price)
        max_copy_price = num(decision.get("max_copy_price"), limit_price)
        passive_price = min(source_price, limit_price, max_copy_price)
        maker_fallback_allowed = bool(
            enable_maker_fallback
            and source_price <= INVENTORY_MAKER_FALLBACK_PRICE_CEILING + 1e-9
            and passive_price > 0
            and passive_price <= max_copy_price + 1e-9
            and best_ask > limit_price + 1e-9
        )
        if maker_fallback_allowed and passive_at_source_sealed:
            decision.update(
                {
                    "reason": "inventory_best_ask_above_limit_passive_lane_sealed",
                    "passive_at_source_sealed": True,
                    "maker_fallback_candidate": False,
                }
            )
            blockers[decision["reason"]] += 1
            decisions.append(decision)
            continue
        if best_ask > limit_price + 1e-9 and not maker_fallback_allowed:
            decision["reason"] = "inventory_best_ask_above_vwap_plus_buffer"
            blockers[decision["reason"]] += 1
            decisions.append(decision)
            continue
        metadata = dict(intent.metadata or {})
        sweep_clamped_limit = min(
            limit_price,
            best_ask + max(0.0, float(max_sweep_drift_price)),
        )
        metadata["inventory_best_ask_gate"] = {
            "status": PASS,
            "flow_stage": "LIVE",
            "best_ask": round(best_ask, 6),
            "gate_probe_best_ask": round(best_ask, 6),
            "best_ask_observed_at_s": decision.get("best_ask_observed_at_s"),
            "gate_evaluated_at_s": decision.get("gate_evaluated_at_s"),
            "gate_probe_best_ask_age_at_gate_s": decision.get(
                "gate_probe_best_ask_age_at_gate_s"
            ),
            "event_age_s": decision.get("event_age_s"),
            "max_gate_probe_best_ask_age_at_gate_s": INVENTORY_BEST_ASK_MAX_AGE_AT_GATE_S,
            "book_cache_ttl_s": INVENTORY_BEST_ASK_BOOK_CACHE_TTL_S,
            "threshold_independent_of_cache_ttl": (
                INVENTORY_BEST_ASK_MAX_AGE_AT_GATE_S
                < INVENTORY_BEST_ASK_BOOK_CACHE_TTL_S
            ),
            "generation_sha256": str(generation_sha256 or "").strip() or None,
            "ruled_entry_floor": RULED_01A_ENTRY_PRICE_MIN,
            "gate_probe_entry_floor": RULED_01A_GATE_PROBE_PRICE_MIN,
            "max_sweep_drift_price": round(
                max(0.0, float(max_sweep_drift_price)), 6
            ),
            "original_intent_limit_price": round(limit_price, 6),
            "submitted_limit_price": round(sweep_clamped_limit, 6),
            "sweep_clamp_applied": sweep_clamped_limit < limit_price - 1e-9,
            "below_ruled_entry_floor": False,
            "max_converge_price": round(limit_price, 6),
            "rule": "best_ask_at_or_below_running_vwap_plus_drift_buffer",
            "maker_fallback_candidate": maker_fallback_allowed,
        }
        if maker_fallback_allowed:
            maker_fallback_candidates += 1
            metadata["inventory_best_ask_gate"].update(
                {
                    "reason": "best_ask_above_buffered_limit_allowed_passive_at_source",
                    "maker_fallback_price_ceiling": round(INVENTORY_MAKER_FALLBACK_PRICE_CEILING, 6),
                    "execution_path": "direct_post_only_gtc_at_source",
                    "original_source_price": round(source_price, 6),
                    "buffered_limit_price": round(limit_price, 6),
                    "max_copy_price": round(max_copy_price, 6),
                    "passive_price": round(passive_price, 6),
                    "best_ask": round(best_ask, 6),
                    "cancel_policy": "cancel_at_btc_5m_window_end",
                }
            )
        metadata["inventory_best_ask_gate"]["book_route_primary"] = str(
            decision.get("book_route_primary") or ""
        )
        metadata["inventory_best_ask_gate"]["book_route_winner"] = str(
            decision.get("book_route_winner") or ""
        )
        metadata["inventory_best_ask_gate"]["book_cache_hit"] = bool(decision.get("book_cache_hit"))
        if decision.get("relay_secondary_attempted"):
            metadata["inventory_best_ask_gate"]["relay_secondary_attempted"] = True
            metadata["inventory_best_ask_gate"]["direct_clob_primary_error"] = str(
                decision.get("direct_clob_primary_error") or ""
            )[:500]
        intent_payload = {**intent.asdict(), "metadata": metadata}
        if maker_fallback_allowed:
            intent_payload.update(
                {
                    "limit_price": round(passive_price, 6),
                    "shares": round(float(intent.copy_size_usd) / passive_price, 6),
                }
            )
        elif sweep_clamped_limit < limit_price - 1e-9:
            intent_payload.update(
                {
                    "limit_price": round(sweep_clamped_limit, 6),
                    "shares": round(
                        float(intent.copy_size_usd) / sweep_clamped_limit,
                        6,
                    ),
                }
            )
        output.append(CopyIntent.from_dict(intent_payload))
        decision.update(
            {
                "status": PASS,
                "reason": "maker_fallback_candidate" if maker_fallback_allowed else "accepted",
                "maker_fallback_candidate": maker_fallback_allowed,
                "submitted_limit_price": round(sweep_clamped_limit, 6),
                "sweep_clamp_applied": sweep_clamped_limit < limit_price - 1e-9,
            }
        )
        decisions.append(decision)

    gate_age_samples = sorted(
        float(decision["gate_probe_best_ask_age_at_gate_s"])
        for decision in decisions
        if isinstance(
            decision.get("gate_probe_best_ask_age_at_gate_s"),
            (int, float),
        )
    )
    gate_age_p95_index = (
        max(0, math.ceil(0.95 * len(gate_age_samples)) - 1)
        if gate_age_samples
        else 0
    )
    return output, {
        "enabled": True,
        "flow_stage": "LIVE",
        "copy_model": "inventory_like",
        "enforce_copy_model_coverage": enforce_copy_model_coverage,
        "status": PASS if not blockers else ANALYZE,
        "input_intents": len(intents),
        "inventory_intents": len(inventory_intents),
        "output_intents": len(output),
        "blocked_intents": len(intents) - len(output),
        "timeout_s": round(max(0.1, float(timeout_s)), 6),
        "relay_timeout_s": round(relay_timeout_s, 6),
        "book_cache_ttl_s": round(INVENTORY_BEST_ASK_BOOK_CACHE_TTL_S, 6),
        "max_gate_probe_best_ask_age_at_gate_s": round(
            INVENTORY_BEST_ASK_MAX_AGE_AT_GATE_S, 6
        ),
        "threshold_independent_of_cache_ttl": (
            INVENTORY_BEST_ASK_MAX_AGE_AT_GATE_S
            < INVENTORY_BEST_ASK_BOOK_CACHE_TTL_S
        ),
        "generation_sha256": str(generation_sha256 or "").strip() or None,
        "gate_probe_best_ask_age_at_gate_s": {
            "scope": "current_guard_cycle_all_gate_evaluations_including_abstains",
            "count": len(gate_age_samples),
            "min": round(gate_age_samples[0], 6) if gate_age_samples else None,
            "median": round(statistics.median(gate_age_samples), 6)
            if gate_age_samples
            else None,
            "p95": round(gate_age_samples[gate_age_p95_index], 6)
            if gate_age_samples
            else None,
            "max": round(gate_age_samples[-1], 6) if gate_age_samples else None,
        },
        "book_cache_hits": cache_hits,
        "direct_clob_primary_passes": direct_primary_passes,
        "relay_secondary_attempts": relay_secondary_attempts,
        "relay_secondary_passes": relay_secondary_passes,
        "relay_secondary_errors": relay_secondary_errors,
        "relay_503_fast_path": relay_503_fast_path,
        "direct_clob_fallback_attempts": relay_secondary_attempts,
        "direct_clob_fallback_passes": direct_primary_passes + cache_hits,
        "direct_clob_fallback_errors": relay_secondary_errors,
        "maker_fallback_enabled": bool(enable_maker_fallback),
        "passive_at_source_sealed": passive_at_source_sealed,
        "maker_fallback_price_ceiling": round(INVENTORY_MAKER_FALLBACK_PRICE_CEILING, 6),
        "maker_fallback_candidates": maker_fallback_candidates,
        "runtime_profile": {
            "flow_stage": "SELF-DEV",
            "total_s": round(time.perf_counter() - gate_started, 6),
            "substage_timers": [
                {"name": "parallel_clob_book_fetch", "duration_s": round(fetch_duration_s, 6)},
                {"name": "apply_book_gate_decisions", "duration_s": round(time.perf_counter() - apply_started, 6)},
            ],
            "clob_book_fetch_concurrency": max(1, min(INVENTORY_BEST_ASK_MAX_CONCURRENCY, len(inventory_intents))),
            "book_cache_hits": cache_hits,
            "direct_clob_primary_passes": direct_primary_passes,
            "relay_secondary_attempts": relay_secondary_attempts,
            "relay_503_fast_path": relay_503_fast_path,
            "relay_timeout_s": round(relay_timeout_s, 6),
            "relay_retries": 1,
        },
        "blocker_counts": dict(sorted(blockers.items())),
        "blocker_taxonomy": {
            "stale_book_at_gate": (
                "gate probe best ask was older than the ruled threshold at gate evaluation"
            ),
            "below_ruled_entry_floor": (
                "gate best ask is below the one-tick-protected 0.26 probe floor"
            ),
            "inventory_best_ask_below_ruled_entry_floor": (
                "legacy label superseded by below_ruled_entry_floor"
            ),
            "copy_model_outside_best_ask_floor_coverage": (
                "live BUY copy model is outside the best-ask floor coverage set"
            ),
        },
        "sample_decisions": decisions[:20],
    }


def _apply_live_hard_buy_price_cap(
    intents: list[CopyIntent],
    *,
    max_buy_price: float,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    cap = max(0.0, float(max_buy_price))
    if cap <= 0:
        return intents, {
            "enabled": False,
            "flow_stage": "LIVE",
            "reason": "wallet_copy_max_buy_price_not_configured",
            "input_intents": len(intents),
            "output_intents": len(intents),
        }
    output: list[CopyIntent] = []
    filtered: list[dict[str, Any]] = []
    clamped: list[dict[str, Any]] = []
    for intent in intents:
        is_buy = str(intent.action or "").upper() == "BUY"
        price = float(intent.limit_price)
        if is_buy and price > cap + 1e-9:
            metadata = dict(intent.metadata or {})
            drift_buffer = metadata.get("drift_buffer") if isinstance(metadata.get("drift_buffer"), dict) else {}
            source_limit = num(drift_buffer.get("source_limit_price"))
            if drift_buffer.get("status") == "APPLIED" and 0.0 < source_limit <= cap + 1e-9:
                capped_price = round(cap, 4)
                capped_shares = round(float(intent.copy_size_usd) / capped_price, 6) if capped_price > 0 else intent.shares
                metadata["hard_entry_cap"] = {
                    "status": "CLAMPED_TO_CAP",
                    "flow_stage": "LIVE",
                    "rule": "drift_buffered_buy_limit_was_clamped_to_active_hard_entry_cap",
                    "source_limit_price": round(source_limit, 6),
                    "buffered_limit_price": round(price, 6),
                    "max_buy_price": round(cap, 6),
                }
                output.append(
                    CopyIntent.from_dict(
                        {
                            **intent.asdict(),
                            "limit_price": capped_price,
                            "shares": capped_shares,
                            "metadata": metadata,
                        }
                    )
                )
                clamped.append(
                    {
                        "intent_id": intent.intent_id,
                        "source_wallet": intent.source_wallet.lower(),
                        "market_slug": intent.market_slug,
                        "outcome": intent.outcome,
                        "source_limit_price": round(source_limit, 6),
                        "buffered_limit_price": round(price, 6),
                        "clamped_limit_price": capped_price,
                        "taxonomy": "hard_entry_cap_clamped",
                    }
                )
                continue
            is_fable_045_band = abs(cap - 0.45) < 1e-9 and price <= 0.50 + 1e-9
            taxonomy = "price_cap_045" if is_fable_045_band else "hard_entry_cap_skip"
            row = {
                "intent_id": intent.intent_id,
                "source_wallet": intent.source_wallet.lower(),
                "market_slug": intent.market_slug,
                "condition_id": intent.condition_id,
                "outcome": intent.outcome,
                "side": intent.side,
                "token_id": intent.token_id,
                "limit_price": round(price, 6),
                "max_buy_price": round(cap, 6),
                "copy_size_usd": round(float(intent.copy_size_usd), 6),
                "taxonomy": taxonomy,
                "reject_reason": taxonomy,
                "shadow_counterfactual_retained": True,
                "shadow_outcome_status": "PENDING_RESOLUTION",
            }
            if is_fable_045_band:
                row.update(
                    {
                        "flow_stage": "LIVE/LEARN",
                        "price_cap_rule_id": "fable-20260712T0044-price-cap-045",
                        "previous_max_buy_price": 0.50,
                        "price_cap_band": "45_50",
                    }
                )
            filtered.append(row)
            continue
        output.append(intent)
    taxonomy_counts: Counter[str] = Counter(str(row.get("taxonomy") or "") for row in filtered)
    taxonomy_counts.pop("", None)
    return output, {
        "enabled": True,
        "flow_stage": "LIVE",
        "status": PASS,
        "rule": "drop_live_buy_copyintents_above_active_hard_entry_cap_before_parity_or_submission",
        "input_intents": len(intents),
        "output_intents": len(output),
        "filtered_intents": len(filtered),
        "blocked_intents": len(filtered),
        "clamped_intents": len(clamped),
        "max_buy_price": round(cap, 6),
        "taxonomy_counts": dict(sorted(taxonomy_counts.items())),
        "sample_filtered_intents": filtered[:20],
        "sample_clamped_intents": clamped[:20],
    }


def _apply_live_hard_buy_price_floor(
    intents: list[CopyIntent],
    *,
    min_buy_price: float,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    floor = max(0.0, float(min_buy_price))
    if floor <= 0:
        return intents, {
            "enabled": False,
            "flow_stage": "LIVE",
            "reason": "wallet_copy_min_buy_price_not_configured",
            "input_intents": len(intents),
            "output_intents": len(intents),
        }
    output: list[CopyIntent] = []
    filtered: list[dict[str, Any]] = []
    for intent in intents:
        is_buy = str(intent.action or "").upper() == "BUY"
        price = float(intent.limit_price)
        if is_buy and price < floor - 1e-9:
            filtered.append(
                {
                    "intent_id": intent.intent_id,
                    "source_wallet": intent.source_wallet.lower(),
                    "market_slug": intent.market_slug,
                    "outcome": intent.outcome,
                    "limit_price": round(price, 6),
                    "min_buy_price": round(floor, 6),
                    "taxonomy": "hard_entry_floor_skip",
                }
            )
            continue
        output.append(intent)
    return output, {
        "enabled": True,
        "flow_stage": "LIVE",
        "status": PASS,
        "rule": "drop_live_buy_copyintents_below_active_hard_entry_floor_before_parity_or_submission",
        "input_intents": len(intents),
        "output_intents": len(output),
        "filtered_intents": len(filtered),
        "blocked_intents": len(filtered),
        "min_buy_price": round(floor, 6),
        "taxonomy_counts": {"hard_entry_floor_skip": len(filtered)} if filtered else {},
        "sample_filtered_intents": filtered[:20],
    }


def _intent_window_time_s(intent: CopyIntent, *, now_ts: float) -> float | None:
    window_start = _btc_5m_window_start_s(intent.market_slug)
    if window_start is None:
        return None
    return max(0.0, float(now_ts) - float(window_start))


def _intent_signal_age_s(intent: CopyIntent) -> float | None:
    if intent.api_latency_s is not None and float(intent.api_latency_s) >= 0:
        return max(0.0, float(intent.api_latency_s))
    if (
        intent.event_ts is not None
        and intent.observed_ts is not None
        and float(intent.event_ts) > 0
        and float(intent.observed_ts) > 0
    ):
        return max(0.0, float(intent.observed_ts) - float(intent.event_ts))
    return None


def _intent_latency_split(intent: CopyIntent) -> dict[str, Any]:
    event_ts = float(intent.event_ts) if intent.event_ts is not None and float(intent.event_ts) > 0 else None
    observed_ts = (
        float(intent.observed_ts) if intent.observed_ts is not None and float(intent.observed_ts) > 0 else None
    )
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    detection_source = str(metadata.get("detection_source") or "").strip()
    observation_sources = metadata.get("observation_sources")
    if not isinstance(observation_sources, list):
        observation_sources = []
    api_indexing_lag_s = None
    if intent.api_latency_s is not None and float(intent.api_latency_s) >= 0:
        api_indexing_lag_s = max(0.0, float(intent.api_latency_s))
    elif event_ts is not None and observed_ts is not None:
        api_indexing_lag_s = max(0.0, observed_ts - event_ts)
    status = "PASS"
    finding = None
    if api_indexing_lag_s is None:
        status = "MISSING_TIMESTAMPS"
    elif api_indexing_lag_s >= 60.0:
        status = "DATAAPI_INDEXING_LAG_GTE_60S"
        finding = "dataapi_indexing_lag_alone_cannot_pass_60s_signal_age_bar"
    return {
        "event_ts": None if event_ts is None else round(event_ts, 6),
        "detection_observed_ts": None if observed_ts is None else round(observed_ts, 6),
        "observed_ts": None if observed_ts is None else round(observed_ts, 6),
        "dataapi_first_seen_ts": None if observed_ts is None else round(observed_ts, 6),
        "detection_source": detection_source,
        "observation_sources": [str(source) for source in observation_sources if str(source or "")],
        "alternate_observed_ts": metadata.get("alternate_observed_ts"),
        "alternate_detection_source": metadata.get("alternate_detection_source"),
        "api_indexing_lag_s": None if api_indexing_lag_s is None else round(api_indexing_lag_s, 6),
        "poll_wait_s": None,
        "latency_split_status": status,
        "latency_split_finding": finding,
        "latency_split_note": "poll_wait unavailable on CopyIntent; Data API first_seen split is ledgered for WS comparison",
    }


def _apply_profit_latency_suppression_gate(
    intents: list[CopyIntent],
    *,
    now_ts: float,
    window_time_suppress_gte_s: float,
    signal_age_suppress_gte_s: float,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    window_threshold = max(0.0, float(window_time_suppress_gte_s))
    signal_threshold = max(0.0, float(signal_age_suppress_gte_s))
    window_time_taxonomy = f"window_time_gte_{int(window_threshold)}s"
    if window_threshold <= 0 and signal_threshold <= 0:
        return intents, {
            "enabled": False,
            "flow_stage": "LIVE/LEARN",
            "reason": "profit_latency_suppression_not_configured",
            "input_intents": len(intents),
            "output_intents": len(intents),
            "blocked_intents": 0,
        }
    output: list[CopyIntent] = []
    filtered: list[dict[str, Any]] = []
    passed: list[dict[str, Any]] = []
    taxonomy_counts: Counter[str] = Counter()
    for intent in intents:
        is_buy = str(intent.action or "").upper() == "BUY"
        window_time_s = _intent_window_time_s(intent, now_ts=now_ts)
        signal_age_s = _intent_signal_age_s(intent)
        latency_split = _intent_latency_split(intent)
        taxonomies: list[str] = []
        if is_buy and window_threshold > 0 and window_time_s is not None and window_time_s >= window_threshold:
            taxonomies.append(window_time_taxonomy)
        if is_buy and signal_threshold > 0 and signal_age_s is not None and signal_age_s >= signal_threshold:
            taxonomies.append("signal_age_gte_60s")
        if taxonomies:
            for taxonomy in taxonomies:
                taxonomy_counts[taxonomy] += 1
            filtered.append(
                {
                    "intent_id": intent.intent_id,
                    "source_wallet": intent.source_wallet.lower(),
                    "market_slug": intent.market_slug,
                    "outcome": intent.outcome,
                    "limit_price": round(float(intent.limit_price), 6),
                    "copy_size_usd": round(float(intent.copy_size_usd), 6),
                    "window_time_s": None if window_time_s is None else round(window_time_s, 6),
                    "window_time_suppress_gte_s": round(window_threshold, 6),
                    "signal_age_s": None if signal_age_s is None else round(signal_age_s, 6),
                    "signal_age_suppress_gte_s": round(signal_threshold, 6),
                    **latency_split,
                    "taxonomy": taxonomies[0],
                    "taxonomy_tags": taxonomies,
                    "reject_reason": "+".join(taxonomies),
                    "shadow_counterfactual_retained": True,
                }
            )
            continue
        passed.append(
            {
                "intent_id": intent.intent_id,
                "source_wallet": intent.source_wallet.lower(),
                "market_slug": intent.market_slug,
                "outcome": intent.outcome,
                "limit_price": round(float(intent.limit_price), 6),
                "copy_size_usd": round(float(intent.copy_size_usd), 6),
                "window_time_s": None if window_time_s is None else round(window_time_s, 6),
                "signal_age_s": None if signal_age_s is None else round(signal_age_s, 6),
                **latency_split,
                "taxonomy": "profit_latency_pass",
            }
        )
        output.append(intent)
    return output, {
        "enabled": True,
        "flow_stage": "LIVE/LEARN",
        "status": PASS,
        "rule": "suppress_live_buy_copyintents_in_lossy_window_time_or_stale_signal_buckets_before_submission",
        "input_intents": len(intents),
        "output_intents": len(output),
        "filtered_intents": len(filtered),
        "blocked_intents": len(filtered),
        "window_time_suppress_gte_s": round(window_threshold, 6),
        "signal_age_suppress_gte_s": round(signal_threshold, 6),
        "taxonomy_counts": dict(sorted(taxonomy_counts.items())),
        "shadow_counterfactual": {
            "retained": True,
            "basis": "filtered CopyIntent remains in arm-state diagnostics for suppressed-signal counterfactual pricing",
            "re_enable_bar": ">=20 shadow-priced suppressed signals with positive aggregate counterfactual PnL",
        },
        "sample_filtered_intents": filtered[:20],
        "filtered_intents_detail": filtered,
        "sample_passed_intents": passed[:20],
    }


def _append_profit_latency_suppression_events(
    event_log_path: str,
    summary: dict[str, Any],
    *,
    decision_ts: str,
) -> int:
    raw_rows = summary.get("filtered_intents_detail")
    rows = [row for row in raw_rows if isinstance(row, dict)] if isinstance(raw_rows, list) else []
    if not rows:
        return 0
    events: list[dict[str, Any]] = []
    for row in rows:
        raw_tags = row.get("taxonomy_tags")
        tags = [str(tag) for tag in raw_tags if str(tag)] if isinstance(raw_tags, list) else []
        events.append(
            {
                "event": "wallet_copy_live_profit_latency_suppression_reject",
                "event_type": "FABLE_APPROVED_SUPPRESSION_REJECT",
                "approved_suppression": True,
                "flow_stage": "LIVE/LEARN",
                "fable_direction": "2026-07-07T22:05:32Z",
                "ts": decision_ts,
                "intent_id": row.get("intent_id"),
                "source_wallet": row.get("source_wallet"),
                "market_slug": row.get("market_slug"),
                "outcome": row.get("outcome"),
                "limit_price": row.get("limit_price"),
                "copy_size_usd": row.get("copy_size_usd"),
                "window_time_s": row.get("window_time_s"),
                "window_time_suppress_gte_s": row.get("window_time_suppress_gte_s"),
                "signal_age_s": row.get("signal_age_s"),
                "signal_age_suppress_gte_s": row.get("signal_age_suppress_gte_s"),
                "event_ts": row.get("event_ts"),
                "detection_observed_ts": row.get("detection_observed_ts"),
                "observed_ts": row.get("observed_ts"),
                "dataapi_first_seen_ts": row.get("dataapi_first_seen_ts"),
                "detection_source": row.get("detection_source"),
                "observation_sources": row.get("observation_sources") or [],
                "alternate_observed_ts": row.get("alternate_observed_ts"),
                "alternate_detection_source": row.get("alternate_detection_source"),
                "api_indexing_lag_s": row.get("api_indexing_lag_s"),
                "poll_wait_s": row.get("poll_wait_s"),
                "latency_split_status": row.get("latency_split_status"),
                "latency_split_finding": row.get("latency_split_finding"),
                "latency_split_note": row.get("latency_split_note"),
                "taxonomy": row.get("taxonomy"),
                "taxonomy_tags": tags,
                "reject_reason": row.get("reject_reason"),
                "shadow_counterfactual_retained": bool(row.get("shadow_counterfactual_retained")),
            }
        )
    append_jsonl_many(event_log_path, events)
    return len(events)


def _price_bucket(price: float) -> str:
    if price < 0.25:
        return "00_00_25"
    if price < 0.50:
        return "01_25_50"
    if price < 0.70:
        return "02_50_70"
    return "03_70_100"


def _toxicity_direction(intent: CopyIntent) -> str:
    text = str(intent.outcome or intent.side or "").strip().upper()
    if text in {"UP", "YES"}:
        return "UP"
    if text in {"DOWN", "NO"}:
        return "DOWN"
    return ""


def _load_toxicity_deny_cells(config_path: str) -> tuple[set[tuple[str, str, str]], dict[str, Any]]:
    loaded = load_json(config_path, default={})
    if not isinstance(loaded, dict):
        return set(), {"enabled": False, "reason": "config_invalid", "path": config_path}
    cells: set[tuple[str, str, str]] = set()
    raw_cells = loaded.get("cells") if isinstance(loaded.get("cells"), list) else []
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
        "criteria": loaded.get("criteria") if isinstance(loaded.get("criteria"), dict) else {},
        "configured_cells": len(cells),
    }


def _apply_toxicity_protection_gate(
    intents: list[CopyIntent],
    *,
    config_path: str,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    cells, base_summary = _load_toxicity_deny_cells(config_path)
    if not base_summary.get("enabled") or not cells:
        return intents, {
            **base_summary,
            "flow_stage": "LIVE",
            "input_intents": len(intents),
            "output_intents": len(intents),
            "blocked_intents": 0,
        }
    output: list[CopyIntent] = []
    filtered: list[dict[str, Any]] = []
    for intent in intents:
        price = float(intent.limit_price)
        wallet = str(intent.source_wallet or "").lower()
        bucket = _price_bucket(price)
        direction = _toxicity_direction(intent)
        cell = (wallet, bucket, direction)
        wildcard_cell = (wallet, bucket, "")
        if str(intent.action or "").upper() == "BUY" and (cell in cells or wildcard_cell in cells):
            filtered.append(
                {
                    "intent_id": intent.intent_id,
                    "source_wallet": wallet,
                    "market_slug": intent.market_slug,
                    "outcome": intent.outcome,
                    "price_bucket": bucket,
                    "direction": direction,
                    "limit_price": round(price, 6),
                    "taxonomy": "toxicity_protection",
                    "reject_reason": "toxicity_protection",
                }
            )
            continue
        output.append(intent)
    return output, {
        **base_summary,
        "flow_stage": "LIVE",
        "status": PASS,
        "rule": "guard_side_reject_source_wallet_x_price_bucket_x_direction_cells_with_negative_our_fill_roi",
        "reject_reason": "toxicity_protection",
        "input_intents": len(intents),
        "output_intents": len(output),
        "blocked_intents": len(filtered),
        "taxonomy_counts": {"toxicity_protection": len(filtered)} if filtered else {},
        "sample_filtered_intents": filtered[:20],
    }


def _load_entry_price_band_gate(config_path: str) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    loaded = load_json(config_path, default={})
    if not isinstance(loaded, dict):
        return [], {"enabled": False, "reason": "config_invalid", "path": config_path}
    raw_bands = loaded.get("blocked_bands") if isinstance(loaded.get("blocked_bands"), list) else []
    bands: list[dict[str, Any]] = []
    for row in raw_bands:
        if not isinstance(row, dict):
            continue
        try:
            minimum = float(row.get("min_price_inclusive"))
            maximum = float(row.get("max_price_exclusive"))
        except (TypeError, ValueError):
            continue
        if 0.0 <= minimum < maximum <= 1.01:
            bands.append({**row, "min_price_inclusive": minimum, "max_price_exclusive": maximum})
    enabled = loaded.get("enabled") is True and bool(bands)
    shadow_mode = str(loaded.get("shadow_mode") or "")
    inverted_shadow = not enabled and bool(bands) and shadow_mode == "inverted_post_removal"
    return bands, {
        "enabled": enabled,
        "inverted_shadow": inverted_shadow,
        "shadow_mode": shadow_mode or None,
        "path": config_path,
        "direction_id": loaded.get("direction_id"),
        "experiment_id": loaded.get("experiment_id"),
        "shadow_experiment_id": loaded.get("shadow_experiment_id"),
        "generated_at": loaded.get("generated_at"),
        "source_artifact": loaded.get("source_artifact"),
        "blocked_bands": bands,
        "reason": (
            None
            if enabled
            else "gate_removed_inverted_shadow_active"
            if inverted_shadow
            else "gate_disabled_or_no_valid_blocked_bands"
        ),
    }


def _apply_entry_price_band_gate(
    intents: list[CopyIntent],
    *,
    config_path: str,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    bands, base = _load_entry_price_band_gate(config_path)
    if not base.get("enabled") and not base.get("inverted_shadow"):
        return intents, {
            **base,
            "flow_stage": "LIVE/LEARN/DEFEND",
            "input_intents": len(intents),
            "output_intents": len(intents),
            "blocked_intents": 0,
        }
    inverted_shadow = bool(base.get("inverted_shadow"))
    output: list[CopyIntent] = []
    matched_intents: list[dict[str, Any]] = []
    for intent in intents:
        price = float(intent.limit_price)
        matched = next(
            (
                row
                for row in bands
                if float(row["min_price_inclusive"]) <= price < float(row["max_price_exclusive"])
            ),
            None,
        )
        if str(intent.action or "").upper() != "BUY" or matched is None:
            output.append(intent)
            continue
        if inverted_shadow:
            output.append(intent)
        expected_fee = expected_polymarket_buy_fee_usd(shares=float(intent.shares), price=price)
        counterfactual_id = stable_id(
            "epbgcf",
            {
                "intent_id": intent.intent_id,
                "experiment_id": (
                    base.get("shadow_experiment_id") if inverted_shadow else base.get("experiment_id")
                ),
            },
        )
        taxonomy = "entry_price_band_gate_shadow" if inverted_shadow else "entry_price_band_gate"
        effective_experiment_id = (
            base.get("shadow_experiment_id") if inverted_shadow else base.get("experiment_id")
        )
        matched_intents.append(
            {
                "counterfactual_id": counterfactual_id,
                "experiment_id": effective_experiment_id,
                "intent_id": intent.intent_id,
                "source_event_id": intent.source_event_id,
                "source_wallet": intent.source_wallet.lower(),
                "market_slug": intent.market_slug,
                "condition_id": intent.condition_id,
                "outcome": intent.outcome,
                "side": intent.side,
                "limit_price": round(price, 6),
                "shares": round(float(intent.shares), 6),
                "copy_size_usd": round(float(intent.copy_size_usd), 6),
                "expected_fee_usd": round(float(expected_fee), 6),
                "entry_price_band": matched.get("slice"),
                "evidence_n_resolved_windows": matched.get("n_resolved_windows"),
                "evidence_post_fee_pnl_usd": matched.get("post_fee_pnl_usd"),
                "taxonomy": taxonomy,
                "taxonomy_tags": [taxonomy],
                "reject_reason": None if inverted_shadow else "entry_price_band_gate",
                "shadow_mode": base.get("shadow_mode"),
                "live_gate_applied": not inverted_shadow,
                "shadow_counterfactual_retained": True,
            }
        )
    blocked_intents = 0 if inverted_shadow else len(matched_intents)
    return output, {
        **base,
        "flow_stage": "LIVE/LEARN/DEFEND",
        "status": PASS,
        "rule": (
            "retain_previously_blocked_band_intents_live_and_measure_readd_case_in_shadow"
            if inverted_shadow
            else "block_only_fable_ruled_live_measured_post_fee_negative_entry_price_bands"
        ),
        "input_intents": len(intents),
        "output_intents": len(output),
        "blocked_intents": blocked_intents,
        "filtered_intents": blocked_intents,
        "shadow_matched_intents": len(matched_intents) if inverted_shadow else 0,
        "taxonomy_counts": (
            {"entry_price_band_gate": len(matched_intents)}
            if matched_intents and not inverted_shadow
            else {}
        ),
        "counterfactual_shadow": {
            "retained": True,
            "experiment_id": (
                base.get("shadow_experiment_id") if inverted_shadow else base.get("experiment_id")
            ),
            "decision_boundary": ">=100 resolved gated-flow windows or 7 days, whichever comes first",
        },
        "sample_filtered_intents": matched_intents[:20] if not inverted_shadow else [],
        "sample_shadow_intents": matched_intents[:20] if inverted_shadow else [],
        "filtered_intents_detail": matched_intents,
    }


def _append_entry_price_band_gate_events(
    event_log_path: str,
    summary: dict[str, Any],
    *,
    decision_ts: str,
) -> int:
    raw_rows = summary.get("filtered_intents_detail")
    rows = [row for row in raw_rows if isinstance(row, dict)] if isinstance(raw_rows, list) else []
    if not rows:
        return 0
    inverted_shadow = bool(summary.get("inverted_shadow"))
    events = [
        {
            "event": "wallet_copy_live_entry_price_band_gate_counterfactual",
            "event_type": (
                "COUNTERFACTUAL_SHADOW_NOT_APPLIED"
                if inverted_shadow
                else "FABLE_APPROVED_SUPPRESSION_REJECT"
            ),
            "approved_suppression": not inverted_shadow,
            "flow_stage": "LIVE/LEARN/DEFEND",
            "fable_direction": summary.get("direction_id"),
            "experiment_id": summary.get("experiment_id"),
            "ts": decision_ts,
            **row,
            "counterfactual_shadow_row": {
                "status": "PENDING_RESOLUTION",
                "would_submit_without_gate": not inverted_shadow,
                "would_be_suppressed_if_gate_enabled": inverted_shadow,
                "live_path_gate_applied": not inverted_shadow,
                "paper_only": True,
                "live_orders_allowed": False,
                "limit_price": row.get("limit_price"),
                "shares": row.get("shares"),
                "copy_size_usd": row.get("copy_size_usd"),
                "expected_fee_usd": row.get("expected_fee_usd"),
            },
        }
        for row in rows
    ]
    append_jsonl_many(event_log_path, events)
    return len(events)


def _intent_policy_cap_usd(intent: CopyIntent) -> float:
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    policy = metadata.get("wallet_copy_policy") if isinstance(metadata.get("wallet_copy_policy"), dict) else {}
    inventory = metadata.get("inventory_v2") if isinstance(metadata.get("inventory_v2"), dict) else {}
    values = []
    for payload in (policy, inventory, metadata):
        for key in ("effective_live_cap_usd", "max_order_usd", "policy_max_order_usd"):
            try:
                value = float(payload.get(key) or 0.0)
            except (TypeError, ValueError):
                value = 0.0
            if value > 0:
                values.append(value)
    return min(values) if values else float(intent.copy_size_usd)


def _nearest_precision_executable_tick(
    *, size_usd: float, effective_price: float, min_order_usd: float, cap_usd: float, hard_price_cap: float
) -> tuple[float | None, float | None]:
    candidates: list[tuple[float, float, float]] = []
    for cents in range(1, int(hard_price_cap * 100.0 + 1e-9) + 1):
        price = cents / 100.0
        amount = _market_buy_amount_with_valid_share_precision(
            size_usd, price, min_amount_usd=min_order_usd, max_overshoot_usd=0.5
        )
        if min_order_usd - 1e-9 <= amount <= cap_usd + 0.10 + 1e-9:
            candidates.append((abs(price - effective_price), price, amount))
    if not candidates:
        return None, None
    _distance, price, amount = min(candidates, key=lambda item: (item[0], item[1]))
    return price, amount


def _apply_market_buy_precision_preflight(
    intents: list[CopyIntent],
    *,
    min_order_usd: float,
    hard_max_buy_price: float,
    max_chase_ticks: int,
    chase_max_price: float,
    chase_tick_size: float,
    enable_maker_fallback: bool = False,
    passive_at_source_sealed: bool | None = None,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    output: list[CopyIntent] = []
    filtered: list[dict[str, Any]] = []
    passive_source_rows: list[dict[str, Any]] = []
    taker_first_rows: list[dict[str, Any]] = []
    passive_at_source_sealed = (
        _passive_at_source_holdout_sealed()
        if passive_at_source_sealed is None
        else bool(passive_at_source_sealed)
    )
    for intent in intents:
        if str(intent.action or "").upper() != "BUY":
            output.append(intent)
            continue
        copied_price = float(intent.limit_price)
        caps = [value for value in (1.0, chase_max_price, hard_max_buy_price) if value > 0]
        chase_cap = min(caps) if caps else copied_price
        effective_price = copied_price
        if max_chase_ticks > 0 and chase_tick_size > 0 and copied_price <= chase_cap + 1e-9:
            effective_price = min(chase_cap, copied_price + max_chase_ticks * chase_tick_size)
        effective_price = round(effective_price, 6)
        cap_usd = _intent_policy_cap_usd(intent)
        policy_metadata = (
            intent.metadata.get("wallet_copy_policy")
            if isinstance(intent.metadata, dict)
            and isinstance(intent.metadata.get("wallet_copy_policy"), dict)
            else {}
        )
        maker_funding_cap_usd = num(
            policy_metadata.get("maker_min_share_funding_cap_usd"), 0.0
        )
        maker_original_policy_cap_usd = num(
            policy_metadata.get("maker_min_share_original_policy_cap_usd"), 0.0
        )
        precision_amount = _market_buy_amount_with_valid_share_precision(
            float(intent.copy_size_usd),
            effective_price,
            min_amount_usd=min_order_usd,
            max_overshoot_usd=0.5,
        )
        infeasible = precision_amount < min_order_usd - 1e-9 or precision_amount > cap_usd + 0.10 + 1e-9
        maker_funding_required = bool(
            maker_funding_cap_usd > 0
            and maker_original_policy_cap_usd >= maker_funding_cap_usd - 1e-9
            and 0.25 - 1e-9 <= copied_price < 0.50 - 1e-9
            and float(intent.copy_size_usd) <= cap_usd + 1e-9
            and float(intent.shares) < DRIP_CLOB_MIN_SHARES - 1e-9
            and DRIP_CLOB_MIN_SHARES * copied_price
            <= maker_funding_cap_usd + 1e-9
        )
        if maker_funding_required and passive_at_source_sealed:
            metadata = dict(intent.metadata or {})
            capsule = {
                "schema_version": 1,
                "flow_stage": "LIVE/MONEY/DEFEND",
                "status": "precision_taker_eligible_passive_sealed",
                "execution_path": "taker_first_existing_limit",
                "original_limit_price": round(copied_price, 6),
                "copy_size_usd": round(float(intent.copy_size_usd), 6),
                "shares": round(float(intent.shares), 6),
                "passive_at_source_sealed": True,
                "no_chase_caps_preserved": True,
            }
            metadata["precision_taker_first_passive_sealed"] = capsule
            output.append(CopyIntent.from_dict({**intent.asdict(), "metadata": metadata}))
            taker_first_rows.append(
                {
                    "intent_id": intent.intent_id,
                    "source_wallet": intent.source_wallet,
                    "market_slug": intent.market_slug,
                    "outcome": intent.outcome,
                    **capsule,
                }
            )
            continue
        if not infeasible and not maker_funding_required:
            output.append(intent)
            continue
        metadata = dict(intent.metadata or {})
        drift = metadata.get("drift_buffer") if isinstance(metadata.get("drift_buffer"), dict) else {}
        source_price = num(drift.get("source_limit_price"), copied_price)
        clamped_target_usd = min(float(intent.copy_size_usd), cap_usd)
        passive_target_usd = max(min_order_usd, clamped_target_usd)
        passive_shares = (
            math.ceil((passive_target_usd / source_price) * 100.0 - 1e-9) / 100.0
            if source_price > 0 and min_order_usd > 0
            else 0.0
        )
        passive_notional = round(passive_shares * source_price, 6)
        passive_cap_limit = cap_usd + 0.10
        funded_notional = round(DRIP_CLOB_MIN_SHARES * source_price, 6)
        passive_feasible = bool(
            enable_maker_fallback
            and _copy_model_is_inventory_like(metadata.get("copy_model"))
            and source_price > 0
            and source_price <= INVENTORY_MAKER_FALLBACK_PRICE_CEILING + 1e-9
            and (
                maker_funding_required
                or (
                    passive_shares >= 5.0 - 1e-9
                    and min_order_usd - 1e-9
                    <= passive_notional
                    <= passive_cap_limit + 1e-9
                )
            )
        )
        if passive_feasible:
            precision_capsule = {
                "schema_version": 1,
                "flow_stage": "LIVE/ROTATE/DEFEND",
                "status": "precision_requires_passive_source",
                "execution_path": "direct_post_only_gtc_at_source",
                "original_source_price": round(source_price, 6),
                "pre_clamp_copy_size_usd": round(float(intent.copy_size_usd), 6),
                "effective_cap_usd": round(cap_usd, 6),
                "clamped_target_usd": round(clamped_target_usd, 6),
                "precision_cap_limit_usd": round(passive_cap_limit, 6),
                "passive_price": round(source_price, 6),
                "passive_shares": round(passive_shares, 6),
                "passive_notional_usd": passive_notional,
                "maker_min_share_funding_required": maker_funding_required,
                "maker_min_share_funded_shares": (
                    DRIP_CLOB_MIN_SHARES if maker_funding_required else None
                ),
                "maker_min_share_funded_notional_usd": (
                    funded_notional if maker_funding_required else None
                ),
                "maker_min_share_funding_cap_usd": (
                    maker_funding_cap_usd if maker_funding_required else None
                ),
                "cancel_policy": "cancel_at_btc_5m_window_end",
                "no_chase": True,
                "no_ask_cross": True,
            }
            metadata["precision_requires_passive_source"] = precision_capsule
            routed_size_usd = (
                round(passive_target_usd, 6)
                if maker_funding_required
                else passive_notional
            )
            routed_shares = (
                round(passive_target_usd / source_price, 6)
                if maker_funding_required and source_price > 0
                else round(passive_shares, 6)
            )
            payload = {
                **intent.asdict(),
                "limit_price": round(source_price, 6),
                "copy_size_usd": routed_size_usd,
                "shares": routed_shares,
                "order_type": "GTC",
                "metadata": metadata,
            }
            output.append(CopyIntent.from_dict(payload))
            passive_source_rows.append(
                {
                    "intent_id": intent.intent_id,
                    "source_wallet": intent.source_wallet,
                    "market_slug": intent.market_slug,
                    "outcome": intent.outcome,
                    **precision_capsule,
                }
            )
            continue
        nearest_price, nearest_amount = _nearest_precision_executable_tick(
            size_usd=float(intent.copy_size_usd),
            effective_price=effective_price,
            min_order_usd=min_order_usd,
            cap_usd=cap_usd,
            hard_price_cap=hard_max_buy_price or chase_cap,
        )
        filtered.append(
            {
                "intent_id": intent.intent_id,
                "source_wallet": intent.source_wallet,
                "condition_id": intent.condition_id,
                "market_slug": intent.market_slug,
                "outcome": intent.outcome,
                "copy_size_usd": round(float(intent.copy_size_usd), 6),
                "copied_limit_price": round(copied_price, 6),
                "effective_chase_price": effective_price,
                "precision_safe_amount_usd": round(precision_amount, 6),
                "min_order_usd": round(min_order_usd, 6),
                "policy_cap_usd": round(cap_usd, 6),
                "precision_cap_limit_usd": round(cap_usd + 0.10, 6),
                "nearest_executable_tick": nearest_price,
                "nearest_executable_amount_usd": nearest_amount,
                "nearest_tick_price_delta": round(nearest_price - effective_price, 6)
                if nearest_price is not None
                else None,
                "taxonomy": "market_buy_precision_infeasible",
                "taxonomy_tags": ["market_buy_precision_infeasible"],
                "reject_reason": "market_buy_precision_infeasible",
                "shadow_counterfactual_retained": True,
            }
        )
    return output, {
        "enabled": True,
        "flow_stage": "LIVE/LEARN/DEFEND",
        "status": PASS,
        "input_intents": len(intents),
        "output_intents": len(output),
        "blocked_intents": len(filtered),
        "filtered_intents": len(filtered),
        "passive_source_intents": len(passive_source_rows),
        "passive_at_source_sealed": passive_at_source_sealed,
        "taker_eligible_emitted": len(taker_first_rows),
        "taxonomy_counts": {
            **({"market_buy_precision_infeasible": len(filtered)} if filtered else {}),
            **({"precision_requires_passive_source": len(passive_source_rows)} if passive_source_rows else {}),
            **({"precision_taker_eligible_passive_sealed": len(taker_first_rows)} if taker_first_rows else {}),
        },
        "sample_filtered_intents": filtered[:20],
        "filtered_intents_detail": filtered,
        "sample_passive_source_intents": passive_source_rows[:20],
        "passive_source_intents_detail": passive_source_rows,
        "sample_taker_eligible_emitted": taker_first_rows[:20],
        "taker_eligible_emitted_detail": taker_first_rows,
        "rule": (
            "suppress arithmetic-infeasible market BUYs; only maker-enabled, cap-clamped "
            "source-price intents may route as strict post-only GTC"
        ),
    }


def _append_market_buy_precision_preflight_events(
    event_log_path: str, summary: dict[str, Any], *, decision_ts: str
) -> int:
    rows = [row for row in summary.get("filtered_intents_detail") or [] if isinstance(row, dict)]
    if not rows:
        return 0
    events = [
        {
            "event": "wallet_copy_live_market_buy_precision_infeasible_reject",
            "event_type": "FABLE_APPROVED_SUPPRESSION_REJECT",
            "approved_suppression": True,
            "flow_stage": "LIVE/LEARN/DEFEND",
            "fable_direction": "2026-07-21T10:55Z-fable-precision-preflight",
            "ts": decision_ts,
            **row,
        }
        for row in rows
    ]
    append_jsonl_many(event_log_path, events)
    return len(events)


def _apply_expected_fee_capture_gate(intents: list[CopyIntent]) -> tuple[list[CopyIntent], dict[str, Any]]:
    """Stamp expected embedded fee before parity/execution without filtering."""

    output: list[CopyIntent] = []
    rows: list[dict[str, Any]] = []
    fee_sum = 0.0
    for intent in intents:
        metadata = dict(intent.metadata or {})
        is_buy = str(intent.action or "").upper() == "BUY"
        price = float(intent.limit_price)
        shares = float(intent.shares)
        estimated_response_cost = round(max(0.0, shares * price), 6)
        expected_fee = expected_polymarket_buy_fee_usd(shares=shares, price=price)
        modeled_fee = modeled_unvalidated_polymarket_buy_fee_usd(shares=shares, price=price)
        if is_buy:
            fee_sum = round(fee_sum + expected_fee, 6)
            metadata["expected_fee_gate"] = {
                "status": PASS,
                "flow_stage": "LIVE/SELF-DEV",
                "formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
                "fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
                "fee_rate_pct": round(POLYMARKET_EMBEDDED_FEE_RATE * 100.0, 6),
                "source": POLYMARKET_EMBEDDED_FEE_SOURCE,
                "price_basis": "pre_submit_limit_price",
                "limit_price": round(price, 6),
                "shares": round(shares, 6),
                "estimated_response_cost_usd": estimated_response_cost,
                "expected_fee_usd": expected_fee,
                "expected_total_cost_usd": estimated_response_cost,
                "modeled_unvalidated": True,
                "modeled_unvalidated_fee_usd": modeled_fee,
                "accounting_authority": False,
                "threshold_change": False,
                "rule": "banked response cost is immutable; receipt premium is diagnostic only",
            }
            rows.append(
                {
                    "intent_id": intent.intent_id,
                    "source_wallet": intent.source_wallet.lower(),
                    "market_slug": intent.market_slug,
                    "outcome": intent.outcome,
                    "limit_price": round(price, 6),
                    "shares": round(shares, 6),
                    "estimated_response_cost_usd": estimated_response_cost,
                    "expected_fee_usd": expected_fee,
                    "modeled_unvalidated_fee_usd": modeled_fee,
                }
            )
        output.append(CopyIntent.from_dict({**intent.asdict(), "metadata": metadata}))
    return output, {
        "enabled": True,
        "flow_stage": "LIVE/SELF-DEV",
        "status": PASS,
        "input_intents": len(intents),
        "output_intents": len(output),
        "buy_intents_with_fee_estimate": len(rows),
        "expected_fee_sum_usd": round(fee_sum, 6),
        "fee_rate": POLYMARKET_EMBEDDED_FEE_RATE,
        "fee_rate_pct": round(POLYMARKET_EMBEDDED_FEE_RATE * 100.0, 6),
        "formula": POLYMARKET_EMBEDDED_FEE_FORMULA,
        "source": POLYMARKET_EMBEDDED_FEE_SOURCE,
        "modeled_unvalidated": True,
        "accounting_authority": False,
        "threshold_change": False,
        "rule": "zero authoritative fee; measured receipt premium retained as a diagnostic only",
        "sample_intents": rows[:20],
    }


def _floor_live_min_order_intents(
    intents: list[CopyIntent],
    *,
    min_live_order_usd: float,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    if min_live_order_usd <= 0:
        return intents, {"enabled": False, "floored_intents": 0}
    output: list[CopyIntent] = []
    floored = 0
    samples: list[dict[str, Any]] = []
    for intent in intents:
        if (
            str(intent.action or "").upper() == "BUY"
            and _btc_5m_window_start_s(intent.market_slug) is not None
            and float(intent.copy_size_usd) < float(min_live_order_usd)
        ):
            limit_price = float(intent.limit_price)
            shares = round(float(min_live_order_usd) / limit_price, 6) if limit_price > 0 else intent.shares
            metadata = dict(intent.metadata or {})
            metadata["sizing"] = {
                "floored_to_min": True,
                "original_copy_size_usd": round(float(intent.copy_size_usd), 6),
                "floored_copy_size_usd": round(float(min_live_order_usd), 6),
                "reason": "btc5m_gate_passing_intent_below_live_min_order",
            }
            intent = CopyIntent.from_dict(
                {
                    **intent.asdict(),
                    "copy_size_usd": round(float(min_live_order_usd), 6),
                    "shares": shares,
                    "metadata": metadata,
                }
            )
            floored += 1
            samples.append(
                {
                    "intent_id": intent.intent_id,
                    "source_wallet": intent.source_wallet.lower(),
                    "limit_price": round(limit_price, 6),
                    "shares": shares,
                    "original_copy_size_usd": metadata["sizing"]["original_copy_size_usd"],
                    "floored_copy_size_usd": metadata["sizing"]["floored_copy_size_usd"],
                }
            )
        output.append(intent)
    return output, {
        "enabled": True,
        "min_live_order_usd": round(float(min_live_order_usd), 6),
        "input_intents": len(intents),
        "output_intents": len(output),
        "floored_intents": floored,
        "sample_floored_intents": samples[:5],
    }


def _filter_removed_all_intents(summary: dict[str, Any], intents: list[CopyIntent]) -> bool:
    return bool(summary.get("blocked_intents")) and not intents


def _intent_runtime_diagnostics(
    intent: CopyIntent,
    *,
    now_ts: float,
    max_event_age_s: float,
    live_build_max_observed_age_s: float = LIVE_BUILD_MAX_OBSERVED_AGE_S,
) -> dict[str, Any]:
    event_age = _intent_age_s(intent, now_ts=now_ts)
    observed_age = _intent_observed_age_s(intent, now_ts=now_ts)
    observation_event_age = _intent_observation_event_age_s(intent)
    metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
    inventory = metadata.get("inventory_v2") if isinstance(metadata.get("inventory_v2"), dict) else {}
    window_start = _btc_5m_window_start_s(intent.market_slug)
    btc_5m_scope_ok = window_start is not None
    window_close = None if window_start is None else window_start + 300.0
    seconds_after_close = None if window_close is None else now_ts - window_close
    observed_after_close = bool(
        window_close is not None
        and intent.observed_ts is not None
        and float(intent.observed_ts) > window_close
    )
    market_closed_now = bool(seconds_after_close is not None and seconds_after_close >= 0)
    fresh_by_event_age = bool(
        max_event_age_s <= 0
        or (event_age is not None and event_age <= float(max_event_age_s))
    )
    fresh_by_observation_event_age = bool(
        max_event_age_s <= 0
        or (
            observation_event_age is not None
            and observation_event_age <= float(max_event_age_s)
            and observed_age is not None
            and observed_age <= float(max_event_age_s)
        )
    )
    fresh_for_live_build = bool(
        live_build_max_observed_age_s <= 0
        or (observed_age is not None and observed_age <= float(live_build_max_observed_age_s))
    )
    try:
        watermark_ts = float(inventory.get("freshness_watermark_ts") or 0.0)
    except (TypeError, ValueError):
        watermark_ts = 0.0
    watermark_age_s = max(0.0, now_ts - watermark_ts) if watermark_ts > 0 else None
    fresh_by_watermark_age = bool(
        live_build_max_observed_age_s <= 0
        or (watermark_age_s is not None and watermark_age_s <= float(live_build_max_observed_age_s))
    )
    fresh_by_rtds_watermark = bool(
        inventory.get("freshness_confirmed_unchanged") is True
        and inventory.get("freshness_watermark_ts") is not None
        and fresh_by_watermark_age
    )
    effective_fresh_for_live_build = bool(fresh_for_live_build or fresh_by_rtds_watermark)
    live_tradeable_window_open = bool(
        btc_5m_scope_ok
        and
        (fresh_by_event_age or fresh_by_observation_event_age or fresh_by_rtds_watermark)
        and effective_fresh_for_live_build
        and not market_closed_now
        and not observed_after_close
    )
    if fresh_by_event_age:
        freshness_basis = "event_ts"
    elif fresh_by_observation_event_age:
        freshness_basis = "observed_fresh_source_event"
    elif fresh_by_rtds_watermark:
        freshness_basis = "rtds_watermark_confirmed_unchanged"
    else:
        freshness_basis = "stale"
    return {
        "intent_id": intent.intent_id,
        "source_event_id": intent.source_event_id,
        "market_slug": intent.market_slug,
        "side": intent.side,
        "limit_price": float(intent.limit_price),
        "copy_size_usd": float(intent.copy_size_usd),
        "event_age_s": None if event_age is None else round(event_age, 6),
        "observed_age_s": None if observed_age is None else round(observed_age, 6),
        "watermark_age_s": None if watermark_age_s is None else round(watermark_age_s, 6),
        "observation_event_age_s": None if observation_event_age is None else round(observation_event_age, 6),
        "window_start_s": window_start,
        "btc_5m_scope_ok": btc_5m_scope_ok,
        "window_close_s": window_close,
        "seconds_after_close_s": None if seconds_after_close is None else round(seconds_after_close, 6),
        "market_closed_now": market_closed_now,
        "observed_after_market_close": observed_after_close,
        "fresh_by_event_age": fresh_by_event_age,
        "fresh_by_observation_event_age": fresh_by_observation_event_age,
        "fresh_by_rtds_watermark": fresh_by_rtds_watermark,
        "fresh_for_live_build": effective_fresh_for_live_build,
        "fresh_for_live_build_by_observed_ts": fresh_for_live_build,
        "fresh_for_live_build_by_watermark": fresh_by_watermark_age,
        "live_build_max_observed_age_s": round(float(live_build_max_observed_age_s), 6),
        "freshness_basis": freshness_basis,
        "live_tradeable_window_open": live_tradeable_window_open,
    }


def _candidate_runtime_freshness_summary(
    intents: list[CopyIntent],
    *,
    now_ts: float,
    max_event_age_s: float,
    live_build_max_observed_age_s: float = LIVE_BUILD_MAX_OBSERVED_AGE_S,
) -> dict[str, Any]:
    rows = sorted(
        intents,
        key=lambda intent: (intent.observed_ts or intent.event_ts or 0.0, intent.intent_id),
        reverse=True,
    )
    latest = (
        _intent_runtime_diagnostics(
            rows[0],
            now_ts=now_ts,
            max_event_age_s=max_event_age_s,
            live_build_max_observed_age_s=live_build_max_observed_age_s,
        )
        if rows
        else {}
    )
    diagnostics = [
        _intent_runtime_diagnostics(
            intent,
            now_ts=now_ts,
            max_event_age_s=max_event_age_s,
            live_build_max_observed_age_s=live_build_max_observed_age_s,
        )
        for intent in rows
    ]
    return {
        "latest_candidate_intent_runtime": latest,
        "candidate_intents_event_age_gt_max": sum(
            1
            for row in diagnostics
            if max_event_age_s > 0
            and row.get("event_age_s") is not None
            and float(row["event_age_s"]) > float(max_event_age_s)
        ),
        "candidate_intents_event_age_missing": sum(1 for row in diagnostics if row.get("event_age_s") is None),
        "candidate_intents_market_closed_now": sum(1 for row in diagnostics if row.get("market_closed_now")),
        "candidate_intents_observed_after_market_close": sum(
            1 for row in diagnostics if row.get("observed_after_market_close")
        ),
        "candidate_intents_observed_age_gt_live_build_max": sum(
            1 for row in diagnostics if not row.get("fresh_for_live_build")
        ),
        "candidate_intents_live_tradeable_window_open": sum(
            1 for row in diagnostics if row.get("live_tradeable_window_open")
        ),
        "sample_candidate_runtime_intents": diagnostics[:5],
    }


def _fresh_intents(
    intents: list[CopyIntent],
    *,
    max_event_age_s: float,
    max_intents: int,
    min_copy_size_usd: float = 0.0,
    live_build_max_observed_age_s: float = LIVE_BUILD_MAX_OBSERVED_AGE_S,
) -> list[CopyIntent]:
    now = time.time()
    def _sort_key(intent: CopyIntent) -> tuple[float, float, str]:
        metadata = intent.metadata if isinstance(intent.metadata, dict) else {}
        if _copy_model_is_inventory_like(metadata.get("copy_model")):
            return (float(intent.copy_size_usd), intent.observed_ts or intent.event_ts or 0.0, intent.intent_id)
        return (intent.observed_ts or intent.event_ts or 0.0, 0.0, intent.intent_id)

    rows = sorted(intents, key=_sort_key, reverse=True)
    if max_event_age_s > 0:
        rows = [
            intent
            for intent in rows
            if _intent_runtime_diagnostics(
                intent,
                now_ts=now,
                max_event_age_s=max_event_age_s,
                live_build_max_observed_age_s=live_build_max_observed_age_s,
            ).get("live_tradeable_window_open")
        ]
    else:
        rows = [
            intent
            for intent in rows
            if _intent_runtime_diagnostics(
                intent,
                now_ts=now,
                max_event_age_s=max_event_age_s,
                live_build_max_observed_age_s=live_build_max_observed_age_s,
            ).get("live_tradeable_window_open")
        ]
    if min_copy_size_usd > 0:
        rows = [intent for intent in rows if float(intent.copy_size_usd) >= float(min_copy_size_usd)]
    if max_intents > 0:
        rows = rows[: int(max_intents)]
    return rows


def _indexed_source_wallet_windows(
    history_state: str,
    *,
    history_window_index_path: str,
    source_wallet: str,
    min_window_start: int,
    max_window_start: int,
) -> set[int]:
    if not history_window_index_path or not source_wallet:
        return set()
    index = load_history_window_index(history_state, index_path=history_window_index_path)
    windows = index.get("windows") if isinstance(index, dict) else {}
    if not isinstance(windows, dict):
        return set()
    wallet_key = source_wallet.lower()
    selected: set[int] = set()
    for raw_window_start, wallet_spans in windows.items():
        try:
            window_start = int(float(raw_window_start))
        except (TypeError, ValueError):
            continue
        if window_start < min_window_start or window_start > max_window_start:
            continue
        if isinstance(wallet_spans, dict) and wallet_key in wallet_spans:
            selected.add(window_start)
    return selected


def _unknown_clob_token_placeholder(side: str) -> str:
    return f"{UNKNOWN_CLOB_TOKEN_PREFIX}{str(side).lower()}_token__"


def _known_token_id(value: Any) -> str:
    token = str(value or "")
    if not token or token.startswith(UNKNOWN_CLOB_TOKEN_PREFIX):
        return ""
    return token


def _token_pair_from_side_map(
    side_map: dict[str, str],
    *,
    allow_partial: bool,
) -> list[str]:
    yes = _known_token_id(side_map.get("YES"))
    no = _known_token_id(side_map.get("NO"))
    if yes and no:
        return [yes, no]
    if allow_partial and (yes or no):
        return [yes or _unknown_clob_token_placeholder("YES"), no or _unknown_clob_token_placeholder("NO")]
    return []


def _token_pair_for_intent(intent: CopyIntent) -> list[str]:
    token_id = _known_token_id(intent.token_id)
    if not token_id:
        return []
    outcome = str(intent.outcome or "").strip().lower()
    if outcome in {"up", "yes"}:
        return [token_id, _unknown_clob_token_placeholder("NO")]
    if outcome in {"down", "no"}:
        return [_unknown_clob_token_placeholder("YES"), token_id]
    return []


def _token_pair_matches_intent(token_ids: list[str], intent: CopyIntent) -> bool:
    if len(token_ids) < 2 or not _known_token_id(intent.token_id):
        return False
    outcome = str(intent.outcome or "").strip().lower()
    if outcome in {"up", "yes"}:
        return str(token_ids[0]) == str(intent.token_id)
    if outcome in {"down", "no"}:
        return str(token_ids[1]) == str(intent.token_id)
    return False


def _merge_token_pairs(existing: list[str], incoming: list[str]) -> list[str]:
    existing = list(existing or [])
    incoming = list(incoming or [])
    if len(existing) < 2:
        existing = [
            existing[0] if len(existing) > 0 else _unknown_clob_token_placeholder("YES"),
            existing[1] if len(existing) > 1 else _unknown_clob_token_placeholder("NO"),
        ]
    if len(incoming) < 2:
        incoming = [
            incoming[0] if len(incoming) > 0 else _unknown_clob_token_placeholder("YES"),
            incoming[1] if len(incoming) > 1 else _unknown_clob_token_placeholder("NO"),
        ]
    yes = _known_token_id(existing[0]) or _known_token_id(incoming[0]) or str(existing[0] or incoming[0])
    no = _known_token_id(existing[1]) or _known_token_id(incoming[1]) or str(existing[1] or incoming[1])
    return [yes, no]


def _token_pair_preferring_intent(token_ids: list[str], intent: CopyIntent) -> list[str]:
    pair = _merge_token_pairs(token_ids, _token_pair_for_intent(intent))
    intent_token = _known_token_id(intent.token_id)
    if not intent_token:
        return pair
    outcome = str(intent.outcome or "").strip().lower()
    if outcome in {"up", "yes"}:
        pair[0] = intent_token
    elif outcome in {"down", "no"}:
        pair[1] = intent_token
    return pair


def _merge_token_maps(*maps: dict[str, list[str]]) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for token_map in maps:
        for key, token_ids in token_map.items():
            if not key or len(token_ids or []) < 2:
                continue
            merged[key] = _merge_token_pairs(merged.get(key, []), token_ids)
    return merged


def _history_token_maps(events: list[Any], *, allow_partial: bool = False) -> dict[str, list[str]]:
    token_maps: dict[str, dict[str, str]] = {}
    for event in events:
        token_id = str(getattr(event, "token_id", "") or "")
        if not token_id:
            continue
        outcome = str(getattr(event, "outcome", "") or "").strip().lower()
        if outcome in {"up", "yes"}:
            side = "YES"
        elif outcome in {"down", "no"}:
            side = "NO"
        else:
            continue
        for key in (str(getattr(event, "condition_id", "") or ""), str(getattr(event, "market_slug", "") or "")):
            if not key:
                continue
            token_maps.setdefault(key, {})[side] = token_id
    return {
        key: token_pair
        for key, row in token_maps.items()
        if (token_pair := _token_pair_from_side_map(row, allow_partial=allow_partial))
    }


def _intent_token_maps(intents: list[CopyIntent], *, allow_partial: bool = True) -> dict[str, list[str]]:
    token_maps: dict[str, dict[str, str]] = {}
    for intent in intents:
        token_id = _known_token_id(intent.token_id)
        if not token_id:
            continue
        outcome = str(intent.outcome or "").strip().lower()
        if outcome in {"up", "yes"}:
            side = "YES"
        elif outcome in {"down", "no"}:
            side = "NO"
        else:
            continue
        for key in (str(intent.condition_id or ""), str(intent.market_slug or "")):
            if key:
                token_maps.setdefault(key, {})[side] = token_id
    return {
        key: token_pair
        for key, row in token_maps.items()
        if (token_pair := _token_pair_from_side_map(row, allow_partial=allow_partial))
    }


def _build_candidate_intents(
    *,
    candidate: dict[str, Any],
    history_state: str,
    args: argparse.Namespace,
) -> tuple[list[CopyIntent], dict[str, Any], list[str], dict[str, list[str]]]:
    blockers: list[str] = []
    policy = _candidate_policy(candidate)
    if policy is None:
        return [], {"status": CORRECTION, "candidate_intents": 0}, ["candidate_policy_missing"], {}

    candidate_type = str(candidate.get("candidate_type") or "")
    source_wallet = _candidate_source_wallet(candidate)
    copy_model = str(getattr(args, "copy_model", "per_order") or "per_order")
    min_live_floor_pin_enabled = bool(
        copy_model == DRIP_COPY_MODEL
        and bool(getattr(args, "execute_live", False))
        and bool(getattr(args, "explicit_live_operator_go", False))
        and bool(getattr(args, "live_orders_allowed", False))
        and _min_live_floor_pin_enabled_for_candidate(candidate, policy)
    )
    now_ts = time.time()
    history_window_starts: set[int] = set()
    history_scope = "full"
    if _copy_model_is_inventory_like(copy_model) and candidate_type != "MULTI_WALLET_INVENTORY" and source_wallet:
        current_window_start = int(now_ts // 300.0) * 300
        history_window_starts = {current_window_start, current_window_start - 300}
        future_lookahead_s = max(
            0.0,
            float(getattr(args, "inventory_future_window_lookahead_s", INVENTORY_FUTURE_WINDOW_LOOKAHEAD_S)),
        )
        history_window_starts.update(
            _indexed_source_wallet_windows(
                history_state,
                history_window_index_path=str(getattr(args, "history_window_index", "") or ""),
                source_wallet=source_wallet,
                min_window_start=current_window_start - 300,
                max_window_start=current_window_start + int(future_lookahead_s),
            )
        )
        history_scope = "candidate_current_previous_and_indexed_future_btc5m_windows"
    events = load_events_from_history(
        history_state,
        source_wallet=source_wallet if history_window_starts else "",
        window_starts=history_window_starts,
        history_window_index_path=str(getattr(args, "history_window_index", "") or "") or None,
    )
    history_index_stats = getattr(load_events_from_history, "last_index_stats", {"enabled": False})
    history_token_map = _history_token_maps(events, allow_partial=True)
    source_events = events
    if candidate_type != "MULTI_WALLET_INVENTORY":
        if not source_wallet:
            blockers.append("candidate_source_wallet_missing")
        else:
            source_events = [event for event in events if event.source_wallet.lower() == source_wallet]

    live_build_max_observed_age_s = float(
        getattr(args, "live_build_max_observed_age_s", LIVE_BUILD_MAX_OBSERVED_AGE_S)
    )
    if _copy_model_is_inventory_like(copy_model) and candidate_type != "MULTI_WALLET_INVENTORY":
        signal_watermark_state = _signal_watermark_state(args)
        observation_watermarks = _load_observation_watermarks(
            signal_watermark_state,
            source_wallet=source_wallet,
        )
        converge_orders_per_window = int(getattr(args, "inventory_max_converge_orders_per_window", 6))
        if copy_model == DRIP_COPY_MODEL:
            converge_orders_per_window = max(
                converge_orders_per_window,
                int(getattr(args, "drip_max_tranches_per_window", DRIP_MAX_TRANCHES_PER_WINDOW)),
            )
        inventory_feedstock, feedstock_prefilter = _drop_closed_btc5m_feedstock(
            source_events,
            now_ts=now_ts,
        )
        candidate_intents, live_event_prefilter = _build_inventory_v2_intents(
            inventory_feedstock,
            policy,
            now_ts=now_ts,
            max_event_age_s=float(args.max_event_age_s),
            live_build_max_observed_age_s=live_build_max_observed_age_s,
            live_ledger_state=str(args.live_ledger_state),
            late_window_stop_s=_candidate_late_window_stop_s(candidate, args),
            max_converge_orders_per_window=converge_orders_per_window,
            copy_model=copy_model,
            drip_min_tranche_usd=float(getattr(args, "drip_min_tranche_usd", DRIP_MIN_TRANCHE_USD)),
            drip_max_tranche_usd=float(getattr(args, "drip_max_tranche_usd", DRIP_MAX_TRANCHE_USD)),
            strong_tier_suspended=_mission_strong_tier_suspended(),
            observation_watermarks=observation_watermarks,
            min_live_floor_pin_enabled=min_live_floor_pin_enabled,
        )
        live_event_prefilter["feedstock_prefilter"] = feedstock_prefilter
        live_event_prefilter["signal_watermark_state"] = signal_watermark_state
        live_event_prefilter["min_live_floor_pin_enabled"] = min_live_floor_pin_enabled
        live_event_prefilter["min_live_floor_pin_direction_id"] = (
            MIN_LIVE_FLOOR_PIN_DIRECTION_ID if min_live_floor_pin_enabled else None
        )
        candidate_build_events = []
        base_intents = candidate_intents
    else:
        candidate_build_events, live_event_prefilter = _live_candidate_build_events(
            source_events,
            policy,
            now_ts=now_ts,
            max_event_age_s=float(args.max_event_age_s),
            live_build_max_observed_age_s=live_build_max_observed_age_s,
        )

        base_intents = intents_for_policy(candidate_build_events, policy)
        candidate_intents = base_intents
    inventory_plan_count = 0
    inventory_pass_plan_count = 0
    if candidate_type == "MULTI_WALLET_INVENTORY":
        plans = build_inventory_plans(
            base_intents,
            config=InventoryConfig(
                min_agreeing_wallets=int(args.min_agreeing_wallets),
                max_price_spread=float(args.max_price_spread),
                max_window_usd=float(args.max_window_usd),
                max_per_wallet_usd=float(args.max_per_wallet_usd),
                min_plan_usd=float(args.min_inventory_plan_usd),
            ),
        )
        inventory_plan_count = len(plans)
        inventory_pass_plan_count = sum(1 for plan in plans if plan.status == PASS)
        candidate_intents = [intent for intent in (inventory_plan_to_intent(plan) for plan in plans) if intent is not None]

    if policy.maker_min_share_funding_cap_usd > 0:
        funded_intents: list[CopyIntent] = []
        for intent in candidate_intents:
            metadata = dict(intent.metadata or {})
            wallet_copy_policy = (
                dict(metadata.get("wallet_copy_policy"))
                if isinstance(metadata.get("wallet_copy_policy"), dict)
                else {}
            )
            wallet_copy_policy.update(
                {
                    "maker_min_share_funding_cap_usd": round(
                        policy.maker_min_share_funding_cap_usd, 6
                    ),
                    "maker_min_share_original_policy_cap_usd": round(
                        policy.maker_min_share_original_policy_cap_usd, 6
                    ),
                    "maker_min_share_base_request_cap_usd": round(
                        policy.maker_min_share_base_request_cap_usd, 6
                    ),
                }
            )
            metadata["wallet_copy_policy"] = wallet_copy_policy
            funded_intents.append(
                CopyIntent.from_dict({**intent.asdict(), "metadata": metadata})
            )
        candidate_intents = funded_intents

    runtime_freshness_summary = _candidate_runtime_freshness_summary(
        candidate_intents,
        now_ts=now_ts,
        max_event_age_s=float(args.max_event_age_s),
        live_build_max_observed_age_s=live_build_max_observed_age_s,
    )
    candidate_build_events_filtered_reasons = dict(
        sorted(
            {
                **{
                    str(reason): int(count)
                    for reason, count in (live_event_prefilter.get("skip_counts") or {}).items()
                },
                **{
                    f"policy_{reason}": int(count)
                    for reason, count in (live_event_prefilter.get("policy_reject_counts") or {}).items()
                },
            }.items()
        )
    )
    fresh_before_live_min = _fresh_intents(
        candidate_intents,
        max_event_age_s=float(args.max_event_age_s),
        max_intents=0,
        live_build_max_observed_age_s=live_build_max_observed_age_s,
    )
    min_live_order_usd = float(args.min_live_order_usd) if bool(args.execute_live) else 0.0
    fresh = _fresh_intents(
        candidate_intents,
        max_event_age_s=float(args.max_event_age_s),
        max_intents=int(args.max_intents),
        min_copy_size_usd=0.0,
        live_build_max_observed_age_s=live_build_max_observed_age_s,
    )
    below_live_min = [
        intent
        for intent in fresh_before_live_min
        if min_live_order_usd > 0 and float(intent.copy_size_usd) < min_live_order_usd
    ]
    if not events:
        blockers.append("history_events_missing")
    if not source_events:
        blockers.append("candidate_source_events_missing")
    if candidate_build_events and not base_intents:
        blockers.append("candidate_base_intents_missing")
    if candidate_build_events and not candidate_intents:
        blockers.append("candidate_copy_intents_missing")
    if not fresh:
        blockers.append("fresh_candidate_copy_intents_missing")
    token_map = _merge_token_maps(history_token_map, _intent_token_maps(fresh, allow_partial=True))
    return fresh, {
        "status": PASS if not blockers else CORRECTION,
        "candidate_type": candidate_type,
        "candidate_id": candidate.get("candidate_id"),
        "policy": policy.asdict(),
        "source_wallet": source_wallet,
        "copy_model": copy_model,
        "history_events": len(events),
        "history_scope": history_scope,
        "history_window_starts": sorted(history_window_starts),
        "history_index": history_index_stats,
        "source_events": len(source_events),
        "intent_build_feedstock_events": int(
            (live_event_prefilter.get("feedstock_prefilter") or {}).get(
                "retained_events", len(source_events)
            )
        ),
        "candidate_build_events": len(candidate_build_events),
        "candidate_build_events_filtered": len(source_events) - len(candidate_build_events),
        "candidate_build_events_filtered_reasons": candidate_build_events_filtered_reasons,
        "candidate_build_events_filtered_samples": list(live_event_prefilter.get("sample_filtered_events") or [])[:20],
        "live_event_prefilter": live_event_prefilter,
        "base_intents": len(base_intents),
        "candidate_intents": len(candidate_intents),
        "fresh_candidate_intents_before_live_min": len(fresh_before_live_min),
        "fresh_candidate_intents_below_live_min_order": len(below_live_min),
        "fresh_candidate_intents": len(fresh),
        "max_event_age_s": float(args.max_event_age_s),
        "live_build_max_observed_age_s": live_build_max_observed_age_s,
        "min_live_order_usd": min_live_order_usd,
        "max_intents": int(args.max_intents),
        "drip_max_tranches_per_window": int(getattr(args, "drip_max_tranches_per_window", DRIP_MAX_TRANCHES_PER_WINDOW)),
        "drip_min_tranche_usd": float(getattr(args, "drip_min_tranche_usd", DRIP_MIN_TRANCHE_USD)),
        "drip_max_tranche_usd": float(getattr(args, "drip_max_tranche_usd", DRIP_MAX_TRANCHE_USD)),
        "inventory_plans": inventory_plan_count,
        "inventory_pass_plans": inventory_pass_plan_count,
        "history_token_mapped_conditions": len(history_token_map),
        "fresh_intent_token_mapped_conditions": len(token_map),
        "sample_intents": [intent.asdict() for intent in fresh[:5]],
        **runtime_freshness_summary,
    }, blockers, token_map


def _parse_token_ids(value: Any) -> list[str]:
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            value = []
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if str(item or "")]


def _clob_token_ids_for_intent(
    intent: CopyIntent,
    *,
    gamma: GammaMarketClient,
    cache: dict[str, list[str]],
    fallback_token_map: dict[str, list[str]],
    profile_counts: Counter[str] | None = None,
) -> list[str]:
    key = intent.condition_id or intent.market_slug
    if key in cache:
        if profile_counts is not None:
            profile_counts["token_cache_hits"] += 1
        cached = cache[key]
        if _token_pair_matches_intent(cached, intent):
            return cached
        cache[key] = _token_pair_preferring_intent(cached, intent)
        return cache[key]
    fallback = fallback_token_map.get(intent.condition_id) or fallback_token_map.get(intent.market_slug) or []
    if len(fallback) >= 2:
        if profile_counts is not None:
            profile_counts["fallback_full_token_pair_hits"] += 1
        token_ids = list(fallback)
        if not _token_pair_matches_intent(token_ids, intent):
            token_ids = _token_pair_preferring_intent(token_ids, intent)
        cache[key] = token_ids
        return token_ids
    token_ids: list[str] = []
    if intent.market_slug:
        if profile_counts is not None:
            profile_counts["gamma_market_lookups"] += 1
        market = gamma.market_by_slug(intent.market_slug)
        token_ids = _parse_token_ids(market.get("clobTokenIds") or market.get("clob_token_ids") or [])
    if len(token_ids) < 2:
        if fallback and profile_counts is not None:
            profile_counts["fallback_partial_token_pair_hits"] += 1
        token_ids = fallback
    if len(token_ids) >= 2 and not _token_pair_matches_intent(token_ids, intent):
        token_ids = _token_pair_preferring_intent(token_ids, intent)
    if len(token_ids) < 2:
        if profile_counts is not None:
            profile_counts["synthetic_token_pairs"] += 1
        token_ids = _token_pair_for_intent(intent)
    cache[key] = token_ids
    return token_ids


def _parity_capsules(
    intents: list[CopyIntent],
    *,
    operator_approval_id: str,
    gamma_timeout_s: float,
    fallback_token_map: dict[str, list[str]],
    profile_out: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, list[str]], list[str]]:
    started = time.perf_counter()
    gamma = GammaMarketClient(timeout_s=gamma_timeout_s)
    token_cache: dict[str, list[str]] = {}
    capsules: list[dict[str, Any]] = []
    blockers: list[str] = []
    profile_counts: Counter[str] = Counter()
    approval = operator_approval_id or "DRY_RUN_PLACEHOLDER_NOT_OPERATOR_APPROVAL"
    for intent in intents:
        token_started = time.perf_counter()
        token_ids = _clob_token_ids_for_intent(
            intent,
            gamma=gamma,
            cache=token_cache,
            fallback_token_map=fallback_token_map,
            profile_counts=profile_counts,
        )
        profile_counts["token_mapping_duration_us"] += int((time.perf_counter() - token_started) * 1_000_000)
        if len(token_ids) < 2:
            blockers.append(f"clob_token_mapping_missing:{intent.condition_id or intent.market_slug}")
        elif intent.token_id and intent.token_id not in token_ids:
            blockers.append(f"intent_token_not_in_clob_token_mapping:{intent.intent_id}")
        elif not intent.token_id:
            blockers.append(f"intent_token_id_missing:{intent.intent_id}")
        capsule = build_copy_intent_parity_capsule(
            intent,
            clob_token_ids=token_ids,
            operator_approval_id=approval,
        )
        if capsule.get("status") != PASS:
            blockers.extend(str(blocker) for blocker in capsule.get("blockers") or [])
        capsules.append(capsule)
    if profile_out is not None:
        token_mapping_s = float(profile_counts.get("token_mapping_duration_us", 0)) / 1_000_000.0
        profile_out.update(
            {
                "flow_stage": "SELF-DEV",
                "total_s": round(time.perf_counter() - started, 6),
                "substage_timers": [
                    {"name": "token_mapping", "duration_s": round(token_mapping_s, 6)},
                    {
                        "name": "capsule_build_and_validation",
                        "duration_s": round(max(0.0, time.perf_counter() - started - token_mapping_s), 6),
                    },
                ],
                "intents": len(intents),
                "token_cache_entries": len(token_cache),
                "token_cache_hits": int(profile_counts.get("token_cache_hits", 0)),
                "fallback_full_token_pair_hits": int(profile_counts.get("fallback_full_token_pair_hits", 0)),
                "fallback_partial_token_pair_hits": int(profile_counts.get("fallback_partial_token_pair_hits", 0)),
                "gamma_market_lookups": int(profile_counts.get("gamma_market_lookups", 0)),
                "synthetic_token_pairs": int(profile_counts.get("synthetic_token_pairs", 0)),
            }
        )
    return capsules, token_cache, sorted(set(blockers))


def _proof_blockers(
    profit_state: dict[str, Any],
    snapshot: LiveAdmissionSnapshot,
    *,
    candidate: dict[str, Any] | None = None,
) -> list[str]:
    decision = _as_dict(profit_state.get("decision"))
    certificate = _as_dict(profit_state.get("live_readiness_certificate"))
    candidate = candidate if isinstance(candidate, dict) else _selected_candidate(profit_state)
    blockers = [str(item) for item in decision.get("live_admission_blockers") or []]
    blockers.extend(str(item) for item in certificate.get("blockers") or [])
    blockers.extend(_hard_paper_blockers_for_candidate(profit_state, candidate))
    if snapshot.decision_status != PASS:
        blockers.append("profit_engine_decision_not_pass")
    if snapshot.live_admission_status != PASS:
        blockers.append("live_admission_status_not_pass")
    if certificate and not bool(certificate.get("live_ready")):
        blockers.append("live_readiness_certificate_not_ready")
    return sorted(set(blockers))


def _drop_already_live_submitted_intents(
    intents: list[CopyIntent],
    *,
    live_ledger_state: str,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    ledger = load_json(live_ledger_state, default={}, cache_readonly=True)
    orders = ledger.get("orders") if isinstance(ledger, dict) else []
    submitted_intent_ids = {
        str(row.get("intent_id") or "")
        for row in (orders if isinstance(orders, list) else [])
        if isinstance(row, dict) and str(row.get("intent_id") or "")
    }
    fresh = [intent for intent in intents if intent.intent_id not in submitted_intent_ids]
    skipped = [intent.intent_id for intent in intents if intent.intent_id in submitted_intent_ids]
    return fresh, {
        "live_ledger_state": live_ledger_state,
        "submitted_intent_ids_seen": len(submitted_intent_ids),
        "input_intents": len(intents),
        "new_intents": len(fresh),
        "already_submitted_intents": len(skipped),
        "skipped_intent_ids": skipped[:20],
    }


def _live_window_occupancy(live_ledger_state: str) -> dict[str, dict[str, int]]:
    ledger = load_json(live_ledger_state, default={}, cache_readonly=True)
    orders = ledger.get("orders") if isinstance(ledger, dict) else []
    occupancy: dict[str, dict[str, int]] = {}
    for row in orders if isinstance(orders, list) else []:
        if not isinstance(row, dict):
            continue
        source_intent = _as_dict(row.get("source_intent"))
        market_slug = str(row.get("market_slug") or source_intent.get("market_slug") or "")
        if _btc_5m_window_start_s(market_slug) is None:
            continue
        final_status = str(row.get("final_status") or row.get("status") or "").upper()
        if final_status not in {"FILLED", "SUBMITTED"}:
            continue
        bucket = occupancy.setdefault(market_slug, {"filled": 0, "submitted": 0})
        if final_status == "FILLED":
            bucket["filled"] += 1
        elif final_status == "SUBMITTED":
            bucket["submitted"] += 1
    return occupancy


def _apply_window_fill_cap_gate(
    intents: list[CopyIntent],
    *,
    live_ledger_state: str,
    per_window_fill_cap: int,
) -> tuple[list[CopyIntent], dict[str, Any]]:
    cap = max(0, int(per_window_fill_cap))
    if cap <= 0:
        return intents, {
            "enabled": False,
            "flow_stage": "LIVE/DEFEND",
            "reason": "per_window_fill_cap_not_configured",
            "input_intents": len(intents),
            "output_intents": len(intents),
            "blocked_intents": 0,
        }
    occupancy = _live_window_occupancy(live_ledger_state)
    reserved_by_market = {
        market_slug: int(counts.get("filled") or 0) + int(counts.get("submitted") or 0)
        for market_slug, counts in occupancy.items()
    }
    output: list[CopyIntent] = []
    filtered: list[dict[str, Any]] = []
    for intent in intents:
        market_slug = str(intent.market_slug or "")
        is_buy = str(intent.action or "").upper() == "BUY"
        if is_buy and _btc_5m_window_start_s(market_slug) is not None:
            counts = occupancy.get(market_slug) or {}
            occupied_before = int(reserved_by_market.get(market_slug) or 0)
            if occupied_before >= cap:
                filtered.append(
                    {
                        "intent_id": intent.intent_id,
                        "source_wallet": intent.source_wallet.lower(),
                        "market_slug": market_slug,
                        "outcome": intent.outcome,
                        "limit_price": round(float(intent.limit_price), 6),
                        "copy_size_usd": round(float(intent.copy_size_usd), 6),
                        "per_window_fill_cap": cap,
                        "window_fills_before": int(counts.get("filled") or 0),
                        "window_submitted_before": int(counts.get("submitted") or 0),
                        "window_occupied_before": occupied_before,
                        "taxonomy": "window_fill_cap",
                        "reject_reason": "window_fill_cap",
                        "fable_direction": WINDOW_FILL_CAP_DIRECTION_ID,
                    }
                )
                continue
            reserved_by_market[market_slug] = occupied_before + 1
        output.append(intent)
    return output, {
        "enabled": True,
        "flow_stage": "LIVE/DEFEND",
        "status": PASS,
        "rule": "skip_later_live_buy_copyintents_once_a_btc5m_market_has_reached_the_fable_per_window_fill_cap",
        "fable_direction": WINDOW_FILL_CAP_DIRECTION_ID,
        "input_intents": len(intents),
        "output_intents": len(output),
        "filtered_intents": len(filtered),
        "blocked_intents": len(filtered),
        "per_window_fill_cap": cap,
        "occupied_market_windows": len(occupancy),
        "taxonomy_counts": {"window_fill_cap": len(filtered)} if filtered else {},
        "sample_filtered_intents": filtered[:20],
        "filtered_intents_detail": filtered,
    }


def _append_window_fill_cap_events(
    event_log_path: str,
    summary: dict[str, Any],
    *,
    decision_ts: str,
) -> int:
    raw_rows = summary.get("filtered_intents_detail")
    rows = [row for row in raw_rows if isinstance(row, dict)] if isinstance(raw_rows, list) else []
    if not rows:
        return 0
    events: list[dict[str, Any]] = []
    for row in rows:
        events.append(
            {
                "event": "wallet_copy_live_window_fill_cap_skip",
                "event_type": "FABLE_APPROVED_SUPPRESSION_REJECT",
                "approved_suppression": True,
                "flow_stage": "LIVE/DEFEND",
                "fable_direction": WINDOW_FILL_CAP_DIRECTION_ID,
                "ts": decision_ts,
                "intent_id": row.get("intent_id"),
                "source_wallet": row.get("source_wallet"),
                "market_slug": row.get("market_slug"),
                "outcome": row.get("outcome"),
                "limit_price": row.get("limit_price"),
                "copy_size_usd": row.get("copy_size_usd"),
                "per_window_fill_cap": row.get("per_window_fill_cap"),
                "window_fills_before": row.get("window_fills_before"),
                "window_submitted_before": row.get("window_submitted_before"),
                "window_occupied_before": row.get("window_occupied_before"),
                "taxonomy": "window_fill_cap",
                "reject_reason": "window_fill_cap",
            }
        )
    append_jsonl_many(event_log_path, events)
    return len(events)


def _inventory_order_status_by_intent_id(live_ledger_state: str) -> dict[str, str]:
    ledger = load_json(live_ledger_state, default={}, cache_readonly=True)
    orders = ledger.get("orders") if isinstance(ledger, dict) else []
    status_by_intent: dict[str, str] = {}
    for row in orders if isinstance(orders, list) else []:
        if not isinstance(row, dict):
            continue
        intent_id = str(row.get("intent_id") or "")
        if not intent_id:
            continue
        source_intent = _as_dict(row.get("source_intent"))
        metadata = _as_dict(source_intent.get("metadata"))
        if not _copy_model_is_inventory_like(row.get("copy_model") or metadata.get("copy_model")):
            continue
        status_by_intent[intent_id] = str(row.get("final_status") or row.get("status") or "").upper()
    return status_by_intent


def _participation_key(row: dict[str, Any]) -> tuple[str, str, str, str] | None:
    source_wallet = str(row.get("source_wallet") or "").lower()
    market_slug = str(row.get("market_slug") or "")
    condition_id = str(row.get("condition_id") or "")
    outcome = str(row.get("outcome") or "")
    if not source_wallet or not (market_slug or condition_id) or not outcome:
        return None
    return (source_wallet, market_slug or condition_id, condition_id, outcome)


def _participation_window_start(row: dict[str, Any]) -> float:
    try:
        explicit = float(row.get("window_start_s") or 0.0)
    except (TypeError, ValueError):
        explicit = 0.0
    if explicit > 0:
        return explicit
    slug = str(row.get("market_slug") or "")
    marker = slug.rsplit("-", 1)[-1]
    if slug.startswith("btc-updown-5m-") and marker.isdigit():
        return float(marker)
    return 0.0


def _participation_window_close_ts(row: dict[str, Any]) -> float | None:
    try:
        explicit = float(row.get("window_close_ts") or 0.0)
    except (TypeError, ValueError):
        explicit = 0.0
    if explicit > 0:
        return explicit
    start = _participation_window_start(row)
    return start + 300.0 if start > 0 else None


_PARTICIPATION_STALE_OPEN_GRACE_S = 30.0 * 60.0


def _participation_miss_evaluable(row: dict[str, Any], *, now_ts: float | None = None) -> tuple[bool, str]:
    window_start_s = _participation_window_start(row)
    if window_start_s > 0 and now_ts is not None and window_start_s > now_ts:
        return False, "slug_window_in_future"
    close_ts = _participation_window_close_ts(row)
    if close_ts is not None and now_ts is not None:
        if close_ts > now_ts:
            return False, "market_close_in_future"
        if now_ts >= close_ts + _PARTICIPATION_STALE_OPEN_GRACE_S:
            return True, "market_lifecycle_evaluable_after_close_grace"
    time_to_close_value = row.get("time_to_close_s")
    if time_to_close_value is not None:
        try:
            time_to_close_s = float(time_to_close_value)
        except (TypeError, ValueError):
            time_to_close_s = 0.0
        if time_to_close_s > 0:
            return False, "market_still_open"
    return True, "market_lifecycle_evaluable"


def _participation_missed_active_window(row: dict[str, Any], *, now_ts: float | None = None) -> bool:
    eligible = int(row.get("wallet_eligible_orders") or 0)
    submits = int(row.get("our_submits") or 0)
    if eligible <= 0 or submits > 0:
        return False
    evaluable, reason = _participation_miss_evaluable(row, now_ts=now_ts)
    row["miss_pending_market_lifecycle"] = not evaluable
    row["miss_pending_market_lifecycle_reason"] = reason
    return evaluable


def _participation_window_key(row: dict[str, Any]) -> tuple[str, str] | None:
    source_wallet = str(row.get("source_wallet") or "").lower()
    market_slug = str(row.get("market_slug") or row.get("condition_id") or "")
    if not market_slug:
        return None
    return source_wallet, market_slug


def _participation_window_rollups(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rollups_by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = _participation_window_key(row)
        if key is None:
            continue
        rollup = rollups_by_key.setdefault(
            key,
            {
                "flow_stage": "LIVE",
                "source_wallet": key[0],
                "market_slug": key[1],
                "window_start_s": _participation_window_start(row),
                "condition_ids": set(),
                "outcomes": set(),
                "wallet_eligible_orders": 0,
                "our_submits": 0,
                "our_fills": 0,
                "our_attempts": 0,
                "dominant_skip_reason_counts": Counter(),
            },
        )
        if row.get("condition_id"):
            rollup["condition_ids"].add(str(row.get("condition_id")))
        if row.get("outcome"):
            rollup["outcomes"].add(str(row.get("outcome")))
        rollup["wallet_eligible_orders"] += int(row.get("wallet_eligible_orders") or 0)
        rollup["our_submits"] += int(row.get("our_submits") or 0)
        rollup["our_fills"] += int(row.get("our_fills") or 0)
        rollup["our_attempts"] += int(row.get("our_attempts") or 0)
        rollup["dominant_skip_reason_counts"][str(row.get("dominant_skip_reason") or "unknown")] += 1

    rollups: list[dict[str, Any]] = []
    for rollup in rollups_by_key.values():
        reason_counts = rollup["dominant_skip_reason_counts"]
        dominant_reason = reason_counts.most_common(1)[0][0] if reason_counts else "unknown"
        child_rows = [
            row
            for row in rows
            if _participation_window_key(row) == (rollup.get("source_wallet"), rollup.get("market_slug"))
        ]
        pending_reasons = Counter(
            str(row.get("miss_pending_market_lifecycle_reason") or "unknown")
            for row in child_rows
            if row.get("miss_pending_market_lifecycle")
        )
        out = {
            **rollup,
            "condition_ids": sorted(rollup["condition_ids"]),
            "outcomes": sorted(rollup["outcomes"]),
            "dominant_skip_reason_counts": dict(sorted(reason_counts.items())),
            "dominant_skip_reason": dominant_reason,
            "miss_pending_market_lifecycle": bool(pending_reasons),
            "miss_pending_market_lifecycle_reasons": dict(sorted(pending_reasons.items())),
            "missed_active_window": bool(
                child_rows
                and all(row.get("missed_active_window") for row in child_rows)
                and not pending_reasons
            ),
        }
        rollups.append(annotate_participation_window(out, child_rows))
    return sorted(
        rollups,
        key=lambda row: float(row.get("window_start_s") or 0.0),
        reverse=True,
    )


def _blocked_decision_reasons(summary: dict[str, Any]) -> dict[str, str]:
    reasons: dict[str, str] = {}
    for row in summary.get("sample_decisions") or []:
        if not isinstance(row, dict):
            continue
        intent_id = str(row.get("intent_id") or "")
        reason = str(row.get("reason") or "")
        if intent_id and reason and str(row.get("status") or "") != PASS and reason != "accepted":
            reasons[intent_id] = reason
    for row in summary.get("sample_filtered_intents") or []:
        if not isinstance(row, dict):
            continue
        intent_id = str(row.get("intent_id") or "")
        reason = str(row.get("taxonomy") or row.get("reason") or "")
        if intent_id and reason:
            reasons[intent_id] = reason
    return reasons


def _window_participation_counters(
    intent_summary: dict[str, Any],
    *,
    final_intents: list[CopyIntent],
    live_dedupe_summary: dict[str, Any],
    live_ledger_state: str,
    submitted: bool,
    now_ts: float | None = None,
) -> dict[str, Any]:
    prefilter = _as_dict(intent_summary.get("live_event_prefilter"))
    source_rows = [
        dict(row)
        for row in prefilter.get("inventory_window_participation") or []
        if isinstance(row, dict)
    ]
    if not source_rows:
        return {
            "enabled": False,
            "flow_stage": "LIVE",
            "reason": "no_inventory_window_participation_rows",
            "rows": [],
            "active_windows": 0,
            "missed_active_windows": 0,
            "consecutive_missed_active_windows": 0,
            "pending_market_lifecycle_windows": 0,
            "incident_triggered": False,
        }

    positions = _submitted_position_by_key(live_ledger_state)
    status_by_intent = _inventory_order_status_by_intent_id(live_ledger_state)
    final_intent_ids = {intent.intent_id for intent in final_intents}
    deduped_intent_ids = {str(item) for item in live_dedupe_summary.get("skipped_intent_ids") or [] if str(item)}
    drift_reasons = _blocked_decision_reasons(_as_dict(intent_summary.get("drift_buffer")))
    best_ask_reasons = _blocked_decision_reasons(_as_dict(intent_summary.get("inventory_best_ask_gate")))
    hard_cap_reasons = _blocked_decision_reasons(_as_dict(intent_summary.get("live_hard_entry_cap")))
    hard_floor_reasons = _blocked_decision_reasons(_as_dict(intent_summary.get("live_hard_entry_floor")))
    entry_price_band_reasons = _blocked_decision_reasons(_as_dict(intent_summary.get("entry_price_band_gate")))
    profit_latency_reasons = _blocked_decision_reasons(_as_dict(intent_summary.get("profit_latency_suppression")))
    toxicity_reasons = _blocked_decision_reasons(_as_dict(intent_summary.get("toxicity_protection")))
    window_fill_cap_reasons = _blocked_decision_reasons(_as_dict(intent_summary.get("window_fill_cap")))
    now_ts = time.time() if now_ts is None else float(now_ts)

    rows: list[dict[str, Any]] = []
    reason_counts: Counter[str] = Counter()
    for row in source_rows[:20]:
        out = _inventory_window_participation_row(row)
        key = _participation_key(out)
        if key is not None:
            position = positions.get(key) or {}
            out["our_submits"] = int(position.get("orders") or out.get("our_submits") or 0)
            out["our_fills"] = int(position.get("fills") or out.get("our_fills") or 0)
            out["our_attempts"] = int(
                position.get("converge_orders") or out.get("our_attempts") or out.get("our_submits") or 0
            )
        intent_id = str(out.get("intent_id") or "")
        status = str(out.get("status") or "")
        reason = str(out.get("dominant_skip_reason") or row.get("reason") or "")
        if status != "PASS":
            reason = reason or str(row.get("reason") or "inventory_group_skipped")
        elif intent_id in drift_reasons:
            reason = drift_reasons[intent_id]
        elif intent_id in best_ask_reasons:
            reason = best_ask_reasons[intent_id]
        elif intent_id in hard_cap_reasons:
            reason = hard_cap_reasons[intent_id]
        elif intent_id in hard_floor_reasons:
            reason = hard_floor_reasons[intent_id]
        elif intent_id in entry_price_band_reasons:
            reason = entry_price_band_reasons[intent_id]
        elif intent_id in profit_latency_reasons:
            reason = profit_latency_reasons[intent_id]
        elif intent_id in toxicity_reasons:
            reason = toxicity_reasons[intent_id]
        elif intent_id in window_fill_cap_reasons:
            reason = window_fill_cap_reasons[intent_id]
        elif intent_id in deduped_intent_ids:
            reason = "already_submitted_intent"
        elif intent_id in final_intent_ids:
            live_status = status_by_intent.get(intent_id, "")
            if submitted and live_status == "FILLED":
                reason = "filled"
            elif submitted and live_status == "SUBMITTED":
                reason = "submitted"
            elif submitted and live_status == "REJECTED":
                reason = "live_order_rejected"
            elif submitted:
                reason = "submitted"
            else:
                reason = "ready_to_submit"
        elif status == "PASS":
            reason = "filtered_after_inventory_build"
        reason = reason or "unknown"
        out["dominant_skip_reason"] = reason
        window_start_s = _participation_window_start(out)
        latest_observed_ts = out.get("latest_observed_ts")
        if window_start_s > 0 and latest_observed_ts is not None:
            try:
                observed_delta_s = round(float(latest_observed_ts) - window_start_s, 6)
                out["slug_observed_delta_s"] = observed_delta_s
                if out.get("observed_slug_epoch_delta_s") is None:
                    out["observed_slug_epoch_delta_s"] = observed_delta_s
            except (TypeError, ValueError):
                pass
        out["missed_active_window"] = _participation_missed_active_window(out, now_ts=now_ts)
        annotate_participation_item(out)
        reason_counts[reason] += 1
        rows.append(out)

    window_rollups = _participation_window_rollups(rows)
    pending_market_lifecycle_windows = sum(
        1
        for row in window_rollups
        if int(row.get("wallet_eligible_orders") or 0) > 0 and row.get("miss_pending_market_lifecycle")
    )
    active_windows = [
        row
        for row in window_rollups
        if int(row.get("wallet_eligible_orders") or 0) > 0 and not row.get("miss_pending_market_lifecycle")
    ]
    missed_active_windows = sum(1 for row in active_windows if row.get("missed_active_window"))
    consecutive_missed = 0
    for row in active_windows:
        if row.get("missed_active_window"):
            consecutive_missed += 1
            continue
        break
    adjusted_participation = summarize_adjusted_participation(
        active_windows,
        incident_threshold_windows=PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS,
    )
    raw_incident_triggered = consecutive_missed >= PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS
    return {
        "enabled": True,
        "flow_stage": "LIVE",
        "schema_version": 1,
        "rows": rows,
        "window_rollups": window_rollups,
        "active_windows": len(active_windows),
        "missed_active_windows": missed_active_windows,
        "consecutive_missed_active_windows": consecutive_missed,
        "pending_market_lifecycle_windows": pending_market_lifecycle_windows,
        "incident_threshold_windows": PARTICIPATION_INCIDENT_THRESHOLD_WINDOWS,
        "raw_incident_triggered": raw_incident_triggered,
        "incident_triggered": bool(adjusted_participation.get("adjusted_incident_triggered")),
        "adjusted_participation": adjusted_participation,
        "dominant_skip_reason_counts": dict(sorted(reason_counts.items())),
        "rule": (
            "raw_window_wallet_eligible_orders_gt_0_and_window_our_submits_eq_0_is_raw_miss;"
            " adjusted_skip_taxonomy_excludes_no_signal_and_counts_correct_skip_as_participation_equivalent;"
            " current_generation_incident_threshold_6"
        ),
    }


def _operator_gate_blockers(args: argparse.Namespace, snapshot: LiveAdmissionSnapshot) -> list[str]:
    blockers: list[str] = []
    if not args.operator_approval_id:
        blockers.append("operator_approval_id_missing")
    if Path(args.runtime_live_paused_flag).exists():
        blockers.append("runtime_live_paused_flag_present")
    if args.execute_live:
        if not args.explicit_live_operator_go:
            blockers.append("explicit_live_operator_go_missing")
        if not args.live_orders_allowed:
            blockers.append("live_orders_allowed_flag_missing")
    return blockers


def _operator_promoted_snapshot(
    args: argparse.Namespace,
    snapshot: LiveAdmissionSnapshot,
    *,
    candidate: dict[str, Any] | None = None,
) -> LiveAdmissionSnapshot:
    """Create the live execution snapshot without mutating the paper proof state."""

    if not (args.execute_live and args.explicit_live_operator_go and args.live_orders_allowed and args.operator_approval_id):
        return snapshot
    promoted = replace(
        snapshot,
        paper_only=False,
        live_orders_allowed=True,
        operator_approval_id=str(args.operator_approval_id),
    )
    if not candidate:
        return promoted

    policy = _as_dict(candidate.get("policy"))
    policy_id = str(policy.get("policy_id") or candidate.get("policy_id") or "")
    sizing_policy_id = str(policy.get("sizing_policy_id") or candidate.get("sizing_policy_id") or "")
    source_wallet = _candidate_source_wallet(candidate)
    candidate_type = str(candidate.get("candidate_type") or "")
    overrides: dict[str, Any] = {}
    if policy_id:
        overrides["candidate_policy_id"] = policy_id
    if sizing_policy_id:
        overrides["sizing_policy_id"] = sizing_policy_id
    if source_wallet:
        overrides["candidate_source_wallet"] = source_wallet
    if candidate_type:
        overrides["candidate_type"] = candidate_type
    if _as_dict(candidate.get("live_target_profile")).get("status") == PASS:
        overrides["live_tracker_truth_status"] = PASS
    if not overrides:
        return promoted
    return replace(promoted, **overrides)


async def _execute_live(
    *,
    intents: list[CopyIntent],
    token_map: dict[str, list[str]],
    snapshot: LiveAdmissionSnapshot,
    args: argparse.Namespace,
) -> dict[str, Any]:
    executor = await _live_trade_executor(
        max_buy_price=float(getattr(args, "wallet_copy_max_buy_price", 0.0) or 0.0),
        min_buy_price=float(getattr(args, "wallet_copy_min_buy_price", 0.0) or 0.0),
    )
    live_intents = [promote_intent_for_live(intent, operator_approval_id=args.operator_approval_id) for intent in intents]
    adapter = CopyExecutionAdapter(
        gate=ExecutionGate(
            mode="live",
            explicit_operator_go=bool(args.explicit_live_operator_go),
            live_orders_allowed=bool(args.live_orders_allowed),
            dry_run=False,
            admission_snapshot=snapshot,
        ),
        live_lifecycle=LiveWalletCopyLifecycle(
            LiveExecutionLedgerConfig(state_path=args.live_ledger_state, event_log_path=args.live_ledger_event_log)
        ),
        trade_executor=executor,
        enable_maker_fallback=bool(args.enable_maker_fallback),
        per_window_fill_cap=int(getattr(args, "per_window_fill_cap", DEFAULT_PER_WINDOW_FILL_CAP)),
    )
    return await adapter.execute_async(live_intents, clob_token_ids_by_condition=token_map)


async def _live_trade_executor(*, max_buy_price: float = 0.0, min_buy_price: float = 0.0) -> Any:
    """Return the warm process-local executor used by the fused live guard."""

    global _LIVE_TRADE_EXECUTOR, _LIVE_TRADE_EXECUTOR_SIGNATURE
    from src.config import Config  # noqa: PLC0415
    from src.trade_executor import TradeExecutor  # noqa: PLC0415

    config = Config()
    hard_cap = max(0.0, float(max_buy_price))
    hard_floor = max(0.0, float(min_buy_price))
    if hard_floor > 0:
        config.wallet_copy_min_buy_price = hard_floor
    if hard_cap > 0:
        config.wallet_copy_max_buy_price = hard_cap
        chase_cap = max(0.0, float(getattr(config, "wallet_copy_chase_max_price", 0.0) or 0.0))
        if chase_cap > hard_cap:
            config.wallet_copy_chase_max_price = hard_cap
    config.validate_execution_ready()
    signature = (
        str(config.clob_host),
        int(config.chain_id),
        str(config.polymarket_proxy or ""),
        bool(config.private_key),
        bool(config.builder_api_key),
        round(float(getattr(config, "wallet_copy_min_buy_price", 0.0) or 0.0), 6),
        round(float(getattr(config, "wallet_copy_max_buy_price", 0.0) or 0.0), 6),
        round(float(getattr(config, "wallet_copy_chase_max_price", 0.0) or 0.0), 6),
    )
    if _LIVE_TRADE_EXECUTOR is not None and _LIVE_TRADE_EXECUTOR_SIGNATURE == signature:
        return _LIVE_TRADE_EXECUTOR

    executor = TradeExecutor(config)
    await executor.initialize()
    if getattr(executor, "mock_mode", False):
        raise RuntimeError("live wallet-copy execution requires real CLOB client; TradeExecutor is in mock mode")
    _LIVE_TRADE_EXECUTOR = executor
    _LIVE_TRADE_EXECUTOR_SIGNATURE = signature
    return executor


async def _cancel_due_maker_fallbacks(args: argparse.Namespace) -> dict[str, Any]:
    lifecycle = LiveWalletCopyLifecycle(
        LiveExecutionLedgerConfig(state_path=args.live_ledger_state, event_log_path=args.live_ledger_event_log)
    )
    due = lifecycle.due_maker_fallback_orders()
    if not due:
        return {
            "status": "NO_DUE_MAKER_FALLBACKS",
            "enabled": bool(args.enable_maker_fallback),
            "due_orders": 0,
        }
    executor = await _live_trade_executor(
        max_buy_price=float(getattr(args, "wallet_copy_max_buy_price", 0.0) or 0.0),
        min_buy_price=float(getattr(args, "wallet_copy_min_buy_price", 0.0) or 0.0),
    )
    result = await lifecycle.cancel_due_maker_fallback_orders(executor)
    return {**result, "enabled": bool(args.enable_maker_fallback)}


def run_live_execution(args: argparse.Namespace) -> tuple[int, dict[str, Any]]:
    cycle_started = time.perf_counter()
    stage_started = cycle_started
    runtime_stage_timers: list[dict[str, Any]] = []

    def mark_stage(name: str) -> None:
        nonlocal stage_started
        now = time.perf_counter()
        runtime_stage_timers.append(
            {
                "name": name,
                "duration_s": round(now - stage_started, 6),
                "elapsed_s": round(now - cycle_started, 6),
            }
        )
        stage_started = now

    profit_state = load_json(args.profit_state, default={})
    if not isinstance(profit_state, dict):
        profit_state = {}
    mark_stage("load_profit_state")
    selected_override = _selected_candidate_override(args)
    candidate = selected_override or _selected_candidate(profit_state, override_id=args.candidate_id)
    if args.candidate_id and str(candidate.get("candidate_id") or "") != str(args.candidate_id):
        rotation_candidate = _promotion_rotation_runtime_candidate(
            str(getattr(args, "promotion_rotation_state", DEFAULT_PROMOTION_ROTATION_STATE) or ""),
            candidate_id_pin=str(args.candidate_id or ""),
        )
        if rotation_candidate:
            candidate = rotation_candidate
        else:
            mission_candidate = _mission_active_member_candidate(candidate_id_pin=str(args.candidate_id or ""))
            if mission_candidate:
                candidate = mission_candidate
    candidate = _mission_runtime_candidate_policy(candidate)
    candidate = _apply_selected_candidate_override(candidate, selected_override)
    runtime_paused = Path(args.runtime_live_paused_flag).exists()
    mark_stage("select_candidate")
    proof_snapshot = LiveAdmissionSnapshot.from_profit_state(
        profit_state,
        operator_approval_id=args.operator_approval_id,
        runtime_live_paused=runtime_paused,
    )
    execution_snapshot = _operator_promoted_snapshot(args, proof_snapshot, candidate=candidate)
    mark_stage("build_admission_snapshots")
    intents, intent_summary, intent_blockers, history_token_map = _build_candidate_intents(
        candidate=candidate,
        history_state=args.history_state,
        args=args,
    )
    built_intent_rows = [intent.asdict() for intent in intents]
    mark_stage("build_candidate_intents")
    alpha_decay_report, alpha_decay_profiles, alpha_decay_profile_cache = _load_alpha_decay_profile_cache(
        args.alpha_decay_report
    )
    mark_stage("load_alpha_decay_profiles")
    intents, drift_buffer_summary = _apply_drift_buffer_policy(
        intents,
        alpha_decay_report=alpha_decay_report,
        execution_profiles=alpha_decay_profiles,
        profile_cache_summary=alpha_decay_profile_cache,
        enabled=bool(args.enable_drift_buffer),
        max_buffer_price=float(args.max_drift_buffer_price),
    )
    mark_stage("apply_drift_buffer")
    if _filter_removed_all_intents(drift_buffer_summary, intents):
        intent_blockers.append("drift_buffer_filtered_candidate_copy_intents")
    intent_summary["drift_buffer"] = drift_buffer_summary
    intent_summary["fresh_candidate_intents_after_drift_buffer"] = len(intents)
    passive_at_source_sealed = _passive_at_source_holdout_sealed()
    intents, inventory_best_ask_gate_summary = _apply_inventory_best_ask_gate(
        intents,
        timeout_s=float(getattr(args, "inventory_best_ask_timeout_s", 1.0)),
        enable_maker_fallback=bool(getattr(args, "enable_maker_fallback", False)),
        passive_at_source_sealed=passive_at_source_sealed,
        generation_sha256=str(
            getattr(args, "live_guard_generation_sha256", "") or ""
        ),
        max_sweep_drift_price=float(
            getattr(args, "max_drift_buffer_price", 0.05) or 0.05
        ),
    )
    if _filter_removed_all_intents(inventory_best_ask_gate_summary, intents):
        intent_blockers.append("inventory_best_ask_filtered_candidate_copy_intents")
    mark_stage("inventory_best_ask_gate")
    intent_summary["inventory_best_ask_gate"] = inventory_best_ask_gate_summary
    intent_summary["fresh_candidate_intents_after_inventory_best_ask_gate"] = len(intents)
    intents, hard_entry_cap_summary = _apply_live_hard_buy_price_cap(
        intents,
        max_buy_price=float(getattr(args, "wallet_copy_max_buy_price", 0.0) or 0.0),
    )
    if _filter_removed_all_intents(hard_entry_cap_summary, intents):
        intent_blockers.append("hard_entry_cap_filtered_candidate_copy_intents")
    mark_stage("live_hard_entry_cap")
    intent_summary["live_hard_entry_cap"] = hard_entry_cap_summary
    intent_summary["fresh_candidate_intents_after_hard_entry_cap"] = len(intents)
    intents, hard_entry_floor_summary = _apply_live_hard_buy_price_floor(
        intents,
        min_buy_price=float(getattr(args, "wallet_copy_min_buy_price", 0.0) or 0.0),
    )
    if _filter_removed_all_intents(hard_entry_floor_summary, intents):
        intent_blockers.append("hard_entry_floor_filtered_candidate_copy_intents")
    mark_stage("live_hard_entry_floor")
    intent_summary["live_hard_entry_floor"] = hard_entry_floor_summary
    intent_summary["fresh_candidate_intents_after_hard_entry_floor"] = len(intents)
    intents, entry_price_band_gate_summary = _apply_entry_price_band_gate(
        intents,
        config_path=str(
            getattr(args, "entry_price_band_gate_config", DEFAULT_ENTRY_PRICE_BAND_GATE_CONFIG)
        ),
    )
    entry_price_band_events_written = _append_entry_price_band_gate_events(
        str(getattr(args, "live_ledger_event_log", "data/research/wallet_copy_live_execution_events.jsonl")),
        entry_price_band_gate_summary,
        decision_ts=utc_now_iso(),
    )
    if entry_price_band_events_written:
        entry_price_band_gate_summary["events_ledger_appended"] = entry_price_band_events_written
        entry_price_band_gate_summary["events_ledger_path"] = str(
            getattr(args, "live_ledger_event_log", "data/research/wallet_copy_live_execution_events.jsonl")
        )
    if _filter_removed_all_intents(entry_price_band_gate_summary, intents):
        intent_blockers.append("entry_price_band_gate_filtered_candidate_copy_intents")
    mark_stage("entry_price_band_gate")
    intent_summary["entry_price_band_gate"] = entry_price_band_gate_summary
    intent_summary["fresh_candidate_intents_after_entry_price_band_gate"] = len(intents)
    intents, profit_latency_suppression_summary = _apply_profit_latency_suppression_gate(
        intents,
        now_ts=time.time(),
        window_time_suppress_gte_s=float(getattr(args, "profit_latency_window_time_suppress_gte_s", 0.0) or 0.0),
        signal_age_suppress_gte_s=float(getattr(args, "profit_latency_signal_age_suppress_gte_s", 0.0) or 0.0),
    )
    suppression_events_written = _append_profit_latency_suppression_events(
        str(getattr(args, "live_ledger_event_log", "data/research/wallet_copy_live_execution_events.jsonl")),
        profit_latency_suppression_summary,
        decision_ts=utc_now_iso(),
    )
    if suppression_events_written:
        profit_latency_suppression_summary["events_ledger_appended"] = suppression_events_written
        profit_latency_suppression_summary["events_ledger_path"] = str(
            getattr(args, "live_ledger_event_log", "data/research/wallet_copy_live_execution_events.jsonl")
        )
    if _filter_removed_all_intents(profit_latency_suppression_summary, intents):
        intent_blockers.append("profit_latency_suppression_filtered_candidate_copy_intents")
    mark_stage("profit_latency_suppression_gate")
    intent_summary["profit_latency_suppression"] = profit_latency_suppression_summary
    intent_summary["fresh_candidate_intents_after_profit_latency_suppression"] = len(intents)
    intents, toxicity_protection_summary = _apply_toxicity_protection_gate(
        intents,
        config_path=str(getattr(args, "toxicity_denylist_config", "configs/wallet_copy/toxicity_denylist.json")),
    )
    mark_stage("toxicity_protection_gate")
    intent_summary["toxicity_protection"] = toxicity_protection_summary
    intent_summary["fresh_candidate_intents_after_toxicity_protection"] = len(intents)
    min_live_order_usd = float(args.min_live_order_usd) if bool(args.execute_live) else 0.0
    intents, live_min_order_floor_summary = _floor_live_min_order_intents(
        intents,
        min_live_order_usd=min_live_order_usd,
    )
    mark_stage("live_min_order_floor")
    intent_summary["live_min_order_floor"] = live_min_order_floor_summary
    intent_summary["fresh_candidate_intents_below_live_min_order"] = sum(
        1 for intent in intents if min_live_order_usd > 0 and float(intent.copy_size_usd) < min_live_order_usd
    )
    intents, expected_fee_gate_summary = _apply_expected_fee_capture_gate(intents)
    mark_stage("expected_fee_capture_gate")
    intent_summary["expected_fee_capture_gate"] = expected_fee_gate_summary
    intent_summary["fresh_candidate_intents_after_expected_fee_gate"] = len(intents)
    precision_config = Config()
    intents, precision_preflight_summary = _apply_market_buy_precision_preflight(
        intents,
        min_order_usd=min_live_order_usd,
        hard_max_buy_price=float(getattr(args, "wallet_copy_max_buy_price", 0.0) or 0.0),
        max_chase_ticks=int(getattr(precision_config, "wallet_copy_max_chase_ticks", 0) or 0),
        chase_max_price=float(getattr(precision_config, "wallet_copy_chase_max_price", 0.0) or 0.0),
        chase_tick_size=float(getattr(precision_config, "wallet_copy_chase_tick_size", 0.01) or 0.01),
        enable_maker_fallback=bool(getattr(args, "enable_maker_fallback", False)),
        passive_at_source_sealed=passive_at_source_sealed,
    )
    precision_events_written = _append_market_buy_precision_preflight_events(
        str(getattr(args, "live_ledger_event_log", "data/research/wallet_copy_live_execution_events.jsonl")),
        precision_preflight_summary,
        decision_ts=utc_now_iso(),
    )
    if precision_events_written:
        precision_preflight_summary["events_ledger_appended"] = precision_events_written
    if _filter_removed_all_intents(precision_preflight_summary, intents):
        intent_blockers.append("market_buy_precision_infeasible_candidate_copy_intents")
    mark_stage("market_buy_precision_preflight")
    intent_summary["market_buy_precision_preflight"] = precision_preflight_summary
    intent_summary["fresh_candidate_intents_after_market_buy_precision_preflight"] = len(intents)
    no_fresh_intents = "fresh_candidate_copy_intents_missing" in set(intent_blockers)
    allowed_no_fresh_blockers = {
        "candidate_source_events_missing",
        "fresh_candidate_copy_intents_missing",
        "history_events_missing",
    }
    if (
        args.allow_no_fresh_intents
        and no_fresh_intents
        and set(intent_blockers).issubset(allowed_no_fresh_blockers)
    ):
        intent_blockers = []
    intents, live_dedupe_summary = _drop_already_live_submitted_intents(
        intents,
        live_ledger_state=args.live_ledger_state,
    )
    mark_stage("drop_already_submitted")
    intents, window_fill_cap_summary = _apply_window_fill_cap_gate(
        intents,
        live_ledger_state=args.live_ledger_state,
        per_window_fill_cap=int(getattr(args, "per_window_fill_cap", DEFAULT_PER_WINDOW_FILL_CAP)),
    )
    window_fill_cap_events_written = _append_window_fill_cap_events(
        str(getattr(args, "live_ledger_event_log", "data/research/wallet_copy_live_execution_events.jsonl")),
        window_fill_cap_summary,
        decision_ts=utc_now_iso(),
    )
    if window_fill_cap_events_written:
        window_fill_cap_summary["events_ledger_appended"] = window_fill_cap_events_written
        window_fill_cap_summary["events_ledger_path"] = str(
            getattr(args, "live_ledger_event_log", "data/research/wallet_copy_live_execution_events.jsonl")
        )
    if _filter_removed_all_intents(window_fill_cap_summary, intents):
        intent_blockers.append("window_fill_cap_filtered_candidate_copy_intents")
    mark_stage("window_fill_cap_gate")
    intent_summary["window_fill_cap"] = window_fill_cap_summary
    intent_summary["fresh_candidate_intents_after_window_fill_cap"] = len(intents)
    no_new_live_intents = bool(live_dedupe_summary.get("input_intents")) and not intents
    parity_capsules_profile: dict[str, Any] = {}
    capsules, token_map, token_blockers = _parity_capsules(
        intents,
        operator_approval_id=args.operator_approval_id,
        gamma_timeout_s=float(args.gamma_timeout_s),
        fallback_token_map=history_token_map,
        profile_out=parity_capsules_profile,
    )
    mark_stage("build_parity_capsules")
    proof_blockers = _proof_blockers(profit_state, proof_snapshot, candidate=candidate)
    gate_blockers = _operator_gate_blockers(args, proof_snapshot)
    blockers = sorted(set(proof_blockers + intent_blockers + token_blockers + (gate_blockers if args.execute_live else [])))
    mark_stage("evaluate_gates")

    maker_fallback_cancel_result: dict[str, Any] = {}
    if (
        args.execute_live
        and not bool(getattr(args, "suppress_submission", False))
        and bool(args.enable_maker_fallback)
        and not gate_blockers
    ):
        maker_fallback_cancel_result = asyncio.run(_cancel_due_maker_fallbacks(args))
    mark_stage("cancel_due_maker_fallbacks")

    suppress_submission = bool(getattr(args, "suppress_submission", False))
    planned_intent_rows = [intent.asdict() for intent in intents]
    planned_intent_hash = hashlib.sha256(
        json.dumps(planned_intent_rows, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    execution_result: dict[str, Any] | None = None
    submitted = False
    if args.execute_live and not suppress_submission and not blockers and intents:
        with _LIVE_SUBMIT_LOCK:
            execution_result = asyncio.run(
                _execute_live(intents=intents, token_map=token_map, snapshot=execution_snapshot, args=args)
            )
        submitted = True
    elif args.execute_live and not suppress_submission and not blockers:
        execution_result = {"status": "NO_LIVE_INTENTS", "results": []}
    mark_stage("execute_live_orders")

    if suppress_submission and not blockers and intents:
        status = "LIVE_PLAN_SURVIVOR"
    elif suppress_submission:
        status = "LIVE_PLAN_PROTECTED_NO_SURVIVOR"
    elif submitted:
        status = "LIVE_EXECUTION_SUBMITTED"
    elif no_new_live_intents and not blockers:
        status = "LIVE_ARMED_NO_NEW_INTENTS"
    elif no_fresh_intents and args.allow_no_fresh_intents and not blockers:
        status = "LIVE_ARMED_NO_FRESH_INTENTS"
    elif proof_blockers or intent_blockers or token_blockers:
        status = CORRECTION
    elif gate_blockers:
        status = "LIVE_READY_BEHIND_OPERATOR_GATE"
    else:
        status = "LIVE_ARMED_DRY_RUN"

    window_participation = _window_participation_counters(
        intent_summary,
        final_intents=intents,
        live_dedupe_summary=live_dedupe_summary,
        live_ledger_state=args.live_ledger_state,
        submitted=submitted,
    )
    mark_stage("window_participation")
    intent_summary["window_participation"] = window_participation

    copy_contract = _runtime_copy_contract()
    payload = {
        "schema_version": 1,
        "kind": "wallet_copy_live_execution_arm_state",
        "generated_at": utc_now_iso(),
        "status": status,
        "candidate_id": candidate.get("candidate_id"),
        "candidate_type": candidate.get("candidate_type"),
        "source_wallet": _candidate_source_wallet(candidate),
        "policy_id": _as_dict(candidate.get("policy")).get("policy_id"),
        **copy_contract,
        "execution_filter": {
            "max_event_age_s": float(args.max_event_age_s),
            "live_build_max_observed_age_s": float(
                getattr(args, "live_build_max_observed_age_s", LIVE_BUILD_MAX_OBSERVED_AGE_S)
            ),
            "max_intents": int(args.max_intents),
            "min_live_order_usd": float(args.min_live_order_usd),
            "max_window_usd": float(args.max_window_usd),
            "max_per_wallet_usd": float(args.max_per_wallet_usd),
            "min_inventory_plan_usd": float(args.min_inventory_plan_usd),
            "min_agreeing_wallets": int(args.min_agreeing_wallets),
            "max_price_spread": float(args.max_price_spread),
            "wallet_copy_min_buy_price": float(getattr(args, "wallet_copy_min_buy_price", 0.0) or 0.0),
            "wallet_copy_max_buy_price": float(getattr(args, "wallet_copy_max_buy_price", 0.0) or 0.0),
            "profit_latency_window_time_suppress_gte_s": float(
                getattr(args, "profit_latency_window_time_suppress_gte_s", 0.0) or 0.0
            ),
            "profit_latency_signal_age_suppress_gte_s": float(
                getattr(args, "profit_latency_signal_age_suppress_gte_s", 0.0) or 0.0
            ),
            "alpha_decay_report": args.alpha_decay_report,
            "toxicity_denylist_config": str(
                getattr(args, "toxicity_denylist_config", "configs/wallet_copy/toxicity_denylist.json")
            ),
            "entry_price_band_gate_config": str(
                getattr(args, "entry_price_band_gate_config", DEFAULT_ENTRY_PRICE_BAND_GATE_CONFIG)
            ),
            "enable_drift_buffer": bool(args.enable_drift_buffer),
            "max_drift_buffer_price": float(args.max_drift_buffer_price),
            "enable_maker_fallback": bool(args.enable_maker_fallback),
            "copy_model": str(getattr(args, "copy_model", "per_order") or "per_order"),
            "inventory_late_window_stop_s": float(getattr(args, "inventory_late_window_stop_s", 60.0)),
            "inventory_max_converge_orders_per_window": int(
                getattr(args, "inventory_max_converge_orders_per_window", 6)
            ),
            "inventory_best_ask_timeout_s": float(getattr(args, "inventory_best_ask_timeout_s", 1.0)),
            "drip_min_tranche_usd": float(getattr(args, "drip_min_tranche_usd", DRIP_MIN_TRANCHE_USD)),
            "drip_max_tranche_usd": float(getattr(args, "drip_max_tranche_usd", DRIP_MAX_TRANCHE_USD)),
            "drip_max_tranches_per_window": int(
                getattr(args, "drip_max_tranches_per_window", DRIP_MAX_TRANCHES_PER_WINDOW)
            ),
            "per_window_fill_cap": int(getattr(args, "per_window_fill_cap", DEFAULT_PER_WINDOW_FILL_CAP)),
        },
        "paper_only": not (
            args.execute_live
            and execution_snapshot.paper_only is False
            and execution_snapshot.live_orders_allowed is True
            and not gate_blockers
            and not proof_blockers
        ),
        "live_orders_allowed": bool(
            args.execute_live
            and execution_snapshot.paper_only is False
            and execution_snapshot.live_orders_allowed is True
            and not gate_blockers
            and not proof_blockers
        ),
        "execution_requested": bool(args.execute_live),
        "submission_suppressed": suppress_submission,
        "built_intents": built_intent_rows,
        "planned_intents": planned_intent_rows,
        "planned_intent_hash": planned_intent_hash,
        "planned_token_map": token_map,
        "orders_submitted": len(_as_dict(execution_result).get("results") or []) if execution_result else 0,
        "profit_state": args.profit_state,
        "history_state": args.history_state,
        "selected_candidate": {
            "candidate_id": candidate.get("candidate_id"),
            "candidate_type": candidate.get("candidate_type"),
            "status": candidate.get("status"),
            "source_wallet": _candidate_source_wallet(candidate),
            "policy": candidate.get("policy") if isinstance(candidate.get("policy"), dict) else {},
        },
        "proof_admission_snapshot": proof_snapshot.__dict__,
        "admission_snapshot": execution_snapshot.__dict__,
        "operator_gate": {
            "proof_state_preserved_paper_only": bool(proof_snapshot.paper_only),
            "proof_state_preserved_live_orders_allowed": bool(proof_snapshot.live_orders_allowed),
            "execution_snapshot_promoted": bool(
                execution_snapshot.paper_only is False
                and execution_snapshot.live_orders_allowed is True
                and args.execute_live
            ),
            "operator_approval_id": str(args.operator_approval_id or ""),
        },
        "proof_blockers": proof_blockers,
        "operator_gate_blockers": gate_blockers,
        "intent_blockers": sorted(set(intent_blockers)),
        "token_mapping_blockers": sorted(set(token_blockers)),
        "blockers": blockers,
        "candidate_intent_summary": intent_summary,
        "window_participation": window_participation,
        "drift_buffer": drift_buffer_summary,
        "inventory_best_ask_gate": inventory_best_ask_gate_summary,
        "live_hard_entry_cap": hard_entry_cap_summary,
        "entry_price_band_gate": entry_price_band_gate_summary,
        "profit_latency_suppression": profit_latency_suppression_summary,
        "market_buy_precision_preflight": precision_preflight_summary,
        "toxicity_protection": toxicity_protection_summary,
        "window_fill_cap": window_fill_cap_summary,
        "parity_capsules_profile": parity_capsules_profile,
        "runtime_profile": {
            "flow_stage": "SELF-DEV",
            "total_s": round(time.perf_counter() - cycle_started, 6),
            "stage_timers": runtime_stage_timers,
            "alpha_decay_profile_cache": alpha_decay_profile_cache,
            "execute_live_profile": _as_dict(execution_result).get("execute_live_profile") if execution_result else {},
        },
        "live_dedupe_summary": live_dedupe_summary,
        "token_map_summary": {
            "conditions": len(token_map),
            "mapped_conditions": sum(1 for ids in token_map.values() if len(ids) >= 2),
            "missing_conditions": [key for key, ids in token_map.items() if len(ids) < 2],
        },
        "parity_summary": {
            "capsules": len(capsules),
            "pass_capsules": sum(1 for capsule in capsules if capsule.get("status") == PASS),
            "failed_capsules": [capsule.get("paper_intent_id") for capsule in capsules if capsule.get("status") != PASS],
            "sample_capsules": [
                {key: value for key, value in capsule.items() if key not in {"paper_intent", "live_intent", "live_trade_decision"}}
                for capsule in capsules[:5]
            ],
        },
        "live_connection_plan": {
            "next_gate": (
                "when proof is PASS and operator approval is present, keep live execution only under "
                "the pinned guard; copy selected policy-eligible intents, not every source order 1-to-1"
            ),
            "same_copy_intent_path": True,
            "parity_scope": "selected_policy_eligible_copy_intents",
            "trade_executor_required": "src.trade_executor.TradeExecutor",
            "arm_state_path": args.state,
            "live_ledger_state_path": args.live_ledger_state,
            "live_ledger_event_log_path": args.live_ledger_event_log,
        },
        "execution_result": execution_result or {},
        "maker_fallback_cancel_result": maker_fallback_cancel_result,
    }
    mark_stage("prepare_arm_state")
    payload["runtime_profile"]["total_s"] = round(time.perf_counter() - cycle_started, 6)
    payload["runtime_profile"]["stage_timers"] = runtime_stage_timers
    atomic_write_json(args.state, payload)
    mark_stage("write_arm_state")
    summary = {
        "state": args.state,
        "status": status,
        "candidate_id": payload["selected_candidate"].get("candidate_id"),
        "source_wallet": payload["selected_candidate"].get("source_wallet"),
        "policy_id": payload["selected_candidate"].get("policy", {}).get("policy_id"),
        "selected_candidate": payload["selected_candidate"],
        "candidate_intent_summary": intent_summary,
        "fresh_candidate_intents": intent_summary.get("fresh_candidate_intents"),
        "new_live_candidate_intents": live_dedupe_summary.get("new_intents"),
        "already_submitted_intents": live_dedupe_summary.get("already_submitted_intents"),
        "proof_blockers": proof_blockers,
        "operator_gate_blockers": gate_blockers,
        "intent_blockers": sorted(set(intent_blockers)),
        "token_mapping_blockers": sorted(set(token_blockers)),
        "blockers": blockers,
        "orders_submitted": payload["orders_submitted"],
        "submission_suppressed": suppress_submission,
        "built_intents": built_intent_rows,
        "planned_intents": planned_intent_rows,
        "planned_intent_hash": planned_intent_hash,
        "planned_token_map": token_map,
        "paper_only": payload["paper_only"],
        "live_orders_allowed": payload["live_orders_allowed"],
        "enable_maker_fallback": bool(args.enable_maker_fallback),
        "maker_fallback_cancel_result": maker_fallback_cancel_result,
        "window_participation": window_participation,
        "profit_latency_suppression": profit_latency_suppression_summary,
        "market_buy_precision_preflight": precision_preflight_summary,
        "entry_price_band_gate": entry_price_band_gate_summary,
        "toxicity_protection": toxicity_protection_summary,
        "window_fill_cap": window_fill_cap_summary,
        "runtime_profile": payload["runtime_profile"],
        "parity_capsules_profile": parity_capsules_profile,
    }
    rc = (
        0
        if status
        in {
            "LIVE_READY_BEHIND_OPERATOR_GATE",
            "LIVE_ARMED_DRY_RUN",
            "LIVE_ARMED_NO_FRESH_INTENTS",
            "LIVE_ARMED_NO_NEW_INTENTS",
            "LIVE_EXECUTION_SUBMITTED",
            "LIVE_PLAN_SURVIVOR",
            "LIVE_PLAN_PROTECTED_NO_SURVIVOR",
        }
        else 2
    )
    return rc, summary


def submit_preplanned_live_execution(
    args: argparse.Namespace,
    *,
    plan: dict[str, Any],
) -> tuple[int, dict[str, Any]]:
    """Submit the exact protected plan once, without rebuilding its intents."""
    intent_rows = plan.get("planned_intents") if isinstance(plan.get("planned_intents"), list) else []
    intents = [CopyIntent.from_dict(row) for row in intent_rows if isinstance(row, dict)]
    computed_hash = hashlib.sha256(
        json.dumps([intent.asdict() for intent in intents], sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    expected_hash = str(plan.get("planned_intent_hash") or "")
    token_map = plan.get("planned_token_map") if isinstance(plan.get("planned_token_map"), dict) else {}
    candidate_id = str(plan.get("candidate_id") or "")
    source_wallet = str(plan.get("source_wallet") or "").lower()
    blockers: list[str] = []
    if not intents:
        blockers.append("preplanned_intents_missing")
    if not expected_hash or computed_hash != expected_hash:
        blockers.append("preplanned_intent_hash_mismatch")

    profit_state = load_json(args.profit_state, default={})
    if not isinstance(profit_state, dict):
        profit_state = {}
    selected_override = _selected_candidate_override(args)
    candidate = selected_override or _selected_candidate(profit_state, override_id=candidate_id)
    if candidate_id and str(candidate.get("candidate_id") or "") != candidate_id:
        candidate = (
            _promotion_rotation_runtime_candidate(
                str(getattr(args, "promotion_rotation_state", DEFAULT_PROMOTION_ROTATION_STATE) or ""),
                candidate_id_pin=candidate_id,
            )
            or _mission_active_member_candidate(candidate_id_pin=candidate_id)
            or candidate
        )
    candidate = _apply_selected_candidate_override(
        _mission_runtime_candidate_policy(candidate),
        selected_override,
    )
    proof_snapshot = LiveAdmissionSnapshot.from_profit_state(
        profit_state,
        operator_approval_id=args.operator_approval_id,
        runtime_live_paused=Path(args.runtime_live_paused_flag).exists(),
    )
    execution_snapshot = _operator_promoted_snapshot(args, proof_snapshot, candidate=candidate)
    blockers.extend(_proof_blockers(profit_state, proof_snapshot, candidate=candidate))
    blockers.extend(_operator_gate_blockers(args, proof_snapshot))
    if str(candidate.get("candidate_id") or "") != candidate_id:
        blockers.append("preplanned_candidate_changed")
    if _candidate_source_wallet(candidate).lower() != source_wallet:
        blockers.append("preplanned_source_wallet_changed")

    deduped, dedupe_summary = _drop_already_live_submitted_intents(
        intents,
        live_ledger_state=args.live_ledger_state,
    )
    capped, window_fill_cap = _apply_window_fill_cap_gate(
        deduped,
        live_ledger_state=args.live_ledger_state,
        per_window_fill_cap=int(getattr(args, "per_window_fill_cap", DEFAULT_PER_WINDOW_FILL_CAP)),
    )
    if [intent.intent_id for intent in capped] != [intent.intent_id for intent in intents]:
        blockers.append("preplanned_intents_changed_before_submit")
    if any(
        intent.token_id
        and intent.token_id
        not in (
            token_map.get(intent.condition_id)
            or token_map.get(intent.market_slug)
            or []
        )
        for intent in intents
    ):
        blockers.append("preplanned_token_map_changed")
    blockers = sorted(set(blockers))

    execution_result: dict[str, Any] = {}
    attribution_persisted: dict[str, Any] = {"status": "NOT_SUBMITTED", "orders_updated": 0}
    if not blockers:
        with _LIVE_SUBMIT_LOCK:
            execution_result = asyncio.run(
                _execute_live(
                    intents=intents,
                    token_map=token_map,
                    snapshot=execution_snapshot,
                    args=args,
                )
            )
        ledger = load_json(args.live_ledger_state, default={})
        orders = ledger.get("orders") if isinstance(ledger, dict) and isinstance(ledger.get("orders"), list) else []
        intent_ids = {intent.intent_id for intent in intents}
        updated = 0
        for order in orders:
            if not isinstance(order, dict) or str(order.get("intent_id") or "") not in intent_ids:
                continue
            accepted_at = order.get("submitted_at") or order.get("updated_at")
            order["alternate_transport_attribution"] = {
                "schema_version": 1,
                "flow_stage": "LIVE/ROTATE",
                "source_wallet": source_wallet,
                "candidate_id": candidate_id,
                "policy_id": str(plan.get("policy_id") or _as_dict(candidate.get("policy")).get("policy_id") or ""),
                "planned_intent_hash": computed_hash,
                "plan_hash_verified": True,
                "accepted_at": accepted_at,
                "resolved_post_fee_pnl_usd": None,
                "resolution_status": "PENDING",
            }
            updated += 1
        if updated:
            atomic_write_json(args.live_ledger_state, ledger)
        attribution_persisted = {
            "status": "PERSISTED" if updated else "NO_MATCHING_ORDER",
            "orders_updated": updated,
            "source_wallet": source_wallet,
            "candidate_id": candidate_id,
            "policy_id": str(plan.get("policy_id") or _as_dict(candidate.get("policy")).get("policy_id") or ""),
            "planned_intent_hash": computed_hash,
        }
    status = "LIVE_EXECUTION_SUBMITTED" if not blockers else CORRECTION
    orders_submitted = len(_as_dict(execution_result).get("results") or [])
    summary = {
        "schema_version": 1,
        "kind": "wallet_copy_live_execution_preplanned_submit",
        "generated_at": utc_now_iso(),
        "status": status,
        "candidate_id": candidate_id,
        "source_wallet": source_wallet,
        "policy_id": _as_dict(candidate.get("policy")).get("policy_id"),
        "planned_intents": [intent.asdict() for intent in intents],
        "planned_intent_hash": computed_hash,
        "plan_hash_verified": bool(expected_hash and computed_hash == expected_hash),
        "planned_intents_submitted_unchanged": not blockers,
        "submit_stage_invocations": 1 if not blockers else 0,
        "orders_submitted": orders_submitted,
        "orders_accepted": sum(
            1
            for row in _as_dict(execution_result).get("results") or []
            if str(_as_dict(row).get("status") or "").upper() in {"ACCEPTED", "FILLED", "SUBMITTED"}
        ),
        "blockers": blockers,
        "intent_blockers": [],
        "proof_blockers": _proof_blockers(profit_state, proof_snapshot, candidate=candidate),
        "operator_gate_blockers": _operator_gate_blockers(args, proof_snapshot),
        "live_dedupe_summary": dedupe_summary,
        "window_fill_cap": window_fill_cap,
        "execution_result": execution_result,
        "alternate_transport_attribution": attribution_persisted,
        "paper_only": False if not blockers else True,
        "live_orders_allowed": bool(not blockers),
        "candidate_intent_summary": {
            "fresh_candidate_intents": len(intents),
            "fresh_candidate_intents_after_window_fill_cap": len(capped),
            "sample_intents": [intent.asdict() for intent in intents[:5]],
        },
    }
    atomic_write_json(args.state, summary)
    return (0 if not blockers else 2), summary


def main() -> int:
    rc, summary = run_live_execution(parse_args())
    print(json.dumps(summary, indent=2, sort_keys=True))
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
